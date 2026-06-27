/*-------------------------------------------------------------------------
 *
 * shm_producer.c
 *      Producer side of the ClickHouse SHM block-stream ABI (version 1).
 *
 *      A faithful C port of ClickHouse's reference in-process producer
 *      (src/Storages/SharedMemorySource/TestProducer/InProcessProducer.cpp)
 *      and control socket (Wire/ControlSocket.cpp), driven from a PostgreSQL
 *      backend. Single-threaded: the control socket is serviced (non-blocking
 *      accept + SCM_RIGHTS of the readiness eventfd) opportunistically from
 *      publish() and the ring-full wait loop, so no background thread is needed
 *      inside the backend.
 *
 * Copyright (c) 2025-2026, ClickHouse, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"          /* CHECK_FOR_INTERRUPTS */
#include "utils/memutils.h"     /* MemoryContextCallback */

#include "shm_producer.h"
#include "shm_offload.h"        /* PgchTcpSendMethod */
#include "shm_phase.h"
#include "shm_arrow.h"          /* Apache Arrow IPC serializer (Phase 2 Branch A, D-HC-0207) */

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>
#include <time.h>            /* Phase 3 P2 microbench: clock_gettime/nanosleep for injected send-latency */
#include <unistd.h>
#include <sys/epoll.h>       /* Phase 3 P1: producer-side EPOLLOUT readiness wait for non-blocking send */
#include <sys/eventfd.h>
#include <sys/uio.h>         /* Phase 3 P2: struct iovec / sendmsg for the coalesced pipelined send */
#include <sys/mman.h>
#include <signal.h>             /* kill() for backend-liveness checks */
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <linux/errqueue.h>    /* Branch B B-it4: sock_extended_err + SO_EE_* zero-copy completion flags */

/* --------------------------------------------------------------------- */
/* Wire layout — byte-for-byte mirror of ClickHouse Wire/Layout.h (ABI v1).
 * Atomic fields are plain integers accessed with __atomic_* builtins, which
 * are layout-compatible with std::atomic<T> for scalar T on the supported
 * little-endian targets (x86_64, aarch64). */
/* --------------------------------------------------------------------- */

/* ASCII "SHM_ADOP" as a little-endian uint64. */
#define SHM_MAGIC          UINT64_C(0x504F44415F4D4853)
#define SHM_ABI_VERSION_1  1u
#define SHM_IMPL_MAX_K     256u
#define SHM_IMPL_MAX_COLS  128u   /* raised 64->128 for ClickBench Q24 SELECT * (105 cols);
                                   * must match consumer IMPL_MAX_COLUMNS (Wire/Layout.h).
                                   * Soft validation bound -- schema/descriptor regions are
                                   * sized dynamically by n_columns, not a fixed [64] array. */
#define SHM_IMPL_MAX_ROWS  (1u << 20)
#define SHM_PADDING_FOR_SIMD 64u
#define SHM_SCHEMA_STR_MAX 64u

enum { SHM_STATE_EMPTY = 0, SHM_STATE_WRITING = 1, SHM_STATE_PUBLISHED = 2 };

typedef struct ShmHandshake {
    uint64_t magic;                 /* release-stored last */
    uint32_t abi_version;
    uint32_t ring_depth_k;
    uint32_t schema_count;
    uint32_t reserved_pad32;
    uint64_t slot_table_offset;
    uint64_t slot_table_stride;
    uint64_t data_region_offset;
    uint64_t data_region_size;
    uint64_t schema_table_offset;
    uint64_t schema_table_size;
    uint64_t reserved64[6];
} __attribute__((aligned(64))) ShmHandshake;

typedef struct ShmSlot {
    uint32_t state;
    uint32_t slot_index;
    uint64_t transition_counter;
    uint64_t sequence;
    uint64_t retain_refcount;
    uint8_t  eos_marker;
    uint8_t  reserved_pad8[7];
    uint64_t row_count;
    uint64_t per_column_descriptors_offset;
} __attribute__((aligned(64))) ShmSlot;

typedef struct ShmSchemaEntry {
    char name[SHM_SCHEMA_STR_MAX];
    char type_string[SHM_SCHEMA_STR_MAX];
} __attribute__((aligned(8))) ShmSchemaEntry;

typedef struct ShmColumnDescriptor {
    uint32_t type;
    uint32_t reserved_pad32;
    uint64_t value_offset;
    uint64_t value_count;
    uint64_t value_padding;
    uint64_t offsets_offset;
    uint64_t offsets_count;
    uint64_t offsets_padding;
} __attribute__((aligned(8))) ShmColumnDescriptor;

/* --- Hot-Cold Phase 1: TCP stream framing (byte-for-byte mirror of CH Wire/TcpFrame.h). --- */
#define PGCH_TCP_MAGIC          UINT64_C(0x01005043544D4853)  /* "SHMTCP\0\1" LE; == CH SHM_TCP_MAGIC */
#define PGCH_TCP_ABI_VERSION_1  1u

#pragma pack(push, 1)
typedef struct TcpHandshakeHeader {
    uint64_t magic;
    uint32_t abi_version;
    uint32_t schema_count;
} TcpHandshakeHeader;

typedef struct TcpBlockHeader {
    uint64_t payload_len;
    uint64_t row_count;
    uint64_t descriptors_offset;
    uint8_t  eos_marker;
    uint8_t  reserved[7];
} TcpBlockHeader;
#pragma pack(pop)

StaticAssertDecl(sizeof(ShmHandshake) == 128, "ShmHandshake must be 128 bytes");
StaticAssertDecl(sizeof(ShmSlot) == 64, "ShmSlot must be 64 bytes");
StaticAssertDecl(sizeof(ShmSchemaEntry) == 128, "ShmSchemaEntry must be 128 bytes");
StaticAssertDecl(sizeof(TcpHandshakeHeader) == 16, "TcpHandshakeHeader must be 16 bytes");
StaticAssertDecl(sizeof(TcpBlockHeader) == 32, "TcpBlockHeader must be 32 bytes");
StaticAssertDecl(sizeof(ShmColumnDescriptor) == 56, "ShmColumnDescriptor must be 56 bytes");

/* The Arrow serializer consumes the deformed buffers via ShmArrowColBuffers, which is
 * field-for-field identical to ShmColumnPayload, so a published payload array can be passed
 * to shm_arrow_encode_record_batch() by reinterpret-cast (no per-block copy). Pin that. */
StaticAssertDecl(sizeof(ShmArrowColBuffers) == sizeof(ShmColumnPayload),
                 "ShmArrowColBuffers must match ShmColumnPayload");
StaticAssertDecl(offsetof(ShmArrowColBuffers, value_buf)     == offsetof(ShmColumnPayload, value_buf) &&
                 offsetof(ShmArrowColBuffers, value_len)     == offsetof(ShmColumnPayload, value_len) &&
                 offsetof(ShmArrowColBuffers, offsets_buf)   == offsetof(ShmColumnPayload, offsets_buf) &&
                 offsetof(ShmArrowColBuffers, offsets_count) == offsetof(ShmColumnPayload, offsets_count),
                 "ShmArrowColBuffers layout must match ShmColumnPayload");

#define MAX_PARKED_CONNS 16

struct ShmProducer {
    char       *shm_name;       /* with leading '/' */
    char       *socket_path;    /* /tmp/clickhouse_shm_<sanitized>.sock */
    void       *mapping;
    size_t      mapping_size;
    int         shm_fd;
    int         event_fd;
    int         listen_fd;

    uint32_t    ring_depth_k;
    int         n_columns;
    ShmColumnSchema *schema;     /* copy of the caller's schema (carries wire tags) */
    size_t      per_slot_capacity;
    size_t      per_slot_payload_offset;

    uint64_t   *next_sequence;   /* per slot */
    uint32_t    next_slot;
    bool        eos_published;
    bool        cleaned;
    /*
     * Originating backend PID (0 = no check). When set, the ring-full wait polls
     * it so a producer cannot block forever if the backend has died (which
     * cancels the ClickHouse consumer, so nothing will ever drain this ring) --
     * the wait then errors and the worker tears itself down. Clean query cancel
     * (backend still alive) is handled by the backend reaping its workers.
     */
    int         origin_pid;

    int         parked_conns[MAX_PARKED_CONNS];
    int         n_parked;

    /*
     * Socket-pump thread. A single PostgreSQL backend cannot both block
     * publishing into the ring and block on the ClickHouse query result, so a
     * dedicated thread accepts the consumer's control-socket connection and
     * hands over the readiness eventfd via SCM_RIGHTS. The thread touches ONLY
     * raw fds (no PostgreSQL API / palloc / ereport), mirroring the ClickHouse
     * reference producer's accept loop.
     */
    pthread_t   pump_thread;
    bool        pump_running;
    volatile sig_atomic_t pump_stop;
    int         pump_stop_fd;   /* eventfd; cleanup writes it to wake the pump out of poll() at once */

    MemoryContext owner_cxt;
    MemoryContextCallback cleanup_cb;

    /* Optional per-worker phase stopwatch (benchmark instrumentation; NULL = off). */
    PgchPhaseTimers *timers;

    /*
     * Hot-Cold Phase 1 TCP transport (PGCH_PRODUCER_TRANSPORT_TCP). When set, there is no SHM
     * mapping / control socket / pump thread: `listen_fd` is a 127.0.0.1 TCP listener bound to
     * `tcp_port`, `tcp_conn_fd` is the accepted consumer connection (-1 until the first publish),
     * and blocks are serialized into `tcp_scratch` (frame-relative) and sent. Backpressure is the
     * blocking send() (TCP flow control) in place of the SHM ring-full wait.
     */
    ShmProducerTransport transport;
    uint16_t   tcp_port;
    int        tcp_conn_fd;
    bool       tcp_handshake_sent;
    char      *tcp_scratch;
    size_t     tcp_scratch_cap;

    /*
     * Hot-Cold Phase 3 Branch P2: pipelined send reactor. `send_k` frame buffers (>=1) so the producer
     * may serialize up to K blocks ahead of the SINGLE in-flight socket send (one frame SENDING at a time
     * preserves byte-stream order). Frames are a FIFO ring: `send_drain` is the oldest occupied (the
     * send head), `send_count` is the number occupied (READY/SENDING/ZC_PENDING). Non-blocking publish
     * serializes into the fill slot and pumps the head; backpressure (all K busy) waits on EPOLLOUT (or
     * reaps zerocopy). K=1 ⇒ single-in-flight (P1) behavior. Lazily sized at the first publish.
     */
    int        send_k;
    int        send_k_configured;   /* the GUC value before any RLIMIT_MEMLOCK cap (for logging) */
    int        tcp_sndbuf_bytes;    /* P2 experiment: per-socket SO_SNDBUF override (0 = default 32 MiB) */
    int        tcp_send_delay_us;   /* P2 microbench: injected per-frame in-flight latency, us (0 = off) */
    struct SendFrame *send_frames;  /* [send_k]; NULL until tcp_reactor_init */
    int        send_count;          /* occupied frames (READY/SENDING/ZC_PENDING), 0..send_k */
    int        send_drain;          /* index of the oldest occupied frame (the send head) */
    uint64_t   send_pump_calls;     /* observability: reactor pump invocations */
    uint64_t   send_overlap_frames; /* frames whose send was advanced while a later block was being prepared */

    /*
     * Hot-Cold Phase 2 Branch A (PGCH_PRODUCER_TRANSPORT_ARROW, D-HC-0207): the Arrow IPC
     * serializer. Lazily created on the first publish (when the connection is accepted and the
     * Arrow Schema message is sent); NULL for every other transport. Freed in producer_cleanup.
     */
    ShmArrowEncoder *arrow_enc;

    /*
     * Hot-Cold Phase 3, Branch P1 (D-HC-0302): TCP send submission method. `tcp_send_method` is a
     * PgchTcpSendMethod (blocking | epoll | msg_zerocopy). The epoll path does a non-blocking send() and,
     * on EAGAIN, waits for writability on a per-worker epoll fd (`tcp_send_epoll_fd`, conn fd registered
     * EPOLLOUT, lazily created in tcp_accept_conn when this method is selected) with a ~100ms slice that
     * re-checks interrupts + backend death -- one send in flight (the producer is single-threaded per
     * stream). io_uring was removed in P1 (it was used synchronously, hence a loopback wash).
     */
    int        tcp_send_method;
    int        tcp_send_epoll_fd;   /* P1 epoll path: conn fd registered EPOLLOUT; -1 until accept (epoll method) */
    uint64_t   tcp_epoll_sends;     /* logical tcp_send_all calls taken via the epoll non-blocking path */
    uint64_t   tcp_blocking_sends;  /* logical tcp_send_all calls taken via blocking send() */
    uint64_t   tcp_send_bytes;      /* total bytes handed to tcp_send_all (header+schema+payloads) */
    /* Branch B (B-it4): MSG_ZEROCOPY send-side measured-null. With SO_ZEROCOPY set on tcp_conn_fd,
     * each send(MSG_ZEROCOPY) is acknowledged by an SO_EE_ORIGIN_ZEROCOPY errqueue completion whose
     * SO_EE_CODE_ZEROCOPY_COPIED flag, set on this loopback/NIC-less host, proves the DEFERRED COPY
     * (a pessimization, not an elimination). tcp_zc_seq_* mirror the kernel's per-socket zerocopy
     * sequence counter so the send buffer is reused only after the kernel releases it. */
    uint64_t   tcp_zc_sends;            /* send(MSG_ZEROCOPY) calls issued */
    uint64_t   tcp_zc_notifs;           /* SO_EE_ORIGIN_ZEROCOPY completions drained */
    uint64_t   tcp_zc_copied;           /* completions with SO_EE_CODE_ZEROCOPY_COPIED (deferred copy) */
    uint32_t   tcp_zc_seq_next;         /* next send's zerocopy seq (mirrors the kernel counter) */
    uint32_t   tcp_zc_seq_acked;        /* highest completed seq drained off the errqueue */
    bool       tcp_zc_seq_acked_valid;  /* tcp_zc_seq_acked holds a real value */
    bool       tcp_zc_sockopt_set;      /* SO_ZEROCOPY applied to tcp_conn_fd (else fall back to copy) */
};

/* --------------------------------------------------------------------- */
/* Small helpers */
/* --------------------------------------------------------------------- */

/* shm_wire_fixed_width_size() is now a static inline in shm_wire.h (PG-free,
 * shared with the Arrow serializer shm_arrow.c). */

static inline size_t
align_up(size_t v, size_t a)
{
    return (v + a - 1) & ~(a - 1);
}

/* True if a configured originating backend has gone away (so the ClickHouse
 * consumer was cancelled and this ring will never drain). kill(pid, 0) probes
 * existence; ESRCH means the process is gone. */
static inline bool
origin_backend_dead(ShmProducer *p)
{
    return p->origin_pid > 0 && kill((pid_t) p->origin_pid, 0) < 0 && errno == ESRCH;
}

static ShmHandshake *
hs_of(ShmProducer *p)
{
    return (ShmHandshake *) p->mapping;
}

static ShmSlot *
slot_at(ShmProducer *p, uint32_t i)
{
    char *base = (char *) p->mapping + hs_of(p)->slot_table_offset;
    return (ShmSlot *) (base + (size_t) i * hs_of(p)->slot_table_stride);
}

static char *
data_region(ShmProducer *p)
{
    return (char *) p->mapping + hs_of(p)->data_region_offset;
}

/* /tmp/clickhouse_shm_<sanitized>.sock — must match ClickHouse
 * controlSocketPathForShmName(): strip leading '/', embedded '/' -> '_'. */
static char *
control_socket_path(const char *shm_name)
{
    StringInfoData buf;
    const char *s = shm_name;

    while (*s == '/')
        s++;
    initStringInfo(&buf);
    appendStringInfoString(&buf, "/tmp/clickhouse_shm_");
    for (; *s; s++)
        appendStringInfoChar(&buf, *s == '/' ? '_' : *s);
    appendStringInfoString(&buf, ".sock");
    return buf.data;
}

/* --------------------------------------------------------------------- */
/* Control socket: non-blocking accept + SCM_RIGHTS send of the eventfd. */
/* --------------------------------------------------------------------- */

static void
prune_parked_conns(ShmProducer *p)
{
    int i = 0;
    while (i < p->n_parked)
    {
        struct pollfd pfd;
        pfd.fd = p->parked_conns[i];
        pfd.events = 0;
        pfd.revents = 0;
        if (poll(&pfd, 1, 0) > 0 && (pfd.revents & (POLLHUP | POLLERR | POLLNVAL)))
        {
            close(p->parked_conns[i]);
            p->parked_conns[i] = p->parked_conns[--p->n_parked];
        }
        else
            i++;
    }
}

static void
send_eventfd(int conn_fd, int eventfd_to_pass)
{
    struct msghdr msg;
    struct iovec iov;
    char dummy = 0;
    union {
        char buf[CMSG_SPACE(sizeof(int))];
        struct cmsghdr align;
    } cmsg_buf;
    struct cmsghdr *cmsg;

    memset(&msg, 0, sizeof(msg));
    memset(&cmsg_buf, 0, sizeof(cmsg_buf));
    iov.iov_base = &dummy;
    iov.iov_len = 1;
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsg_buf.buf;
    msg.msg_controllen = sizeof(cmsg_buf.buf);

    cmsg = CMSG_FIRSTHDR(&msg);
    cmsg->cmsg_level = SOL_SOCKET;
    cmsg->cmsg_type = SCM_RIGHTS;
    cmsg->cmsg_len = CMSG_LEN(sizeof(int));
    memcpy(CMSG_DATA(cmsg), &eventfd_to_pass, sizeof(int));

    /* Best effort: a consumer that vanished mid-handshake must not error the
     * producer; the connection is simply dropped by the caller. */
    (void) sendmsg(conn_fd, &msg, MSG_NOSIGNAL);
}

/* Accept any pending consumer connections and hand each the readiness eventfd.
 * Runs only on the pump thread, which solely owns parked_conns. */
static void
pump_control_socket(ShmProducer *p)
{
    prune_parked_conns(p);
    for (;;)
    {
        int conn = accept4(p->listen_fd, NULL, NULL, SOCK_CLOEXEC | SOCK_NONBLOCK);
        if (conn < 0)
            break;              /* EAGAIN/EWOULDBLOCK: no pending connection */

        send_eventfd(conn, p->event_fd);

        /* Park the fd while the consumer is alive; closing it early would surface
         * POLLHUP on the consumer and trip SHM_PRODUCER_DEATH_BEFORE_EOS. */
        if (p->n_parked < MAX_PARKED_CONNS)
            p->parked_conns[p->n_parked++] = conn;
        else
            close(conn);
    }
}

/* Pump thread: accept consumer connections and serve the readiness eventfd until
 * asked to stop. Pure syscalls only — never touches PostgreSQL state. */
static void *
pump_thread_main(void *arg)
{
    ShmProducer *p = (ShmProducer *) arg;
    int i;

    while (!p->pump_stop)
    {
        struct pollfd pfd[2];
        nfds_t        nfds = 0;
        int           listen_idx;
        int           stop_idx = -1;

        pfd[nfds].fd = p->listen_fd;
        pfd[nfds].events = POLLIN;
        pfd[nfds].revents = 0;
        listen_idx = (int) nfds++;
        if (p->pump_stop_fd >= 0)
        {
            pfd[nfds].fd = p->pump_stop_fd;
            pfd[nfds].events = POLLIN;
            pfd[nfds].revents = 0;
            stop_idx = (int) nfds++;
        }

        /* The 50ms timeout still bounds the parked-connection prune cadence; the
         * stop eventfd makes teardown wake the thread immediately regardless. */
        if (poll(pfd, nfds, 50) > 0)
        {
            if (stop_idx >= 0 && (pfd[stop_idx].revents & POLLIN))
                break;                              /* teardown asked us to stop */
            if (pfd[listen_idx].revents & POLLIN)
                pump_control_socket(p);
            else
                prune_parked_conns(p);
        }
        else
            prune_parked_conns(p);
    }

    for (i = 0; i < p->n_parked; i++)
        close(p->parked_conns[i]);
    p->n_parked = 0;
    return NULL;
}

/* --------------------------------------------------------------------- */
/* Lifecycle */
/* --------------------------------------------------------------------- */

static void
producer_cleanup(ShmProducer *p)
{
    int i;

    if (p->cleaned)
        return;
    p->cleaned = true;

    /* Stop the pump thread before touching its fds; it closes parked conns. */
    if (p->pump_running)
    {
        p->pump_stop = 1;
        /* Wake the pump out of poll() at once; otherwise it sleeps up to the poll
         * timeout before noticing pump_stop, and that latency lands on the query's
         * critical path via the backend's WaitForBackgroundWorkerShutdown. */
        if (p->pump_stop_fd >= 0)
        {
            uint64_t one = 1;
            ssize_t  wr;

            do { wr = write(p->pump_stop_fd, &one, sizeof(one)); } while (wr < 0 && errno == EINTR);
        }
        pthread_join(p->pump_thread, NULL);
        p->pump_running = false;
    }

    for (i = 0; i < p->n_parked; i++)
        close(p->parked_conns[i]);
    p->n_parked = 0;

    /* Phase 3 P1 (H8): close the per-worker send-readiness epoll fd before the socket. It only holds an
     * EPOLLOUT registration on tcp_conn_fd (no buffer references), so order vs the socket close is not
     * load-bearing, but closing it here keeps the per-stream fd count honest (no leaked epoll fd). */
    if (p->tcp_send_epoll_fd >= 0) { close(p->tcp_send_epoll_fd); p->tcp_send_epoll_fd = -1; }
#ifdef PGCH_USE_NANOARROW
    /* Free the Arrow encoder's malloc'd nanoarrow buffers (not palloc'd, so not reclaimed by the
     * owner-context reset that drives this callback). */
    if (p->arrow_enc != NULL) { shm_arrow_encoder_destroy(p->arrow_enc); p->arrow_enc = NULL; }
#endif
    if (p->tcp_conn_fd >= 0) { close(p->tcp_conn_fd); p->tcp_conn_fd = -1; }
    if (p->listen_fd >= 0) { close(p->listen_fd); p->listen_fd = -1; }
    if (p->event_fd >= 0)  { close(p->event_fd);  p->event_fd = -1; }
    if (p->pump_stop_fd >= 0) { close(p->pump_stop_fd); p->pump_stop_fd = -1; }
    if (p->mapping && p->mapping != MAP_FAILED)
    {
        munmap(p->mapping, p->mapping_size);
        p->mapping = NULL;
    }
    if (p->shm_fd >= 0) { close(p->shm_fd); p->shm_fd = -1; }
    if (p->shm_name)
        shm_unlink(p->shm_name);
    if (p->socket_path)
        unlink(p->socket_path);
}

static void
producer_cleanup_callback(void *arg)
{
    producer_cleanup((ShmProducer *) arg);
}

/* --------------------------------------------------------------------- */
/* Hot-Cold Phase 1: TCP transport (D-HC-0101/0103). One listener per stream; the consumer
 * connects, receives the handshake, then a sequence of frame-relative BLOCK frames + EOS. */
/* --------------------------------------------------------------------- */

/* Blocking send-all, cancellation/backend-death responsive (SO_SNDTIMEO slices -> EAGAIN). */
static void
tcp_send_all_blocking(ShmProducer *p, const void *buf, size_t n)
{
    const char *ptr = (const char *) buf;
    while (n > 0)
    {
        ssize_t w;

        CHECK_FOR_INTERRUPTS();
        w = send(p->tcp_conn_fd, ptr, n, MSG_NOSIGNAL);
        if (w > 0) { ptr += w; n -= (size_t) w; continue; }
        if (w < 0 && errno == EINTR)
            continue;
        if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        {
            struct pollfd pfd;

            if (origin_backend_dead(p))
                ereport(ERROR, (errmsg("pg_clickhouse: originating backend (pid %d) exited; "
                                       "abandoning TCP stream", p->origin_pid)));
            pfd.fd = p->tcp_conn_fd; pfd.events = POLLOUT; pfd.revents = 0;
            (void) poll(&pfd, 1, 100);
            continue;
        }
        if (origin_backend_dead(p))
            ereport(ERROR, (errmsg("pg_clickhouse: originating backend (pid %d) exited; "
                                   "abandoning TCP stream", p->origin_pid)));
        ereport(ERROR, (errcode_for_file_access(),
                        errmsg("pg_clickhouse: TCP send to consumer failed: %m")));
    }
}

/*
 * Hot-Cold Phase 3, Branch P1: wait (~timeout_ms) for the connected socket to become writable.
 * The readiness primitive is behind this tiny helper so a BSD/macOS kqueue backend is a drop-in: this
 * host is Linux (the whole TU uses accept4/MSG_ZEROCOPY/linux errqueue), so only the epoll backend is
 * implemented; on any other platform the helper is a bounded no-op and the caller's blocking-send
 * fallback (tcp_send_all_blocking) carries the wait. Raw epoll (not PG's WaitEventSet) is used so it stays
 * consistent with the raw poll() in the blocking/zerocopy paths and can later fold in the MSG_ZEROCOPY
 * errqueue POLLERR. The bounded slice lets the send loop re-poll CHECK_FOR_INTERRUPTS + backend death.
 */
static void
tcp_wait_writable(ShmProducer *p, int timeout_ms)
{
#if defined(__linux__)
    struct epoll_event ev;

    if (p->tcp_send_epoll_fd < 0)
    {
        /* No epoll fd (defensive): degrade to a bounded poll(POLLOUT) so we never busy-spin. */
        struct pollfd pfd;
        pfd.fd = p->tcp_conn_fd; pfd.events = POLLOUT; pfd.revents = 0;
        (void) poll(&pfd, 1, timeout_ms);
        return;
    }
    (void) epoll_wait(p->tcp_send_epoll_fd, &ev, 1, timeout_ms);
#else
    struct pollfd pfd;
    pfd.fd = p->tcp_conn_fd; pfd.events = POLLOUT; pfd.revents = 0;
    (void) poll(&pfd, 1, timeout_ms);
#endif
}

/*
 * Send-all over a NON-BLOCKING send() with epoll(EPOLLOUT) backpressure (Branch P1 default). One send in
 * flight: send() returning means the kernel has copied the bytes (buffer immediately reusable). On
 * EAGAIN/EWOULDBLOCK (the socket send buffer is full -- TCP flow control) wait for writability on the
 * per-worker epoll fd with a ~100ms slice that re-checks CHECK_FOR_INTERRUPTS + backend death (replacing
 * the io_uring wait_cqe_timeout slice). The kernel copy count is identical to the old io_uring
 * IORING_OP_SEND path -- P1 changes only the submission/readiness mechanism. Partial sends advance.
 */
static void
tcp_send_all_epoll(ShmProducer *p, const void *buf, size_t n)
{
    const char *ptr = (const char *) buf;

    while (n > 0)
    {
        ssize_t w;

        CHECK_FOR_INTERRUPTS();
        w = send(p->tcp_conn_fd, ptr, n, MSG_NOSIGNAL);
        if (w > 0) { ptr += w; n -= (size_t) w; continue; }
        if (w < 0 && errno == EINTR)
            continue;
        if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        {
            if (origin_backend_dead(p))
                ereport(ERROR, (errmsg("pg_clickhouse: originating backend (pid %d) exited; "
                                       "abandoning TCP stream", p->origin_pid)));
            tcp_wait_writable(p, 100);   /* epoll_wait(EPOLLOUT, 100ms) */
            continue;
        }
        if (origin_backend_dead(p))
            ereport(ERROR, (errmsg("pg_clickhouse: originating backend (pid %d) exited; "
                                   "abandoning TCP stream", p->origin_pid)));
        ereport(ERROR, (errcode_for_file_access(),
                        errmsg("pg_clickhouse: epoll TCP send to consumer failed: %m")));
    }
}

#ifndef SOL_IP
#define SOL_IP IPPROTO_IP
#endif

/*
 * Branch B (B-it4): reap all currently-ready SO_EE_ORIGIN_ZEROCOPY completions off MSG_ERRQUEUE
 * (non-blocking -- returns when the queue is momentarily empty), releasing the kernel's pin on the
 * sent pages so subsequent sends do not hit ENOBUFS (RLIMIT_MEMLOCK). Counts completions and -- the
 * measured-null proof -- the ones carrying SO_EE_CODE_ZEROCOPY_COPIED (set on this loopback host: the
 * kernel deferred a copy). Advances tcp_zc_seq_acked to the highest completed sequence seen.
 */
static void
tcp_zc_reap(ShmProducer *p)
{
    for (;;)
    {
        struct msghdr msg;
        struct cmsghdr *cm;
        char ctrl[256];
        ssize_t r;

        memset(&msg, 0, sizeof(msg));
        msg.msg_control = ctrl;
        msg.msg_controllen = sizeof(ctrl);
        r = recvmsg(p->tcp_conn_fd, &msg, MSG_ERRQUEUE);
        if (r < 0)
        {
            if (errno == EINTR)
                continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return;   /* errqueue momentarily empty */
            ereport(ERROR, (errcode_for_file_access(),
                            errmsg("pg_clickhouse: MSG_ERRQUEUE recvmsg failed: %m")));
        }
        for (cm = CMSG_FIRSTHDR(&msg); cm != NULL; cm = CMSG_NXTHDR(&msg, cm))
        {
            struct sock_extended_err *serr;

            if (!((cm->cmsg_level == SOL_IP && cm->cmsg_type == IP_RECVERR) ||
                  (cm->cmsg_level == SOL_IPV6 && cm->cmsg_type == IPV6_RECVERR)))
                continue;
            serr = (struct sock_extended_err *) CMSG_DATA(cm);
            if (serr->ee_origin != SO_EE_ORIGIN_ZEROCOPY)
                continue;
            p->tcp_zc_notifs++;
            if (serr->ee_code & SO_EE_CODE_ZEROCOPY_COPIED)
                p->tcp_zc_copied++;
            /* ee_data is the highest sequence number in this (possibly coalesced) completion range. */
            if (!p->tcp_zc_seq_acked_valid || (int32_t) (serr->ee_data - p->tcp_zc_seq_acked) > 0)
            {
                p->tcp_zc_seq_acked = serr->ee_data;
                p->tcp_zc_seq_acked_valid = true;
            }
        }
    }
}

/*
 * Block (reaping + polling the errqueue) until the kernel has released every send up to `target_seq`
 * (the last send's zerocopy sequence), so the just-sent buffer is safe for the caller to reuse.
 */
static void
tcp_zc_drain_until(ShmProducer *p, uint32_t target_seq)
{
    for (;;)
    {
        struct pollfd pfd;

        tcp_zc_reap(p);
        if (p->tcp_zc_seq_acked_valid && (int32_t) (p->tcp_zc_seq_acked - target_seq) >= 0)
            return;
        CHECK_FOR_INTERRUPTS();
        if (origin_backend_dead(p))
            ereport(ERROR, (errmsg("pg_clickhouse: originating backend (pid %d) exited; "
                                   "abandoning TCP stream", p->origin_pid)));
        pfd.fd = p->tcp_conn_fd; pfd.events = POLLERR; pfd.revents = 0;
        (void) poll(&pfd, 1, 100);
    }
}

/* Bound the pages pinned by a single send(MSG_ZEROCOPY) well under RLIMIT_MEMLOCK (8 MiB here), so the
 * accounting limit is not exceeded; combined with reap-after-each-send, outstanding stays ~1 chunk. */
#define PGCH_ZC_SEND_CHUNK (1024 * 1024)

/*
 * Branch B (B-it4): send `n` bytes via send(MSG_ZEROCOPY) in <=PGCH_ZC_SEND_CHUNK chunks, reaping the
 * zerocopy completions as it goes, then block until the kernel releases the buffer so the caller may
 * reuse it next block. ENOBUFS (too many outstanding pins) is handled by draining + retrying, NOT a
 * hard error. If SO_ZEROCOPY was not enabled on the fd, fall back to the plain blocking copying send.
 */
static void
tcp_send_all_msg_zerocopy(ShmProducer *p, const void *buf, size_t n)
{
    const char *ptr = (const char *) buf;
    uint32_t last_seq = 0;
    bool sent_zc = false;

    if (!p->tcp_zc_sockopt_set)
    {
        tcp_send_all_blocking(p, buf, n);
        return;
    }

    while (n > 0)
    {
        size_t chunk = n < (size_t) PGCH_ZC_SEND_CHUNK ? n : (size_t) PGCH_ZC_SEND_CHUNK;
        ssize_t w;

        CHECK_FOR_INTERRUPTS();
        w = send(p->tcp_conn_fd, ptr, chunk, MSG_NOSIGNAL | MSG_ZEROCOPY);
        if (w > 0)
        {
            ptr += w; n -= (size_t) w;
            last_seq = p->tcp_zc_seq_next++;   /* mirrors the kernel's per-socket zerocopy counter */
            p->tcp_zc_sends++;
            sent_zc = true;
            tcp_zc_reap(p);   /* release pinned pages so the next send does not hit ENOBUFS */
            continue;
        }
        if (w < 0 && errno == EINTR)
            continue;
        if (w < 0 && errno == ENOBUFS)
        {
            /* Outstanding zerocopy pins hit RLIMIT_MEMLOCK -- block until they are released, then retry
             * (the chunk cap guarantees a single send fits once nothing else is outstanding). */
            if (sent_zc)
                tcp_zc_drain_until(p, last_seq);
            else
            {
                /* Nothing outstanding yet but still ENOBUFS (only reachable if RLIMIT_MEMLOCK is set below
                 * one chunk). Back off on the errqueue/socket instead of busy-spinning. (Review B #1.) */
                struct pollfd pfd;

                if (origin_backend_dead(p))
                    ereport(ERROR, (errmsg("pg_clickhouse: originating backend (pid %d) exited; "
                                           "abandoning TCP stream", p->origin_pid)));
                pfd.fd = p->tcp_conn_fd; pfd.events = POLLERR | POLLOUT; pfd.revents = 0;
                (void) poll(&pfd, 1, 100);
            }
            continue;
        }
        if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        {
            struct pollfd pfd;

            if (origin_backend_dead(p))
                ereport(ERROR, (errmsg("pg_clickhouse: originating backend (pid %d) exited; "
                                       "abandoning TCP stream", p->origin_pid)));
            pfd.fd = p->tcp_conn_fd; pfd.events = POLLOUT; pfd.revents = 0;
            (void) poll(&pfd, 1, 100);
            continue;
        }
        if (origin_backend_dead(p))
            ereport(ERROR, (errmsg("pg_clickhouse: originating backend (pid %d) exited; "
                                   "abandoning TCP stream", p->origin_pid)));
        ereport(ERROR, (errcode_for_file_access(),
                        errmsg("pg_clickhouse: MSG_ZEROCOPY send to consumer failed: %m")));
    }

    /* Wait for the kernel to release this buffer before the caller reuses it. On loopback the deferred
     * copy completes promptly with SO_EE_CODE_ZEROCOPY_COPIED set -- the honest measured null. */
    if (sent_zc)
        tcp_zc_drain_until(p, last_seq);
}

/*
 * Send-all dispatcher: msg_zerocopy (Branch B B-it4) / epoll non-blocking send (Branch P1 default) when
 * selected, else blocking. The selection is per-stream (snapshotted into the worker header). The epoll
 * path uses tcp_send_epoll_fd (created in tcp_accept_conn when this method is selected); if that creation
 * failed it stays -1 and tcp_wait_writable degrades to a bounded poll(POLLOUT) -- never a busy-spin.
 */
static void
tcp_send_all(ShmProducer *p, const void *buf, size_t n)
{
    p->tcp_send_bytes += n;
    if (p->tcp_send_method == PGCH_TCP_SEND_MSG_ZEROCOPY)
    {
        tcp_send_all_msg_zerocopy(p, buf, n);
        return;
    }
    if (p->tcp_send_method == PGCH_TCP_SEND_EPOLL)
    {
        p->tcp_epoll_sends++;
        tcp_send_all_epoll(p, buf, n);
        return;
    }
    p->tcp_blocking_sends++;
    tcp_send_all_blocking(p, buf, n);
}

/* Accept the consumer connection (poll loop honouring cancel + backend death) and set
 * TCP_NODELAY + a send timeout + a multi-block send buffer. Shared by the bespoke TCP and
 * Arrow transports (the wire-specific handshake/schema is sent by the caller). */
static void
tcp_accept_conn(ShmProducer *p)
{
    struct timeval tv;
    int one = 1;

    for (;;)
    {
        struct pollfd pfd;
        int conn;

        pfd.fd = p->listen_fd; pfd.events = POLLIN; pfd.revents = 0;
        (void) poll(&pfd, 1, 100);
        CHECK_FOR_INTERRUPTS();
        if (origin_backend_dead(p))
            ereport(ERROR, (errmsg("pg_clickhouse: originating backend (pid %d) exited before the "
                                   "TCP consumer connected", p->origin_pid)));
        conn = accept4(p->listen_fd, NULL, NULL, SOCK_CLOEXEC);
        if (conn < 0)
        {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)
                continue;
            ereport(ERROR, (errcode_for_file_access(), errmsg("pg_clickhouse: TCP accept failed: %m")));
        }
        p->tcp_conn_fd = conn;
        break;
    }
    (void) setsockopt(p->tcp_conn_fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    tv.tv_sec = 0; tv.tv_usec = 100000;   /* 100ms send slices -> EAGAIN so send observes cancel */
    (void) setsockopt(p->tcp_conn_fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    /* Size the send buffer to several blocks so this producer can run ahead while the (often
     * single-threaded) consumer processes a block -- the TCP analog of the SHM ring's K in-flight
     * slots; without it producer-send and consumer-process serialize. Capped by net.core.wmem_max
     * (raised to match the SHM 64 MiB data region; see 10-REPRODUCTION). */
    {
        int sndbuf = (p->tcp_sndbuf_bytes > 0) ? p->tcp_sndbuf_bytes : 32 * 1024 * 1024;
        socklen_t slen = sizeof(sndbuf);
        int eff = 0;
        (void) setsockopt(p->tcp_conn_fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
        if (p->tcp_sndbuf_bytes > 0 &&
            getsockopt(p->tcp_conn_fd, SOL_SOCKET, SO_SNDBUF, &eff, &slen) == 0)
            ereport(LOG, (errmsg("pg_clickhouse: P2 producer SO_SNDBUF requested=%d effective=%d "
                                 "(experiment knob; default is 32 MiB)", sndbuf, eff)));
    }
    /* Branch B (B-it4): enable SO_ZEROCOPY so send(MSG_ZEROCOPY) attempts a zero-copy send and posts
     * an SO_EE_ORIGIN_ZEROCOPY completion to the errqueue. Only when that send method is selected (it
     * changes completion semantics -- every such send MUST be drained). If unavailable, the send path
     * falls back to a plain copying send (tcp_zc_sockopt_set stays false). */
    if (p->tcp_send_method == PGCH_TCP_SEND_MSG_ZEROCOPY)
    {
        int z = 1;
        if (setsockopt(p->tcp_conn_fd, SOL_SOCKET, SO_ZEROCOPY, &z, sizeof(z)) == 0)
            p->tcp_zc_sockopt_set = true;
        else
            ereport(LOG, (errmsg("pg_clickhouse: SO_ZEROCOPY unavailable (%m); MSG_ZEROCOPY send "
                                 "falls back to a plain copying send")));
    }
    /* Branch P1/P2: for the epoll AND msg_zerocopy paths, switch the connection to O_NONBLOCK and build a
     * per-worker epoll fd watching the conn fd for EPOLLOUT (the pipelined reactor waits on it under
     * backpressure — non-blocking sends are required for K-deep run-ahead, incl. the zerocopy path whose
     * frames are reaped asynchronously). Only the 'blocking' method keeps the blocking socket + SO_SNDTIMEO.
     * On any failure we leave tcp_send_epoll_fd = -1; tcp_wait_writable then degrades to a bounded
     * poll(POLLOUT), so a missing epoll fd never busy-spins. */
    if (p->tcp_send_method == PGCH_TCP_SEND_EPOLL || p->tcp_send_method == PGCH_TCP_SEND_MSG_ZEROCOPY)
    {
        int flags = fcntl(p->tcp_conn_fd, F_GETFL, 0);
        if (flags < 0 || fcntl(p->tcp_conn_fd, F_SETFL, flags | O_NONBLOCK) < 0)
            ereport(ERROR, (errcode_for_file_access(),
                            errmsg("pg_clickhouse: fcntl(O_NONBLOCK) on TCP conn failed: %m")));
        p->tcp_send_epoll_fd = epoll_create1(EPOLL_CLOEXEC);
        if (p->tcp_send_epoll_fd < 0)
            ereport(LOG, (errmsg("pg_clickhouse: epoll_create1 for TCP send failed (%m); "
                                 "send backpressure falls back to poll(POLLOUT)")));
        else
        {
            struct epoll_event ev;

            memset(&ev, 0, sizeof(ev));
            /* Watch for writability. EPOLLERR/EPOLLHUP are ALWAYS reported by epoll_wait regardless of the
             * requested set (epoll(7)), so we need not request them; on a dying peer epoll_wait then
             * returns immediately and the next send() surfaces the real error (EPIPE/ECONNRESET) within
             * sub-ms -- a bounded, CHECK_FOR_INTERRUPTS-checked transient in tcp_send_all_epoll, not a hang
             * (adversarial review P1 NB2). */
            ev.events = EPOLLOUT;
            ev.data.fd = p->tcp_conn_fd;
            if (epoll_ctl(p->tcp_send_epoll_fd, EPOLL_CTL_ADD, p->tcp_conn_fd, &ev) < 0)
            {
                ereport(LOG, (errmsg("pg_clickhouse: epoll_ctl(ADD conn) for TCP send failed (%m); "
                                     "send backpressure falls back to poll(POLLOUT)")));
                close(p->tcp_send_epoll_fd);
                p->tcp_send_epoll_fd = -1;
            }
        }
    }
}

/* Accept the consumer and send the bespoke one-shot handshake (header + ShmSchemaEntry[]). */
static void
tcp_accept_and_handshake(ShmProducer *p)
{
    TcpHandshakeHeader hs;
    ShmSchemaEntry *se;
    int i;

    tcp_accept_conn(p);

    hs.magic = PGCH_TCP_MAGIC;
    hs.abi_version = PGCH_TCP_ABI_VERSION_1;
    hs.schema_count = (uint32_t) p->n_columns;
    tcp_send_all(p, &hs, sizeof(hs));

    se = (ShmSchemaEntry *) palloc0(sizeof(ShmSchemaEntry) * p->n_columns);
    for (i = 0; i < p->n_columns; i++)
    {
        memcpy(se[i].name, p->schema[i].name, sizeof(se[i].name));
        memcpy(se[i].type_string, p->schema[i].type_string, sizeof(se[i].type_string));
    }
    tcp_send_all(p, se, sizeof(ShmSchemaEntry) * p->n_columns);
    pfree(se);
    p->tcp_handshake_sent = true;
}

#ifdef PGCH_USE_NANOARROW
/* Arrow transport (D-HC-0207): accept the consumer, build the Arrow encoder from the column
 * schema, and send the Arrow IPC Schema message. The wire is a standard Arrow IPC stream
 * (Schema message, then one RecordBatch message per block, then the EOS marker) -- NO bespoke
 * TcpHandshakeHeader/TcpBlockHeader. tcp_handshake_sent is reused as the "stream started" flag. */
static void
arrow_accept_and_send_schema(ShmProducer *p)
{
    ShmArrowField *fields;
    const uint8_t *msg;
    size_t msg_len;
    char errbuf[256];
    int i;

    tcp_accept_conn(p);

    fields = (ShmArrowField *) palloc(sizeof(ShmArrowField) * (p->n_columns > 0 ? p->n_columns : 1));
    for (i = 0; i < p->n_columns; i++)
    {
        fields[i].wire = p->schema[i].wire;
        fields[i].name = p->schema[i].name;   /* outlives the encoder (producer-owned schema) */
    }
    p->arrow_enc = shm_arrow_encoder_create(fields, p->n_columns, errbuf, sizeof(errbuf));
    pfree(fields);
    if (p->arrow_enc == NULL)
        ereport(ERROR, (errmsg("pg_clickhouse: Arrow encoder init failed: %s", errbuf)));

    if (!shm_arrow_encode_schema(p->arrow_enc, &msg, &msg_len, errbuf, sizeof(errbuf)))
        ereport(ERROR, (errmsg("pg_clickhouse: Arrow schema encode failed: %s", errbuf)));
    tcp_send_all(p, msg, msg_len);
    p->tcp_handshake_sent = true;
}

/* Arrow analog of tcp_publish_block: lazy accept + schema on the first call, then one Arrow IPC
 * RecordBatch message per block (encapsulated metadata followed by the body), and the Arrow IPC
 * EOS marker for end-of-stream. */
static void
arrow_publish_block(ShmProducer *p, const ShmColumnPayload *payloads, int n_payloads,
                    size_t row_count, bool is_eos)
{
    int saved_phase = -1;
    const uint8_t *meta, *body;
    size_t meta_len, body_len;
    char errbuf[256];

    if (p->eos_published)
        ereport(ERROR, (errmsg("pg_clickhouse: shm stream already ended")));
    if (!is_eos && n_payloads != p->n_columns)
        ereport(ERROR, (errmsg("pg_clickhouse: shm payload count %d != schema %d", n_payloads, p->n_columns)));
    if (row_count > SHM_IMPL_MAX_ROWS)
        ereport(ERROR, (errmsg("pg_clickhouse: shm row_count %zu exceeds limit %u", row_count, SHM_IMPL_MAX_ROWS)));

    if (!p->tcp_handshake_sent)
        arrow_accept_and_send_schema(p);

    if (p->timers != NULL && p->timers->enabled)
        saved_phase = p->timers->cur;
    pgch_phase_switch(p->timers, PGCH_PH_PUBLISH);

    if (is_eos)
    {
        tcp_send_all(p, SHM_ARROW_EOS_MARKER, sizeof(SHM_ARROW_EOS_MARKER));
    }
    else
    {
        /* ShmColumnPayload is field-compatible with ShmArrowColBuffers (static-asserted above). */
        if (!shm_arrow_encode_record_batch(p->arrow_enc,
                                           (const ShmArrowColBuffers *) payloads, n_payloads, row_count,
                                           &meta, &meta_len, &body, &body_len, errbuf, sizeof(errbuf)))
        {
            if (saved_phase >= 0)
                pgch_phase_switch(p->timers, saved_phase);
            ereport(ERROR, (errmsg("pg_clickhouse: Arrow record-batch encode failed: %s", errbuf)));
        }
        tcp_send_all(p, meta, meta_len);
        if (body_len > 0)
            tcp_send_all(p, body, body_len);
    }

    if (saved_phase >= 0)
        pgch_phase_switch(p->timers, saved_phase);
}
#endif /* PGCH_USE_NANOARROW */

/* Lay one block into `buf` (cap bytes) frame-relative (descriptors at offset 0, then per-column
 * buffers with the SHM align / SIMD-padding / offsets[-1]-zero-sentinel layout). Returns the
 * used payload length. MUST match the consumer's adopt() expectations and block_footprint().
 * P2: `buf` is the acquired frame's payload buffer (one of K), not the single tcp_scratch. */
static size_t
tcp_serialize_block(ShmProducer *p, const ShmColumnPayload *payloads, size_t row_count,
                    char *buf, size_t cap)
{
    ShmColumnDescriptor *descs = (ShmColumnDescriptor *) buf;
    size_t cursor = align_up((size_t) p->n_columns * sizeof(ShmColumnDescriptor), 8);
    int i;

    memset(descs, 0, (size_t) p->n_columns * sizeof(ShmColumnDescriptor));
    for (i = 0; i < p->n_columns; i++)
    {
        const ShmColumnPayload *pay = &payloads[i];
        ShmColumnDescriptor *d = &descs[i];
        ShmWireType wire = p->schema[i].wire;

        d->type = (uint32_t) wire;
        if (wire == SHM_WIRE_STRING)
        {
            size_t chars_bytes = pay->value_len;
            size_t offs_bytes = pay->offsets_count * sizeof(uint64_t);
            size_t chars_off = align_up(cursor, 8);
            size_t sentinel_off, offs_off;

            cursor = chars_off + chars_bytes + SHM_PADDING_FOR_SIMD;
            if (cursor > cap) goto overflow;
            if (chars_bytes) memcpy(buf + chars_off, pay->value_buf, chars_bytes);

            sentinel_off = align_up(cursor, 8);
            cursor = sentinel_off + sizeof(uint64_t);
            if (cursor > cap) goto overflow;
            memset(buf + sentinel_off, 0, sizeof(uint64_t));   /* offsets[-1] zero sentinel */

            offs_off = align_up(cursor, 8);
            cursor = offs_off + offs_bytes + SHM_PADDING_FOR_SIMD;
            if (cursor > cap) goto overflow;
            if (offs_bytes) memcpy(buf + offs_off, pay->offsets_buf, offs_bytes);

            d->value_offset = chars_off; d->value_count = chars_bytes; d->value_padding = SHM_PADDING_FOR_SIMD;
            d->offsets_offset = offs_off; d->offsets_count = row_count; d->offsets_padding = SHM_PADDING_FOR_SIMD;
        }
        else
        {
            size_t elem = shm_wire_fixed_width_size(wire);
            size_t vbytes = row_count * elem;
            size_t voff = align_up(cursor, elem > 8 ? elem : 8);

            cursor = voff + vbytes + SHM_PADDING_FOR_SIMD;
            if (cursor > cap) goto overflow;
            if (vbytes) memcpy(buf + voff, pay->value_buf, vbytes);

            d->value_offset = voff; d->value_count = row_count; d->value_padding = SHM_PADDING_FOR_SIMD;
        }
    }
    return cursor;

overflow:
    ereport(ERROR, (errmsg("pg_clickhouse: TCP block serialize overflow (scratch %zu bytes too small)", cap)));
    return 0;   /* unreachable */
}

/* ---------------------------------------------------------------------------------------------------
 * Hot-Cold Phase 3 Branch P2: pipelined send reactor (K-deep frame pool, exactly ONE send in flight).
 *
 * A frame carries a coalesced 2-entry iovec {header/meta, payload/body} (one sendmsg per block, H9). The
 * pool is a FIFO ring: send_drain = oldest occupied (the send head), send_count = #occupied. Only the head
 * is ever mid-send (byte-stream order). Publish serializes block N into a FREE fill-slot frame (the
 * snapshot, synchronous), then pumps the head non-blocking and returns -- so the scan/deform loop refills
 * cz->bufs for N+1 while the kernel drains frame N. Backpressure (all K occupied) waits on EPOLLOUT (or
 * reaps a zerocopy completion). K=1 ⇒ pool of one ⇒ acquire waits for the single frame to drain = the P1
 * single-in-flight behavior exactly (no special case).
 * --------------------------------------------------------------------------------------------------- */

typedef struct SendFrame
{
    struct iovec   iov[2];      /* iov[0]=header/meta, iov[1]=payload/body (when n_iov==2)            */
    int            n_iov;       /* 1 (header-only, e.g. EOS / empty payload) or 2                     */
    size_t         total_len;   /* sum of iov lengths                                                 */
    size_t         sent;        /* bytes handed to the kernel so far (partial-send resume offset)     */
    bool           is_eos;
    TcpBlockHeader bh;          /* bespoke header storage (iov[0] points here)                        */
    char          *scratch;     /* bespoke frame-owned payload buffer (iov[1]); arrow frames: unused  */
    size_t         scratch_cap;
    bool           zc_pending;  /* fully sent via MSG_ZEROCOPY; buffer pinned until the seq is reaped  */
    uint32_t       zc_seq;      /* last zerocopy seq this frame's send(s) consumed                    */
    int64_t        ready_at_ns; /* P2 microbench: monotonic ns before which this sent frame may not be    */
                                /* reclaimed (sent_time + tcp_send_delay_us); 0 when no delay injected    */
} SendFrame;

/* Monotonic now in nanoseconds (P2 injected-latency microbench). */
static int64_t
monotonic_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t) ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

/* Tight per-frame size: the max serialized footprint of ONE block (rows_per_block rows) for THIS schema,
 * capped at tcp_scratch_cap. For an all-fixed-width schema this is exact and small (~MB); a String column's
 * chars are variable, so any String column makes the frame fall back to tcp_scratch_cap (the safe upper
 * bound = the whole data region). Sizing frames to the real block instead of the 64 MiB data region saves
 * (K-1) x ~60 MiB per stream AND — critically — lets the zerocopy RLIMIT_MEMLOCK cap (K x frame <= 8 MiB)
 * admit K>1 for all-fixed schemas (otherwise K x 64 MiB forces K=1 for every query). */
static size_t
tcp_frame_capacity(const ShmProducer *p)
{
    size_t rows_pb = PGCH_SHM_ROWS_PER_BLOCK;
    size_t cur = align_up((size_t) p->n_columns * sizeof(ShmColumnDescriptor), 8);
    int i;

    for (i = 0; i < p->n_columns; i++)
    {
        ShmWireType wire = p->schema[i].wire;
        size_t elem;

        if (wire == SHM_WIRE_STRING)
            return p->tcp_scratch_cap;   /* variable chars: fall back to the safe upper bound */
        elem = shm_wire_fixed_width_size(wire);
        cur = align_up(cur, elem > 8 ? elem : 8) + rows_pb * elem + SHM_PADDING_FOR_SIMD;
    }
    cur += 4096;   /* small slack for the header + alignment rounding */
    return (cur < p->tcp_scratch_cap) ? cur : p->tcp_scratch_cap;
}

/* Lazily size + allocate the K-deep frame pool at the first publish (after the scratch cap is known). */
static void
tcp_reactor_init(ShmProducer *p)
{
    int k, i;
    size_t frame_cap;

    if (p->send_frames != NULL)
        return;
    k = p->send_k_configured >= 1 ? p->send_k_configured : 1;
    frame_cap = tcp_frame_capacity(p);
    /* H4: bound K x max_frame <= RLIMIT_MEMLOCK (8 MiB here) on the zerocopy path so the pinned-pages
     * accounting limit is not exceeded (max_frame = the tight per-block size, NOT the 64 MiB buffer alloc;
     * the pinned pages are the SENT block bytes); the per-send ENOBUFS+drain is the runtime backstop. */
    if (p->tcp_send_method == PGCH_TCP_SEND_MSG_ZEROCOPY && frame_cap > 0)
    {
        size_t budget = (size_t) 8 * 1024 * 1024;
        int kmax = (int) (budget / frame_cap);
        if (kmax < 1) kmax = 1;
        if (k > kmax)
        {
            ereport(LOG, (errmsg("pg_clickhouse: P2 capping run-ahead K=%d -> %d for msg_zerocopy "
                                 "(K x frame %zu must stay under RLIMIT_MEMLOCK 8 MiB)",
                                 k, kmax, frame_cap)));
            k = kmax;
        }
    }
    /* Memory cap for ALL methods (adversarial review P2 NB-4): bound the per-stream frame pool to
     * PGCH_P2_POOL_BUDGET so a String-schema query (frame_cap == the data-region fallback, ~64 MiB) at a
     * high K cannot blow up memory (K x 64 MiB x W streams). For an all-fixed schema (~MB frames) this
     * never bites at any reasonable K. */
    {
        size_t pool_budget = (size_t) 128 * 1024 * 1024;
        int kmax_mem = frame_cap > 0 ? (int) (pool_budget / frame_cap) : k;
        if (kmax_mem < 1) kmax_mem = 1;
        if (k > kmax_mem)
        {
            ereport(LOG, (errmsg("pg_clickhouse: P2 capping run-ahead K=%d -> %d "
                                 "(K x frame %zu must stay under the %zu-byte per-stream pool budget)",
                                 k, kmax_mem, frame_cap, pool_budget)));
            k = kmax_mem;
        }
    }
    p->send_k = k;
    /* Allocate in the producer's owner context (stable for the stream lifetime): tcp_reactor_init runs
     * lazily at the first publish, which may be inside a transient per-block context. */
    p->send_frames = (SendFrame *) MemoryContextAllocZero(p->owner_cxt, sizeof(SendFrame) * (size_t) k);
    for (i = 0; i < k; i++)
    {
        p->send_frames[i].scratch = (char *) MemoryContextAlloc(p->owner_cxt, frame_cap);
        p->send_frames[i].scratch_cap = frame_cap;
    }
    p->send_count = 0;
    p->send_drain = 0;
    ereport(LOG, (errmsg("pg_clickhouse: P2 send reactor K=%d (configured %d) method=%d frame=%zu bytes "
                         "pool=%zu bytes", k, p->send_k_configured, p->tcp_send_method, frame_cap,
                         (size_t) k * frame_cap)));
}

/* One non-blocking sendmsg of the head frame's REMAINING iovec (from frame->sent). Returns 1 = head fully
 * handed to the kernel, 0 = partial progress (keep pumping), -1 = WOULDBLOCK / no progress (stop). Throws
 * on a hard error / backend death. For msg_zerocopy each successful sendmsg consumes a zc seq (tagged on
 * the frame) and reaps completions; the frame stays pinned (reclaimed only when the seq is acked). */
static int
reactor_send_one(ShmProducer *p, int idx)
{
    SendFrame *f = &p->send_frames[idx];
    struct msghdr msg;
    struct iovec  iov[2];
    int     ni = 0, i;
    size_t  skip = f->sent;
    int     flags = MSG_NOSIGNAL;
    ssize_t w;

    for (i = 0; i < f->n_iov; i++)
    {
        size_t len = f->iov[i].iov_len;
        if (skip >= len) { skip -= len; continue; }
        iov[ni].iov_base = (char *) f->iov[i].iov_base + skip;
        iov[ni].iov_len  = len - skip;
        skip = 0;
        ni++;
    }
    if (ni == 0)
        return 1;   /* already fully sent */

    if (p->tcp_send_method == PGCH_TCP_SEND_MSG_ZEROCOPY && p->tcp_zc_sockopt_set)
        flags |= MSG_ZEROCOPY;

    CHECK_FOR_INTERRUPTS();
    memset(&msg, 0, sizeof(msg));
    msg.msg_iov = iov;
    msg.msg_iovlen = ni;
    w = sendmsg(p->tcp_conn_fd, &msg, flags);
    if (w > 0)
    {
        f->sent += (size_t) w;
        if (flags & MSG_ZEROCOPY)
        {
            f->zc_seq = p->tcp_zc_seq_next++;   /* mirrors the kernel's per-socket zerocopy counter */
            f->zc_pending = true;
            p->tcp_zc_sends++;
            tcp_zc_reap(p);
        }
        if (f->sent >= f->total_len)
        {
            if (!(flags & MSG_ZEROCOPY))
            {
                if (f->is_eos) { /* EOS frame fully sent */ }
                p->tcp_epoll_sends++;   /* a logical frame send completed via the non-blocking path */
            }
            /* P2 microbench: this frame is "in flight" for tcp_send_delay_us after its send completes,
             * emulating real-NIC send/completion latency the loopback deferred-copy cannot reproduce. */
            f->ready_at_ns = (p->tcp_send_delay_us > 0)
                ? monotonic_ns() + (int64_t) p->tcp_send_delay_us * 1000LL : 0;
            return 1;
        }
        return 0;   /* partial progress */
    }
    if (w < 0 && errno == EINTR)
        return 0;
    if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        return -1;   /* socket send buffer full (TCP backpressure) */
    if (w < 0 && errno == ENOBUFS)
    {
        /* zerocopy pinned-pages hit RLIMIT_MEMLOCK; reap to release some, then let the caller wait. */
        tcp_zc_reap(p);
        return -1;
    }
    if (origin_backend_dead(p))
        ereport(ERROR, (errmsg("pg_clickhouse: originating backend (pid %d) exited; abandoning TCP stream",
                               p->origin_pid)));
    ereport(ERROR, (errcode_for_file_access(), errmsg("pg_clickhouse: pipelined TCP send to consumer failed: %m")));
    return -1;   /* unreachable */
}

/* Reclaim fully-sent (and, for zerocopy, reaped) head frames to FREE. Returns #reclaimed. */
static int
reactor_reclaim(ShmProducer *p)
{
    int n = 0;

    while (p->send_count > 0)
    {
        SendFrame *f = &p->send_frames[p->send_drain];

        if (f->sent < f->total_len)
            break;                              /* head not fully sent yet */
        if (f->zc_pending)
        {
            tcp_zc_reap(p);
            if (!(p->tcp_zc_seq_acked_valid && (int32_t) (p->tcp_zc_seq_acked - f->zc_seq) >= 0))
                break;                          /* still pinned by the kernel */
            f->zc_pending = false;
        }
        if (f->ready_at_ns != 0 && monotonic_ns() < f->ready_at_ns)
            break;                              /* P2 microbench: still within the injected in-flight latency */
        p->send_drain = (p->send_drain + 1) % p->send_k;
        p->send_count--;
        n++;
    }
    return n;
}

/* The send position: the first occupied frame (scanning FIFO from send_drain) that is not yet fully sent.
 * Returns -1 if every occupied frame is fully sent (all in flight awaiting completion). send_count is <= K
 * so the scan is trivially cheap. This decouples the SEND cursor from the RECLAIM cursor (send_drain): a
 * frame is sent as soon as the prior one is fully handed to the kernel, WITHOUT waiting for the prior
 * frame's completion (zerocopy reap / injected latency) — so up to K frames are in flight (sent, awaiting
 * completion) concurrently. Byte-stream order is preserved (frames sent in FIFO order; one sendmsg at a
 * time on the single-threaded producer). */
static int
reactor_send_pos(ShmProducer *p)
{
    int i;
    for (i = 0; i < p->send_count; i++)
    {
        int idx = (p->send_drain + i) % p->send_k;
        if (p->send_frames[idx].sent < p->send_frames[idx].total_len)
            return idx;
    }
    return -1;
}

/* Non-blocking pump: reclaim completed frames from the drain head, then SEND the next not-fully-sent frame
 * as far as the socket accepts, advancing the send cursor across frames (send-ahead) until WOULDBLOCK or
 * every queued frame is fully sent. The kernel drains the accepted bytes to the wire — and, on the
 * zerocopy / injected-latency path, up to K frames sit in flight awaiting completion — while the caller
 * returns to scan/deform the next block. */
static void
reactor_pump(ShmProducer *p)
{
    p->send_pump_calls++;
    for (;;)
    {
        int pos, r;

        reactor_reclaim(p);                     /* free any completed in-flight frames (advances send_drain) */
        pos = reactor_send_pos(p);
        if (pos < 0)
            break;                              /* nothing left to send (all occupied frames fully sent) */
        r = reactor_send_one(p, pos);
        if (r < 0)
            break;                              /* WOULDBLOCK — socket buffer full */
        /* r==0 (partial) or 1 (this frame fully sent): loop to send the next frame */
    }
}

/* P2 microbench: sleep (interrupt-checked, 5ms slices) until the head frame's injected in-flight latency
 * elapses, so reactor_reclaim can then free it. Avoids spinning tcp_wait_writable on an already-sent head. */
static void
reactor_wait_delay(ShmProducer *p, SendFrame *head)
{
    while (head->ready_at_ns != 0)
    {
        int64_t rem = head->ready_at_ns - monotonic_ns();
        struct timespec ts;

        if (rem <= 0)
            return;
        CHECK_FOR_INTERRUPTS();
        if (origin_backend_dead(p))
            ereport(ERROR, (errmsg("pg_clickhouse: originating backend (pid %d) exited; "
                                   "abandoning TCP stream", p->origin_pid)));
        if (rem > 5000000LL) rem = 5000000LL;   /* 5ms slice to stay interrupt-responsive */
        ts.tv_sec = rem / 1000000000LL;
        ts.tv_nsec = rem % 1000000000LL;
        (void) nanosleep(&ts, NULL);
    }
}

/* True if the head frame is fully sent (+ zc reaped) but still within its injected in-flight latency. */
static inline bool
reactor_head_delay_pending(ShmProducer *p)
{
    SendFrame *head = &p->send_frames[p->send_drain];
    return head->sent >= head->total_len && !head->zc_pending
        && head->ready_at_ns != 0 && monotonic_ns() < head->ready_at_ns;
}

/* Acquire a FREE fill-slot frame, pumping the in-flight send and (if all K are occupied) waiting on
 * writability / a zerocopy completion / the injected latency until one frees. The producer backpressure point. */
static SendFrame *
reactor_acquire(ShmProducer *p)
{
    SendFrame *f;

    while (p->send_count >= p->send_k)
    {
        reactor_pump(p);
        if (p->send_count < p->send_k)
            break;
        {
            SendFrame *head = &p->send_frames[p->send_drain];

            if (head->sent >= head->total_len && head->zc_pending)
                tcp_zc_drain_until(p, head->zc_seq);   /* H4: block on THIS frame's seq only (its buffer is needed) */
            else if (reactor_head_delay_pending(p))
                reactor_wait_delay(p, head);           /* P2 microbench: wait out injected in-flight latency */
            else
            {
                if (origin_backend_dead(p))
                    ereport(ERROR, (errmsg("pg_clickhouse: originating backend (pid %d) exited; "
                                           "abandoning TCP stream", p->origin_pid)));
                tcp_wait_writable(p, 100);
            }
        }
    }
    f = &p->send_frames[(p->send_drain + p->send_count) % p->send_k];
    f->sent = 0;
    f->zc_pending = false;
    f->ready_at_ns = 0;
    f->n_iov = 0;
    f->total_len = 0;
    f->is_eos = false;
    return f;
}

/* Block (pumping + waiting) until every queued frame is fully sent to the kernel and, for zerocopy, every
 * pinned buffer reaped. Used for EOS (H5: the consumer must see all data + EOS) and as the teardown flush. */
static void
reactor_drain_all(ShmProducer *p)
{
    while (p->send_count > 0)
    {
        reactor_pump(p);
        if (p->send_count == 0)
            break;
        {
            SendFrame *head = &p->send_frames[p->send_drain];

            if (head->sent >= head->total_len && head->zc_pending)
                tcp_zc_drain_until(p, head->zc_seq);
            else if (reactor_head_delay_pending(p))
                reactor_wait_delay(p, head);
            else
            {
                if (origin_backend_dead(p))
                    ereport(ERROR, (errmsg("pg_clickhouse: originating backend (pid %d) exited; "
                                           "abandoning TCP stream", p->origin_pid)));
                tcp_wait_writable(p, 100);
            }
        }
    }
}

/* TCP analog of publish_block (P2): lazy accept+handshake on the first call, then NON-BLOCKING publish:
 * serialize block N into a free pooled frame (the snapshot copy out of cz->bufs), enqueue it, pump the one
 * in-flight send, and return so scan/deform of N+1 overlaps the drain of frame N. The EOS frame is enqueued
 * after the last data frame and then drained synchronously (H5). */
static void
tcp_publish_block(ShmProducer *p, const ShmColumnPayload *payloads, int n_payloads,
                  size_t row_count, bool is_eos)
{
    int saved_phase = -1;
    SendFrame *f;
    size_t payload_len = 0;

    if (p->eos_published)
        ereport(ERROR, (errmsg("pg_clickhouse: shm stream already ended")));
    if (!is_eos && n_payloads != p->n_columns)
        ereport(ERROR, (errmsg("pg_clickhouse: shm payload count %d != schema %d", n_payloads, p->n_columns)));
    if (row_count > SHM_IMPL_MAX_ROWS)
        ereport(ERROR, (errmsg("pg_clickhouse: shm row_count %zu exceeds limit %u", row_count, SHM_IMPL_MAX_ROWS)));

    if (!p->tcp_handshake_sent)
        tcp_accept_and_handshake(p);
    tcp_reactor_init(p);

    /* PUBLISH phase = serialize the snapshot + submit + (when all K busy) the backpressure wait. The
     * kernel drain of an in-flight frame overlaps the SUBSEQUENT block's DEFORM/READ (it returns here). */
    if (p->timers != NULL && p->timers->enabled)
        saved_phase = p->timers->cur;
    pgch_phase_switch(p->timers, PGCH_PH_PUBLISH);

    f = reactor_acquire(p);
    if (!is_eos)
        payload_len = tcp_serialize_block(p, payloads, row_count, f->scratch, f->scratch_cap);

    memset(&f->bh, 0, sizeof(f->bh));
    f->bh.payload_len = payload_len;
    f->bh.row_count = is_eos ? 0 : row_count;
    f->bh.descriptors_offset = 0;
    f->bh.eos_marker = is_eos ? 1 : 0;
    f->is_eos = is_eos;
    f->iov[0].iov_base = &f->bh;
    f->iov[0].iov_len  = sizeof(f->bh);
    if (payload_len > 0)
    {
        f->iov[1].iov_base = f->scratch;
        f->iov[1].iov_len  = payload_len;
        f->n_iov = 2;
    }
    else
        f->n_iov = 1;
    f->total_len = sizeof(f->bh) + payload_len;
    f->sent = 0;

    /* Enqueue + pump (non-blocking). A frame queued behind an already-in-flight one is the overlap. */
    p->send_count++;
    p->tcp_send_bytes += f->total_len;
    if (p->send_count > 1)
        p->send_overlap_frames++;
    reactor_pump(p);

    if (is_eos)
        reactor_drain_all(p);   /* H5: flush all data frames + EOS to the kernel before returning */

    if (saved_phase >= 0)
        pgch_phase_switch(p->timers, saved_phase);
}

/* Bring up the TCP listener (ephemeral 127.0.0.1 port) + the per-block serialize scratch. */
static void
tcp_producer_setup(ShmProducer *p, size_t scratch_size)
{
    struct sockaddr_in addr;
    socklen_t alen = sizeof(addr);
    int one = 1;

    p->listen_fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
    if (p->listen_fd < 0)
        ereport(ERROR, (errcode_for_file_access(), errmsg("pg_clickhouse: TCP socket() failed: %m")));
    (void) setsockopt(p->listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;   /* ephemeral */
    if (bind(p->listen_fd, (struct sockaddr *) &addr, sizeof(addr)) < 0)
        ereport(ERROR, (errcode_for_file_access(), errmsg("pg_clickhouse: TCP bind() failed: %m")));
    if (listen(p->listen_fd, 1) < 0)
        ereport(ERROR, (errcode_for_file_access(), errmsg("pg_clickhouse: TCP listen() failed: %m")));
    if (getsockname(p->listen_fd, (struct sockaddr *) &addr, &alen) < 0)
        ereport(ERROR, (errcode_for_file_access(), errmsg("pg_clickhouse: TCP getsockname() failed: %m")));
    p->tcp_port = ntohs(addr.sin_port);

    /* Columnizer block sizing (matches block_footprint): descriptors at frame offset 0, payload
     * after; the whole scratch is one block's budget (no K-slot split for TCP). */
    p->per_slot_payload_offset = align_up((size_t) p->n_columns * sizeof(ShmColumnDescriptor), 8);
    p->per_slot_capacity = scratch_size;
    p->tcp_scratch_cap = scratch_size;
    p->tcp_scratch = palloc(scratch_size);
}

ShmProducer *
shm_producer_create(const char *name,
                    const ShmColumnSchema *schema, int n_columns,
                    uint32_t ring_depth_k, size_t data_region_size,
                    MemoryContext owner_cxt,
                    ShmProducerTransport transport)
{
    MemoryContext old = MemoryContextSwitchTo(owner_cxt);
    ShmProducer *p = palloc0(sizeof(ShmProducer));
    ShmHandshake *hs;
    ShmSchemaEntry *schema_base;
    size_t off;
    long page = sysconf(_SC_PAGESIZE);
    struct sockaddr_un addr;
    int i;

    p->shm_fd = p->event_fd = p->listen_fd = -1;
    p->pump_stop_fd = -1;
    p->mapping = NULL;
    p->owner_cxt = owner_cxt;
    p->transport = transport;
    p->tcp_conn_fd = -1;
    p->tcp_send_epoll_fd = -1;
    p->tcp_handshake_sent = false;
    p->tcp_scratch = NULL;
    p->tcp_scratch_cap = 0;
    p->send_k_configured = 1;   /* set by shm_producer_set_send_inflight; default single-in-flight (P1) */
    p->send_frames = NULL;      /* lazily allocated at the first publish (tcp_reactor_init) */
    p->tcp_sndbuf_bytes = 0;    /* set by shm_producer_set_sndbuf; 0 = default 32 MiB */
    p->tcp_send_delay_us = 0;   /* set by shm_producer_set_send_delay_us; 0 = no injected latency */
    p->tcp_port = 0;
    p->tcp_send_method = PGCH_TCP_SEND_BLOCKING;   /* set by shm_producer_set_tcp_send_method */

    if (ring_depth_k == 0 || ring_depth_k > SHM_IMPL_MAX_K)
        ereport(ERROR, (errmsg("pg_clickhouse: shm ring depth %u out of range (1..%u)",
                               ring_depth_k, SHM_IMPL_MAX_K)));
    if (n_columns <= 0 || (uint32_t) n_columns > SHM_IMPL_MAX_COLS)
        ereport(ERROR, (errmsg("pg_clickhouse: shm column count %d out of range (1..%u)",
                               n_columns, SHM_IMPL_MAX_COLS)));
    if (data_region_size == 0)
        ereport(ERROR, (errmsg("pg_clickhouse: shm data region size must be > 0")));

    /* Normalize the name (leading '/'). */
    if (name[0] == '/')
        p->shm_name = pstrdup(name);
    else
        p->shm_name = psprintf("/%s", name);
    p->socket_path = control_socket_path(p->shm_name);

    /* Register cleanup before acquiring any kernel resource, so an ereport on
     * the very next line still unwinds the (so far empty) producer safely. */
    p->cleanup_cb.func = producer_cleanup_callback;
    p->cleanup_cb.arg = p;
    MemoryContextRegisterResetCallback(owner_cxt, &p->cleanup_cb);

    /* Lay out the region (mirrors InProcessProducer::computeTotalSize). */
    p->ring_depth_k = ring_depth_k;
    p->n_columns = n_columns;
    p->schema = palloc0(sizeof(ShmColumnSchema) * n_columns);
    memcpy(p->schema, schema, sizeof(ShmColumnSchema) * n_columns);

    /* Socket transports (bespoke TCP or Arrow): no SHM object / control socket / pump thread — a
     * per-stream TCP listener (+ a bespoke serialize scratch; the Arrow encoder owns its own
     * buffers). The cleanup callback (registered above) closes them. */
    if (transport == PGCH_PRODUCER_TRANSPORT_TCP || transport == PGCH_PRODUCER_TRANSPORT_ARROW)
    {
        tcp_producer_setup(p, data_region_size);
        MemoryContextSwitchTo(old);
        return p;
    }

    off = align_up(sizeof(ShmHandshake), (size_t) page);          /* slot table */
    off += (size_t) ring_depth_k * sizeof(ShmSlot);
    off += (size_t) n_columns * sizeof(ShmSchemaEntry);
    off = align_up(off, (size_t) page);                           /* data region */
    off += data_region_size;
    p->mapping_size = align_up(off, (size_t) page);

    /* A stale object from a prior crash blocks O_EXCL. */
    shm_unlink(p->shm_name);
    p->shm_fd = shm_open(p->shm_name, O_RDWR | O_CREAT | O_EXCL, 0666);
    if (p->shm_fd < 0)
        ereport(ERROR, (errcode_for_file_access(),
                        errmsg("pg_clickhouse: shm_open(\"%s\") failed: %m", p->shm_name)));
    /* The co-located ClickHouse consumer typically runs as a different OS user.
     * Phase-1 trust model: grant rw to the local, trusted consumer (shm_open's
     * mode is masked by umask, so force it explicitly). */
    (void) fchmod(p->shm_fd, 0666);
    if (ftruncate(p->shm_fd, (off_t) p->mapping_size) != 0)
        ereport(ERROR, (errcode_for_file_access(),
                        errmsg("pg_clickhouse: ftruncate(\"%s\") failed: %m", p->shm_name)));
    p->mapping = mmap(NULL, p->mapping_size, PROT_READ | PROT_WRITE, MAP_SHARED, p->shm_fd, 0);
    if (p->mapping == MAP_FAILED)
        ereport(ERROR, (errcode_for_file_access(),
                        errmsg("pg_clickhouse: mmap(\"%s\") failed: %m", p->shm_name)));

    /* ftruncate zeroes the object: EMPTY state, zero counters/refcounts. */

    /* Schema table. */
    schema_base = (ShmSchemaEntry *) ((char *) p->mapping
                  + align_up(sizeof(ShmHandshake), (size_t) page)
                  + (size_t) ring_depth_k * sizeof(ShmSlot));
    for (i = 0; i < n_columns; i++)
    {
        if (strlen(schema[i].name) + 1 > SHM_SCHEMA_STR_MAX
            || strlen(schema[i].type_string) + 1 > SHM_SCHEMA_STR_MAX)
            ereport(ERROR, (errmsg("pg_clickhouse: shm schema name/type too long")));
        memset(&schema_base[i], 0, sizeof(ShmSchemaEntry));
        memcpy(schema_base[i].name, schema[i].name, strlen(schema[i].name));
        memcpy(schema_base[i].type_string, schema[i].type_string, strlen(schema[i].type_string));
    }

    /* Handshake (magic last, release). */
    hs = hs_of(p);
    hs->abi_version = SHM_ABI_VERSION_1;
    hs->ring_depth_k = ring_depth_k;
    hs->schema_count = (uint32_t) n_columns;
    hs->reserved_pad32 = 0;
    hs->slot_table_offset = align_up(sizeof(ShmHandshake), (size_t) page);
    hs->slot_table_stride = sizeof(ShmSlot);
    hs->schema_table_offset = hs->slot_table_offset + (uint64_t) ring_depth_k * sizeof(ShmSlot);
    hs->schema_table_size = (uint64_t) n_columns * sizeof(ShmSchemaEntry);
    hs->data_region_offset = align_up(hs->schema_table_offset + hs->schema_table_size, (size_t) page);
    hs->data_region_size = data_region_size;
    memset(hs->reserved64, 0, sizeof(hs->reserved64));
    __atomic_store_n(&hs->magic, SHM_MAGIC, __ATOMIC_RELEASE);

    /* Per-slot bookkeeping. Round per-slot capacity down to 64 so each slot's
     * descriptor array and first column buffer are well-aligned. */
    p->per_slot_capacity = (data_region_size / ring_depth_k) & ~((size_t) 63);
    if (p->per_slot_capacity < SHM_PADDING_FOR_SIMD * 4)
        ereport(ERROR, (errmsg("pg_clickhouse: shm data region too small for %u slots", ring_depth_k)));
    p->per_slot_payload_offset = align_up((size_t) n_columns * sizeof(ShmColumnDescriptor), 64);
    p->next_sequence = palloc0(sizeof(uint64_t) * ring_depth_k);
    p->next_slot = 0;

    for (i = 0; i < (int) ring_depth_k; i++)
        slot_at(p, (uint32_t) i)->slot_index = (uint32_t) i;

    /* Readiness eventfd (semaphore semantics, matching the consumer). */
    p->event_fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK | EFD_SEMAPHORE);
    if (p->event_fd < 0)
        ereport(ERROR, (errcode_for_file_access(), errmsg("pg_clickhouse: eventfd() failed: %m")));

    /* Control socket. */
    if (strlen(p->socket_path) >= sizeof(addr.sun_path))
        ereport(ERROR, (errmsg("pg_clickhouse: shm control socket path too long: %s", p->socket_path)));
    unlink(p->socket_path);
    p->listen_fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
    if (p->listen_fd < 0)
        ereport(ERROR, (errcode_for_file_access(), errmsg("pg_clickhouse: socket() failed: %m")));
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    memcpy(addr.sun_path, p->socket_path, strlen(p->socket_path));
    if (bind(p->listen_fd, (struct sockaddr *) &addr, sizeof(addr)) < 0)
        ereport(ERROR, (errcode_for_file_access(),
                        errmsg("pg_clickhouse: bind(\"%s\") failed: %m", p->socket_path)));
    /* Allow the co-located consumer (different OS user) to connect (see SHM note). */
    (void) chmod(p->socket_path, 0666);
    if (listen(p->listen_fd, 16) < 0)
        ereport(ERROR, (errcode_for_file_access(), errmsg("pg_clickhouse: listen() failed: %m")));

    /* Start the socket-pump thread (serves the readiness eventfd to consumers).
     * The stop eventfd lets teardown wake the thread out of poll() immediately
     * rather than waiting up to one poll timeout -- otherwise ~half the poll
     * interval is added to every query's worker-reap on the critical path. */
    p->pump_stop = 0;
    p->pump_stop_fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (p->pump_stop_fd < 0)
        ereport(ERROR, (errcode_for_file_access(),
                        errmsg("pg_clickhouse: eventfd() failed for control-socket pump: %m")));
    if (pthread_create(&p->pump_thread, NULL, pump_thread_main, p) != 0)
        ereport(ERROR, (errmsg("pg_clickhouse: could not start SHM control-socket pump thread")));
    p->pump_running = true;

    MemoryContextSwitchTo(old);
    return p;
}

const char *
shm_producer_shm_name(const ShmProducer *p)
{
    return p->shm_name;
}

void
shm_producer_set_origin_pid(ShmProducer *p, int pid)
{
    p->origin_pid = pid;
}

void
shm_producer_set_tcp_send_method(ShmProducer *p, int method)
{
    p->tcp_send_method = method;
}

void
shm_producer_set_send_inflight(ShmProducer *p, int k)
{
    /* P2 producer run-ahead depth (the GUC already clamps to [1,64]); the pool is sized at the first
     * publish (tcp_reactor_init), which may further cap K for msg_zerocopy (RLIMIT_MEMLOCK). */
    p->send_k_configured = (k >= 1) ? k : 1;
}

void
shm_producer_set_sndbuf(ShmProducer *p, int bytes)
{
    p->tcp_sndbuf_bytes = (bytes > 0) ? bytes : 0;
}

void
shm_producer_set_send_delay_us(ShmProducer *p, int us)
{
    p->tcp_send_delay_us = (us > 0) ? us : 0;
}

void
shm_producer_tcp_pipeline_stats(const ShmProducer *p, int *k_effective,
                                uint64_t *pump_calls, uint64_t *overlap_frames)
{
    /* P2 mechanism observability: k_effective = the pool depth actually used (post RLIMIT cap);
     * overlap_frames = #frames enqueued while an earlier frame was still in flight (the structural
     * overlap signal — >0 means deform/serialize of a later block ran ahead of the send of an earlier
     * one). All zero for an SHM producer or a stream that never pipelined. */
    if (k_effective)    *k_effective = p->send_k;
    if (pump_calls)     *pump_calls = p->send_pump_calls;
    if (overlap_frames) *overlap_frames = p->send_overlap_frames;
}

void
shm_producer_tcp_send_stats(const ShmProducer *p, uint64_t *epoll_sends,
                            uint64_t *blocking_sends, uint64_t *send_bytes)
{
    if (epoll_sends)    *epoll_sends = p->tcp_epoll_sends;
    if (blocking_sends) *blocking_sends = p->tcp_blocking_sends;
    if (send_bytes)     *send_bytes = p->tcp_send_bytes;
}

void
shm_producer_tcp_zc_stats(const ShmProducer *p, uint64_t *zc_sends,
                          uint64_t *zc_notifs, uint64_t *zc_copied)
{
    if (zc_sends)  *zc_sends = p->tcp_zc_sends;
    if (zc_notifs) *zc_notifs = p->tcp_zc_notifs;
    if (zc_copied) *zc_copied = p->tcp_zc_copied;
}

void
shm_producer_set_phase_timers(ShmProducer *p, struct PgchPhaseTimers *t)
{
    p->timers = (PgchPhaseTimers *) t;
}

/* --------------------------------------------------------------------- */
/* Publish */
/* --------------------------------------------------------------------- */

static void
publish_block(ShmProducer *p, const ShmColumnPayload *payloads, int n_payloads,
              size_t row_count, bool is_eos)
{
    uint32_t slot_pos;
    ShmSlot *slot;
    char *region = NULL;
    ShmColumnDescriptor *descs;
    size_t slot_data_base;
    size_t cursor;
    size_t slot_end;
    int i;
    int saved_phase = -1;   /* phase to restore on return (benchmark instrumentation) */

    if (p->eos_published)
        ereport(ERROR, (errmsg("pg_clickhouse: shm stream already ended")));
    if (n_payloads != p->n_columns)
        ereport(ERROR, (errmsg("pg_clickhouse: shm payload count %d != schema %d",
                               n_payloads, p->n_columns)));
    if (row_count > SHM_IMPL_MAX_ROWS)
        ereport(ERROR, (errmsg("pg_clickhouse: shm row_count %zu exceeds limit %u",
                               row_count, SHM_IMPL_MAX_ROWS)));

    slot_pos = p->next_slot % p->ring_depth_k;
    slot = slot_at(p, slot_pos);

    /* Charge the ring-full wait to PUBLISH_STALL (idle time waiting on the
     * consumer, NOT producer work), save/restoring the caller's phase. */
    if (p->timers != NULL && p->timers->enabled)
        saved_phase = p->timers->cur;
    pgch_phase_switch(p->timers, PGCH_PH_STALL);

    /* Wait for the slot to be reusable. The consumer drives PUBLISHED->EMPTY on
     * the last retain drop, so we poll state==EMPTY (never retain_refcount). Pump
     * the control socket while waiting so the consumer can attach and drain. */
    while (__atomic_load_n(&slot->state, __ATOMIC_ACQUIRE) != SHM_STATE_EMPTY)
    {
        /* The pump thread services the control socket; just wait for the
         * consumer to free a slot (honouring query cancel). */
        CHECK_FOR_INTERRUPTS();
        if (origin_backend_dead(p))
            ereport(ERROR,
                    (errmsg("pg_clickhouse: originating backend (pid %d) exited; "
                            "abandoning SHM stream", p->origin_pid)));
        pg_usleep(1000L);       /* 1 ms */
    }

    /* Wait over: the memcpy of built columns into the slot is PUBLISH work. */
    pgch_phase_switch(p->timers, PGCH_PH_PUBLISH);

    /* EMPTY -> WRITING (counter bump before the state store, both release). */
    __atomic_fetch_add(&slot->transition_counter, 1, __ATOMIC_RELEASE);
    __atomic_store_n(&slot->state, SHM_STATE_WRITING, __ATOMIC_RELEASE);

    region = data_region(p);
    slot_data_base = (size_t) slot_pos * p->per_slot_capacity;
    descs = (ShmColumnDescriptor *) (region + slot_data_base);
    memset(descs, 0, (size_t) p->n_columns * sizeof(ShmColumnDescriptor));

    cursor = slot_data_base + p->per_slot_payload_offset;
    slot_end = slot_data_base + p->per_slot_capacity;

    for (i = 0; i < p->n_columns; i++)
    {
        const ShmColumnPayload *pay = is_eos ? NULL : &payloads[i];
        ShmColumnDescriptor *d = &descs[i];
        ShmWireType wire = p->schema[i].wire;

        d->type = (uint32_t) wire;

        if (wire == SHM_WIRE_STRING)
        {
            size_t chars_bytes = is_eos ? 0 : pay->value_len;
            size_t offs_bytes = is_eos ? 0 : pay->offsets_count * sizeof(uint64_t);
            size_t chars_off, sentinel_off, offs_off;

            /* chars buffer + SIMD pad. */
            chars_off = align_up(cursor, 8);
            cursor = chars_off + chars_bytes + SHM_PADDING_FOR_SIMD;
            if (cursor > slot_end)
                goto overflow;
            if (chars_bytes > 0)
                memcpy(region + chars_off, pay->value_buf, chars_bytes);

            /* 8-byte offsets[-1] zero sentinel, then offsets[0..]. */
            sentinel_off = align_up(cursor, 8);
            cursor = sentinel_off + sizeof(uint64_t);
            if (cursor > slot_end)
                goto overflow;
            memset(region + sentinel_off, 0, sizeof(uint64_t));

            offs_off = align_up(cursor, 8);
            cursor = offs_off + offs_bytes + SHM_PADDING_FOR_SIMD;
            if (cursor > slot_end)
                goto overflow;
            if (offs_bytes > 0)
                memcpy(region + offs_off, pay->offsets_buf, offs_bytes);

            d->value_offset = chars_off;
            d->value_count = chars_bytes;
            d->value_padding = SHM_PADDING_FOR_SIMD;
            d->offsets_offset = offs_off;          /* points AFTER the sentinel */
            d->offsets_count = is_eos ? 0 : pay->offsets_count;
            d->offsets_padding = SHM_PADDING_FOR_SIMD;
        }
        else
        {
            size_t elem = shm_wire_fixed_width_size(wire);
            size_t vbytes = is_eos ? 0 : row_count * elem;
            /* Align the value buffer to the element's natural alignment so the
             * consumer's `value_offset % elem_size == 0` check holds for every
             * width. Only Decimal128 (16) exceeds 8; smaller widths keep >=8. The
             * per-slot region base (per_slot_capacity, payload offset) is already
             * 64-aligned, so a 16-aligned cursor stays within the slot. */
            size_t voff = align_up(cursor, elem > 8 ? elem : 8);

            cursor = voff + vbytes + SHM_PADDING_FOR_SIMD;
            if (cursor > slot_end)
                goto overflow;
            if (vbytes > 0)
                memcpy(region + voff, pay->value_buf, vbytes);

            d->value_offset = voff;
            d->value_count = is_eos ? 0 : row_count;
            d->value_padding = SHM_PADDING_FOR_SIMD;
        }
    }

    /* Plain slot metadata (written before the publishing release-store). */
    slot->per_column_descriptors_offset = slot_data_base;
    slot->row_count = row_count;
    __atomic_store_n(&slot->eos_marker, is_eos ? 1 : 0, __ATOMIC_RELAXED);
    __atomic_store_n(&slot->sequence, ++p->next_sequence[slot_pos], __ATOMIC_RELAXED);

    /* WRITING -> PUBLISHED (counter bump before the state store, both release). */
    __atomic_fetch_add(&slot->transition_counter, 1, __ATOMIC_RELEASE);
    __atomic_store_n(&slot->state, SHM_STATE_PUBLISHED, __ATOMIC_RELEASE);

    /* Notify the consumer (ordered after publication) and service the socket. */
    {
        uint64_t one = 1;
        ssize_t w = write(p->event_fd, &one, sizeof(one));
        (void) w;   /* EAGAIN on a full counter is harmless: consumer still polls */
    }

    /* Resume the caller's phase (PUBLISH work charged; back to DEFORM, etc.). */
    if (saved_phase >= 0)
        pgch_phase_switch(p->timers, saved_phase);

    p->next_slot++;
    return;

overflow:
    ereport(ERROR, (errmsg("pg_clickhouse: shm per-slot region overflow "
                           "(slot %u capacity %zu bytes too small for block)",
                           slot_pos, p->per_slot_capacity)));
}

/*
 * Usable per-slot data-region capacity in bytes (the budget a single published
 * block's payload must fit within). The columnizer uses this to size byte-bounded
 * blocks so a wide projection never trips publish_block's overflow.
 */
size_t
shm_producer_slot_capacity(const ShmProducer *p)
{
    return p->per_slot_capacity;
}

/*
 * Bytes a block of `row_count` rows would occupy in a slot, given each column's
 * staged string-chars length in `string_lens[i]` (ignored for fixed-width
 * columns; pass 0). MUST mirror publish_block's slot layout exactly (per-column
 * alignment, SIMD padding, and the per-string offsets sentinel) so the columnizer
 * can publish a short block before the next sub-batch would overflow the slot.
 */
size_t
shm_producer_block_footprint(const ShmProducer *p, const size_t *string_lens,
                             size_t row_count)
{
    size_t cursor = p->per_slot_payload_offset;
    int    i;

    for (i = 0; i < p->n_columns; i++)
    {
        ShmWireType wire = p->schema[i].wire;

        if (wire == SHM_WIRE_STRING)
        {
            size_t chars_bytes = string_lens ? string_lens[i] : 0;
            size_t offs_bytes = row_count * sizeof(uint64_t);

            cursor = align_up(cursor, 8) + chars_bytes + SHM_PADDING_FOR_SIMD;  /* chars */
            cursor = align_up(cursor, 8) + sizeof(uint64_t);                    /* sentinel */
            cursor = align_up(cursor, 8) + offs_bytes + SHM_PADDING_FOR_SIMD;   /* offsets */
        }
        else
        {
            size_t elem = shm_wire_fixed_width_size(wire);

            cursor = align_up(cursor, elem > 8 ? elem : 8) + row_count * elem + SHM_PADDING_FOR_SIMD;
        }
    }
    return cursor;
}

void
shm_producer_publish(ShmProducer *p, const ShmColumnPayload *payloads, int n_payloads,
                     size_t row_count)
{
    if (p->transport == PGCH_PRODUCER_TRANSPORT_TCP)
        tcp_publish_block(p, payloads, n_payloads, row_count, false);
    else if (p->transport == PGCH_PRODUCER_TRANSPORT_ARROW)
#ifdef PGCH_USE_NANOARROW
        arrow_publish_block(p, payloads, n_payloads, row_count, false);
#else
        ereport(ERROR, (errmsg("pg_clickhouse: arrow transport requires a build with nanoarrow")));
#endif
    else
        publish_block(p, payloads, n_payloads, row_count, false);
}

void
shm_producer_signal_eos(ShmProducer *p)
{
    if (p->transport == PGCH_PRODUCER_TRANSPORT_TCP)
        tcp_publish_block(p, NULL, p->n_columns, 0, true);
    else if (p->transport == PGCH_PRODUCER_TRANSPORT_ARROW)
#ifdef PGCH_USE_NANOARROW
        arrow_publish_block(p, NULL, p->n_columns, 0, true);
#else
        ereport(ERROR, (errmsg("pg_clickhouse: arrow transport requires a build with nanoarrow")));
#endif
    else
        publish_block(p, NULL, p->n_columns, 0, true);
    p->eos_published = true;
}

uint16_t
shm_producer_tcp_port(const ShmProducer *p)
{
    return p->tcp_port;
}

/* --------------------------------------------------------------------- */
/* Destroy: drain consumer retains, then release kernel resources. */
/* --------------------------------------------------------------------- */

void
shm_producer_destroy(ShmProducer *p)
{
    int spins = 0;

    if (p->cleaned)
        return;

    /* Socket transports (bespoke TCP / Arrow) have no shared ring/slots to drain: the consumer
     * reads the byte stream and the kernel guarantees buffered bytes are delivered before the FIN,
     * so close immediately. */
    if (p->transport == PGCH_PRODUCER_TRANSPORT_TCP || p->transport == PGCH_PRODUCER_TRANSPORT_ARROW)
    {
        producer_cleanup(p);
        return;
    }

    /* Producer must outlive every consumer retain. Wait (cooperatively, bounded)
     * until all slots are released (consumer drove them back to EMPTY). Keep
     * pumping the control socket so a consumer that attaches during teardown is
     * still handed the readiness eventfd. A generous cap bounds the wait if no
     * consumer ever attaches (e.g. the query was cancelled before reading). */
    for (;;)
    {
        bool all_empty = true;
        uint32_t i;

        /* The pump thread keeps serving the control socket; just observe drain. */
        for (i = 0; i < p->ring_depth_k; i++)
        {
            if (__atomic_load_n(&slot_at(p, i)->state, __ATOMIC_ACQUIRE) != SHM_STATE_EMPTY)
            {
                all_empty = false;
                break;
            }
        }
        if (all_empty)
            break;
        if (++spins > 60000)    /* ~60s cap */
            break;
        /* If the backend has gone, the consumer was cancelled and will never
         * release its retains -- stop waiting and reclaim immediately. */
        if (origin_backend_dead(p))
            break;
        CHECK_FOR_INTERRUPTS();
        pg_usleep(1000L);
    }

    producer_cleanup(p);
}
