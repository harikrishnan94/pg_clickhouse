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
#include "shm_phase.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <signal.h>             /* kill() for backend-liveness checks */
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

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
};

/* --------------------------------------------------------------------- */
/* Small helpers */
/* --------------------------------------------------------------------- */

size_t
shm_wire_fixed_width_size(ShmWireType t)
{
    switch (t)
    {
        case SHM_WIRE_INT8:  case SHM_WIRE_UINT8:  return 1;
        case SHM_WIRE_INT16: case SHM_WIRE_UINT16: case SHM_WIRE_DATE: return 2;
        case SHM_WIRE_INT32: case SHM_WIRE_UINT32: case SHM_WIRE_FLOAT32:
        case SHM_WIRE_DATETIME: case SHM_WIRE_DATE32: case SHM_WIRE_DECIMAL32: return 4;
        case SHM_WIRE_INT64: case SHM_WIRE_UINT64: case SHM_WIRE_FLOAT64:
        case SHM_WIRE_DECIMAL64: case SHM_WIRE_DATETIME64: return 8;
        case SHM_WIRE_DECIMAL128: return 16;
        case SHM_WIRE_STRING: return 0;
    }
    return 0;
}

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
tcp_send_all(ShmProducer *p, const void *buf, size_t n)
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

/* Accept the consumer (poll loop honouring cancel + backend death), set TCP_NODELAY + a send
 * timeout, and send the one-shot handshake (header + ShmSchemaEntry[n_columns]). */
static void
tcp_accept_and_handshake(ShmProducer *p)
{
    TcpHandshakeHeader hs;
    ShmSchemaEntry *se;
    struct timeval tv;
    int one = 1;
    int i;

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
        int sndbuf = 32 * 1024 * 1024;
        (void) setsockopt(p->tcp_conn_fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
    }

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

/* Lay one block into p->tcp_scratch frame-relative (descriptors at offset 0, then per-column
 * buffers with the SHM align / SIMD-padding / offsets[-1]-zero-sentinel layout). Returns the
 * used payload length. MUST match the consumer's adopt() expectations and block_footprint(). */
static size_t
tcp_serialize_block(ShmProducer *p, const ShmColumnPayload *payloads, size_t row_count)
{
    char *buf = p->tcp_scratch;
    size_t cap = p->tcp_scratch_cap;
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

/* TCP analog of publish_block: lazy accept+handshake on the first call, serialize + send. */
static void
tcp_publish_block(ShmProducer *p, const ShmColumnPayload *payloads, int n_payloads,
                  size_t row_count, bool is_eos)
{
    int saved_phase = -1;
    TcpBlockHeader bh;
    size_t payload_len = 0;

    if (p->eos_published)
        ereport(ERROR, (errmsg("pg_clickhouse: shm stream already ended")));
    if (!is_eos && n_payloads != p->n_columns)
        ereport(ERROR, (errmsg("pg_clickhouse: shm payload count %d != schema %d", n_payloads, p->n_columns)));
    if (row_count > SHM_IMPL_MAX_ROWS)
        ereport(ERROR, (errmsg("pg_clickhouse: shm row_count %zu exceeds limit %u", row_count, SHM_IMPL_MAX_ROWS)));

    if (!p->tcp_handshake_sent)
        tcp_accept_and_handshake(p);

    /* Serialize + socket send is the TCP PUBLISH phase (CPU clock excludes the send-blocked wait,
     * which lands in PUBLISH wall — TCP has no separate ring-full STALL). */
    if (p->timers != NULL && p->timers->enabled)
        saved_phase = p->timers->cur;
    pgch_phase_switch(p->timers, PGCH_PH_PUBLISH);

    if (!is_eos)
        payload_len = tcp_serialize_block(p, payloads, row_count);

    memset(&bh, 0, sizeof(bh));
    bh.payload_len = payload_len;
    bh.row_count = is_eos ? 0 : row_count;
    bh.descriptors_offset = 0;
    bh.eos_marker = is_eos ? 1 : 0;
    tcp_send_all(p, &bh, sizeof(bh));
    if (payload_len > 0)
        tcp_send_all(p, p->tcp_scratch, payload_len);

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
    p->tcp_handshake_sent = false;
    p->tcp_scratch = NULL;
    p->tcp_scratch_cap = 0;
    p->tcp_port = 0;

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

    /* TCP transport: no SHM object / control socket / pump thread — a per-stream TCP listener and
     * a serialize scratch buffer instead. The cleanup callback (registered above) closes them. */
    if (transport == PGCH_PRODUCER_TRANSPORT_TCP)
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
    else
        publish_block(p, payloads, n_payloads, row_count, false);
}

void
shm_producer_signal_eos(ShmProducer *p)
{
    if (p->transport == PGCH_PRODUCER_TRANSPORT_TCP)
        tcp_publish_block(p, NULL, p->n_columns, 0, true);
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

    /* TCP transport has no shared ring/slots to drain: the consumer reads the byte stream and the
     * kernel guarantees buffered bytes are delivered before the FIN, so close immediately. */
    if (p->transport == PGCH_PRODUCER_TRANSPORT_TCP)
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
