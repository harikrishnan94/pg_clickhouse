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

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>

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
#define SHM_IMPL_MAX_COLS  64u
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

StaticAssertDecl(sizeof(ShmHandshake) == 128, "ShmHandshake must be 128 bytes");
StaticAssertDecl(sizeof(ShmSlot) == 64, "ShmSlot must be 64 bytes");
StaticAssertDecl(sizeof(ShmSchemaEntry) == 128, "ShmSchemaEntry must be 128 bytes");
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

    MemoryContext owner_cxt;
    MemoryContextCallback cleanup_cb;
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
        case SHM_WIRE_DATETIME: case SHM_WIRE_DATE32: return 4;
        case SHM_WIRE_INT64: case SHM_WIRE_UINT64: case SHM_WIRE_FLOAT64: return 8;
        case SHM_WIRE_STRING: return 0;
    }
    return 0;
}

static inline size_t
align_up(size_t v, size_t a)
{
    return (v + a - 1) & ~(a - 1);
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
        struct pollfd pfd;

        pfd.fd = p->listen_fd;
        pfd.events = POLLIN;
        pfd.revents = 0;
        if (poll(&pfd, 1, 50) > 0 && (pfd.revents & POLLIN))
            pump_control_socket(p);
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
        pthread_join(p->pump_thread, NULL);
        p->pump_running = false;
    }

    for (i = 0; i < p->n_parked; i++)
        close(p->parked_conns[i]);
    p->n_parked = 0;

    if (p->listen_fd >= 0) { close(p->listen_fd); p->listen_fd = -1; }
    if (p->event_fd >= 0)  { close(p->event_fd);  p->event_fd = -1; }
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

ShmProducer *
shm_producer_create(const char *name,
                    const ShmColumnSchema *schema, int n_columns,
                    uint32_t ring_depth_k, size_t data_region_size,
                    MemoryContext owner_cxt)
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
    p->mapping = NULL;
    p->owner_cxt = owner_cxt;

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

    /* Start the socket-pump thread (serves the readiness eventfd to consumers). */
    p->pump_stop = 0;
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

    /* Wait for the slot to be reusable. The consumer drives PUBLISHED->EMPTY on
     * the last retain drop, so we poll state==EMPTY (never retain_refcount). Pump
     * the control socket while waiting so the consumer can attach and drain. */
    while (__atomic_load_n(&slot->state, __ATOMIC_ACQUIRE) != SHM_STATE_EMPTY)
    {
        /* The pump thread services the control socket; just wait for the
         * consumer to free a slot (honouring query cancel). */
        CHECK_FOR_INTERRUPTS();
        pg_usleep(1000L);       /* 1 ms */
    }

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
            size_t voff = align_up(cursor, 8);     /* 8-aligned => any elem alignment */

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

    p->next_slot++;
    return;

overflow:
    ereport(ERROR, (errmsg("pg_clickhouse: shm per-slot region overflow "
                           "(slot %u capacity %zu bytes too small for block)",
                           slot_pos, p->per_slot_capacity)));
}

void
shm_producer_publish(ShmProducer *p, const ShmColumnPayload *payloads, int n_payloads,
                     size_t row_count)
{
    publish_block(p, payloads, n_payloads, row_count, false);
}

void
shm_producer_signal_eos(ShmProducer *p)
{
    publish_block(p, NULL, p->n_columns, 0, true);
    p->eos_published = true;
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
        CHECK_FOR_INTERRUPTS();
        pg_usleep(1000L);
    }

    producer_cleanup(p);
}
