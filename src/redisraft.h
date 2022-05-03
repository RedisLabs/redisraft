/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020-2021 Redis Ltd.
 *
 * RedisRaft is licensed under the Redis Source Available License (RSAL).
 */

#ifndef REDISRAFT_H
#define REDISRAFT_H

#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <time.h>
#include <netinet/in.h>

#include "hiredis/hiredis.h"
#ifdef HAVE_TLS
#include <openssl/ssl.h>
#include <openssl/err.h>
#include "hiredis/hiredis_ssl.h"
#endif
#include "hiredis/async.h"
#include "redismodule.h"
#include "raft.h"
#include "queue.h"

#include "version.h"

#define UNUSED(x)   ((void) (x))

/* Disable GNU attributes for non-GNU compilers */
#ifndef __GNUC__
    #define __attribute__(a)
#endif

#ifndef MIN
    #define MIN(a,b) (((a)<(b))?(a):(b))
#endif

#ifndef MAX
    #define MAX(a,b) (((a)<(b))?(b):(a))
#endif

/* --------------- Forward declarations -------------- */

struct RaftReq;
struct EntryCache;
struct RedisRaftConfig;
struct Node;
struct Connection;
struct ShardingInfo;
struct ShardGroup;

#define REDIS_RAFT_DATATYPE_NAME     "redisraft"
#define REDIS_RAFT_DATATYPE_ENCVER   1

extern int redis_raft_trace;

extern int redis_raft_loglevel;
extern const char *redis_raft_log_levels[];

extern RedisModuleCtx *redis_raft_log_ctx;

#define LOG_LEVEL_DEBUG   0
#define LOG_LEVEL_VERBOSE 1
#define LOG_LEVEL_NOTICE  2
#define LOG_LEVEL_WARNING 3
#define LOG_LEVEL_COUNT   (LOG_LEVEL_WARNING + 1)

#define TRACE_OFF         0
#define TRACE_NODE        1
#define TRACE_CONN        2
#define TRACE_RAFTLIB     4
#define TRACE_RAFTLOG     8
#define TRACE_GENERIC     16
#define TRACE_ALL         ((TRACE_GENERIC * 2) - 1)

#define LOG(level, fmt, ...)                                                   \
    do {                                                                       \
       if (level >= redis_raft_loglevel) {                                     \
            RedisModule_Log(redis_raft_log_ctx,                                \
                            redis_raft_log_levels[level], fmt, ##__VA_ARGS__); \
       }                                                                       \
    } while(0)

#define LOG_DEBUG(fmt, ...) LOG(LOG_LEVEL_DEBUG, fmt, ##__VA_ARGS__)
#define LOG_VERBOSE(fmt, ...) LOG(LOG_LEVEL_VERBOSE, fmt, ##__VA_ARGS__)
#define LOG_NOTICE(fmt, ...) LOG(LOG_LEVEL_NOTICE, fmt, ##__VA_ARGS__)
#define LOG_WARNING(fmt, ...) LOG(LOG_LEVEL_WARNING, fmt, ##__VA_ARGS__)


#define PANIC(fmt, ...)                                                        \
    do {                                                                       \
        LOG_WARNING("\n\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n"                      \
                    "REDIS RAFT PANIC\n"                                       \
                    "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n"                        \
                    fmt, ##__VA_ARGS__);                                       \
        abort();                                                               \
    } while (0)

#define TRACE_MODULE(MODULE,fmt,...)                                           \
    do {                                                                       \
         if (redis_raft_trace & TRACE_##MODULE) {                              \
            LOG(LOG_LEVEL_DEBUG, fmt, ##__VA_ARGS__);                          \
         }                                                                     \
    } while (0)

#define TRACE(fmt,...) TRACE_MODULE(GENERIC, fmt, ##__VA_ARGS__)

#define NODE_TRACE(node, fmt, ...) \
    TRACE_MODULE(NODE, "<node {%p:%d}> "fmt, (node), (node) ? (node)->id : 0, ##__VA_ARGS__)

#define NODE_LOG(level, node, fmt, ...) \
    LOG(level, "<{node:%d}> " fmt, (node) ? (node)->id : 0, ##__VA_ARGS__)

#define NODE_LOG_DEBUG(node, fmt, ...) NODE_LOG(LOG_LEVEL_DEBUG, node, fmt, ##__VA_ARGS__)
#define NODE_LOG_VERBOSE(node, fmt, ...) NODE_LOG(LOG_LEVEL_VERBOSE, node, fmt, ##__VA_ARGS__)
#define NODE_LOG_NOTICE(node, fmt, ...) NODE_LOG(LOG_LEVEL_NOTICE, node, fmt, ##__VA_ARGS__)
#define NODE_LOG_WARNING(node, fmt, ...) NODE_LOG(LOG_LEVEL_WARNING, node, fmt, ##__VA_ARGS__)

/* -------------------- Connections -------------------- */

/* Longest length of a NodeAddr string, including null terminator */
#define NODEADDR_MAXLEN      (255 + 1 + 5 + 1)

/* Node address specifier. */
typedef struct node_addr {
    uint16_t port;
    char host[256];             /* Hostname or IP address */
} NodeAddr;

/* A singly linked list of NodeAddr elements */
typedef struct NodeAddrListElement {
    NodeAddr addr;
    struct NodeAddrListElement *next;
} NodeAddrListElement;

typedef enum ConnState {
    CONN_DISCONNECTED,
    CONN_RESOLVING,
    CONN_CONNECTING,
    CONN_CONNECTED,
    CONN_CONNECT_ERROR
} ConnState;

typedef void (*ConnectionCallbackFunc)(struct Connection *conn);
typedef void (*ConnectionFreeFunc)(void *privdata);

/* Connection flags for Connection.flags */
#define CONN_TERMINATING    (1 << 0)

/* A connection represents a single outgoing Redis connection, such as the
 * one used to communicate with another node.
 *
 * Essentially it is a wrapper around a hiredis asyncRedisContext, providing
 * additional capabilities such as handling asynchronous DNS resolution,
 * dropped connections and re-connects, etc.
 */

typedef struct Connection {
    unsigned long id;                   /* Unique connection ID */
    ConnState state;
    unsigned int flags;
    NodeAddr addr;                      /* Address of last ConnConnect() */
    char ipaddr[INET6_ADDRSTRLEN+1];    /* Resolved IP address */
    redisAsyncContext *rc;              /* hiredis async context */
    struct RedisRaftCtx *rr;            /* Pointer back to redis_raft */
    long long last_connected_time;      /* Last connection time */
    unsigned long int connect_oks;      /* Successful connects */
    unsigned long int connect_errors;   /* Connection errors since last connection */
    struct timeval timeout;             /* Timeout to use if not null */
    void *privdata;                     /* User provided pointer */

    struct AddrinfoResult {
        int rc;
        struct addrinfo *addr;
    } addrinfo_result;

    /* Connect callback is guaranteed after ConnConnect(); Callback should check
     * connection state as it will also be called on error.
     */
    ConnectionCallbackFunc connect_callback;

    /* Idle callback is called periodically for every connection that is in idle
     * state, i.e. CONN_DISCONNECTED or CONN_CONNECT_ERROR.
     *
     * Typically it is used to either (re-)establish the connection using ConnConnect()
     * or destroy the connection.
     */
    ConnectionCallbackFunc idle_callback;

    /* Free callback is called when the connection gets freed, and as a last chance
     * to free privdata.
     */
    ConnectionFreeFunc free_callback;

    /* Linkage to global connections list */
    LIST_ENTRY(Connection) entries;
} Connection;

/* -------------------- Global Raft Context -------------------- */

/* General state of the module */
typedef enum RedisRaftState {
    REDIS_RAFT_UNINITIALIZED,       /* Waiting for RAFT.CLUSTER command */
    REDIS_RAFT_UP,                  /* Up and running */
    REDIS_RAFT_LOADING,             /* Loading (or attempting) RDB/Raft Log on startup */
    REDIS_RAFT_JOINING              /* Processing a RAFT.CLUSTER JOIN command */
} RedisRaftState;

/* A node configuration entry that describes the known configuration of a specific
 * node at the time of snapshot.
 */
typedef struct SnapshotCfgEntry {
    raft_node_id_t  id;
    int             voting;
    NodeAddr        addr;
    struct SnapshotCfgEntry *next;
} SnapshotCfgEntry;

typedef struct NodeIdEntry {
    raft_node_id_t id;
    struct NodeIdEntry *next;
} NodeIdEntry;

#define RAFT_DBID_LEN               32
#define RAFT_SHARDGROUP_NODEID_LEN  40  /* Combined DBID_LEN + 32-bit node id */

/* Snapshot metadata.  There is a single instance of this struct available at all times,
 * which is accessed as follows:
 * 1. During cluster setup, it is initialized (e.g. with a unique dbid).
 * 2. The last applied term and index fields are updated every time we apply a log entry
 *    into the dataset, to reflect the real-time state of the snapshot.
 * 3. On rdbsave, the record gets serialized (using a dummy key for now; TODO use a global
 *    state mechanism when Redis Module API supports it).
 * 4. On rdbload, the record gets loaded and the loaded flag is set.
 */
typedef struct RaftSnapshotInfo {
    bool loaded;
    char dbid[RAFT_DBID_LEN+1];
    raft_term_t last_applied_term;
    raft_index_t last_applied_idx;
    SnapshotCfgEntry *cfg;
    NodeIdEntry *used_node_ids;  /* All node ids that are, or have ever been, part of this cluster */
} RaftSnapshotInfo;

typedef struct SnapshotFile {
    void *mmap;
    size_t len;
} SnapshotFile;

typedef struct RaftMeta {
    raft_term_t term;                           /* Last term we're aware of */
    raft_node_id_t vote;                        /* Our vote in the last term, or -1 */
} RaftMeta;

/* threadpool.c */
struct Task {
    STAILQ_ENTRY(Task) entry;
    void *arg;
    void (*run)(void *arg);
};

typedef struct ThreadPool {
    int thread_count;
    pthread_t *threads;

    STAILQ_HEAD(tasks, Task) tasks;

    pthread_mutex_t mtx;
    pthread_cond_t cond;
    int shutdown;
} ThreadPool;

void threadPoolInit(ThreadPool *pool, int thread_count);
void threadPoolAdd(ThreadPool *pool, void *arg, void (*run)(void *arg));
void threadPoolShutdown(ThreadPool *pool);

typedef struct FsyncThreadResult {
    raft_index_t fsync_index;
    uint64_t time;
} FsyncThreadResult;

/* fsync.c */
typedef struct FsyncThread
{
    pthread_t id;
    pthread_cond_t cond;
    pthread_mutex_t mtx;

    bool need_fsync;
    bool running;

    int fd;
    raft_index_t requested_index;

    void (*on_complete)(void *result);

} FsyncThread;

void fsyncThreadStart(FsyncThread *th, void (*on_complete)(void *result));
void fsyncThreadAddTask(FsyncThread *th, int fd, raft_index_t requested_index);
void fsyncThreadWaitUntilCompleted(FsyncThread *th);


/* Global Raft context */
typedef struct RedisRaftCtx {
    void *raft;                                  /* Raft library context */
    RedisModuleCtx *ctx;                         /* Redis module thread-safe context; only used to push
                                                    commands we get from the leader. */
    RedisRaftState state;                        /* Raft module state */
    ThreadPool thread_pool;                      /* Thread pool for slow operations */
    FsyncThread fsyncThread;                     /* Thread to call fsync on raft log file */
    struct RaftLog *log;                         /* Raft persistent log; May be NULL if not used */
    struct RaftMeta meta;                        /* Raft metadata for voted_for and term */
    struct EntryCache *logcache;                 /* Log entry cache to keep entries in memory for faster access */
    struct RedisRaftConfig *config;              /* User provided configuration */
    bool snapshot_in_progress;                   /* Indicates we're creating a snapshot in the background */
    raft_index_t incoming_snapshot_idx;          /* Incoming snapshot's last included idx to verify chunks
                                                    belong to the same snapshot */
    char incoming_snapshot_file[256];            /* File name for incoming snapshots. When received fully,
                                                    it will be renamed to the original rdb file */
    raft_index_t last_snapshot_idx;              /* Last included idx of the snapshot operation currently in progress */
    raft_term_t last_snapshot_term;              /* Last included term of the snapshot operation currently in progress */
    int snapshot_child_fd;                       /* Pipe connected to snapshot child process */
    SnapshotFile outgoing_snapshot_file;         /* Snapshot file memory map to send to followers */
    RaftSnapshotInfo snapshot_info;              /* Current snapshot info */
    struct RaftReq *debug_req;                   /* Current RAFT.DEBUG request context, if processing one */
    struct RaftReq *transfer_req;                /* RaftReq if a leader transfer is in progress */
    RedisModuleCommandFilter *registered_filter; /* Command filter is used for intercepting redis commands */
    struct ShardingInfo *sharding_info;          /* Information about sharding, when cluster mode is enabled */
    RedisModuleDict *multi_client_state;         /* A dict that tracks multi state of the clients */

    /* General stats */
    unsigned long client_attached_entries;       /* Number of log entries attached to user connections */
    unsigned long long proxy_reqs;               /* Number of proxied requests */
    unsigned long long proxy_failed_reqs;        /* Number of failed proxy requests, i.e. did not send */
    unsigned long long proxy_failed_responses;   /* Number of failed proxy responses, i.e. did not complete */
    unsigned long proxy_outstanding_reqs;        /* Number of proxied requests pending */
    unsigned long snapshots_loaded;              /* Number of snapshots loaded */
    unsigned long snapshots_created;             /* Number of snapshots created */
    char *resp_call_fmt;                         /* Format string to use in RedisModule_Call(), Redis version-specific */
    int entered_eval;                            /* handling a lua script */
#ifdef HAVE_TLS
    SSL_CTX *ssl;                                /* OpenSSL context for use by hiredis */
#endif

} RedisRaftCtx;

extern RedisRaftCtx redis_raft;

extern raft_log_impl_t RaftLogImpl;

#define REDIS_RAFT_DEFAULT_LOG_FILENAME               "redisraft.db"
#define REDIS_RAFT_DEFAULT_INTERVAL                   100  /* milliseconds */
#define REDIS_RAFT_DEFAULT_REQUEST_TIMEOUT            200  /* milliseconds */
#define REDIS_RAFT_DEFAULT_ELECTION_TIMEOUT           1000 /* usec */
#define REDIS_RAFT_DEFAULT_CONNECTION_TIMEOUT         3000 /* usec */
#define REDIS_RAFT_DEFAULT_JOIN_TIMEOUT               120  /* seconds */
#define REDIS_RAFT_DEFAULT_RECONNECT_INTERVAL         100
#define REDIS_RAFT_DEFAULT_PROXY_RESPONSE_TIMEOUT     10000
#define REDIS_RAFT_DEFAULT_RAFT_RESPONSE_TIMEOUT      1000
#define REDIS_RAFT_DEFAULT_LOG_MAX_CACHE_SIZE         (8*1000*1000)
#define REDIS_RAFT_DEFAULT_LOG_MAX_FILE_SIZE          (64*1000*1000)
#define REDIS_RAFT_HASH_SLOTS                         16384
#define REDIS_RAFT_HASH_MIN_SLOT                      0
#define REDIS_RAFT_HASH_MAX_SLOT                      16383
#define REDIS_RAFT_DEFAULT_SHARDGROUP_UPDATE_INTERVAL 5000
#define REDIS_RAFT_DEFAULT_MAX_APPENDENTRIES          4

static inline bool HashSlotValid(long slot)
{
    return (slot >= REDIS_RAFT_HASH_MIN_SLOT && slot <= REDIS_RAFT_HASH_MAX_SLOT);
}

static inline bool HashSlotRangeValid(long start_slot, long end_slot)
{
    return (HashSlotValid(start_slot) && HashSlotValid(end_slot) &&
            start_slot <= end_slot);
}

typedef struct RedisRaftConfig {
    raft_node_id_t id;          /* Local node Id */
    NodeAddr addr;              /* Address of local node, if specified */
    char *rdb_filename;         /* Original Redis dbfilename */
    char *raft_log_filename;    /* Raft log file name, derived from dbfilename */
    bool follower_proxy;        /* Do follower nodes proxy requests to leader? */
    bool quorum_reads;          /* Reads have to go through quorum */
    /* Tuning */
    int raft_interval;
    int request_timeout;
    int election_timeout;
    int connection_timeout;
    int join_timeout;
    int reconnect_interval;
    int proxy_response_timeout;
    int raft_response_timeout;
    int max_appendentries_inflight;
    /* Cache and file compaction */
    unsigned long raft_log_max_cache_size;
    unsigned long raft_log_max_file_size;
    bool raft_log_fsync;
    /* Cluster mode */
    bool sharding;                      /* Are we running in a sharding configuration? */
    char *slot_config;                  /* Defining multiple slot ranges (# or #:#) that are delimited by ',' */
    int shardgroup_update_interval;     /* Milliseconds between shardgroup updates */
    char *ignored_commands;             /* Comma delimited list of commands that should not be intercepted */
    int external_sharding;              /* use external sharding orchestrator only */
    bool tls_enabled;                   /* use TLS for all inter cluster communication */
    char *tls_ca_cert;
    char *tls_cert;
    char *tls_key;
    char *tls_key_pass;
    char *cluster_user;                 /* acl user to use for internode communication */
    char *cluster_password;             /* password used for internode communication */
} RedisRaftConfig;

typedef struct PendingResponse {
    bool proxy;
    int id;
    long long request_time;
    STAILQ_ENTRY(PendingResponse) entries;
} PendingResponse;

/* Maintains all state about peer nodes */
typedef struct Node {
    raft_node_id_t id;              /* Raft unique node ID */
    RedisRaftCtx *rr;               /* RedisRaftCtx handle */
    Connection *conn;               /* Connection to node */
    NodeAddr addr;                  /* Node's address */
    long pending_raft_response_num;     /* Number of pending Raft responses */
    long pending_proxy_response_num;    /* Number of pending proxy responses */
    STAILQ_HEAD(pending_responses, PendingResponse) pending_responses;
    LIST_ENTRY(Node) entries;
} Node;

/* General purpose status code.  Convention is this:
 * In redisraft.c (Redis Module wrapper) we generally use REDISMODULE_OK/REDISMODULE_ERR.
 * Elsewhere we stick to it.
 */
typedef enum RRStatus {
    RR_OK       = 0,
    RR_ERROR
} RRStatus;

/* Request types. */
enum RaftReqType {
    RR_GENERIC = 1,
    RR_CLUSTER_JOIN,
    RR_CFGCHANGE_ADDNODE,
    RR_CFGCHANGE_REMOVENODE,
    RR_REDISCOMMAND,
    RR_DEBUG,
    RR_SHARDGROUP_ADD,
    RR_SHARDGROUPS_REPLACE,
    RR_SHARDGROUP_LINK,
    RR_TRANSFER_LEADER,
    RR_RAFTREQ_MAX
};

extern const char *RaftReqTypeStr[];

typedef struct {
    raft_node_id_t id;
    NodeAddr addr;
} RaftCfgChange;

typedef struct {
    int argc;
    RedisModuleString **argv;
} RaftRedisCommand;

typedef struct {
    int size;           /* Size of allocated array */
    int len;            /* Number of elements in array */
    RaftRedisCommand **commands;
} RaftRedisCommandArray;

/* Max length of a ShardGroupNode string, including newline and null terminator */
#define SHARDGROUPNODE_MAXLEN   (RAFT_SHARDGROUP_NODEID_LEN+1 + NODEADDR_MAXLEN + 2)

/* Describes a node in a ShardGroup (foreign RedisRaft cluster). */
typedef struct ShardGroupNode {
    char node_id[RAFT_SHARDGROUP_NODEID_LEN+1]; /* Combined dbid + node_id */
    NodeAddr addr;                              /* Node address and port */
} ShardGroupNode;

/* Max length of a ShardGroup string, including newline and null terminator
 * but excluding nodes and shard groups */
#define SHARDGROUP_MAXLEN       (10 + 1 + 10 + 1 + 1)

enum SlotRangeType {
    SLOTRANGE_TYPE_UNDEF = 0,
    SLOTRANGE_TYPE_STABLE,
    SLOTRANGE_TYPE_IMPORTING,
    SLOTRANGE_TYPE_MIGRATING,
    SLOTRANGE_TYPE_MAX
};

static inline bool SlotRangeTypeValid(enum SlotRangeType val) {
    return (val > SLOTRANGE_TYPE_UNDEF && val < SLOTRANGE_TYPE_MAX);
}

#define SLOT_RANGE_MAXLEN (10 + 1 + 10 + 1 + 10 + 1 + 1)

typedef struct ShardGroupSlotRange {
    unsigned int start_slot; /* First slot, inclusive */
    unsigned int end_slot;   /* Last slot, inclusive */
    enum SlotRangeType type; /* type of slot range, normal, importing, exporting */
} ShardGroupSlotRange;

/* Describes a ShardGroup. A ShardGroup is a RedisRaft cluster that
 * is assigned with a specific range of hash slots.
 */
typedef struct ShardGroup {
    /* Configuration */
    char id[RAFT_DBID_LEN+1];            /* Local shardgroup identifier */
    unsigned int slot_ranges_num;        /* Number of slot ranges */
    ShardGroupSlotRange *slot_ranges;    /* individual slot ranges */
    unsigned int nodes_num;              /* Number of nodes listed */
    ShardGroupNode *nodes;               /* Nodes array */

    /* Runtime state */
    unsigned int next_redir;             /* Round-robin -MOVED index */

    /* Synchronization state */
    unsigned int node_conn_idx;          /* Next node to connect to, when looking for a live one */
    NodeAddr conn_addr;                  /* Address to use on next connect, if use_conn_addr is set */
    bool use_conn_addr;                  /* Should we use conn_addr? Otherwise iterate node_conn_idx? */
    Connection *conn;                    /* Connection we use */
    long long last_updated;              /* Last time of successful update (mstime) */
    bool update_in_progress;             /* Are we currently updating? */
    bool local;                          /* ShardGroup struct that corresponds to local cluster */
} ShardGroup;

#define RAFT_LOGTYPE_ADD_SHARDGROUP      (RAFT_LOGTYPE_NUM+1)
#define RAFT_LOGTYPE_UPDATE_SHARDGROUP   (RAFT_LOGTYPE_NUM+2)
#define RAFT_LOGTYPE_REPLACE_SHARDGROUPS (RAFT_LOGTYPE_NUM+3)

/* Sharding information, used when cluster_mode is enabled and multiple
 * RedisRaft clusters operate together to perform sharding.
 */
typedef struct ShardingInfo {
    unsigned int shard_groups_num;       /* Number of shard groups */
    RedisModuleDict *shard_group_map;    /* shard group id -> x in shard_groups[x] */

    /* Maps hash slots to ShardGroups indexes.
     *
     * Note that a one-based index into the shard_groups array is used,
     * since a zero value indicates the slot is unassigned. The index
     * should therefore be adjusted before refering the array.
     */
    ShardGroup *stable_slots_map[REDIS_RAFT_HASH_SLOTS];
    ShardGroup *importing_slots_map[REDIS_RAFT_HASH_SLOTS];
    ShardGroup *migrating_slots_map[REDIS_RAFT_HASH_SLOTS];
} ShardingInfo;

typedef struct RaftReq {
    int type;
    RedisModuleBlockedClient *client;
    RedisModuleCtx *ctx;
    union {
        struct {
            Node *proxy_node;
            int hash_slot;
            RaftRedisCommandArray cmds;
            raft_entry_resp_t response;
        } redis;
        struct {
            int fail;
            int delay;
        } debug;
    } r;
} RaftReq;

#define RAFTLOG_VERSION     1

/* Flags for RaftLogOpen */
#define RAFTLOG_KEEP_INDEX  1                   /* Index was written by this process, safe to use. */

typedef struct RaftLog {
    uint32_t            version;                /* Log file format version */
    char                dbid[RAFT_DBID_LEN+1];  /* DB unique ID */
    raft_node_id_t      node_id;                /* Node ID */
    raft_index_t        num_entries;            /* Entries in log */
    raft_term_t         snapshot_last_term;     /* Last term included in snapshot */
    raft_index_t        snapshot_last_idx;      /* Last index included in snapshot */
    raft_index_t        index;                  /* Index of last entry */
    size_t              file_size;              /* File size at the time of last write */
    const char          *filename;
    FILE                *file;
    FILE                *idxfile;
    off_t               idxoffset;              /* Index file position */
    uint64_t            fsync_count;            /* Count of fsync() calls */
    uint64_t            fsync_max;              /* Slowest fsync() call in microseconds */
    uint64_t            fsync_total;            /* Total time fsync() calls consumed in microseconds */
} RaftLog;


#define SNAPSHOT_RESULT_MAGIC    0x70616e73  /* "snap" */
typedef struct SnapshotResult {
    int magic;
    int success;
    char rdb_filename[256];
    char err[256];
} SnapshotResult;

/* Entry type for the internal command table used by RedisRaft,
 * used to determine how different intercepted Redis commands are
 * handled.
 */
typedef struct {
    char *name;                 /* Command name */
    unsigned int flags;         /* Command flags, see CMD_SPEC_* */
} CommandSpec;

#define CMD_SPEC_READONLY       (1<<1)      /* Command is a read-only command */
#define CMD_SPEC_WRITE          (1<<2)      /* Command is a (potentially) write command */
#define CMD_SPEC_UNSUPPORTED    (1<<3)      /* Command is not supported, should be rejected */
#define CMD_SPEC_DONT_INTERCEPT (1<<4)      /* Command should not be intercepted to RAFT */
#define CMD_SPEC_SORT_REPLY     (1<<5)      /* Command output should be sorted within a lua script */
#define CMD_SPEC_RANDOM         (1<<6)      /* Commands that are always random */

/* Command filtering re-entrancy counter handling.
 *
 * This mechanism tracks calls from Redis Raft into Redis and used by the
 * command filtering hook to avoid raftizing commands as they're pushed from the log
 * to the FSM.
 *
 * Redis Module API provides the REDISMODULE_CMDFILTER_NOSELF flag which does
 * the same thing, but does not apply to executions from a thread safe context.
 *
 * This must wrap every call to RedisModule_Call(), after the Redis lock has been
 * acquired, and unless the called command is known to be excluded from raftizing.
 */

extern int redis_raft_in_rm_call;   /* defined in common.c */

static inline void enterRedisModuleCall(void) {
    redis_raft_in_rm_call++;
}

static inline void exitRedisModuleCall(void) {
    redis_raft_in_rm_call--;
}

static inline int checkInRedisModuleCall(void) {
    return redis_raft_in_rm_call;
}

typedef struct JoinLinkState {
    NodeAddrListElement *addr;
    NodeAddrListElement *addr_iter;
    Connection *conn;
    time_t start;                       /* Time we initiated the join, to enable it to fail if it takes too long */
    RaftReq *req;                       /* Original RaftReq, so we can return a reply */
    bool failed;                        /* unrecoverable failure */
    const char *type;                   /* error message to print if exhaust time */
    bool started;                       /* we have started connecting */
    ConnectionCallbackFunc connect_callback;
    ConnectionCallbackFunc fail_callback;
    void (*complete_callback)(RaftReq *req);
} JoinLinkState;

/* common.c */
void joinLinkIdleCallback(Connection *conn);
void joinLinkFreeCallback(void *privdata);
const char *getStateStr(RedisRaftCtx *rr);
void replyRaftError(RedisModuleCtx *ctx, int error);
raft_node_t *getLeaderNodeOrReply(RedisRaftCtx *rr, RedisModuleCtx *ctx);
RRStatus checkLeader(RedisRaftCtx *rr, RedisModuleCtx *ctx, Node **ret_leader);
RRStatus checkRaftNotLoading(RedisRaftCtx *rr, RedisModuleCtx *ctx);
RRStatus checkRaftUninitialized(RedisRaftCtx *rr, RedisModuleCtx *ctx);
RRStatus checkRaftState(RedisRaftCtx *rr, RedisModuleCtx *ctx);
void replyRedirect(RedisModuleCtx *ctx, int slot, NodeAddr *addr);
bool parseMovedReply(const char *str, NodeAddr *addr);

/* node_addr.c */
bool NodeAddrParse(const char *node_addr, size_t node_addr_len, NodeAddr *result);
bool NodeAddrEqual(const NodeAddr *a1, const NodeAddr *a2);
void NodeAddrListAddElement(NodeAddrListElement **head, const NodeAddr *addr);
void NodeAddrListConcat(NodeAddrListElement **head, const NodeAddrListElement *other);
void NodeAddrListFree(NodeAddrListElement *head);

/* node.c */
Node *NodeCreate(RedisRaftCtx *rr, int id, const NodeAddr *addr);
void HandleNodeStates(RedisRaftCtx *rr);
void NodeAddPendingResponse(Node *node, bool proxy);
void NodeDismissPendingResponse(Node *node);

/* serialization.c */
raft_entry_t *RaftRedisCommandArraySerialize(const RaftRedisCommandArray *source);
size_t RaftRedisCommandDeserialize(RaftRedisCommand *target, const void *buf, size_t buf_size);
RRStatus RaftRedisCommandArrayDeserialize(RaftRedisCommandArray *target, const void *buf, size_t buf_size);
void RaftRedisCommandArrayFree(RaftRedisCommandArray *array);
void RaftRedisCommandFree(RaftRedisCommand *r);
RaftRedisCommand *RaftRedisCommandArrayExtend(RaftRedisCommandArray *target);
void RaftRedisCommandArrayMove(RaftRedisCommandArray *target, RaftRedisCommandArray *source);

/* raft.c */
RRStatus RedisRaftInit(RedisModuleCtx *ctx, RedisRaftCtx *rr, RedisRaftConfig *config);
void RaftReqFree(RaftReq *req);
RaftReq *RaftReqInit(RedisModuleCtx *ctx, enum RaftReqType type);
void RaftLibraryInit(RedisRaftCtx *rr, bool cluster_init);
void RaftExecuteCommandArray(RedisModuleCtx *ctx, RedisModuleCtx *reply_ctx, RaftRedisCommandArray *array);
void addUsedNodeId(RedisRaftCtx *rr, raft_node_id_t node_id);
raft_node_id_t makeRandomNodeId(RedisRaftCtx *rr);
void entryAttachRaftReq(RedisRaftCtx *rr, raft_entry_t *entry, RaftReq *req);
RaftReq *entryDetachRaftReq(RedisRaftCtx *rr, raft_entry_t *entry);
void shutdownAfterRemoval(RedisRaftCtx *rr);
bool hasNodeIdBeenUsed(RedisRaftCtx *rr, raft_node_id_t node_id);
void callRaftPeriodic(RedisModuleCtx *ctx, void *arg);
void callHandleNodeStates(RedisModuleCtx *ctx, void *arg);
void handleBeforeSleep(RedisRaftCtx *rr);
void handleFsyncCompleted(void *arg);

/* util.c */
int RedisModuleStringToInt(RedisModuleString *str, int *value);
char *catsnprintf(char *strbuf, size_t *strbuf_len, const char *fmt, ...)  __attribute__((format(printf, 3, 4)));

char *StrCreate(const void *buf, size_t len);
char *StrCreateFromString(RedisModuleString *str);
int stringmatchlen(const char *pattern, int patternLen, const char *string, int stringLen, int nocase);
int stringmatch(const char *pattern, const char *string, int nocase);
RRStatus parseMemorySize(const char *value, unsigned long *result);
RRStatus formatExactMemorySize(unsigned long value, char *buf, size_t buf_size);
void handleRMCallError(RedisModuleCtx *reply_ctx, int ret_errno, const char *cmd, size_t cmdlen);
void AddBasicLocalShardGroup(RedisRaftCtx *rr);

/* log.c */
RaftLog *RaftLogCreate(const char *filename, const char *dbid, raft_term_t snapshot_term, raft_index_t snapshot_index, RedisRaftConfig *config);
RaftLog *RaftLogOpen(const char *filename, RedisRaftConfig *config, int flags);
void RaftLogClose(RaftLog *log);
RRStatus RaftLogAppend(RaftLog *log, raft_entry_t *entry);
int RaftLogLoadEntries(RaftLog *log, int (*callback)(void *, raft_entry_t *, raft_index_t), void *callback_arg);
RRStatus RaftLogWriteEntry(RaftLog *log, raft_entry_t *entry);
RRStatus RaftLogSync(RaftLog *log, bool sync);
raft_entry_t *RaftLogGet(RaftLog *log, raft_index_t idx);
RRStatus RaftLogDelete(RaftLog *log, raft_index_t from_idx, raft_entry_notify_f cb, void *cb_arg);
RRStatus RaftLogReset(RaftLog *log, raft_index_t index, raft_term_t term);
raft_index_t RaftLogCount(RaftLog *log);
raft_index_t RaftLogFirstIdx(RaftLog *log);
raft_index_t RaftLogCurrentIdx(RaftLog *log);
long long int RaftLogRewrite(RedisRaftCtx *rr, const char *filename, raft_index_t last_idx, raft_term_t last_term);
void RaftLogRemoveFiles(const char *filename);
void RaftLogArchiveFiles(RedisRaftCtx *rr);
RRStatus RaftLogRewriteSwitch(RedisRaftCtx *rr, RaftLog *new_log, unsigned long new_log_entries);
int RaftMetaRead(RaftMeta *meta, const char* filename);
int RaftMetaWrite(RaftMeta *meta, const char* filename, raft_term_t term, raft_node_id_t vote);

typedef struct EntryCache {
    raft_index_t size;                  /* Size of ptrs */
    raft_index_t len;                   /* Number of entries in cache */
    raft_index_t start_idx;             /* Log index of first entry */
    raft_index_t start;                 /* ptrs array index of first entry */
    unsigned long int entries_memsize;  /* Total memory used by entries */
    raft_entry_t **ptrs;
} EntryCache;

EntryCache *EntryCacheNew(raft_index_t initial_size);
void EntryCacheFree(EntryCache *cache);
void EntryCacheAppend(EntryCache *cache, raft_entry_t *ety, raft_index_t idx);
raft_entry_t *EntryCacheGet(EntryCache *cache, raft_index_t idx);
long EntryCacheDeleteHead(EntryCache *cache, raft_index_t idx);
long EntryCacheDeleteTail(EntryCache *cache, raft_index_t index);
long EntryCacheCompact(EntryCache *cache, size_t max_memory);

/* config.c */
void ConfigInit(RedisModuleCtx *ctx, RedisRaftConfig *config);
RRStatus ConfigParseArgs(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, RedisRaftConfig *target);
void ConfigSet(RedisRaftCtx *rr, RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
void ConfigGet(RedisRaftCtx *rr, RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
RRStatus ConfigReadFromRedis(RedisRaftCtx *rr);
RRStatus ConfigureRedis(RedisModuleCtx *ctx);
void updateTLSConfig(RedisModuleCtx *ctx, RedisRaftConfig *config);

/* snapshot.c */
extern RedisModuleTypeMethods RedisRaftTypeMethods;
extern RedisModuleType *RedisRaftType;
void initSnapshotTransferData(RedisRaftCtx *ctx);
void createOutgoingSnapshotMmap(RedisRaftCtx *ctx);
RRStatus initiateSnapshot(RedisRaftCtx *rr);
RRStatus finalizeSnapshot(RedisRaftCtx *rr, SnapshotResult *sr);
void cancelSnapshot(RedisRaftCtx *rr, SnapshotResult *sr);
int pollSnapshotStatus(RedisRaftCtx *rr, SnapshotResult *sr);
void configRaftFromSnapshotInfo(RedisRaftCtx *rr);
int raftLoadSnapshot(raft_server_t *raft, void *udata, raft_index_t idx, raft_term_t term);
int raftSendSnapshot(raft_server_t *raft, void *udata, raft_node_t *node, raft_snapshot_req_t *msg);
int raftClearSnapshot(raft_server_t *raft, void *udata);
int raftGetSnapshotChunk(raft_server_t *raft, void *udata, raft_node_t *node, raft_size_t offset, raft_snapshot_chunk_t *chunk);
int raftStoreSnapshotChunk(raft_server_t *raft, void *udata, raft_index_t idx, raft_size_t offset, raft_snapshot_chunk_t *chunk);
void archiveSnapshot(RedisRaftCtx *rr);

/* proxy.c */
RRStatus ProxyCommand(RedisRaftCtx *rr, RedisModuleCtx *ctx, RaftRedisCommandArray *cmds, Node *leader);

/* connection.c */
Connection *ConnCreate(RedisRaftCtx *rr, void *privdata, ConnectionCallbackFunc idle_cb, ConnectionFreeFunc free_cb);
RRStatus ConnConnect(Connection *conn, const NodeAddr *addr, ConnectionCallbackFunc connect_callback);
void ConnAsyncTerminate(Connection *conn);
void ConnMarkDisconnected(Connection *conn);
void HandleIdleConnections(RedisRaftCtx *rr);
void *ConnGetPrivateData(Connection *conn);
RedisRaftCtx *ConnGetRedisRaftCtx(Connection *conn);
redisAsyncContext *ConnGetRedisCtx(Connection *conn);
bool ConnIsIdle(Connection *conn);
bool ConnIsConnected(Connection *conn);
const char *ConnGetStateStr(Connection *conn);

/* cluster.c */
char *ShardGroupSerialize(ShardGroup *sg);
ShardGroup *ShardGroupDeserialize(const char *buf, size_t buf_len);
ShardGroup *ShardGroupCreate();
void ShardGroupFree(ShardGroup *sg);
void ShardGroupTerm(ShardGroup *sg);
ShardGroup *ShardGroupParse(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int base_argv_idx, int *num_elems);
ShardGroup **ShardGroupsParse(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int *len);
RRStatus computeHashSlotOrReplyError(RedisRaftCtx *rr, RedisModuleCtx *ctx, RaftRedisCommandArray *cmds, int *slot);
void ShardingHandleClusterCommand(RedisRaftCtx *rr, RedisModuleCtx *ctx, RaftRedisCommand *cmd);
void ShardingInfoInit(RedisRaftCtx *rr);
void ShardingInfoReset(RedisRaftCtx *rr);
RRStatus ShardingInfoValidateShardGroup(RedisRaftCtx *rr, ShardGroup *new_sg);
RRStatus ShardingInfoAddShardGroup(RedisRaftCtx *rr, ShardGroup *new_sg);
RRStatus ShardingInfoUpdateShardGroup(RedisRaftCtx *rr, ShardGroup *new_sg);
void ShardingInfoRDBSave(RedisModuleIO *rdb);
void ShardingInfoRDBLoad(RedisModuleIO *rdb);
void ShardingPeriodicCall(RedisRaftCtx *rr);
RRStatus ShardGroupAppendLogEntry(RedisRaftCtx *rr, ShardGroup *sg, int type, void *user_data);
RRStatus ShardGroupsAppendLogEntry(RedisRaftCtx *rr, int num_sg, ShardGroup **sg, int type, void *user_data);
void ShardGroupLink(RedisRaftCtx *rr, RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
void ShardGroupGet(RedisRaftCtx *rr, RedisModuleCtx *ctx);
void ShardGroupAdd(RedisRaftCtx *rr, RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
void ShardGroupReplace(RedisRaftCtx *rr, RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

/* join.c */
void JoinCluster(RedisRaftCtx *rr, NodeAddrListElement *el, RaftReq *req, void (*complete_callback)(RaftReq *req));

/* commands.c */
RRStatus CommandSpecInit(RedisModuleCtx *ctx, RedisRaftConfig *config);
unsigned int CommandSpecGetAggregateFlags(RaftRedisCommandArray *array, unsigned int default_flags);
const CommandSpec *CommandSpecGet(const RedisModuleString *cmd);

/* sort.c */
void handleSort(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

/* multi.c */
void MultiInitClientState(RedisRaftCtx *rr);
uint64_t MultiClientStateCount(RedisRaftCtx *rr);
void MultiFreeClientState(RedisRaftCtx *rr, unsigned long long client_id);
bool MultiHandleCommand(RedisRaftCtx *rr, RedisModuleCtx *ctx, RaftRedisCommandArray *cmds);
#endif  /* _REDISRAFT_H */
