#ifndef REDISRAFT_CONFIG_H
#define REDISRAFT_CONFIG_H

#include "node.h"
#include "raft.h"
#include "redismodule.h"

typedef struct RedisRaftConfig {
    RedisModuleString *str_conf_ref; /* Reference of the last string config that we pass to Redis module API */

    raft_node_id_t id;       /* Local node Id */
    NodeAddr addr;           /* Address of local node, if specified */
    char *snapshot_filename; /* Raft snapshot filename */
    char *log_filename;      /* Raft log file name, derived from dbfilename */

    /* Tuning */
    int periodic_interval;            /* raft_periodic() interval */
    int request_timeout;              /* Milliseconds before sending a heartbeat message to the followers */
    int election_timeout;             /* Milliseconds before starting an election if there is no leader */
    int connection_timeout;           /* Milliseconds the node will continue to try connecting to another node */
    int join_timeout;                 /* Milliseconds the node will continue to try joining a cluster */
    int reconnect_interval;           /* Milliseconds to wait to reconnect to a node if connection drops */
    int response_timeout;             /* Milliseconds to wait for a response to a Raft message */
    long long append_req_max_count;   /* Max in-flight appendreq message count between two nodes. */
    long long append_req_max_size;    /* Max appendreq message size in bytes. Just an approximation. */
    long long snapshot_req_max_count; /* Max in-flight snapshotreq message count between two nodes. */
    long long snapshot_req_max_size;  /* Max snapshotreq message size in bytes. Just an approximation. */

    /* Cache and file compaction */
    unsigned long log_max_cache_size; /* The memory limit for the in-memory Raft log cache */
    unsigned long log_max_file_size;  /* The maximum desired Raft log file size in bytes */

} RedisRaftConfig;

int ConfigInit(RedisRaftConfig *config, RedisModuleCtx *ctx);
void ConfigFree(RedisRaftConfig *config);

#endif
