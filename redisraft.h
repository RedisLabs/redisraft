#ifndef _REDISRAFT_H
#define _REDISRAFT_H

#include <stdint.h>
#include <stdbool.h>
#include <sys/queue.h>

#include "uv.h"
#include "hiredis/hiredis.h"
#include "hiredis/async.h"
#include "redismodule.h"
#include "raft.h"

#define NODE_CONNECTING     0
#define NODE_CONNECTED      1

#define LOG(fmt, ...) \
    fprintf(stderr, "<<redis-raft>> " fmt, ##__VA_ARGS__)
#define LOG_NODE(node, fmt, ...) \
    LOG("node:%u: " fmt, (node)->id, ##__VA_ARGS__)


typedef struct {
    void *raft;                 /* Raft library context */
    RedisModuleCtx *ctx;        /* Redis module thread-safe context; only used to push commands
                                   we get from the leader. */
    uv_thread_t thread;         /* Raft I/O thread */
    uv_loop_t *loop;            /* Raft I/O loop */
    uv_async_t rqueue_sig;      /* A signal we have something on rqueue */
    uv_timer_t ptimer;          /* Periodic timer to invoke Raft periodic function */
    STAILQ_HEAD(rqueue, raft_req) rqueue;     /* Requests queue (from Redis) */
    STAILQ_HEAD(cqueue, raft_req) cqueue;     /* Pending commit queue */
} redis_raft_t;


typedef struct node_addr {
    uint16_t port;
    char *host;
} node_addr_t;


typedef struct {
    int id;
    int state;
    node_addr_t addr;
    redisAsyncContext *rc;
    uv_getaddrinfo_t uv_resolver;
    uv_tcp_t uv_tcp;
    uv_connect_t uv_connect;
    redis_raft_t *rr;
} node_t;

struct raft_req;
typedef int (*raft_req_callback_t)(struct raft_req *);

enum raft_req_type {
    RAFT_REQ_ADDNODE = 1,
    RAFT_REQ_APPENDENTRIES,
    RAFT_REQ_REQUESTVOTE,
    RAFT_REQ_REDISCOMMAND
};

extern raft_req_callback_t raft_req_callbacks[];

typedef struct raft_req {
    int type;
    STAILQ_ENTRY(raft_req) entries;
    RedisModuleBlockedClient *client;
    RedisModuleCtx *ctx;
    union {
        struct {
            int id;
            node_addr_t addr;
        } addnode;
        struct {
            int src_node_id;
            msg_appendentries_t msg;
        } appendentries;
        struct {
            int src_node_id;
            msg_requestvote_t msg;
        } requestvote;
        struct {
            int argc;
            RedisModuleString **argv;
            msg_entry_response_t response;
        } raft;
    } r;
} raft_req_t;


/* node.c */
extern void node_free(node_t *node);
extern node_t *node_init(int id, const node_addr_t *addr);
extern void node_connect(node_t *node, redis_raft_t *rr);
extern bool parse_node_addr(const char *node_addr, size_t node_addr_len, node_addr_t *result);

/* util.c */
extern int rmstring_to_int(RedisModuleString *str, int *value);
#endif  /* _REDISRAFT_H */
