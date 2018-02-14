#define _POSIX_C_SOURCE 200112L
#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <sys/queue.h>

#include <pthread.h>

#define REDISMODULE_EXPERIMENTAL_API
#include "redismodule.h"
#include "raft.h"

#include "uv.h"

#define LOG(fmt, ...) \
    fprintf(stderr, "redis-raft * " fmt, __VA_ARGS__)
#define LOG_VERBOSE(fmt, ...) \
    fprintf(stderr, "redis-raft - " fmt, __VA_ARGS__)

#define LOG_NODE(node, fmt, ...) \
    LOG("node:%08x: " fmt, (node)->id, __VA_ARGS__)

typedef struct {
    int id;
    char *addr;
    uint16_t port;
    struct sockaddr_in sockaddr;
    uv_getaddrinfo_t uv_resolver;
    uv_tcp_t uv_tcp;
    uv_connect_t uv_connect;
} node_t;


struct redis_raft_req;
typedef int (*redis_raft_req_callback_t)(struct redis_raft_req *);

typedef struct redis_raft_req {
    STAILQ_ENTRY(redis_raft_req) entries;
    RedisModuleBlockedClient *client;
    RedisModuleCtx *ctx;
    redis_raft_req_callback_t callback;
    int argc;
    RedisModuleString **argv;
    struct redis_raft_req *next;
} redis_raft_req_t;

typedef struct {
    void *raft;
    uv_thread_t thread;
    uv_loop_t *loop;
    uv_async_t async;
    STAILQ_HEAD(rqueue, redis_raft_req) rqueue;
    RedisModuleCtx *ctx;
} redis_raft_t;

redis_raft_t redis_raft = { 0 };

static void node_free(node_t *node)
{
    if (!node) {
        return;
    }
    if (node->addr) {
        RedisModule_Free(node->addr);
        node->addr = NULL;
    }
    RedisModule_Free(node);
}

static int enqueue_raft_command(RedisModuleCtx *ctx, redis_raft_req_callback_t callback,
        RedisModuleString **argv, int argc)
{
    redis_raft_req_t *req = RedisModule_Alloc(sizeof(redis_raft_req_t));
    req->client = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
    req->ctx = RedisModule_GetThreadSafeContext(req->client);
    req->callback = callback;
    req->argv = argv;
    req->argc = argc;

    STAILQ_INSERT_TAIL(&redis_raft.rqueue, req, entries);
    uv_async_send(&redis_raft.async);

    return REDISMODULE_OK;
}

#define LOGLEVEL_DEBUG      "debug"
#define LOGLEVEL_VERBOSE    "verbose"
#define LOGLEVEL_NOTICE     "notice"
#define LOGLEVEL_WARNING    "warning"

#define VALID_NODE_ID(x)    ((x) > 0)

static bool parse_node_addr(const char *node_addr, size_t node_addr_len, node_t *node)
{
    char buf[32] = { 0 };
    char *endptr;
    unsigned long l;

    /* Split */
    char *colon = memrchr(node_addr, ':', node_addr_len);
    if (!colon) {
        return false;
    }

    /* Get port */
    int portlen = node_addr_len - (colon + 1 - node_addr);
    if (portlen >= sizeof(buf) || portlen < 1) {
        return false;
    }

    strncpy(buf, colon + 1, portlen);
    l = strtoul(buf, &endptr, 10);
    if (*endptr != '\0' || l < 1 || l > 65535) {
        return false;
    }
    node->port = l;

    /* Get addr */
    int addrlen = colon - node_addr;
    node->addr = RedisModule_Alloc(addrlen + 1);
    memcpy(node->addr, node_addr, addrlen);
    node->addr[addrlen] = '\0';

    return true;
}

static void node_on_connect(uv_connect_t *req, int status)
{
    node_t *node = uv_handle_get_data((uv_handle_t *)req->handle);

    if (status < 0) {
        LOG("node:%08x: failed to connect: %s\n", node->id, uv_err_name(status));
        return;
    }

    LOG("node:%08x connected.\n", node->id);
}

static void node_on_resolved(uv_getaddrinfo_t *resolver, int status, struct addrinfo *res)
{
    int r;

    node_t *node = uv_req_get_data((uv_req_t *)resolver);
    if (status < 0) {
        LOG("node:%08x: failed to resolve '%s': %s\n", node->id, node->addr, uv_err_name(status));
        return;
    }

    char addr[17] = { 0 };
    uv_ip4_name((struct sockaddr_in *) res->ai_addr, addr, 16);

    /* Initiate connection */
    memcpy(&node->sockaddr, res->ai_addr, sizeof(node->sockaddr));
    node->sockaddr.sin_port = htons(node->port);
    uv_freeaddrinfo(res);

    LOG_VERBOSE("node:%08x: connecting at %s:%u...\n", node->id, addr, node->port);
    uv_tcp_init(redis_raft.loop, &node->uv_tcp);
    uv_handle_set_data((uv_handle_t *) &node->uv_tcp, node);
    if ((r = uv_tcp_connect(&node->uv_connect, &node->uv_tcp,
                    (struct sockaddr *) &node->sockaddr, node_on_connect)) < 0) {
        LOG_VERBOSE("node:%08x: connect error: %s\n", node->id, uv_err_name(r));
    }
}

static void node_connect(node_t *node)
{
    /* Always begin by resolving */
    struct addrinfo hints = {
        .ai_family = PF_INET,
        .ai_socktype = SOCK_STREAM,
        .ai_protocol = IPPROTO_TCP,
        .ai_flags = 0
    };

    uv_req_set_data((uv_req_t *)&node->uv_resolver, node);
    int r = uv_getaddrinfo(redis_raft.loop, &node->uv_resolver, node_on_resolved,
            node->addr, NULL, &hints);
    if (r) {
        fprintf(stderr, "node:%08x: resolver error: %s\n", node->id, uv_err_name(r));
        return;
    }
}

static int _cmd_raft_addnode(redis_raft_req_t *req)
{
    node_t *node = NULL;
    long long node_id;
    const char *node_addr;

    /* Validate node id */
    if (RedisModule_StringToLongLong(req->argv[1], &node_id) != REDISMODULE_OK ||
        !VALID_NODE_ID(node_id)) {
            RedisModule_ReplyWithError(req->ctx, "invalid node id");
            goto exit;
    }

    /* Parse address and create node */
    size_t node_addr_len;
    node_addr = RedisModule_StringPtrLen(req->argv[2], &node_addr_len);
    node = RedisModule_Calloc(1, sizeof(node_t));
    if (!parse_node_addr(node_addr, node_addr_len, node)) {
        RedisModule_ReplyWithError(req->ctx, "invalid node address");
        node_free(node);
        goto exit;
    }

    node->id = node_id;
    node_connect(node);
    RedisModule_ReplyWithSimpleString(req->ctx, "OK");

exit:
    RedisModule_FreeThreadSafeContext(req->ctx);
    RedisModule_UnblockClient(req->client, NULL);
    req->ctx = NULL;
    return REDISMODULE_OK;
}

static void handle_cmd(uv_async_t *handle)
{
    while (!STAILQ_EMPTY(&redis_raft.rqueue)) {
        redis_raft_req_t *req = STAILQ_FIRST(&redis_raft.rqueue);
        req->callback(req);
        STAILQ_REMOVE_HEAD(&redis_raft.rqueue, entries);
        RedisModule_Free(req);
    }
}

static void redis_raft_thread(void *arg)
{
    redis_raft_t *rr = (redis_raft_t *) arg;

    rr->loop = RedisModule_Alloc(sizeof(uv_loop_t));
    uv_loop_init(rr->loop);

    uv_async_init(rr->loop, &rr->async, handle_cmd);
    uv_run(rr->loop, UV_RUN_DEFAULT);
}


int redis_raft_init(RedisModuleCtx *ctx, redis_raft_t *rr, int node_id)
{
    memset(rr, 0, sizeof(redis_raft_t));
    STAILQ_INIT(&rr->rqueue);
    rr->ctx = RedisModule_GetThreadSafeContext(NULL);

    /* Create a global thread safe context */


    /* Initialize raft library */
    rr->raft = raft_new();
    if (!raft_add_node(rr->raft, NULL, node_id, 1)) {
        RedisModule_Log(ctx, LOGLEVEL_WARNING, "Failed to initialize raft_node");
        return REDISMODULE_ERR;
    }

    /* Start RAFT thread */
    if (uv_thread_create(&rr->thread, redis_raft_thread, &redis_raft) < 0) {
        RedisModule_Log(ctx, LOGLEVEL_WARNING, "Failed to initialize redis_raft thread");
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}


int cmd_raft_addnode(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    redis_raft_t *rr = &redis_raft;

    if (argc != 3) {
        RedisModule_WrongArity(ctx);
        return REDISMODULE_OK;
    }

    if (enqueue_raft_command(ctx, _cmd_raft_addnode, argv, argc) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}


int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if (RedisModule_Init(ctx, "redisraft", 1, REDISMODULE_APIVER_1) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    if (argc < 1) {
        RedisModule_Log(ctx, LOGLEVEL_WARNING, "Missing node ID");
        return REDISMODULE_ERR;
    }
    
    long long id;
    if (RedisModule_StringToLongLong(argv[0], &id) != REDISMODULE_OK || id <= 0) {
        RedisModule_Log(ctx, LOGLEVEL_WARNING, "Invalid node ID");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "raft.addnode",
                cmd_raft_addnode, "admin", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    return redis_raft_init(ctx, &redis_raft, id);
}
