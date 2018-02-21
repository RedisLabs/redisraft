#include "redisraft.h"
#include "hiredis/adapters/libuv.h"


void node_addr_free(node_addr_t *node_addr)
{
    if (node_addr && node_addr->host) {
        RedisModule_Free(node_addr->host);
        node_addr->host = NULL;
    }
}


node_t *node_init(int id, const node_addr_t *addr)
{
    node_t *node = RedisModule_Calloc(1, sizeof(node_t));

    node->id = id;
    node->addr.host = RedisModule_Strdup(addr->host);
    node->addr.port = addr->port;

    return node;
}


void node_free(node_t *node)
{
    if (!node) {
        return;
    }
    node_addr_free(&node->addr);
    RedisModule_Free(node);
}

static void node_on_connect(const redisAsyncContext *c, int status)
{
    node_t *node = (node_t *) c->data;
    if (status != REDIS_OK) {
        /* TODO: Add reconnection here */
        LOG_NODE(node, "failed to connect, status = %d\n", status);
        return;
    }
    node->state &= ~NODE_CONNECTING;
    node->state |= NODE_CONNECTED;
    LOG_NODE(node, "connection established.\n");
}

static void node_on_disconnect(const redisAsyncContext *c, int status)
{
    node_t *node = (node_t *) c->data;
    node->state &= ~(NODE_CONNECTED|NODE_CONNECTING);
    LOG_NODE(node, "connection dropped, reconnecting\n");

    node_connect(node, node->rr);
}

static void node_on_resolved(uv_getaddrinfo_t *resolver, int status, struct addrinfo *res)
{
    int r;

    node_t *node = uv_req_get_data((uv_req_t *)resolver);
    if (status < 0) {
        LOG_NODE(node, "failed to resolve '%s': %s\n", node->addr.host, uv_strerror(status));
        return;
    }

    char addr[17] = { 0 };
    uv_ip4_name((struct sockaddr_in *) res->ai_addr, addr, 16);
    LOG_NODE(node, "connecting at %s:%u...\n", addr, node->addr.port);

    /* Initiate connection */
    node->rc = redisAsyncConnect(addr, node->addr.port);
    if (node->rc->err) {
        LOG_NODE(node, "failed to initiate connection\n");

        /* We try again */
        uv_freeaddrinfo(res);
        node_connect(node, node->rr);
        return;
    }

    node->rc->data = node;
    redisLibuvAttach(node->rc, node->rr->loop);
    redisAsyncSetConnectCallback(node->rc, node_on_connect);
    redisAsyncSetDisconnectCallback(node->rc, node_on_disconnect);
    uv_freeaddrinfo(res);
}

void node_connect(node_t *node, redis_raft_t *rr)
{
    struct addrinfo hints = {
        .ai_family = PF_INET,
        .ai_socktype = SOCK_STREAM,
        .ai_protocol = IPPROTO_TCP,
        .ai_flags = 0
    };

    /* Don't try to connect if we're already connecting */
    if (node->state & NODE_CONNECTING) {
        return;
    }

    node->state = NODE_CONNECTING;
    node->rr = rr;
    uv_req_set_data((uv_req_t *)&node->uv_resolver, node);
    int r = uv_getaddrinfo(rr->loop, &node->uv_resolver, node_on_resolved,
            node->addr.host, NULL, &hints);
    if (r) {
        LOG_NODE(node, "resolver error: %s: %s\n", node->addr.host, uv_strerror(r));
        return;
    }
}

bool node_addr_parse(const char *node_addr, size_t node_addr_len, node_addr_t *result)
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
    result->port = l;

    /* Get addr */
    int addrlen = colon - node_addr;
    result->host = RedisModule_Alloc(addrlen + 1);
    memcpy(result->host, node_addr, addrlen);
    result->host[addrlen] = '\0';

    return true;
}

node_config_t *node_config_parse(RedisModuleCtx *ctx, const char *str)
{
    node_config_t *c = RedisModule_Calloc(1, sizeof(node_config_t));
    char *errptr;

    c->id = strtol(str, &errptr, 10);
    if (*errptr != ',') {
        RedisModule_Log(ctx, LOGLEVEL_WARNING, "Invalid node configuration format");
        goto parse_error;
    }

    if (!node_addr_parse(errptr + 1, strlen(errptr + 1), &c->addr)) {
        RedisModule_Log(ctx, LOGLEVEL_WARNING, "Invalid node 'addr' field");
        goto parse_error;
    }

    return c;

parse_error:
    node_addr_free(&c->addr);
    RedisModule_Free(c);
    return NULL;
}
