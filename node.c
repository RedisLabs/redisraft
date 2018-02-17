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
    node->state |= NODE_CONNECTED;
    LOG_NODE(node, "connection established.\n");
}

static void node_on_disconnect(const redisAsyncContext *c, int status)
{
    node_t *node = (node_t *) c->data;
    node->state &= ~NODE_CONNECTED;
    LOG_NODE(node, "connection dropped.\n");
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

    /* Always begin by resolving */
    struct addrinfo hints = {
        .ai_family = PF_INET,
        .ai_socktype = SOCK_STREAM,
        .ai_protocol = IPPROTO_TCP,
        .ai_flags = 0
    };

    node->rr = rr;
    uv_req_set_data((uv_req_t *)&node->uv_resolver, node);
    int r = uv_getaddrinfo(rr->loop, &node->uv_resolver, node_on_resolved,
            node->addr.host, NULL, &hints);
    if (r) {
        LOG_NODE(node, "resolver error: %s: %s\n", node->addr.host, uv_strerror(r));
        return;
    }
}

bool parse_node_addr(const char *node_addr, size_t node_addr_len, node_addr_t *result)
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


