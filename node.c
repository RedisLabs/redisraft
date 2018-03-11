#include "redisraft.h"
#include "hiredis/adapters/libuv.h"


Node *NodeInit(int id, const NodeAddr *addr)
{
    Node *node = RedisModule_Calloc(1, sizeof(Node));

    node->id = id;
    strcpy(node->addr.host, addr->host);
    node->addr.port = addr->port;

    return node;
}


void NodeFree(Node *node)
{
    if (!node) {
        return;
    }
    RedisModule_Free(node);
}

static void handleNodeConnect(const redisAsyncContext *c, int status)
{
    Node *node = (Node *) c->data;
    if (status != REDIS_OK) {
        /* TODO: Add reconnection here */
        NODE_LOG_ERROR(node, "Failed to connect, status = %d\n", status);
        return;
    }
    node->state &= ~NODE_CONNECTING;
    node->state |= NODE_CONNECTED;
    NODE_LOG_INFO(node, "Connection established.\n");
}

static void handleNodeDisconnect(const redisAsyncContext *c, int status)
{
    Node *node = (Node *) c->data;
    node->state &= ~(NODE_CONNECTED|NODE_CONNECTING);
    NODE_LOG_INFO(node, "Connection dropped, reconnecting\n");

    NodeConnect(node, node->rr);
}

static void handleNodeResolved(uv_getaddrinfo_t *resolver, int status, struct addrinfo *res)
{
    int r;

    Node *node = uv_req_get_data((uv_req_t *)resolver);
    if (status < 0) {
        NODE_LOG_ERROR(node, "Failed to resolve '%s': %s\n", node->addr.host, uv_strerror(status));
        return;
    }

    char addr[17] = { 0 };
    uv_ip4_name((struct sockaddr_in *) res->ai_addr, addr, 16);
    NODE_LOG_INFO(node, "connecting at %s:%u...\n", addr, node->addr.port);

    /* Initiate connection */
    node->rc = redisAsyncConnect(addr, node->addr.port);
    if (node->rc->err) {
        NODE_LOG_ERROR(node, "Failed to initiate connection\n");

        /* We try again */
        uv_freeaddrinfo(res);
        NodeConnect(node, node->rr);
        return;
    }

    node->rc->data = node;
    redisLibuvAttach(node->rc, node->rr->loop);
    redisAsyncSetConnectCallback(node->rc, handleNodeConnect);
    redisAsyncSetDisconnectCallback(node->rc, handleNodeDisconnect);
    uv_freeaddrinfo(res);
}

void NodeConnect(Node *node, RedisRaftCtx *rr)
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

    NODE_LOG_INFO(node, "Resolving '%s'...\n", node->addr.host);

    node->state = NODE_CONNECTING;
    node->rr = rr;
    uv_req_set_data((uv_req_t *)&node->uv_resolver, node);
    int r = uv_getaddrinfo(rr->loop, &node->uv_resolver, handleNodeResolved,
            node->addr.host, NULL, &hints);
    if (r) {
        NODE_LOG_INFO(node, "Resolver error: %s: %s\n", node->addr.host, uv_strerror(r));
        return;
    }
}

bool NodeAddrParse(const char *node_addr, size_t node_addr_len, NodeAddr *result)
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
    if (addrlen >= sizeof(result->host)) {
        addrlen = sizeof(result->host)-1;
    }
    memcpy(result->host, node_addr, addrlen);
    result->host[addrlen] = '\0';

    return true;
}

bool NodeConfigParse(RedisModuleCtx *ctx, const char *str, NodeConfig *c)
{
    memset(c, 0, sizeof(NodeConfig));
    char *errptr;

    c->id = strtol(str, &errptr, 10);
    if (*errptr != ',') {
        RedisModule_Log(ctx, REDIS_WARNING, "Invalid node configuration format");
        return false;
    }

    if (!NodeAddrParse(errptr + 1, strlen(errptr + 1), &c->addr)) {
        RedisModule_Log(ctx, REDIS_WARNING, "Invalid node 'addr' field");
        return false;
    }

    return true;
}

