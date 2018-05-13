#include "redisraft.h"
#include "hiredis/adapters/libuv.h"

#include <assert.h>

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
    if (status == REDIS_OK) {
        node->state = NODE_CONNECTED;
        NODE_LOG_INFO(node, "Connection established.\n");
    } else {
        node->state = NODE_CONNECT_ERROR;
        NODE_LOG_ERROR(node, "Failed to connect, status = %d\n", status);
    }

    /* Call explicit connect callback (even if failed) */
    if (node->connect_callback) {
        node->connect_callback(c, status);
    }
}

static void handleNodeDisconnect(const redisAsyncContext *c, int status)
{
    Node *node = (Node *) c->data;
    node->state = NODE_DISCONNECTED;
    NODE_LOG_INFO(node, "Connection dropped.\n");
}

static void handleNodeResolved(uv_getaddrinfo_t *resolver, int status, struct addrinfo *res)
{
    int r;

    Node *node = uv_req_get_data((uv_req_t *)resolver);
    if (status < 0) {
        NODE_LOG_ERROR(node, "Failed to resolve '%s': %s\n", node->addr.host, uv_strerror(status));
        node->state = NODE_CONNECT_ERROR;
        return;
    }

    char addr[17] = { 0 };
    uv_ip4_name((struct sockaddr_in *) res->ai_addr, addr, 16);
    NODE_LOG_INFO(node, "connecting at %s:%u...\n", addr, node->addr.port);

    /* Initiate connection */
    node->rc = redisAsyncConnect(addr, node->addr.port);
    if (node->rc->err) {
        uv_freeaddrinfo(res);

        NODE_LOG_ERROR(node, "Failed to initiate connection\n");
        node->state = NODE_CONNECT_ERROR;
        return;
    }

    node->rc->data = node;
    redisLibuvAttach(node->rc, node->rr->loop);
    redisAsyncSetConnectCallback(node->rc, handleNodeConnect);
    redisAsyncSetDisconnectCallback(node->rc, handleNodeDisconnect);
    uv_freeaddrinfo(res);
}

bool NodeConnect(Node *node, RedisRaftCtx *rr, NodeConnectCallbackFunc connect_callback)
{
    struct addrinfo hints = {
        .ai_family = PF_INET,
        .ai_socktype = SOCK_STREAM,
        .ai_protocol = IPPROTO_TCP,
        .ai_flags = 0
    };

    assert(NODE_STATE_IDLE(node->state));

    NODE_LOG_INFO(node, "Resolving '%s'...\n", node->addr.host);
    node->state = NODE_CONNECTING;
    node->rr = rr;
    node->connect_callback = connect_callback;
    uv_req_set_data((uv_req_t *)&node->uv_resolver, node);
    int r = uv_getaddrinfo(rr->loop, &node->uv_resolver, handleNodeResolved,
            node->addr.host, NULL, &hints);
    if (r) {
        NODE_LOG_INFO(node, "Resolver error: %s: %s\n", node->addr.host, uv_strerror(r));
        node->state = NODE_CONNECT_ERROR;
        return false;
    }

    return true;
}

bool NodeAddrParse(const char *node_addr, size_t node_addr_len, NodeAddr *result)
{
    char buf[32] = { 0 };
    char *endptr;
    unsigned long l;

    /* Split */
    const char *colon = node_addr + node_addr_len;
    while (colon > node_addr && *colon != ':') {
        colon--;
    }
    if (*colon != ':') {
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

/* Compare two NodeAddr sructs */
bool NodeAddrEqual(NodeAddr *a1, NodeAddr *a2)
{
    return (a1->port == a2->port && !strcmp(a1->host, a2->host));
}

/* Add a NodeAddrListElement to a chain of elements.  If an existing element with the same
 * address already exists, nothing is done.  The addr pointer provided is copied into newly
 * allocated memory, caller should free addr if necessary.
 */
void NodeAddrListAddElement(NodeAddrListElement *head, NodeAddr *addr)
{
    assert(head != NULL);
    do {
        if (NodeAddrEqual(&head->addr, addr)) {
            return;
        }

        if (head->next) {
            head = head->next;
        } else {
            NodeAddrListElement *new = RedisModule_Calloc(1, sizeof(NodeAddrListElement));
            head->next = new;
            new->addr.port = addr->port;
            strcpy(new->addr.host, addr->host);
            return;
        }
    } while (1);
}
