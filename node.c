#include "redisraft.h"
#include "hiredis/adapters/libuv.h"

#include <assert.h>

static LIST_HEAD(node_list, Node) node_list = LIST_HEAD_INITIALIZER(node_list);

Node *NodeInit(int id, const NodeAddr *addr)
{
    Node *node = RedisModule_Calloc(1, sizeof(Node));

    node->id = id;
    strcpy(node->addr.host, addr->host);
    node->addr.port = addr->port;

    LIST_INSERT_HEAD(&node_list, node, entries);

    return node;
}

void NodeFree(Node *node)
{
    if (!node) {
        return;
    }

    LIST_REMOVE(node, entries);
    RedisModule_Free(node);
}

static void handleNodeConnect(const redisAsyncContext *c, int status)
{
    Node *node = (Node *) c->data;
    if (status == REDIS_OK) {
        node->state = NODE_CONNECTED;
        //NODE_LOG_INFO(node, "Connection established.\n");
    } else {
        node->state = NODE_CONNECT_ERROR;
        node->rc = NULL;
        //NODE_LOG_ERROR(node, "Failed to connect, status = %d\n", status);
    }

    /* If we're terminating, abort now */
    if (node->flags & NODE_TERMINATING) {
        if (status == REDIS_OK) {
            redisAsyncDisconnect(node->rc);
        }
        return;
    }

    /* Call explicit connect callback (even if failed) */
    if (node->connect_callback) {
        node->connect_callback(c, status);
    }
}

static void handleNodeDisconnect(const redisAsyncContext *c, int status)
{
    Node *node = (Node *) c->data;
    if (node) {
        node->rc = NULL;
        node->state = NODE_DISCONNECTED;
        //NODE_LOG_INFO(node, "Connection dropped.\n");
    }
}

static void handleNodeResolved(uv_getaddrinfo_t *resolver, int status, struct addrinfo *res)
{
    int r;

    Node *node = uv_req_get_data((uv_req_t *)resolver);
    if (node->flags & NODE_TERMINATING) {
        node->state = NODE_DISCONNECTED;
        uv_freeaddrinfo(res);
        return;
    }

    if (status < 0) {
        NODE_LOG_ERROR(node, "Failed to resolve '%s': %s\n", node->addr.host, uv_strerror(status));
        node->state = NODE_CONNECT_ERROR;
        uv_freeaddrinfo(res);
        return;
    }

    char addr[17] = { 0 };
    uv_ip4_name((struct sockaddr_in *) res->ai_addr, addr, 16);
    uv_freeaddrinfo(res);
    //NODE_LOG_INFO(node, "connecting at %s:%u...\n", addr, node->addr.port);

    /* Initiate connection */
    node->rc = redisAsyncConnect(addr, node->addr.port);
    if (node->rc->err) {
        //NODE_LOG_ERROR(node, "Failed to initiate connection\n");
        node->state = NODE_CONNECT_ERROR;

        redisAsyncFree(node->rc);
        node->rc = NULL;
        return;
    }

    node->rc->data = node;
    node->state = NODE_CONNECTING;

    redisLibuvAttach(node->rc, node->rr->loop);
    redisAsyncSetConnectCallback(node->rc, handleNodeConnect);
    redisAsyncSetDisconnectCallback(node->rc, handleNodeDisconnect);
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

    //NODE_LOG_INFO(node, "Resolving '%s'...\n", node->addr.host);
    node->state = NODE_RESOLVING;
    node->rr = rr;
    node->connect_callback = connect_callback;
    uv_req_set_data((uv_req_t *)&node->uv_resolver, node);
    int r = uv_getaddrinfo(rr->loop, &node->uv_resolver, handleNodeResolved,
            node->addr.host, NULL, &hints);
    if (r) {
        //NODE_LOG_INFO(node, "Resolver error: %s: %s\n", node->addr.host, uv_strerror(r));
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

void NodeUnlink(Node *n)
{
    if (!n) {
        return;
    }

    if (n != NULL) {
        if (n->rc) {
            n->rc->data = NULL;
        }

        if (n->state == NODE_RESOLVING) {
            uv_cancel((uv_req_t *) &n->uv_resolver);
        } else if (n->state == NODE_CONNECTED || n->state == NODE_CONNECTING) {
            n->unlinked = true;
            redisAsyncDisconnect(n->rc);
        } else {
            NodeFree(n);
        }
    }
}

void handleAddNodeResponse(redisAsyncContext *c, void *r, void *privdata)
{
    Node *node = privdata;
    RedisRaftCtx *rr = node->rr;

    redisReply *reply = r;
    assert(reply != NULL);

    if (!reply) {
        NODE_LOG_ERROR(node, "RAFT.ADDNODE failed: connection dropped.\n");
    } else if (reply->type == REDIS_REPLY_ERROR) {
        /* -MOVED? */
        if (strlen(reply->str) > 6 && !strncmp(reply->str, "MOVED ", 6)) {
            const char *addrstr = reply->str + 6;
            NodeAddr addr;
            if (!NodeAddrParse(addrstr, strlen(addrstr), &addr)) {
                NODE_LOG_ERROR(node, "RAFT.ADDNODE failed: invalid MOVED response: %s\n", reply->str);
            } else {
                NODE_LOG_INFO(node, "Join redirected to leader: %s\n", addrstr);
                NodeAddrListAddElement(rr->join_addr, &addr);
            }
        }
    } else if (reply->type != REDIS_REPLY_STATUS || strcmp(reply->str, "OK")) {
        NODE_LOG_ERROR(node, "invalid RAFT.ADDNODE reply: %s\n", reply->str);
    } else {
        NODE_LOG_INFO(node, "Cluster join request placed as node:%d.\n",
                raft_get_nodeid(rr->raft));
        rr->state = REDIS_RAFT_UP;
    }

    if (rr->state != REDIS_RAFT_UP) {
        rr->join_node->state = NODE_CONNECT_ERROR;
    }

    redisAsyncDisconnect(c);
}


void sendAddNodeRequest(const redisAsyncContext *c, int status)
{
    Node *node = (Node *) c->data;
    RedisRaftCtx *rr = node->rr;

    /* Connection is not good?  Terminate and continue */
    if (status != REDIS_OK) {
        node->state = NODE_CONNECT_ERROR;
    } else if (redisAsyncCommand(node->rc, handleAddNodeResponse, node,
        "RAFT.ADDNODE %d %s:%u",
        raft_get_nodeid(rr->raft),
        rr->config->addr.host,
        rr->config->addr.port) != REDIS_OK) {

        node->state = NODE_CONNECT_ERROR;
    }
}

static void initiateAddNode(RedisRaftCtx *rr)
{
    if (!rr->join_addr) {
        rr->join_addr = rr->config->join;
        rr->join_addr_iter = rr->join_addr;
    }

    /* Allocate a node and initiate connection */
    if (!rr->join_node) {
        rr->join_node = NodeInit(0, &rr->join_addr->addr);
    } else {
        /* Try next address we have */
        rr->join_addr_iter = rr->join_addr_iter->next;
        if (!rr->join_addr_iter) {
            rr->join_addr_iter = rr->join_addr;
        }
        rr->join_node->addr = rr->join_addr_iter->addr;
    }

    NodeConnect(rr->join_node, rr, sendAddNodeRequest);
}

void HandleNodeStates(RedisRaftCtx *rr)
{

    /* When in joining state, we don't have nodes to worry about but only the
     * join_node synthetic node which establishes the initial connection.
     */
    if (rr->state == REDIS_RAFT_JOINING) {
        if (!rr->join_node || NODE_STATE_IDLE(rr->join_node->state)) {
            initiateAddNode(rr);
        }
        return;
    }

    /* Iterate nodes and find nodes that require reconnection */
    Node *node, *tmp;
    LIST_FOREACH_SAFE(node, &node_list, entries, tmp) {
        if (NODE_STATE_IDLE(node->state)) {
            if (node->flags & NODE_TERMINATING) {
                LIST_REMOVE(node, entries);
                NodeFree(node);
            } else {
                NodeConnect(node, rr, NULL);
            }
        }
    }
}

