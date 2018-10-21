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

    node->state = NODE_RESOLVING;
    node->rr = rr;
    node->connect_callback = connect_callback;
    uv_req_set_data((uv_req_t *)&node->uv_resolver, node);
    int r = uv_getaddrinfo(rr->loop, &node->uv_resolver, handleNodeResolved,
            node->addr.host, NULL, &hints);
    if (r) {
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

void handleNodeAddResponse(redisAsyncContext *c, void *r, void *privdata)
{
    Node *node = privdata;
    RedisRaftCtx *rr = node->rr;

    redisReply *reply = r;
    assert(reply != NULL);

    if (!reply) {
        LOG_ERROR("RAFT.NODE ADD failed: connection dropped.\n");
    } else if (reply->type == REDIS_REPLY_ERROR) {
        /* -MOVED? */
        if (strlen(reply->str) > 6 && !strncmp(reply->str, "MOVED ", 6)) {
            const char *addrstr = reply->str + 6;
            NodeAddr addr;
            if (!NodeAddrParse(addrstr, strlen(addrstr), &addr)) {
                LOG_ERROR("RAFT.NODE ADD failed: invalid MOVED response: %s\n", reply->str);
            } else {
                LOG_INFO("Join redirected to leader: %s\n", addrstr);
                NodeAddrListAddElement(rr->join_state->addr, &addr);
            }
        } else {
            LOG_ERROR("RAFT.NODE ADD failed: %s\n", reply->str);
        }
    } else if (reply->type != REDIS_REPLY_STATUS || strlen(reply->str) < 4 ||
            strlen(reply->str) > 3 + RAFT_DBID_LEN || strncmp(reply->str, "OK", 2)) {
        LOG_ERROR("RAFT.NODE ADD invalid reply: %s\n", reply->str);
    } else {
        LOG_INFO("RAFT.NODE ADD succeeded, dbid: %s\n", reply->str + 3);
        strncpy(rr->snapshot_info.dbid, reply->str + 3, RAFT_DBID_LEN);
        rr->snapshot_info.dbid[RAFT_DBID_LEN] = '\0';
        rr->state = REDIS_RAFT_UP;

        /* Initialize Raft log.  We delay this operation as we want to create the log
         * with the proper dbid which is only received now.
         */
        if (rr->config->raftlog) {
            rr->log = RaftLogCreate(rr->config->raftlog, rr->snapshot_info.dbid,
                    rr->snapshot_info.last_applied_term, rr->snapshot_info.last_applied_idx);
            if (!rr->log) {
                PANIC("Failed to initialize Raft log");
            }
        }
    }

    if (rr->state != REDIS_RAFT_UP) {
        /* TODO: Throttle failed attempts, especially if server returned an error... */
        node->state = NODE_CONNECT_ERROR;
    }

    redisAsyncDisconnect(c);
}


void sendNodeAddRequest(const redisAsyncContext *c, int status)
{
    Node *node = (Node *) c->data;
    RedisRaftCtx *rr = node->rr;

    /* Connection is not good?  Terminate and continue */
    if (status != REDIS_OK) {
        node->state = NODE_CONNECT_ERROR;
    } else if (redisAsyncCommand(node->rc, handleNodeAddResponse, node,
        "RAFT.NODE %s %d %s:%u",
        "ADD",
        raft_get_nodeid(rr->raft),
        rr->config->addr.host, rr->config->addr.port) != REDIS_OK) {

        node->state = NODE_CONNECT_ERROR;
    }
}

static void initiateNodeAdd(RedisRaftCtx *rr)
{
    assert(rr->join_state != NULL);
    assert(rr->join_state->addr != NULL);

    /* Reset address iterator */
    if (!rr->join_state->addr_iter) {
        rr->join_state->addr_iter = rr->join_state->addr;
    }

    /* Allocate a node and initiate connection */
    if (!rr->join_state->node) {
        rr->join_state->node = NodeInit(0, &rr->join_state->addr->addr);
    } else {
        /* Try next address we have */
        rr->join_state->addr_iter = rr->join_state->addr_iter->next;
        if (!rr->join_state->addr_iter) {
            rr->join_state->addr_iter = rr->join_state->addr;
        }
        rr->join_state->node->addr = rr->join_state->addr_iter->addr;
    }

    NodeConnect(rr->join_state->node, rr, sendNodeAddRequest);
}

void HandleNodeStates(RedisRaftCtx *rr)
{

    /* When in joining state, we don't have nodes to worry about but only the
     * join_node synthetic node which establishes the initial connection.
     */
    if (rr->state == REDIS_RAFT_JOINING) {
        assert(rr->join_state != NULL);
        if (!rr->join_state->node || NODE_STATE_IDLE(rr->join_state->node->state)) {
            initiateNodeAdd(rr);
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

