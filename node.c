/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020 Redis Labs
 *
 * RedisRaft is dual licensed under the GNU Affero General Public License version 3
 * (AGPLv3) or the Redis Source Available License (RSAL).
 */

#include <time.h>

#include "redisraft.h"
#include "hiredis/adapters/libuv.h"

#include <assert.h>

static LIST_HEAD(node_list, Node) node_list = LIST_HEAD_INITIALIZER(node_list);

const char *NodeStateStr[] = {
    "disconnected",
    "resolving",
    "connecting",
    "connected",
    "connect_error"
};

Node *NodeInit(int id, const NodeAddr *addr)
{
    Node *node = RedisModule_Calloc(1, sizeof(Node));
    STAILQ_INIT(&node->pending_responses);

    node->id = id;
    strcpy(node->addr.host, addr->host);
    node->addr.port = addr->port;

    LIST_INSERT_HEAD(&node_list, node, entries);

    return node;
}

static void clearPendingResponses(Node *node)
{
    node->pending_raft_response_num = 0;
    node->pending_proxy_response_num = 0;

    while (!STAILQ_EMPTY(&node->pending_responses)) {
        PendingResponse *resp = STAILQ_FIRST(&node->pending_responses);
        STAILQ_REMOVE_HEAD(&node->pending_responses, entries);
        RedisModule_Free(resp);
    }
}

void NodeFree(Node *node)
{
    if (!node) {
        return;
    }

    clearPendingResponses(node);

    LIST_REMOVE(node, entries);
    RedisModule_Free(node);
}

static void handleNodeConnect(const redisAsyncContext *c, int status)
{
    Node *node = (Node *) c->data;

    NODE_TRACE(node, "handleNodeConnect() callback called, status=%d\n", status);

    if (status == REDIS_OK) {
        node->state = NODE_CONNECTED;
        node->connect_oks++;
        node->last_connected_time = RedisModule_Milliseconds();
        clearPendingResponses(node);

        NODE_TRACE(node, "Node connection established.\n");
    } else {
        node->state = NODE_CONNECT_ERROR;
        node->rc = NULL;
        node->connect_errors++;
    }

    /* If we're terminating, abort now */
    if (node->flags & NODE_TERMINATING) {
        NodeMarkRemoved(node);
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

    NODE_TRACE(node, "handleNodeDisconnect() callback called, rc=%p\n",
        node ? node->rc : NULL);

    if (node) {
        node->state = NODE_DISCONNECTED;
    }
}

static void freeNodeOnRemoval(void *privdata)
{
    Node *node = (Node *) privdata;

    NODE_TRACE(node, "freeNodeOnRemoval() callback called, flags=%d, rc=%p\n",
         node ? node->flags : 0,
         node ? node->rc : NULL);

    node->rc = NULL;
    if (!node || !(node->flags & NODE_TERMINATING)) {
        return;
    }

    NodeFree(node);
}

static void handleNodeResolved(uv_getaddrinfo_t *resolver, int status, struct addrinfo *res)
{
    Node *node = uv_req_get_data((uv_req_t *)resolver);

    NODE_TRACE(node, "handleNodeResolved(), flags=%d, state=%s, rc=%p\n",
        node->flags,
        NodeStateStr[node->state],
        node->rc);

    if (node->flags & NODE_TERMINATING) {
        node->state = NODE_DISCONNECTED;
        uv_freeaddrinfo(res);
        return;
    }

    if (status < 0) {
        NODE_LOG_ERROR(node, "Failed to resolve '%s': %s\n", node->addr.host, uv_strerror(status));
        node->state = NODE_CONNECT_ERROR;
        node->connect_errors++;
        uv_freeaddrinfo(res);
        return;
    }

    uv_ip4_name((struct sockaddr_in *) res->ai_addr, node->ipaddr, sizeof(node->ipaddr)-1);
    uv_freeaddrinfo(res);

    /* Initiate connection */
    if (node->rc != NULL) {
        redisAsyncFree(node->rc);
    }
    node->rc = redisAsyncConnect(node->ipaddr, node->addr.port);
    if (node->rc->err) {
        node->state = NODE_CONNECT_ERROR;
        node->connect_errors++;

        redisAsyncFree(node->rc);
        node->rc = NULL;
        return;
    }

    node->rc->data = node;
    node->rc->dataCleanup = freeNodeOnRemoval;
    node->state = NODE_CONNECTING;
    node->flags &= ~NODE_TERMINATING;

    redisLibuvAttach(node->rc, node->rr->loop);
    redisAsyncSetConnectCallback(node->rc, handleNodeConnect);
    redisAsyncSetDisconnectCallback(node->rc, handleNodeDisconnect);
}

void NodeMarkDisconnected(Node *node)
{
    NODE_TRACE(node, "NodeMarkDisconnected() called, rc=%p\n",
        node->rc);

    node->state = NODE_DISCONNECTED;
    if (node->rc) {
        redisAsyncFree(node->rc);
        node->rc = NULL;
    }
}

void NodeMarkRemoved(Node *node)
{
    NODE_TRACE(node, "NodeMarkRemoved() called, rc=%p\n",
        node->rc);

    node->flags |= NODE_TERMINATING;
    /*
     * Consider doing this immediately rather than delaying; in some
     * cases we would still want to delay this to deliver outgoing
     * messages, so a deferred flag could be used.
     *
    LIST_REMOVE(node, entries);
    if (node->rc) {
        redisAsyncFree(node->rc);
        node->rc = NULL;
    }
    */
}

bool NodeConnect(Node *node, RedisRaftCtx *rr, NodeConnectCallbackFunc connect_callback)
{
    struct addrinfo hints = {
        .ai_family = PF_INET,
        .ai_socktype = SOCK_STREAM,
        .ai_protocol = IPPROTO_TCP,
        .ai_flags = 0
    };

    NODE_TRACE(node, "NodeConnect() called.\n");

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
void NodeAddrListAddElement(NodeAddrListElement **head, NodeAddr *addr)
{
    while (*head != NULL) {
        if (NodeAddrEqual(&(*head)->addr, addr)) {
            return;
        }

        head = &(*head)->next;
    }

    *head = RedisModule_Calloc(1, sizeof(NodeAddrListElement));
    (*head)->addr = *addr;
}

void NodeAddrListFree(NodeAddrListElement *head)
{
    NodeAddrListElement *t;

    while (head != NULL) {
        t = head->next;
        RedisModule_Free(head);
        head = t;
    }
}

static bool parseMovedReply(const char *str, NodeAddr *addr)
{
    if (strlen(str) < 15 || strncmp(str, "MOVED ", 6))
        return false;

    const char *tok = str + 6;
    const char *tok2;

    /* Handle current or cluster-style -MOVED replies. */
    if ((tok2 = strchr(tok, ' ')) == NULL) {
        return NodeAddrParse(tok, strlen(tok), addr);
    } else {
        return NodeAddrParse(tok2, strlen(tok2), addr);
    }
}

void handleNodeAddResponse(redisAsyncContext *c, void *r, void *privdata)
{
    Node *node = privdata;
    RedisRaftCtx *rr = node->rr;

    redisReply *reply = r;

    if (!reply) {
        LOG_ERROR("RAFT.NODE ADD failed: connection dropped.\n");
    } else if (reply->type == REDIS_REPLY_ERROR) {
        /* -MOVED? */
        if (strlen(reply->str) > 6 && !strncmp(reply->str, "MOVED ", 6)) {
            NodeAddr addr;
            if (!parseMovedReply(reply->str, &addr)) {
                LOG_ERROR("RAFT.NODE ADD failed: invalid MOVED response: %s\n", reply->str);
            } else {
                LOG_VERBOSE("Join redirected to leader: %s:%d\n", addr.host, addr.port);
                NodeAddrListAddElement(&rr->join_state->addr, &addr);
            }
        } else {
            LOG_ERROR("RAFT.NODE ADD failed: %s\n", reply->str);
        }
    } else if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 2) {
        LOG_ERROR("RAFT.NODE ADD invalid reply.\n");
    } else {
        raft_node_id_t node_id = reply->element[0]->integer;

        LOG_INFO("Joined Raft cluster, node id: %u, dbid: %.*s\n",
                node_id,
                reply->element[1]->len, reply->element[1]->str);

        strncpy(rr->snapshot_info.dbid, reply->element[1]->str, reply->element[1]->len);
        rr->snapshot_info.dbid[RAFT_DBID_LEN] = '\0';

        rr->config->id = node_id;

        HandleClusterJoinCompleted(rr);
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
        rr->config->id,
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

/* Track a new pending response for a request that was sent to the node.
 * This is used to track connection liveness and decide when it should be
 * dropped.
 */
void NodeAddPendingResponse(Node *node, bool proxy)
{
    static int response_id = 0;

    PendingResponse *resp = RedisModule_Calloc(1, sizeof(PendingResponse));
    resp->proxy = proxy;
    resp->request_time = RedisModule_Milliseconds();
    resp->id = ++response_id;

    if (proxy) {
        node->pending_proxy_response_num++;
    } else {
        node->pending_raft_response_num++;
    }
    STAILQ_INSERT_TAIL(&node->pending_responses, resp, entries);

    NODE_TRACE(node, "NodeAddPendingResponse: id=%d, type=%s, request_time=%lld\n",
            resp->id, proxy ? "proxy" : "raft", resp->request_time);
}

/* Acknowledge a response that has been received and remove it from the
 * node's list of pending responses.
 */
void NodeDismissPendingResponse(Node *node)
{
    PendingResponse *resp = STAILQ_FIRST(&node->pending_responses);
    STAILQ_REMOVE_HEAD(&node->pending_responses, entries);

    if (resp->proxy) {
        node->pending_proxy_response_num--;
    } else {
        node->pending_raft_response_num--;
    }

    NODE_TRACE(node, "NodeDismissPendingResponse: id=%d, type=%s, latency=%lld\n",
            resp->id, resp->proxy ? "proxy" : "raft",
            RedisModule_Milliseconds() - resp->request_time);

    RedisModule_Free(resp);
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

    if (rr->state == REDIS_RAFT_LOADING)
        return;

    /* Iterate nodes and find nodes that require reconnection */
    Node *node, *tmp;
    LIST_FOREACH_SAFE(node, &node_list, entries, tmp) {
        if (NODE_IS_CONNECTED(node) && !STAILQ_EMPTY(&node->pending_responses)) {
            PendingResponse *resp = STAILQ_FIRST(&node->pending_responses);
            long timeout;

            if (raft_is_leader(rr->raft)) {
                timeout = rr->config->raft_response_timeout;
            } else {
                timeout = resp->proxy ? rr->config->proxy_response_timeout : rr->config->raft_response_timeout;
            }

            if (timeout && resp->request_time + timeout < RedisModule_Milliseconds()) {
                NODE_TRACE(node, "Pending %s response timeout expired, reconnecting.\n",
                        resp->proxy ? "proxy" : "raft");
                NodeMarkDisconnected(node);
            }
        }

        if (NODE_STATE_IDLE(node->state)) {
            if (node->flags & NODE_TERMINATING) {
                LIST_REMOVE(node, entries);
                if (node->rc) {
                    /* Note: redisAsyncFree will call the freeNodeOnRemoval()
                     * callback which will free the Node structure.
                     */
                    redisAsyncContext *ac = node->rc;
                    node->rc = NULL;
                    redisAsyncFree(ac);
                } else {
                    NodeFree(node);
                }
            } else {
                raft_node_t *n = raft_get_node(rr->raft, node->id);
                if (n != NULL && raft_node_is_active(n)) {
                    NodeConnect(node, rr, NULL);
                }
            }
        }
    }
}

