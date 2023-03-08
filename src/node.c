/*
 * Copyright Redis Ltd. 2020 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "redisraft.h"

#include <string.h>

/* We maintain a list of Nodes which correlate with the nodes maintained by the
 * Raft library.
 *
 * For every node we maintain a Connection that allows communicating with it
 * using Redis commands. We also maintain additional information like general
 * metrics, and information about pending responses (used to implement timeouts
 * and reconnects).
 */

/* Clear all pending responses and metrics from the node. We have to do that
 * when reconnecting.
 */
static void clearPendingResponses(Node *node)
{
    node->pending_raft_response_num = 0;
    node->pending_proxy_response_num = 0;

    struct sc_list *tmp, *it;

    sc_list_foreach_safe (&node->pending_responses, tmp, it) {
        PendingResponse *resp = sc_list_entry(it, PendingResponse, entries);
        sc_list_del(&node->pending_responses, it);
        RedisModule_Free(resp);
    }
}

/* Connect callback */
static void handleNodeConnect(Connection *conn)
{
    Node *node = (Node *) ConnGetPrivateData(conn);

    if (ConnIsConnected(conn)) {
        clearPendingResponses(node);
        NODE_TRACE(node, "Node connection established.");
    }
}

/* Idle callback: when we have a connection associated with an active node,
 * we initiate ConnConnect().
 */
static void nodeIdleCallback(Connection *conn)
{
    Node *node = ConnGetPrivateData(conn);
    RedisRaftCtx *rr = ConnGetRedisRaftCtx(conn);

    raft_node_t *raft_node = raft_get_node(rr->raft, node->id);
    if (raft_node != NULL && raft_node_is_active(raft_node)) {
        ConnConnect(node->conn, &node->addr, handleNodeConnect);
    }
}

/* Free node object and remove it from the nodes linked list */
static void NodeFree(Node *node)
{
    if (!node) {
        return;
    }

    clearPendingResponses(node);

    sc_list_del(&node->rr->nodes, &node->entries);
    RedisModule_Free(node);
}

/* hiredis free callback */
static void nodeFreeCallback(void *privdata)
{
    Node *node = (Node *) privdata;
    NodeFree(node);
}

/* Create a new node object, put it in the nodes list and create a connection
 * object for it.
 *
 * Note that at this point no actual connection is made. The idle callback
 * fires at a later stage and handles connection setup.
 */

Node *NodeCreate(RedisRaftCtx *rr, int id, const NodeAddr *addr)
{
    Node *node = RedisModule_Calloc(1, sizeof(Node));

    sc_list_init(&node->pending_responses);
    sc_list_init(&node->entries);

    node->id = id;
    node->rr = rr;

    strcpy(node->addr.host, addr->host);
    node->addr.port = addr->port;

    sc_list_add_head(&rr->nodes, &node->entries);

    node->conn = ConnCreate(node->rr, node, nodeIdleCallback, nodeFreeCallback,
                            rr->config.cluster_user, rr->config.cluster_password);
    return node;
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
    sc_list_init(&resp->entries);

    if (proxy) {
        node->pending_proxy_response_num++;
    } else {
        node->pending_raft_response_num++;
    }
    sc_list_add_tail(&node->pending_responses, &resp->entries);

    NODE_TRACE(node, "NodeAddPendingResponse: id=%d, type=%s, request_time=%lld",
               resp->id, proxy ? "proxy" : "raft", resp->request_time);
}

/* Acknowledge a response that has been received and remove it from the
 * node's list of pending responses.
 */
void NodeDismissPendingResponse(Node *node)
{
    struct sc_list *elem = sc_list_pop_head(&node->pending_responses);
    PendingResponse *resp = sc_list_entry(elem, PendingResponse, entries);

    if (resp->proxy) {
        node->pending_proxy_response_num--;
    } else {
        node->pending_raft_response_num--;
    }

    NODE_TRACE(node, "NodeDismissPendingResponse: id=%d, type=%s, latency=%lld",
               resp->id, resp->proxy ? "proxy" : "raft",
               RedisModule_Milliseconds() - resp->request_time);

    RedisModule_Free(resp);
}

/* Gets called periodically to look for nodes with commands that should time out
 * and trigger a reconnect.
 */
void HandleNodeStates(RedisRaftCtx *rr)
{
    if (rr->state == REDIS_RAFT_LOADING)
        return;

    /* Iterate nodes and find nodes that require reconnection */
    struct sc_list *tmp, *it;

    sc_list_foreach_safe (&rr->nodes, tmp, it) {
        Node *node = sc_list_entry(it, Node, entries);
        struct sc_list *head = sc_list_head(&node->pending_responses);

        if (ConnIsConnected(node->conn) && head != NULL) {
            PendingResponse *resp = sc_list_entry(head, PendingResponse, entries);
            long timeout;

            if (raft_is_leader(rr->raft)) {
                timeout = rr->config.response_timeout;
            } else {
                timeout = resp->proxy ? rr->config.proxy_response_timeout : rr->config.response_timeout;
            }

            if (timeout && resp->request_time + timeout < RedisModule_Milliseconds()) {
                NODE_TRACE(node, "Pending %s response timeout expired, reconnecting.",
                           resp->proxy ? "proxy" : "raft");
                ConnMarkDisconnected(node->conn);
            }
        }
    }
}
