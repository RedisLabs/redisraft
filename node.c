/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020 Redis Labs
 *
 * RedisRaft is dual licensed under the GNU Affero General Public License version 3
 * (AGPLv3) or the Redis Source Available License (RSAL).
 */

#include <string.h>
#include "redisraft.h"

/* We maintain a list of Nodes which correlate with the nodes maintained by the
 * Raft library.
 *
 * For every node we maintain a Connection that allows communicating with it
 * using Redis commands. We also maintain additional information like general
 * metrics, and information about pending responses (used to implement timeouts
 * and reconnects).
 */

static LIST_HEAD(node_list, Node) node_list = LIST_HEAD_INITIALIZER(node_list);

/* Clear all pending responses and metrics from the node. We have to do that
 * when reconnecting.
 */
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

    LIST_REMOVE(node, entries);
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
    STAILQ_INIT(&node->pending_responses);

    node->id = id;
    node->rr = rr;

    strcpy(node->addr.host, addr->host);
    node->addr.port = addr->port;

    LIST_INSERT_HEAD(&node_list, node, entries);
    node->conn = ConnCreate(node->rr, node, nodeIdleCallback, nodeFreeCallback);

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

    if (proxy) {
        node->pending_proxy_response_num++;
    } else {
        node->pending_raft_response_num++;
    }
    STAILQ_INSERT_TAIL(&node->pending_responses, resp, entries);

    NODE_TRACE(node, "NodeAddPendingResponse: id=%d, type=%s, request_time=%lld",
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
    Node *node, *tmp;
    LIST_FOREACH_SAFE(node, &node_list, entries, tmp) {
        if (ConnIsConnected(node->conn) && !STAILQ_EMPTY(&node->pending_responses)) {
            PendingResponse *resp = STAILQ_FIRST(&node->pending_responses);
            long timeout;

            if (raft_is_leader(rr->raft)) {
                timeout = rr->config->raft_response_timeout;
            } else {
                timeout = resp->proxy ? rr->config->proxy_response_timeout : rr->config->raft_response_timeout;
            }

            if (timeout && resp->request_time + timeout < RedisModule_Milliseconds()) {
                NODE_TRACE(node, "Pending %s response timeout expired, reconnecting.",
                        resp->proxy ? "proxy" : "raft");
                ConnMarkDisconnected(node->conn);
            }
        }
    }
}

