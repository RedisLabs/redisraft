/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020-2021 Redis Ltd.
 *
 * RedisRaft is licensed under the Redis Source Available License (RSAL).
 */

#include "node.h"

#include "node_addr.h"
#include "redismodule.h"
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

static LIST_HEAD(node_list, Node) node_list = LIST_HEAD_INITIALIZER(node_list);

/* Clear all pending responses and metrics from the node. We have to do that
 * when reconnecting.
 */
static void clearPendingResponses(Node *node)
{
    node->pending_raft_response_num = 0;

    while (!STAILQ_EMPTY(&node->pending_responses)) {
        PendingResponse *resp = STAILQ_FIRST(&node->pending_responses);
        STAILQ_REMOVE_HEAD(&node->pending_responses, entries);
        RedisModule_Free(resp);
    }
}

/* Connect callback */
static void handleNodeConnect(Connection *conn)
{
    Node *node = ConnGetPrivateData(conn);

    if (ConnIsConnected(conn)) {
        clearPendingResponses(node);
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
    NodeFree(privdata);
}

/* Create a new node object, put it in the nodes list and create a connection
 * object for it.
 *
 * Note that at this point no actual connection is made. The idle callback
 * fires at a later stage and handles connection setup.
 */

Node *NodeCreate(RedisRaftCtx *rr, int id, const NodeAddr *addr)
{
    Node *node = RedisModule_Calloc(1, sizeof(*node));

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
void NodeAddPendingResponse(Node *node)
{
    PendingResponse *resp = RedisModule_Calloc(1, sizeof(*resp));

    resp->request_time = RedisModule_Milliseconds();
    node->pending_raft_response_num++;
    STAILQ_INSERT_TAIL(&node->pending_responses, resp, entries);
}

/* Acknowledge a response that has been received and remove it from the
 * node's list of pending responses.
 */
void NodeDismissPendingResponse(Node *node)
{
    PendingResponse *resp = STAILQ_FIRST(&node->pending_responses);
    STAILQ_REMOVE_HEAD(&node->pending_responses, entries);

    node->pending_raft_response_num--;
    RedisModule_Free(resp);
}

/* Gets called periodically to look for nodes with commands that should time out
 * and trigger a reconnect.
 */
void NodeReconnect(RedisRaftCtx *rr)
{
    /* Iterate nodes and find nodes that require reconnection */
    Node *node, *tmp;

    LIST_FOREACH_SAFE (node, &node_list, entries, tmp) {
        if (!ConnIsConnected(node->conn)) {
            continue;
        }

        if (!STAILQ_EMPTY(&node->pending_responses)) {
            PendingResponse *resp = STAILQ_FIRST(&node->pending_responses);
            long timeout = rr->config.response_timeout;
            long deadline = resp->request_time + timeout;

            if (timeout > 0 && RedisModule_Milliseconds() > deadline) {
                ConnMarkDisconnected(node->conn);
            }
        }
    }
}
