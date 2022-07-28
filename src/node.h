#ifndef REDISRAFT_NODE_H
#define REDISRAFT_NODE_H

#include "node_addr.h"
#include "queue.h"
#include "raft.h"

typedef struct PendingResponse {
    long long request_time;
    STAILQ_ENTRY(PendingResponse) entries;
} PendingResponse;

/* Maintains all state about peer nodes */
typedef struct Node {
    raft_node_id_t id;              /* Raft unique node ID */
    struct RedisRaftCtx *rr;        /* RedisRaftCtx handle */
    struct Connection *conn;        /* Connection to node */
    NodeAddr addr;                  /* Node's address */
    long pending_raft_response_num; /* Number of pending Raft responses */
    LIST_ENTRY(Node) entries;
    STAILQ_HEAD(pending_responses, PendingResponse) pending_responses;
} Node;

Node *NodeCreate(struct RedisRaftCtx *rr, int id, const NodeAddr *addr);
void NodeReconnect(struct RedisRaftCtx *rr);
void NodeAddPendingResponse(Node *node);
void NodeDismissPendingResponse(Node *node);

#endif
