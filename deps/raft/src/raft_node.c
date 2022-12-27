/**
 * Copyright (c) 2013, Willem-Hendrik Thiart
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 *
 * @file
 * @brief Representation of a peer
 * @author Willem Thiart himself@willemthiart.com
 * @version 0.1
 */

#include "raft.h"
#include "raft_private.h"

#define RAFT_NODE_VOTED_FOR_ME        (1 << 0)
#define RAFT_NODE_VOTING              (1 << 1)
#define RAFT_NODE_HAS_SUFFICIENT_LOG  (1 << 2)
#define RAFT_NODE_INACTIVE            (1 << 3)
#define RAFT_NODE_VOTING_COMMITTED    (1 << 4)
#define RAFT_NODE_ADDITION_COMMITTED  (1 << 5)

struct raft_node {
    void *udata;

    raft_index_t next_idx;
    raft_index_t match_idx;

    raft_msg_id_t next_msgid;
    raft_msg_id_t match_msgid;
    raft_msg_id_t max_seen_msgid;

    int flags;

    raft_node_id_t id;

    /* Next snapshot offset to send to this node */
    raft_size_t snapshot_offset;
};

raft_node_t *raft_node_new(void *udata, raft_node_id_t id, int voting)
{
    raft_node_t *me;

    me = raft_calloc(1, sizeof(*me));
    if (!me) {
        return NULL;
    }

    me->udata = udata;
    me->next_idx = 1;
    me->match_idx = 0;
    me->id = id;
    raft_node_set_voting(me, voting);

    return me;
}

void raft_node_free(raft_node_t *node)
{
    raft_free(node);
}

raft_index_t raft_node_get_next_idx(raft_node_t *node)
{
    return node->next_idx;
}

void raft_node_set_next_idx(raft_node_t *node, raft_index_t idx)
{
    /* log index begins at 1 */
    node->next_idx = idx < 1 ? 1 : idx;
}

raft_index_t raft_node_get_match_idx(raft_node_t *node)
{
    return node->match_idx;
}

void raft_node_set_match_idx(raft_node_t *node, raft_index_t idx)
{
    node->match_idx = idx;
}

void raft_node_set_match_msgid(raft_node_t *node, raft_msg_id_t msgid)
{
    node->match_msgid = msgid;
}

raft_msg_id_t raft_node_get_match_msgid(raft_node_t *node)
{
    return node->match_msgid;
}

void raft_node_set_next_msgid(raft_node_t *node, raft_msg_id_t msgid)
{
    node->next_msgid = msgid;
}

raft_msg_id_t raft_node_get_next_msgid(raft_node_t *node)
{
    return node->next_msgid;
}

void raft_node_update_max_seen_msg_id(raft_node_t *node, raft_msg_id_t msg_id)
{
    if (node->max_seen_msgid < msg_id) {
        node->max_seen_msgid = msg_id;
    }
}

raft_msg_id_t raft_node_get_max_seen_msg_id(raft_node_t *node)
{
    return node->max_seen_msgid;
}

void* raft_node_get_udata(raft_node_t *node)
{
    return node->udata;
}

void raft_node_set_udata(raft_node_t *node, void* udata)
{
    node->udata = udata;
}

void raft_node_clear_flags(raft_node_t *node)
{
    node->flags = 0;
}

void raft_node_set_voted_for_me(raft_node_t *node, int vote)
{
    if (vote) {
        node->flags |= RAFT_NODE_VOTED_FOR_ME;
    } else {
        node->flags &= ~RAFT_NODE_VOTED_FOR_ME;
    }
}

int raft_node_has_vote_for_me(raft_node_t *node)
{
    return (node->flags & RAFT_NODE_VOTED_FOR_ME) != 0;
}

void raft_node_set_voting(raft_node_t *node, int voting)
{
    if (voting) {
        node->flags |= RAFT_NODE_VOTING;
    } else {
        node->flags &= ~RAFT_NODE_VOTING;
    }
}

int raft_node_is_voting(raft_node_t *node)
{
    return (node->flags & RAFT_NODE_VOTING &&
           !(node->flags & RAFT_NODE_INACTIVE));
}

int raft_node_has_sufficient_logs(raft_node_t *node)
{
    return (node->flags & RAFT_NODE_HAS_SUFFICIENT_LOG) != 0;
}

void raft_node_set_has_sufficient_logs(raft_node_t *node, int sufficient_logs)
{
    if (sufficient_logs) {
        node->flags |= RAFT_NODE_HAS_SUFFICIENT_LOG;
    } else {
        node->flags &= ~RAFT_NODE_HAS_SUFFICIENT_LOG;
    }
}

void raft_node_set_active(raft_node_t *node, int active)
{
    if (!active) {
        node->flags |= RAFT_NODE_INACTIVE;
    } else {
        node->flags &= ~RAFT_NODE_INACTIVE;
    }
}

int raft_node_is_active(raft_node_t *node)
{
    return (node->flags & RAFT_NODE_INACTIVE) == 0;
}

void raft_node_set_voting_committed(raft_node_t *node, int voting)
{
    if (voting) {
        node->flags |= RAFT_NODE_VOTING_COMMITTED;
    } else {
        node->flags &= ~RAFT_NODE_VOTING_COMMITTED;
    }
}

int raft_node_is_voting_committed(raft_node_t *node)
{
    return (node->flags & RAFT_NODE_VOTING_COMMITTED) != 0;
}

raft_node_id_t raft_node_get_id(raft_node_t *node)
{
    return node != NULL ? node->id : RAFT_NODE_ID_NONE;
}

void raft_node_set_addition_committed(raft_node_t *node, int committed)
{
    if (committed) {
        node->flags |= RAFT_NODE_ADDITION_COMMITTED;
    } else {
        node->flags &= ~RAFT_NODE_ADDITION_COMMITTED;
    }
}

int raft_node_is_addition_committed(raft_node_t *node)
{
    return (node->flags & RAFT_NODE_ADDITION_COMMITTED) != 0;
}

raft_size_t raft_node_get_snapshot_offset(raft_node_t *node)
{
    return node->snapshot_offset;
}

void raft_node_set_snapshot_offset(raft_node_t *node, raft_size_t offset)
{
    node->snapshot_offset = offset;
}
