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

#include <string.h>
#include <assert.h>

#include "raft.h"
#include "raft_private.h"

#define RAFT_NODE_VOTED_FOR_ME        (1 << 0)
#define RAFT_NODE_VOTING              (1 << 1)
#define RAFT_NODE_HAS_SUFFICIENT_LOG  (1 << 2)
#define RAFT_NODE_INACTIVE            (1 << 3)
#define RAFT_NODE_VOTING_COMMITTED    (1 << 4)
#define RAFT_NODE_ADDITION_COMMITTED  (1 << 5)

typedef struct
{
    void* udata;

    raft_index_t next_idx;
    raft_index_t match_idx;

    int flags;

    raft_node_id_t id;

    /* last AE heartbeat response received */
    raft_term_t last_acked_term;
    raft_msg_id_t last_acked_msgid;
    raft_msg_id_t max_seen_msgid;
} raft_node_private_t;

raft_node_t* raft_node_new(void* udata, raft_node_id_t id)
{
    raft_node_private_t* me;

    me = raft_calloc(1, sizeof(raft_node_private_t));
    if (!me)
        return NULL;

    me->udata = udata;
    me->next_idx = 1;
    me->match_idx = 0;
    me->id = id;
    me->flags = RAFT_NODE_VOTING;
    return (raft_node_t*)me;
}

void raft_node_free(raft_node_t* me_)
{
    raft_free(me_);
}

raft_index_t raft_node_get_next_idx(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    return me->next_idx;
}

void raft_node_set_next_idx(raft_node_t* me_, raft_index_t idx)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    /* log index begins at 1 */
    me->next_idx = idx < 1 ? 1 : idx;
}

raft_index_t raft_node_get_match_idx(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    return me->match_idx;
}

void raft_node_set_match_idx(raft_node_t* me_, raft_index_t idx)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    me->match_idx = idx;
}

void* raft_node_get_udata(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    return me->udata;
}

void raft_node_set_udata(raft_node_t* me_, void* udata)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    me->udata = udata;
}

void raft_node_vote_for_me(raft_node_t* me_, const int vote)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    if (vote)
        me->flags |= RAFT_NODE_VOTED_FOR_ME;
    else
        me->flags &= ~RAFT_NODE_VOTED_FOR_ME;
}

int raft_node_has_vote_for_me(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    return (me->flags & RAFT_NODE_VOTED_FOR_ME) != 0;
}

void raft_node_set_voting(raft_node_t* me_, int voting)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    if (voting)
    {
        assert(!raft_node_is_voting(me_));
        me->flags |= RAFT_NODE_VOTING;
    }
    else
    {
        assert(raft_node_is_voting(me_));
        me->flags &= ~RAFT_NODE_VOTING;
    }
}

int raft_node_is_voting(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    return (me->flags & RAFT_NODE_VOTING && !(me->flags & RAFT_NODE_INACTIVE));
}

int raft_node_has_sufficient_logs(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    return (me->flags & RAFT_NODE_HAS_SUFFICIENT_LOG) != 0;
}

void raft_node_set_has_sufficient_logs(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    me->flags |= RAFT_NODE_HAS_SUFFICIENT_LOG;
}

void raft_node_set_active(raft_node_t* me_, int active)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    if (!active)
        me->flags |= RAFT_NODE_INACTIVE;
    else
        me->flags &= ~RAFT_NODE_INACTIVE;
}

int raft_node_is_active(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    return (me->flags & RAFT_NODE_INACTIVE) == 0;
}

void raft_node_set_voting_committed(raft_node_t* me_, int voting)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    if (voting)
        me->flags |= RAFT_NODE_VOTING_COMMITTED;
    else
        me->flags &= ~RAFT_NODE_VOTING_COMMITTED;
}

int raft_node_is_voting_committed(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    return (me->flags & RAFT_NODE_VOTING_COMMITTED) != 0;
}

raft_node_id_t raft_node_get_id(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    return me != NULL ? me->id : -1;
}

void raft_node_set_addition_committed(raft_node_t* me_, int committed)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    if (committed)
        me->flags |= RAFT_NODE_ADDITION_COMMITTED;
    else
        me->flags &= ~RAFT_NODE_ADDITION_COMMITTED;
}

int raft_node_is_addition_committed(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    return (me->flags & RAFT_NODE_ADDITION_COMMITTED) != 0;
}

void raft_node_set_last_ack(raft_node_t* me_, raft_msg_id_t msgid, raft_term_t term)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    me->last_acked_msgid = msgid;
    me->last_acked_term = term;
}

raft_msg_id_t raft_node_get_last_acked_msgid(raft_node_t* me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    return me->last_acked_msgid;
}

void raft_node_update_max_seen_msg_id(raft_node_t *me_, raft_msg_id_t msg_id)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    if (msg_id > me->max_seen_msgid) {
        me->max_seen_msgid = msg_id;
    }
}

raft_msg_id_t raft_node_get_max_seen_msg_id(raft_node_t *me_)
{
    raft_node_private_t* me = (raft_node_private_t*)me_;
    return me->max_seen_msgid;
}