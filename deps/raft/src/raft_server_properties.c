/**
 * Copyright (c) 2013, Willem-Hendrik Thiart
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 *
 * @file
 * @author Willem Thiart himself@willemthiart.com
 */

#include <string.h>
#include <assert.h>

#include "raft.h"
#include "raft_private.h"

raft_node_id_t raft_get_nodeid(raft_server_t* me)
{
    return raft_node_get_id(me->node);
}

int raft_get_num_nodes(raft_server_t* me)
{
    return me->num_nodes;
}

int raft_get_num_voting_nodes(raft_server_t* me)
{
    int i, num = 0;
    for (i = 0; i < me->num_nodes; i++)
        if (raft_node_is_voting(me->nodes[i]))
            num++;
    return num;
}

int raft_get_timeout_elapsed(raft_server_t* me)
{
    return me->timeout_elapsed;
}

raft_index_t raft_get_log_count(raft_server_t* me)
{
    return me->log_impl->count(me->log);
}

int raft_get_voted_for(raft_server_t* me)
{
    return me->voted_for;
}

int raft_set_current_term(raft_server_t* me, const raft_term_t term)
{
    if (me->current_term < term)
    {
        if (me->cb.persist_term)
        {
            int e = me->cb.persist_term(me, me->udata, term, RAFT_NODE_ID_NONE);
            if (0 != e)
                return e;
        }
        me->current_term = term;
        me->voted_for = RAFT_NODE_ID_NONE;
    }
    return 0;
}

raft_term_t raft_get_current_term(raft_server_t* me)
{
    return me->current_term;
}

raft_index_t raft_get_current_idx(raft_server_t* me)
{
    return me->log_impl->current_idx(me->log);
}

void raft_set_commit_idx(raft_server_t* me, raft_index_t idx)
{
    assert(me->commit_idx <= idx);
    assert(idx <= raft_get_current_idx(me));
    me->commit_idx = idx;
}

void raft_set_last_applied_idx(raft_server_t* me, raft_index_t idx)
{
    me->last_applied_idx = idx;
}

raft_index_t raft_get_last_applied_idx(raft_server_t* me)
{
    return me->last_applied_idx;
}

raft_index_t raft_get_commit_idx(raft_server_t* me)
{
    return me->commit_idx;
}

void raft_set_state(raft_server_t* me, int state)
{
    /* if became the leader, then update the current leader entry */
    if (state == RAFT_STATE_LEADER)
        me->leader_id = raft_node_get_id(me->node);
    me->state = state;
}

int raft_get_state(raft_server_t* me)
{
    return me->state;
}

const char *raft_get_state_str(raft_server_t* me)
{
    switch (me->state) {
        case RAFT_STATE_FOLLOWER:
            return "follower";
        case RAFT_STATE_PRECANDIDATE:
            return "pre-candidate";
        case RAFT_STATE_CANDIDATE:
            return "candidate";
        case RAFT_STATE_LEADER:
            return "leader";
        default:
            return "unknown";
    }
}

raft_node_t* raft_get_node(raft_server_t *me, raft_node_id_t nodeid)
{
    int i;

    for (i = 0; i < me->num_nodes; i++)
        if (nodeid == raft_node_get_id(me->nodes[i]))
            return me->nodes[i];

    return NULL;
}

raft_node_t* raft_get_my_node(raft_server_t *me)
{
    int i;

    for (i = 0; i < me->num_nodes; i++)
        if (raft_get_nodeid(me) == raft_node_get_id(me->nodes[i]))
            return me->nodes[i];

    return NULL;
}

raft_node_t* raft_get_node_from_idx(raft_server_t* me, const raft_index_t idx)
{
    return me->nodes[idx];
}

raft_node_id_t raft_get_leader_id(raft_server_t* me)
{
    return me->leader_id;
}

raft_node_t* raft_get_leader_node(raft_server_t* me)
{
    return raft_get_node(me, me->leader_id);
}

void* raft_get_udata(raft_server_t* me)
{
    return me->udata;
}

int raft_is_follower(raft_server_t* me)
{
    return raft_get_state(me) == RAFT_STATE_FOLLOWER;
}

int raft_is_leader(raft_server_t* me)
{
    return raft_get_state(me) == RAFT_STATE_LEADER;
}

int raft_is_precandidate(raft_server_t* me)
{
    return raft_get_state(me) == RAFT_STATE_PRECANDIDATE;
}

int raft_is_candidate(raft_server_t* me)
{
    return raft_get_state(me) == RAFT_STATE_CANDIDATE;
}

raft_term_t raft_get_last_log_term(raft_server_t* me)
{
    raft_index_t current_idx = raft_get_current_idx(me);
    if (0 < current_idx)
    {
        raft_entry_t* ety = raft_get_entry_from_idx(me, current_idx);
        if (ety) {
            raft_term_t term = ety->term;
            raft_entry_release(ety);
            return term;
        } else if (raft_get_snapshot_last_idx(me) == current_idx) {
            return raft_get_snapshot_last_term(me);
        }
    }

    return 0;
}

int raft_snapshot_is_in_progress(raft_server_t *me)
{
    return me->snapshot_in_progress;
}

int raft_is_apply_allowed(raft_server_t* me)
{
    return !me->disable_apply &&
           (!raft_snapshot_is_in_progress(me) || me->nonblocking_apply);
}

raft_entry_t *raft_get_last_applied_entry(raft_server_t *me)
{
    if (raft_get_last_applied_idx(me) == 0)
        return NULL;
    return me->log_impl->get(me->log, raft_get_last_applied_idx(me));
}

raft_index_t raft_get_snapshot_last_idx(raft_server_t *me)
{
    return me->snapshot_last_idx;
}

raft_term_t raft_get_snapshot_last_term(raft_server_t *me)
{
    return me->snapshot_last_term;
}

void raft_set_snapshot_metadata(raft_server_t *me, raft_term_t term, raft_index_t idx)
{
    me->last_applied_idx = idx;
    me->snapshot_last_term = term;
    me->snapshot_last_idx = idx;
}

int raft_is_single_node_voting_cluster(raft_server_t *me)
{
    return (1 == raft_get_num_voting_nodes(me) && raft_node_is_voting(me->node));
}

raft_msg_id_t raft_get_msg_id(raft_server_t* me)
{
    return me->msg_id;
}

/* return the targeted node_id if we are in the middle of attempting a leadership transfer
 * return RAFT_NODE_ID_NONE if no leadership transfer is in progress
 */
raft_node_id_t raft_get_transfer_leader(raft_server_t* me)
{
    return me->node_transferring_leader_to;
}

