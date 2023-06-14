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

raft_node_id_t raft_get_nodeid(raft_server_t *me)
{
    return raft_node_get_id(me->node);
}

int raft_get_num_nodes(raft_server_t *me)
{
    return me->num_nodes;
}

int raft_get_num_voting_nodes(raft_server_t *me)
{
    int num = 0;

    for (int i = 0; i < me->num_nodes; i++) {
        if (raft_node_is_voting(me->nodes[i])) {
            num++;
        }
    }

    return num;
}

raft_time_t raft_get_timeout_elapsed(raft_server_t *me)
{
    return me->timeout_elapsed;
}

raft_index_t raft_get_log_count(raft_server_t *me)
{
    return me->log_impl->count(me->log);
}

raft_node_id_t raft_get_voted_for(raft_server_t *me)
{
    return me->voted_for;
}

int raft_set_current_term(raft_server_t *me, const raft_term_t term)
{
    if (me->current_term >= term) {
        return 0;
    }

    if (me->cb.persist_metadata) {
        int e = me->cb.persist_metadata(me, me->udata, term, RAFT_NODE_ID_NONE);
        if (e != 0) {
            return e;
        }
    }

    me->current_term = term;
    me->voted_for = RAFT_NODE_ID_NONE;

    return 0;
}

raft_term_t raft_get_current_term(raft_server_t *me)
{
    return me->current_term;
}

raft_index_t raft_get_current_idx(raft_server_t *me)
{
    return me->log_impl->current_idx(me->log);
}

void raft_set_commit_idx(raft_server_t *me, raft_index_t idx)
{
    assert(me->commit_idx <= idx);
    assert(idx <= raft_get_current_idx(me));
    me->commit_idx = idx;
}

void raft_set_last_applied_idx(raft_server_t *me, raft_index_t idx)
{
    me->last_applied_idx = idx;
}

raft_index_t raft_get_last_applied_idx(raft_server_t *me)
{
    return me->last_applied_idx;
}

raft_term_t raft_get_last_applied_term(raft_server_t *me)
{
    return me->last_applied_term;
}

raft_index_t raft_get_commit_idx(raft_server_t *me)
{
    return me->commit_idx;
}

void raft_set_state(raft_server_t *me, int state)
{
    if (state == RAFT_STATE_LEADER) {
        me->leader_id = raft_node_get_id(me->node);
    }
    me->state = state;
}

int raft_get_state(raft_server_t *me)
{
    return me->state;
}

const char *raft_get_state_str(raft_server_t *me)
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

const char *raft_get_error_str(int err)
{
    switch (err) {
        case RAFT_ERR_NOT_LEADER:
            return "not leader";
        case RAFT_ERR_ONE_VOTING_CHANGE_ONLY:
            return "one voting change only";
        case RAFT_ERR_SHUTDOWN:
            return "shutdown";
        case RAFT_ERR_NOMEM:
            return "out of memory";
        case RAFT_ERR_SNAPSHOT_IN_PROGRESS:
            return "snapshot is in progress";
        case RAFT_ERR_SNAPSHOT_ALREADY_LOADED:
            return "snapshot already loaded";
        case RAFT_ERR_INVALID_NODEID:
            return "invalid node id";
        case RAFT_ERR_LEADER_TRANSFER_IN_PROGRESS:
            return "leader transfer is in progress";
        case RAFT_ERR_DONE:
            return "done";
        case RAFT_ERR_NOTFOUND:
            return "not found";
        case RAFT_ERR_MISUSE:
            return "misuse";
        case RAFT_ERR_TRYAGAIN:
            return "try again";
        default:
            return "unknown error";
    }
}

raft_node_t *raft_get_node(raft_server_t *me, raft_node_id_t nodeid)
{
    for (int i = 0; i < me->num_nodes; i++) {
        if (nodeid == raft_node_get_id(me->nodes[i])) {
            return me->nodes[i];
        }
    }

    return NULL;
}

raft_node_t *raft_get_my_node(raft_server_t *me)
{
    return me->node;
}

raft_node_t *raft_get_node_from_idx(raft_server_t *me, const raft_index_t idx)
{
    return me->nodes[idx];
}

raft_node_id_t raft_get_leader_id(raft_server_t *me)
{
    return me->leader_id;
}

raft_node_t *raft_get_leader_node(raft_server_t *me)
{
    return raft_get_node(me, me->leader_id);
}

void *raft_get_udata(raft_server_t *me)
{
    return me->udata;
}

int raft_is_follower(raft_server_t *me)
{
    return me->state == RAFT_STATE_FOLLOWER;
}

int raft_is_leader(raft_server_t *me)
{
    return me->state == RAFT_STATE_LEADER;
}

int raft_is_precandidate(raft_server_t *me)
{
    return me->state == RAFT_STATE_PRECANDIDATE;
}

int raft_is_candidate(raft_server_t *me)
{
    return me->state == RAFT_STATE_CANDIDATE;
}

raft_term_t raft_get_last_log_term(raft_server_t *me)
{
    raft_index_t current_idx = raft_get_current_idx(me);

    if (current_idx == 0) {
        return 0;
    }
    
    if (current_idx == me->snapshot_last_idx) {
        return me->snapshot_last_term;
    }

    raft_entry_t *ety = raft_get_entry_from_idx(me, current_idx);
    if (ety) {
        raft_term_t term = ety->term;
        raft_entry_release(ety);
        return term;
    }

    return 0;
}

int raft_snapshot_is_in_progress(raft_server_t *me)
{
    return me->snapshot_in_progress;
}

int raft_is_apply_allowed(raft_server_t *me)
{
    return !me->disable_apply &&
           (!me->snapshot_in_progress || me->nonblocking_apply);
}

raft_entry_t *raft_get_last_applied_entry(raft_server_t *me)
{
    if (me->last_applied_idx == 0) {
        return NULL;
    }
    return me->log_impl->get(me->log, me->last_applied_idx);
}

raft_index_t raft_get_snapshot_last_idx(raft_server_t *me)
{
    return me->snapshot_last_idx;
}

raft_term_t raft_get_snapshot_last_term(raft_server_t *me)
{
    return me->snapshot_last_term;
}

int raft_is_single_node_voting_cluster(raft_server_t *me)
{
    return (raft_get_num_voting_nodes(me) == 1 && raft_node_is_voting(me->node));
}

raft_msg_id_t raft_get_msg_id(raft_server_t *me)
{
    return me->msg_id;
}

raft_node_id_t raft_get_transfer_leader(raft_server_t *me)
{
    return me->node_transferring_leader_to;
}

void raft_get_server_stats(raft_server_t *me, raft_server_stats_t *stats)
{
    *stats = me->stats;
}
