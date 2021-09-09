/**
 * Copyright (c) 2013, Willem-Hendrik Thiart
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 *
 * @file
 * @brief Implementation of a Raft server
 * @author Willem Thiart himself@willemthiart.com
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <stdint.h>
#include <stdarg.h>

#include "raft.h"
#include "raft_private.h"

#ifndef min
    #define min(a, b) ((a) < (b) ? (a) : (b))
#endif

#ifndef max
    #define max(a, b) ((a) < (b) ? (b) : (a))
#endif

#ifndef __GNUC__
    #define __attribute__(a)
#endif

void *(*raft_malloc)(size_t) = malloc;
void *(*raft_calloc)(size_t, size_t) = calloc;
void *(*raft_realloc)(void *, size_t) = realloc;
void (*raft_free)(void *) = free;

void raft_set_heap_functions(void *(*_malloc)(size_t),
                             void *(*_calloc)(size_t, size_t),
                             void *(*_realloc)(void *, size_t),
                             void (*_free)(void *))
{
    raft_malloc = _malloc;
    raft_calloc = _calloc;
    raft_realloc = _realloc;
    raft_free = _free;
}

static void raft_log_node(raft_server_t *me_,
                          raft_node_id_t id,
                          const char *fmt, ...) __attribute__ ((format (printf, 3, 4)));

static void raft_log_node(raft_server_t *me_,
                          raft_node_id_t id,
                          const char *fmt, ...)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (me->cb.log == NULL)
        return;

    char buf[1024];
    va_list args;

    va_start(args, fmt);
    int n = vsnprintf(buf, sizeof(buf), fmt, args);
    if (n < 0) {
        buf[0] = '\0';
    }
    va_end(args);

    me->cb.log(me_, id, me->udata, buf);
}

#define raft_log(me, ...) (raft_log_node(me, RAFT_UNKNOWN_NODE_ID, __VA_ARGS__))

void raft_randomize_election_timeout(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    /* [election_timeout, 2 * election_timeout) */
    me->election_timeout_rand = me->election_timeout + rand() % me->election_timeout;
    raft_log(me_, "randomize election timeout to %d", me->election_timeout_rand);
}

void raft_update_quorum_meta(raft_server_t* me_, raft_msg_id_t id)
{
    raft_server_private_t* me = (raft_server_private_t*) me_;

    // Make sure that timeout is greater than 'randomized election timeout'
    me->quorum_timeout = me->election_timeout * 2;
    me->last_acked_msg_id = id;
}

raft_server_t* raft_new_with_log(const raft_log_impl_t *log_impl, void *log_arg)
{
    raft_server_private_t* me = raft_calloc(1, sizeof(raft_server_private_t));
    if (!me)
        return NULL;

    me->current_term = 0;
    me->voted_for = -1;
    me->timeout_elapsed = 0;
    me->request_timeout = 200;
    me->election_timeout = 1000;

    raft_update_quorum_meta((raft_server_t*)me, me->msg_id);
    raft_randomize_election_timeout((raft_server_t*)me);

    me->log_impl = log_impl;
    me->log = me->log_impl->init(me, log_arg);
    if (!me->log) {
        raft_free(me);
        return NULL;
    }

    me->voting_cfg_change_log_idx = -1;
    raft_set_state((raft_server_t*)me, RAFT_STATE_FOLLOWER);
    me->leader_id = RAFT_UNKNOWN_NODE_ID;

    me->snapshot_in_progress = 0;
    raft_set_snapshot_metadata((raft_server_t*)me, 0, 0);

    return (raft_server_t*)me;
}

raft_server_t* raft_new(void)
{
    return raft_new_with_log(&raft_log_internal_impl, NULL);
}

void raft_set_callbacks(raft_server_t* me_, raft_cbs_t* funcs, void* udata)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    memcpy(&me->cb, funcs, sizeof(raft_cbs_t));
    me->udata = udata;
}

void raft_destroy(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    me->log_impl->free(me->log);
    raft_free(me_);
}

void raft_clear(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    me->current_term = 0;
    me->voted_for = -1;
    me->timeout_elapsed = 0;
    raft_randomize_election_timeout(me_);
    me->voting_cfg_change_log_idx = -1;
    raft_set_state((raft_server_t*)me, RAFT_STATE_FOLLOWER);
    me->leader_id = RAFT_UNKNOWN_NODE_ID;
    me->commit_idx = 0;
    me->last_applied_idx = 0;
    me->num_nodes = 0;
    me->node = NULL;
    me->log_impl->reset(me->log, 1, 1);
}

raft_node_t* raft_add_node_internal(raft_server_t* me_, raft_entry_t *ety, void* udata, raft_node_id_t id, int is_self)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (id == RAFT_UNKNOWN_NODE_ID)
        return NULL;

    /* set to voting if node already exists */
    raft_node_t* node = raft_get_node(me_, id);
    if (node)
    {
        if (!raft_node_is_voting(node))
        {
            raft_node_set_voting(node, 1);
            return node;
        }
        else
            /* we shouldn't add a node twice */
            return NULL;
    }

    node = raft_node_new(udata, id);
    if (!node)
        return NULL;

    void* p = raft_realloc(me->nodes, sizeof(void*) * (me->num_nodes + 1));
    if (!p) {
        raft_node_free(node);
        return NULL;
    }
    me->num_nodes++;
    me->nodes = p;
    me->nodes[me->num_nodes - 1] = node;
    if (is_self)
        me->node = me->nodes[me->num_nodes - 1];

    node = me->nodes[me->num_nodes - 1];

    if (me->cb.notify_membership_event)
        me->cb.notify_membership_event(me_, raft_get_udata(me_), node, ety, RAFT_MEMBERSHIP_ADD);

    return node;
}

static raft_node_t* raft_add_non_voting_node_internal(raft_server_t* me_, raft_entry_t *ety, void* udata, raft_node_id_t id, int is_self)
{
    if (raft_get_node(me_, id))
        return NULL;

    raft_node_t* node = raft_add_node_internal(me_, ety, udata, id, is_self);
    if (!node)
        return NULL;

    raft_node_set_voting(node, 0);
    return node;
}

raft_node_t* raft_add_node(raft_server_t* me_, void* udata, raft_node_id_t id, int is_self)
{
    return raft_add_node_internal(me_, NULL, udata, id, is_self);
}

raft_node_t* raft_add_non_voting_node(raft_server_t* me_, void* udata, raft_node_id_t id, int is_self)
{
    return raft_add_non_voting_node_internal(me_, NULL, udata, id, is_self);
}

void raft_remove_node(raft_server_t* me_, raft_node_t* node)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (me->cb.notify_membership_event)
        me->cb.notify_membership_event(me_, raft_get_udata(me_), node, NULL, RAFT_MEMBERSHIP_REMOVE);

    assert(node);

    int i, found = 0;
    for (i = 0; i < me->num_nodes; i++)
    {
        if (me->nodes[i] == node)
        {
            found = 1;
            break;
        }
    }
    assert(found);
    memmove(&me->nodes[i], &me->nodes[i + 1], sizeof(*me->nodes) * (me->num_nodes - i - 1));
    me->num_nodes--;

    if (me->leader_id == raft_node_get_id(node)) {
        me->leader_id = RAFT_UNKNOWN_NODE_ID;
    }

    raft_node_free(node);
}

void raft_handle_append_cfg_change(raft_server_t* me_, raft_entry_t* ety, raft_index_t idx)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (!raft_entry_is_cfg_change(ety))
        return;

    if (!me->cb.log_get_node_id)
        return;

    void* udata = raft_get_udata(me_);
    raft_node_id_t node_id = me->cb.log_get_node_id(me_, udata, ety, idx);
    raft_node_t* node = raft_get_node(me_, node_id);
    int is_self = node_id == raft_get_nodeid(me_);

    switch (ety->type)
    {
        case RAFT_LOGTYPE_ADD_NONVOTING_NODE:
            if (!is_self)
            {
                if (node && !raft_node_is_active(node))
                {
                    raft_node_set_active(node, 1);
                }
                else if (!node)
                {
                    node = raft_add_non_voting_node_internal(me_, ety, NULL, node_id, is_self);
                    assert(node);
                }
             }
            break;

        case RAFT_LOGTYPE_ADD_NODE:
            node = raft_add_node_internal(me_, ety, NULL, node_id, is_self);
            assert(node);
            assert(raft_node_is_voting(node));
            break;

        case RAFT_LOGTYPE_REMOVE_NODE:
            if (node) {
                raft_node_set_active(node, 0);
            }
            break;

        default:
            assert(0);
    }
}

void raft_handle_remove_cfg_change(raft_server_t* me_, raft_entry_t* ety, const raft_index_t idx)
{
    raft_server_private_t* me = (raft_server_private_t*) me_;

    if (!raft_entry_is_cfg_change(ety))
        return;

    if (!me->cb.log_get_node_id)
        return;

    void* udata = raft_get_udata(me_);
    raft_node_id_t node_id = me->cb.log_get_node_id(me_, udata, ety, idx);
    raft_node_t* node = raft_get_node(me_, node_id);

    switch (ety->type)
    {
        case RAFT_LOGTYPE_REMOVE_NODE:
            raft_node_set_active(node, 1);
            break;

        case RAFT_LOGTYPE_ADD_NONVOTING_NODE:
            assert(node_id != raft_get_nodeid(me_));
            raft_remove_node(me_, node);
            break;

        case RAFT_LOGTYPE_ADD_NODE:
            raft_node_set_voting(node, 0);
            break;

        default:
            assert(0);
            break;
    }
}

int raft_delete_entry_from_idx(raft_server_t* me_, raft_index_t idx)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    assert(raft_get_commit_idx(me_) < idx);

    if (idx <= me->voting_cfg_change_log_idx)
        me->voting_cfg_change_log_idx = -1;

    return me->log_impl->pop(me->log, idx,
            (func_entry_notify_f) raft_handle_remove_cfg_change, me_);
}

int raft_election_start(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    raft_log(me_,
        "election starting: %d %d, term: %ld ci: %ld",
        me->election_timeout_rand, me->timeout_elapsed, me->current_term,
        raft_get_current_idx(me_));

    return raft_become_candidate(me_);
}

int raft_become_leader(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int i;

    raft_log(me_, "becoming leader term:%ld", raft_get_current_term(me_));
    if (me->cb.notify_state_event)
        me->cb.notify_state_event(me_, raft_get_udata(me_), RAFT_STATE_LEADER);

    raft_index_t next_idx = raft_get_current_idx(me_) + 1;

    if (raft_get_current_term(me_) > 1) {
        raft_entry_t *noop = raft_entry_new(0);
        noop->term = raft_get_current_term(me_);
        noop->type = RAFT_LOGTYPE_NO_OP;

        int e = raft_append_entry(me_, noop);
        raft_entry_release(noop);
        if (0 != e)
            return e;

        // Commit noop immediately if this is a single node cluster
        if (raft_is_single_node_voting_cluster(me_)) {
            raft_set_commit_idx(me_, raft_get_current_idx(me_));
        }
    }

    raft_set_state(me_, RAFT_STATE_LEADER);
    raft_update_quorum_meta(me_, me->msg_id);
    me->timeout_elapsed = 0;

    for (i = 0; i < me->num_nodes; i++)
    {
        raft_node_t* node = me->nodes[i];

        if (me->node == node)
            continue;

        raft_node_set_next_idx(node, next_idx);
        raft_node_set_match_idx(node, 0);
        raft_send_appendentries(me_, node);
    }

    return 0;
}

int raft_become_candidate(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int i;

    raft_log(me_, "becoming candidate");
    if (me->cb.notify_state_event)
        me->cb.notify_state_event(me_, raft_get_udata(me_), RAFT_STATE_CANDIDATE);

    int e = raft_set_current_term(me_, raft_get_current_term(me_) + 1);
    if (0 != e)
        return e;
    for (i = 0; i < me->num_nodes; i++)
        raft_node_vote_for_me(me->nodes[i], 0);

    if (raft_node_is_voting(me->node))
        raft_vote(me_, me->node);

    me->leader_id = RAFT_UNKNOWN_NODE_ID;
    raft_set_state(me_, RAFT_STATE_CANDIDATE);

    raft_randomize_election_timeout(me_);
    me->timeout_elapsed = 0;

    for (i = 0; i < me->num_nodes; i++)
    {
        raft_node_t* node = me->nodes[i];

        if (me->node != node && raft_node_is_voting(node))
        {
            raft_send_requestvote(me_, node);
        }
    }
    return 0;
}

void raft_become_follower(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    raft_log(me_, "becoming follower");
    if (me->cb.notify_state_event)
        me->cb.notify_state_event(me_, raft_get_udata(me_), RAFT_STATE_FOLLOWER);

    raft_set_state(me_, RAFT_STATE_FOLLOWER);
    raft_randomize_election_timeout(me_);
    me->timeout_elapsed = 0;
}

static int msgid_cmp(const void *a, const void *b)
{
    raft_msg_id_t va = *((raft_msg_id_t*) a);
    raft_msg_id_t vb = *((raft_msg_id_t*) b);

    return va > vb ? -1 : 1;
}

static raft_msg_id_t quorum_msg_id(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*) me_;
    raft_msg_id_t msg_ids[me->num_nodes];
    int num_voters = 0;

    for (int i = 0; i < me->num_nodes; i++) {
        raft_node_t* node = me->nodes[i];

        if (!raft_node_is_voting(node))
            continue;

        if (me->node == node) {
            msg_ids[num_voters++] = me->msg_id;
        } else {
            msg_ids[num_voters++] = raft_node_get_last_acked_msgid(node);
        }
    }

    assert(num_voters == raft_get_num_voting_nodes(me_));

    /**
     *  Sort the acknowledged msg_ids in the descending order and return
     *  the median value. Median value means it's the highest msg_id
     *  acknowledged by the majority.
     */
    qsort(msg_ids, num_voters, sizeof(raft_msg_id_t), msgid_cmp);

    return msg_ids[num_voters / 2];
}

int raft_periodic(raft_server_t* me_, int msec_since_last_period)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    me->timeout_elapsed += msec_since_last_period;

    /* Only one voting node means it's safe for us to become the leader */
    if (raft_is_single_node_voting_cluster(me_) && !raft_is_leader(me_)) {
        // need to update term on new leadership
        int e = raft_set_current_term(me_, raft_get_current_term(me_) + 1);
        if (e != 0) {
            return e;
        }

        e = raft_become_leader(me_);
        if (e != 0) {
            return e;
        }
    }

    if (me->state == RAFT_STATE_LEADER)
    {
        if (me->request_timeout <= me->timeout_elapsed)
        {
            me->msg_id++;
            raft_send_appendentries_all(me_);
        }

        me->quorum_timeout -= msec_since_last_period;
        if (me->quorum_timeout < 0)
        {
            /**
             * Check-quorum implementation
             *
             * Periodically (every quorum_timeout), we enter this check to
             * verify quorum exists. The current 'quorum msg id' should be
             * greater than the last time we were here. It means we've got
             * responses for append entry requests from the cluster, so we
             * conclude that cluster is operational and quorum exists. In that
             * case, we save the current quorum msg id and update the
             * quorum_timeout timer. Otherwise, it means quorum does not exist.
             * We should step down and become a follower.
             */
            raft_msg_id_t quorum_id = quorum_msg_id(me_);

            if (me->last_acked_msg_id == quorum_id)
            {
                raft_log(me_, "quorum does not exist, stepping down");
                raft_become_follower(me_);
            }

            raft_update_quorum_meta(me_, quorum_id);
        }
    }
    else if (me->election_timeout_rand <= me->timeout_elapsed &&
        /* Don't become the leader when building snapshots or bad things will
         * happen when we get a client request */
        !raft_snapshot_is_in_progress(me_))
    {
        int e = raft_election_start(me_);
        if (0 != e)
            return e;
    }

    if (me->last_applied_idx < raft_get_commit_idx(me_) &&
            raft_is_apply_allowed(me_))
    {
        int e = raft_apply_all(me_);
        if (0 != e)
            return e;
    }

    raft_process_read_queue(me_);

    return 0;
}

raft_entry_t* raft_get_entry_from_idx(raft_server_t* me_, raft_index_t etyidx)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    return me->log_impl->get(me->log, etyidx);
}

int raft_voting_change_is_in_progress(raft_server_t* me_)
{
    return ((raft_server_private_t*)me_)->voting_cfg_change_log_idx != -1;
}

int raft_recv_appendentries_response(raft_server_t* me_,
                                     raft_node_t* node,
                                     msg_appendentries_response_t* r)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    raft_log_node(me_, raft_node_get_id(node),
          "received appendentries response %s ci:%ld rci:%ld msgid:%lu",
          r->success == 1 ? "SUCCESS" : "fail",
          raft_get_current_idx(me_),
          r->current_idx, r->msg_id);

    if (!node)
        return -1;

    if (!raft_is_leader(me_))
        return RAFT_ERR_NOT_LEADER;

    if (raft_node_get_last_acked_msgid(node) > r->msg_id) {
        // this was received out of order and is now irrelevant.
        return 0;
    }

    /* If response contains term T > currentTerm: set currentTerm = T
       and convert to follower (§5.3) */
    if (me->current_term < r->term)
    {
        int e = raft_set_current_term(me_, r->term);
        if (0 != e)
            return e;
        raft_become_follower(me_);
        me->leader_id = RAFT_UNKNOWN_NODE_ID;
        return 0;
    }
    else if (me->current_term != r->term)
        return 0;

    // if we got here, it means that the follower has acked us as a leader, even if it cant accept the append_entry
    raft_node_set_last_ack(node, r->msg_id, r->term);

    raft_index_t match_idx = raft_node_get_match_idx(node);

    if (0 == r->success)
    {
        /* If AppendEntries fails because of log inconsistency:
           decrement nextIndex and retry (§5.3) */
        raft_index_t next_idx = r->prev_log_idx + 1;
        assert(0 < next_idx);
        /* Stale response -- ignore */
        if (r->current_idx < match_idx)
            return 0;
        if (r->current_idx < next_idx - 1)
            raft_node_set_next_idx(node, min(r->current_idx + 1, raft_get_current_idx(me_)));
        else
            raft_node_set_next_idx(node, next_idx - 1);

        /* retry */
        raft_send_appendentries(me_, node);
        return 0;
    }


    if (!raft_node_is_voting(node) &&
        !raft_voting_change_is_in_progress(me_) &&
        raft_get_current_idx(me_) <= r->current_idx + 1 &&
        !raft_node_is_voting_committed(node) &&
        raft_node_is_addition_committed(node) &&
        me->cb.node_has_sufficient_logs &&
        0 == raft_node_has_sufficient_logs(node)
        )
    {
        int e = me->cb.node_has_sufficient_logs(me_, me->udata, node);
        if (0 == e)
            raft_node_set_has_sufficient_logs(node);
    }

    if (r->current_idx <= match_idx)
        return 0;

    assert(r->current_idx <= raft_get_current_idx(me_));

    raft_node_set_next_idx(node, r->current_idx + 1);
    raft_node_set_match_idx(node, r->current_idx);

    /* Update commit idx */
    raft_index_t point = r->current_idx;
    if (point)
    {
        raft_entry_t* ety = raft_get_entry_from_idx(me_, point);
        if (raft_get_commit_idx(me_) < point && ety->term == me->current_term)
        {
            int votes = raft_node_is_voting(me->node) ? 1 : 0;
            for (int i = 0; i < me->num_nodes; i++)
            {
                raft_node_t* follower = me->nodes[i];
                if (me->node != follower &&
                    raft_node_is_voting(follower) &&
                    point <= raft_node_get_match_idx(follower))
                {
                    votes++;
                }
            }

            if (raft_get_num_voting_nodes(me_) / 2 < votes)
                raft_set_commit_idx(me_, point);
        }
        if (ety)
            raft_entry_release(ety);
    }

    /* Aggressively send remaining entries */
    if (raft_node_get_next_idx(node) <= raft_get_current_idx(me_))
        raft_send_appendentries(me_, node);

    /* periodic applies committed entries lazily */

    return 0;
}

int raft_recv_appendentries(
    raft_server_t* me_,
    raft_node_t* node,
    msg_appendentries_t* ae,
    msg_appendentries_response_t *r
    )
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int e = 0;

    if (0 < ae->n_entries)
    {
        raft_log_node(
            me_, ae->leader_id,
            "recvd appendentries li:%d t:%ld ci:%ld lc:%ld pli:%ld plt:%ld #%d",
            ae->leader_id, ae->term, raft_get_current_idx(me_),
            ae->leader_commit, ae->prev_log_idx, ae->prev_log_term,
            ae->n_entries);
    }

    r->msg_id = ae->msg_id;
    r->prev_log_idx = ae->prev_log_idx;
    r->success = 0;

    if (raft_is_candidate(me_) && me->current_term == ae->term)
    {
        raft_become_follower(me_);
    }
    else if (me->current_term < ae->term)
    {
        e = raft_set_current_term(me_, ae->term);
        if (0 != e)
            goto out;
        raft_become_follower(me_);
    }
    else if (ae->term < me->current_term)
    {
        /* 1. Reply false if term < currentTerm (§5.1) */
        raft_log_node(me_, ae->leader_id,
                    "AE term %ld is less than current term %ld",
                    ae->term, me->current_term);
        goto out;
    }

    /* update current leader because ae->term is up to date */
    me->leader_id = ae->leader_id;
    me->timeout_elapsed = 0;

    /* Not the first appendentries we've received */
    /* NOTE: the log starts at 1 */
    if (0 < ae->prev_log_idx)
    {
        raft_entry_t* ety = raft_get_entry_from_idx(me_, ae->prev_log_idx);

        /* Is a snapshot */
        if (ae->prev_log_idx == me->snapshot_last_idx)
        {
            if (me->snapshot_last_term != ae->prev_log_term)
            {
                /* Should never happen; something is seriously wrong! */
                raft_log_node(me_, ae->leader_id,
                            "Snapshot AE prev conflicts with committed entry");
                e = RAFT_ERR_SHUTDOWN;
                if (ety)
                    raft_entry_release(ety);
                goto out;
            }
        }
        /* 2. Reply false if log doesn't contain an entry at prevLogIndex
           whose term matches prevLogTerm (§5.3) */
        else if (!ety)
        {
            raft_log_node(me_, ae->leader_id,
                      "AE no log at prev_idx %ld", ae->prev_log_idx);
            goto out;
        }
        else if (ety->term != ae->prev_log_term)
        {
            raft_log_node(me_, ae->leader_id, "AE term doesn't match prev_term (ie. %ld vs %ld) ci:%ld comi:%ld lcomi:%ld pli:%ld",
                  ety->term, ae->prev_log_term, raft_get_current_idx(me_),
                  raft_get_commit_idx(me_), ae->leader_commit, ae->prev_log_idx);
            if (ae->prev_log_idx <= raft_get_commit_idx(me_))
            {
                /* Should never happen; something is seriously wrong! */
                raft_log_node(me_, ae->leader_id,
                            "AE prev conflicts with committed entry");
                e = RAFT_ERR_SHUTDOWN;
                raft_entry_release(ety);
                goto out;
            }
            /* Delete all the following log entries because they don't match */
            e = raft_delete_entry_from_idx(me_, ae->prev_log_idx);
            raft_entry_release(ety);
            goto out;
        }
        if (ety)
            raft_entry_release(ety);
    }

    r->success = 1;
    r->current_idx = ae->prev_log_idx;

    /* 3. If an existing entry conflicts with a new one (same index
       but different terms), delete the existing entry and all that
       follow it (§5.3) */
    int i;
    for (i = 0; i < ae->n_entries; i++)
    {
        raft_entry_t* ety = ae->entries[i];
        raft_index_t ety_index = ae->prev_log_idx + 1 + i;

        raft_entry_t* existing_ety = raft_get_entry_from_idx(me_, ety_index);
        raft_term_t existing_term = existing_ety ? existing_ety->term : 0;
        if (existing_ety)
            raft_entry_release(existing_ety);

        if (existing_ety && existing_term != ety->term)
        {
            if (ety_index <= raft_get_commit_idx(me_))
            {
                /* Should never happen; something is seriously wrong! */
                raft_log_node(me_, ae->leader_id, "AE entry conflicts with committed entry ci:%ld comi:%ld lcomi:%ld pli:%ld",
                      raft_get_current_idx(me_), raft_get_commit_idx(me_),
                      ae->leader_commit, ae->prev_log_idx);
                e = RAFT_ERR_SHUTDOWN;
                goto out;
            }
            e = raft_delete_entry_from_idx(me_, ety_index);
            if (0 != e)
                goto out;
            break;
        }
        else if (!existing_ety)
            break;
        r->current_idx = ety_index;
    }

    /* Pick up remainder in case of mismatch or missing entry */
    for (; i < ae->n_entries; i++)
    {
        e = raft_append_entry(me_, ae->entries[i]);
        if (0 != e)
            goto out;
        r->current_idx = ae->prev_log_idx + 1 + i;
    }

    /* 4. If leaderCommit > commitIndex, set commitIndex =
        min(leaderCommit, index of most recent entry) */
    if (raft_get_commit_idx(me_) < ae->leader_commit)
    {
        raft_index_t last_log_idx = max(raft_get_current_idx(me_), 1);
        raft_set_commit_idx(me_, min(last_log_idx, ae->leader_commit));
    }

out:
    r->term = me->current_term;
    if (0 == r->success)
        r->current_idx = raft_get_current_idx(me_);
    return e;
}

int raft_already_voted(raft_server_t* me_)
{
    return ((raft_server_private_t*)me_)->voted_for != -1;
}

static int raft_should_grant_vote(raft_server_t* me_, msg_requestvote_t* vr)
{
    if (vr->term < raft_get_current_term(me_))
        return 0;

    /* TODO: if voted for is candidate return 1 (if below checks pass) */
    if (raft_already_voted(me_))
        return 0;

    /* Below we check if log is more up-to-date... */

    raft_index_t current_idx = raft_get_current_idx(me_);

    /* Our log is definitely not more up-to-date if it's empty! */
    if (0 == current_idx)
        return 1;

    raft_term_t ety_term = raft_get_last_log_term(me_);

    if (ety_term < vr->last_log_term)
        return 1;

    if (vr->last_log_term == ety_term && current_idx <= vr->last_log_idx)
        return 1;

    return 0;
}

int raft_recv_requestvote(raft_server_t* me_,
                          raft_node_t* node,
                          msg_requestvote_t* vr,
                          msg_requestvote_response_t *r)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int e = 0;

    r->vote_granted = 0;

    if (!node)
        node = raft_get_node(me_, vr->candidate_id);

    /* Reject request if we have a leader */
    if (me->leader_id != RAFT_UNKNOWN_NODE_ID &&
        me->leader_id != vr->candidate_id &&
        me->timeout_elapsed < me->election_timeout) {
        goto done;
    }

    if (raft_get_current_term(me_) < vr->term)
    {
        e = raft_set_current_term(me_, vr->term);
        if (0 != e) {
            goto done;
        }
        raft_become_follower(me_);
        me->leader_id = RAFT_UNKNOWN_NODE_ID;
    }

    if (raft_should_grant_vote(me_, vr))
    {
        /* It shouldn't be possible for a leader or candidate to grant a vote
         * Both states would have voted for themselves */
        assert(!(raft_is_leader(me_) || raft_is_candidate(me_)));

        e = raft_vote_for_nodeid(me_, vr->candidate_id);
        if (0 == e)
            r->vote_granted = 1;

        /* must be in an election. */
        me->leader_id = RAFT_UNKNOWN_NODE_ID;
        me->timeout_elapsed = 0;
    }

done:
    raft_log_node(me_, vr->candidate_id, "node requested vote: %d replying: %s",
          vr->candidate_id,
          r->vote_granted == 1 ? "granted" :
          r->vote_granted == 0 ? "not granted" : "unknown");

    r->term = raft_get_current_term(me_);
    return e;
}

int raft_votes_is_majority(const int num_nodes, const int nvotes)
{
    if (num_nodes < nvotes)
        return 0;
    int half = num_nodes / 2;
    return half + 1 <= nvotes;
}

int raft_recv_requestvote_response(raft_server_t* me_,
                                   raft_node_t* node,
                                   msg_requestvote_response_t* r)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    raft_log_node(me_, raft_node_get_id(node),
               "node responded to requestvote status:%s ct:%ld rt:%ld",
               r->vote_granted == 1 ? "granted" :
               r->vote_granted == 0 ? "not granted" : "unknown",
               me->current_term,
               r->term);

    if (!raft_is_candidate(me_) || raft_get_current_term(me_) > r->term)
    {
        return 0;
    }

    if (raft_get_current_term(me_) < r->term)
    {
        int e = raft_set_current_term(me_, r->term);
        if (0 != e)
            return e;
        raft_become_follower(me_);
        me->leader_id = RAFT_UNKNOWN_NODE_ID;
        return 0;
    }

    if (r->vote_granted)
    {
        if (node)
            raft_node_vote_for_me(node, 1);
        int votes = raft_get_nvotes_for_me(me_);
        if (raft_votes_is_majority(raft_get_num_voting_nodes(me_), votes)) {
            int e = raft_become_leader(me_);
            if (0 != e)
                return e;
        }
    }

    return 0;
}

int raft_recv_entry(raft_server_t* me_,
                    msg_entry_t* ety,
                    msg_entry_response_t *r)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int i;

    if (raft_entry_is_voting_cfg_change(ety))
    {
        /* Only one voting cfg change at a time */
        if (raft_voting_change_is_in_progress(me_))
            return RAFT_ERR_ONE_VOTING_CHANGE_ONLY;

        /* Multi-threading: need to fail here because user might be
         * snapshotting membership settings. */
        if (!raft_is_apply_allowed(me_))
            return RAFT_ERR_SNAPSHOT_IN_PROGRESS;
    }

    if (!raft_is_leader(me_))
        return RAFT_ERR_NOT_LEADER;

    raft_log(me_, "received entry t:%ld id: %d idx: %ld",
          me->current_term, ety->id, raft_get_current_idx(me_) + 1);

    ety->term = me->current_term;
    int e = raft_append_entry(me_, ety);
    if (0 != e)
        return e;

    for (i = 0; i < me->num_nodes; i++)
    {
        raft_node_t* node = me->nodes[i];

        if (me->node == node || !node)
           continue;

        /* Only send new entries.
         * Don't send the entry to peers who are behind, to prevent them from
         * becoming congested. */
        raft_index_t next_idx = raft_node_get_next_idx(node);
        if (next_idx == raft_get_current_idx(me_))
            raft_send_appendentries(me_, node);
    }

    /* if we are the only voter, commit now, as no appendentries_response will occur */
    if (raft_is_single_node_voting_cluster(me_)) {
        raft_set_commit_idx(me_, raft_get_current_idx(me_));
    }

    r->id = ety->id;
    r->idx = raft_get_current_idx(me_);
    r->term = me->current_term;

    /* FIXME: is this required if raft_append_entry does this too? */
    if (raft_entry_is_voting_cfg_change(ety))
        me->voting_cfg_change_log_idx = raft_get_current_idx(me_);

    return 0;
}

int raft_send_requestvote(raft_server_t* me_, raft_node_t* node)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    msg_requestvote_t rv;
    int e = 0;

    assert(node);
    assert(node != me->node);

    raft_node_id_t id = raft_node_get_id(node);
    raft_log_node(me_, id, "sending requestvote to: %d", id);

    rv.term = me->current_term;
    rv.last_log_idx = raft_get_current_idx(me_);
    rv.last_log_term = raft_get_last_log_term(me_);
    rv.candidate_id = raft_get_nodeid(me_);
    if (me->cb.send_requestvote)
        e = me->cb.send_requestvote(me_, me->udata, node, &rv);
    return e;
}

int raft_append_entry(raft_server_t* me_, raft_entry_t* ety)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    int e = me->log_impl->append(me->log, ety);
    if (e < 0)
        return e;

    if (raft_entry_is_voting_cfg_change(ety))
        me->voting_cfg_change_log_idx = raft_get_current_idx(me_) - 1;

    if (raft_entry_is_cfg_change(ety)) {
        raft_handle_append_cfg_change(me_, ety, raft_get_current_idx(me_));
    }

    return 0;
}

int raft_apply_entry(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (!raft_is_apply_allowed(me_))
        return -1;

    /* Don't apply after the commit_idx */
    if (me->last_applied_idx == raft_get_commit_idx(me_))
        return -1;

    raft_index_t log_idx = me->last_applied_idx + 1;

    raft_entry_t* ety = raft_get_entry_from_idx(me_, log_idx);
    if (!ety)
        return -1;

    raft_log(me_, "applying log: %ld, id: %d size: %d",
          log_idx, ety->id, ety->data_len);

    me->last_applied_idx++;
    if (me->cb.applylog)
    {
        int e = me->cb.applylog(me_, me->udata, ety, me->last_applied_idx);
        if (RAFT_ERR_SHUTDOWN == e) {
            raft_entry_release(ety);
            return RAFT_ERR_SHUTDOWN;
        }
    }

    /* voting cfg change is now complete.
     * TODO: there seem to be a possible off-by-one bug hidden here, requiring
     * checking log_idx >= voting_cfg_change_log_idx rather than plain ==.
     */
    if (log_idx >= me->voting_cfg_change_log_idx)
        me->voting_cfg_change_log_idx = -1;

    if (!raft_entry_is_cfg_change(ety))
        goto exit;

    raft_node_id_t node_id = me->cb.log_get_node_id(me_, raft_get_udata(me_), ety, log_idx);
    raft_node_t* node = raft_get_node(me_, node_id);

    switch (ety->type) {
        case RAFT_LOGTYPE_ADD_NODE:
            raft_node_set_addition_committed(node, 1);
            raft_node_set_voting_committed(node, 1);
            /* Membership Change: confirm connection with cluster */
            raft_node_set_has_sufficient_logs(node);
            if (node_id == raft_get_nodeid(me_))
                me->connected = RAFT_NODE_STATUS_CONNECTED;
            break;
        case RAFT_LOGTYPE_ADD_NONVOTING_NODE:
            raft_node_set_addition_committed(node, 1);
            break;
        case RAFT_LOGTYPE_REMOVE_NODE:
            if (node) {
                raft_remove_node(me_, node);
            }
            break;
        default:
            break;
    }

exit:

    raft_entry_release(ety);
    return 0;
}

raft_entry_t** raft_get_entries_from_idx(raft_server_t* me_, raft_index_t idx, int* n_etys)
{
    if (raft_get_current_idx(me_) < idx) {
        *n_etys = 0;
        return NULL;
    }

    raft_server_private_t* me = (raft_server_private_t*)me_;
    raft_index_t size = raft_get_current_idx(me_) - idx + 1;
    raft_entry_t **e = raft_malloc(size * sizeof(raft_entry_t*));
    int n = me->log_impl->get_batch(me->log, idx, (int) size, e);

    if (n < 1) {
        raft_free(e);
        *n_etys = 0;
        return NULL;
    }

    *n_etys = n;
    return e;
}

int raft_send_appendentries(raft_server_t* me_, raft_node_t* node)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    assert(node);
    assert(node != me->node);

    if (!raft_node_is_active(node)) {
        return 0;
    }

    raft_index_t next_idx = raft_node_get_next_idx(node);

    /* figure out if the client needs a snapshot sent */
    if (me->snapshot_last_idx > 0 && next_idx <= me->snapshot_last_idx)
    {
        if (me->cb.send_snapshot)
            me->cb.send_snapshot(me_, me->udata, node);

        return RAFT_ERR_NEEDS_SNAPSHOT;
    }

    if (!me->cb.send_appendentries)
        return -1;

    msg_appendentries_t ae = {
        .term = me->current_term,
        .leader_commit = raft_get_commit_idx(me_),
        .msg_id = ++me->msg_id,
    };

    ae.entries = raft_get_entries_from_idx(me_, next_idx, &ae.n_entries);
    assert((!ae.entries && 0 == ae.n_entries) ||
            (ae.entries && 0 < ae.n_entries));

    /* previous log is the log just before the new logs */
    if (next_idx > 1)
    {
        raft_entry_t* prev_ety = raft_get_entry_from_idx(me_, next_idx - 1);
        if (!prev_ety)
        {
            ae.prev_log_idx = me->snapshot_last_idx;
            ae.prev_log_term = me->snapshot_last_term;
        }
        else
        {
            ae.prev_log_idx = next_idx - 1;
            ae.prev_log_term = prev_ety->term;
            raft_entry_release(prev_ety);
        }
    }

    raft_log_node(me_,
              raft_node_get_id(node),
              "sending appendentries: ci:%lu comi:%lu t:%lu lc:%lu pli:%lu plt:%lu msgid:%lu #%d",
              raft_get_current_idx(me_),
              raft_get_commit_idx(me_),
              ae.term,
              ae.leader_commit,
              ae.prev_log_idx,
              ae.prev_log_term,
              ae.msg_id,
              ae.n_entries);

    int res = me->cb.send_appendentries(me_, me->udata, node, &ae);
    raft_entry_release_list(ae.entries, ae.n_entries);
    raft_free(ae.entries);

    return res;
}

int raft_send_appendentries_all(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int i, e;
    int ret = 0;

    me->timeout_elapsed = 0;
    for (i = 0; i < me->num_nodes; i++)
    {
        if (me->node == me->nodes[i])
            continue;

        e = raft_send_appendentries(me_, me->nodes[i]);
        if (0 != e)
            ret = e;
    }

    return ret;
}

int raft_get_nvotes_for_me(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int i, votes;

    for (i = 0, votes = 0; i < me->num_nodes; i++)
    {
        if (me->node != me->nodes[i] &&
            raft_node_is_voting(me->nodes[i]) &&
            raft_node_has_vote_for_me(me->nodes[i]))
        {
            votes += 1;
        }
    }

    if (me->voted_for == raft_get_nodeid(me_))
        votes += 1;

    return votes;
}

int raft_vote(raft_server_t* me_, raft_node_t* node)
{
    return raft_vote_for_nodeid(me_, node ? raft_node_get_id(node) : -1);
}

int raft_vote_for_nodeid(raft_server_t* me_, const raft_node_id_t nodeid)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (me->cb.persist_vote) {
        int e = me->cb.persist_vote(me_, me->udata, nodeid);
        if (0 != e)
            return e;
    }
    me->voted_for = nodeid;
    return 0;
}

int raft_msg_entry_response_committed(raft_server_t* me_,
                                      const msg_entry_response_t* r)
{
    raft_entry_t* ety = raft_get_entry_from_idx(me_, r->idx);
    if (!ety)
        return 0;
    raft_term_t ety_term = ety->term;
    raft_entry_release(ety);

    /* entry from another leader has invalidated this entry message */
    if (r->term != ety_term)
        return -1;
    return r->idx <= raft_get_commit_idx(me_);
}

int raft_apply_all(raft_server_t* me_)
{
    if (!raft_is_apply_allowed(me_))
        return 0;

    while (raft_get_last_applied_idx(me_) < raft_get_commit_idx(me_))
    {
        int e = raft_apply_entry(me_);
        if (0 != e)
            return e;
    }

    return 0;
}

int raft_entry_is_voting_cfg_change(raft_entry_t* ety)
{
    return RAFT_LOGTYPE_ADD_NODE == ety->type ||
           RAFT_LOGTYPE_REMOVE_NODE == ety->type;
}

int raft_entry_is_cfg_change(raft_entry_t* ety)
{
    return (
        RAFT_LOGTYPE_ADD_NODE == ety->type ||
        RAFT_LOGTYPE_ADD_NONVOTING_NODE == ety->type ||
        RAFT_LOGTYPE_REMOVE_NODE == ety->type);
}

int raft_poll_entry(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    /* We should never drop uncommitted entries */
    assert(me->log_impl->first_idx(me->log) <= raft_get_commit_idx(me_));

    int e = me->log_impl->poll(me->log, me->log_impl->first_idx(me->log));
    if (e != 0)
        return e;

    return 0;
}

int raft_pop_entry(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    raft_index_t cur_idx = me->log_impl->current_idx(me->log);

    return me->log_impl->pop(me->log, cur_idx,
            (func_entry_notify_f) raft_handle_remove_cfg_change, me_);
}

raft_index_t raft_get_first_entry_idx(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    assert(0 < raft_get_current_idx(me_));

    if (me->snapshot_last_idx == 0)
        return 1;

    return me->snapshot_last_idx;
}

raft_index_t raft_get_num_snapshottable_logs(raft_server_t *me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    if (raft_get_log_count(me_) <= 1)
        return 0;
    return raft_get_commit_idx(me_) - me->log_impl->first_idx(me->log) + 1;
}

int raft_begin_snapshot(raft_server_t *me_, int flags)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (raft_get_num_snapshottable_logs(me_) == 0)
        return -1;

    raft_index_t snapshot_target = raft_get_commit_idx(me_);
    if (!snapshot_target)
        return -1;

    raft_entry_t* ety = raft_get_entry_from_idx(me_, snapshot_target);
    if (!ety)
        return -1;
    raft_term_t ety_term = ety->term;
    raft_entry_release(ety);

    /* we need to get all the way to the commit idx */
    int e = raft_apply_all(me_);
    if (e != 0)
        return e;

    assert(raft_get_commit_idx(me_) == raft_get_last_applied_idx(me_));

    raft_set_snapshot_metadata(me_, ety_term, snapshot_target);
    me->snapshot_in_progress = 1;
    me->snapshot_flags = flags;

    raft_log(me_,
        "begin snapshot sli:%ld slt:%ld slogs:%ld",
        me->snapshot_last_idx,
        me->snapshot_last_term,
        raft_get_num_snapshottable_logs(me_));

    return 0;
}

int raft_cancel_snapshot(raft_server_t *me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (!me->snapshot_in_progress)
        return -1;

    me->snapshot_last_idx = me->saved_snapshot_last_idx;
    me->snapshot_last_term = me->saved_snapshot_last_term;

    me->snapshot_in_progress = 0;

    return 0;
}

int raft_end_snapshot(raft_server_t *me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (!me->snapshot_in_progress || me->snapshot_last_idx == 0)
        return -1;

    // TODO: What is the purpose of this assert? Looks wrong.
    // assert(raft_get_num_snapshottable_logs(me_) != 0);

    /* If needed, remove compacted logs */
    int e = me->log_impl->poll(me->log, me->snapshot_last_idx + 1);
    if (e != 0)
        return e;

    me->snapshot_in_progress = 0;

    raft_log(me_,
        "end snapshot base:%ld commit-index:%ld current-index:%ld",
        me->log_impl->first_idx(me->log) - 1,
        raft_get_commit_idx(me_),
        raft_get_current_idx(me_));

    if (!raft_is_leader(me_))
        return 0;

    for (int i = 0; i < me->num_nodes; i++)
    {
        raft_node_t* node = me->nodes[i];

        if (me->node == node || !raft_node_is_active(node))
            continue;

        raft_index_t next_idx = raft_node_get_next_idx(node);

        /* figure out if the client needs a snapshot sent */
        if (me->snapshot_last_idx > 0 && next_idx <= me->snapshot_last_idx)
        {
            if (me->cb.send_snapshot)
                me->cb.send_snapshot(me_, me->udata, node);
        }
    }

    return 0;
}

int raft_begin_load_snapshot(
    raft_server_t *me_,
    raft_term_t last_included_term,
    raft_index_t last_included_index)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;

    if (last_included_index == -1)
        return -1;

    if (last_included_index == 0 || last_included_term == 0)
        return -1;

    /* loading the snapshot will break cluster safety */
    if (last_included_index < me->last_applied_idx)
        return -1;

    /* snapshot was unnecessary */
    if (last_included_index < raft_get_current_idx(me_))
        return -1;

    if (last_included_term == me->snapshot_last_term && last_included_index == me->snapshot_last_idx)
        return RAFT_ERR_SNAPSHOT_ALREADY_LOADED;

    if (me->current_term < last_included_term) {
        raft_set_current_term(me_, last_included_term);
        me->current_term = last_included_term;
    }

    raft_set_state((raft_server_t*)me, RAFT_STATE_FOLLOWER);
    me->leader_id = RAFT_UNKNOWN_NODE_ID;

    me->log_impl->reset(me->log, last_included_index + 1, last_included_term);

    if (raft_get_commit_idx(me_) < last_included_index)
        raft_set_commit_idx(me_, last_included_index);

    me->last_applied_idx = last_included_index;
    raft_set_snapshot_metadata(me_, last_included_term, me->last_applied_idx);

    /* remove all nodes but self */
    int i, my_node_by_idx = 0;
    for (i = 0; i < me->num_nodes; i++)
    {
        if (raft_get_nodeid(me_) == raft_node_get_id(me->nodes[i]))
            my_node_by_idx = i;
        else {
            raft_node_free(me->nodes[i]);
            me->nodes[i] = NULL;
        }
    }

    /* this will be realloc'd by a raft_add_node */
    me->nodes[0] = me->nodes[my_node_by_idx];
    me->num_nodes = 1;

    raft_log(me_,
        "loaded snapshot sli:%ld slt:%ld slogs:%ld",
        me->snapshot_last_idx,
        me->snapshot_last_term,
        raft_get_num_snapshottable_logs(me_));

    return 0;
}

int raft_end_load_snapshot(raft_server_t *me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    int i;

    /* Set nodes' voting status as committed */
    for (i = 0; i < me->num_nodes; i++)
    {
        raft_node_t* node = me->nodes[i];
        raft_node_set_voting_committed(node, raft_node_is_voting(node));
        raft_node_set_addition_committed(node, 1);
        if (raft_node_is_voting(node))
            raft_node_set_has_sufficient_logs(node);
    }

    return 0;
}

void *raft_get_log(raft_server_t *me_)
{
    raft_server_private_t* me = (raft_server_private_t*)me_;
    return me->log;
}

raft_entry_t *raft_entry_new(unsigned int data_len)
{
    raft_entry_t *ety = raft_calloc(1, sizeof(raft_entry_t) + data_len);
    ety->data_len = data_len;
    ety->refs = 1;

    return ety;
}

void raft_entry_hold(raft_entry_t *ety)
{
    assert(ety->refs < UINT16_MAX);
    ety->refs++;
}

void raft_entry_release(raft_entry_t *ety)
{
    assert(ety->refs > 0);
    ety->refs--;

    if (!ety->refs) {
        if (ety->free_func) {
            ety->free_func(ety);
        } else {
            raft_free(ety);
        }
    }
}

void raft_entry_release_list(raft_entry_t **ety_list, size_t len)
{
    size_t i;

    for (i = 0; i < len; i++) {
        raft_entry_release(ety_list[i]);
    }
}

void raft_queue_read_request(raft_server_t* me_, func_read_request_callback_f cb, void *cb_arg)
{
    raft_server_private_t* me = (raft_server_private_t*) me_;

    raft_read_request_t *req = raft_malloc(sizeof(raft_read_request_t));

    req->read_idx = raft_get_current_idx(me_);
    req->read_term = raft_get_current_term(me_);
    req->msg_id = ++me->msg_id;
    req->cb = cb;
    req->cb_arg = cb_arg;
    req->next = NULL;

    if (!me->read_queue_head)
        me->read_queue_head = req;
    if (me->read_queue_tail)
        me->read_queue_tail->next = req;
    me->read_queue_tail = req;

    raft_send_appendentries_all(me_);
}

static void pop_read_queue(raft_server_private_t *me, int can_read)
{
    raft_read_request_t *p = me->read_queue_head;

    p->cb(p->cb_arg, can_read);

    /* remove entry and update head/tail */
    if (p->next) {
        me->read_queue_head = p->next;
        if (!me->read_queue_head->next)
            me->read_queue_tail = me->read_queue_head;
    } else {
        me->read_queue_head = NULL;
        me->read_queue_tail = NULL;
    }

    raft_free(p);
}

void raft_process_read_queue(raft_server_t* me_)
{
    raft_server_private_t* me = (raft_server_private_t*) me_;

    if (!me->read_queue_head)
        return;

    /* As a follower we drop all queued read requests */
    if (raft_is_follower(me_)) {
        while (me->read_queue_head) {
            pop_read_queue(me, 0);
        }
        return;
    }

    /* As a leader we can process requests that fulfill these conditions:
     * 1) Heartbeat acknowledged by majority
     * 2) We're on the same term (note: is this needed or over cautious?)
     */
    if (!raft_is_leader(me_))
        return;

    /* Quickly bail if nothing to do */
    if (!me->read_queue_head)
        return;

    if (raft_get_num_voting_nodes(me_) > 1) {
        raft_entry_t *ety = raft_get_entry_from_idx(me_, raft_get_commit_idx(me_));
        if (!ety)
            return;

        raft_term_t ety_term = ety->term;
        raft_entry_release(ety);

        /* Don't read if we did not commit an entry this term yet!
         * A new term has a NO_OP committed so if everything is well
         * we can except that to happen.
         */
        if (ety_term < me->current_term)
            return;
    }

    raft_msg_id_t last_acked_msgid = quorum_msg_id(me_);
    raft_index_t last_applied_idx = me->last_applied_idx;

    /* Special case: the log's first index is 1, so we need to account
     * for that in case we read before anything was ever committed.
     *
     * Note that this also implies a single node because adding nodes would
     * bump the log and commit index.
     */
    if (!me->commit_idx && !me->last_applied_idx && raft_get_current_idx(me_) == 1)
        last_applied_idx = 1;

    while (me->read_queue_head &&
            me->read_queue_head->msg_id <= last_acked_msgid &&
            me->read_queue_head->read_idx <= last_applied_idx) {
        pop_read_queue(me, me->read_queue_head->read_term == me->current_term);
    }
}
