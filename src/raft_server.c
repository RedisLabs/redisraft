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
#include <limits.h>

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

__attribute__ ((format (printf, 2, 3)))
static void raft_log(raft_server_t *me, const char *fmt, ...)
{
    if (!me->log_enabled || me->cb.log == NULL) {
        return;
    }

    char buf[1024];
    va_list args;

    va_start(args, fmt);
    int n = vsnprintf(buf, sizeof(buf), fmt, args);
    if (n < 0) {
        buf[0] = '\0';
    }
    va_end(args);

    me->cb.log(me, me->udata, buf);
}

void raft_randomize_election_timeout(raft_server_t *me)
{
    raft_time_t random = (rand() % me->election_timeout);

    /* [election_timeout, 2 * election_timeout) */
    me->election_timeout_rand = me->election_timeout + random;
    raft_log(me, "randomized election timeout:%lld", me->election_timeout_rand);
}

void raft_update_quorum_meta(raft_server_t* me, raft_msg_id_t id)
{
    // Make sure that timeout is greater than 'randomized election timeout'
    me->quorum_timeout = me->election_timeout * 2;
    me->last_acked_msg_id = id;
}

int raft_clear_incoming_snapshot(raft_server_t* me, raft_index_t new_idx)
{
    int e = 0;

    if (me->snapshot_recv_idx != 0)
        e = me->cb.clear_snapshot(me, me->udata);

    me->snapshot_recv_idx = new_idx;
    me->snapshot_recv_offset = 0;

    return e;
}

raft_server_t* raft_new_with_log(const raft_log_impl_t *log_impl, void *log_arg)
{
    raft_server_t *me = raft_calloc(1, sizeof(*me));
    if (!me)
        return NULL;

    me->current_term = 0;
    me->voted_for = RAFT_NODE_ID_NONE;
    me->timeout_elapsed = 0;
    me->request_timeout = 200;
    me->election_timeout = 1000;
    me->node_transferring_leader_to = RAFT_NODE_ID_NONE;
    me->auto_flush = 1;
    me->exec_deadline = LLONG_MAX;
    me->msg_id = 0;

    raft_update_quorum_meta(me, me->msg_id);

    raft_randomize_election_timeout(me);
    me->log_impl = log_impl;
    me->log = me->log_impl->init(me, log_arg);
    if (!me->log) {
        raft_free(me);
        return NULL;
    }

    me->voting_cfg_change_log_idx = -1;
    raft_set_state(me, RAFT_STATE_FOLLOWER);
    me->leader_id = RAFT_NODE_ID_NONE;

    me->snapshot_in_progress = 0;

    return me;
}

raft_server_t* raft_new(void)
{
    return raft_new_with_log(&raft_log_internal_impl, NULL);
}

int raft_restore_metadata(raft_server_t *me,
                          raft_term_t term,
                          raft_node_id_t vote)
{
    me->current_term = term;
    me->voted_for = vote;

    return 0;
}

void raft_set_callbacks(raft_server_t* me, raft_cbs_t* funcs, void* udata)
{
    me->cb = *funcs;
    me->udata = udata;
}

void raft_destroy(raft_server_t* me)
{
    me->log_impl->free(me->log);
    raft_free(me);
}

void raft_clear(raft_server_t* me)
{
    me->current_term = 0;
    me->voted_for = RAFT_NODE_ID_NONE;
    me->timeout_elapsed = 0;
    raft_randomize_election_timeout(me);
    me->voting_cfg_change_log_idx = -1;
    raft_set_state(me, RAFT_STATE_FOLLOWER);
    me->leader_id = RAFT_NODE_ID_NONE;
    me->commit_idx = 0;
    me->last_applied_idx = 0;
    me->last_applied_term = 0;
    me->num_nodes = 0;
    me->node = NULL;
    me->log_impl->reset(me->log, 1, 1);
}

raft_node_t* raft_add_node_internal(raft_server_t *me, raft_entry_t *ety, void *udata, raft_node_id_t id, int is_self, int voting)
{
    if (id == RAFT_NODE_ID_NONE)
        return NULL;

    /* set to voting if node already exists */
    raft_node_t* node = raft_get_node(me, id);
    if (node)
    {
        /* node can be promoted only from a non voting state into a voting one */
        if (!raft_node_is_voting(node) && voting)
        {
            raft_node_set_voting(node, 1);
            return node;
        }
        else
            /* we shouldn't add a node with the same voting mode twice */
            return NULL;
    }

    node = raft_node_new(udata, id, voting);
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

    if (me->cb.notify_membership_event) {
        me->cb.notify_membership_event(me, me->udata, node, ety,
                                       RAFT_MEMBERSHIP_ADD);
    }

    return node;
}

raft_node_t* raft_add_node(raft_server_t* me, void* udata, raft_node_id_t id, int is_self)
{
    return raft_add_node_internal(me, NULL, udata, id, is_self, 1);
}

raft_node_t* raft_add_non_voting_node(raft_server_t* me, void* udata, raft_node_id_t id, int is_self)
{
    return raft_add_node_internal(me, NULL, udata, id, is_self, 0);
}

void raft_remove_node(raft_server_t* me, raft_node_t* node)
{
    if (me->cb.notify_membership_event) {
        me->cb.notify_membership_event(me, me->udata, node, NULL,
                                       RAFT_MEMBERSHIP_REMOVE);
    }

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
    (void) found;

    memmove(&me->nodes[i], &me->nodes[i + 1], sizeof(*me->nodes) * (me->num_nodes - i - 1));
    me->num_nodes--;

    if (me->leader_id == raft_node_get_id(node)) {
        me->leader_id = RAFT_NODE_ID_NONE;
    }

    raft_node_free(node);
}

void raft_handle_append_cfg_change(raft_server_t* me, raft_entry_t* ety, raft_index_t idx)
{
    if (!raft_entry_is_cfg_change(ety))
        return;

    if (!me->cb.get_node_id)
        return;

    raft_node_id_t node_id = me->cb.get_node_id(me, me->udata, ety, idx);
    raft_node_t* node = raft_get_node(me, node_id);
    int is_self = node_id == raft_get_nodeid(me);

    switch (ety->type)
    {
        case RAFT_LOGTYPE_ADD_NODE:
            node = raft_add_node_internal(me, ety, NULL, node_id, is_self, 1);
            assert(node);
            assert(raft_node_is_voting(node));
            break;

        case RAFT_LOGTYPE_ADD_NONVOTING_NODE:
            if (!is_self)
            {
                assert(!node || !raft_node_is_voting(node));
                if (node && !raft_node_is_active(node))
                {
                    raft_node_set_active(node, 1);
                }
                else if (!node)
                {
                    node = raft_add_node_internal(me, ety, NULL, node_id, is_self, 0);
                    assert(node);
                    assert(!raft_node_is_voting(node));
                }
             }
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

void raft_handle_remove_cfg_change(raft_server_t* me, raft_entry_t* ety, const raft_index_t idx)
{
    if (!raft_entry_is_cfg_change(ety))
        return;

    if (!me->cb.get_node_id)
        return;

    raft_node_id_t node_id = me->cb.get_node_id(me, me->udata, ety, idx);
    raft_node_t* node = raft_get_node(me, node_id);

    switch (ety->type)
    {
        case RAFT_LOGTYPE_ADD_NODE:
            raft_node_set_voting(node, 0);
            break;

        case RAFT_LOGTYPE_ADD_NONVOTING_NODE:
            assert(node_id != raft_get_nodeid(me));
            raft_remove_node(me, node);
            break;

        case RAFT_LOGTYPE_REMOVE_NODE:
            if (node) {
                raft_node_set_active(node, 1);
            }
            break;

        default:
            assert(0);
            break;
    }
}

void raft_handle_apply_cfg_change(raft_server_t* me, raft_entry_t* ety, raft_index_t idx)
{
    if (!raft_entry_is_cfg_change(ety))
        return;

    if (!me->cb.get_node_id)
        return;

    raft_node_id_t node_id = me->cb.get_node_id(me, me->udata, ety, idx);
    raft_node_t* node = raft_get_node(me, node_id);

    switch (ety->type) {
        case RAFT_LOGTYPE_ADD_NODE:
            raft_node_set_addition_committed(node, 1);
            raft_node_set_voting_committed(node, 1);
            raft_node_set_has_sufficient_logs(node, 1);
            break;
        case RAFT_LOGTYPE_ADD_NONVOTING_NODE:
            raft_node_set_addition_committed(node, 1);
            break;
        case RAFT_LOGTYPE_REMOVE_NODE:
            if (node) {
                raft_remove_node(me, node);
            }
            break;
        default:
            break;
    }
}

int raft_delete_entry_from_idx(raft_server_t *me, raft_index_t idx)
{
    assert(me->commit_idx < idx);

    if (me->log_impl->count(me->log) == 0) {
        return 0;
    }

    raft_index_t first = me->log_impl->first_idx(me->log);
    raft_index_t last = me->log_impl->current_idx(me->log);

    if (idx < first || idx > last) {
        return 0;
    }

    /* Delete entries starting from the last one. */
    while (last >= idx) {
        raft_entry_t *ety = raft_get_entry_from_idx(me, last);

        int e = me->log_impl->pop(me->log, last);
        if (e != 0) {
            me->log_impl->sync(me->log);
            raft_entry_release(ety);
            return e;
        }

        raft_handle_remove_cfg_change(me, ety, last);
        raft_entry_release(ety);

        if (me->voting_cfg_change_log_idx == last) {
            me->voting_cfg_change_log_idx = -1;
        }

        last--;
    }

    return me->log_impl->sync(me->log);
}

int raft_election_start(raft_server_t *me, int skip_precandidate)
{
    raft_log(me, "election starting, timeout_elapsed:%lld, term:%ld",
             me->timeout_elapsed, me->current_term);

    me->leader_id = RAFT_NODE_ID_NONE;
    me->timeout_elapsed = 0;
    raft_randomize_election_timeout(me);

    return skip_precandidate ? raft_become_candidate(me):
                               raft_become_precandidate(me);
}

void raft_accept_leader(raft_server_t* me, raft_node_id_t leader)
{
    if (!raft_is_follower(me)) {
        raft_become_follower(me);
    }

    if (me->leader_id != leader) {
        raft_clear_incoming_snapshot(me, 0);
    }

    me->timeout_elapsed = 0;
    me->leader_id = leader;

    raft_reset_transfer_leader(me, 0);
}

int raft_become_leader(raft_server_t *me)
{
    raft_entry_t *noop = raft_entry_new(0);
    noop->term = me->current_term;
    noop->type = RAFT_LOGTYPE_NO_OP;

    int e = raft_append_entry(me, noop);
    raft_entry_release(noop);
    if (e != 0) {
        return e;
    }

    e = me->log_impl->sync(me->log);
    if (e != 0) {
        return e;
    }

    raft_index_t current_idx = raft_get_current_idx(me);

    raft_node_set_match_idx(me->node, current_idx);
    me->next_sync_index = current_idx + 1;

    /* Commit noop immediately if this is a single node cluster. */
    if (raft_is_single_node_voting_cluster(me)) {
        raft_set_commit_idx(me, current_idx);
    }

    raft_set_state(me, RAFT_STATE_LEADER);
    raft_update_quorum_meta(me, me->msg_id);
    raft_clear_incoming_snapshot(me, 0);
    raft_reset_transfer_leader(me, 0);
    me->timeout_elapsed = 0;

    for (int i = 0; i < me->num_nodes; i++) {
        raft_node_t *node = me->nodes[i];

        if (me->node == node) {
            continue;
        }

        raft_node_set_snapshot_offset(node, 0);
        raft_node_set_next_idx(node, current_idx);
        raft_node_set_match_idx(node, 0);
        raft_send_appendentries(me, node);
    }

    if (me->cb.notify_state_event) {
        me->cb.notify_state_event(me, me->udata, RAFT_STATE_LEADER);
    }

    raft_log(me, "become leader, term:%ld", me->current_term);

    return 0;
}

int raft_become_precandidate(raft_server_t *me)
{
    for (int i = 0; i < me->num_nodes; i++) {
        raft_node_set_voted_for_me(me->nodes[i], 0);
    }

    raft_set_state(me, RAFT_STATE_PRECANDIDATE);

    for (int i = 0; i < me->num_nodes; i++) {
        raft_node_t *node = me->nodes[i];

        if (me->node != node && raft_node_is_voting(node)) {
            raft_send_requestvote(me, node);
        }
    }

    if (me->cb.notify_state_event) {
        me->cb.notify_state_event(me, me->udata, RAFT_STATE_PRECANDIDATE);
    }

    raft_log(me, "become pre-candidate, term:%ld", me->current_term);

    return 0;
}

int raft_become_candidate(raft_server_t *me)
{
    int e = raft_set_current_term(me, me->current_term + 1);
    if (e != 0) {
        return e;
    }

    for (int i = 0; i < me->num_nodes; i++) {
        raft_node_set_voted_for_me(me->nodes[i], 0);
    }

    if (raft_node_is_voting(me->node)) {
        e = raft_vote(me, me->node);
        if (e != 0) {
            return e;
        }
    }

    me->leader_id = RAFT_NODE_ID_NONE;
    raft_set_state(me, RAFT_STATE_CANDIDATE);

    for (int i = 0; i < me->num_nodes; i++) {
        raft_node_t *node = me->nodes[i];

        if (me->node != node && raft_node_is_voting(node)) {
            raft_send_requestvote(me, node);
        }
    }

    if (me->cb.notify_state_event) {
        me->cb.notify_state_event(me, me->udata, RAFT_STATE_CANDIDATE);
    }

    raft_log(me, "become candidate, term:%ld", me->current_term);

    return 0;
}

void raft_become_follower(raft_server_t* me)
{
    raft_set_state(me, RAFT_STATE_FOLLOWER);
    raft_randomize_election_timeout(me);
    raft_clear_incoming_snapshot(me, 0);
    me->timeout_elapsed = 0;
    me->leader_id = RAFT_NODE_ID_NONE;

    if (me->cb.notify_state_event) {
        me->cb.notify_state_event(me, me->udata, RAFT_STATE_FOLLOWER);
    }

    raft_log(me, "become follower, term:%ld", me->current_term);
}

static int msgid_cmp(const void *a, const void *b)
{
    raft_msg_id_t va = *((raft_msg_id_t*) a);
    raft_msg_id_t vb = *((raft_msg_id_t*) b);

    return va > vb ? -1 : 1;
}

static raft_msg_id_t quorum_msg_id(raft_server_t* me)
{
    raft_msg_id_t msg_ids[me->num_nodes];
    int num_voters = 0;

    for (int i = 0; i < me->num_nodes; i++) {
        raft_node_t* node = me->nodes[i];

        if (!raft_node_is_voting(node))
            continue;

        if (me->node == node) {
            msg_ids[num_voters++] = me->msg_id;
        } else {
            msg_ids[num_voters++] = raft_node_get_match_msgid(node);
        }
    }

    assert(num_voters == raft_get_num_voting_nodes(me));

    /**
     *  Sort the acknowledged msg_ids in the descending order and return
     *  the median value. Median value means it's the highest msg_id
     *  acknowledged by the majority.
     */
    qsort(msg_ids, num_voters, sizeof(raft_msg_id_t), msgid_cmp);

    return msg_ids[num_voters / 2];
}

static raft_time_t raft_time_millis(raft_server_t *me)
{
    return me->cb.timestamp ? me->cb.timestamp(me, me->udata) / 1000 : 0;
}

int raft_periodic(raft_server_t *me)
{
    return raft_periodic_internal(me, -1);
}

int raft_periodic_internal(raft_server_t *me,
                           raft_time_t msec_since_last_period)
{
    if (msec_since_last_period < 0) {
        raft_time_t timestamp = raft_time_millis(me);
        assert(timestamp >= me->timestamp);

        /* If this is the first call, previous timestamp will be zero. In this
         * case, we just assume `0` millisecond has passed. */
        if (me->timestamp == 0) {
            msec_since_last_period = 0;
        } else {
            msec_since_last_period = timestamp - me->timestamp;
        }

        me->timestamp = timestamp;
    }

    me->timeout_elapsed += msec_since_last_period;

    /* Only one voting node means it's safe for us to become the leader */
    if (raft_is_single_node_voting_cluster(me) && !raft_is_leader(me)) {
        // need to update term on new leadership
        int e = raft_set_current_term(me, me->current_term + 1);
        if (e != 0) {
            return e;
        }

        e = raft_become_leader(me);
        if (e != 0) {
            return e;
        }
    }

    /* needs to be outside state check, as can become a followr and still timeout */
    if (me->node_transferring_leader_to != RAFT_NODE_ID_NONE) {
        me->transfer_leader_time -= msec_since_last_period;
        if (me->transfer_leader_time < 0) {
            raft_reset_transfer_leader(me, 1);
        }
    }

    if (me->state == RAFT_STATE_LEADER)
    {
        if (me->request_timeout <= me->timeout_elapsed)
        {
            me->msg_id++;
            me->timeout_elapsed = 0;
            raft_send_appendentries_all(me);
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
            raft_msg_id_t quorum_id = quorum_msg_id(me);

            if (me->last_acked_msg_id == quorum_id)
            {
                raft_log(me, "quorum does not exist, stepping down");
                raft_become_follower(me);
            }

            raft_update_quorum_meta(me, quorum_id);
	    }
    }
    else if (me->election_timeout_rand <= me->timeout_elapsed)
    {
        int e = raft_election_start(me, 0);
        if (0 != e)
            return e;
    }

    if (me->auto_flush) {
        return raft_exec_operations(me);
    }

    return 0;
}

raft_entry_t* raft_get_entry_from_idx(raft_server_t* me, raft_index_t etyidx)
{
    return me->log_impl->get(me->log, etyidx);
}

int raft_voting_change_is_in_progress(raft_server_t* me)
{
    return me->voting_cfg_change_log_idx != -1;
}

int raft_recv_appendentries_response(raft_server_t *me,
                                     raft_node_t *node,
                                     raft_appendentries_resp_t *resp)
{
    raft_log(me, "%d <-- %d, recv appendentries_resp "
                 "id:%lu, t:%ld, s:%d, ci:%ld",
                 raft_get_nodeid(me), raft_node_get_id(node),
                 resp->msg_id, resp->term, resp->success, resp->current_idx);

    me->stats.appendentries_resp_received++;

    if (resp->success == 0) {
        me->stats.appendentries_req_failed++;
    }

    if (!node || !raft_is_leader(me)) {
        return 0;
    }

    if (resp->msg_id < raft_node_get_match_msgid(node) ||
        resp->term < me->current_term) {
        return 0;
    }

    /* If response contains term T > currentTerm: set currentTerm = T
       and convert to follower (§5.3) */
    if (resp->term > me->current_term) {
        int e = raft_set_current_term(me, resp->term);
        if (e != 0) {
            return e;
        }

        raft_become_follower(me);
        return 0;
    }

    if (resp->success == 0) {
        /* Stale response -- ignore */
        if (resp->current_idx < raft_node_get_match_idx(node)) {
            return 0;
        }

        raft_index_t current_idx = raft_get_current_idx(me);
        raft_index_t next = min(resp->current_idx + 1, current_idx);
        assert(0 < next);

        raft_node_set_next_idx(node, next);

        /* retry */
        raft_send_appendentries(me, node);
        return 0;
    }

    if (!me->sent_timeout_now &&
        raft_get_transfer_leader(me) == raft_node_get_id(node) &&
        raft_get_current_idx(me) == resp->current_idx) {

        me->cb.send_timeoutnow(me, me->udata, node);
        me->sent_timeout_now = 1;

        raft_log(me, "%d --> %d, sent timeout_now", raft_get_nodeid(me),
                 raft_node_get_id(node));
    }

    if (!raft_node_is_voting(node) &&
        !raft_voting_change_is_in_progress(me) &&
        raft_get_current_idx(me) <= resp->current_idx + 1 &&
        !raft_node_is_voting_committed(node) &&
        raft_node_is_addition_committed(node) &&
        raft_node_has_sufficient_logs(node) == 0) {

        if (me->cb.node_has_sufficient_logs) {
            int e = me->cb.node_has_sufficient_logs(me, me->udata, node);
            if (e == 0) {
                raft_node_set_has_sufficient_logs(node, 1);
            }
        }
    }

    raft_index_t match_idx = raft_node_get_match_idx(node);
    if (resp->current_idx > match_idx) {
        raft_node_set_match_idx(node, resp->current_idx);
    }
    assert(resp->current_idx <= raft_get_current_idx(me));

    raft_msg_id_t match_msgid = raft_node_get_match_msgid(node);
    if (resp->msg_id > match_msgid) {
        raft_node_set_match_msgid(node, resp->msg_id);
    }
    assert(resp->msg_id <= me->msg_id);

    if (me->auto_flush) {
        return raft_flush(me, 0);
    }

    return 0;
}

int raft_recv_appendentries(raft_server_t *me,
                            raft_node_t *node,
                            raft_appendentries_req_t *req,
                            raft_appendentries_resp_t *resp)
{
    int e = 0;

    raft_log(me, "%d <-- %d, recv appendentries_req "
             "lead:%d, id:%ld, t:%ld, pli:%ld, plt:%ld, lc:%ld ent:%ld",
             raft_get_nodeid(me), raft_node_get_id(node),
             req->leader_id, req->msg_id, req->term, req->prev_log_idx,
             req->prev_log_term, req->leader_commit, req->n_entries);

    me->stats.appendentries_req_received++;
    if (req->n_entries) {
        me->stats.appendentries_req_with_entry++;
    }

    resp->msg_id = req->msg_id;
    resp->success = 0;

    if (req->term < me->current_term) {
        /* 1. Reply false if term < currentTerm (§5.1) */
        raft_log(me, "AE term:%ld is less than current term:%ld",
                 req->term, me->current_term);
        goto out;
    }

    if (req->term > me->current_term) {
        e = raft_set_current_term(me, req->term);
        if (e != 0) {
            goto out;
        }
    }

    /* update current leader because ae->term is up to date */
    raft_accept_leader(me, req->leader_id);

    if (req->prev_log_idx == me->snapshot_last_idx) {
        if (req->prev_log_term != me->snapshot_last_term) {
            /* Should never happen; something is seriously wrong! */
            raft_log(me, "AE prev_log_term:%ld and snapshot term:%ld conflict",
                     req->prev_log_term, me->snapshot_last_term);

            e = RAFT_ERR_SHUTDOWN;
            goto out;
        }
    } else {
        /* 2. Reply false if log doesn't contain an entry at prevLogIndex
           whose term matches prevLogTerm (§5.3) */
        raft_entry_t *ety = raft_get_entry_from_idx(me, req->prev_log_idx);
        if (!ety) {
            raft_log(me, "AE no log at prev_log_idx:%ld", req->prev_log_idx);
            goto out;
        }

        if (ety->term != req->prev_log_term) {
            raft_log(me, "AE prev_log_term:%ld doesn't match entry term:%ld",
                     req->prev_log_term, ety->term);

            if (req->prev_log_idx <= me->commit_idx) {
                /* Should never happen; something is seriously wrong! */
                raft_log(me, "AE prev_log_idx:%ld is less than commit idx:%ld",
                         req->prev_log_idx, me->commit_idx);

                e = RAFT_ERR_SHUTDOWN;
                raft_entry_release(ety);
                goto out;
            }

            /* Delete all the following log entries because they don't match */
            e = raft_delete_entry_from_idx(me, req->prev_log_idx);
            raft_entry_release(ety);
            goto out;
        }

        raft_entry_release(ety);
    }

    resp->success = 1;
    resp->current_idx = req->prev_log_idx;

    /* Synchronize msg_id to leader. Not really needed for raft, but needed for
     * virtraft for msg_id to be increasing cluster wide so that can verify
     * read_queue correctness easily. Otherwise, it'd be fine for msg_id to be
     * unique to each raft_server_t.
     */
    if (me->msg_id < req->msg_id) {
        me->msg_id = req->msg_id;
    }

    /* 3. If an existing entry conflicts with a new one (same index
       but different terms), delete the existing entry and all that
       follow it (§5.3) */
    int i;
    for (i = 0; i < req->n_entries; i++) {
        raft_entry_t *ety = req->entries[i];
        raft_index_t ety_index = req->prev_log_idx + 1 + i;

        raft_entry_t *existing_ety = raft_get_entry_from_idx(me, ety_index);
        if (!existing_ety) {
            break;
        }

        raft_term_t existing_term = existing_ety->term;
        raft_entry_release(existing_ety);

        if (ety->term != existing_term) {
            if (ety_index <= me->commit_idx) {
                /* Should never happen; something is seriously wrong! */
                raft_log(me, "AE entry index:%ld is less than commit idx:%ld",
                         ety_index, me->commit_idx);
                e = RAFT_ERR_SHUTDOWN;
                goto out;
            }

            e = raft_delete_entry_from_idx(me, ety_index);
            if (e != 0) {
                goto out;
            }
            break;
        }
        resp->current_idx = ety_index;
    }

    /* Pick up remainder in case of mismatch or missing entry */
    for (; i < req->n_entries; i++) {
        e = raft_append_entry(me, req->entries[i]);
        if (e != 0) {
            goto out;
        }
        resp->current_idx = req->prev_log_idx + 1 + i;
    }

    if (req->n_entries > 0) {
        e = me->log_impl->sync(me->log);
        if (e != 0) {
            goto out;
        }
    }

    /* 4. If leaderCommit > commitIndex, set commitIndex =
        min(leaderCommit, index of most recent entry) */
    if (me->commit_idx < req->leader_commit) {
        raft_index_t last_log_idx = max(raft_get_current_idx(me), 1);
        raft_set_commit_idx(me, min(last_log_idx, req->leader_commit));
    }

out:
    /* 'node' might be created after processing this message, fetching again. */
    node = raft_get_node(me, req->leader_id);
    if (node != NULL) {
        raft_node_update_max_seen_msg_id(node, req->msg_id);
    }

    resp->term = me->current_term;
    if (resp->success == 0) {
        resp->current_idx = raft_get_current_idx(me);
    }

    raft_log(me, "%d --> %d, sent appendentries_resp "
             "id:%lu, t:%ld, s:%d, ci:%ld",
             raft_get_nodeid(me), raft_node_get_id(node),
             resp->msg_id, resp->term, resp->success, resp->current_idx);
    return e;
}

int raft_recv_requestvote(raft_server_t *me,
                          raft_node_t *node,
                          raft_requestvote_req_t *req,
                          raft_requestvote_resp_t *resp)
{
    (void) node;
    int e = 0;

    raft_log(me, "%d <-- %d, recv requestvote_req "
             "pv:%d, t:%ld, ci:%d, lli:%ld, llt:%ld",
             raft_get_nodeid(me), raft_node_get_id(node),
             req->prevote, req->term, req->candidate_id, req->last_log_idx,
             req->last_log_term);

    req->prevote ? me->stats.requestvote_prevote_req_received++ :
                   me->stats.requestvote_req_received++;

    resp->prevote = req->prevote;
    resp->request_term = req->term;
    resp->vote_granted = 0;

    /* Reject request if we have a leader, during prevote */
    if (req->prevote &&
        me->leader_id != RAFT_NODE_ID_NONE &&
        me->leader_id != req->candidate_id &&
        me->timeout_elapsed < me->election_timeout) {
        goto done;
    }

    /* Update the term only if this is not a prevote request */
    if (!req->prevote && me->current_term < req->term) {
        e = raft_set_current_term(me, req->term);
        if (e != 0) {
            goto done;
        }

        raft_become_follower(me);
    }

    if (me->current_term > req->term) {
        goto done;
    }

    if (me->current_term == req->term) {
        /* If already voted for some other node for this term, return failure */
        if (me->voted_for != RAFT_NODE_ID_NONE &&
            me->voted_for != req->candidate_id) {
            goto done;
        }
    }

    /* Below we check if log is more up-to-date... */
    raft_index_t current_idx = raft_get_current_idx(me);
    raft_term_t ety_term = raft_get_last_log_term(me);

    if (req->last_log_term < ety_term ||
        (req->last_log_term == ety_term && req->last_log_idx < current_idx)) {
        goto done;
    }

    resp->vote_granted = 1;

    if (!req->prevote) {
        /* It shouldn't be possible for a leader or candidate to grant a vote
         * Both states would have voted for themselves */
        assert(!(raft_is_leader(me) || raft_is_candidate(me)));

        e = raft_vote_for_nodeid(me, req->candidate_id);
        if (e != 0) {
            resp->vote_granted = 0;
        }

        /* must be in an election. */
        me->leader_id = RAFT_NODE_ID_NONE;
        me->timeout_elapsed = 0;
    }

done:
    if (resp->vote_granted) {
        resp->prevote ? me->stats.requestvote_prevote_req_granted++ :
                        me->stats.requestvote_req_granted++;
    }

    resp->term = me->current_term;

    raft_log(me, "%d --> %d, sent requestvote_resp "
             "pv:%d, rt:%ld, t:%ld, vg:%d",
             raft_get_nodeid(me), raft_node_get_id(node), resp->prevote,
             resp->request_term, resp->term, resp->vote_granted);

    return e;
}

int raft_votes_is_majority(const int num_nodes, const int nvotes)
{
    if (num_nodes < nvotes)
        return 0;
    int half = num_nodes / 2;
    return half + 1 <= nvotes;
}

int raft_recv_requestvote_response(raft_server_t *me,
                                   raft_node_t *node,
                                   raft_requestvote_resp_t *resp)
{
    raft_log(me, "%d <-- %d, recv requestvote_resp "
             "pv:%d, rt:%ld, t:%ld, vg:%d",
             raft_get_nodeid(me), raft_node_get_id(node),
             resp->prevote, resp->request_term, resp->term, resp->vote_granted);

    resp->prevote ? me->stats.requestvote_prevote_resp_received++ :
                    me->stats.requestvote_resp_received++;

    if (resp->vote_granted == 0) {
        resp->prevote ? me->stats.requestvote_prevote_req_failed++ :
                        me->stats.requestvote_req_failed++;
    }

    if (resp->term > me->current_term) {
        int e = raft_set_current_term(me, resp->term);
        if (e != 0) {
            return e;
        }

        raft_become_follower(me);
        return 0;
    }

    if (resp->prevote) {
        /* Validate prevote is not stale */
        if (!raft_is_precandidate(me) ||
            resp->request_term != me->current_term + 1) {
            return 0;
        }
    } else {
        /* Validate reqvote is not stale */
        if (!raft_is_candidate(me) || resp->request_term != me->current_term) {
            return 0;
        }
    }

    if (resp->vote_granted) {
        if (node) {
            raft_node_set_voted_for_me(node, 1);
        }

        int votes = raft_get_nvotes_for_me(me);
        int nodes = raft_get_num_voting_nodes(me);

        if (raft_votes_is_majority(nodes, votes)) {
            int e = raft_is_precandidate(me) ? raft_become_candidate(me) :
                                               raft_become_leader(me);
            if (e != 0) {
                return e;
            }
        }
    }

    return 0;
}

int raft_recv_entry(raft_server_t *me,
                    raft_entry_req_t *ety,
                    raft_entry_resp_t *r)
{
    if (!raft_is_leader(me)) {
        return RAFT_ERR_NOT_LEADER;
    }

    if (raft_entry_is_voting_cfg_change(ety)) {
        /* Only one voting cfg change at a time */
        if (raft_voting_change_is_in_progress(me)) {
            /* On restart, if logs are not replayed yet, we don't know whether
             * cfg change entry was applied in the previous run. Returning
             * ONE_VOTING_CHANGE_ONLY might be confusing if there is no ongoing
             * config change. So, we check if we've applied an entry from the
             * current term which implicitly means we replayed previous logs.
             * Then, returning TRYAGAIN if log replay is not completed yet. */
            int log_replayed = (me->current_term == me->last_applied_term);
            return log_replayed ? RAFT_ERR_ONE_VOTING_CHANGE_ONLY:
                                  RAFT_ERR_TRYAGAIN;
        }

        /* Multi-threading: need to fail here because user might be
         * snapshotting membership settings. */
        if (!raft_is_apply_allowed(me)) {
            return RAFT_ERR_SNAPSHOT_IN_PROGRESS;
        }
    }

    if (raft_get_transfer_leader(me) != RAFT_NODE_ID_NONE) {
        return RAFT_ERR_LEADER_TRANSFER_IN_PROGRESS;
    }

    ety->term = me->current_term;

    int e = raft_append_entry(me, ety);
    if (e != 0) {
        return e;
    }

    if (r) {
        r->id = ety->id;
        r->idx = raft_get_current_idx(me);
        r->term = me->current_term;
    }

    raft_log(me, "recv entry t:%ld, id:%d, type:%d, len:%llu. assigned idx:%ld",
             ety->term, ety->id, ety->type, ety->data_len,
             raft_get_current_idx(me));

    if (me->auto_flush) {
        e = me->log_impl->sync(me->log);
        if (e != 0) {
            return e;
        }
        return raft_flush(me, raft_get_current_idx(me));
    }

    return 0;
}

int raft_send_requestvote(raft_server_t *me, raft_node_t *node)
{
    int e = 0;
    raft_requestvote_req_t req;

    assert(node);
    assert(node != me->node);

    if (raft_is_precandidate(me)) {
        req.prevote = 1;
        req.term = me->current_term + 1;
    } else {
        req.prevote = 0;
        req.term = me->current_term;
    }

    req.last_log_idx = raft_get_current_idx(me);
    req.last_log_term = raft_get_last_log_term(me);
    req.candidate_id = raft_get_nodeid(me);

    raft_log(me, "%d --> %d, sent requestvote_req "
             "pv:%d, t:%ld, ci:%d, lli:%ld, llt:%ld",
             raft_get_nodeid(me), raft_node_get_id(node),
             req.prevote, req.term, req.candidate_id, req.last_log_idx,
             req.last_log_term);

    if (me->cb.send_requestvote) {
        req.prevote ? me->stats.requestvote_prevote_req_sent++ :
                      me->stats.requestvote_req_sent++;

        e = me->cb.send_requestvote(me, me->udata, node, &req);
    }

    return e;
}

int raft_append_entry(raft_server_t* me, raft_entry_t* ety)
{
    /* Don't allow inserting entries that are > our term.
     * term needs to be updated first
     */
    assert(me->current_term >= ety->term);

    int e = me->log_impl->append(me->log, ety);
    if (e < 0)
        return e;

    if (raft_entry_is_voting_cfg_change(ety))
        me->voting_cfg_change_log_idx = raft_get_current_idx(me);

    if (raft_entry_is_cfg_change(ety)) {
        raft_handle_append_cfg_change(me, ety, raft_get_current_idx(me));
    }

    return 0;
}

int raft_apply_entry(raft_server_t* me)
{
    if (!raft_is_apply_allowed(me))
        return -1;

    /* Don't apply after the commit_idx */
    if (me->last_applied_idx == me->commit_idx)
        return -1;

    raft_index_t idx = me->last_applied_idx + 1;

    raft_entry_t* ety = raft_get_entry_from_idx(me, idx);
    if (!ety)
        return -1;

    raft_log(me, "applying log: %ld, id: %d size: %llu",
             idx, ety->id, ety->data_len);

    if (me->cb.applylog)
    {
        int e = me->cb.applylog(me, me->udata, ety, idx);
        assert(e == 0 || e == RAFT_ERR_SHUTDOWN);
        if (RAFT_ERR_SHUTDOWN == e) {
            raft_entry_release(ety);
            return RAFT_ERR_SHUTDOWN;
        }
    }

    me->last_applied_idx = idx;
    me->last_applied_term = ety->term;

    if (idx == me->voting_cfg_change_log_idx)
        me->voting_cfg_change_log_idx = -1;

    if (raft_entry_is_cfg_change(ety))
        raft_handle_apply_cfg_change(me, ety, idx);

    raft_entry_release(ety);
    return 0;
}

static raft_entry_t **raft_get_entries(raft_server_t *me,
                                       raft_node_t *node,
                                       raft_index_t idx,
                                       raft_index_t *n_etys)
{

    const int max_entries_in_append_req = 64 * 1024;
    raft_index_t n;

    /* If callback is not implemented, fetch entries from log implementation. */
    if (!me->cb.get_entries_to_send) {
        return raft_get_entries_from_idx(me, idx, n_etys);
    }

    *n_etys = 0;

    if (raft_get_current_idx(me) < idx) {
        return NULL;
    }

    raft_index_t size = raft_get_current_idx(me) - idx + 1;
    if (size > max_entries_in_append_req) {
        size = max_entries_in_append_req;
    }

    raft_entry_t **e = raft_malloc(size * sizeof(*e));

    n = me->cb.get_entries_to_send(me, me->udata, node, idx, size, e);
    if (n < 0) {
        n = 0;
    }

    *n_etys = n;
    return e;
}

raft_entry_t **raft_get_entries_from_idx(raft_server_t *me,
                                         raft_index_t idx,
                                         raft_index_t *n_etys)
{
    *n_etys = 0;

    if (raft_get_current_idx(me) < idx) {
        return NULL;
    }

    raft_index_t size = raft_get_current_idx(me) - idx + 1;
    raft_entry_t **e = raft_malloc(size * sizeof(raft_entry_t*));

    raft_index_t n = me->log_impl->get_batch(me->log, idx, size, e);
    if (n < 1) {
        return NULL;
    }

    *n_etys = n;
    return e;
}

int raft_send_snapshot(raft_server_t *me, raft_node_t *node)
{
    if (!me->cb.send_snapshot) {
        return 0;
    }

    while (1) {
        raft_size_t offset = raft_node_get_snapshot_offset(node);

        raft_snapshot_req_t req = {
            .leader_id = raft_get_nodeid(me),
            .snapshot_index = me->snapshot_last_idx,
            .snapshot_term = me->snapshot_last_term,
            .term = me->current_term,
            .msg_id = ++me->msg_id,
            .chunk.offset = offset
        };

        raft_snapshot_chunk_t *chunk = &req.chunk;

        int e = me->cb.get_snapshot_chunk(me, me->udata, node, offset, chunk);
        if (e != 0) {
            return (e != RAFT_ERR_DONE) ? e : 0;
        }

        raft_log(me, "%d --> %d, sent snapshot_req "
                 "t:%ld, li:%d, id:%ld, si:%ld, st:%ld, o:%llu len:%llu lc:%d",
                 raft_get_nodeid(me), raft_node_get_id(node),
                 req.term, req.leader_id, req.msg_id, req.snapshot_index,
                 req.snapshot_term, req.chunk.offset, req.chunk.len,
                 req.chunk.last_chunk);

        me->stats.snapshot_req_sent++;
        e = me->cb.send_snapshot(me, me->udata, node, &req);
        if (e != 0) {
            return e;
        }

        if (chunk->last_chunk) {
            raft_node_set_snapshot_offset(node, 0);
            raft_node_set_next_idx(node, me->snapshot_last_idx + 1);
            return 0;
        }

        raft_node_set_snapshot_offset(node, offset + chunk->len);
    }
}

int raft_recv_snapshot(raft_server_t *me,
                       raft_node_t *node,
                       raft_snapshot_req_t *req,
                       raft_snapshot_resp_t *resp)
{
    int e = 0;

    raft_log(me, "%d <-- %d, recv snapshot_req "
             "t:%ld, lead:%d, id:%ld, si:%ld, st:%ld, o:%llu len:%llu lc:%d",
             raft_get_nodeid(me), raft_node_get_id(node),
             req->term, req->leader_id, req->msg_id, req->snapshot_index,
             req->snapshot_term, req->chunk.offset, req->chunk.len,
             req->chunk.last_chunk);

    me->stats.snapshot_req_received++;

    resp->msg_id = req->msg_id;
    resp->snapshot_index = req->snapshot_index;
    resp->last_chunk = req->chunk.last_chunk;
    resp->offset = 0;
    resp->success = 0;

    if (req->term < me->current_term) {
        raft_log(me, "Snapshot req term:%ld is less than current term:%ld",
                 req->term, me->current_term);
        goto out;
    }

    if (req->term > me->current_term) {
        e = raft_set_current_term(me, req->term);
        if (e != 0) {
            goto out;
        }
    }

    raft_accept_leader(me, req->leader_id);

    /** If we already have the snapshot or the log entries in this snapshot,
     * inform the leader. */
    if (req->snapshot_index <= raft_get_current_idx(me)) {
        /** Set response as if it is the last chunk to tell leader that we have
         * the snapshot */
        resp->last_chunk = 1;
        goto success;
    }

    /** In case leader takes another snapshot, it may start sending a more
     * recent snapshot. In that case, we dismiss existing snapshot file. */
    if (me->snapshot_recv_idx != req->snapshot_index) {
        e = raft_clear_incoming_snapshot(me, req->snapshot_index);
        if (e != 0) {
            goto out;
        }
    }

    /* Set current offset in the response. From now on, in case of an error,
     * this offset will be reported to the leader. If store_snapshot_chunk() or
     * load_snapshot() fails later in this function, we'll request current chunk
     * from the leader again so, we can retry the operation. */
    resp->offset = me->snapshot_recv_offset;

    /** Reject message if this is not our current offset. */
    if (me->snapshot_recv_offset != req->chunk.offset) {
        goto out;
    }

    e = me->cb.store_snapshot_chunk(me, me->udata, req->snapshot_index,
                                    req->chunk.offset, &req->chunk);
    if (e != 0) {
        goto out;
    }

    if (req->chunk.last_chunk) {
        e = me->cb.load_snapshot(me, me->udata, req->snapshot_term,
                                 req->snapshot_index);
        if (e != 0) {
            goto out;
        }
    }

    me->snapshot_recv_offset = req->chunk.offset + req->chunk.len;

success:
    resp->offset = req->chunk.len + req->chunk.offset;
    resp->success = 1;
out:
    resp->term = me->current_term;

    /* 'node' might be created after processing this message, fetching again. */
    node = raft_get_node(me, req->leader_id);
    if (node != NULL) {
        raft_node_update_max_seen_msg_id(node, req->msg_id);
    }

    raft_log(me, "%d --> %d, sent snapshot_resp "
             "id:%lu, si:%ld, t:%ld, s:%d, o:%llu, lc:%d",
             raft_get_nodeid(me), raft_node_get_id(node), resp->msg_id,
             resp->snapshot_index, resp->term, resp->success, resp->offset,
             resp->last_chunk);
    return e;
}

int raft_recv_snapshot_response(raft_server_t *me,
                                raft_node_t *node,
                                raft_snapshot_resp_t *resp)
{
    raft_log(me, "%d <-- %d, recv snapshot_resp "
             "id:%lu, si:%ld, t:%ld, s:%d, o:%llu, lc:%d",
             raft_get_nodeid(me), raft_node_get_id(node), resp->msg_id,
             resp->snapshot_index, resp->term, resp->success, resp->offset,
             resp->last_chunk);

    me->stats.snapshot_resp_received++;

    if (resp->success == 0) {
        me->stats.snapshot_req_failed++;
    }

    if (!node || !raft_is_leader(me)) {
        return 0;
    }

    if (resp->term < me->current_term ||
        resp->msg_id < raft_node_get_match_msgid(node) ) {
        return 0;
    }

    if (resp->term > me->current_term) {
        int e = raft_set_current_term(me, resp->term);
        if (e != 0) {
            return e;
        }

        raft_become_follower(me);
        return 0;
    }

    raft_node_set_match_msgid(node, resp->msg_id);

    /* Do not update the offset if we have a more recent snapshot now. */
    if (resp->snapshot_index != me->snapshot_last_idx) {
        goto out;
    }

    if (!resp->success) {
        raft_node_set_snapshot_offset(node, resp->offset);
    }

    if (resp->success && resp->last_chunk) {
        raft_node_set_snapshot_offset(node, 0);
        raft_node_set_next_idx(node, max(me->snapshot_last_idx + 1,
                                         raft_node_get_next_idx(node)));
    }

out:
    if (me->auto_flush) {
        return raft_flush(me, 0);
    }

    return 0;
}

raft_term_t raft_get_previous_log_term(raft_server_t *me, raft_index_t idx)
{
    assert(idx > 0);

    raft_entry_t *ety = raft_get_entry_from_idx(me, idx - 1);
    if (!ety) {
        return me->snapshot_last_term;
    }

    raft_term_t term = ety->term;
    raft_entry_release(ety);

    return term;
}

int raft_send_appendentries(raft_server_t *me, raft_node_t *node)
{
    assert(node != me->node);
    assert(raft_node_get_next_idx(node) != 0);

    int e;

    if (!raft_node_is_active(node)) {
        return 0;
    }

    if (me->snapshot_last_idx >= raft_node_get_next_idx(node)) {
        return raft_send_snapshot(me, node);
    }

    if (!me->cb.send_appendentries) {
        return -1;
    }

    do {
        if (me->cb.backpressure) {
            if (me->cb.backpressure(me, me->udata, node) != 0) {
                return 0;
            }
        }

        raft_index_t next_idx = raft_node_get_next_idx(node);

        raft_appendentries_req_t req = {
            .term = me->current_term,
            .leader_id = raft_get_nodeid(me),
            .leader_commit = me->commit_idx,
            .msg_id = ++me->msg_id,
            .prev_log_idx = next_idx - 1,
            .prev_log_term = raft_get_previous_log_term(me, next_idx),
            .entries = raft_get_entries(me, node, next_idx, &req.n_entries)
        };

        raft_log(me, "%d --> %d, sent appendentries_req "
                 "lead:%d, id:%ld, t:%ld, pli:%ld, plt:%ld, lc:%ld ent:%ld",
                 raft_get_nodeid(me), raft_node_get_id(node),
                 req.leader_id, req.msg_id, req.term, req.prev_log_idx,
                 req.prev_log_term, req.leader_commit, req.n_entries);

        me->stats.appendentries_req_sent++;
        e = me->cb.send_appendentries(me, me->udata, node, &req);

        raft_node_set_next_idx(node, next_idx + req.n_entries);
        raft_node_set_next_msgid(node, req.msg_id + 1);
        raft_entry_release_list(req.entries, req.n_entries);

        if (e != 0) {
            return e;
        }

    } while (raft_node_get_next_idx(node) <= raft_get_current_idx(me));

    return 0;
}

int raft_send_appendentries_all(raft_server_t* me)
{
    int i, e;
    int ret = 0;

    for (i = 0; i < me->num_nodes; i++)
    {
        if (me->node == me->nodes[i])
            continue;

        e = raft_send_appendentries(me, me->nodes[i]);
        if (0 != e)
            ret = e;
    }

    return ret;
}

int raft_get_nvotes_for_me(raft_server_t* me)
{
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

    if (raft_node_is_voting(me->node))
        votes += 1;

    return votes;
}

int raft_vote(raft_server_t* me, raft_node_t* node)
{
    return raft_vote_for_nodeid(me, raft_node_get_id(node));
}

int raft_vote_for_nodeid(raft_server_t* me, const raft_node_id_t nodeid)
{
    int e;

    if (me->cb.persist_metadata) {
        e = me->cb.persist_metadata(me, me->udata, me->current_term, nodeid);
        if (e != 0) {
            return e;
        }
    }

    me->voted_for = nodeid;

    return 0;
}

int raft_msg_entry_response_committed(raft_server_t* me,
                                      const raft_entry_resp_t* r)
{
    raft_entry_t* ety = raft_get_entry_from_idx(me, r->idx);
    if (!ety)
        return 0;
    raft_term_t ety_term = ety->term;
    raft_entry_release(ety);

    /* entry from another leader has invalidated this entry message */
    if (r->term != ety_term)
        return -1;
    return r->idx <= me->commit_idx;
}

int raft_pending_operations(raft_server_t *me)
{
    return me->pending_operations;
}

static void raft_set_pending(raft_server_t *me)
{
    me->stats.exec_throttled++;
    me->pending_operations = 1;
}

static void raft_unset_pending(raft_server_t *me)
{
    me->pending_operations = 0;
}

int raft_apply_all(raft_server_t *me)
{
    if (!raft_is_apply_allowed(me)) {
        return 0;
    }

    while (me->commit_idx > me->last_applied_idx) {
        if (raft_time_millis(me) > me->exec_deadline) {
            raft_set_pending(me);
            return 0;
        }

        int e = raft_apply_entry(me);
        if (e != 0) {
            return e;
        }
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

raft_index_t raft_get_first_entry_idx(raft_server_t* me)
{
    assert(0 < raft_get_current_idx(me));

    return me->log_impl->first_idx(me->log);
}

raft_index_t raft_get_num_snapshottable_logs(raft_server_t *me)
{
    if (raft_get_log_count(me) <= 1)
        return 0;
    return me->commit_idx - me->log_impl->first_idx(me->log) + 1;
}

int raft_restore_snapshot(raft_server_t *me,
                          raft_term_t last_included_term,
                          raft_index_t last_included_index)
{
    if (last_included_term < 0 || last_included_index < 0 ||
        me->current_term != 0 || me->commit_idx != 0 ||
        me->snapshot_last_term != 0 || me->snapshot_last_idx != 0 ||
        me->last_applied_term != 0 || me->last_applied_idx != 0) {
        return RAFT_ERR_MISUSE;
    }

    me->current_term = last_included_term;
    me->commit_idx = last_included_index;
    me->last_applied_term = last_included_term;
    me->last_applied_idx = last_included_index;
    me->snapshot_last_term = last_included_term;
    me->snapshot_last_idx = last_included_index;

    /* Adjust node flags as they are configured by `voting` data only. */
    for (int i = 0; i < me->num_nodes; i++) {
        int voting = raft_node_is_voting(me->nodes[i]);

        raft_node_set_has_sufficient_logs(me->nodes[i], voting);
        raft_node_set_voting_committed(me->nodes[i], voting);
        raft_node_set_addition_committed(me->nodes[i], 1);
    }

    raft_log(me, "restored snapshot lii:%ld lit:%ld",
             last_included_index, last_included_term);

    return 0;
}

int raft_begin_snapshot(raft_server_t *me)
{
    raft_index_t entry_count = raft_get_num_snapshottable_logs(me);
    if (entry_count == 0) {
        return -1;
    }

    /* we need to get all the way to the commit idx */
    int e = raft_apply_all(me);
    if (e != 0) {
        return e;
    }

    assert(me->commit_idx == me->last_applied_idx);

    me->snapshot_in_progress = 1;
    me->next_snapshot_last_idx = me->last_applied_idx;
    me->next_snapshot_last_term = me->last_applied_term;

    raft_log(me, "begin snapshot lai:%ld lat:%ld ec:%ld",
             me->last_applied_idx, me->last_applied_term, entry_count);

    return 0;
}

int raft_cancel_snapshot(raft_server_t *me)
{
    me->snapshot_in_progress = 0;
    return 0;
}

int raft_end_snapshot(raft_server_t *me)
{
    if (!me->snapshot_in_progress)
        return -1;

    me->snapshot_last_idx = me->next_snapshot_last_idx;
    me->snapshot_last_term = me->next_snapshot_last_term;

    /* If needed, remove compacted logs */
    int e = me->log_impl->poll(me->log, me->snapshot_last_idx + 1);
    if (e != 0)
        return e;

    e = me->log_impl->sync(me->log);
    if (e != 0)
        return e;

    me->snapshot_in_progress = 0;

    raft_log(me,"end snapshot, base:%ld, commit-index:%ld, current-index:%ld",
             me->log_impl->first_idx(me->log) - 1, me->commit_idx,
             raft_get_current_idx(me));

    me->stats.snapshots_created++;

    if (!raft_is_leader(me))
        return 0;

    for (int i = 0; i < me->num_nodes; i++)
    {
        raft_node_t* node = me->nodes[i];

        if (me->node == node || !raft_node_is_active(node))
            continue;

        raft_node_set_snapshot_offset(node, 0);
        raft_index_t next_idx = raft_node_get_next_idx(node);

        /* figure out if the client needs a snapshot sent */
        if (me->snapshot_last_idx > 0 && next_idx <= me->snapshot_last_idx)
        {
            raft_send_snapshot(me, node);
        }
    }

    return 0;
}

int raft_begin_load_snapshot(raft_server_t *me,
                             raft_term_t last_included_term,
                             raft_index_t last_included_index)
{
    if (last_included_term <= 0 ||
        last_included_index <= 0 ||
        last_included_index < me->last_applied_idx ||
        last_included_index < raft_get_current_idx(me)) {
        return RAFT_ERR_MISUSE;
    }

    if (last_included_index <= me->snapshot_last_idx) {
        return RAFT_ERR_SNAPSHOT_ALREADY_LOADED;
    }

    if (last_included_term > me->current_term) {
        int e = raft_set_current_term(me, last_included_term);
        if (e != 0) {
            return e;
        }
    }

    me->log_impl->reset(me->log, last_included_index + 1, last_included_term);

    if (last_included_index > me->commit_idx) {
        raft_set_commit_idx(me, last_included_index);
    }

    me->last_applied_term = last_included_term;
    me->last_applied_idx = last_included_index;
    me->next_snapshot_last_term = last_included_term;
    me->next_snapshot_last_idx = last_included_index;

    /* remove all nodes but self */
    for (int i = 0; i < me->num_nodes; i++) {
        if (me->nodes[i] == me->node) {
            continue;
        }

        raft_node_free(me->nodes[i]);
    }

    raft_node_clear_flags(me->node);

    me->num_nodes = 1;
    me->nodes = raft_realloc(me->nodes, sizeof(*me->nodes) * me->num_nodes);
    me->nodes[0] = me->node;

    raft_log(me, "begin loading snapshot lii:%ld lit:%ld",
             last_included_index, last_included_term);

    return 0;
}

int raft_end_load_snapshot(raft_server_t *me)
{
    me->snapshot_last_idx = me->next_snapshot_last_idx;
    me->snapshot_last_term = me->next_snapshot_last_term;

    /* Adjust node flags as they are configured by `voting` data only. */
    for (int i = 0; i < me->num_nodes; i++) {
        int voting = raft_node_is_voting(me->nodes[i]);

        raft_node_set_has_sufficient_logs(me->nodes[i], voting);
        raft_node_set_voting_committed(me->nodes[i], voting);
        raft_node_set_addition_committed(me->nodes[i], 1);
    }

    me->stats.snapshots_received++;

    raft_log(me, "loaded snapshot sli:%ld slt:%ld",
             me->snapshot_last_idx, me->snapshot_last_term);

    return 0;
}

void *raft_get_log(raft_server_t *me)
{
    return me->log;
}

raft_entry_t *raft_entry_new(raft_size_t data_len)
{
    raft_entry_t *ety = raft_calloc(1, sizeof(raft_entry_t) + data_len);
    ety->data_len = data_len;
    ety->refs = 1;
    ety->id = rand();

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
    if (!ety_list) {
        return;
    }

    for (size_t i = 0; i < len; i++) {
        raft_entry_release(ety_list[i]);
    }

    raft_free(ety_list);
}

int raft_recv_read_request(raft_server_t* me, raft_read_request_callback_f cb, void *cb_arg)
{
    if (!raft_is_leader(me)) {
        return RAFT_ERR_NOT_LEADER;
    }

    raft_read_request_t *req = raft_malloc(sizeof(*req));

    req->read_idx = raft_get_current_idx(me);
    req->msg_id = ++me->msg_id;
    req->cb = cb;
    req->cb_arg = cb_arg;
    req->next = NULL;

    if (!me->read_queue_head)
        me->read_queue_head = req;
    if (me->read_queue_tail)
        me->read_queue_tail->next = req;
    me->read_queue_tail = req;

    if (me->auto_flush)
        return raft_flush(me, 0);

    return 0;
}

static void pop_read_queue(raft_server_t *me, int can_read)
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

void raft_process_read_queue(raft_server_t *me)
{
    if (!me->read_queue_head) {
        return;
    }

    /* If not the leader, we drop all queued read requests */
    if (!raft_is_leader(me)) {
        while (me->read_queue_head) {
            pop_read_queue(me, 0);
        }
        return;
    }

    /* As a leader we can process requests that fulfill these conditions:
     * 1) Applied NOOP entry from this term.
     * 2) Heartbeat acknowledged by majority.
     * 3) State machine has advanced enough.
     */
    if (me->last_applied_term < me->current_term) {
        return;
    }

    raft_msg_id_t last_acked_msgid = quorum_msg_id(me);

    while (me->read_queue_head &&
           me->read_queue_head->msg_id <= last_acked_msgid &&
           me->read_queue_head->read_idx <= me->last_applied_idx) {

        if (raft_time_millis(me) > me->exec_deadline) {
            raft_set_pending(me);
            return;
        }

        pop_read_queue(me, 1);
    }
}

int raft_transfer_leader(raft_server_t* me, raft_node_id_t node_id, long timeout)
{
    if (me->state != RAFT_STATE_LEADER) {
        return RAFT_ERR_NOT_LEADER;
    }

    if (me->node_transferring_leader_to != RAFT_NODE_ID_NONE) {
        return RAFT_ERR_LEADER_TRANSFER_IN_PROGRESS;
    }

    raft_node_t *target = NULL;

    if (node_id != RAFT_NODE_ID_NONE) {
        target = raft_get_node(me, node_id);
    } else {
        /* Find the most up-to-date node. As we need to replicate less entries
         * to this node, it is the best candidate for leader transfer. */
        raft_index_t max = 0;

        for (int i = 0; i < raft_get_num_nodes(me); i++) {
            raft_node_t *node = me->nodes[i];
            raft_index_t match = raft_node_get_match_idx(node);

            if (node != me->node && match > max) {
                target = node;
                max = match;
            }
        }
    }

    if (target == NULL || target == me->node) {
        return RAFT_ERR_INVALID_NODEID;
    }

    if (raft_get_current_idx(me) == raft_node_get_match_idx(target)) {
        me->cb.send_timeoutnow(me, me->udata, target);
        me->sent_timeout_now = 1;

        raft_log(me, "%d --> %d, sent timeout_now", raft_get_nodeid(me),
                 raft_node_get_id(target));
    }

    me->node_transferring_leader_to = raft_node_get_id(target);
    me->transfer_leader_time = (timeout != 0) ? timeout : me->election_timeout;

    return 0;
}

/* Forces the raft server to invoke an election as part of leader transfer
 * operation. */
int raft_timeout_now(raft_server_t* me)
{
    if (raft_is_leader(me)) {
        return 0;
    }

    /* Starting an election but pre-candidate round will be skipped. This node
     * becomes candidate immediately. It means the term will be incremented and
     * appendentries RPCs from the prior leader will be rejected. Otherwise,
     * in pre-candidate round, this node would have to accept RPCs from the
     * prior leader and abort the election. Also, by skipping pre-candidate
     * round, leader transfer operation can be completed faster and cluster will
     * experience less downtime.
     */
    return raft_election_start(me, 1);
}

/* Stop trying to transfer leader to a targeted node
 * internally used because either we have timed out our attempt or because we are no longer the leader
 * possible to be used by a client as well.
 */
void raft_reset_transfer_leader(raft_server_t* me, int timed_out)
{
    raft_leader_transfer_e result;

    if (me->node_transferring_leader_to == RAFT_NODE_ID_NONE)  {
        return;
    }

    if (timed_out) {
        result = RAFT_LEADER_TRANSFER_TIMEOUT;
    } else {
        result = (me->node_transferring_leader_to == me->leader_id) ?
                    RAFT_LEADER_TRANSFER_EXPECTED_LEADER :
                    RAFT_LEADER_TRANSFER_UNEXPECTED_LEADER;
    }

    if (me->cb.notify_transfer_event) {
        me->cb.notify_transfer_event(me, me->udata, result);
    }

    me->node_transferring_leader_to = RAFT_NODE_ID_NONE;
    me->transfer_leader_time = 0;
    me->sent_timeout_now = 0;
}

static int index_cmp(const void *a, const void *b)
{
    raft_index_t va = *((raft_index_t*) a);
    raft_index_t vb = *((raft_index_t*) b);

    return va > vb ? -1 : 1;
}

static void raft_update_commit_idx(raft_server_t* me)
{
    raft_index_t indexes[me->num_nodes];
    int num_voters = 0;

    memset(indexes, 0, sizeof(indexes));

    for (int i = 0; i < me->num_nodes; i++) {
        if (!raft_node_is_voting(me->nodes[i]))
            continue;

        indexes[num_voters++] = raft_node_get_match_idx(me->nodes[i]);
    }

    qsort(indexes, num_voters, sizeof(raft_index_t), index_cmp);
    raft_index_t commit = indexes[num_voters / 2];
    if (commit > me->commit_idx) {
        /* Leader can only commit entries from the current term */
        raft_entry_t *ety = raft_get_entry_from_idx(me, commit);
        if (ety->term == me->current_term)
            raft_set_commit_idx(me, commit);

        raft_entry_release(ety);
    }
}

raft_index_t raft_get_index_to_sync(raft_server_t *me)
{
    raft_index_t idx = raft_get_current_idx(me);

    if (!raft_is_leader(me) || idx < me->next_sync_index) {
        return 0;
    }

    me->next_sync_index = idx + 1;
    return idx;
}

int raft_flush(raft_server_t* me, raft_index_t sync_index)
{
    if (!raft_is_leader(me)) {
        goto out;
    }

    if (sync_index > raft_node_get_match_idx(me->node)) {
        raft_node_set_match_idx(me->node, sync_index);
    }

    raft_update_commit_idx(me);

    raft_msg_id_t last = me->read_queue_tail ? me->read_queue_tail->msg_id : 0;

    for (int i = 0; i < me->num_nodes; i++)
    {
        if (me->node == me->nodes[i])
            continue;

        if (raft_node_get_next_msgid(me->nodes[i]) > last &&
            raft_node_get_next_idx(me->nodes[i]) > raft_get_current_idx(me))
            continue;

        raft_send_appendentries(me, me->nodes[i]);
    }

out:
    return raft_exec_operations(me);
}

int raft_config(raft_server_t *me, int set, raft_config_e config, ...)
{
    int ret = 0;
    va_list va;

    va_start(va, config);

    switch (config) {
        case RAFT_CONFIG_ELECTION_TIMEOUT:
            if (set) {
                me->election_timeout = va_arg(va, int);
                raft_update_quorum_meta(me, me->last_acked_msg_id);
                raft_randomize_election_timeout(me);
            } else {
                *(va_arg(va, int*)) = (int) me->election_timeout;
            }
            break;
        case RAFT_CONFIG_REQUEST_TIMEOUT:
            if (set) {
                me->request_timeout = va_arg(va, int);
            } else {
                *(va_arg(va, int*)) = (int) me->request_timeout;
            }
            break;
        case RAFT_CONFIG_AUTO_FLUSH:
            if (set) {
                me->auto_flush = (va_arg(va, int)) ? 1 : 0;
            } else {
                *(va_arg(va, int*)) = me->auto_flush;
            }
            break;
        case RAFT_CONFIG_LOG_ENABLED:
            if (set) {
                me->log_enabled = (va_arg(va, int)) ? 1 : 0;
            } else {
                *(va_arg(va, int*)) = me->log_enabled;
            }
            break;
        case RAFT_CONFIG_NONBLOCKING_APPLY:
            if (set) {
                me->nonblocking_apply = (va_arg(va, int)) ? 1 : 0;
            } else {
                *(va_arg(va, int*)) = me->nonblocking_apply;
            }
            break;
        case RAFT_CONFIG_DISABLE_APPLY:
            if (set) {
                me->disable_apply = (va_arg(va, int)) ? 1 : 0;
            } else {
                *(va_arg(va, int*)) = me->disable_apply;
            }
            break;
        default:
            ret = RAFT_ERR_NOTFOUND;
            break;
    }

    va_end(va);
    return ret;
}

int raft_exec_operations(raft_server_t *me)
{
    /* Operations should take less than `request-timeout`. If we block here more
     * than request-timeout, it means this server won't generate responses on
     * time for the existing requests. */
    me->exec_deadline = raft_time_millis(me) + (me->request_timeout / 2);
    raft_unset_pending(me);

    int e = raft_apply_all(me);
    if (e != 0) {
       goto out;
    }

    raft_process_read_queue(me);

out:
    /* Set deadline to MAX to prevent raft_apply_all() to return early if this
     * function is called manually or inside raft_begin_snapshot() */
    me->exec_deadline = LLONG_MAX;

    return e;
}

int raft_restore_log(raft_server_t *me)
{
    if (me->snapshot_last_idx != me->last_applied_idx) {
        return RAFT_ERR_MISUSE;
    }

    raft_index_t i = me->snapshot_last_idx + 1;

    while (1) {
        raft_entry_t *ety = me->log_impl->get(me->log, i);
        if (!ety) {
            return 0;
        }

        if (raft_entry_is_voting_cfg_change(ety)) {
            me->voting_cfg_change_log_idx = i;
        }

        if (raft_entry_is_cfg_change(ety)) {
            raft_handle_append_cfg_change(me, ety, i);
        }

        raft_entry_release(ety);
        i++;
    }

    return 0;
}
