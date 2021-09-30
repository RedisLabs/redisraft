/**
 * Copyright (c) 2013, Willem-Hendrik Thiart
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 *
 * @file
 * @author Willem Thiart himself@willemthiart.com
 */

#ifndef RAFT_PRIVATE_H_
#define RAFT_PRIVATE_H_

#include "raft_types.h"

enum {
    RAFT_NODE_STATUS_DISCONNECTED,
    RAFT_NODE_STATUS_CONNECTED,
    RAFT_NODE_STATUS_CONNECTING,
    RAFT_NODE_STATUS_DISCONNECTING
};

struct raft_log_impl;

typedef struct raft_read_request {
    raft_index_t read_idx;
    raft_term_t read_term;

    raft_msg_id_t msg_id;
    func_read_request_callback_f cb;
    void *cb_arg;

    struct raft_read_request *next;
} raft_read_request_t;

typedef struct {
    /* Persistent state: */

    /* the server's best guess of what the current term is
     * starts at zero */
    raft_term_t current_term;

    /* The candidate the server voted for in its current term,
     * or Nil if it hasn't voted for any.  */
    raft_node_id_t voted_for;

    /* log storage engine */
    const struct raft_log_impl *log_impl;
    void *log;

    /* Volatile state: */

    /* idx of highest log entry known to be committed */
    raft_index_t commit_idx;

    /* idx of highest log entry applied to state machine */
    raft_index_t last_applied_idx;

    /* follower/leader/candidate indicator */
    int state;

    /* amount of time left till timeout */
    int timeout_elapsed;

    raft_node_t* nodes;
    int num_nodes;

    int election_timeout;
    int election_timeout_rand;
    int request_timeout;

    /* timer interval to check if we still have quorum */
    long quorum_timeout;

    /* latest quorum id for the previous quorum_timeout round */
    raft_msg_id_t last_acked_msg_id;

    /* what this node thinks is the node ID of the current leader or
     * RAFT_NODE_ID_NONE if there isn't a known current leader. */
    raft_node_id_t leader_id;

    /* callbacks */
    raft_cbs_t cb;
    void* udata;

    /* my node ID */
    raft_node_t* node;

    /* the log which has a voting cfg change, otherwise -1 */
    raft_index_t voting_cfg_change_log_idx;

    /* Our membership with the cluster is confirmed (ie. configuration log was
     * committed) */
    int connected;

    int snapshot_in_progress;
    int snapshot_flags;

    /* Last compacted snapshot */
    raft_index_t snapshot_last_idx;
    raft_term_t snapshot_last_term;

    /* Previous index/term values stored during snapshot,
     * which are restored if the operation is cancelled.
     */
    raft_index_t saved_snapshot_last_idx;
    raft_term_t saved_snapshot_last_term;

    /* Read requests that await a network round trip to confirm
     * we're still the leader.
     */
    raft_msg_id_t msg_id;
    /*
     * the maximum msg_id we've seen from our current leader.  reset on term change
     */
    raft_msg_id_t max_seen_msg_id;
    raft_read_request_t *read_queue_head;
    raft_read_request_t *read_queue_tail;

    raft_node_id_t node_transferring_leader_to; // the node we are targeting for leadership
    long transfer_leader_time; // how long we should wait for leadership transfer to take, before aborting

    int timeout_now;
} raft_server_private_t;

int raft_election_start(raft_server_t* me);

int raft_become_candidate(raft_server_t* me);

int raft_become_precandidate(raft_server_t* me);

void raft_randomize_election_timeout(raft_server_t* me_);

void raft_update_quorum_meta(raft_server_t* me_, raft_msg_id_t id);

/**
 * @return 0 on error */
int raft_send_requestvote(raft_server_t* me, raft_node_t* node);

int raft_send_appendentries(raft_server_t* me, raft_node_t* node);

int raft_send_appendentries_all(raft_server_t* me_);

/**
 * Apply entry at lastApplied + 1. Entry becomes 'committed'.
 * @return 1 if entry committed, 0 otherwise */
int raft_apply_entry(raft_server_t* me_);

void raft_set_last_applied_idx(raft_server_t* me, raft_index_t idx);

void raft_set_state(raft_server_t* me_, int state);

raft_node_t* raft_node_new(void* udata, raft_node_id_t id);

void raft_node_free(raft_node_t* me_);

void raft_node_set_match_idx(raft_node_t* node, raft_index_t idx);

void raft_node_vote_for_me(raft_node_t* me_, int vote);

int raft_node_has_vote_for_me(raft_node_t* me_);

void raft_node_set_has_sufficient_logs(raft_node_t* me_);

int raft_is_single_node_voting_cluster(raft_server_t *me_);

int raft_votes_is_majority(int nnodes, int nvotes);

raft_index_t raft_get_num_snapshottable_logs(raft_server_t* me_);

void raft_node_set_last_ack(raft_node_t* me_, raft_msg_id_t msgid, raft_term_t term);

raft_msg_id_t raft_node_get_last_acked_msgid(raft_node_t* me_);

/* Heap functions */
extern void *(*raft_malloc)(size_t size);
extern void *(*raft_calloc)(size_t nmemb, size_t size);
extern void *(*raft_realloc)(void *ptr, size_t size);
extern void (*raft_free)(void *ptr);

/* update the max_seen_msg_id for this node */
void raft_node_update_max_seen_msg_id(raft_node_t *me_, raft_msg_id_t msg_id);
/* get the max message id this server has seen from its the specified node */
raft_msg_id_t raft_node_get_max_seen_msg_id(raft_node_t *me_);
/* get the server's current msg_id */
raft_msg_id_t raft_get_msg_id(raft_server_t* me_);


#endif /* RAFT_PRIVATE_H_ */
