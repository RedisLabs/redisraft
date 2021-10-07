#include <stdbool.h>
#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include "CuTest.h"

#include "raft.h"
#include "raft_log.h"
#include "raft_private.h"
#include "mock_send_functions.h"

#include "helpers.h"

static int __raft_persist_term(
    raft_server_t* raft,
    void *udata,
    raft_term_t term,
    int vote
    )
{
    return 0;
}

static int __raft_persist_vote(
    raft_server_t* raft,
    void *udata,
    int vote
    )
{
    return 0;
}

static int __raft_applylog(
    raft_server_t* raft,
    void *udata,
    raft_entry_t *ety,
    raft_index_t idx
    )
{
    return 0;
}

static int __raft_send_requestvote(raft_server_t* raft,
                            void* udata,
                            raft_node_t* node,
                            msg_requestvote_t* msg)
{
    return 0;
}

static int __raft_send_appendentries(raft_server_t* raft,
                              void* udata,
                              raft_node_t* node,
                              msg_appendentries_t* msg)
{
    return 0;
}

static int __raft_send_appendentries_capture(raft_server_t* raft,
                              void* udata,
                              raft_node_t* node,
                              msg_appendentries_t* msg)
{
    *((msg_appendentries_t*)udata) = *msg;
    return 0;
}

static int __raft_send_snapshot_increment(raft_server_t* raft,
        void* udata,
        raft_node_t* node)
{
    int *counter = udata;

    (*counter)++;
    return 0;
}

/* static raft_cbs_t generic_funcs = { */
/*     .persist_term = __raft_persist_term, */
/*     .persist_vote = __raft_persist_vote, */
/* }; */

static int max_election_timeout(int election_timeout)
{
	return 2 * election_timeout;
}

// TODO: don't apply logs while snapshotting
// TODO: don't cause elections while snapshotting

void TestRaft_leader_begin_snapshot_fails_if_no_logs_to_compact(CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    msg_entry_response_t cr;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* I am the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));

    /* entry message */
    msg_entry_t *ety = __MAKE_ENTRY(1, 1, "entry");

    /* receive entry */
    raft_recv_entry(r, ety, &cr);

    ety = __MAKE_ENTRY(2, 1, "entry");
    raft_recv_entry(r, ety, &cr);
    CuAssertIntEquals(tc, 2, raft_get_log_count(r));
    CuAssertIntEquals(tc, -1, raft_begin_snapshot(r, 0));

    raft_set_commit_idx(r, 1);
    CuAssertIntEquals(tc, 0, raft_begin_snapshot(r, 0));
}

void TestRaft_leader_will_not_apply_entry_if_snapshot_is_in_progress(CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    msg_entry_response_t cr;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* I am the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));

    /* entry message */
    msg_entry_t *ety = __MAKE_ENTRY(1, 1, "entry");

    /* receive entry */
    raft_recv_entry(r, ety, &cr);
    raft_recv_entry(r, ety, &cr);
    raft_set_commit_idx(r, 1);
    CuAssertIntEquals(tc, 2, raft_get_log_count(r));

    CuAssertIntEquals(tc, 0, raft_begin_snapshot(r, 0));
    CuAssertIntEquals(tc, 1, raft_get_last_applied_idx(r));
    raft_set_commit_idx(r, 2);
    CuAssertIntEquals(tc, -1, raft_apply_entry(r));
    CuAssertIntEquals(tc, 1, raft_get_last_applied_idx(r));
}

void TestRaft_leader_snapshot_end_fails_if_snapshot_not_in_progress(CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* I am the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));
    CuAssertIntEquals(tc, -1, raft_end_snapshot(r));
}

void TestRaft_leader_snapshot_begin_fails_if_less_than_2_logs_to_compact(CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    msg_entry_response_t cr;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* I am the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));

    /* entry message */
    msg_entry_t *ety = __MAKE_ENTRY(1, 1, "entry");

    /* receive entry */
    raft_recv_entry(r, ety, &cr);
    raft_set_commit_idx(r, 1);
    CuAssertIntEquals(tc, 1, raft_get_log_count(r));
    CuAssertIntEquals(tc, -1, raft_begin_snapshot(r, 0));
}

void TestRaft_leader_snapshot_end_succeeds_if_log_compacted(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    msg_entry_response_t cr;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* I am the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));

    /* entry message */
    msg_entry_t *ety = __MAKE_ENTRY(1, 1, "entry");

    /* receive entry */
    raft_recv_entry(r, ety, &cr);

    ety = __MAKE_ENTRY(2, 1, "entry");
    raft_recv_entry(r, ety, &cr);
    ety = __MAKE_ENTRY(3, 1, "entry");
    raft_recv_entry(r, ety, &cr);
    raft_set_commit_idx(r, 2);
    CuAssertIntEquals(tc, 3, raft_get_log_count(r));
    CuAssertIntEquals(tc, 2, raft_get_num_snapshottable_logs(r));
    CuAssertIntEquals(tc, 1, raft_get_last_log_term(r));

    CuAssertIntEquals(tc, 0, raft_begin_snapshot(r, 0));

    int i = raft_get_first_entry_idx(r);
    for (; i < raft_get_commit_idx(r); i++) {
        CuAssertIntEquals(tc, 0, raft_poll_entry(r));
    }

    CuAssertIntEquals(tc, 0, raft_begin_snapshot(r, 0));
    CuAssertIntEquals(tc, 0, raft_end_snapshot(r));
    CuAssertIntEquals(tc, 0, raft_get_num_snapshottable_logs(r));
    CuAssertIntEquals(tc, 1, raft_get_log_count(r));
    CuAssertIntEquals(tc, 2, raft_get_commit_idx(r));
    CuAssertIntEquals(tc, 2, raft_get_last_applied_idx(r));
    CuAssertIntEquals(tc, 1, raft_get_last_log_term(r));
    CuAssertIntEquals(tc, 0, raft_periodic(r, 1000));


    /* the above test returns correct term as didn't snapshot all entries, makes sure works if we snapshot all */

    /* snapshot doesn't work if only one entry to snapshot, so add another one */
    ety = __MAKE_ENTRY(4, 1, "entry");
    raft_recv_entry(r, ety, &cr);

    raft_set_commit_idx(r, 4);
    CuAssertIntEquals(tc, 0, raft_periodic(r, 1000));
    CuAssertIntEquals(tc, 2, raft_get_log_count(r));
    CuAssertIntEquals(tc, 4, raft_get_commit_idx(r));
    raft_server_private_t *r_p = (raft_server_private_t *) r;
    CuAssertIntEquals(tc, 3, r_p->log_impl->first_idx(r_p->log));
    CuAssertIntEquals(tc, 2, raft_get_num_snapshottable_logs(r));

    i = raft_get_first_entry_idx(r);
    for (; i < raft_get_commit_idx(r); i++) {
        printf("raft_poll_entry(2): %d\n", i);
        CuAssertIntEquals(tc, 0, raft_poll_entry(r));
    }

    CuAssertIntEquals(tc, 0, raft_begin_snapshot(r, 0));
    CuAssertIntEquals(tc, 0, raft_end_snapshot(r));
    CuAssertIntEquals(tc, 0, raft_get_num_snapshottable_logs(r));
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));
    CuAssertIntEquals(tc, 4, raft_get_commit_idx(r));
    CuAssertIntEquals(tc, 4, raft_get_last_applied_idx(r));
    CuAssertIntEquals(tc, 1, raft_get_last_log_term(r));
}

void TestRaft_leader_snapshot_end_succeeds_if_log_compacted2(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    msg_entry_response_t cr;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* I am the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));

    /* entry message */
    msg_entry_t *ety = __MAKE_ENTRY(1, 1, "entry");

    /* receive entry */
    raft_recv_entry(r, ety, &cr);
    ety = __MAKE_ENTRY(2, 1, "entry");
    raft_recv_entry(r, ety, &cr);
    ety = __MAKE_ENTRY(3, 1, "entry");
    raft_recv_entry(r, ety, &cr);
    raft_set_commit_idx(r, 2);
    CuAssertIntEquals(tc, 3, raft_get_log_count(r));
    CuAssertIntEquals(tc, 2, raft_get_num_snapshottable_logs(r));

    CuAssertIntEquals(tc, 0, raft_begin_snapshot(r, 0));

    int i = raft_get_first_entry_idx(r);
    for (; i <= raft_get_commit_idx(r); i++)
        CuAssertIntEquals(tc, 0, raft_poll_entry(r));

    CuAssertIntEquals(tc, 0, raft_end_snapshot(r));
    CuAssertIntEquals(tc, 0, raft_get_num_snapshottable_logs(r));
    CuAssertIntEquals(tc, 1, raft_get_log_count(r));
    CuAssertIntEquals(tc, 2, raft_get_commit_idx(r));
    CuAssertIntEquals(tc, 2, raft_get_last_applied_idx(r));
    CuAssertIntEquals(tc, 0, raft_periodic(r, 1000));
}

void TestRaft_joinee_needs_to_get_snapshot(CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    msg_entry_response_t cr;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* I am the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));

    /* entry message */
    msg_entry_t *ety = __MAKE_ENTRY(1, 1, "entry");

    /* receive entry */
    raft_recv_entry(r, ety, &cr);
    ety = __MAKE_ENTRY(2, 1, "entry");
    raft_recv_entry(r, ety, &cr);
    raft_set_commit_idx(r, 1);
    CuAssertIntEquals(tc, 2, raft_get_log_count(r));
    CuAssertIntEquals(tc, 1, raft_get_num_snapshottable_logs(r));

    CuAssertIntEquals(tc, 0, raft_begin_snapshot(r, 0));
    CuAssertIntEquals(tc, 1, raft_get_last_applied_idx(r));
    CuAssertIntEquals(tc, -1, raft_apply_entry(r));
    CuAssertIntEquals(tc, 1, raft_get_last_applied_idx(r));
}

void TestRaft_follower_load_from_snapshot(CuTest * tc)
{
    raft_cbs_t funcs = {
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_state(r, RAFT_STATE_FOLLOWER);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));

    /* entry message */
    msg_entry_t *ety = __MAKE_ENTRY(1, 1, "entry");

    CuAssertIntEquals(tc, 0, raft_get_log_count(r));
    CuAssertIntEquals(tc, 0, raft_begin_load_snapshot(r, 5, 5));
    CuAssertIntEquals(tc, 0, raft_end_load_snapshot(r));
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));
    CuAssertIntEquals(tc, 0, raft_get_num_snapshottable_logs(r));
    CuAssertIntEquals(tc, 5, raft_get_commit_idx(r));
    CuAssertIntEquals(tc, 5, raft_get_last_applied_idx(r));

    CuAssertIntEquals(tc, 0, raft_periodic(r, 1000));

    /* current idx means snapshot was unnecessary */
    ety = __MAKE_ENTRY(2, 1, "entry");
    raft_append_entry(r, ety);
    ety = __MAKE_ENTRY(3, 1, "entry");
    raft_append_entry(r, ety);
    raft_set_commit_idx(r, 7);
    CuAssertIntEquals(tc, -1, raft_begin_load_snapshot(r, 6, 5));
    CuAssertIntEquals(tc, 7, raft_get_commit_idx(r));
}

void TestRaft_follower_load_from_snapshot_fails_if_term_is_0(CuTest * tc)
{
    raft_cbs_t funcs = {
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_state(r, RAFT_STATE_FOLLOWER);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));
    CuAssertIntEquals(tc, -1, raft_begin_load_snapshot(r, 0, 5));
}

void TestRaft_follower_load_from_snapshot_fails_if_already_loaded(CuTest * tc)
{
    raft_cbs_t funcs = {
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_state(r, RAFT_STATE_FOLLOWER);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));

    CuAssertIntEquals(tc, 0, raft_get_log_count(r));
    CuAssertIntEquals(tc, 0, raft_begin_load_snapshot(r, 5, 5));
    CuAssertIntEquals(tc, 0, raft_end_load_snapshot(r));
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));
    CuAssertIntEquals(tc, 0, raft_get_num_snapshottable_logs(r));
    CuAssertIntEquals(tc, 5, raft_get_commit_idx(r));
    CuAssertIntEquals(tc, 5, raft_get_last_applied_idx(r));

    CuAssertIntEquals(tc, RAFT_ERR_SNAPSHOT_ALREADY_LOADED, raft_begin_load_snapshot(r, 5, 5));
}

void TestRaft_follower_load_from_snapshot_does_not_break_cluster_safety(CuTest * tc)
{
    raft_cbs_t funcs = {
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_state(r, RAFT_STATE_FOLLOWER);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));

    /* entry message */
    __RAFT_APPEND_ENTRY(r, 1, 1, "entry");
    __RAFT_APPEND_ENTRY(r, 2, 1, "entry");
    __RAFT_APPEND_ENTRY(r, 3, 1, "entry");

    CuAssertIntEquals(tc, -1, raft_begin_load_snapshot(r, 2, 2));
}

void TestRaft_follower_load_from_snapshot_fails_if_log_is_newer(CuTest * tc)
{
    raft_cbs_t funcs = {
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_state(r, RAFT_STATE_FOLLOWER);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));

    raft_set_last_applied_idx(r, 5);

    CuAssertIntEquals(tc, -1, raft_begin_load_snapshot(r, 2, 2));
}

void TestRaft_leader_sends_snapshot_when_node_next_index_was_compacted(CuTest* tc)
{
    raft_cbs_t funcs = {
        .send_snapshot = __raft_send_snapshot_increment
    };

    int increment = 0;

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, &increment);

    raft_node_t* node;
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);

    /* entry 1 */
    __RAFT_APPEND_ENTRIES_SEQ_ID(r, 3, 1, 1, "aaa");
    CuAssertIntEquals(tc, 3, raft_get_current_idx(r));

    /* compact entry 1 & 2 */
    CuAssertIntEquals(tc, 0, raft_begin_load_snapshot(r, 2, 3));
    CuAssertIntEquals(tc, 0, raft_end_load_snapshot(r));
    CuAssertIntEquals(tc, 3, raft_get_current_idx(r));

    /* reconfigure nodes based on config data "embedded in snapshot" */
    node = raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);

    /* node wants an entry that was compacted */
    raft_node_set_next_idx(node, raft_get_current_idx(r));

    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 2);

    /* verify snapshot is sent */
    int rc = raft_send_appendentries(r, node);
    CuAssertIntEquals(tc, RAFT_ERR_NEEDS_SNAPSHOT, rc);
    CuAssertIntEquals(tc, 1, increment);

    /* update callbacks, verify correct appendreq is sent after the snapshot */
    funcs = (raft_cbs_t) {
        .send_appendentries = __raft_send_appendentries_capture,
    };

    msg_appendentries_t ae;
    raft_set_callbacks(r, &funcs, &ae);

    /* node wants an entry just one after the snapshot index */
    raft_node_set_next_idx(node, raft_get_current_idx(r) + 1);

    CuAssertIntEquals(tc, 0, raft_send_appendentries(r, node));
    CuAssertIntEquals(tc, 2, ae.term);
    CuAssertIntEquals(tc, 3, ae.prev_log_idx);
    CuAssertIntEquals(tc, 2, ae.prev_log_term);
}

void TestRaft_recv_entry_fails_if_snapshot_in_progress(CuTest* tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    msg_entry_response_t cr;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* I am the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));

    /* entry message */
    msg_entry_t *ety = __MAKE_ENTRY(1, 1, "entry");

    /* receive entry */
    raft_recv_entry(r, ety, &cr);
    ety = __MAKE_ENTRY(2, 1, "entry");
    raft_recv_entry(r, ety, &cr);
    CuAssertIntEquals(tc, 2, raft_get_log_count(r));

    raft_set_commit_idx(r, 1);
    CuAssertIntEquals(tc, 0, raft_begin_snapshot(r, 0));

    ety = __MAKE_ENTRY(3, 1, "entry");
    ety->type = RAFT_LOGTYPE_ADD_NODE;
    CuAssertIntEquals(tc, RAFT_ERR_SNAPSHOT_IN_PROGRESS, raft_recv_entry(r, ety, &cr));
}

void TestRaft_recv_entry_succeeds_if_snapshot_nonblocking_apply_is_set(CuTest* tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    msg_entry_response_t cr;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* I am the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));

    /* entry message */
    msg_entry_t *ety = __MAKE_ENTRY(1, 1, "entry");

    /* receive entry */
    raft_recv_entry(r, ety, &cr);
    ety = __MAKE_ENTRY(2, 1, "entry");
    raft_recv_entry(r, ety, &cr);
    CuAssertIntEquals(tc, 2, raft_get_log_count(r));

    raft_set_commit_idx(r, 1);
    CuAssertIntEquals(tc, 0, raft_begin_snapshot(r, RAFT_SNAPSHOT_NONBLOCKING_APPLY));

    ety = __MAKE_ENTRY(3, 1, "entry");
    ety->type = RAFT_LOGTYPE_ADD_NODE;
    CuAssertIntEquals(tc, 0, raft_recv_entry(r, ety, &cr));
}


void TestRaft_follower_recv_appendentries_is_successful_when_previous_log_idx_equals_snapshot_last_idx(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_term = __raft_persist_term,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    CuAssertIntEquals(tc, 0, raft_begin_load_snapshot(r, 2, 2));
    CuAssertIntEquals(tc, 0, raft_end_load_snapshot(r));

    msg_appendentries_t ae;
    msg_appendentries_response_t aer;

    memset(&ae, 0, sizeof(msg_appendentries_t));
    ae.term = 3;
    ae.prev_log_idx = 2;
    ae.prev_log_term = 2;
    /* include entries */
    ae.entries = __MAKE_ENTRY_ARRAY(3, 3, NULL);
    ae.n_entries = 1;
    CuAssertIntEquals(tc, 0, raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer));
    CuAssertIntEquals(tc, 1, aer.success);
}

void TestRaft_leader_sends_appendentries_with_correct_prev_log_idx_when_snapshotted(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = sender_appendentries,
        .log                = NULL
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_set_callbacks(r, &funcs, sender);
    CuAssertTrue(tc, NULL != raft_add_node(r, NULL, 1, 1));
    CuAssertTrue(tc, NULL != raft_add_node(r, NULL, 2, 0));

    CuAssertIntEquals(tc, 0, raft_begin_load_snapshot(r, 2, 4));
    CuAssertIntEquals(tc, 0, raft_end_load_snapshot(r));

    /* i'm leader */
    raft_set_state(r, RAFT_STATE_LEADER);

    /* reload node configuration; we expect the app to decode it from
     * the loaded snapshot.
     */
    raft_node_t* p = raft_add_node(r, NULL, 2, 0);
    CuAssertTrue(tc, NULL != p);
    raft_node_set_next_idx(p, 5);

    /* receive appendentries messages */
    raft_send_appendentries(r, p);
    msg_appendentries_t* ae = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != ae);
    CuAssertIntEquals(tc, 2, ae->prev_log_term);
    CuAssertIntEquals(tc, 4, ae->prev_log_idx);
}

void TestRaft_cancel_snapshot_restores_state(CuTest* tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    msg_entry_response_t cr;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* I am the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));

    /* single entry */
    msg_entry_t *ety = __MAKE_ENTRY(1, 1, "entry");
    raft_recv_entry(r, ety, &cr);

    ety = __MAKE_ENTRY(2, 1, "entry");
    raft_recv_entry(r, ety, &cr);
    raft_set_commit_idx(r, 2);

    CuAssertIntEquals(tc, 0, raft_begin_snapshot(r, 0));
    CuAssertIntEquals(tc, 0, raft_end_snapshot(r));

    /* more entries  */
    ety = __MAKE_ENTRY(3, 1, "entry");
    raft_recv_entry(r, ety, &cr);
    ety = __MAKE_ENTRY(4, 1, "entry");
    raft_recv_entry(r, ety, &cr);
    CuAssertIntEquals(tc, 2, raft_get_log_count(r));
    CuAssertIntEquals(tc, 2, raft_get_snapshot_last_idx(r));

    /* begin and cancel another snapshot */
    raft_set_commit_idx(r, 4);
    CuAssertIntEquals(tc, 0, raft_begin_snapshot(r, 0));
    CuAssertIntEquals(tc, 1, raft_snapshot_is_in_progress(r));
    CuAssertIntEquals(tc, 0, raft_cancel_snapshot(r));

    /* snapshot no longer in progress, index must not have changed */
    CuAssertIntEquals(tc, 0, raft_snapshot_is_in_progress(r));
    CuAssertIntEquals(tc, 2, raft_get_snapshot_last_idx(r));
}

void TestRaft_leader_sends_snapshot_if_log_was_compacted(CuTest* tc)
{
    raft_cbs_t funcs = {
        .send_snapshot = __raft_send_snapshot_increment,
        .send_appendentries = __raft_send_appendentries
    };

    int send_snapshot_count = 0;

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, &send_snapshot_count);

    raft_node_t* node;
    raft_add_node(r, NULL, 1, 1);
    node = raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);

    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);

    /* entry 1 */
    char *str = "aaa";
    __RAFT_APPEND_ENTRY(r, 1, 1, str);

    /* entry 2 */
    __RAFT_APPEND_ENTRY(r, 2, 1, str);

    /* entry 3 */
    __RAFT_APPEND_ENTRY(r, 3, 1, str);
    CuAssertIntEquals(tc, 3, raft_get_current_idx(r));

    /* compact entry 1 & 2 */
    raft_set_commit_idx(r, 2);
    CuAssertIntEquals(tc, 0, raft_begin_snapshot(r, 0));
    CuAssertIntEquals(tc, 0, raft_end_snapshot(r));
    CuAssertIntEquals(tc, 1, raft_get_log_count(r));

    /* at this point a snapshot should have been sent; we'll continue
     * assuming the node was not available to get it.
     */
    CuAssertIntEquals(tc, 2, send_snapshot_count);
    send_snapshot_count = 0;

    /* node wants an entry that was compacted */
    raft_node_set_match_idx(node, 1);
    raft_node_set_next_idx(node, 2);

    msg_appendentries_response_t aer;
    aer.term = 1;
    aer.success = 0;
    aer.current_idx = 1;

    raft_recv_appendentries_response(r, node, &aer);
    CuAssertIntEquals(tc, 1, send_snapshot_count);
}

int main(void)
{
    CuString *output = CuStringNew();
    CuSuite* suite = CuSuiteNew();

    SUITE_ADD_TEST(suite, TestRaft_leader_begin_snapshot_fails_if_no_logs_to_compact);
    SUITE_ADD_TEST(suite, TestRaft_leader_will_not_apply_entry_if_snapshot_is_in_progress);
    SUITE_ADD_TEST(suite, TestRaft_leader_snapshot_end_fails_if_snapshot_not_in_progress);
    SUITE_ADD_TEST(suite, TestRaft_leader_snapshot_begin_fails_if_less_than_2_logs_to_compact);
    SUITE_ADD_TEST(suite, TestRaft_leader_snapshot_end_succeeds_if_log_compacted);
    SUITE_ADD_TEST(suite, TestRaft_leader_snapshot_end_succeeds_if_log_compacted2);
    SUITE_ADD_TEST(suite, TestRaft_joinee_needs_to_get_snapshot);
    SUITE_ADD_TEST(suite, TestRaft_follower_load_from_snapshot);
    SUITE_ADD_TEST(suite, TestRaft_follower_load_from_snapshot_fails_if_term_is_0);
    SUITE_ADD_TEST(suite, TestRaft_follower_load_from_snapshot_fails_if_already_loaded);
    SUITE_ADD_TEST(suite, TestRaft_follower_load_from_snapshot_does_not_break_cluster_safety);
    SUITE_ADD_TEST(suite, TestRaft_follower_load_from_snapshot_fails_if_log_is_newer);
    SUITE_ADD_TEST(suite, TestRaft_leader_sends_snapshot_when_node_next_index_was_compacted);
    SUITE_ADD_TEST(suite, TestRaft_recv_entry_fails_if_snapshot_in_progress);
    SUITE_ADD_TEST(suite, TestRaft_recv_entry_succeeds_if_snapshot_nonblocking_apply_is_set);
    SUITE_ADD_TEST(suite, TestRaft_follower_recv_appendentries_is_successful_when_previous_log_idx_equals_snapshot_last_idx);
    SUITE_ADD_TEST(suite, TestRaft_leader_sends_appendentries_with_correct_prev_log_idx_when_snapshotted);
    SUITE_ADD_TEST(suite, TestRaft_cancel_snapshot_restores_state);
    SUITE_ADD_TEST(suite, TestRaft_leader_sends_snapshot_if_log_was_compacted);

    CuSuiteRun(suite);
    CuSuiteDetails(suite, output);
    printf("%s\n", output->buffer);

    return suite->failCount == 0 ? 0 : 1;
}
