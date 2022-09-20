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

static int __raft_persist_metadata(
    raft_server_t* raft,
    void *udata,
    raft_term_t term,
    raft_node_id_t vote
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
                            raft_requestvote_req_t* msg)
{
    return 0;
}

static int __raft_send_appendentries(raft_server_t* raft,
                              void* udata,
                              raft_node_t* node,
                              raft_appendentries_req_t* msg)
{
    return 0;
}

static int __raft_send_appendentries_capture(raft_server_t* raft,
                              void* udata,
                              raft_node_t* node,
                              raft_appendentries_req_t* msg)
{
    *((raft_appendentries_req_t*)udata) = *msg;
    return 0;
}

static int __raft_send_snapshot_increment(raft_server_t* raft,
        void* udata,
        raft_node_t* node,
        raft_snapshot_req_t *msg)
{
    int *counter = udata;

    (*counter)++;
    return 0;
}

static int __raft_get_snapshot_chunk(raft_server_t* raft,
    void *user_data,
    raft_node_t* node,
    raft_size_t offset,
    raft_snapshot_chunk_t* chunk)
{
    if (offset > 0) {
        return RAFT_ERR_DONE;
    }

    chunk->data = "test";
    chunk->len = strlen("test");
    chunk->last_chunk = 1;

    return 0;
}

static int __raft_store_snapshot_chunk(raft_server_t* raft,
    void *user_data,
    raft_index_t snapshot_index,
    raft_size_t offset,
    raft_snapshot_chunk_t* chunk)
{
    return 0;
}

static int __raft_clear_snapshot(raft_server_t* raft,
    void *user_data)
{
    return 0;
}

struct test_data
{
    int send;
    int get_chunk;
    int store_chunk;
    int clear;
    int load;
};

static int test_send_snapshot_increment(raft_server_t* raft,
                                          void* udata,
                                          raft_node_t* node,
                                          raft_snapshot_req_t *msg)
{
    struct test_data *t = udata;

    t->send++;
    return 0;
}

static int test_get_snapshot_chunk(raft_server_t* raft,
                                     void *user_data,
                                     raft_node_t* node,
                                     raft_size_t offset,
                                     raft_snapshot_chunk_t* chunk)
{
    struct test_data *t = user_data;

    t->get_chunk++;

    if (offset > 0) {
        return RAFT_ERR_DONE;
    }

    chunk->data = "test";
    chunk->len = strlen("test");
    chunk->last_chunk = 1;

    return 0;
}

static int test_store_snapshot_chunk(raft_server_t* raft,
                                       void *user_data,
                                       raft_index_t snapshot_index,
                                       raft_size_t offset,
                                       raft_snapshot_chunk_t* chunk)
{
    struct test_data *t = user_data;

    t->store_chunk++;
    return 0;
}

static int test_clear_snapshot(raft_server_t* raft,
                                 void *user_data)
{
    struct test_data *t = user_data;
    t->clear++;

    return 0;
}

static int test_load_snapshot(raft_server_t* raft,
                               void *user_data,
                              raft_term_t snapshot_term,
                              raft_index_t snapshot_index)
{
    struct test_data *t = user_data;
    t->load++;

    raft_begin_load_snapshot(raft, snapshot_term, snapshot_index);
    raft_end_load_snapshot(raft);

    return 0;
}

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

    raft_entry_resp_t cr;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* I am the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));

    /* entry message */
    raft_entry_req_t *ety = __MAKE_ENTRY(1, 1, "entry");

    /* receive entry */
    raft_recv_entry(r, ety, &cr);

    ety = __MAKE_ENTRY(2, 1, "entry");
    raft_recv_entry(r, ety, &cr);
    CuAssertIntEquals(tc, 2, raft_get_log_count(r));
    CuAssertIntEquals(tc, -1, raft_begin_snapshot(r));

    raft_set_commit_idx(r, 1);
    CuAssertIntEquals(tc, 0, raft_begin_snapshot(r));
}

void TestRaft_leader_will_not_apply_entry_if_snapshot_is_in_progress(CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_entry_resp_t cr;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* I am the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));

    /* entry message */
    raft_entry_req_t *ety = __MAKE_ENTRY(1, 1, "entry");

    /* receive entry */
    raft_recv_entry(r, ety, &cr);
    raft_recv_entry(r, ety, &cr);
    raft_set_commit_idx(r, 1);
    CuAssertIntEquals(tc, 2, raft_get_log_count(r));

    CuAssertIntEquals(tc, 0, raft_begin_snapshot(r));
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

    raft_entry_resp_t cr;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* I am the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));

    /* entry message */
    raft_entry_req_t *ety = __MAKE_ENTRY(1, 1, "entry");

    /* receive entry */
    raft_recv_entry(r, ety, &cr);
    raft_set_commit_idx(r, 1);
    CuAssertIntEquals(tc, 1, raft_get_log_count(r));
    CuAssertIntEquals(tc, -1, raft_begin_snapshot(r));
}

void TestRaft_leader_snapshot_end_succeeds_if_log_compacted(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
        .send_appendentries = __raft_send_appendentries,
    };

    raft_server_t *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_entry_resp_t cr;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* I am the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));

    /* entry message */
    raft_entry_req_t *ety = __MAKE_ENTRY(1, 1, "entry");

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
    CuAssertIntEquals(tc, 0, raft_begin_snapshot(r));
    CuAssertIntEquals(tc, 0, raft_end_snapshot(r));
    CuAssertIntEquals(tc, 0, raft_get_num_snapshottable_logs(r));
    CuAssertIntEquals(tc, 1, raft_get_log_count(r));
    CuAssertIntEquals(tc, 2, raft_get_commit_idx(r));
    CuAssertIntEquals(tc, 2, raft_get_last_applied_idx(r));
    CuAssertIntEquals(tc, 1, raft_get_last_log_term(r));
    CuAssertIntEquals(tc, 0, raft_periodic_internal(r, 1000));


    /* the above test returns correct term as didn't snapshot all entries, makes sure works if we snapshot all */

    /* snapshot doesn't work if only one entry to snapshot, so add another one */
    ety = __MAKE_ENTRY(4, 1, "entry");
    raft_recv_entry(r, ety, &cr);

    raft_set_commit_idx(r, 4);
    CuAssertIntEquals(tc, 0, raft_periodic_internal(r, 1000));
    CuAssertIntEquals(tc, 2, raft_get_log_count(r));
    CuAssertIntEquals(tc, 4, raft_get_commit_idx(r));
    CuAssertIntEquals(tc, 3, r->log_impl->first_idx(r->log));
    CuAssertIntEquals(tc, 2, raft_get_num_snapshottable_logs(r));
    CuAssertIntEquals(tc, 0, raft_begin_snapshot(r));
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
        .persist_metadata = __raft_persist_metadata,
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_entry_resp_t cr;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* I am the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));

    /* entry message */
    raft_entry_req_t *ety = __MAKE_ENTRY(1, 1, "entry");

    /* receive entry */
    raft_recv_entry(r, ety, &cr);
    ety = __MAKE_ENTRY(2, 1, "entry");
    raft_recv_entry(r, ety, &cr);
    ety = __MAKE_ENTRY(3, 1, "entry");
    raft_recv_entry(r, ety, &cr);
    raft_set_commit_idx(r, 2);
    CuAssertIntEquals(tc, 3, raft_get_log_count(r));
    CuAssertIntEquals(tc, 2, raft_get_num_snapshottable_logs(r));

    CuAssertIntEquals(tc, 0, raft_begin_snapshot(r));
    CuAssertIntEquals(tc, 0, raft_end_snapshot(r));
    CuAssertIntEquals(tc, 0, raft_get_num_snapshottable_logs(r));
    CuAssertIntEquals(tc, 1, raft_get_log_count(r));
    CuAssertIntEquals(tc, 2, raft_get_commit_idx(r));
    CuAssertIntEquals(tc, 2, raft_get_last_applied_idx(r));
    CuAssertIntEquals(tc, 0, raft_periodic_internal(r, 1000));
}

void TestRaft_joinee_needs_to_get_snapshot(CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_entry_resp_t cr;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* I am the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));

    /* entry message */
    raft_entry_req_t *ety = __MAKE_ENTRY(1, 1, "entry");

    /* receive entry */
    raft_recv_entry(r, ety, &cr);
    ety = __MAKE_ENTRY(2, 1, "entry");
    raft_recv_entry(r, ety, &cr);
    raft_set_commit_idx(r, 1);
    CuAssertIntEquals(tc, 2, raft_get_log_count(r));
    CuAssertIntEquals(tc, 1, raft_get_num_snapshottable_logs(r));

    CuAssertIntEquals(tc, 0, raft_begin_snapshot(r));
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
    raft_entry_req_t *ety = __MAKE_ENTRY(1, 1, "entry");

    CuAssertIntEquals(tc, 0, raft_get_log_count(r));
    CuAssertIntEquals(tc, 0, raft_begin_load_snapshot(r, 5, 5));
    CuAssertIntEquals(tc, 0, raft_end_load_snapshot(r));
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));
    CuAssertIntEquals(tc, 0, raft_get_num_snapshottable_logs(r));
    CuAssertIntEquals(tc, 5, raft_get_commit_idx(r));
    CuAssertIntEquals(tc, 5, raft_get_last_applied_idx(r));

    CuAssertIntEquals(tc, 0, raft_periodic_internal(r, 1000));

    /* current idx means snapshot was unnecessary */
    ety = __MAKE_ENTRY(2, 1, "entry");
    raft_append_entry(r, ety);
    ety = __MAKE_ENTRY(3, 1, "entry");
    raft_append_entry(r, ety);
    raft_set_commit_idx(r, 7);
    CuAssertIntEquals(tc, RAFT_ERR_MISUSE, raft_begin_load_snapshot(r, 6, 5));
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
    CuAssertIntEquals(tc, RAFT_ERR_MISUSE, raft_begin_load_snapshot(r, 0, 5));
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
    raft_set_current_term(r, 1);

    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_state(r, RAFT_STATE_FOLLOWER);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));

    /* entry message */
    __RAFT_APPEND_ENTRY(r, 1, 1, "entry");
    __RAFT_APPEND_ENTRY(r, 2, 1, "entry");
    __RAFT_APPEND_ENTRY(r, 3, 1, "entry");

    CuAssertIntEquals(tc, RAFT_ERR_MISUSE, raft_begin_load_snapshot(r, 2, 2));
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

    CuAssertIntEquals(tc, RAFT_ERR_MISUSE, raft_begin_load_snapshot(r, 2, 2));
}

void TestRaft_leader_sends_snapshot_when_node_next_index_was_compacted(CuTest* tc)
{
    raft_cbs_t funcs = {
        .send_snapshot = __raft_send_snapshot_increment,
        .clear_snapshot = __raft_clear_snapshot,
        .store_snapshot_chunk = __raft_store_snapshot_chunk,
        .get_snapshot_chunk = __raft_get_snapshot_chunk
    };

    int increment = 0;

    void *r = raft_new();
    raft_set_current_term(r, 3);
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

    /* verify snapshot is sent */
    int rc = raft_send_appendentries(r, node);
    CuAssertIntEquals(tc, 0, rc);
    CuAssertIntEquals(tc, 1, increment);

    /* update callbacks, verify correct appendreq is sent after the snapshot */
    funcs = (raft_cbs_t) {
        .send_appendentries = __raft_send_appendentries_capture,
    };

    raft_appendentries_req_t ae;
    raft_set_callbacks(r, &funcs, &ae);

    /* node wants an entry just one after the snapshot index */
    raft_node_set_next_idx(node, raft_get_current_idx(r) + 1);

    CuAssertIntEquals(tc, 0, raft_send_appendentries(r, node));
    CuAssertIntEquals(tc, 3, ae.term);
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

    raft_entry_resp_t cr;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* I am the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));

    /* entry message */
    raft_entry_req_t *ety = __MAKE_ENTRY(1, 1, "entry");

    /* receive entry */
    raft_recv_entry(r, ety, &cr);
    ety = __MAKE_ENTRY(2, 1, "entry");
    raft_recv_entry(r, ety, &cr);
    CuAssertIntEquals(tc, 2, raft_get_log_count(r));

    raft_set_commit_idx(r, 1);
    CuAssertIntEquals(tc, 0, raft_begin_snapshot(r));

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

    raft_entry_resp_t cr;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* I am the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));

    /* entry message */
    raft_entry_req_t *ety = __MAKE_ENTRY(1, 1, "entry");

    /* receive entry */
    raft_recv_entry(r, ety, &cr);
    ety = __MAKE_ENTRY(2, 1, "entry");
    raft_recv_entry(r, ety, &cr);
    CuAssertIntEquals(tc, 2, raft_get_log_count(r));

    raft_set_commit_idx(r, 1);
    raft_config(r, 1, RAFT_CONFIG_NONBLOCKING_APPLY, 1);

    CuAssertIntEquals(tc, 0, raft_begin_snapshot(r));

    ety = __MAKE_ENTRY(3, 1, "entry");
    ety->type = RAFT_LOGTYPE_ADD_NODE;
    CuAssertIntEquals(tc, 0, raft_recv_entry(r, ety, &cr));
}


void TestRaft_follower_recv_appendentries_is_successful_when_previous_log_idx_equals_snapshot_last_idx(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    CuAssertIntEquals(tc, 0, raft_begin_load_snapshot(r, 2, 2));
    CuAssertIntEquals(tc, 0, raft_end_load_snapshot(r));

    raft_appendentries_req_t ae;
    raft_appendentries_resp_t aer;

    memset(&ae, 0, sizeof(raft_appendentries_req_t));
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
    raft_appendentries_req_t* ae = sender_poll_msg_data(sender);
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

    raft_entry_resp_t cr;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* I am the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    CuAssertIntEquals(tc, 0, raft_get_log_count(r));

    /* single entry */
    raft_entry_req_t *ety = __MAKE_ENTRY(1, 1, "entry");
    raft_recv_entry(r, ety, &cr);

    ety = __MAKE_ENTRY(2, 1, "entry");
    raft_recv_entry(r, ety, &cr);
    raft_set_commit_idx(r, 2);

    CuAssertIntEquals(tc, 0, raft_begin_snapshot(r));
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
    CuAssertIntEquals(tc, 0, raft_begin_snapshot(r));
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
        .send_appendentries = __raft_send_appendentries,
        .clear_snapshot = __raft_clear_snapshot,
        .store_snapshot_chunk = __raft_store_snapshot_chunk,
        .get_snapshot_chunk = __raft_get_snapshot_chunk
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
    CuAssertIntEquals(tc, 0, raft_begin_snapshot(r));
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

    raft_appendentries_resp_t aer;
    aer.term = 1;
    aer.success = 0;
    aer.current_idx = 1;

    raft_recv_appendentries_response(r, node, &aer);
    CuAssertIntEquals(tc, 1, send_snapshot_count);
}

void TestRaft_clear_snapshot_on_leader_change(CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_snapshot = test_send_snapshot_increment,
        .send_appendentries = __raft_send_appendentries,
        .clear_snapshot = test_clear_snapshot,
        .load_snapshot = test_load_snapshot,
        .store_snapshot_chunk = test_store_snapshot_chunk,
        .get_snapshot_chunk = test_get_snapshot_chunk
    };

    struct test_data data = {0};

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, &data);
    raft_add_node(r, NULL, 1, 1);

    raft_snapshot_req_t msg = {
        .leader_id = 2,
        .snapshot_index = 1,
        .snapshot_term = 1,
        .msg_id = 1,
        .chunk.data = "tmp",
        .chunk.len = strlen("tmp"),
        .chunk.last_chunk = 0,
    };

    raft_snapshot_resp_t resp = {0};

    raft_recv_snapshot(r, NULL, &msg, &resp);
    CuAssertIntEquals(tc, 1, data.store_chunk);

    msg.msg_id = 2;
    msg.leader_id = 3;

    raft_recv_snapshot(r, NULL, &msg, &resp);
    CuAssertIntEquals(tc, 1, data.clear);
    CuAssertIntEquals(tc, 2, data.store_chunk);
}

void TestRaft_reject_wrong_offset(CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_snapshot = test_send_snapshot_increment,
        .send_appendentries = __raft_send_appendentries,
        .clear_snapshot = test_clear_snapshot,
        .load_snapshot = test_load_snapshot,
        .store_snapshot_chunk = test_store_snapshot_chunk,
        .get_snapshot_chunk = test_get_snapshot_chunk
    };

    struct test_data data = {0};

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, &data);
    raft_add_node(r, NULL, 1, 1);

    raft_snapshot_req_t msg = {
        .leader_id = 2,
        .snapshot_index = 1,
        .snapshot_term = 1,
        .msg_id = 1,
        .chunk.data = "tmp",
        .chunk.offset = 0,
        .chunk.len = 50,
        .chunk.last_chunk = 0,
    };

    raft_snapshot_resp_t resp = {0};

    raft_recv_snapshot(r, NULL, &msg, &resp);
    CuAssertIntEquals(tc, 1, data.store_chunk);

    msg.chunk.offset = 80;
    raft_recv_snapshot(r, NULL, &msg, &resp);

    CuAssertIntEquals(tc, 0, resp.success);
    CuAssertIntEquals(tc, 50, resp.offset);
}

void TestRaft_set_last_chunk_on_duplicate(CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_snapshot = test_send_snapshot_increment,
        .send_appendentries = __raft_send_appendentries,
        .clear_snapshot = test_clear_snapshot,
        .load_snapshot = test_load_snapshot,
        .store_snapshot_chunk = test_store_snapshot_chunk,
        .get_snapshot_chunk = test_get_snapshot_chunk
    };

    struct test_data data = {0};

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, &data);
    raft_add_node(r, NULL, 1, 1);

    raft_snapshot_req_t msg = {
        .term = 1,
        .leader_id = 2,
        .snapshot_index = 5,
        .snapshot_term = 1,
        .msg_id = 1,
        .chunk.data = "tmp",
        .chunk.offset = 0,
        .chunk.len = 50,
        .chunk.last_chunk = 1,
    };

    raft_snapshot_resp_t resp = {0};

    raft_recv_snapshot(r, NULL, &msg, &resp);
    CuAssertIntEquals(tc, 1, data.store_chunk);
    CuAssertIntEquals(tc, 1, data.load);

    raft_snapshot_req_t msg2 = {
        .term = 1,
        .leader_id = 2,
        .snapshot_index = 4,
        .snapshot_term = 1,
        .msg_id = 1,
        .chunk.offset = 0,
        .chunk.len = 50,
        .chunk.last_chunk = 0,
    };

    raft_recv_snapshot(r, NULL, &msg2, &resp);

    CuAssertIntEquals(tc, 1, resp.success);
    CuAssertIntEquals(tc, 1, resp.last_chunk);
}

void TestRaft_set_last_chunk_if_log_is_more_advanced(CuTest * tc)
{
    raft_cbs_t funcs = {
            .send_snapshot = test_send_snapshot_increment,
            .send_appendentries = __raft_send_appendentries,
            .clear_snapshot = test_clear_snapshot,
            .load_snapshot = test_load_snapshot,
            .store_snapshot_chunk = test_store_snapshot_chunk,
            .get_snapshot_chunk = test_get_snapshot_chunk
    };

    struct test_data data = {0};

    void *r = raft_new();
    raft_set_current_term(r, 1);
    raft_set_callbacks(r, &funcs, &data);
    raft_add_node(r, NULL, 1, 1);

    __RAFT_APPEND_ENTRY(r, 1, 1, "entry");
    __RAFT_APPEND_ENTRY(r, 2, 1, "entry");
    __RAFT_APPEND_ENTRY(r, 4, 1, "entry");
    __RAFT_APPEND_ENTRY(r, 5, 1, "entry");
    __RAFT_APPEND_ENTRY(r, 6, 1, "entry");

    raft_snapshot_req_t msg = {
            .term = 1,
            .leader_id = 2,
            .snapshot_index = 5,
            .snapshot_term = 1,
            .msg_id = 1,
            .chunk.data = "tmp",
            .chunk.offset = 0,
            .chunk.len = 50,
            .chunk.last_chunk = 1,
    };

    raft_snapshot_resp_t resp = {0};

    raft_recv_snapshot(r, NULL, &msg, &resp);
    CuAssertIntEquals(tc, 0, data.store_chunk);
    CuAssertIntEquals(tc, 0, data.load);
    CuAssertIntEquals(tc, 1, resp.success);
    CuAssertIntEquals(tc, 1, resp.last_chunk);
}

void TestRaft_restore_after_restart(CuTest * tc)
{
    raft_server_t *r = raft_new();
    raft_add_node(r, NULL, 1, 1);

    /* Restore should fail if the term is not 0 */
    raft_set_current_term(r, 10);
    CuAssertIntEquals(tc, RAFT_ERR_MISUSE, raft_restore_snapshot(r, -1, 3));
    CuAssertIntEquals(tc, RAFT_ERR_MISUSE, raft_restore_snapshot(r, 11, 5));

    r = raft_new();
    raft_add_node(r, NULL, 1, 1);

    /* Restore should work on start-up */
    CuAssertIntEquals(tc, 0, raft_restore_snapshot(r, 11, 5));
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
    SUITE_ADD_TEST(suite, TestRaft_clear_snapshot_on_leader_change);
    SUITE_ADD_TEST(suite, TestRaft_reject_wrong_offset);
    SUITE_ADD_TEST(suite, TestRaft_set_last_chunk_on_duplicate);
    SUITE_ADD_TEST(suite, TestRaft_set_last_chunk_if_log_is_more_advanced);
    SUITE_ADD_TEST(suite, TestRaft_restore_after_restart);

    CuSuiteRun(suite);
    CuSuiteDetails(suite, output);
    printf("%s\n", output->buffer);

    return suite->failCount == 0 ? 0 : 1;
}
