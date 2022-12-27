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

// TODO: leader doesn't timeout and cause election

static int __raft_persist_metadata(
    raft_server_t* raft,
    void *udata,
    raft_term_t term,
    raft_node_id_t vote
    )
{
    return 0;
}

int __raft_applylog(
    raft_server_t* raft,
    void *udata,
    raft_entry_t *ety,
    raft_index_t idx
    )
{
    return 0;
}

int __raft_applylog_shutdown(
    raft_server_t* raft,
    void *udata,
    raft_entry_t *ety,
    raft_index_t idx
    )
{
    return RAFT_ERR_SHUTDOWN;
}

int __raft_send_requestvote(raft_server_t* raft,
                            void* udata,
                            raft_node_t* node,
                            raft_requestvote_req_t * msg)
{
    return 0;
}

static int __raft_send_appendentries(raft_server_t* raft,
                              void* udata,
                              raft_node_t* node,
                                     raft_appendentries_req_t * msg)
{
    return 0;
}

static raft_node_id_t __raft_get_node_id(raft_server_t* raft,
        void *udata,
        raft_entry_t *entry,
        raft_index_t entry_idx)
{
    return atoi(entry->data);
}

static int __raft_log_offer(raft_server_t* raft,
        void* udata,
        raft_entry_t *entry,
        raft_index_t entry_idx)
{
    return 0;
}

static int __raft_node_has_sufficient_logs(
    raft_server_t* raft,
    void *user_data,
    raft_node_t* node)
{
    int *flag = (int*)user_data;
    *flag += 1;
    return 0;
}

raft_cbs_t generic_funcs = {
    .persist_metadata = __raft_persist_metadata,
};

static int max_election_timeout(int election_timeout)
{
	return 2 * election_timeout;
}

void TestRaft_server_voted_for_records_who_we_voted_for(CuTest * tc)
{
    void *r = raft_new();
    raft_set_callbacks(r, &generic_funcs, NULL);
    raft_add_node(r, NULL, 2, 0);
    raft_vote(r, raft_get_node(r, 2));
    CuAssertTrue(tc, 2 == raft_get_voted_for(r));
}

void TestRaft_server_get_my_node(CuTest * tc)
{
    void *r = raft_new();
    raft_node_t* me = raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    CuAssertTrue(tc, me == raft_get_my_node(r));
}

void TestRaft_server_idx_starts_at_1(CuTest * tc)
{
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    CuAssertTrue(tc, 0 == raft_get_current_idx(r));
    raft_set_current_term(r, 1);

    __RAFT_APPEND_ENTRY(r, 1, 1, "aaa");
    CuAssertTrue(tc, 1 == raft_get_current_idx(r));
}

void TestRaft_server_currentterm_defaults_to_0(CuTest * tc)
{
    void *r = raft_new();
    CuAssertTrue(tc, 0 == raft_get_current_term(r));
}

void TestRaft_server_set_currentterm_sets_term(CuTest * tc)
{
    void *r = raft_new();
    raft_set_callbacks(r, &generic_funcs, NULL);
    raft_set_current_term(r, 5);
    CuAssertTrue(tc, 5 == raft_get_current_term(r));
}

void TestRaft_server_voting_results_in_voting(CuTest * tc)
{
    void *r = raft_new();
    raft_set_callbacks(r, &generic_funcs, NULL);
    raft_add_node(r, NULL, 1, 0);
    raft_add_node(r, NULL, 9, 0);

    raft_vote(r, raft_get_node(r, 1));
    CuAssertTrue(tc, 1 == raft_get_voted_for(r));
    raft_vote(r, raft_get_node(r, 9));
    CuAssertTrue(tc, 9 == raft_get_voted_for(r));
}

void TestRaft_server_add_node_makes_non_voting_node_voting(CuTest * tc)
{
    void *r = raft_new();
    void* n1 = raft_add_non_voting_node(r, NULL, 9, 0);

    CuAssertTrue(tc, !raft_node_is_voting(n1));
    raft_add_node(r, NULL, 9, 0);
    CuAssertTrue(tc, raft_node_is_voting(n1));
    CuAssertIntEquals(tc, 1, raft_get_num_nodes(r));
}

void TestRaft_server_add_node_with_already_existing_id_is_not_allowed(CuTest * tc)
{
    void *r = raft_new();
    raft_add_node(r, NULL, 9, 0);
    raft_add_node(r, NULL, 11, 0);

    CuAssertTrue(tc, NULL == raft_add_node(r, NULL, 9, 0));
    CuAssertTrue(tc, NULL == raft_add_node(r, NULL, 11, 0));
}

void TestRaft_server_add_non_voting_node_with_already_existing_id_is_not_allowed(CuTest * tc)
{
    void *r = raft_new();
    raft_add_non_voting_node(r, NULL, 9, 0);
    raft_add_non_voting_node(r, NULL, 11, 0);

    CuAssertTrue(tc, NULL == raft_add_non_voting_node(r, NULL, 9, 0));
    CuAssertTrue(tc, NULL == raft_add_non_voting_node(r, NULL, 11, 0));
}

void TestRaft_server_add_non_voting_node_with_already_existing_voting_id_is_not_allowed(CuTest * tc)
{
    void *r = raft_new();
    raft_add_node(r, NULL, 9, 0);
    raft_add_node(r, NULL, 11, 0);

    CuAssertTrue(tc, NULL == raft_add_non_voting_node(r, NULL, 9, 0));
    CuAssertTrue(tc, NULL == raft_add_non_voting_node(r, NULL, 11, 0));
}

void TestRaft_server_remove_node(CuTest * tc)
{
    void *r = raft_new();
    void* n1 = raft_add_node(r, NULL, 1, 1);
    void* n2 = raft_add_node(r, NULL, 9, 0);

    raft_remove_node(r, n1);
    CuAssertTrue(tc, NULL == raft_get_node(r, 1));
    CuAssertTrue(tc, NULL != raft_get_node(r, 9));
    raft_remove_node(r, n2);
    CuAssertTrue(tc, NULL == raft_get_node(r, 9));
}

void TestRaft_election_start_does_not_increment_term(CuTest * tc)
{
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_set_callbacks(r, &generic_funcs, NULL);
    raft_set_current_term(r, 1);
    raft_election_start(r, 0);
    CuAssertTrue(tc, 1 == raft_get_current_term(r));
}

void TestRaft_election_become_candidate_increments_term(CuTest * tc)
{
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_set_callbacks(r, &generic_funcs, NULL);
    raft_set_current_term(r, 1);
    raft_become_candidate(r);
    CuAssertTrue(tc, 2 == raft_get_current_term(r));
}

void TestRaft_set_state(CuTest * tc)
{
    void *r = raft_new();
    raft_set_state(r, RAFT_STATE_LEADER);
    CuAssertTrue(tc, RAFT_STATE_LEADER == raft_get_state(r));
    CuAssertStrEquals(tc, "leader", raft_get_state_str(r));

    raft_set_state(r, RAFT_STATE_CANDIDATE);
    CuAssertTrue(tc, RAFT_STATE_CANDIDATE == raft_get_state(r));
    CuAssertStrEquals(tc, "candidate", raft_get_state_str(r));

    raft_set_state(r, RAFT_STATE_PRECANDIDATE);
    CuAssertTrue(tc, RAFT_STATE_PRECANDIDATE == raft_get_state(r));
    CuAssertStrEquals(tc, "pre-candidate", raft_get_state_str(r));

    raft_set_state(r, RAFT_STATE_FOLLOWER);
    CuAssertTrue(tc, RAFT_STATE_FOLLOWER == raft_get_state(r));
    CuAssertStrEquals(tc, "follower", raft_get_state_str(r));
}

void TestRaft_server_starts_as_follower(CuTest * tc)
{
    void *r = raft_new();
    CuAssertTrue(tc, RAFT_STATE_FOLLOWER == raft_get_state(r));
}

void TestRaft_server_starts_with_election_timeout_of_1000ms(CuTest * tc)
{
    void *r = raft_new();

    int election_timeout;
    raft_config(r, 0, RAFT_CONFIG_ELECTION_TIMEOUT, &election_timeout);

    CuAssertTrue(tc, 1000 == election_timeout);
}

void TestRaft_server_starts_with_request_timeout_of_200ms(CuTest * tc)
{
    void *r = raft_new();

    int request_timeout;
    raft_config(r, 0, RAFT_CONFIG_REQUEST_TIMEOUT, &request_timeout);

    CuAssertTrue(tc, 200 == request_timeout);
}

void TestRaft_server_entry_append_increases_logidx(CuTest* tc)
{
    void *r = raft_new();
    raft_set_current_term(r, 1);

    raft_entry_t *ety = __MAKE_ENTRY(1, 1, "aaa");
    CuAssertTrue(tc, 0 == raft_get_current_idx(r));
    raft_append_entry(r, ety);
    CuAssertTrue(tc, 1 == raft_get_current_idx(r));
}

// this eems like a total duplicte of previous test case
void TestRaft_server_append_entry_means_entry_gets_current_term(CuTest* tc)
{
    void *r = raft_new();
    raft_set_current_term(r, 1);

    raft_entry_t *ety = __MAKE_ENTRY(1, 1, "aaa");
    CuAssertTrue(tc, 0 == raft_get_current_idx(r));
    raft_append_entry(r, ety);
    CuAssertTrue(tc, 1 == raft_get_current_idx(r));
}

void TestRaft_server_append_entry_is_retrievable(CuTest * tc)
{
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_set_callbacks(r, &generic_funcs, NULL);
    raft_set_state(r, RAFT_STATE_CANDIDATE);

    raft_set_current_term(r, 5);
    __RAFT_APPEND_ENTRY(r, 100, 1, "aaa");

    raft_entry_t* kept =  raft_get_entry_from_idx(r, 1);
    CuAssertIntEquals(tc, 3, kept->data_len);
    CuAssertStrEquals(tc, "aaa", kept->data);
}

static int __raft_logentry_offer(
    raft_server_t* raft,
    void *udata,
    raft_entry_t *ety,
    raft_index_t ety_idx
    )
{
    CuAssertIntEquals(udata, ety_idx, 1);
    memcpy(ety->data, udata, ety->data_len);
    return 0;
}

#if 0
/* TODO: no support for duplicate detection yet */
void
T_estRaft_server_append_entry_not_sucessful_if_entry_with_id_already_appended(
    CuTest* tc)
{
    void *r;
    raft_entry_t ety;
    char *str = "aaa";

    ety.data.buf = str;
    ety.data.len = 3;
    ety.id = 1;
    ety.term = 1;

    r = raft_new();
    CuAssertTrue(tc, 1 == raft_get_current_idx(r));
    raft_append_entry(r, &ety);
    raft_append_entry(r, &ety);
    CuAssertTrue(tc, 2 == raft_get_current_idx(r));

    /* different ID so we can be successful */
    ety.id = 2;
    raft_append_entry(r, &ety);
    CuAssertTrue(tc, 3 == raft_get_current_idx(r));
}
#endif

void TestRaft_server_entry_is_retrieveable_using_idx(CuTest* tc)
{
    raft_entry_t *ety_appended;
    char *str = "aaa";
    char *str2 = "bbb";

    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_set_current_term(r, 1);

    __RAFT_APPEND_ENTRY(r, 1, 1, str);
    __RAFT_APPEND_ENTRY(r, 2, 1, str2);

    CuAssertTrue(tc, NULL != (ety_appended = raft_get_entry_from_idx(r, 2)));
    CuAssertTrue(tc, !strncmp(ety_appended->data, str2, 3));
}

void TestRaft_server_wont_apply_entry_if_we_dont_have_entry_to_apply(CuTest* tc)
{
    void *r = raft_new();
    raft_set_commit_idx(r, 0);
    raft_set_last_applied_idx(r, 0);

    raft_apply_entry(r);
    CuAssertTrue(tc, 0 == raft_get_last_applied_idx(r));
    CuAssertTrue(tc, 0 == raft_get_commit_idx(r));
}

void TestRaft_server_wont_apply_entry_if_there_isnt_a_majority(CuTest* tc)
{
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);
    raft_set_commit_idx(r, 0);
    raft_set_last_applied_idx(r, 0);
    raft_set_current_term(r, 1);

    raft_apply_entry(r);
    CuAssertTrue(tc, 0 == raft_get_last_applied_idx(r));
    CuAssertTrue(tc, 0 == raft_get_commit_idx(r));

    char *str = "aaa";
    __RAFT_APPEND_ENTRY(r, 1, 1, str);
    raft_apply_entry(r);
    /* Not allowed to be applied because we haven't confirmed a majority yet */
    CuAssertTrue(tc, 0 == raft_get_last_applied_idx(r));
    CuAssertTrue(tc, 0 == raft_get_commit_idx(r));
}

/* If commitidx > lastApplied: increment lastApplied, apply log[lastApplied]
 * to state machine (§5.3) */
void TestRaft_server_increment_lastApplied_when_lastApplied_lt_commitidx(
    CuTest* tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
        .applylog = __raft_applylog,
    };

    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_node_set_voting(raft_get_my_node(r), 0);
    raft_set_callbacks(r, &funcs, NULL);

    /* must be follower */
    raft_set_state(r, RAFT_STATE_FOLLOWER);
    raft_set_current_term(r, 1);
    raft_set_last_applied_idx(r, 0);

    /* need at least one entry */
    __RAFT_APPEND_ENTRY(r, 1, 1, "aaa");

    raft_set_commit_idx(r, 1);

    /* let time lapse */
    raft_periodic_internal(r, 1);
    CuAssertIntEquals(tc, 1, raft_get_last_applied_idx(r));
}

void TestRaft_user_applylog_error_propogates_to_periodic(
    CuTest* tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
        .applylog = __raft_applylog_shutdown,
    };

    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_set_callbacks(r, &funcs, NULL);

    /* must be follower */
    raft_set_state(r, RAFT_STATE_FOLLOWER);
    raft_set_current_term(r, 1);
    raft_set_last_applied_idx(r, 0);

    /* need at least one entry */
    __RAFT_APPEND_ENTRY(r, 1, 1, "aaa");

    raft_set_commit_idx(r, 1);

    /* let time lapse */
    CuAssertIntEquals(tc, RAFT_ERR_SHUTDOWN, raft_periodic_internal(r, 1));
    CuAssertIntEquals(tc, 0, raft_get_last_applied_idx(r));
}

void TestRaft_server_apply_entry_increments_last_applied_idx(CuTest* tc)
{
    raft_cbs_t funcs = {
        .applylog = __raft_applylog,
    };

    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_set_callbacks(r, &funcs, NULL);
    raft_set_last_applied_idx(r, 0);
    raft_set_current_term(r, 1);

    __RAFT_APPEND_ENTRY(r, 1, 1, "aaa");
    raft_set_commit_idx(r, 1);
    raft_apply_entry(r);
    CuAssertTrue(tc, 1 == raft_get_last_applied_idx(r));
}

void TestRaft_server_periodic_elapses_election_timeout(CuTest * tc)
{
    void *r = raft_new();
    /* we don't want to set the timeout to zero */
    raft_config(r, 1, RAFT_CONFIG_ELECTION_TIMEOUT, 1000);
    CuAssertTrue(tc, 0 == raft_get_timeout_elapsed(r));

    raft_periodic_internal(r, 0);
    CuAssertTrue(tc, 0 == raft_get_timeout_elapsed(r));

    raft_periodic_internal(r, 100);
    CuAssertTrue(tc, 100 == raft_get_timeout_elapsed(r));
}

void TestRaft_server_election_timeout_does_not_promote_us_to_leader_if_there_is_are_more_than_1_nodes(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
        .send_requestvote = __raft_send_requestvote,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_config(r, 1, RAFT_CONFIG_ELECTION_TIMEOUT, 1000);

    /* clock over (ie. 1000 + 1), causing new election */
    raft_periodic_internal(r, 1001);

    CuAssertTrue(tc, 0 == raft_is_leader(r));
}

void TestRaft_server_election_timeout_does_not_promote_us_to_leader_if_we_are_not_voting_node(CuTest * tc)
{
    void *r = raft_new();
    raft_add_non_voting_node(r, NULL, 1, 1);
    raft_config(r, 1, RAFT_CONFIG_ELECTION_TIMEOUT, 1000);

    /* clock over (ie. 1000 + 1), causing new election */
    raft_periodic_internal(r, 1001);

    CuAssertTrue(tc, 0 == raft_is_leader(r));
    CuAssertTrue(tc, 0 == raft_get_current_term(r));
}

void TestRaft_server_election_timeout_does_not_start_election_if_there_are_no_voting_nodes(CuTest * tc)
{
    void *r = raft_new();
    raft_add_non_voting_node(r, NULL, 1, 1);
    raft_add_non_voting_node(r, NULL, 2, 0);
    raft_config(r, 1, RAFT_CONFIG_ELECTION_TIMEOUT, 1000);

    /* clock over (ie. 1000 + 1), causing new election */
    raft_periodic_internal(r, 1001);

    CuAssertTrue(tc, 0 == raft_get_current_term(r));
}

void TestRaft_server_election_timeout_does_promote_us_to_leader_if_there_is_only_1_node(CuTest * tc)
{
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_config(r, 1, RAFT_CONFIG_ELECTION_TIMEOUT, 1000);
    raft_term_t old_term = raft_get_current_term(r);

    /* clock over (ie. 1000 + 1), causing new election */
    raft_periodic_internal(r, 1001);

    CuAssertTrue(tc, 1 == raft_is_leader(r));
    CuAssertTrue(tc, old_term + 1 == raft_get_current_term(r));
}

void TestRaft_server_election_timeout_does_promote_us_to_leader_if_there_is_only_1_voting_node(CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_non_voting_node(r, NULL, 2, 0);
    raft_config(r, 1, RAFT_CONFIG_ELECTION_TIMEOUT, 1000);

    /* clock over (ie. 1000 + 1), causing new election */
    raft_periodic_internal(r, 1001);

    CuAssertTrue(tc, 1 == raft_is_leader(r));
}

void TestRaft_server_recv_entry_auto_commits_if_we_are_the_only_node(CuTest * tc)
{
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_config(r, 1, RAFT_CONFIG_ELECTION_TIMEOUT, 1000);
    raft_become_leader(r);
    CuAssertTrue(tc, 1 == raft_get_commit_idx(r));

    /* entry message */
    raft_entry_req_t *ety = __MAKE_ENTRY(1, 1, "entry");

    /* receive entry */
    raft_entry_resp_t cr;
    raft_recv_entry(r, ety, &cr);
    CuAssertTrue(tc, 2 == raft_get_log_count(r));
    CuAssertTrue(tc, 2 == raft_get_commit_idx(r));
}

void TestRaft_server_recv_entry_fails_if_there_is_already_a_voting_change(CuTest * tc)
{
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_config(r, 1, RAFT_CONFIG_AUTO_FLUSH, 0);
    raft_config(r, 1, RAFT_CONFIG_ELECTION_TIMEOUT, 1000);
    raft_become_leader(r);
    CuAssertTrue(tc, 1 == raft_get_commit_idx(r));

    /* entry message */
    raft_entry_req_t *ety = __MAKE_ENTRY(1, 1, "entry");
    ety->type = RAFT_LOGTYPE_ADD_NODE;

    /* receive entry */
    raft_entry_resp_t cr;
    CuAssertTrue(tc, 0 == raft_recv_entry(r, ety, &cr));
    CuAssertTrue(tc, 2 == raft_get_log_count(r));

    raft_entry_t *ety2 = __MAKE_ENTRY(2, 1, "entry");
    ety2->type = RAFT_LOGTYPE_ADD_NODE;
    CuAssertTrue(tc, RAFT_ERR_ONE_VOTING_CHANGE_ONLY == raft_recv_entry(r, ety2, &cr));
    CuAssertTrue(tc, 1 == raft_get_commit_idx(r));
}

void TestRaft_server_cfg_sets_num_nodes(CuTest * tc)
{
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    CuAssertTrue(tc, 2 == raft_get_num_nodes(r));
}

void TestRaft_server_cant_get_node_we_dont_have(CuTest * tc)
{
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    CuAssertTrue(tc, NULL == raft_get_node(r, 0));
    CuAssertTrue(tc, NULL != raft_get_node(r, 1));
    CuAssertTrue(tc, NULL != raft_get_node(r, 2));
    CuAssertTrue(tc, NULL == raft_get_node(r, 3));
}

/* If term > currentTerm, set currentTerm to term (step down if candidate or
 * leader) */
void TestRaft_votes_are_majority_is_true(
    CuTest * tc
    )
{
    /* 1 of 3 = lose */
    CuAssertTrue(tc, 0 == raft_votes_is_majority(3, 1));

    /* 2 of 3 = win */
    CuAssertTrue(tc, 1 == raft_votes_is_majority(3, 2));

    /* 2 of 5 = lose */
    CuAssertTrue(tc, 0 == raft_votes_is_majority(5, 2));

    /* 3 of 5 = win */
    CuAssertTrue(tc, 1 == raft_votes_is_majority(5, 3));

    /* 2 of 1?? This is an error */
    CuAssertTrue(tc, 0 == raft_votes_is_majority(1, 2));
}

void TestRaft_server_recv_requestvote_response_dont_increase_votes_for_me_when_not_granted(
    CuTest * tc
    )
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);
    CuAssertTrue(tc, 1 == raft_get_nvotes_for_me(r));

    raft_requestvote_resp_t rvr;
    memset(&rvr, 0, sizeof(raft_requestvote_resp_t));
    rvr.term = 1;
    rvr.vote_granted = 0;
    int e = raft_recv_requestvote_response(r, raft_get_node(r, 2), &rvr);
    CuAssertIntEquals(tc, 0, e);
    CuAssertTrue(tc, 1 == raft_get_nvotes_for_me(r));
}

void TestRaft_server_recv_requestvote_response_dont_increase_votes_for_me_when_term_is_not_equal(
    CuTest * tc
    )
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 3);
    CuAssertTrue(tc, 1 == raft_get_nvotes_for_me(r));

    raft_requestvote_resp_t rvr;
    memset(&rvr, 0, sizeof(raft_requestvote_resp_t));
    rvr.term = 2;
    rvr.vote_granted = 1;
    raft_recv_requestvote_response(r, raft_get_node(r, 2), &rvr);
    CuAssertTrue(tc, 1 == raft_get_nvotes_for_me(r));
}

void TestRaft_server_recv_requestvote_response_increase_votes_for_me(
    CuTest * tc
    )
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
        .send_requestvote = __raft_send_requestvote,
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);
    CuAssertTrue(tc, 1 == raft_get_nvotes_for_me(r));
    CuAssertIntEquals(tc, 1, raft_get_current_term(r));

    raft_become_candidate(r);
    CuAssertIntEquals(tc, 2, raft_get_current_term(r));
    CuAssertTrue(tc, 1 == raft_get_nvotes_for_me(r));

    raft_requestvote_resp_t rvr;
    memset(&rvr, 0, sizeof(raft_requestvote_resp_t));
    rvr.request_term = 2;
    rvr.term = 2;
    rvr.vote_granted = 1;
    int e = raft_recv_requestvote_response(r, raft_get_node(r, 2), &rvr);
    CuAssertIntEquals(tc, 0, e);
    CuAssertTrue(tc, 2 == raft_get_nvotes_for_me(r));
}

void TestRaft_server_recv_requestvote_response_must_be_candidate_to_receive(
    CuTest * tc
    )
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);
    CuAssertTrue(tc, 1 == raft_get_nvotes_for_me(r));

    raft_become_leader(r);

    raft_requestvote_resp_t rvr;
    memset(&rvr, 0, sizeof(raft_requestvote_resp_t));
    rvr.term = 1;
    rvr.vote_granted = 1;
    raft_recv_requestvote_response(r, raft_get_node(r, 2), &rvr);
    CuAssertTrue(tc, 1 == raft_get_nvotes_for_me(r));
}

/* Reply false if term < currentTerm (§5.1) */
void TestRaft_server_recv_requestvote_reply_false_if_term_less_than_current_term(
    CuTest * tc
    )
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_requestvote_resp_t rvr;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 2);

    /* term is less than current term */
    raft_requestvote_req_t rv;
    memset(&rv, 0, sizeof(raft_requestvote_req_t));
    rv.term = 1;
    int e = raft_recv_requestvote(r, raft_get_node(r, 2), &rv, &rvr);
    CuAssertIntEquals(tc, 0, e);
    CuAssertIntEquals(tc, 0, rvr.vote_granted);
}

void TestRaft_leader_recv_requestvote_does_not_step_down(
    CuTest * tc
    )
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_requestvote_resp_t rvr;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);
    raft_vote(r, raft_get_node(r, 1));
    raft_become_leader(r);
    CuAssertIntEquals(tc, 1, raft_is_leader(r));

    /* term is less than current term */
    raft_requestvote_req_t rv;
    memset(&rv, 0, sizeof(raft_requestvote_req_t));
    rv.term = 1;
    raft_recv_requestvote(r, raft_get_node(r, 2), &rv, &rvr);
    CuAssertIntEquals(tc, 1, raft_get_leader_id(r));
}

/* Reply true if term >= currentTerm (§5.1) */
void TestRaft_server_recv_requestvote_reply_true_if_term_greater_than_or_equal_to_current_term(
    CuTest * tc
    )
{
    raft_requestvote_req_t rv;
    raft_requestvote_resp_t rvr;

    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);

    /* term is less than current term */
    memset(&rv, 0, sizeof(raft_requestvote_req_t));
    rv.term = 2;
    rv.last_log_idx = 1;
    raft_recv_requestvote(r, raft_get_node(r, 2), &rv, &rvr);

    CuAssertTrue(tc, 1 == rvr.vote_granted);
}

void TestRaft_server_recv_requestvote_reset_timeout(
    CuTest * tc
    )
{
    raft_requestvote_req_t rv;
    raft_requestvote_resp_t rvr;

    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);

    raft_config(r, 1, RAFT_CONFIG_ELECTION_TIMEOUT, 1000);
    raft_periodic_internal(r, 900);

    memset(&rv, 0, sizeof(raft_requestvote_req_t));
    rv.term = 2;
    rv.last_log_idx = 1;
    raft_recv_requestvote(r, raft_get_node(r, 2), &rv, &rvr);
    CuAssertTrue(tc, 1 == rvr.vote_granted);
    CuAssertIntEquals(tc, 0, raft_get_timeout_elapsed(r));
}

void TestRaft_server_recv_requestvote_candidate_step_down_if_term_is_higher_than_current_term(
    CuTest * tc
    )
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
        .send_requestvote = __raft_send_requestvote,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_become_candidate(r);
    raft_set_current_term(r, 1);
    CuAssertIntEquals(tc, 1, raft_get_voted_for(r));

    /* current term is less than term */
    raft_requestvote_req_t rv;
    memset(&rv, 0, sizeof(raft_requestvote_req_t));
    rv.candidate_id = 2;
    rv.term = 2;
    rv.last_log_idx = 1;
    raft_requestvote_resp_t rvr;
    raft_recv_requestvote(r, raft_get_node(r, 2), &rv, &rvr);
    CuAssertIntEquals(tc, 1, raft_is_follower(r));
    CuAssertIntEquals(tc, 2, raft_get_current_term(r));
    CuAssertIntEquals(tc, 2, raft_get_voted_for(r));
}

void TestRaft_server_recv_requestvote_depends_on_candidate_id(
    CuTest * tc
    )
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
        .send_requestvote = __raft_send_requestvote,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_become_candidate(r);
    raft_set_current_term(r, 1);
    CuAssertIntEquals(tc, 1, raft_get_voted_for(r));

    /* current term is less than term */
    raft_requestvote_req_t rv;
    memset(&rv, 0, sizeof(raft_requestvote_req_t));
    rv.candidate_id = 3;
    rv.term = 2;
    rv.last_log_idx = 1;
    raft_requestvote_resp_t rvr;
    raft_recv_requestvote(r, NULL, &rv, &rvr);
    CuAssertIntEquals(tc, 1, raft_is_follower(r));
    CuAssertIntEquals(tc, 2, raft_get_current_term(r));
    CuAssertIntEquals(tc, 3, raft_get_voted_for(r));
}

/* If votedFor is null or candidateId, and candidate's log is at
 * least as up-to-date as local log, grant vote (§5.2, §5.4) */
void TestRaft_server_recv_requestvote_dont_grant_vote_if_we_didnt_vote_for_this_candidate(
    CuTest * tc
    )
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 0, 0);
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);

    /* vote for self */
    raft_vote_for_nodeid(r, 1);

    raft_requestvote_req_t rv = {};
    rv.term = 1;
    rv.candidate_id = 2;
    rv.last_log_idx = 1;
    rv.last_log_term = 1;
    raft_requestvote_resp_t rvr;
    raft_recv_requestvote(r, raft_get_node(r, 2), &rv, &rvr);
    CuAssertTrue(tc, 0 == rvr.vote_granted);

    /* vote for ID 0 */
    raft_vote_for_nodeid(r, 0);
    raft_recv_requestvote(r, raft_get_node(r, 2), &rv, &rvr);
    CuAssertTrue(tc, 0 == rvr.vote_granted);
}

/* If requestvote is received within the minimum election timeout of
 * hearing from a current leader, it does not update its term or grant its
 * vote (§6).
 */
void TestRaft_server_recv_requestvote_ignore_if_master_is_fresh(CuTest * tc)
{
    raft_cbs_t funcs = { 0
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);
    raft_config(r, 1, RAFT_CONFIG_ELECTION_TIMEOUT, 1000);

    raft_appendentries_req_t ae = { 0 };
    raft_appendentries_resp_t aer;
    ae.term = 1;

    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertTrue(tc, 1 == aer.success);

    raft_requestvote_req_t rv = {
        .prevote = 1,
        .term = 2,
        .candidate_id = 3,
        .last_log_idx = 0,
        .last_log_term = 1
    };
    raft_requestvote_resp_t rvr;
    raft_recv_requestvote(r, raft_get_node(r, 3), &rv, &rvr);
    CuAssertTrue(tc, 1 != rvr.vote_granted);

    /* After election timeout passed, the same requestvote should be accepted */
    raft_periodic_internal(r, 1001);
    raft_recv_requestvote(r, raft_get_node(r, 3), &rv, &rvr);
    CuAssertTrue(tc, 1 == rvr.vote_granted);
}

void TestRaft_server_recv_prevote_ignore_if_candidate(CuTest * tc)
{
    void *r = raft_new();

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);
    raft_config(r, 1, RAFT_CONFIG_ELECTION_TIMEOUT, 1000);

    raft_become_candidate(r);

    raft_requestvote_resp_t rv = {
        .term = 1,
        .request_term = 1,
        .prevote = 1,
    };

    raft_recv_requestvote_response(r, raft_get_node(r, 2), &rv);

    CuAssertTrue(tc, raft_is_candidate(r));
    CuAssertTrue(tc, raft_get_nvotes_for_me(r) == 1);
}

void TestRaft_server_recv_reqvote_ignore_if_not_candidate(CuTest * tc)
{
    void *r = raft_new();

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);
    raft_config(r, 1, RAFT_CONFIG_ELECTION_TIMEOUT, 1000);

    raft_become_precandidate(r);

    raft_requestvote_resp_t rv = {
        .term = 1,
        .request_term = 1
    };

    raft_recv_requestvote_response(r, raft_get_node(r, 2), &rv);

    CuAssertTrue(tc, raft_is_precandidate(r));
    CuAssertTrue(tc, raft_get_nvotes_for_me(r) == 1);
}

void TestRaft_server_recv_reqvote_always_update_term(CuTest * tc)
{
    void *r = raft_new();

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);
    raft_config(r, 1, RAFT_CONFIG_ELECTION_TIMEOUT, 1000);

    raft_become_precandidate(r);

    raft_requestvote_resp_t rv = {
        .term = 3,
        .request_term = 3,
    };

    raft_recv_requestvote_response(r, raft_get_node(r, 2), &rv);

    CuAssertTrue(tc, raft_is_follower(r));
    CuAssertTrue(tc, raft_get_current_term(r) == 3);
}

void TestRaft_follower_becomes_follower_is_follower(CuTest * tc)
{
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_become_follower(r);
    CuAssertTrue(tc, raft_is_follower(r));
}

void TestRaft_follower_becomes_follower_does_not_clear_voted_for(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);

    raft_vote(r, raft_get_node(r, 1));
    CuAssertIntEquals(tc, 1, raft_get_voted_for(r));
    raft_become_follower(r);
    CuAssertIntEquals(tc, 1, raft_get_voted_for(r));
}

/* 5.1 */
void TestRaft_follower_recv_appendentries_reply_false_if_term_less_than_currentterm(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    /* no leader known at this point */
    CuAssertTrue(tc, -1 == raft_get_leader_id(r));

    /* term is low */
    raft_appendentries_req_t ae;
    memset(&ae, 0, sizeof(raft_appendentries_req_t));
    ae.term = 1;

    /*  higher current term */
    raft_set_current_term(r, 5);
    raft_appendentries_resp_t aer;
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    CuAssertTrue(tc, 0 == aer.success);
    /* rejected appendentries doesn't change the current leader. */
    CuAssertTrue(tc, -1 == raft_get_leader_id(r));
}

void TestRaft_follower_recv_snapshot_reply_false_if_term_less_than_currentterm(
        CuTest * tc)
{
    raft_cbs_t funcs = {
            .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* No leader known at this point */
    CuAssertTrue(tc, -1 == raft_get_leader_id(r));

    raft_snapshot_req_t req = {
            .term = 1
    };

    /*  higher current term */
    raft_set_current_term(r, 5);

    raft_snapshot_resp_t resp;
    raft_recv_snapshot(r, raft_get_node(r, 2), &req, &resp);

    CuAssertTrue(tc, resp.success == 0);

    /* rejected snapshot req doesn't change the current leader. */
    CuAssertTrue(tc, raft_get_leader_id(r) == RAFT_NODE_ID_NONE);
}

void TestRaft_follower_recv_appendentries_does_not_need_node(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_appendentries_req_t ae = {};
    ae.term = 1;
    raft_appendentries_resp_t aer;
    raft_recv_appendentries(r, NULL, &ae, &aer);
    CuAssertTrue(tc, 1 == aer.success);
}

/* TODO: check if test case is needed */
void TestRaft_follower_recv_appendentries_updates_currentterm_if_term_gt_currentterm(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_appendentries_req_t ae;
    raft_appendentries_resp_t aer;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /*  older currentterm */
    raft_set_current_term(r, 1);
    CuAssertTrue(tc, -1 == raft_get_leader_id(r));

    /*  newer term for appendentry */
    memset(&ae, 0, sizeof(raft_appendentries_req_t));
    /* no prev log idx */
    ae.prev_log_idx = 0;
    ae.term = 2;
    ae.leader_id = 2;

    /*  appendentry has newer term, so we change our currentterm */
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertTrue(tc, 1 == aer.success);
    CuAssertTrue(tc, 2 == aer.term);
    /* term has been updated */
    CuAssertTrue(tc, 2 == raft_get_current_term(r));
    /* and leader has been updated */
    CuAssertIntEquals(tc, 2, raft_get_leader_id(r));
}

void TestRaft_follower_recv_appendentries_does_not_log_if_no_entries_are_specified(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_appendentries_req_t ae;
    raft_appendentries_resp_t aer;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_state(r, RAFT_STATE_FOLLOWER);

    /*  log size s */
    CuAssertTrue(tc, 0 == raft_get_log_count(r));

    /* receive an appendentry with commit */
    memset(&ae, 0, sizeof(raft_appendentries_req_t));
    ae.term = 1;
    ae.prev_log_term = 1;
    ae.prev_log_idx = 4;
    ae.leader_commit = 5;
    ae.n_entries = 0;

    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertTrue(tc, 0 == raft_get_log_count(r));
}

void TestRaft_follower_recv_appendentries_increases_log(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_appendentries_req_t ae;
    raft_appendentries_resp_t aer;
    char *str = "aaa";

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_state(r, RAFT_STATE_FOLLOWER);

    /*  log size s */
    CuAssertTrue(tc, 0 == raft_get_log_count(r));

    /* receive an appendentry with commit */
    memset(&ae, 0, sizeof(raft_appendentries_req_t));
    ae.term = 3;
    ae.prev_log_term = 0;
    /* first appendentries msg */
    ae.prev_log_idx = 0;
    ae.leader_commit = 5;
    /* include one entry */
    /* check that old terms are passed onto the log */
    ae.entries = __MAKE_ENTRY_ARRAY(1, 2, str);
    ae.n_entries = 1;

    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertTrue(tc, 1 == aer.success);
    CuAssertTrue(tc, 1 == raft_get_log_count(r));
    raft_entry_t* log = raft_get_entry_from_idx(r, 1);
    CuAssertTrue(tc, 2 == log->term);
}

/*  5.3 */
void TestRaft_follower_recv_appendentries_reply_false_if_doesnt_have_log_at_prev_log_idx_which_matches_prev_log_term(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    char *str = "aaa";

    raft_appendentries_req_t ae;
    raft_appendentries_resp_t aer;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* term is different from appendentries */
    raft_set_current_term(r, 2);
    // TODO at log manually?

    /* log idx that server doesn't have */
    memset(&ae, 0, sizeof(raft_appendentries_req_t));
    ae.term = 2;
    ae.prev_log_idx = 1;
    /* prev_log_term is less than current term (ie. 2) */
    ae.prev_log_term = 1;
    /* include one entry */
    ae.entries = __MAKE_ENTRY_ARRAY(1, 0, str);
    ae.n_entries = 1;

    /* trigger reply */
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    /* reply is false */
    CuAssertTrue(tc, 0 == aer.success);
}

static raft_entry_t* __create_mock_entries_for_conflict_tests(
        CuTest * tc,
        raft_server_t* r,
        char** strs)
{
    raft_entry_t *ety_appended;

    /* increase log size */
    char *str1 = strs[0];
    __RAFT_APPEND_ENTRY(r, 1, 1, str1);
    CuAssertTrue(tc, 1 == raft_get_log_count(r));

    /* this log will be overwritten by a later appendentries */
    char *str2 = strs[1];
    __RAFT_APPEND_ENTRY(r, 2, 1, str2);
    CuAssertTrue(tc, 2 == raft_get_log_count(r));
    CuAssertTrue(tc, NULL != (ety_appended = raft_get_entry_from_idx(r, 2)));
    CuAssertTrue(tc, !strncmp(ety_appended->data, str2, 3));

    /* this log will be overwritten by a later appendentries */
    char *str3 = strs[2];
    __RAFT_APPEND_ENTRY(r, 3, 1, str3);
    CuAssertTrue(tc, 3 == raft_get_log_count(r));
    CuAssertTrue(tc, NULL != (ety_appended = raft_get_entry_from_idx(r, 3)));
    CuAssertTrue(tc, !strncmp(ety_appended->data, str3, 3));

    return ety_appended;
}

/* 5.3 */
void TestRaft_follower_recv_appendentries_delete_entries_if_conflict_with_new_entries_via_prev_log_idx(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_appendentries_req_t ae;
    raft_appendentries_resp_t aer;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_current_term(r, 1);

    char* strs[] = {"111", "222", "333"};
    raft_entry_t *ety_appended = __create_mock_entries_for_conflict_tests(tc, r, strs);

    /* pass a appendentry that is newer  */
    memset(&ae, 0, sizeof(raft_appendentries_req_t));
    ae.term = 2;
    /* entries from 2 onwards will be overwritten by this appendentries message */
    ae.prev_log_idx = 1;
    ae.prev_log_term = 1;
    /* include one entry */
    char *str4 = "444";
    ae.entries = __MAKE_ENTRY_ARRAY(4, 0, str4);
    ae.n_entries = 1;

    /* str4 has overwritten the last 2 entries */
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertTrue(tc, 1 == aer.success);
    CuAssertTrue(tc, 2 == raft_get_log_count(r));
    /* str1 is still there */
    CuAssertTrue(tc, NULL != (ety_appended = raft_get_entry_from_idx(r, 1)));
    CuAssertTrue(tc, !strncmp(ety_appended->data, strs[0], 3));
    /* str4 has overwritten the last 2 entries */
    CuAssertTrue(tc, NULL != (ety_appended = raft_get_entry_from_idx(r, 2)));
    CuAssertTrue(tc, !strncmp(ety_appended->data, str4, 3));
}

void TestRaft_follower_recv_appendentries_delete_entries_if_conflict_with_new_entries_via_prev_log_idx_at_idx_0(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_appendentries_req_t ae;
    raft_appendentries_resp_t aer;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_current_term(r, 1);

    char* strs[] = {"111", "222", "333"};
    raft_entry_t *ety_appended = __create_mock_entries_for_conflict_tests(tc, r, strs);

    /* pass a appendentry that is newer  */

    memset(&ae, 0, sizeof(raft_appendentries_req_t));
    ae.term = 2;
    /* ALL append entries will be overwritten by this appendentries message */
    ae.prev_log_idx = 0;
    ae.prev_log_term = 0;
    /* include one entry */
    char *str4 = "444";
    ae.entries = __MAKE_ENTRY_ARRAY(4, 0, str4);
    ae.n_entries = 1;

    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertTrue(tc, 1 == aer.success);
    CuAssertTrue(tc, 1 == raft_get_log_count(r));
    /* str1 is gone */
    CuAssertTrue(tc, NULL != (ety_appended = raft_get_entry_from_idx(r, 1)));
    CuAssertTrue(tc, !strncmp(ety_appended->data, str4, 3));
}

void TestRaft_follower_recv_appendentries_delete_entries_if_conflict_with_new_entries_greater_than_prev_log_idx(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_appendentries_req_t ae;
    raft_appendentries_resp_t aer;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_current_term(r, 1);

    char* strs[] = {"111", "222", "333"};
    raft_entry_t *ety_appended;

    __create_mock_entries_for_conflict_tests(tc, r, strs);
    CuAssertIntEquals(tc, 3, raft_get_log_count(r));

    memset(&ae, 0, sizeof(raft_appendentries_req_t));
    ae.term = 2;
    ae.prev_log_idx = 1;
    ae.prev_log_term = 1;

    ae.entries = __MAKE_ENTRY_ARRAY(1, 0, NULL);
    ae.n_entries = 1;

    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertTrue(tc, 1 == aer.success);
    CuAssertIntEquals(tc, 2, raft_get_log_count(r));
    CuAssertTrue(tc, NULL != (ety_appended = raft_get_entry_from_idx(r, 1)));
    CuAssertTrue(tc, !strncmp(ety_appended->data, strs[0], 3));
}

// TODO: add TestRaft_follower_recv_appendentries_delete_entries_if_term_is_different

void TestRaft_follower_recv_appendentries_add_new_entries_not_already_in_log(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);

    raft_appendentries_req_t ae;
    raft_appendentries_resp_t aer;

    memset(&ae, 0, sizeof(raft_appendentries_req_t));
    ae.term = 1;
    ae.prev_log_idx = 0;
    ae.prev_log_term = 0;
    /* include entries */
    ae.entries = __MAKE_ENTRY_ARRAY_SEQ_ID(2, 1, 1, "aaa");
    ae.n_entries = 2;
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    CuAssertTrue(tc, 1 == aer.success);
    CuAssertTrue(tc, 2 == raft_get_log_count(r));
}

void TestRaft_follower_recv_appendentries_does_not_add_dupe_entries_already_in_log(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);

    raft_appendentries_req_t ae;
    raft_appendentries_resp_t aer;

    memset(&ae, 0, sizeof(raft_appendentries_req_t));
    ae.term = 1;
    ae.prev_log_idx = 0;
    ae.prev_log_term = 0;
    /* include 1 entry */
    ae.entries = __MAKE_ENTRY_ARRAY(1, 0, "aaa");
    ae.n_entries = 1;
    memset(&aer, 0, sizeof(aer));
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    memset(&aer, 0, sizeof(aer));
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    /* still successful even when no raft_append_entry() happened! */
    CuAssertTrue(tc, 1 == aer.success);
    CuAssertIntEquals(tc, 1, raft_get_log_count(r));

    /* lets get the server to append 2 now! */
    ae.entries = __MAKE_ENTRY_ARRAY_SEQ_ID(2, 1, 1, "aaa");
    ae.n_entries = 2;
    memset(&aer, 0, sizeof(aer));
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertTrue(tc, 1 == aer.success);
    CuAssertIntEquals(tc, 2, raft_get_log_count(r));
}

typedef enum {
    __RAFT_NO_ERR = 0,
    __RAFT_LOG_OFFER_ERR,
    __RAFT_LOG_POP_ERR
} __raft_error_type_e;

typedef struct {
    __raft_error_type_e type;
    raft_index_t idx;
} __raft_error_t;

static int __raft_log_offer_error(
    raft_server_t* raft,
    void *user_data,
    raft_entry_t *entry,
    raft_index_t entry_idx)
{
    __raft_error_t *error = user_data;

    if (__RAFT_LOG_OFFER_ERR == error->type && entry_idx == error->idx)
        return RAFT_ERR_NOMEM;
    return 0;
}

static int __raft_log_pop_error(
    raft_server_t* raft,
    void *user_data,
    raft_entry_t *entry,
    raft_index_t entry_idx)
{
    __raft_error_t *error = user_data;

    if (__RAFT_LOG_POP_ERR == error->type && entry_idx == error->idx)
        return RAFT_ERR_NOMEM;
    return 0;
}

void TestRaft_follower_recv_appendentries_partial_failures(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };
    raft_log_cbs_t log_funcs = {
        .log_offer = __raft_log_offer_error,
        .log_pop = __raft_log_pop_error
    };

    void *r = raft_new();
    __raft_error_t error = {};
    raft_set_callbacks(r, &funcs, &error);
    raft_log_set_callbacks(raft_get_log(r), &log_funcs, r);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);

    /* Append entry 1 and 2 of term 1. */
    __RAFT_APPEND_ENTRY(r, 1, 1, "1aa");
    __RAFT_APPEND_ENTRY(r, 2, 1, "1bb");
    CuAssertIntEquals(tc, 2, raft_get_current_idx(r));

    raft_appendentries_req_t ae;
    raft_appendentries_resp_t aer;

    /* To be received: entry 2 and 3 of term 2. */
    memset(&ae, 0, sizeof(raft_appendentries_req_t));
    ae.term = 2;
    ae.prev_log_idx = 1;
    ae.prev_log_term = 1;
    ae.entries = __MAKE_ENTRY_ARRAY_SEQ_ID(2, 2, 2, "aaa");
    ae.n_entries = 2;

    /* Ask log_pop to fail at entry 2. */
    error.type = __RAFT_LOG_POP_ERR;
    error.idx = 2;
    memset(&aer, 0, sizeof(aer));
    int err = raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertIntEquals(tc, RAFT_ERR_NOMEM, err);
    CuAssertIntEquals(tc, 1, aer.success);
    CuAssertIntEquals(tc, 1, aer.current_idx);
    CuAssertIntEquals(tc, 2, raft_get_current_idx(r));
    raft_entry_t *tmp = raft_get_entry_from_idx(r, 2);
    CuAssertTrue(tc, NULL != tmp);
    CuAssertIntEquals(tc, 1, tmp->term);

    /* Ask log_offer to fail at entry 3. */
    error.type = __RAFT_LOG_OFFER_ERR;
    error.idx = 3;
    memset(&aer, 0, sizeof(aer));
    err = raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertIntEquals(tc, RAFT_ERR_NOMEM, err);
    CuAssertIntEquals(tc, 1, aer.success);
    CuAssertIntEquals(tc, 2, aer.current_idx);
    CuAssertIntEquals(tc, 2, raft_get_current_idx(r));
    tmp = raft_get_entry_from_idx(r, 2);
    CuAssertTrue(tc, NULL != tmp);
    CuAssertIntEquals(tc, 2, tmp->term);

    /* No more errors. */
    memset(&error, 0, sizeof(error));
    memset(&aer, 0, sizeof(aer));
    err = raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertIntEquals(tc, 0, err);
    CuAssertIntEquals(tc, 1, aer.success);
    CuAssertIntEquals(tc, 3, aer.current_idx);
    CuAssertIntEquals(tc, 3, raft_get_current_idx(r));
}

/* If leaderCommit > commitidx, set commitidx =
 *  min(leaderCommit, last log idx) */
void TestRaft_follower_recv_appendentries_set_commitidx_to_prevLogIdx(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_appendentries_req_t ae;
    raft_appendentries_resp_t aer;

    memset(&ae, 0, sizeof(raft_appendentries_req_t));
    ae.term = 1;
    ae.prev_log_idx = 0;
    ae.prev_log_term = 0;
    /* include entries */
    raft_entry_req_t e[4] = {
        { .id = 1, .term = 1 },
        { .id = 2, .term = 1 },
        { .id = 3, .term = 1 },
        { .id = 4, .term = 1 }
    };
    raft_entry_req_t *e_array[4] = {
        &e[0], &e[1], &e[2], &e[3]
    };
    ae.entries = e_array;
    ae.n_entries = 4;
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    /* receive an appendentry with commit */
    memset(&ae, 0, sizeof(raft_appendentries_req_t));
    ae.term = 1;
    ae.prev_log_term = 1;
    ae.prev_log_idx = 4;
    ae.leader_commit = 5;
    /* receipt of appendentries changes commit idx */
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    CuAssertTrue(tc, 1 == aer.success);
    /* set to 4 because commitIDX is lower */
    CuAssertIntEquals(tc, 4, raft_get_commit_idx(r));
}

void TestRaft_follower_recv_appendentries_set_commitidx_to_LeaderCommit(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_appendentries_req_t ae;
    raft_appendentries_resp_t aer;

    memset(&ae, 0, sizeof(raft_appendentries_req_t));
    ae.term = 1;
    ae.prev_log_idx = 0;
    ae.prev_log_term = 0;
    /* include entries */
    raft_entry_req_t e[4] = {
        { .id = 1, .term = 1 },
        { .id = 2, .term = 1 },
        { .id = 3, .term = 1 },
        { .id = 4, .term = 1 }
    };
    raft_entry_req_t *e_array[4] = {
        &e[0], &e[1], &e[2], &e[3]
    };

    ae.entries = e_array;
    ae.n_entries = 4;
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    /* receive an appendentry with commit */
    memset(&ae, 0, sizeof(raft_appendentries_req_t));
    ae.term = 1;
    ae.prev_log_term = 1;
    ae.prev_log_idx = 3;
    ae.leader_commit = 3;
    /* receipt of appendentries changes commit idx */
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    CuAssertTrue(tc, 1 == aer.success);
    /* set to 3 because leaderCommit is lower */
    CuAssertIntEquals(tc, 3, raft_get_commit_idx(r));
}

void TestRaft_follower_recv_appendentries_failure_includes_current_idx(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);

    __RAFT_APPEND_ENTRY(r, 1, 1, "aaa");

    /* receive an appendentry with commit */
    raft_appendentries_req_t ae;
    memset(&ae, 0, sizeof(raft_appendentries_req_t));
    /* lower term means failure */
    ae.term = 0;
    ae.prev_log_term = 0;
    ae.prev_log_idx = 0;
    ae.leader_commit = 0;
    raft_appendentries_resp_t aer;
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    CuAssertTrue(tc, 0 == aer.success);
    CuAssertIntEquals(tc, 1, aer.current_idx);

    /* try again with a higher current_idx */
    memset(&aer, 0, sizeof(aer));
    __RAFT_APPEND_ENTRY(r, 2, 1, "aaa");
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertTrue(tc, 0 == aer.success);
    CuAssertIntEquals(tc, 2, aer.current_idx);
}

void TestRaft_follower_becomes_precandidate_when_election_timeout_occurs(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
        .send_requestvote = __raft_send_requestvote,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    /*  1 second election timeout */
    raft_config(r, 1, RAFT_CONFIG_ELECTION_TIMEOUT, 1000);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /*  max election timeout have passed */
    raft_periodic_internal(r, max_election_timeout(1000) + 1);

    /* is a candidate now */
    CuAssertTrue(tc, 1 == raft_is_precandidate(r));
}

/* Candidate 5.2 */
void TestRaft_follower_dont_grant_vote_if_candidate_has_a_less_complete_log(
    CuTest * tc)
{
    raft_requestvote_req_t rv;
    raft_requestvote_resp_t rvr;

    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /*  request vote */
    /*  vote indicates candidate's log is not complete compared to follower */
    memset(&rv, 0, sizeof(raft_requestvote_req_t));
    rv.term = 1;
    rv.candidate_id = 1;
    rv.last_log_idx = 1;
    rv.last_log_term = 1;

    raft_set_current_term(r, 1);

    /* server's idx are more up-to-date */
    __RAFT_APPEND_ENTRY(r, 100, 1, "aaa");
    __RAFT_APPEND_ENTRY(r, 101, 1, "aaa");

    /* vote not granted */
    raft_recv_requestvote(r, raft_get_node(r, 2), &rv, &rvr);
    CuAssertTrue(tc, 0 == rvr.vote_granted);

    /* approve vote, because last_log_term is higher */
    raft_set_current_term(r, 2);
    memset(&rv, 0, sizeof(raft_requestvote_req_t));
    rv.term = 2;
    rv.candidate_id = 1;
    rv.last_log_idx = 1;
    rv.last_log_term = 3;
    raft_recv_requestvote(r, raft_get_node(r, 2), &rv, &rvr);
    CuAssertIntEquals(tc, 1, rvr.vote_granted);
}

void TestRaft_follower_recv_appendentries_heartbeat_does_not_overwrite_logs(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_appendentries_req_t ae;
    raft_appendentries_resp_t aer;

    memset(&ae, 0, sizeof(raft_appendentries_req_t));
    ae.term = 1;
    ae.prev_log_idx = 0;
    ae.prev_log_term = 0;
    /* include entries */
    ae.entries = __MAKE_ENTRY_ARRAY(1, 1, "aaa");
    ae.n_entries = 1;
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    /* The server sends a follow up AE
     * NOTE: the server has received a response from the last AE so
     * prev_log_idx has been incremented */
    memset(&ae, 0, sizeof(raft_appendentries_req_t));
    ae.term = 1;
    ae.prev_log_idx = 1;
    ae.prev_log_term = 1;
    /* include entries */
    ae.entries = __MAKE_ENTRY_ARRAY_SEQ_ID(4, 2, 1, "aaa");
    ae.n_entries = 4;
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    /* receive a heartbeat
     * NOTE: the leader hasn't received the response to the last AE so it can
     * only assume prev_Log_idx is still 1 */
    memset(&ae, 0, sizeof(raft_appendentries_req_t));
    ae.term = 1;
    ae.prev_log_term = 1;
    ae.prev_log_idx = 1;
    /* receipt of appendentries changes commit idx */
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    CuAssertTrue(tc, 1 == aer.success);
    CuAssertIntEquals(tc, 5, raft_get_current_idx(r));
}

void TestRaft_follower_recv_appendentries_does_not_deleted_commited_entries(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_appendentries_req_t ae;
    raft_appendentries_resp_t aer;

    memset(&ae, 0, sizeof(raft_appendentries_req_t));
    ae.term = 1;
    ae.prev_log_idx = 0;
    ae.prev_log_term = 0;
    /* include entries */
    ae.entries = __MAKE_ENTRY_ARRAY(1, 1, "aaa");
    ae.n_entries = 1;
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    /* Follow up AE. Node responded with success */
    memset(&ae, 0, sizeof(raft_appendentries_req_t));
    ae.term = 1;
    ae.prev_log_idx = 1;
    ae.prev_log_term = 1;
    /* include entries */
    ae.entries = __MAKE_ENTRY_ARRAY_SEQ_ID(4, 2, 1,  "aaa");
    ae.n_entries = 4;
    ae.leader_commit = 4;
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    /* The server sends a follow up AE */
    memset(&ae, 0, sizeof(raft_appendentries_req_t));
    ae.term = 1;
    ae.prev_log_idx = 1;
    ae.prev_log_term = 1;
    /* include entries */
    ae.entries = __MAKE_ENTRY_ARRAY_SEQ_ID(5, 2, 1, "aaa");
    ae.n_entries = 5;
    ae.leader_commit = 4;
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    CuAssertTrue(tc, 1 == aer.success);
    CuAssertIntEquals(tc, 6, raft_get_current_idx(r));
    CuAssertIntEquals(tc, 4, raft_get_commit_idx(r));

    /* The server sends a follow up AE.
     * This appendentry forces the node to check if it's going to delete
     * commited logs */
    memset(&ae, 0, sizeof(raft_appendentries_req_t));
    ae.term = 1;
    ae.prev_log_idx = 3;
    ae.prev_log_term = 1;
    /* include entries */
    ae.entries = __MAKE_ENTRY_ARRAY_SEQ_ID(3, 5, 1, "aaa");
    ae.n_entries = 3;
    ae.leader_commit = 4;
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    CuAssertTrue(tc, 1 == aer.success);
    CuAssertIntEquals(tc, 6, raft_get_current_idx(r));
}

void TestRaft_candidate_becomes_candidate_is_candidate(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);
    raft_add_node(r, NULL, 1, 1);
    raft_become_candidate(r);
    CuAssertTrue(tc, raft_is_candidate(r));
}

/* Candidate 5.2 */
void TestRaft_follower_becoming_candidate_increments_current_term(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_set_callbacks(r, &funcs, NULL);

    CuAssertTrue(tc, 0 == raft_get_current_term(r));
    raft_become_candidate(r);
    CuAssertTrue(tc, 1 == raft_get_current_term(r));
}

/* Candidate 5.2 */
void TestRaft_follower_becoming_candidate_votes_for_self(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    CuAssertTrue(tc, -1 == raft_get_voted_for(r));
    raft_become_candidate(r);
    CuAssertTrue(tc, raft_get_nodeid(r) == raft_get_voted_for(r));
    CuAssertTrue(tc, 1 == raft_get_nvotes_for_me(r));
}

/* Candidate 5.2 */
void TestRaft_follower_becoming_candidate_resets_election_timeout(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_callbacks(r, &funcs, NULL);

    raft_config(r, 1, RAFT_CONFIG_ELECTION_TIMEOUT, 1000);
    CuAssertTrue(tc, 0 == raft_get_timeout_elapsed(r));

    raft_periodic_internal(r, 900);
    CuAssertTrue(tc, 900 == raft_get_timeout_elapsed(r));

    raft_become_candidate(r);
    /* time is selected randomly */
    CuAssertTrue(tc, raft_get_timeout_elapsed(r) < 1000);
}

void TestRaft_follower_recv_appendentries_resets_election_timeout(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_config(r, 1, RAFT_CONFIG_ELECTION_TIMEOUT, 1000);
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 1);

    raft_periodic_internal(r, 900);

    raft_appendentries_req_t ae;
    raft_appendentries_resp_t aer;

    memset(&ae, 0, sizeof(raft_appendentries_req_t));
    ae.term = 1;
    raft_recv_appendentries(r, raft_get_node(r, 1), &ae, &aer);
    CuAssertTrue(tc, 0 == raft_get_timeout_elapsed(r));
}

/* Candidate 5.2 */
void TestRaft_follower_becoming_candidate_requests_votes_from_other_servers(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
        .send_requestvote = sender_requestvote,
    };
    raft_requestvote_req_t * rv;

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_set_callbacks(r, &funcs, sender);
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);

    /* set term so we can check it gets included in the outbound message */
    raft_set_current_term(r, 2);

    /* becoming candidate triggers vote requests */
    raft_become_candidate(r);

    /* 2 nodes = 2 vote requests */
    rv = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != rv);
    CuAssertTrue(tc, 2 != rv->term);
    CuAssertTrue(tc, 3 == rv->term);
    /*  TODO: there should be more items */
    rv = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != rv);
    CuAssertTrue(tc, 3 == rv->term);
}

/* Candidate 5.2 */
void TestRaft_candidate_election_timeout_and_no_leader_results_in_new_election(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
        .send_requestvote = __raft_send_requestvote,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_config(r, 1, RAFT_CONFIG_ELECTION_TIMEOUT, 1000);

    /* server wants to be leader, so becomes candidate */
    raft_become_candidate(r);
    CuAssertTrue(tc, 1 == raft_get_current_term(r));

    /* clock over (ie. max election timeout + 1), does not increment term */
    raft_periodic_internal(r, max_election_timeout(1000) + 1);
    CuAssertTrue(tc, 1 == raft_get_current_term(r));
    CuAssertIntEquals(tc, RAFT_STATE_PRECANDIDATE, raft_get_state(r));

    raft_requestvote_resp_t vr0 = {
        .prevote = 1,
        .request_term = raft_get_current_term(r) + 1,
        .term = 0,
        .vote_granted = 1
    };
    /*  receiving this vote gives the server majority */
    raft_recv_requestvote_response(r, raft_get_node(r, 2), &vr0);
    CuAssertIntEquals(tc, RAFT_STATE_CANDIDATE, raft_get_state(r));

    raft_requestvote_resp_t vr1 = {
        .request_term = 2,
        .term = 2,
        .vote_granted = 1
    };

    /*  receiving this vote gives the server majority */
    raft_recv_requestvote_response(r, raft_get_node(r, 2), &vr1);
    CuAssertIntEquals(tc, RAFT_STATE_LEADER, raft_get_state(r));
}

/* Candidate 5.2 */
void TestRaft_candidate_receives_majority_of_votes_becomes_leader(CuTest * tc)
{
    raft_requestvote_resp_t vr;

    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
        .send_requestvote = __raft_send_requestvote,
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);
    raft_add_node(r, NULL, 4, 0);
    raft_add_node(r, NULL, 5, 0);
    CuAssertTrue(tc, 5 == raft_get_num_nodes(r));

    /* vote for self */
    raft_become_candidate(r);
    CuAssertTrue(tc, 1 == raft_get_current_term(r));
    CuAssertTrue(tc, 1 == raft_get_nvotes_for_me(r));

    /* a vote for us */
    memset(&vr, 0, sizeof(raft_requestvote_resp_t));
    vr.request_term = 1;
    vr.term = 1;
    vr.vote_granted = 1;
    /* get one vote */
    raft_recv_requestvote_response(r, raft_get_node(r, 2), &vr);
    CuAssertTrue(tc, 2 == raft_get_nvotes_for_me(r));
    CuAssertTrue(tc, 0 == raft_is_leader(r));

    /* get another vote
     * now has majority (ie. 3/5 votes) */
    raft_recv_requestvote_response(r, raft_get_node(r, 3), &vr);
    CuAssertTrue(tc, 1 == raft_is_leader(r));
}

/* Candidate 5.2 */
void TestRaft_candidate_will_not_respond_to_voterequest_if_it_has_already_voted(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_requestvote_req_t rv;
    raft_requestvote_resp_t rvr;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_vote(r, raft_get_node(r, 1));

    memset(&rv, 0, sizeof(raft_requestvote_req_t));
    raft_recv_requestvote(r, raft_get_node(r, 2), &rv, &rvr);

    /* we've vote already, so won't respond with a vote granted... */
    CuAssertTrue(tc, 0 == rvr.vote_granted);
}

/* Candidate 5.2 */
void TestRaft_candidate_requestvote_includes_logidx(CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_requestvote = sender_requestvote,
        .log              = NULL,
        .persist_metadata = __raft_persist_metadata,
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_state(r, RAFT_STATE_CANDIDATE);

    raft_set_callbacks(r, &funcs, sender);
    raft_set_current_term(r, 5);
    /* 3 entries */
    __RAFT_APPEND_ENTRY(r, 100, 1, "aaa");
    __RAFT_APPEND_ENTRY(r, 101, 1, "aaa");
    __RAFT_APPEND_ENTRY(r, 102, 3, "aaa");
    raft_send_requestvote(r, raft_get_node(r, 2));

    raft_requestvote_req_t * rv = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != rv);
    CuAssertIntEquals(tc, 3, rv->last_log_idx);
    CuAssertIntEquals(tc, 5, rv->term);
    CuAssertIntEquals(tc, 3, rv->last_log_term);
    CuAssertIntEquals(tc, 1, rv->candidate_id);
}

void TestRaft_candidate_recv_requestvote_response_becomes_follower_if_current_term_is_less_than_term(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_current_term(r, 1);
    raft_set_state(r, RAFT_STATE_CANDIDATE);
    raft_vote(r, 0);
    CuAssertTrue(tc, 0 == raft_is_follower(r));
    CuAssertTrue(tc, -1 == raft_get_leader_id(r));
    CuAssertTrue(tc, 1 == raft_get_current_term(r));

    raft_requestvote_resp_t rvr;
    memset(&rvr, 0, sizeof(raft_requestvote_resp_t));
    rvr.request_term = 2;
    rvr.term = 2;
    rvr.vote_granted = 0;
    raft_recv_requestvote_response(r, raft_get_node(r, 2), &rvr);
    CuAssertTrue(tc, 1 == raft_is_follower(r));
    CuAssertTrue(tc, 2 == raft_get_current_term(r));
    CuAssertTrue(tc, -1 == raft_get_voted_for(r));
}

/* Candidate 5.2 */
void TestRaft_candidate_recv_appendentries_frm_leader_results_in_follower(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_state(r, RAFT_STATE_CANDIDATE);
    raft_vote(r, 0);
    CuAssertTrue(tc, 0 == raft_is_follower(r));
    CuAssertTrue(tc, -1 == raft_get_leader_id(r));
    CuAssertTrue(tc, 0 == raft_get_current_term(r));

    /* receive recent appendentries */
    raft_appendentries_req_t ae;
    raft_appendentries_resp_t aer;

    memset(&ae, 0, sizeof(raft_appendentries_req_t));
    ae.term = 1;
    ae.leader_id = 2;
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertTrue(tc, 1 == raft_is_follower(r));
    /* after accepting a leader, it's available as the last known leader */
    CuAssertTrue(tc, 2 == raft_get_leader_id(r));
    CuAssertTrue(tc, 1 == raft_get_current_term(r));
    CuAssertTrue(tc, -1 == raft_get_voted_for(r));
}

/* Candidate 5.2 */
void TestRaft_candidate_recv_appendentries_from_same_term_results_in_step_down(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
        .send_requestvote = __raft_send_requestvote,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_appendentries_req_t ae;
    raft_appendentries_resp_t aer;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_current_term(r, 1);
    raft_become_candidate(r);
    CuAssertTrue(tc, 0 == raft_is_follower(r));
    CuAssertIntEquals(tc, 1, raft_get_voted_for(r));

    memset(&ae, 0, sizeof(raft_appendentries_req_t));
    ae.term = 2;
    ae.prev_log_idx = 1;
    ae.prev_log_term = 1;

    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertTrue(tc, 0 == raft_is_candidate(r));

    /* The election algorithm requires that votedFor always contains the node
     * voted for in the current term (if any), which is why it is persisted.
     * By resetting that to -1 we have the following problem:
     *
     *  Node self, other1 and other2 becomes candidates
     *  Node other1 wins election
     *  Node self gets appendentries
     *  Node self resets votedFor
     *  Node self gets requestvote from other2
     *  Node self votes for Other2
    */
    CuAssertIntEquals(tc, 1, raft_get_voted_for(r));
}

void TestRaft_candidate_recv_appendentries_from_higher_term_results_in_step_down(
        CuTest *tc)
{
    raft_appendentries_resp_t append_resp;
    void *r = raft_new();

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_current_term(r, 2);

    raft_appendentries_req_t append_req1 = {
            .term = 3,
            .leader_id = 2
    };
    raft_set_state(r, RAFT_STATE_CANDIDATE);
    raft_recv_appendentries(r, raft_get_node(r, 2), &append_req1, &append_resp);
    CuAssertIntEquals(tc, 1, raft_is_follower(r));
    CuAssertIntEquals(tc, 3, raft_get_current_term(r));

    raft_appendentries_req_t append_req2 = {
            .term = 4,
            .leader_id = 2
    };
    raft_set_state(r, RAFT_STATE_PRECANDIDATE);
    raft_recv_appendentries(r, raft_get_node(r, 2), &append_req2, &append_resp);
    CuAssertIntEquals(tc, 1, raft_is_follower(r));
    CuAssertIntEquals(tc, 4, raft_get_current_term(r));
}

void TestRaft_candidate_recv_snapshot_from_higher_term_results_in_step_down(
        CuTest *tc)
{
    raft_snapshot_resp_t snapshot_resp;
    void *r = raft_new();

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_current_term(r, 2);

    /* Snapshot req with higher term results in step down */
    raft_snapshot_req_t snapshot_req1 = {
            .term = 5,
            .leader_id = 2
    };
    raft_set_state(r, RAFT_STATE_CANDIDATE);
    raft_recv_snapshot(r, raft_get_node(r, 2), &snapshot_req1, &snapshot_resp);
    CuAssertIntEquals(tc, 1, raft_is_follower(r));
    CuAssertIntEquals(tc, 5, raft_get_current_term(r));

    raft_snapshot_req_t snapshot_req2 = {
            .term = 6,
            .leader_id = 2
    };
    raft_set_state(r, RAFT_STATE_PRECANDIDATE);
    raft_recv_snapshot(r, raft_get_node(r, 2), &snapshot_req2, &snapshot_resp);
    CuAssertIntEquals(tc, 1, raft_is_follower(r));
    CuAssertIntEquals(tc, 6, raft_get_current_term(r));
}

void TestRaft_leader_becomes_leader_is_leader(CuTest * tc)
{
    void *r = raft_new();
    raft_node_t *n = raft_add_node(r, NULL, 1, 1);
    raft_become_leader(r);

    CuAssertTrue(tc, raft_is_leader(r));
    CuAssertTrue(tc, raft_get_leader_node(r) == n);
}

void TestRaft_leader_becomes_leader_does_not_clear_voted_for(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_vote(r, raft_get_node(r, 1));
    CuAssertTrue(tc, 1 == raft_get_voted_for(r));
    raft_become_leader(r);
    CuAssertTrue(tc, 1 == raft_get_voted_for(r));
}

void TestRaft_leader_when_becomes_leader_all_nodes_have_nextidx_equal_to_lastlog_idx_plus_1(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);

    /* candidate to leader */
    raft_set_state(r, RAFT_STATE_CANDIDATE);
    raft_become_leader(r);

    int i;
    for (i = 2; i <= 3; i++)
    {
        raft_node_t* p = raft_get_node(r, i);
        CuAssertTrue(tc, raft_get_current_idx(r) + 1 ==
                     raft_node_get_next_idx(p));
    }
}

/* 5.2 */
void TestRaft_leader_when_it_becomes_a_leader_sends_empty_appendentries(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = sender_appendentries,
        .log                = NULL
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_set_callbacks(r, &funcs, sender);
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);

    /* candidate to leader */
    raft_set_state(r, RAFT_STATE_CANDIDATE);
    raft_become_leader(r);

    /* receive appendentries messages for both nodes */
    raft_appendentries_req_t * ae = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != ae);
    ae = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != ae);
}

/* 5.2
 * Note: commit means it's been appended to the log, not applied to the FSM */
void TestRaft_leader_responds_to_entry_msg_when_entry_is_committed(CuTest * tc)
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
    CuAssertTrue(tc, 0 == raft_get_log_count(r));

    /* entry message */
    raft_entry_req_t *ety = __MAKE_ENTRY(1, 1, "entry");

    /* receive entry */
    raft_recv_entry(r, ety, &cr);
    CuAssertTrue(tc, 1 == raft_get_log_count(r));

    /* trigger response through commit */
    raft_apply_entry(r);
}

void TestRaft_non_leader_recv_entry_msg_fails(CuTest * tc)
{
    raft_entry_resp_t cr;

    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_state(r, RAFT_STATE_FOLLOWER);

    /* entry message */
    raft_entry_t *ety = __MAKE_ENTRY(1, 1, "entry");

    /* receive entry */
    int e = raft_recv_entry(r, ety, &cr);
    CuAssertTrue(tc, RAFT_ERR_NOT_LEADER == e);

    /* receive read request */
    e = raft_recv_read_request(r, NULL, NULL);
    CuAssertTrue(tc, RAFT_ERR_NOT_LEADER == e);
}

/* 5.3 */
void TestRaft_leader_sends_appendentries_with_NextIdx_when_PrevIdx_gt_NextIdx(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = sender_appendentries,
        .log                = NULL
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_set_callbacks(r, &funcs, sender);
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* i'm leader */
    raft_set_state(r, RAFT_STATE_LEADER);

    raft_node_t* p = raft_get_node(r, 2);
    raft_node_set_next_idx(p, 4);

    /* receive appendentries messages */
    raft_send_appendentries(r, p);
    raft_appendentries_req_t * ae = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != ae);
}

void TestRaft_leader_sends_appendentries_with_leader_commit(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = sender_appendentries,
        .log                = NULL
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_set_callbacks(r, &funcs, sender);
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* i'm leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);

    int i;

    for (i=0; i<10; i++)
    {
        __RAFT_APPEND_ENTRY(r, 1, 1, "aaa");
    }

    raft_set_commit_idx(r, 10);

    /* receive appendentries messages */
    raft_send_appendentries(r, raft_get_node(r, 2));
    raft_appendentries_req_t *  ae = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != ae);
    CuAssertTrue(tc, ae->leader_commit == 10);
}

void TestRaft_leader_sends_appendentries_with_prevLogIdx(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = sender_appendentries,
        .log                = NULL
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_set_callbacks(r, &funcs, sender);
    raft_add_node(r, NULL, 1, 1); /* me */
    raft_add_node(r, NULL, 2, 0);

    /* i'm leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 2);

    /* receive appendentries messages */
    raft_send_appendentries(r, raft_get_node(r, 2));
    raft_appendentries_req_t *  ae = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != ae);
    CuAssertIntEquals(tc, 0, ae->prev_log_idx);

    raft_node_t* n = raft_get_node(r, 2);

    /* add 1 entry */
    /* receive appendentries messages */
    __RAFT_APPEND_ENTRY(r, 100, 2, "aaa");
    raft_node_set_next_idx(n, 1);
    raft_send_appendentries(r, raft_get_node(r, 2));
    ae = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != ae);
    CuAssertIntEquals(tc, 0, ae->prev_log_idx);
    CuAssertIntEquals(tc, 1, ae->n_entries);
    CuAssertIntEquals(tc, 100, ae->entries[0]->id);
    CuAssertIntEquals(tc, 2, ae->entries[0]->term);

    /* set next_idx */
    /* receive appendentries messages */
    raft_node_set_next_idx(n, 2);
    raft_send_appendentries(r, raft_get_node(r, 2));
    ae = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != ae);
    CuAssertTrue(tc, ae->prev_log_idx == 1);
}

void TestRaft_leader_sends_appendentries_when_node_has_next_idx_of_0(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = sender_appendentries,
        .log                = NULL
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_set_callbacks(r, &funcs, sender);
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* i'm leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 2);

    /* receive appendentries messages */
    raft_send_appendentries(r, raft_get_node(r, 2));
    raft_appendentries_req_t *  ae = sender_poll_msg_data(sender);

    /* add an entry */
    /* receive appendentries messages */
    raft_node_t* n = raft_get_node(r, 2);
    raft_node_set_next_idx(n, 1);
    __RAFT_APPEND_ENTRY(r, 100, 1, "aaa");
    raft_send_appendentries(r, n);
    ae = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != ae);
    CuAssertTrue(tc, ae->prev_log_idx == 0);
}

void TestRaft_leader_updates_next_idx_on_send_ae(CuTest *tc)
{
    raft_cbs_t funcs = {
            .send_appendentries = sender_appendentries,
            .log                = NULL
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_set_callbacks(r, &funcs, sender);
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* i'm leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);

    /* receive appendentries messages */
    raft_node_t* n = raft_get_node(r, 2);
    raft_send_appendentries(r, n);
    raft_appendentries_req_t *  ae = sender_poll_msg_data(sender);

    raft_node_set_next_idx(n, 1);
    __RAFT_APPEND_ENTRY(r, 100, 1, "aaa");
    raft_send_appendentries(r, n);
    ae = sender_poll_msg_data(sender);
    CuAssertPtrNotNull(tc, ae);
    CuAssertIntEquals(tc, 0,ae->prev_log_idx);
    CuAssertIntEquals(tc, 1, ae->n_entries);
    CuAssertIntEquals(tc, 2, raft_node_get_next_idx(n));

    raft_node_set_next_idx(n, 1);
    __RAFT_APPEND_ENTRY(r, 101, 1, "bbb");
    raft_send_appendentries(r, n);
    ae = sender_poll_msg_data(sender);
    CuAssertPtrNotNull(tc, ae);
    CuAssertIntEquals(tc, 0,ae->prev_log_idx);
    CuAssertIntEquals(tc, 2, ae->n_entries);
    CuAssertIntEquals(tc, 3, raft_node_get_next_idx(n));
}

/* 5.3 */
void TestRaft_leader_retries_appendentries_with_decremented_NextIdx_log_inconsistency(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = sender_appendentries,
        .log                = NULL
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_set_callbacks(r, &funcs, sender);
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* i'm leader */
    raft_set_state(r, RAFT_STATE_LEADER);

    /* receive appendentries messages */
    raft_send_appendentries(r, raft_get_node(r, 2));
    raft_appendentries_req_t * ae = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != ae);
}

/*
 * If there exists an N such that N > commitidx, a majority
 * of matchidx[i] = N, and log[N].term == currentTerm:
 * set commitidx = N (§5.2, §5.4).  */
void TestRaft_leader_append_entry_to_log_increases_idxno(CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_entry_req_t *ety = __MAKE_ENTRY(1, 1, "entry");

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_state(r, RAFT_STATE_LEADER);
    CuAssertTrue(tc, 0 == raft_get_log_count(r));

    raft_recv_entry(r, ety, NULL);
    CuAssertTrue(tc, 1 == raft_get_log_count(r));
}

#if 0
// TODO no support for duplicates
void T_estRaft_leader_doesnt_append_entry_if_unique_id_is_duplicate(CuTest * tc)
{
    void *r;

    /* 2 nodes */
    raft_node_configuration_t cfg[] = {
        { (void*)1 },
        { (void*)2 },
        { NULL     }
    };

    raft_entry_req_t ety;
    ety.id = 1;
    ety.data = "entry";
    ety.data.len = strlen("entry");

    r = raft_new();
    raft_set_configuration(r, cfg, 0);

    raft_set_state(r, RAFT_STATE_LEADER);
    CuAssertTrue(tc, 0 == raft_get_log_count(r));

    raft_recv_entry(r, 1, &ety);
    CuAssertTrue(tc, 1 == raft_get_log_count(r));

    raft_recv_entry(r, 1, &ety);
    CuAssertTrue(tc, 1 == raft_get_log_count(r));
}
#endif

void TestRaft_leader_recv_appendentries_response_increase_commit_idx_when_majority_have_entry_and_atleast_one_newer_entry(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .applylog = __raft_applylog,
        .persist_metadata = __raft_persist_metadata,
        .send_appendentries          = sender_appendentries,
        .log                         = NULL
    };
    raft_appendentries_resp_t aer;

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);
    raft_add_node(r, NULL, 4, 0);
    raft_add_node(r, NULL, 5, 0);
    raft_set_callbacks(r, &funcs, sender);

    /* I'm the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);
    raft_set_commit_idx(r, 0);
    /* the last applied idx will became 1, and then 2 */
    raft_set_last_applied_idx(r, 0);

    /* append entries - we need two */
    __RAFT_APPEND_ENTRY(r, 1, 1, "aaa");
    __RAFT_APPEND_ENTRY(r, 2, 1, "bbb");

    memset(&aer, 0, sizeof(raft_appendentries_resp_t));

    /* FIRST entry log application */
    /* send appendentries -
     * server will be waiting for response */
    raft_send_appendentries(r, raft_get_node(r, 2));
    raft_send_appendentries(r, raft_get_node(r, 3));
    /* receive mock success responses */
    aer.term = 1;
    aer.success = 1;
    aer.current_idx = 1;
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertIntEquals(tc, 0, raft_get_commit_idx(r));
    raft_recv_appendentries_response(r, raft_get_node(r, 3), &aer);
    /* leader will now have majority followers who have appended this log */
    CuAssertIntEquals(tc, 1, raft_get_commit_idx(r));
    raft_periodic_internal(r, 1);
    CuAssertIntEquals(tc, 1, raft_get_last_applied_idx(r));

    /* SECOND entry log application */
    /* send appendentries -
     * server will be waiting for response */
    raft_send_appendentries(r, raft_get_node(r, 2));
    raft_send_appendentries(r, raft_get_node(r, 3));
    /* receive mock success responses */
    aer.term = 1;
    aer.success = 1;
    aer.current_idx = 2;
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertIntEquals(tc, 1, raft_get_commit_idx(r));
    raft_recv_appendentries_response(r, raft_get_node(r, 3), &aer);
    /* leader will now have majority followers who have appended this log */
    CuAssertIntEquals(tc, 2, raft_get_commit_idx(r));
    raft_periodic_internal(r, 1);
    CuAssertIntEquals(tc, 2, raft_get_last_applied_idx(r));
}

void TestRaft_leader_recv_appendentries_response_set_has_sufficient_logs_for_node(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .applylog = __raft_applylog,
        .persist_metadata = __raft_persist_metadata,
        .node_has_sufficient_logs = __raft_node_has_sufficient_logs,
        .log                         = NULL
    };
    raft_appendentries_resp_t aer;

    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);
    raft_add_node(r, NULL, 4, 0);
    raft_node_t* node = raft_add_node(r, NULL, 5, 0);

    int has_sufficient_logs_flag = 0;
    raft_set_callbacks(r, &funcs, &has_sufficient_logs_flag);

    /* I'm the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);
    raft_set_commit_idx(r, 0);
    /* the last applied idx will became 1, and then 2 */
    raft_set_last_applied_idx(r, 0);

    /* append entries - we need two */
    __RAFT_APPEND_ENTRY(r, 1, 1, "aaa");
    __RAFT_APPEND_ENTRY(r, 2, 1, "bbb");

    memset(&aer, 0, sizeof(raft_appendentries_resp_t));

    raft_send_appendentries(r, raft_get_node(r, 5));
    aer.term = 1;
    aer.success = 1;
    aer.current_idx = 2;

    raft_node_set_voting(node, 0);
    raft_node_set_addition_committed(node, 1);
    raft_recv_appendentries_response(r, node, &aer);
    CuAssertIntEquals(tc, 1, has_sufficient_logs_flag);

    raft_recv_appendentries_response(r, node, &aer);
    CuAssertIntEquals(tc, 1, has_sufficient_logs_flag);
}

void TestRaft_leader_recv_appendentries_response_fail_set_has_sufficient_logs_for_node_if_not_addition_committed(
        CuTest * tc)
{
    raft_cbs_t funcs = {
            .applylog = __raft_applylog,
            .persist_metadata = __raft_persist_metadata,
            .node_has_sufficient_logs = __raft_node_has_sufficient_logs,
            .log                         = NULL
    };
    raft_appendentries_resp_t aer;

    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);
    raft_add_node(r, NULL, 4, 0);
    raft_node_t* node = raft_add_node(r, NULL, 5, 0);

    int has_sufficient_logs_flag = 0;
    raft_set_callbacks(r, &funcs, &has_sufficient_logs_flag);

    /* I'm the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 4);
    raft_set_commit_idx(r, 0);
    /* the last applied idx will became 1, and then 2 */
    raft_set_last_applied_idx(r, 0);

    /* append entries - we need two */
    __RAFT_APPEND_ENTRIES_SEQ_ID(r, 3, 1, 1, "aaaa");

    memset(&aer, 0, sizeof(raft_appendentries_resp_t));

    raft_send_appendentries(r, raft_get_node(r, 5));
    aer.term = 1;
    aer.success = 1;
    aer.current_idx = 2;

    raft_node_set_voting(node, 0);
    raft_recv_appendentries_response(r, node, &aer);
    CuAssertIntEquals(tc, 0, has_sufficient_logs_flag);
}

void TestRaft_leader_recv_appendentries_response_increase_commit_idx_using_voting_nodes_majority(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .applylog = __raft_applylog,
        .persist_metadata = __raft_persist_metadata,
        .send_appendentries          = sender_appendentries,
        .log                         = NULL
    };
    raft_appendentries_resp_t aer;

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);
    raft_add_non_voting_node(r, NULL, 4, 0);
    raft_add_non_voting_node(r, NULL, 5, 0);
    raft_set_callbacks(r, &funcs, sender);

    /* I'm the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);
    raft_set_commit_idx(r, 0);
    /* the last applied idx will became 1, and then 2 */
    raft_set_last_applied_idx(r, 0);

    /* append entries - we need two */
    __RAFT_APPEND_ENTRY(r, 1, 1, "aaaa");

    memset(&aer, 0, sizeof(raft_appendentries_resp_t));

    /* FIRST entry log application */
    /* send appendentries -
     * server will be waiting for response */
    raft_send_appendentries(r, raft_get_node(r, 2));
    /* receive mock success responses */
    aer.term = 1;
    aer.success = 1;
    aer.current_idx = 1;
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertIntEquals(tc, 1, raft_get_commit_idx(r));
    /* leader will now have majority followers who have appended this log */
    raft_periodic_internal(r, 1);
    CuAssertIntEquals(tc, 1, raft_get_last_applied_idx(r));
}

void TestRaft_leader_recv_appendentries_response_duplicate_does_not_decrement_match_idx(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
        .send_appendentries          = sender_appendentries,
        .log                         = NULL
    };
    raft_appendentries_resp_t aer;

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);
    raft_set_callbacks(r, &funcs, sender);

    /* I'm the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);
    raft_set_commit_idx(r, 0);
    /* the last applied idx will became 1, and then 2 */
    raft_set_last_applied_idx(r, 0);

    /* append entries - we need two */
    __RAFT_APPEND_ENTRY(r, 1, 1, "aaa");
    __RAFT_APPEND_ENTRY(r, 2, 1, "bbb");

    memset(&aer, 0, sizeof(raft_appendentries_resp_t));

    /* receive msg 1 */
    aer.term = 1;
    aer.success = 1;
    aer.current_idx = 1;
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertIntEquals(tc, 1, raft_node_get_match_idx(raft_get_node(r, 2)));

    /* receive msg 2 */
    aer.term = 1;
    aer.success = 1;
    aer.current_idx = 2;
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertIntEquals(tc, 2, raft_node_get_match_idx(raft_get_node(r, 2)));

    /* receive msg 1 - because of duplication ie. unreliable network */
    aer.term = 1;
    aer.success = 1;
    aer.current_idx = 1;
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertIntEquals(tc, 2, raft_node_get_match_idx(raft_get_node(r, 2)));
}

void TestRaft_leader_recv_appendentries_response_do_not_increase_commit_idx_because_of_old_terms_with_majority(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .applylog = __raft_applylog,
        .persist_metadata = __raft_persist_metadata,
        .send_appendentries          = sender_appendentries,
        .log                         = NULL
    };
    raft_appendentries_resp_t aer;

    void *sender = sender_new(NULL);
    void *r = raft_new();

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);
    raft_add_node(r, NULL, 4, 0);
    raft_add_node(r, NULL, 5, 0);
    raft_set_callbacks(r, &funcs, sender);

    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 2);
    raft_set_commit_idx(r, 0);
    raft_set_last_applied_idx(r, 0);

    /* append entries - we need two */
    __RAFT_APPEND_ENTRY(r, 1, 1, "aaa");
    __RAFT_APPEND_ENTRY(r, 2, 1, "bbb");
    __RAFT_APPEND_ENTRY(r, 3, 2, "aaaa");

    memset(&aer, 0, sizeof(raft_appendentries_resp_t));

    /* FIRST entry log application */
    /* send appendentries -
     * server will be waiting for response */
    raft_send_appendentries(r, raft_get_node(r, 2));
    raft_send_appendentries(r, raft_get_node(r, 3));
    /* receive mock success responses */
    aer.term = 2;
    aer.success = 1;
    aer.current_idx = 1;
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertIntEquals(tc, 0, raft_get_commit_idx(r));
    raft_recv_appendentries_response(r, raft_get_node(r, 3), &aer);
    CuAssertIntEquals(tc, 0, raft_get_commit_idx(r));
    raft_periodic_internal(r, 1);
    CuAssertIntEquals(tc, 0, raft_get_last_applied_idx(r));

    /* SECOND entry log application */
    /* send appendentries -
     * server will be waiting for response */
    raft_send_appendentries(r, raft_get_node(r, 2));
    raft_send_appendentries(r, raft_get_node(r, 3));
    /* receive mock success responses */
    aer.term = 2;
    aer.success = 1;
    aer.current_idx = 2;
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertIntEquals(tc, 0, raft_get_commit_idx(r));
    raft_recv_appendentries_response(r, raft_get_node(r, 3), &aer);
    CuAssertIntEquals(tc, 0, raft_get_commit_idx(r));
    raft_periodic_internal(r, 1);
    CuAssertIntEquals(tc, 0, raft_get_last_applied_idx(r));

    /* THIRD entry log application */
    raft_send_appendentries(r, raft_get_node(r, 2));
    raft_send_appendentries(r, raft_get_node(r, 3));
    /* receive mock success responses
     * let's say that the nodes have majority within leader's current term */
    aer.term = 2;
    aer.success = 1;
    aer.current_idx = 3;
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertIntEquals(tc, 0, raft_get_commit_idx(r));
    raft_recv_appendentries_response(r, raft_get_node(r, 3), &aer);
    CuAssertIntEquals(tc, 3, raft_get_commit_idx(r));
    raft_periodic_internal(r, 1);
    CuAssertIntEquals(tc, 3, raft_get_last_applied_idx(r));
}

void TestRaft_leader_recv_appendentries_response_jumps_to_lower_next_idx(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
        .send_appendentries          = sender_appendentries,
        .log                         = NULL
    };
    raft_appendentries_resp_t aer;

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_callbacks(r, &funcs, sender);

    raft_set_current_term(r, 4);
    raft_set_commit_idx(r, 0);

    /* append entries */
    __RAFT_APPEND_ENTRIES_SEQ_ID_TERM(r, 4, 1, 1, "aaaa");

    raft_appendentries_req_t * ae;

    /* become leader sets next_idx to current_idx
     * it also appends a NO_OP, and as raft_send_appendentries()
     * updates next_idx, it's now one greater
     */
    raft_become_leader(r);
    raft_node_t* node = raft_get_node(r, 2);
    CuAssertIntEquals(tc, 6, raft_node_get_next_idx(node));
    CuAssertTrue(tc, NULL != (ae = sender_poll_msg_data(sender)));
    // this ae contains the leader no op only, hence prev is the idx 4, term 4
    CuAssertIntEquals(tc, 4, ae->prev_log_term);
    CuAssertIntEquals(tc, 4, ae->prev_log_idx);

    /* receive mock success responses */
    memset(&aer, 0, sizeof(raft_appendentries_resp_t));
    aer.term = 4;
    aer.success = 0;
    aer.current_idx = 1;
    CuAssertIntEquals(tc, 0, raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer));
    /* we failed, but raft_recv_appendentries_response() will reset next_idx and resend everything, setting next_idx to current_idx + 1 */
    CuAssertIntEquals(tc, 6, raft_node_get_next_idx(node));
    /* see if new appendentries have appropriate values */
    CuAssertPtrNotNull(tc, (ae = sender_poll_msg_data(sender)));
    /* we replied with a failure above, and told it our current_idx is 1, so thats what new one has */
    CuAssertIntEquals(tc, 1, ae->prev_log_term);
    CuAssertIntEquals(tc, 1, ae->prev_log_idx);
    /* since current_idx is 1, and we are up 5, if it reset next_idx correctly, we should have 4 entries */
    CuAssertIntEquals(tc, 4, ae->n_entries);

    /* receive mock success responses */
    memset(&aer, 0, sizeof(raft_appendentries_resp_t));
    aer.term = 4;
    aer.success = 1;
    aer.current_idx = 5;
    CuAssertIntEquals(tc, 0, raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer));
    // it's 6 and not 2 because the raft_send_appendentries() that is sent at end of recv_response send the rest
    CuAssertIntEquals(tc, 6, raft_node_get_next_idx(node));
}

void TestRaft_leader_recv_appendentries_response_retry_only_if_leader(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
        .send_appendentries          = sender_appendentries,
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);
    raft_set_callbacks(r, &funcs, sender);

    /* I'm the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);
    raft_set_commit_idx(r, 0);
    /* the last applied idx will became 1, and then 2 */
    raft_set_last_applied_idx(r, 0);

    /* append entries - we need two */
    __RAFT_APPEND_ENTRY(r, 1, 1, "aaaa");

    raft_send_appendentries(r, raft_get_node(r, 2));
    raft_send_appendentries(r, raft_get_node(r, 3));

    CuAssertTrue(tc, NULL != sender_poll_msg_data(sender));
    CuAssertTrue(tc, NULL != sender_poll_msg_data(sender));

    raft_become_follower(r);

    /* receive mock success responses */
    raft_appendentries_resp_t aer;
    memset(&aer, 0, sizeof(raft_appendentries_resp_t));
    aer.term = 1;
    aer.success = 1;
    aer.current_idx = 1;
    CuAssertTrue(tc, 0 == raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer));
    CuAssertTrue(tc, NULL == sender_poll_msg_data(sender));
}

void TestRaft_leader_recv_appendentries_response_without_node(CuTest *tc)
{
    /* Receiving appendentries_resp without a node should be ignored silently.*/
    void *r = raft_new();

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);

    /* I'm the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);

    /* receive mock success responses */
    raft_appendentries_resp_t aer = {
            .term = 5000,
            .success = 1,
            .current_idx = 0
    };

    CuAssertIntEquals(tc, 0, raft_recv_appendentries_response(r, NULL, &aer));
    /* Verify response was ignored and term is not 5000. */
    CuAssertIntEquals(tc, 1, raft_get_current_term(r));
}

void TestRaft_leader_recv_snapshot_response_without_node(CuTest *tc)
{
    /* Receiving snapshot_resp without a node should be ignored silently. */

    void *r = raft_new();

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);

    /* I'm the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);

    /* receive mock success responses */
    raft_snapshot_resp_t resp = {
            .term = 5000,
            .success = 1,
            .msg_id = 1,
            .offset = 0,
            .last_chunk = 0,
    };

    CuAssertIntEquals(tc, 0, raft_recv_snapshot_response(r, NULL, &resp));
    /* Verify response was ignored and term is not 5000. */
    CuAssertIntEquals(tc, 1, raft_get_current_term(r));
}

void TestRaft_leader_recv_entry_resets_election_timeout(
    CuTest * tc)
{
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_config(r, 1, RAFT_CONFIG_ELECTION_TIMEOUT, 1000);
    raft_set_state(r, RAFT_STATE_LEADER);

    raft_periodic_internal(r, 900);

    /* entry message */
    raft_entry_req_t *mety = __MAKE_ENTRY(1, 1, "entry");

    /* receive entry */
    raft_entry_resp_t cr;
    raft_recv_entry(r, mety, &cr);
    CuAssertTrue(tc, 0 == raft_get_timeout_elapsed(r));
}

void TestRaft_leader_recv_entry_is_committed_returns_0_if_not_committed(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);
    raft_set_commit_idx(r, 0);

    /* entry message */
    raft_entry_req_t *mety = __MAKE_ENTRY(1, 1, "entry");

    /* receive entry */
    raft_entry_resp_t cr;
    raft_recv_entry(r, mety, &cr);
    CuAssertTrue(tc, 0 == raft_msg_entry_response_committed(r, &cr));

    raft_set_commit_idx(r, 1);
    CuAssertTrue(tc, 1 == raft_msg_entry_response_committed(r, &cr));
}

void TestRaft_leader_recv_entry_is_committed_returns_neg_1_if_invalidated(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);
    raft_set_commit_idx(r, 0);

    /* entry message */
    raft_entry_req_t *mety = __MAKE_ENTRY(1, 1, "entry");

    /* receive entry */
    raft_entry_resp_t cr;
    raft_recv_entry(r, mety, &cr);
    CuAssertTrue(tc, 0 == raft_msg_entry_response_committed(r, &cr));
    CuAssertTrue(tc, cr.term == 1);
    CuAssertTrue(tc, cr.idx == 1);
    CuAssertTrue(tc, 1 == raft_get_current_idx(r));
    CuAssertTrue(tc, 0 == raft_get_commit_idx(r));

    /* append entry that invalidates entry message */
    raft_appendentries_req_t ae;
    memset(&ae, 0, sizeof(raft_appendentries_req_t));
    ae.leader_commit = 1;
    ae.term = 2;
    ae.prev_log_idx = 0;
    ae.prev_log_term = 0;
    raft_appendentries_resp_t aer;
    ae.entries = __MAKE_ENTRY_ARRAY(999, 2, "aaa");
    ae.n_entries = 1;
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertTrue(tc, 1 == aer.success);
    CuAssertTrue(tc, 1 == raft_get_current_idx(r));
    CuAssertTrue(tc, 1 == raft_get_commit_idx(r));
    CuAssertTrue(tc, -1 == raft_msg_entry_response_committed(r, &cr));
}

void TestRaft_leader_recv_entry_fails_if_prevlogidx_less_than_commit(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
        .send_appendentries = __raft_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 2);
    raft_set_commit_idx(r, 0);

    /* entry message */
    raft_entry_req_t *mety = __MAKE_ENTRY(1, 1, "entry");

    /* receive entry */
    raft_entry_resp_t cr;
    raft_recv_entry(r, mety, &cr);
    CuAssertTrue(tc, 0 == raft_msg_entry_response_committed(r, &cr));
    CuAssertTrue(tc, cr.term == 2);
    CuAssertTrue(tc, cr.idx == 1);
    CuAssertTrue(tc, 1 == raft_get_current_idx(r));
    CuAssertTrue(tc, 0 == raft_get_commit_idx(r));

    raft_set_commit_idx(r, 1);

    /* append entry that invalidates entry message */
    raft_appendentries_req_t ae;
    memset(&ae, 0, sizeof(raft_appendentries_req_t));
    ae.leader_commit = 1;
    ae.term = 2;
    ae.prev_log_idx = 1;
    ae.prev_log_term = 1;
    raft_appendentries_resp_t aer;
    ae.entries = __MAKE_ENTRY_ARRAY(999, 2, "aaa");
    ae.n_entries = 1;
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertIntEquals(tc, 0, aer.success);
}

int backpressure(raft_server_t* raft, void* udata, raft_node_t* node)
{
    return 1;
}

void TestRaft_leader_recv_entry_does_not_send_new_appendentries_to_slow_nodes(CuTest * tc)
{
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
        .send_appendentries          = sender_appendentries,
        .backpressure = backpressure
    };

    void *sender = sender_new(NULL);
    raft_set_callbacks(r, &funcs, sender);

    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);
    raft_set_commit_idx(r, 0);

    /* make the node slow */
    raft_node_set_next_idx(raft_get_node(r, 2), 1);

    /* append entries */
    __RAFT_APPEND_ENTRY(r, 1, 1, "aaaa");

    /* entry message */
    raft_entry_req_t *mety = __MAKE_ENTRY(1, 1, "entry");

    /* receive entry */
    raft_entry_resp_t cr;
    raft_recv_entry(r, mety, &cr);

    /* check if the slow node got sent this appendentries */
    raft_appendentries_req_t * ae;
    ae = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL == ae);
}

void TestRaft_leader_recv_appendentries_response_failure_does_not_set_node_nextid_to_0(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
        .send_appendentries          = sender_appendentries,
        .log                         = NULL
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_callbacks(r, &funcs, sender);

    /* I'm the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);
    raft_set_commit_idx(r, 0);

    /* append entries */
    __RAFT_APPEND_ENTRY(r, 1, 1, "aaaa");

    /* send appendentries -
     * server will be waiting for response */
    raft_send_appendentries(r, raft_get_node(r, 2));

    /* receive mock success response */
    raft_appendentries_resp_t aer;
    memset(&aer, 0, sizeof(raft_appendentries_resp_t));
    aer.term = 1;
    aer.success = 0;
    aer.current_idx = 0;
    raft_node_t* p = raft_get_node(r, 2);
    raft_recv_appendentries_response(r, p, &aer);
    CuAssertIntEquals(tc, 2,  raft_node_get_next_idx(p));
    raft_recv_appendentries_response(r, p, &aer);
    CuAssertIntEquals(tc, 2, raft_node_get_next_idx(p));
}

void TestRaft_leader_recv_appendentries_response_increment_idx_of_node(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
        .send_appendentries          = sender_appendentries,
        .log                         = NULL
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_callbacks(r, &funcs, sender);

    /* I'm the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);

    raft_node_t* p = raft_get_node(r, 2);
    CuAssertTrue(tc, 1 == raft_node_get_next_idx(p));

    /* receive mock success responses */
    raft_appendentries_resp_t aer;
    memset(&aer, 0, sizeof(raft_appendentries_resp_t));
    aer.term = 1;
    aer.success = 1;
    aer.current_idx = 0;
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertIntEquals(tc, 1, raft_node_get_next_idx(p));
}

void TestRaft_leader_recv_appendentries_response_drop_message_if_term_is_old(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
        .send_appendentries          = sender_appendentries,
        .log                         = NULL
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_callbacks(r, &funcs, sender);

    /* I'm the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 2);

    raft_node_t* p = raft_get_node(r, 2);
    CuAssertTrue(tc, 1 == raft_node_get_next_idx(p));

    /* receive OLD mock success responses */
    raft_appendentries_resp_t aer;
    aer.term = 1;
    aer.success = 1;
    aer.current_idx = 1;
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertTrue(tc, 1 == raft_node_get_next_idx(p));
}

void TestRaft_leader_recv_appendentries_response_steps_down_if_term_is_newer(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries          = sender_appendentries,
        .log                         = NULL
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_callbacks(r, &funcs, sender);

    /* I'm the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 2);

    raft_node_t* p = raft_get_node(r, 2);
    CuAssertTrue(tc, 1 == raft_node_get_next_idx(p));

    /* receive NEW mock failed responses */
    raft_appendentries_resp_t aer;
    aer.term = 3;
    aer.success = 0;
    aer.current_idx = 2;
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertTrue(tc, 1 == raft_is_follower(r));
    CuAssertTrue(tc, -1 == raft_get_leader_id(r));
}

void TestRaft_leader_recv_appendentries_steps_down_if_newer(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_appendentries_req_t ae;
    raft_appendentries_resp_t aer;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 5);
    /* check that node 1 considers itself the leader */
    CuAssertTrue(tc, 1 == raft_is_leader(r));
    CuAssertTrue(tc, 1 == raft_get_leader_id(r));

    memset(&ae, 0, sizeof(raft_appendentries_req_t));
    ae.term = 6;
    ae.leader_id = 2;
    ae.prev_log_idx = 6;
    ae.prev_log_term = 5;
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    /* after more recent appendentries from node 2, node 1 should
     * consider node 2 the leader. */
    CuAssertTrue(tc, 1 == raft_is_follower(r));
    CuAssertTrue(tc, 2 == raft_get_leader_id(r));
}

void TestRaft_leader_recv_appendentries_steps_down_if_newer_term(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_appendentries_req_t ae;
    raft_appendentries_resp_t aer;

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 5);

    memset(&ae, 0, sizeof(raft_appendentries_req_t));
    ae.term = 6;
    ae.prev_log_idx = 5;
    ae.prev_log_term = 5;
    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);

    CuAssertTrue(tc, 1 == raft_is_follower(r));
}

void TestRaft_leader_sends_empty_appendentries_every_request_timeout(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = sender_appendentries,
        .log                = NULL
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_set_callbacks(r, &funcs, sender);
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);
    raft_config(r, 1, RAFT_CONFIG_ELECTION_TIMEOUT, 1000);
    raft_config(r, 1, RAFT_CONFIG_REQUEST_TIMEOUT, 500);
    CuAssertTrue(tc, 0 == raft_get_timeout_elapsed(r));

    /* candidate to leader */
    raft_set_state(r, RAFT_STATE_CANDIDATE);
    raft_become_leader(r);

    /* receive appendentries messages for both nodes */
    raft_appendentries_req_t * ae;
    ae = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != ae);
    ae = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != ae);

    ae = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL == ae);

    /* force request timeout */
    raft_periodic_internal(r, 501);
    ae = sender_poll_msg_data(sender);
    CuAssertTrue(tc, NULL != ae);
}

/* TODO: If a server receives a request with a stale term number, it rejects the request. */
#if 0
void T_estRaft_leader_sends_appendentries_when_receive_entry_msg(CuTest * tc)
#endif

void TestRaft_leader_recv_requestvote_responds_without_granting(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
        .send_requestvote = __raft_send_requestvote,
        .send_appendentries = sender_appendentries,
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_set_callbacks(r, &funcs, sender);
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);
    raft_config(r, 1, RAFT_CONFIG_ELECTION_TIMEOUT, 1000);
    raft_config(r, 1, RAFT_CONFIG_REQUEST_TIMEOUT, 500);
    CuAssertTrue(tc, 0 == raft_get_timeout_elapsed(r));

    raft_election_start(r, 0);

    raft_requestvote_resp_t rvr = {
        .prevote = 1,
        .request_term = raft_get_current_term(r) + 1,
        .term = 0,
        .vote_granted = 1
    };

    raft_recv_requestvote_response(r, raft_get_node(r, 2), &rvr);
    CuAssertTrue(tc, 1 == raft_is_candidate(r));

    raft_requestvote_resp_t rvr1 = {
        .request_term = 1,
        .term = 1,
        .vote_granted = 1
    };

    raft_recv_requestvote_response(r, raft_get_node(r, 2), &rvr1);
    CuAssertTrue(tc, 1 == raft_is_leader(r));

    /* receive request vote from node 3 */
    raft_requestvote_req_t rv = {
        .term = 1
    };

    raft_recv_requestvote(r, raft_get_node(r, 3), &rv, &rvr);
    CuAssertTrue(tc, 0 == rvr.vote_granted);
}

#if 0
/* This test is disabled because it violates the Raft paper's view on
 * ignoring RequestVotes when a leader is established.
 */
void T_estRaft_leader_recv_requestvote_responds_with_granting_if_term_is_higher(CuTest * tc)
{
    raft_cbs_t funcs = {
        .persist_metadata = __raft_persist_metadata,
        .send_requestvote = __raft_send_requestvote,
        .send_appendentries = sender_appendentries,
    };

    void *sender = sender_new(NULL);
    void *r = raft_new();
    raft_set_callbacks(r, &funcs, sender);
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);
    raft_config(r, 1, RAFT_CONFIG_ELECTION_TIMEOUT, 1000);
    raft_config(r, 1, RAFT_CONFIG_REQUEST_TIMEOUT, 500);
    CuAssertTrue(tc, 0 == raft_get_timeout_elapsed(r));

    raft_election_start(r);

    msg_requestvote_response_t rvr;
    memset(&rvr, 0, sizeof(msg_requestvote_response_t));
    rvr.term = 1;
    rvr.vote_granted = 1;
    raft_recv_requestvote_response(r, raft_get_node(r, 2), &rvr);
    CuAssertTrue(tc, 1 == raft_is_leader(r));

    /* receive request vote from node 3 */
    raft_requestvote_req_t rv;
    memset(&rv, 0, sizeof(raft_requestvote_req_t));
    rv.term = 2;
    raft_recv_requestvote(r, raft_get_node(r, 3), &rv, &rvr);
    CuAssertTrue(tc, 1 == raft_is_follower(r));
}
#endif

int raft_delete_entry_from_idx(raft_server_t* me_, raft_index_t idx);

void TestRaft_leader_recv_entry_add_nonvoting_node_remove_and_revert(CuTest *tc)
{
    raft_cbs_t funcs = {
            .applylog = __raft_applylog,
            .persist_metadata = __raft_persist_metadata,
            .node_has_sufficient_logs = __raft_node_has_sufficient_logs,
            .get_node_id = __raft_get_node_id
    };
    raft_log_cbs_t log_funcs = {
            .log_offer = __raft_log_offer
    };

    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    int has_sufficient_logs_flag = 0;
    raft_set_callbacks(r, &funcs, &has_sufficient_logs_flag);
    raft_log_set_callbacks(raft_get_log(r), &log_funcs, r);

    /* I'm the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);
    raft_set_commit_idx(r, 0);
    raft_set_last_applied_idx(r, 0);

    /* Add the non-voting node */
    raft_entry_t *ety = __MAKE_ENTRY(1, 1, "3");
    ety->type = RAFT_LOGTYPE_ADD_NONVOTING_NODE;
    raft_entry_resp_t etyr;
    CuAssertIntEquals(tc, 0, raft_recv_entry(r, ety, &etyr));
    CuAssertIntEquals(tc, 1, raft_node_is_active(raft_get_node(r, 3)));
    CuAssertIntEquals(tc, 0, raft_node_is_voting(raft_get_node(r, 3)));

    /* append a removal log entry for the non-voting node we just added */
    ety = __MAKE_ENTRY(1, 1, "3");
    ety->type = RAFT_LOGTYPE_REMOVE_NODE;
    CuAssertIntEquals(tc, 0, raft_recv_entry(r, ety, &etyr));
    CuAssertIntEquals(tc, 0, raft_node_is_active(raft_get_node(r, 3)));
    CuAssertIntEquals(tc, 0, raft_node_is_voting(raft_get_node(r, 3)));

    /* revert the log entry for the removal we just appended */
    CuAssertIntEquals(tc, 0, raft_delete_entry_from_idx(r, etyr.idx));
    CuAssertIntEquals(tc, 1, raft_node_is_active(raft_get_node(r, 3)));
    CuAssertIntEquals(tc, 0, raft_node_is_voting(raft_get_node(r, 3)));
}

void TestRaft_leader_recv_appendentries_response_set_has_sufficient_logs_after_voting_committed(
    CuTest * tc)
{
    raft_cbs_t funcs = {
        .applylog = __raft_applylog,
        .persist_metadata = __raft_persist_metadata,
        .node_has_sufficient_logs = __raft_node_has_sufficient_logs,
        .get_node_id = __raft_get_node_id};
    raft_log_cbs_t log_funcs = {
        .log_offer = __raft_log_offer
    };

    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);

    int has_sufficient_logs_flag = 0;
    raft_set_callbacks(r, &funcs, &has_sufficient_logs_flag);
    raft_log_set_callbacks(raft_get_log(r), &log_funcs, r);

    /* I'm the leader */
    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 1);
    raft_set_commit_idx(r, 0);
    raft_set_last_applied_idx(r, 0);

    /* Add two non-voting nodes */
    raft_entry_t *ety = __MAKE_ENTRY(1, 1, "2");
    ety->type = RAFT_LOGTYPE_ADD_NONVOTING_NODE;
    raft_entry_resp_t etyr;
    CuAssertIntEquals(tc, 0, raft_recv_entry(r, ety, &etyr));

    ety = __MAKE_ENTRY(2, 1, "3");
    ety->type = RAFT_LOGTYPE_ADD_NONVOTING_NODE;
    CuAssertIntEquals(tc, 0, raft_recv_entry(r, ety, &etyr));

    raft_appendentries_resp_t aer = {
        .term = 1, .success = 1, .current_idx = 2
    };

    /* node 3 responds so it has sufficient logs and will be promoted */
    raft_node_set_addition_committed(raft_get_node(r, 3), 1);
    raft_recv_appendentries_response(r, raft_get_node(r, 3), &aer);
    CuAssertIntEquals(tc, 1, has_sufficient_logs_flag);

    ety = __MAKE_ENTRY(3, 1, "3");
    ety->type = RAFT_LOGTYPE_ADD_NODE;
    raft_recv_entry(r, ety, &etyr);

    /* we now get a response from node 2, but it's still behind */
    raft_node_set_addition_committed(raft_get_node(r, 2), 1);
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertIntEquals(tc, 1, has_sufficient_logs_flag);

    /* both nodes respond to the promotion */
    aer.current_idx = 3;
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    raft_recv_appendentries_response(r, raft_get_node(r, 3), &aer);
    raft_apply_all(r);

    /* voting change is committed, so next time we hear from node 2
     * it should be considered as having all logs and can be promoted
     * as well.
     */
    CuAssertIntEquals(tc, 1, has_sufficient_logs_flag);
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertIntEquals(tc, 2, has_sufficient_logs_flag);
}

struct read_request_arg {
    int calls;
    int last_cb_safe;
};

static void __read_request_callback(void *arg, int safe)
{
    struct read_request_arg *a = (struct read_request_arg *) arg;
    a->calls++;
    a->last_cb_safe = safe;
}

void TestRaft_read_action_callback(
        CuTest * tc)
{
    void *r = raft_new();
    struct read_request_arg ra = { 0 };
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);
    raft_set_current_term(r, 1);
    raft_config(r, 1, RAFT_CONFIG_ELECTION_TIMEOUT, 1000);
    raft_become_leader(r);
    __RAFT_APPEND_ENTRY(r, 1, 1, "aaa");
    raft_set_commit_idx(r, 1);

    raft_recv_read_request(r, __read_request_callback, &ra);

    /* not acked yet */
    raft_periodic_internal(r, 1);
    CuAssertIntEquals(tc, 0, ra.calls);

    /* acked by node 2 - enough for quorum */
    raft_appendentries_resp_t aer = { .msg_id = 1, .term = 1, .success = 1, .current_idx = 2 };
    CuAssertIntEquals(tc, 0, raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer));

    raft_periodic_internal(r, 1);
    CuAssertIntEquals(tc, 1, ra.calls);
    CuAssertIntEquals(tc, 1, ra.last_cb_safe);
    /* make sure read request is called only once */
    raft_periodic_internal(r, 1);
    CuAssertIntEquals(tc, 1, ra.calls);
    /* entry 2 */
    __RAFT_APPEND_ENTRY(r, 2, 1, "aaa");
    ra.calls = 0;
    raft_recv_read_request(r, __read_request_callback, &ra);

    /* election started, nothing should be read */
    raft_become_candidate(r);
    raft_periodic_internal(r, 1);
    CuAssertIntEquals(tc, 0, ra.last_cb_safe);
    CuAssertIntEquals(tc, 1, ra.calls);

    /* we win, but for safety we will not process read requests
     * from past terms */
    raft_become_leader(r);
    aer.msg_id = 2;
    aer.term = 3;
    CuAssertIntEquals(tc, 0, raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer));
    raft_periodic_internal(r, 1);
    CuAssertIntEquals(tc, 1, ra.calls);
    CuAssertIntEquals(tc, 0, ra.last_cb_safe);
    /* entry 3 */
    __RAFT_APPEND_ENTRY(r, 3, 1, "aaa");

    ra.calls = 0;
    int err = raft_recv_read_request(r, __read_request_callback, &ra);
    CuAssertIntEquals(tc, RAFT_ERR_NOT_LEADER, err);

    raft_become_leader(r);
    err = raft_recv_read_request(r, __read_request_callback, &ra);
    CuAssertIntEquals(tc, 0, err);

    /* elections again, we lose */
    raft_become_candidate(r);
    raft_become_follower(r);
    /* queued read should fire back with can_read==false */
    raft_periodic_internal(r, 1);
    CuAssertIntEquals(tc, 1, ra.calls);
    CuAssertIntEquals(tc, 0, ra.last_cb_safe);
}

void single_node_commits_noop_cb(void* arg, int can_read)
{
    (void) can_read;

    *((char**) arg) = "success";
}

void TestRaft_single_node_commits_noop(CuTest * tc)
{
    static char* str = "test";
    void *r = raft_new();

    raft_add_node(r, NULL, 1, 1);
    raft_set_current_term(r, 2);
    raft_set_commit_idx(r, 0);
    raft_periodic_internal(r, 500);
    raft_recv_read_request(r, single_node_commits_noop_cb, &str);
    raft_periodic_internal(r, 500);

    CuAssertIntEquals(tc, 1, raft_get_commit_idx(r));
    CuAssertStrEquals(tc, "success", str);
}

/*
 * tests:
 * 1) if requestvote prevote flag is set, will not accept a request vote, as not timed out
 * 2) if requestvote prevote flag is not set, will accept a request vote, even though not timed out
 */
void TestRaft_server_recv_requestvote_with_transfer_node(CuTest * tc)
{
    raft_cbs_t funcs = { 0 };

    /* Setup cluster */
    raft_server_t *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);
    raft_set_current_term(r, 1);
    raft_config(r, 1, RAFT_CONFIG_ELECTION_TIMEOUT, 1000);
    raft_set_state(r, RAFT_STATE_LEADER);

    /* setup requestvote struct */
    raft_requestvote_req_t rv = {
            .prevote = 1,
            .term = 2,
            .candidate_id = 2,
            .last_log_idx = 0,
            .last_log_term = 1
    };
    raft_requestvote_resp_t rvr;

    /* test #1: try to request a vote with prevote flag */
    raft_recv_requestvote(r, raft_get_node(r, 2), &rv, &rvr);
    CuAssertTrue(tc, 1 != rvr.vote_granted);

    /* test #2: try to request a vote without the prevote flag */
    rv.prevote = 0;
    raft_recv_requestvote(r, raft_get_node(r, 2), &rv, &rvr);
    CuAssertTrue(tc, 1 == rvr.vote_granted);
}

/*
 * tests:
 * 1) if we set timeout_now, then periodic function should always declare election, even when not timed out yet
 */
void TestRaft_targeted_node_becomes_candidate_when_before_real_timeout_occurs(CuTest * tc)
{
    raft_cbs_t funcs = {
            .persist_metadata = __raft_persist_metadata,
            .send_requestvote = __raft_send_requestvote,
    };

    raft_server_t *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_callbacks(r, &funcs, NULL);

    raft_timeout_now(r);
    CuAssertTrue(tc, 1 == raft_is_candidate(r));
}

void quorum_msg_id_correctness_cb(void* arg, int can_read)
{
    (void) can_read;

    (*(int*) arg)++;
}

void TestRaft_quorum_msg_id_correctness(CuTest * tc)
{
    int val = 0;
    void *r = raft_new();

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);
    raft_config(r, 1, RAFT_CONFIG_ELECTION_TIMEOUT, 10000);
    raft_config(r, 1, RAFT_CONFIG_REQUEST_TIMEOUT, 10000);
    raft_become_leader(r);

    __RAFT_APPEND_ENTRY(r, 1, 1, "aaa");
    raft_set_commit_idx(r, 2);

    raft_periodic_internal(r, 100);
    raft_recv_read_request(r, quorum_msg_id_correctness_cb, &val);
    raft_periodic_internal(r, 200);

    // Read request is pending as it requires two acks
    CuAssertIntEquals(tc, 0, val);

    // Second node acknowledges reaq req
    raft_node_set_match_msgid(raft_get_node(r, 2), 1);
    raft_periodic_internal(r, 200);

    CuAssertIntEquals(tc, 1, val);

    val = 0;
    raft_add_node(r, NULL, 3, 0);
    raft_add_node(r, NULL, 4, 0);
    raft_periodic_internal(r, 100);
    raft_recv_read_request(r, quorum_msg_id_correctness_cb, &val);
    raft_periodic_internal(r, 200);

    // Read request is pending as it requires three acks
    CuAssertIntEquals(tc, 0, val);

    // Second node acknowledges read req,
    raft_node_set_match_msgid(raft_get_node(r, 2), 2);
    raft_periodic_internal(r, 200);
    CuAssertIntEquals(tc, 0, val);

    // Third node acknowledges read req
    raft_node_set_match_msgid(raft_get_node(r, 3), 2);
    raft_periodic_internal(r, 200);
    CuAssertIntEquals(tc, 1, val);
}

int timeoutnow_sent = 0;

int cb_timeoutnow(raft_server_t* raft, void *udata, raft_node_t* node)
{
    timeoutnow_sent++;

    return 0;
}

void TestRaft_callback_timeoutnow_at_set_if_up_to_date(CuTest *tc)
{
    timeoutnow_sent = 0;

    raft_cbs_t funcs = {
            .send_timeoutnow = cb_timeoutnow,
    };

    raft_server_t *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 2);
    raft_set_commit_idx(r, 0);
    raft_set_last_applied_idx(r, 0);

    /* append entry to increment current_idx */
    __RAFT_APPEND_ENTRIES_SEQ_ID(r, 2, 1, 1, "aaaa");

    /* shouldn't trigger callback as match_idx isn't uptodate */
    int ret = raft_transfer_leader(r, 2, 0);
    CuAssertIntEquals(tc, 0, ret);
    CuAssertTrue(tc, 0 == timeoutnow_sent);

    raft_reset_transfer_leader(r, 0);

    /* should trigger callback */
    raft_node_set_match_idx(raft_get_node(r, 2), 2);
    ret = raft_transfer_leader(r, 2, 0);
    CuAssertIntEquals(tc, 0, ret);
    CuAssertTrue(tc, 1 == timeoutnow_sent);
}

void TestRaft_callback_timeoutnow_at_send_appendentries_response_if_up_to_date(CuTest *tc)
{
    timeoutnow_sent = 0;

    raft_cbs_t funcs = {
            .send_timeoutnow = cb_timeoutnow,
    };

    raft_server_t *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_state(r, RAFT_STATE_LEADER);
    raft_set_current_term(r, 2);
    raft_set_commit_idx(r, 0);
    raft_set_last_applied_idx(r, 0);

    /* append entry to increment current_idx */
    __RAFT_APPEND_ENTRIES_SEQ_ID(r, 2, 1, 1, "aaaa");

    /* shouldn't trigger callback as match_idx isn't uptodate */
    int ret = raft_transfer_leader(r, 2, 0);
    CuAssertIntEquals(tc, 0, ret);
    CuAssertTrue(tc, 0 == timeoutnow_sent);

    raft_appendentries_resp_t aer = {
        .term = 2,
        .success = 1,
        .current_idx = 2
    };

    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertTrue(tc, 1 == timeoutnow_sent);

    /* Verify we won't send timeout now message twice. */
    raft_recv_appendentries_response(r, raft_get_node(r, 2), &aer);
    CuAssertTrue(tc, 1 == timeoutnow_sent);
}

void TestRaft_leader_steps_down_if_there_is_no_quorum(CuTest * tc)
{
    void *r = raft_new();

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);
    raft_config(r, 1, RAFT_CONFIG_ELECTION_TIMEOUT, 1000);
    raft_config(r, 1, RAFT_CONFIG_REQUEST_TIMEOUT, 1000);

    // Quorum timeout is twice the election timeout.
    int election_timeout;
    raft_config(r, 0, RAFT_CONFIG_ELECTION_TIMEOUT, &election_timeout);

    raft_time_t quorum_timeout = election_timeout * 2;

    raft_become_leader(r);

    __RAFT_APPEND_ENTRY(r, 1, 1, "aaa");
    __RAFT_APPEND_ENTRY(r, 2, 1, "aaa");
    raft_set_commit_idx(r, 2);

    raft_periodic_internal(r, 200);
    CuAssertTrue(tc, raft_is_leader(r));

    // Waiting more than quorum timeout will make leader step down.
    raft_periodic_internal(r, quorum_timeout + 1);
    CuAssertTrue(tc, !raft_is_leader(r));

    raft_node_set_match_msgid(raft_get_node(r, 2), 1);
    raft_become_leader(r);
    raft_periodic_internal(r, 200);
    CuAssertTrue(tc, raft_is_leader(r));

    // Trigger new round of append entries

    int request_timeout;
    raft_config(r, 0, RAFT_CONFIG_REQUEST_TIMEOUT, &request_timeout);

    raft_periodic_internal(r, request_timeout + 1);
    CuAssertTrue(tc, raft_is_leader(r));

    // If there is an ack from the follower, leader won't step down.
    raft_node_set_match_msgid(raft_get_node(r, 2), 2);
    raft_periodic_internal(r, quorum_timeout);
    CuAssertTrue(tc, raft_is_leader(r));

    // No ack along quorum_timeout, leader will step down.
    raft_periodic_internal(r, quorum_timeout + 1);
    CuAssertTrue(tc, !raft_is_leader(r));
}

void TestRaft_vote_for_unknown_node(CuTest * tc)
{
    void *r = raft_new();

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);

    raft_requestvote_resp_t resp;

    raft_requestvote_req_t req = {
        .term = 2,
        .last_log_idx = 1,
        .last_log_term = 1,

        // not part of the configuration
        .candidate_id = 500,
    };

    raft_recv_requestvote(r, NULL, &req, &resp);
    CuAssertTrue(tc, resp.vote_granted == 1);

    raft_destroy(r);
}

void TestRaft_recv_appendreq_from_unknown_node(CuTest * tc)
{
    void *r = raft_new();

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);
    raft_become_leader(r);

    raft_appendentries_resp_t resp;

    raft_appendentries_req_t req = {
        .term = 1,

        // not part of the configuration
        .leader_id = 10000,
    };

    // Receive the request and set node as leader
    raft_recv_appendentries(r, NULL, &req, &resp);
    CuAssertTrue(tc, resp.success == 1);

    // Verify leader node and leader id returns correct value
    CuAssertIntEquals(tc, 10000, raft_get_leader_id(r));
    CuAssertPtrEquals(tc, NULL, raft_get_leader_node(r));

    // Receive it again to verify everything is ok
    resp = (raft_appendentries_resp_t){0};
    raft_recv_appendentries(r, NULL, &req, &resp);

    CuAssertTrue(tc, resp.success == 1);
    CuAssertIntEquals(tc, 10000, raft_get_leader_id(r));
    CuAssertPtrEquals(tc, NULL, raft_get_leader_node(r));

    raft_destroy(r);
}

void TestRaft_unknown_node_can_become_leader(CuTest * tc)
{
    raft_cbs_t funcs = {
        .get_node_id = __raft_get_node_id};

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);

    raft_appendentries_resp_t resp;

    raft_entry_t **entries = __MAKE_ENTRY_ARRAY(1, 1, "100");
    entries[0]->type = RAFT_LOGTYPE_ADD_NONVOTING_NODE;

    raft_appendentries_req_t req_append = {
        .term = 1,
        .prev_log_idx = 0,
        .prev_log_term = 0,
        .leader_id = 100,
        .msg_id = 1,
        .entries = entries,
        .n_entries = 1
    };

    // Append the entry, this will set the node as leader
    raft_recv_appendentries(r, NULL, &req_append, &resp);
    CuAssertIntEquals(tc, raft_get_leader_id(r), req_append.leader_id);
    CuAssertTrue(tc, resp.success == 1);

    // Send another req with incremented leader_commit to commit the addition
    raft_appendentries_req_t req_commit = {
        .term = 1,
        .prev_log_idx = 1,
        .prev_log_term = 1,
        .leader_id = 100,
        .leader_commit = 1
    };

    raft_recv_appendentries(r, NULL, &req_commit, &resp);

    // Validate added node is the leader
    CuAssertIntEquals(tc, raft_get_leader_id(r), req_append.leader_id);
    CuAssertTrue(tc, resp.success == 1);

    // Validate added node is still the leader
    raft_periodic_internal(r, 1000);
    CuAssertIntEquals(tc, raft_get_leader_id(r), req_append.leader_id);

    // Promote node from non-voting to voting
    raft_node_t *added = raft_get_node(r, req_commit.leader_id);
    CuAssertIntEquals(tc, raft_node_get_id(added), raft_get_leader_id(r));
    CuAssertTrue(tc, raft_node_is_active(added) == 1);

    // Validate node is non-voter
    CuAssertTrue(tc, raft_node_is_voting(added) == 0);

    entries = __MAKE_ENTRY_ARRAY(1, 1, "100");
    entries[0]->type = RAFT_LOGTYPE_ADD_NODE;

    raft_appendentries_req_t req_add_node = {
        .term = 1,
        .leader_id = 100,
        .msg_id = 1,
        .entries = entries,
        .n_entries = 1,
        .prev_log_idx = 1,
        .prev_log_term = 1,
    };

    raft_recv_appendentries(r, NULL, &req_add_node, &resp);
    CuAssertIntEquals(tc, raft_get_leader_id(r), req_append.leader_id);
    CuAssertTrue(tc, resp.success == 1);

    raft_periodic_internal(r, 1000);
    CuAssertIntEquals(tc, raft_get_leader_id(r), req_append.leader_id);
    CuAssertTrue(tc, resp.success == 1);

    added = raft_get_node(r, req_commit.leader_id);
    CuAssertIntEquals(tc, raft_node_get_id(added), raft_get_leader_id(r));
    CuAssertTrue(tc, raft_node_is_active(added) == 1);

    // Validate node is voter
    CuAssertTrue(tc, raft_node_is_voting(added) == 1);

    raft_destroy(r);
}

void TestRaft_removed_node_starts_election(CuTest * tc)
{
    raft_cbs_t funcs = {
        .get_node_id = __raft_get_node_id};

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_node_t *n1 = raft_add_node(r, NULL, 1, 1);
    raft_node_t *n2 = raft_add_node(r, NULL, 2, 0);
    raft_set_current_term(r, 1);
    raft_become_leader(r);

    raft_entry_resp_t entry_resp;
    raft_entry_t *entry = __MAKE_ENTRY(1, 1, "1");
    entry->type = RAFT_LOGTYPE_REMOVE_NODE;

    raft_recv_entry(r, entry, &entry_resp);
    CuAssertTrue(r, raft_is_leader(r));
    CuAssertTrue(r, !raft_node_is_active(n1));
    CuAssertTrue(r, raft_node_is_active(n2));
    CuAssertIntEquals(r, raft_get_num_nodes(r), 2);

    raft_become_follower(r);
    raft_config(r, 1, RAFT_CONFIG_ELECTION_TIMEOUT, 1000);
    raft_periodic_internal(r, 2000);
    raft_become_candidate(r);

    raft_requestvote_resp_t reqresp = {
        .request_term = 2,
        .vote_granted = 1,
        .term = 2
    };
    raft_recv_requestvote_response(r, raft_get_node(r, 2), &reqresp);
    CuAssertTrue(r, raft_is_leader(r));
    CuAssertTrue(r, !raft_node_is_active(n1));
    CuAssertTrue(r, raft_node_is_active(n2));
    CuAssertIntEquals(r, raft_get_num_nodes(r), 2);

    raft_destroy(r);
}

int cb_send_appendentries(raft_server_t* raft,
                         void* udata, raft_node_t* node,
                          raft_appendentries_req_t * msg)
{
    CuTest * tc = udata;
    CuAssertIntEquals(tc, raft_get_nodeid(raft), msg->leader_id);
    CuAssertIntEquals(tc, raft_get_msg_id(raft), msg->msg_id);
    CuAssertIntEquals(tc, raft_get_current_term(raft), msg->term);
    CuAssertIntEquals(tc, raft_get_commit_idx(raft), msg->leader_commit);
    CuAssertIntEquals(tc, 1, msg->prev_log_idx);
    CuAssertIntEquals(tc, 7, msg->prev_log_term);

    return 0;
}

void TestRaft_verify_append_entries_fields_are_set(CuTest * tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = cb_send_appendentries,
    };

    void *r = raft_new();
    raft_set_callbacks(r, &funcs, tc);
    raft_add_node(r, NULL, 100, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_current_term(r, 7);

    raft_entry_t *ety = __MAKE_ENTRY(1, 7, "aaa");
    raft_append_entry(r, ety);
    raft_append_entry(r, ety);
    raft_append_entry(r, ety);
    raft_append_entry(r, ety);

    raft_set_current_term(r, 300);
    raft_set_commit_idx(r, 4);
    raft_node_set_next_idx(raft_get_node(r, 2), 2);
    raft_send_appendentries_all(r);
}

void cb_notify_transfer_event(raft_server_t *raft, void *udata, raft_leader_transfer_e state)
{
    raft_leader_transfer_e * data = udata;
    *data = state;
}

void Test_reset_transfer_leader(CuTest *tc)
{
    raft_leader_transfer_e state;
    raft_cbs_t funcs = {
            .notify_transfer_event = cb_notify_transfer_event,
            .send_timeoutnow = cb_timeoutnow,
    };
    raft_server_t *r = raft_new();
    raft_set_state(r, RAFT_STATE_LEADER);

    raft_set_callbacks(r, &funcs, &state);

    raft_add_node(r, NULL, 100, 1);
    raft_add_node(r, NULL, 2, 0);
    int ret = raft_transfer_leader(r, 2, 0);
    CuAssertIntEquals(tc, 0, ret);
    raft_reset_transfer_leader(r, 0);
    CuAssertIntEquals(tc, RAFT_LEADER_TRANSFER_UNEXPECTED_LEADER, state);

    ret = raft_transfer_leader(r, 2, 0);
    CuAssertIntEquals(tc, 0, ret);
    r->leader_id = 2;
    raft_reset_transfer_leader(r, 0);
    CuAssertIntEquals(tc, RAFT_LEADER_TRANSFER_EXPECTED_LEADER, state);

    /* tests timeout in general, so don't need a separate test for it */
    r->leader_id = 1;
    ret = raft_transfer_leader(r, 2, 1);
    CuAssertIntEquals(tc, 0, ret);
    raft_periodic_internal(r, 2);
    CuAssertIntEquals(tc, RAFT_LEADER_TRANSFER_TIMEOUT, state);
}

void Test_transfer_leader_success(CuTest *tc)
{
    raft_leader_transfer_e state = RAFT_LEADER_TRANSFER_TIMEOUT;
    raft_cbs_t funcs = {
            .notify_transfer_event = cb_notify_transfer_event,
            .send_timeoutnow = cb_timeoutnow
    };
    raft_server_t *r = raft_new();
    raft_set_callbacks(r, &funcs, &state);
    raft_add_node(r, NULL, 100, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_set_state(r, RAFT_STATE_LEADER);

    int ret = raft_transfer_leader(r, 2, 0);
    CuAssertIntEquals(tc, 0, ret);

    raft_appendentries_req_t ae = { 0 };
    raft_appendentries_resp_t aer;
    ae.term = 1;
    ae.leader_id = 2;

    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertTrue(tc, 1 == aer.success);
    CuAssertIntEquals(tc, RAFT_LEADER_TRANSFER_EXPECTED_LEADER, state);
}

void Test_transfer_leader_unexpected(CuTest *tc)
{
    raft_leader_transfer_e state = RAFT_LEADER_TRANSFER_TIMEOUT;
    raft_cbs_t funcs = {
            .notify_transfer_event = cb_notify_transfer_event,
            .send_timeoutnow = cb_timeoutnow
    };
    raft_server_t *r = raft_new();
    raft_set_callbacks(r, &funcs, &state);

    raft_add_node(r, NULL, 100, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);
    raft_set_state(r, RAFT_STATE_LEADER);

    int ret = raft_transfer_leader(r, 3, 0);
    CuAssertIntEquals(tc, 0, ret);

    raft_appendentries_req_t ae = { 0 };
    raft_appendentries_resp_t aer;
    ae.term = 1;
    ae.leader_id = 2;

    raft_recv_appendentries(r, raft_get_node(r, 2), &ae, &aer);
    CuAssertTrue(tc, 1 == aer.success);
    CuAssertIntEquals(tc, RAFT_LEADER_TRANSFER_UNEXPECTED_LEADER, state);
}

void Test_transfer_leader_not_leader(CuTest *tc)
{
    raft_server_t *r = raft_new();

    raft_add_node(r, NULL, 100, 1);
    raft_add_node(r, NULL, 2, 0);
    raft_add_node(r, NULL, 3, 0);

    int ret = raft_transfer_leader(r, 3, 0);
    CuAssertIntEquals(tc, RAFT_ERR_NOT_LEADER, ret);
}

void Test_transfer_automatic(CuTest *tc)
{
    raft_leader_transfer_e state = RAFT_LEADER_TRANSFER_TIMEOUT;
    raft_cbs_t funcs = {
        .notify_transfer_event = cb_notify_transfer_event,
    };
    raft_server_t *r = raft_new();
    raft_set_callbacks(r, &funcs, &state);

    raft_node_t *n1 = raft_add_node(r, NULL, 100, 1);
    raft_set_state(r, RAFT_STATE_LEADER);

    raft_node_t *n2 = raft_add_node(r, NULL, 2, 0);
    raft_node_set_match_idx(n2, 5);

    raft_node_t *n3 = raft_add_node(r, NULL, 3, 0);
    raft_node_set_match_idx(n3, 8);

    raft_node_t *n4 = raft_add_node(r, NULL, 3, 0);
    raft_node_set_match_idx(n3, 6);

    int ret = raft_transfer_leader(r, RAFT_NODE_ID_NONE, 0);
    CuAssertIntEquals(tc, 0, ret);

    CuAssertIntEquals(tc, 3, raft_get_transfer_leader(r));
}

void TestRaft_config(CuTest *tc)
{
    int val;
    int time;
    raft_server_t *r = raft_new();

    CuAssertIntEquals(tc, 0, raft_config(r, 1, RAFT_CONFIG_ELECTION_TIMEOUT, 566));
    CuAssertIntEquals(tc, 0, raft_config(r, 0, RAFT_CONFIG_ELECTION_TIMEOUT, &time));
    CuAssertIntEquals(tc, 566, time);

    CuAssertIntEquals(tc, 0, raft_config(r, 1, RAFT_CONFIG_REQUEST_TIMEOUT, 755));
    CuAssertIntEquals(tc, 0, raft_config(r, 0, RAFT_CONFIG_REQUEST_TIMEOUT, &time));
    CuAssertIntEquals(tc, 755, time);

    CuAssertIntEquals(tc, 0, raft_config(r, 1, RAFT_CONFIG_AUTO_FLUSH, 8218318312));
    CuAssertIntEquals(tc, 0, raft_config(r, 0, RAFT_CONFIG_AUTO_FLUSH, &val));
    CuAssertIntEquals(tc, 1, val);

    CuAssertIntEquals(tc, 0, raft_config(r, 1, RAFT_CONFIG_LOG_ENABLED, 1));
    CuAssertIntEquals(tc, 0, raft_config(r, 0, RAFT_CONFIG_LOG_ENABLED, &val));
    CuAssertIntEquals(tc, 1, val);

    CuAssertIntEquals(tc, 0, raft_config(r, 1, RAFT_CONFIG_NONBLOCKING_APPLY, 1));
    CuAssertIntEquals(tc, 0, raft_config(r, 0, RAFT_CONFIG_NONBLOCKING_APPLY, &val));
    CuAssertIntEquals(tc, 1, val);

    CuAssertIntEquals(tc, 0, raft_config(r, 1, RAFT_CONFIG_DISABLE_APPLY, 1));
    CuAssertIntEquals(tc, 0, raft_config(r, 0, RAFT_CONFIG_DISABLE_APPLY, &val));
    CuAssertIntEquals(tc, 1, val);

    CuAssertIntEquals(tc, RAFT_ERR_NOTFOUND, raft_config(r, 1, -1, 1));
    CuAssertIntEquals(tc, RAFT_ERR_NOTFOUND, raft_config(r, 0, -1, &val));

    long long tmp;
    CuAssertIntEquals(tc, 0, raft_config(r, 1, RAFT_CONFIG_DISABLE_APPLY, 1));
    CuAssertIntEquals(tc, 0, raft_config(r, 0, RAFT_CONFIG_DISABLE_APPLY, &tmp));
    CuAssertIntEquals(tc, 1, val);
}

static int cb_send_ae(raft_server_t *raft, void *udata, raft_node_t *node,
                      raft_appendentries_req_t *msg)
{
    CuTest *tc = udata;
    int *appendentries_msg_count = raft_node_get_udata(node);

    (*appendentries_msg_count)++;

    CuAssertIntEquals(tc, 10, msg->n_entries);
    return 0;
}

static raft_index_t cb_get_entries_to_send(raft_server_t *raft,
                                           void *user_data,
                                           raft_node_t *node,
                                           raft_index_t idx,
                                           raft_index_t entries_n,
                                           raft_entry_t **entries)
{
    const int count = 10;

    /* Fill with 10 entries */
    for (int i = 0; i < count; i++) {
        entries[i] = raft_get_entry_from_idx(raft, idx + i);
    }
    return count;
}

void TestRaft_limit_appendentries_size(CuTest *tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = cb_send_ae,
        .get_entries_to_send = cb_get_entries_to_send
    };

    int appendentries_msg_count = 0;

    raft_server_t *r = raft_new();
    raft_set_callbacks(r, &funcs, tc);
    raft_add_node(r, NULL, 100, 1);

    raft_node_t *node = raft_add_node(r, NULL, 2, 0);
    raft_node_set_udata(node, &appendentries_msg_count);

    raft_set_current_term(r, 1);

    /* Append 200 entries */
    __RAFT_APPEND_ENTRIES_SEQ_ID(r, 200, 0, 1, "test");

    /* 20 appendentries message will be sent. Each message will contain
     * 10 entries. */
    raft_send_appendentries_all(r);
    CuAssertIntEquals(tc, 20, appendentries_msg_count);
}

static int cb_sendmsg(raft_server_t *raft, void *udata, raft_node_t *node,
                      raft_appendentries_req_t *msg)
{
    CuTest *tc = udata;
    int *appendentries_msg_count = raft_node_get_udata(node);
    (*appendentries_msg_count)++;

    return 0;
}

void TestRaft_flush_sends_msg(CuTest *tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = cb_sendmsg,
    };

    int appendentries_msg_count = 0;

    raft_server_t *r = raft_new();
    raft_set_callbacks(r, &funcs, tc);
    raft_config(r, 1, RAFT_CONFIG_AUTO_FLUSH, 0);
    raft_add_node(r, NULL, 100, 1);

    raft_node_t *node = raft_add_node(r, NULL, 2, 0);
    raft_node_set_udata(node, &appendentries_msg_count);

    raft_set_current_term(r, 1);
    raft_become_leader(r);

    raft_recv_read_request(r, NULL, NULL);

    /* Verify that we send appendentries if the next msgid of a node equals
     * to the last read request's msgid. */
    raft_node_set_match_msgid(node, r->msg_id - 1);
    raft_node_set_next_msgid(node, r->msg_id);

    int msg_send = appendentries_msg_count;
    raft_flush(r, 0);
    CuAssertIntEquals(tc, msg_send + 1, appendentries_msg_count);
}

void TestRaft_recv_appendentries_does_not_change_next_idx(CuTest *tc)
{
    raft_cbs_t funcs = {
        .send_appendentries = cb_send_ae,
        .get_entries_to_send = cb_get_entries_to_send
    };

    int appendentries_msg_count = 0;

    raft_server_t *r = raft_new();
    raft_set_callbacks(r, &funcs, tc);
    raft_add_node(r, NULL, 100, 1);

    raft_node_t *node = raft_add_node(r, NULL, 2, 0);
    raft_node_set_udata(node, &appendentries_msg_count);

    raft_set_current_term(r, 1);

    /* Append 200 entries */
    __RAFT_APPEND_ENTRIES_SEQ_ID(r, 200, 0, 1, "test");

    /* 20 appendentries message will be sent. Each message will contain
     * 10 entries. */
    raft_send_appendentries_all(r);
    CuAssertIntEquals(tc, 20, appendentries_msg_count);

    CuAssertIntEquals(tc, 201, raft_node_get_next_idx(node));
    CuAssertIntEquals(tc, 21, raft_node_get_next_msgid(node));

    raft_appendentries_resp_t resp = {
        .success = 1,
        .current_idx = 20,
        .msg_id = 5,
        .term = 1
    };

    raft_recv_appendentries_response(r, node, &resp);

    CuAssertIntEquals(tc, 201, raft_node_get_next_idx(node));
    CuAssertIntEquals(tc, 21, raft_node_get_next_msgid(node));
}

#define OPERATION_DURATION 10000 /* milliseconds */

raft_time_t timestamp(raft_server_t *raft, void *user_data)
{
    raft_time_t *time = user_data;
    *time += OPERATION_DURATION;
    return *time;
}

void TestRaft_apply_entry_timeout(CuTest *tc)
{
    raft_time_t ts = 0;

    raft_cbs_t funcs = {
            .timestamp = timestamp
    };

    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_set_callbacks(r, &funcs, &ts);
    raft_set_current_term(r, 3);
    raft_config(r, 1, RAFT_CONFIG_REQUEST_TIMEOUT, 100);

    __RAFT_APPEND_ENTRIES_SEQ_ID(r, 21, 0, 3, "");
    raft_set_commit_idx(r, 21);

    /* Each execution iteration will apply 5 entries as we throttle when we
     * reach 'RAFT_CONFIG_REQUEST_TIMEOUT / 2'.
     * So, each iteration is '100 / 2 = 50 milliseconds'.
     * Each operation is 10 milliseconds. */
    raft_exec_operations(r);
    CuAssertIntEquals(tc, 1, raft_pending_operations(r));
    CuAssertIntEquals(tc, 5, raft_get_last_applied_idx(r));
    CuAssertIntEquals(tc, 3, raft_get_last_applied_term(r));

    raft_exec_operations(r);
    CuAssertIntEquals(tc, 1, raft_pending_operations(r));
    CuAssertIntEquals(tc, 10, raft_get_last_applied_idx(r));

    raft_exec_operations(r);
    CuAssertIntEquals(tc, 1, raft_pending_operations(r));
    CuAssertIntEquals(tc, 15, raft_get_last_applied_idx(r));

    raft_exec_operations(r);
    CuAssertIntEquals(tc, 1, raft_pending_operations(r));
    CuAssertIntEquals(tc, 20, raft_get_last_applied_idx(r));

    raft_exec_operations(r);
    CuAssertIntEquals(tc, 0, raft_pending_operations(r));
    CuAssertIntEquals(tc, 21, raft_get_last_applied_idx(r));

    raft_exec_operations(r);
    CuAssertIntEquals(tc, 0, raft_pending_operations(r));
    CuAssertIntEquals(tc, 21, raft_get_last_applied_idx(r));
}

void read_request(void *arg, int can_read)
{
    int *count = arg;
    *count -= 1;
}

void TestRaft_apply_read_request_timeout(CuTest *tc)
{
    raft_time_t ts = 0;

    raft_cbs_t funcs = {
            .timestamp = timestamp
    };

    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_set_callbacks(r, &funcs, &ts);
    raft_become_leader(r);
    raft_set_commit_idx(r, 1);
    raft_apply_all(r);
    raft_config(r, 1, RAFT_CONFIG_AUTO_FLUSH, 0);
    raft_config(r, 1, RAFT_CONFIG_REQUEST_TIMEOUT, 100);

    int remaining = 20;
    for (int i = 0; i < remaining; i++) {
        raft_recv_read_request(r, read_request, &remaining);
    }

    /* Each execution iteration will execute 5 operations as we throttle when we
     * reach 'RAFT_CONFIG_REQUEST_TIMEOUT / 2'.
     * So, each iteration is '100 / 2 = 50 milliseconds'.
     * Each operation is 10 milliseconds. */
    raft_exec_operations(r);
    CuAssertIntEquals(tc, 1, raft_pending_operations(r));
    CuAssertIntEquals(tc, 15, remaining);

    raft_exec_operations(r);
    CuAssertIntEquals(tc, 1, raft_pending_operations(r));
    CuAssertIntEquals(tc, 10, remaining);

    raft_exec_operations(r);
    CuAssertIntEquals(tc, 1, raft_pending_operations(r));
    CuAssertIntEquals(tc, 5, remaining);

    raft_exec_operations(r);
    CuAssertIntEquals(tc, 0, raft_pending_operations(r));
    CuAssertIntEquals(tc, 0, remaining);
}


static raft_term_t test_term;
static raft_node_id_t test_vote;

int persist_metadata(raft_server_t* raft, void *udata, raft_term_t term,
                     raft_node_id_t vote)
{
    test_term = term;
    test_vote = vote;

    return 0;
}

void TestRaft_test_metadata_on_restart(CuTest *tc)
{
    raft_cbs_t funcs = {
            .persist_metadata = persist_metadata,
    };

    raft_server_t *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);
    raft_add_node(r, NULL, 10, 1);

    CuAssertIntEquals(tc, 0, raft_restore_metadata(r, test_term, test_vote));

    raft_become_candidate(r);
    raft_become_leader(r);

    raft_clear(r);
    raft_destroy(r);

    r = raft_new();
    raft_add_node(r, NULL, 10, 1);

    CuAssertIntEquals(tc, 0, raft_restore_metadata(r, test_term, test_vote));
    CuAssertIntEquals(tc, 1, raft_get_current_term(r));
    CuAssertIntEquals(tc, 10, raft_get_voted_for(r));
}

void TestRaft_rebuild_config_after_restart(CuTest *tc)
{
    /* Test if we can rebuild the same cluster configuration from the logs after
     * a restart. */

    raft_cbs_t funcs = {
            .get_node_id = __raft_get_node_id
    };

    raft_server_t *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);
    raft_add_non_voting_node(r, NULL, 1, 1);

    raft_become_leader(r);

    raft_entry_req_t *ety = __MAKE_ENTRY(1, 1, "1");
    ety->type = RAFT_LOGTYPE_ADD_NODE;
    CuAssertTrue(tc, 0 == raft_recv_entry(r, ety, NULL));
    raft_periodic_internal(r, 2000);

    raft_entry_t *ety2 = __MAKE_ENTRY(2, 1, "2");
    ety2->type = RAFT_LOGTYPE_ADD_NODE;
    CuAssertTrue(tc, 0 == raft_recv_entry(r, ety2, NULL));

    raft_entry_t *ety3 = __MAKE_ENTRY(3, 1, "3");
    ety3->type = RAFT_LOGTYPE_ADD_NONVOTING_NODE;
    CuAssertTrue(tc, 0 == raft_recv_entry(r, ety3, NULL));

    /* Cluster has three nodes in the configuration now. A bit hacky but let's
     * create another server and use first server's log implementation for the
     * new server. We are just simulating the restart scenario. Normally, new
     * server would read log entries from the disk. */
    raft_server_t *r2 = raft_new();
    r2->log = raft_get_log(r);

    raft_set_callbacks(r2, &funcs, NULL);
    raft_add_non_voting_node(r2, NULL, 1, 1);

    /* Restore configuration with the first server's entries. */
    raft_restore_log(r2);

    /* Verify second server's configuration is same with the first server's */
    CuAssertIntEquals(tc, raft_get_num_nodes(r), raft_get_num_nodes(r2));

    for (int i = 0; i < raft_get_num_nodes(r); i++) {
        raft_node_t *n1 = raft_get_node_from_idx(r, i);
        raft_node_t *n2 = raft_get_node_from_idx(r2, i);

        CuAssertIntEquals(tc, raft_node_is_active(n1), raft_node_is_active(n2));
        CuAssertIntEquals(tc, raft_node_is_voting(n1), raft_node_is_voting(n2));
    }
}

void TestRaft_delete_configuration_change_entries(CuTest *tc)
{
    /* Delete configuration change entries and verify the configuration is
     * rolled back correctly. */

    raft_cbs_t funcs = {
            .get_node_id = __raft_get_node_id
    };

    void *r = raft_new();
    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    raft_set_callbacks(r, &funcs, NULL);
    raft_set_current_term(r, 1);
    raft_set_state(r, RAFT_STATE_LEADER);

    /* If there is no entry, delete should return immediately. */
    CuAssertIntEquals(tc, 0, raft_delete_entry_from_idx(r, 2));

    /* Add the non-voting node. */
    raft_entry_t *ety = __MAKE_ENTRY(1, 1, "3");
    ety->type = RAFT_LOGTYPE_ADD_NONVOTING_NODE;
    CuAssertIntEquals(tc, 0, raft_recv_entry(r, ety, NULL));

    /* If there idx is out of bounds, delete should return immediately. */
    CuAssertIntEquals(tc, 0, raft_delete_entry_from_idx(r, 2));

    /* Append a removal log entry for the non-voting node we just added. */
    ety = __MAKE_ENTRY(1, 1, "3");
    ety->type = RAFT_LOGTYPE_REMOVE_NODE;
    CuAssertIntEquals(tc, 0, raft_recv_entry(r, ety, NULL));

    /* Another configuration change entry should be rejected. */
    ety = __MAKE_ENTRY(1, 1, "4");
    ety->type = RAFT_LOGTYPE_ADD_NODE;
    int err = raft_recv_entry(r, ety, NULL);
    CuAssertIntEquals(tc, RAFT_ERR_ONE_VOTING_CHANGE_ONLY, err);

    /* Add some random entries. */
    __RAFT_APPEND_ENTRIES_SEQ_ID(r, 10, 1, 1, "data");
    CuAssertIntEquals(tc, 12, raft_get_log_count(r));

    /* Delete the removal log entry and others after it. */
    CuAssertIntEquals(tc, 0, raft_delete_entry_from_idx(r, 2));

    CuAssertIntEquals(tc, 1, raft_get_log_count(r));
    CuAssertIntEquals(tc, 1, raft_node_is_active(raft_get_node(r, 3)));
    CuAssertIntEquals(tc, 0, raft_node_is_voting(raft_get_node(r, 3)));

    /* Configuration change entry should be accepted now. */
    CuAssertIntEquals(tc, 0, raft_recv_entry(r, ety, NULL));
}

static int fail_sequence = 0;
static int cb_persist_metadata_fail(raft_server_t *raft, void *udata,
                                    raft_term_t term, raft_node_id_t vote)
{
    if (--fail_sequence >= 0) {
        return 0;
    }

    return -1;
}

void TestRaft_propagate_persist_metadata_failure(CuTest *tc)
{
    int e;

    raft_cbs_t funcs = {
            .persist_metadata = cb_persist_metadata_fail,
    };

    raft_server_t *r = raft_new();
    raft_set_callbacks(r, &funcs, NULL);

    raft_add_node(r, NULL, 1, 1);
    raft_add_node(r, NULL, 2, 0);

    /* This will fail when we increment the term. */
    e = raft_become_candidate(r);
    CuAssertIntEquals(tc, e, -1);

    /* This will fail when we vote for ourselves. */
    fail_sequence = 1;
    e  = raft_become_candidate(r);
    CuAssertIntEquals(tc, e, -1);

    fail_sequence = 0;
    e = raft_begin_load_snapshot(r, 2, 10);
    CuAssertIntEquals(tc, e, -1);
}

int main(void)
{
    CuString *output = CuStringNew();
    CuSuite* suite = CuSuiteNew();

    SUITE_ADD_TEST(suite, TestRaft_server_voted_for_records_who_we_voted_for);
    SUITE_ADD_TEST(suite, TestRaft_server_get_my_node);
    SUITE_ADD_TEST(suite, TestRaft_server_idx_starts_at_1);
    SUITE_ADD_TEST(suite, TestRaft_server_currentterm_defaults_to_0);
    SUITE_ADD_TEST(suite, TestRaft_server_set_currentterm_sets_term);
    SUITE_ADD_TEST(suite, TestRaft_server_voting_results_in_voting);
    SUITE_ADD_TEST(suite, TestRaft_server_add_node_makes_non_voting_node_voting);
    SUITE_ADD_TEST(suite, TestRaft_server_add_node_with_already_existing_id_is_not_allowed);
    SUITE_ADD_TEST(suite, TestRaft_server_add_non_voting_node_with_already_existing_id_is_not_allowed);
    SUITE_ADD_TEST(suite, TestRaft_server_add_non_voting_node_with_already_existing_voting_id_is_not_allowed);
    SUITE_ADD_TEST(suite, TestRaft_server_remove_node);
    SUITE_ADD_TEST(suite, TestRaft_election_start_does_not_increment_term);
    SUITE_ADD_TEST(suite, TestRaft_election_become_candidate_increments_term);
    SUITE_ADD_TEST(suite, TestRaft_set_state);
    SUITE_ADD_TEST(suite, TestRaft_server_starts_as_follower);
    SUITE_ADD_TEST(suite, TestRaft_server_starts_with_election_timeout_of_1000ms);
    SUITE_ADD_TEST(suite, TestRaft_server_starts_with_request_timeout_of_200ms);
    SUITE_ADD_TEST(suite, TestRaft_server_entry_append_increases_logidx);
    SUITE_ADD_TEST(suite, TestRaft_server_append_entry_means_entry_gets_current_term);
    SUITE_ADD_TEST(suite, TestRaft_server_append_entry_is_retrievable);
    SUITE_ADD_TEST(suite, TestRaft_server_entry_is_retrieveable_using_idx);
    SUITE_ADD_TEST(suite, TestRaft_server_wont_apply_entry_if_we_dont_have_entry_to_apply);
    SUITE_ADD_TEST(suite, TestRaft_server_wont_apply_entry_if_there_isnt_a_majority);
    SUITE_ADD_TEST(suite, TestRaft_server_increment_lastApplied_when_lastApplied_lt_commitidx);
    SUITE_ADD_TEST(suite, TestRaft_user_applylog_error_propogates_to_periodic);
    SUITE_ADD_TEST(suite, TestRaft_server_apply_entry_increments_last_applied_idx);
    SUITE_ADD_TEST(suite, TestRaft_server_periodic_elapses_election_timeout);
    SUITE_ADD_TEST(suite, TestRaft_server_election_timeout_does_not_promote_us_to_leader_if_there_is_are_more_than_1_nodes);
    SUITE_ADD_TEST(suite, TestRaft_server_election_timeout_does_not_promote_us_to_leader_if_we_are_not_voting_node);
    SUITE_ADD_TEST(suite, TestRaft_server_election_timeout_does_not_start_election_if_there_are_no_voting_nodes);
    SUITE_ADD_TEST(suite, TestRaft_server_election_timeout_does_promote_us_to_leader_if_there_is_only_1_node);
    SUITE_ADD_TEST(suite, TestRaft_server_election_timeout_does_promote_us_to_leader_if_there_is_only_1_voting_node);
    SUITE_ADD_TEST(suite, TestRaft_server_recv_entry_auto_commits_if_we_are_the_only_node);
    SUITE_ADD_TEST(suite, TestRaft_server_recv_entry_fails_if_there_is_already_a_voting_change);
    SUITE_ADD_TEST(suite, TestRaft_server_cfg_sets_num_nodes);
    SUITE_ADD_TEST(suite, TestRaft_server_cant_get_node_we_dont_have);
    SUITE_ADD_TEST(suite, TestRaft_votes_are_majority_is_true);
    SUITE_ADD_TEST(suite, TestRaft_server_recv_requestvote_response_dont_increase_votes_for_me_when_not_granted);
    SUITE_ADD_TEST(suite, TestRaft_server_recv_requestvote_response_dont_increase_votes_for_me_when_term_is_not_equal);
    SUITE_ADD_TEST(suite, TestRaft_server_recv_requestvote_response_increase_votes_for_me);
    SUITE_ADD_TEST(suite, TestRaft_server_recv_requestvote_response_must_be_candidate_to_receive);
    SUITE_ADD_TEST(suite, TestRaft_server_recv_requestvote_reply_false_if_term_less_than_current_term);
    SUITE_ADD_TEST(suite, TestRaft_leader_recv_requestvote_does_not_step_down);
    SUITE_ADD_TEST(suite, TestRaft_server_recv_requestvote_reply_true_if_term_greater_than_or_equal_to_current_term);
    SUITE_ADD_TEST(suite, TestRaft_server_recv_requestvote_reset_timeout);
    SUITE_ADD_TEST(suite, TestRaft_server_recv_requestvote_candidate_step_down_if_term_is_higher_than_current_term);
    SUITE_ADD_TEST(suite, TestRaft_server_recv_requestvote_depends_on_candidate_id);
    SUITE_ADD_TEST(suite, TestRaft_server_recv_requestvote_dont_grant_vote_if_we_didnt_vote_for_this_candidate);
    SUITE_ADD_TEST(suite, TestRaft_server_recv_requestvote_ignore_if_master_is_fresh);
    SUITE_ADD_TEST(suite, TestRaft_server_recv_prevote_ignore_if_candidate);
    SUITE_ADD_TEST(suite, TestRaft_server_recv_reqvote_ignore_if_not_candidate);
    SUITE_ADD_TEST(suite, TestRaft_server_recv_reqvote_always_update_term);
    SUITE_ADD_TEST(suite, TestRaft_follower_becomes_follower_is_follower);
    SUITE_ADD_TEST(suite, TestRaft_follower_becomes_follower_does_not_clear_voted_for);
    SUITE_ADD_TEST(suite, TestRaft_follower_recv_appendentries_reply_false_if_term_less_than_currentterm);
    SUITE_ADD_TEST(suite, TestRaft_follower_recv_snapshot_reply_false_if_term_less_than_currentterm);
    SUITE_ADD_TEST(suite, TestRaft_follower_recv_appendentries_does_not_need_node);
    SUITE_ADD_TEST(suite, TestRaft_follower_recv_appendentries_updates_currentterm_if_term_gt_currentterm);
    SUITE_ADD_TEST(suite, TestRaft_follower_recv_appendentries_does_not_log_if_no_entries_are_specified);
    SUITE_ADD_TEST(suite, TestRaft_follower_recv_appendentries_increases_log);
    SUITE_ADD_TEST(suite, TestRaft_follower_recv_appendentries_reply_false_if_doesnt_have_log_at_prev_log_idx_which_matches_prev_log_term);
    SUITE_ADD_TEST(suite, TestRaft_follower_recv_appendentries_delete_entries_if_conflict_with_new_entries_via_prev_log_idx);
    SUITE_ADD_TEST(suite, TestRaft_follower_recv_appendentries_delete_entries_if_conflict_with_new_entries_via_prev_log_idx_at_idx_0);
    SUITE_ADD_TEST(suite, TestRaft_follower_recv_appendentries_delete_entries_if_conflict_with_new_entries_greater_than_prev_log_idx);
    SUITE_ADD_TEST(suite, TestRaft_follower_recv_appendentries_add_new_entries_not_already_in_log);
    SUITE_ADD_TEST(suite, TestRaft_follower_recv_appendentries_does_not_add_dupe_entries_already_in_log);
    SUITE_ADD_TEST(suite, TestRaft_follower_recv_appendentries_partial_failures);
    SUITE_ADD_TEST(suite, TestRaft_follower_recv_appendentries_set_commitidx_to_prevLogIdx);
    SUITE_ADD_TEST(suite, TestRaft_follower_recv_appendentries_set_commitidx_to_LeaderCommit);
    SUITE_ADD_TEST(suite, TestRaft_follower_recv_appendentries_failure_includes_current_idx);
    SUITE_ADD_TEST(suite, TestRaft_follower_becomes_precandidate_when_election_timeout_occurs);
    SUITE_ADD_TEST(suite, TestRaft_follower_dont_grant_vote_if_candidate_has_a_less_complete_log);
    SUITE_ADD_TEST(suite, TestRaft_follower_recv_appendentries_heartbeat_does_not_overwrite_logs);
    SUITE_ADD_TEST(suite, TestRaft_follower_recv_appendentries_does_not_deleted_commited_entries);
    SUITE_ADD_TEST(suite, TestRaft_candidate_becomes_candidate_is_candidate);
    SUITE_ADD_TEST(suite, TestRaft_follower_becoming_candidate_increments_current_term);
    SUITE_ADD_TEST(suite, TestRaft_follower_becoming_candidate_votes_for_self);
    SUITE_ADD_TEST(suite, TestRaft_follower_becoming_candidate_resets_election_timeout);
    SUITE_ADD_TEST(suite, TestRaft_follower_recv_appendentries_resets_election_timeout);
    SUITE_ADD_TEST(suite, TestRaft_follower_becoming_candidate_requests_votes_from_other_servers);
    SUITE_ADD_TEST(suite, TestRaft_candidate_election_timeout_and_no_leader_results_in_new_election);
    SUITE_ADD_TEST(suite, TestRaft_candidate_receives_majority_of_votes_becomes_leader);
    SUITE_ADD_TEST(suite, TestRaft_candidate_will_not_respond_to_voterequest_if_it_has_already_voted);
    SUITE_ADD_TEST(suite, TestRaft_candidate_requestvote_includes_logidx);
    SUITE_ADD_TEST(suite, TestRaft_candidate_recv_requestvote_response_becomes_follower_if_current_term_is_less_than_term);
    SUITE_ADD_TEST(suite, TestRaft_candidate_recv_appendentries_frm_leader_results_in_follower);
    SUITE_ADD_TEST(suite, TestRaft_candidate_recv_appendentries_from_same_term_results_in_step_down);
    SUITE_ADD_TEST(suite, TestRaft_candidate_recv_appendentries_from_higher_term_results_in_step_down);
    SUITE_ADD_TEST(suite, TestRaft_candidate_recv_snapshot_from_higher_term_results_in_step_down);
    SUITE_ADD_TEST(suite, TestRaft_leader_becomes_leader_is_leader);
    SUITE_ADD_TEST(suite, TestRaft_leader_becomes_leader_does_not_clear_voted_for);
    SUITE_ADD_TEST(suite, TestRaft_leader_when_becomes_leader_all_nodes_have_nextidx_equal_to_lastlog_idx_plus_1);
    SUITE_ADD_TEST(suite, TestRaft_leader_when_it_becomes_a_leader_sends_empty_appendentries);
    SUITE_ADD_TEST(suite, TestRaft_leader_responds_to_entry_msg_when_entry_is_committed);
    SUITE_ADD_TEST(suite, TestRaft_non_leader_recv_entry_msg_fails);
    SUITE_ADD_TEST(suite, TestRaft_leader_sends_appendentries_with_NextIdx_when_PrevIdx_gt_NextIdx);
    SUITE_ADD_TEST(suite, TestRaft_leader_sends_appendentries_with_leader_commit);
    SUITE_ADD_TEST(suite, TestRaft_leader_sends_appendentries_with_prevLogIdx);
    SUITE_ADD_TEST(suite, TestRaft_leader_sends_appendentries_when_node_has_next_idx_of_0);
    SUITE_ADD_TEST(suite, TestRaft_leader_updates_next_idx_on_send_ae);
    SUITE_ADD_TEST(suite, TestRaft_leader_retries_appendentries_with_decremented_NextIdx_log_inconsistency);
    SUITE_ADD_TEST(suite, TestRaft_leader_append_entry_to_log_increases_idxno);
    SUITE_ADD_TEST(suite, TestRaft_leader_recv_appendentries_response_increase_commit_idx_when_majority_have_entry_and_atleast_one_newer_entry);
    SUITE_ADD_TEST(suite, TestRaft_leader_recv_appendentries_response_set_has_sufficient_logs_for_node);
    SUITE_ADD_TEST(suite, TestRaft_leader_recv_appendentries_response_increase_commit_idx_using_voting_nodes_majority);
    SUITE_ADD_TEST(suite, TestRaft_leader_recv_appendentries_response_duplicate_does_not_decrement_match_idx);
    SUITE_ADD_TEST(suite, TestRaft_leader_recv_appendentries_response_do_not_increase_commit_idx_because_of_old_terms_with_majority);
    SUITE_ADD_TEST(suite, TestRaft_leader_recv_appendentries_response_jumps_to_lower_next_idx);
    SUITE_ADD_TEST(suite, TestRaft_leader_recv_appendentries_response_retry_only_if_leader);
    SUITE_ADD_TEST(suite, TestRaft_leader_recv_appendentries_response_without_node);
    SUITE_ADD_TEST(suite, TestRaft_leader_recv_snapshot_response_without_node);
    SUITE_ADD_TEST(suite, TestRaft_leader_recv_entry_resets_election_timeout);
    SUITE_ADD_TEST(suite, TestRaft_leader_recv_entry_is_committed_returns_0_if_not_committed);
    SUITE_ADD_TEST(suite, TestRaft_leader_recv_entry_is_committed_returns_neg_1_if_invalidated);
    SUITE_ADD_TEST(suite, TestRaft_leader_recv_entry_fails_if_prevlogidx_less_than_commit);
    SUITE_ADD_TEST(suite, TestRaft_leader_recv_entry_does_not_send_new_appendentries_to_slow_nodes);
    SUITE_ADD_TEST(suite, TestRaft_leader_recv_appendentries_response_failure_does_not_set_node_nextid_to_0);
    SUITE_ADD_TEST(suite, TestRaft_leader_recv_appendentries_response_increment_idx_of_node);
    SUITE_ADD_TEST(suite, TestRaft_leader_recv_appendentries_response_drop_message_if_term_is_old);
    SUITE_ADD_TEST(suite, TestRaft_leader_recv_appendentries_response_steps_down_if_term_is_newer);
    SUITE_ADD_TEST(suite, TestRaft_leader_recv_appendentries_steps_down_if_newer);
    SUITE_ADD_TEST(suite, TestRaft_leader_recv_appendentries_steps_down_if_newer_term);
    SUITE_ADD_TEST(suite, TestRaft_leader_sends_empty_appendentries_every_request_timeout);
    SUITE_ADD_TEST(suite, TestRaft_leader_recv_requestvote_responds_without_granting);
    SUITE_ADD_TEST(suite, TestRaft_leader_recv_appendentries_response_set_has_sufficient_logs_after_voting_committed);
    SUITE_ADD_TEST(suite, TestRaft_read_action_callback);
    SUITE_ADD_TEST(suite, TestRaft_single_node_commits_noop);
    SUITE_ADD_TEST(suite, TestRaft_quorum_msg_id_correctness);
    SUITE_ADD_TEST(suite, TestRaft_leader_steps_down_if_there_is_no_quorum);
    SUITE_ADD_TEST(suite, TestRaft_vote_for_unknown_node);
    SUITE_ADD_TEST(suite, TestRaft_recv_appendreq_from_unknown_node);
    SUITE_ADD_TEST(suite, TestRaft_unknown_node_can_become_leader);
    SUITE_ADD_TEST(suite, TestRaft_removed_node_starts_election);
    SUITE_ADD_TEST(suite, TestRaft_verify_append_entries_fields_are_set);
    SUITE_ADD_TEST(suite, TestRaft_server_recv_requestvote_with_transfer_node);
    SUITE_ADD_TEST(suite, TestRaft_targeted_node_becomes_candidate_when_before_real_timeout_occurs);
    SUITE_ADD_TEST(suite, TestRaft_callback_timeoutnow_at_set_if_up_to_date);
    SUITE_ADD_TEST(suite, TestRaft_callback_timeoutnow_at_send_appendentries_response_if_up_to_date);
    SUITE_ADD_TEST(suite, Test_reset_transfer_leader);
    SUITE_ADD_TEST(suite, Test_transfer_leader_success);
    SUITE_ADD_TEST(suite, Test_transfer_leader_unexpected);
    SUITE_ADD_TEST(suite, Test_transfer_leader_not_leader);
    SUITE_ADD_TEST(suite, Test_transfer_automatic);
    SUITE_ADD_TEST(suite, TestRaft_config);
    SUITE_ADD_TEST(suite, TestRaft_limit_appendentries_size);
    SUITE_ADD_TEST(suite, TestRaft_flush_sends_msg);
    SUITE_ADD_TEST(suite, TestRaft_recv_appendentries_does_not_change_next_idx);
    SUITE_ADD_TEST(suite, TestRaft_apply_entry_timeout);
    SUITE_ADD_TEST(suite, TestRaft_apply_read_request_timeout);
    SUITE_ADD_TEST(suite, TestRaft_test_metadata_on_restart);
    SUITE_ADD_TEST(suite, TestRaft_rebuild_config_after_restart);
    SUITE_ADD_TEST(suite, TestRaft_delete_configuration_change_entries);
    SUITE_ADD_TEST(suite, TestRaft_propagate_persist_metadata_failure);
    CuSuiteRun(suite);
    CuSuiteDetails(suite, output);
    printf("%s\n", output->buffer);

    return suite->failCount == 0 ? 0 : 1;
}
