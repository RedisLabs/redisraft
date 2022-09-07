#include <stdbool.h>
#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include "CuTest.h"

#include "linked_list_queue.h"

#include "raft.h"
#include "raft_private.h"
#include "helpers.h"

static raft_node_id_t __get_node_id(
    raft_server_t* raft,
    void *udata,
    raft_entry_t *ety,
    raft_index_t ety_idx
    )
{
    return 0;
}

static int __log_offer(
    raft_server_t* raft,
    void *user_data,
    raft_entry_t *entry,
    raft_index_t entry_idx
    )
{
    CuAssertIntEquals((CuTest*)raft, 1, entry_idx);
    return 0;
}

static int __log_pop(
    raft_server_t* raft,
    void *user_data,
    raft_entry_t *entry,
    raft_index_t entry_idx
    )
{
    raft_entry_t* copy = malloc(sizeof(*entry));
    memcpy(copy, entry, sizeof(*entry));
    llqueue_offer(user_data, copy);
    return 0;
}

static int __log_pop_failing(
    raft_server_t* raft,
    void *user_data,
    raft_entry_t *entry,
    raft_index_t entry_idx
    )
{
    return -1;
}

static const raft_log_impl_t *impl = &raft_log_internal_impl;

static void __LOGIMPL_APPEND_ENTRY(void *l, int id, raft_term_t term, const char *data)
{
    raft_entry_t *e = __MAKE_ENTRY(id, term, data);

    impl->append(l, e);
}

static void __LOGIMPL_APPEND_ENTRIES_SEQ_ID(void *l, int count, int id, raft_term_t term, const char *data)
{
    int i;
    for (i = 0; i < count; i++) {
        raft_entry_t *e = __MAKE_ENTRY(id++, term, data);
        impl->append(l, e);
    }
}

void TestLogImpl_new_is_empty(CuTest * tc)
{
    void *l;

    l = impl->init(NULL, NULL);
    CuAssertTrue(tc, 0 == impl->count(l));
    CuAssertTrue(tc, 1 == impl->first_idx(l));
}

void TestLogImpl_append_is_not_empty(CuTest * tc)
{
    void *l;

    void *r = raft_new();

    l = impl->init(r, NULL);
    __LOGIMPL_APPEND_ENTRY(l, 1, 0, NULL);
    CuAssertIntEquals(tc, 1, impl->count(l));
}

void TestLogImpl_get_at_idx(CuTest * tc)
{
    void *l;

    l = impl->init(NULL, NULL);
    __LOGIMPL_APPEND_ENTRIES_SEQ_ID(l, 3, 1, 0, NULL);
    CuAssertIntEquals(tc, 3, impl->count(l));
    CuAssertIntEquals(tc, 1, impl->get(l, 1)->id);
    CuAssertIntEquals(tc, 2, impl->get(l, 2)->id);
    CuAssertIntEquals(tc, 3, impl->get(l, 3)->id);
}

void TestLogImpl_get_at_idx_returns_null_where_out_of_bounds(CuTest * tc)
{
    void *l;

    l = impl->init(NULL, NULL);
    CuAssertTrue(tc, NULL == impl->get(l, 0));
    CuAssertTrue(tc, NULL == impl->get(l, 1));

    __LOGIMPL_APPEND_ENTRY(l, 1, 0, NULL);
    CuAssertTrue(tc, NULL == impl->get(l, 2));
}

static void event_entry_enqueue(void *arg, raft_entry_t *e, raft_index_t idx)
{
    raft_entry_t* copy = malloc(sizeof(*e));
    memcpy(copy, e, sizeof(*e));
    llqueue_offer(arg, copy);
}

void TestLogImpl_pop(CuTest * tc)
{
    void* queue = llqueue_new();
    void *l;

    void *r = raft_new();
    raft_cbs_t funcs = {
        .get_node_id = __get_node_id};

    raft_set_callbacks(r, &funcs, NULL);

    l = impl->init(r, NULL);
    __LOGIMPL_APPEND_ENTRIES_SEQ_ID(l, 3, 1, 0, NULL);
    CuAssertIntEquals(tc, 3, impl->count(l));
    CuAssertIntEquals(tc, 3, impl->current_idx(l));

    event_entry_enqueue(queue, impl->get(l, 3), 3);
    impl->pop(l, 3);
    CuAssertIntEquals(tc, 2, impl->count(l));
    CuAssertIntEquals(tc, 3, ((raft_entry_t*)llqueue_poll(queue))->id);
    CuAssertTrue(tc, NULL == impl->get(l, 3));

    impl->pop(l, 2);
    CuAssertIntEquals(tc, 1, impl->count(l));
    CuAssertTrue(tc, NULL == impl->get(l, 2));

    impl->pop(l, 1);
    CuAssertIntEquals(tc, 0, impl->count(l));
    CuAssertTrue(tc, NULL == impl->get(l, 1));
}

void TestLogImpl_pop_onwards(CuTest * tc)
{
    void *r = raft_new();

    void *l;

    l = impl->init(r, NULL);
    __LOGIMPL_APPEND_ENTRIES_SEQ_ID(l, 3, 1, 0, NULL);
    CuAssertIntEquals(tc, 3, impl->count(l));

    /* even 3 gets deleted */
    impl->pop(l, 2);
    CuAssertIntEquals(tc, 1, impl->count(l));
    CuAssertIntEquals(tc, 1, impl->get(l, 1)->id);
    CuAssertTrue(tc, NULL == impl->get(l, 2));
    CuAssertTrue(tc, NULL == impl->get(l, 3));
}


void TestLogImpl_pop_fails_for_idx_zero(CuTest * tc)
{
    void *r = raft_new();

    void *l;

    l = impl->init(r, NULL);
    __LOGIMPL_APPEND_ENTRIES_SEQ_ID(l, 4, 1, 0, NULL);
    CuAssertIntEquals(tc, impl->pop(l, 0), -1);
}

void TestLogImpl_poll(CuTest * tc)
{
    void *r = raft_new();

    void *l;

    l = impl->init(r, NULL);
    __LOGIMPL_APPEND_ENTRY(l, 1, 0, NULL);
    CuAssertIntEquals(tc, 1, impl->current_idx(l));

    __LOGIMPL_APPEND_ENTRY(l, 2, 0, NULL);
    CuAssertIntEquals(tc, 2, impl->current_idx(l));

    __LOGIMPL_APPEND_ENTRY(l, 3, 0, NULL);
    CuAssertIntEquals(tc, 3, impl->count(l));
    CuAssertIntEquals(tc, 3, impl->current_idx(l));

    raft_entry_t *ety;

    /* remove 1st */
    ety = NULL;
    CuAssertIntEquals(tc, impl->poll(l, 2), 0);
    CuAssertIntEquals(tc, 2, impl->count(l));
    CuAssertIntEquals(tc, 2, impl->first_idx(l));
    CuAssertTrue(tc, NULL == impl->get(l, 1));
    CuAssertTrue(tc, NULL != impl->get(l, 2));
    CuAssertIntEquals(tc, impl->get(l, 2)->id, 2);
    CuAssertTrue(tc, NULL != impl->get(l, 3));
    CuAssertIntEquals(tc, impl->get(l, 3)->id, 3);
    CuAssertIntEquals(tc, 3, impl->current_idx(l));

    /* remove 2nd */
    ety = NULL;
    CuAssertIntEquals(tc, impl->poll(l, 3), 0);
    CuAssertIntEquals(tc, 1, impl->count(l));
    CuAssertTrue(tc, NULL == impl->get(l, 1));
    CuAssertTrue(tc, NULL == impl->get(l, 2));
    CuAssertTrue(tc, NULL != impl->get(l, 3));
    CuAssertIntEquals(tc, impl->get(l, 3)->id, 3);
    CuAssertIntEquals(tc, 3, impl->current_idx(l));

    /* remove 3rd */
    ety = NULL;
    CuAssertIntEquals(tc, impl->poll(l, 4), 0);
    CuAssertIntEquals(tc, 0, impl->count(l));
    CuAssertTrue(tc, NULL == impl->get(l, 1));
    CuAssertTrue(tc, NULL == impl->get(l, 2));
    CuAssertTrue(tc, NULL == impl->get(l, 3));
    CuAssertIntEquals(tc, 3, impl->current_idx(l));
}

void TestLogImpl_reset_after_compaction(CuTest * tc)
{
    void *l;

    l = impl->init(NULL, NULL);
    CuAssertIntEquals(tc, 0, impl->current_idx(l));

    /* Reset first_idx==1 indicates our next entry is going to be 10 */
    impl->reset(l, 10, 1);

    /* Current index is 9 (first_idx - 1), but log is empty */
    CuAssertIntEquals(tc, 9, impl->current_idx(l));
    CuAssertIntEquals(tc, 0, impl->count(l));
}

void TestLogImpl_load_from_snapshot_clears_log(CuTest * tc)
{
    void *l;

    l = impl->init(NULL, NULL);

    __LOGIMPL_APPEND_ENTRIES_SEQ_ID(l, 2, 1, 0, NULL);
    CuAssertIntEquals(tc, 2, impl->count(l));
    CuAssertIntEquals(tc, 2, impl->current_idx(l));

    impl->reset(l, 10, 1);
    CuAssertIntEquals(tc, 0, impl->count(l));
    CuAssertIntEquals(tc, 9, impl->current_idx(l));
}

void TestLogImpl_pop_after_polling(CuTest * tc)
{
    void *l;

    l = impl->init(NULL, NULL);

    /* append */
    __LOGIMPL_APPEND_ENTRY(l, 1, 0, NULL);
    CuAssertIntEquals(tc, 1, impl->count(l));
    CuAssertIntEquals(tc, 1, impl->first_idx(l));
    CuAssertIntEquals(tc, 1, impl->current_idx(l));

    /* poll */
    CuAssertIntEquals(tc, impl->poll(l, 2), 0);
    CuAssertIntEquals(tc, 0, impl->count(l));
    CuAssertIntEquals(tc, 2, impl->first_idx(l));
    CuAssertIntEquals(tc, 1, impl->current_idx(l));

    /* append */
    __LOGIMPL_APPEND_ENTRY(l, 2, 0, NULL);
    CuAssertIntEquals(tc, 1, impl->count(l));
    CuAssertIntEquals(tc, 2, impl->current_idx(l));

    /* poll */
    CuAssertIntEquals(tc, impl->pop(l, 1), 0);
    CuAssertIntEquals(tc, 0, impl->count(l));
    CuAssertIntEquals(tc, 1, impl->current_idx(l));
}

void TestLogImpl_pop_after_polling_from_double_append(CuTest * tc)
{
    void *r = raft_new();
    void *l;

    l = impl->init(r, NULL);

    /* append append */
    __LOGIMPL_APPEND_ENTRIES_SEQ_ID(l, 2, 1, 0, NULL);
    CuAssertIntEquals(tc, 2, impl->count(l));

    /* poll */
    CuAssertIntEquals(tc, impl->poll(l, 2), 0);
    CuAssertIntEquals(tc, impl->get(l, 2)->id, 2);
    CuAssertIntEquals(tc, 1, impl->count(l));

    /* append */
    __LOGIMPL_APPEND_ENTRY(l, 3, 0, NULL);
    CuAssertIntEquals(tc, 2, impl->count(l));

    /* pop */
    CuAssertIntEquals(tc, impl->pop(l, 1), 0);
    CuAssertIntEquals(tc, 0, impl->count(l));
}

void TestLogImpl_get_from_idx_with_base_off_by_one(CuTest * tc)
{
    void *r = raft_new();

    void *l;

    l = impl->init(r, NULL);

    /* append append */
    __LOGIMPL_APPEND_ENTRIES_SEQ_ID(l, 2, 1, 0, NULL);
    CuAssertIntEquals(tc, 2, impl->count(l));

    /* poll */
    CuAssertIntEquals(tc, impl->poll(l, 2), 0);
    CuAssertIntEquals(tc, 1, impl->count(l));

    /* get off-by-one index */
    raft_entry_t *e[1];
    CuAssertIntEquals(tc, impl->get_batch(l, 1, 1, e), 0);

    /* now get the correct index */
    CuAssertIntEquals(tc, impl->get_batch(l, 2, 1, e), 1);
    CuAssertIntEquals(tc, e[0]->id, 2);
}

int main(void)
{
    CuString *output = CuStringNew();
    CuSuite* suite = CuSuiteNew();

    SUITE_ADD_TEST(suite, TestLogImpl_new_is_empty);
    SUITE_ADD_TEST(suite, TestLogImpl_append_is_not_empty);
    SUITE_ADD_TEST(suite, TestLogImpl_get_at_idx);
    SUITE_ADD_TEST(suite, TestLogImpl_get_at_idx_returns_null_where_out_of_bounds);
    SUITE_ADD_TEST(suite, TestLogImpl_pop);
    SUITE_ADD_TEST(suite, TestLogImpl_pop_onwards);
    SUITE_ADD_TEST(suite, TestLogImpl_pop_fails_for_idx_zero);
    SUITE_ADD_TEST(suite, TestLogImpl_poll);
    SUITE_ADD_TEST(suite, TestLogImpl_reset_after_compaction);
    SUITE_ADD_TEST(suite, TestLogImpl_load_from_snapshot_clears_log);
    SUITE_ADD_TEST(suite, TestLogImpl_pop_after_polling);
    SUITE_ADD_TEST(suite, TestLogImpl_pop_after_polling_from_double_append);
    SUITE_ADD_TEST(suite, TestLogImpl_get_from_idx_with_base_off_by_one);

    CuSuiteRun(suite);
    CuSuiteDetails(suite, output);
    printf("%s\n", output->buffer);

    return suite->failCount == 0 ? 0 : 1;
}
