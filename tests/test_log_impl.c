#include <stdbool.h>
#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include "CuTest.h"

#include "linked_list_queue.h"

#include "raft.h"

static int __logentry_get_node_id(
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
    raft_entry_t e;

    void *r = raft_new();

    memset(&e, 0, sizeof(raft_entry_t));

    e.id = 1;

    l = impl->init(r, NULL);
    CuAssertIntEquals(tc, 0, impl->append(l, &e));
    CuAssertIntEquals(tc, 1, impl->count(l));
}

void TestLogImpl_get_at_idx(CuTest * tc)
{
    void *l;
    raft_entry_t e1, e2, e3;

    memset(&e1, 0, sizeof(raft_entry_t));
    memset(&e2, 0, sizeof(raft_entry_t));
    memset(&e3, 0, sizeof(raft_entry_t));

    l = impl->init(NULL, NULL);
    e1.id = 1;
    CuAssertIntEquals(tc, 0, impl->append(l, &e1));
    e2.id = 2;
    CuAssertIntEquals(tc, 0, impl->append(l, &e2));
    e3.id = 3;
    CuAssertIntEquals(tc, 0, impl->append(l, &e3));
    CuAssertIntEquals(tc, 3, impl->count(l));
    CuAssertIntEquals(tc, e1.id, impl->get(l, 1)->id);
    CuAssertIntEquals(tc, e2.id, impl->get(l, 2)->id);
    CuAssertIntEquals(tc, e3.id, impl->get(l, 3)->id);
}

void TestLogImpl_get_at_idx_returns_null_where_out_of_bounds(CuTest * tc)
{
    void *l;
    raft_entry_t e1;

    memset(&e1, 0, sizeof(raft_entry_t));

    l = impl->init(NULL, NULL);
    CuAssertTrue(tc, NULL == impl->get(l, 0));
    CuAssertTrue(tc, NULL == impl->get(l, 1));

    e1.id = 1;
    CuAssertIntEquals(tc, 0, impl->append(l, &e1));
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
    raft_entry_t e1, e2, e3;

    void *r = raft_new();
    raft_cbs_t funcs = {
        .log_get_node_id = __logentry_get_node_id
    };

    raft_set_callbacks(r, &funcs, NULL);

    l = impl->init(r, NULL);

    memset(&e1, 0, sizeof(raft_entry_t));
    memset(&e2, 0, sizeof(raft_entry_t));
    memset(&e3, 0, sizeof(raft_entry_t));

    e1.id = 1;
    CuAssertIntEquals(tc, 0, impl->append(l, &e1));
    e2.id = 2;
    CuAssertIntEquals(tc, 0, impl->append(l, &e2));
    e3.id = 3;
    CuAssertIntEquals(tc, 0, impl->append(l, &e3));
    CuAssertIntEquals(tc, 3, impl->count(l));
    CuAssertIntEquals(tc, 3, impl->current_idx(l));

    impl->pop(l, 3, event_entry_enqueue, queue);
    CuAssertIntEquals(tc, 2, impl->count(l));
    CuAssertIntEquals(tc, e3.id, ((raft_entry_t*)llqueue_poll(queue))->id);
    CuAssertTrue(tc, NULL == impl->get(l, 3));

    impl->pop(l, 2, NULL, NULL);
    CuAssertIntEquals(tc, 1, impl->count(l));
    CuAssertTrue(tc, NULL == impl->get(l, 2));

    impl->pop(l, 1, NULL, NULL);
    CuAssertIntEquals(tc, 0, impl->count(l));
    CuAssertTrue(tc, NULL == impl->get(l, 1));
}

void TestLogImpl_pop_onwards(CuTest * tc)
{
    void *r = raft_new();

    void *l;
    raft_entry_t e1, e2, e3;

    memset(&e1, 0, sizeof(raft_entry_t));
    memset(&e2, 0, sizeof(raft_entry_t));
    memset(&e3, 0, sizeof(raft_entry_t));

    l = impl->init(r, NULL);
    e1.id = 1;
    CuAssertIntEquals(tc, 0, impl->append(l, &e1));
    e2.id = 2;
    CuAssertIntEquals(tc, 0, impl->append(l, &e2));
    e3.id = 3;
    CuAssertIntEquals(tc, 0, impl->append(l, &e3));
    CuAssertIntEquals(tc, 3, impl->count(l));

    /* even 3 gets deleted */
    impl->pop(l, 2, NULL, NULL);
    CuAssertIntEquals(tc, 1, impl->count(l));
    CuAssertIntEquals(tc, e1.id, impl->get(l, 1)->id);
    CuAssertTrue(tc, NULL == impl->get(l, 2));
    CuAssertTrue(tc, NULL == impl->get(l, 3));
}


void TestLogImpl_pop_fails_for_idx_zero(CuTest * tc)
{
    void *r = raft_new();

    void *l;
    raft_entry_t e1, e2, e3, e4;

    memset(&e1, 0, sizeof(raft_entry_t));
    memset(&e2, 0, sizeof(raft_entry_t));
    memset(&e3, 0, sizeof(raft_entry_t));
    memset(&e4, 0, sizeof(raft_entry_t));

    e1.id = 1;
    e2.id = 2;
    e3.id = 3;
    e4.id = 4;

    l = impl->init(r, NULL);
    CuAssertIntEquals(tc, 0, impl->append(l, &e1));
    CuAssertIntEquals(tc, 0, impl->append(l, &e2));
    CuAssertIntEquals(tc, 0, impl->append(l, &e3));
    CuAssertIntEquals(tc, 0, impl->append(l, &e4));
    CuAssertIntEquals(tc, impl->pop(l, 0, NULL, NULL), -1);
}

void TestLogImpl_poll(CuTest * tc)
{
    void *r = raft_new();

    void *l;
    raft_entry_t e1, e2, e3;

    l = impl->init(r, NULL);

    memset(&e1, 0, sizeof(raft_entry_t));
    memset(&e2, 0, sizeof(raft_entry_t));
    memset(&e3, 0, sizeof(raft_entry_t));

    e1.id = 1;
    CuAssertIntEquals(tc, 0, impl->append(l, &e1));
    CuAssertIntEquals(tc, 1, impl->current_idx(l));

    e2.id = 2;
    CuAssertIntEquals(tc, 0, impl->append(l, &e2));
    CuAssertIntEquals(tc, 2, impl->current_idx(l));

    e3.id = 3;
    CuAssertIntEquals(tc, 0, impl->append(l, &e3));
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
    raft_entry_t e1, e2, e3;

    memset(&e1, 0, sizeof(raft_entry_t));
    memset(&e2, 0, sizeof(raft_entry_t));
    memset(&e3, 0, sizeof(raft_entry_t));

    l = impl->init(NULL, NULL);

    CuAssertIntEquals(tc, 0, impl->append(l, &e1));
    CuAssertIntEquals(tc, 0, impl->append(l, &e2));
    CuAssertIntEquals(tc, 2, impl->count(l));
    CuAssertIntEquals(tc, 2, impl->current_idx(l));

    impl->reset(l, 10, 1);
    CuAssertIntEquals(tc, 0, impl->count(l));
    CuAssertIntEquals(tc, 9, impl->current_idx(l));
}

void TestLogImpl_pop_after_polling(CuTest * tc)
{
    void *l;
    raft_entry_t e1, e2, e3, e4;

    memset(&e1, 0, sizeof(raft_entry_t));
    memset(&e2, 0, sizeof(raft_entry_t));
    memset(&e3, 0, sizeof(raft_entry_t));
    memset(&e4, 0, sizeof(raft_entry_t));

    e1.id = 1;
    e2.id = 2;
    e3.id = 3;
    e4.id = 4;

    l = impl->init(NULL, NULL);

    /* append */
    CuAssertIntEquals(tc, 0, impl->append(l, &e1));
    CuAssertIntEquals(tc, 1, impl->count(l));
    CuAssertIntEquals(tc, 1, impl->first_idx(l));
    CuAssertIntEquals(tc, 1, impl->current_idx(l));

    /* poll */
    CuAssertIntEquals(tc, impl->poll(l, 2), 0);
    CuAssertIntEquals(tc, 0, impl->count(l));
    CuAssertIntEquals(tc, 2, impl->first_idx(l));
    CuAssertIntEquals(tc, 1, impl->current_idx(l));

    /* append */
    CuAssertIntEquals(tc, 0, impl->append(l, &e2));
    CuAssertIntEquals(tc, 1, impl->count(l));
    CuAssertIntEquals(tc, 2, impl->current_idx(l));

    /* poll */
    CuAssertIntEquals(tc, impl->pop(l, 1, NULL, NULL), 0);
    CuAssertIntEquals(tc, 0, impl->count(l));
    CuAssertIntEquals(tc, 1, impl->current_idx(l));
}

void TestLogImpl_pop_after_polling_from_double_append(CuTest * tc)
{
    void *r = raft_new();
    void *l;
    raft_entry_t e1, e2, e3, e4;

    memset(&e1, 0, sizeof(raft_entry_t));
    memset(&e2, 0, sizeof(raft_entry_t));
    memset(&e3, 0, sizeof(raft_entry_t));
    memset(&e4, 0, sizeof(raft_entry_t));

    e1.id = 1;
    e2.id = 2;
    e3.id = 3;
    e4.id = 4;

    l = impl->init(r, NULL);

    /* append append */
    CuAssertIntEquals(tc, 0, impl->append(l, &e1));
    CuAssertIntEquals(tc, 0, impl->append(l, &e2));
    CuAssertIntEquals(tc, 2, impl->count(l));

    /* poll */
    CuAssertIntEquals(tc, impl->poll(l, 2), 0);
    CuAssertIntEquals(tc, impl->get(l, 2)->id, 2);
    CuAssertIntEquals(tc, 1, impl->count(l));

    /* append */
    CuAssertIntEquals(tc, 0, impl->append(l, &e3));
    CuAssertIntEquals(tc, 2, impl->count(l));

    /* pop */
    CuAssertIntEquals(tc, impl->pop(l, 1, NULL, NULL), 0);
    CuAssertIntEquals(tc, 0, impl->count(l));
}

void TestLogImpl_get_from_idx_with_base_off_by_one(CuTest * tc)
{
    void *r = raft_new();

    void *l;
    raft_entry_t e1, e2;

    memset(&e1, 0, sizeof(raft_entry_t));
    memset(&e2, 0, sizeof(raft_entry_t));

    e1.id = 1;
    e2.id = 2;

    l = impl->init(r, NULL);

    /* append append */
    CuAssertIntEquals(tc, 0, impl->append(l, &e1));
    CuAssertIntEquals(tc, 0, impl->append(l, &e2));
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
