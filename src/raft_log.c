/**
 * Copyright (c) 2013, Willem-Hendrik Thiart
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 *
 * @file
 * @brief ADT for managing Raft log entries (aka entries)
 * @author Willem Thiart himself@willemthiart.com
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>

#include "raft.h"
#include "raft_private.h"
#include "raft_log.h"

#define INITIAL_CAPACITY 10

typedef struct
{
    /* size of array */
    raft_index_t size;

    /* the amount of elements in the array */
    raft_index_t count;

    /* position of the queue */
    raft_index_t front, back;

    /* we compact the log, and thus need to increment the Base Log Index */
    raft_index_t base;

    raft_entry_t** entries;

    /* callbacks */
    raft_log_cbs_t cb;

    void* raft;
} log_private_t;

static int mod(raft_index_t a, raft_index_t b)
{
    int r = a % b;
    return r < 0 ? r + b : r;
}

static int __ensurecapacity(log_private_t * me)
{
    raft_index_t i, j;
    raft_entry_t **temp;

    if (me->count < me->size)
        return 0;

    temp = (raft_entry_t**)__raft_calloc(1, sizeof(raft_entry_t *) * me->size * 2);
    if (!temp)
        return RAFT_ERR_NOMEM;

    for (i = 0, j = me->front; i < me->count; i++, j++)
    {
        if (j == me->size)
            j = 0;
        temp[i] = me->entries[j];
    }

    /* clean up old entries */
    __raft_free(me->entries);

    me->size *= 2;
    me->entries = temp;
    me->front = 0;
    me->back = me->count;
    return 0;
}

int log_load_from_snapshot(log_t *me_, raft_index_t idx, raft_term_t term)
{
    log_private_t* me = (log_private_t*)me_;

    log_clear_entries(me_);
    log_clear(me_);
    me->base = idx;

    return 0;
}

log_t* log_alloc(raft_index_t initial_size)
{
    log_private_t* me = (log_private_t*)__raft_calloc(1, sizeof(log_private_t));
    if (!me)
        return NULL;
    me->size = initial_size;
    log_clear((log_t*)me);
    me->entries = (raft_entry_t**)__raft_calloc(1, sizeof(raft_entry_t *) * me->size);
    if (!me->entries) {
        __raft_free(me);
        return NULL;
    }
    return (log_t*)me;
}

log_t* log_new(void)
{
    return log_alloc(INITIAL_CAPACITY);
}

void log_set_callbacks(log_t* me_, raft_log_cbs_t* funcs, void* raft)
{
    log_private_t* me = (log_private_t*)me_;

    me->raft = raft;
    me->cb = *funcs;
}

void log_clear(log_t* me_)
{
    log_private_t* me = (log_private_t*)me_;
    me->count = 0;
    me->back = 0;
    me->front = 0;
    me->base = 0;
}

void log_clear_entries(log_t* me_)
{
    log_private_t* me = (log_private_t*)me_;
    raft_index_t i;

    if (!me->count || !me->cb.log_clear)
        return;

    for (i = me->base; i <= me->base + me->count; i++)
    {
        me->cb.log_clear(me->raft, raft_get_udata(me->raft),
                          me->entries[(me->front + i - me->base) % me->size], i);
    }
}

/** TODO: rename log_append */
int log_append_entry(log_t* me_, raft_entry_t* ety)
{
    log_private_t* me = (log_private_t*)me_;
    raft_index_t idx = me->base + me->count + 1;
    int e;

    e = __ensurecapacity(me);
    if (e != 0)
        return e;

    me->entries[me->back] = ety;

    if (me->cb.log_offer)
    {
        void* ud = raft_get_udata(me->raft);
        e = me->cb.log_offer(me->raft, ud, me->entries[me->back], idx);
        if (0 != e)
            return e;
    }

    me->count++;
    me->back++;
    me->back = me->back % me->size;

    return 0;
}

raft_entry_t** log_get_from_idx(log_t* me_, raft_index_t idx, int *n_etys)
{
    log_private_t* me = (log_private_t*)me_;
    raft_index_t i;

    assert(0 <= idx - 1);

    if (me->base + me->count < idx || idx <= me->base)
    {
        *n_etys = 0;
        return NULL;
    }

    /* idx starts at 1 */
    idx -= 1;

    i = (me->front + idx - me->base) % me->size;

    int logs_till_end_of_log;

    if (i < me->back)
        logs_till_end_of_log = me->back - i;
    else
        logs_till_end_of_log = me->size - i;

    *n_etys = logs_till_end_of_log;
    return &me->entries[i];
}

raft_entry_t* log_get_at_idx(log_t* me_, raft_index_t idx)
{
    log_private_t* me = (log_private_t*)me_;
    raft_index_t i;

    if (idx == 0)
        return NULL;

    if (me->base + me->count < idx || idx <= me->base)
        return NULL;

    /* idx starts at 1 */
    idx -= 1;

    i = (me->front + idx - me->base) % me->size;
    return me->entries[i];
}

raft_index_t log_count(log_t* me_)
{
    return ((log_private_t*)me_)->count;
}

static int __log_delete(log_t* me_, raft_index_t idx, func_entry_notify_f cb, void *cb_arg)
{
    log_private_t* me = (log_private_t*)me_;

    if (0 == idx)
        return -1;

    if (idx < me->base)
        idx = me->base;

    for (; idx <= me->base + me->count && me->count;)
    {
        raft_index_t idx_tmp = me->base + me->count;
        raft_index_t back = mod(me->back - 1, me->size);

        if (me->cb.log_pop)
        {
            int e = me->cb.log_pop(me->raft, raft_get_udata(me->raft),
                                   me->entries[back], idx_tmp);
            if (0 != e)
                return e;
        }

        if (cb)
            cb(cb_arg, me->entries[back], idx_tmp);

        me->back = back;
        me->count--;
    }
    return 0;
}

int log_delete(log_t* me_, raft_index_t idx)
{
    return __log_delete(me_, idx, NULL, NULL);
}

int log_poll(log_t * me_, void** etyp)
{
    log_private_t* me = (log_private_t*)me_;
    raft_index_t idx = me->base + 1;

    if (0 == me->count)
        return -1;

    const void *elem = me->entries[me->front];
    if (me->cb.log_poll)
    {
        int e = me->cb.log_poll(me->raft, raft_get_udata(me->raft),
                                 me->entries[me->front], idx);
        if (0 != e)
            return e;
    }
    me->front++;
    me->front = me->front % me->size;
    me->count--;
    me->base++;

    *etyp = (void*)elem;
    return 0;
}

raft_entry_t *log_peektail(log_t * me_)
{
    log_private_t* me = (log_private_t*)me_;

    if (0 == me->count)
        return NULL;

    if (0 == me->back)
        return me->entries[me->size - 1];
    else
        return me->entries[me->back - 1];
}

void log_empty(log_t * me_)
{
    log_private_t* me = (log_private_t*)me_;

    me->front = 0;
    me->back = 0;
    me->count = 0;
}

void log_free(log_t * me_)
{
    log_private_t* me = (log_private_t*)me_;

    __raft_free(me->entries);
    __raft_free(me);
}

raft_index_t log_get_current_idx(log_t* me_)
{
    log_private_t* me = (log_private_t*)me_;
    return log_count(me_) + me->base;
}

raft_index_t log_get_base(log_t* me_)
{
    return ((log_private_t*)me_)->base;
}

/**
 * The following functions wrap raft_log.c implementation to make it compatible
 * with raft_log_impl_t binding.
 *
 * The rationale for doing this and not modifying raft_log.c directly is to
 * leave test_log.c intact.
 *
 * @todo Ideally test_log.c should be implemented (and extended) to use
 *      raft_log_impl_t, so it can become a test harness for testing arbitrary
 *      log implementations.
 */

void *__log_init(void *raft, void *arg)
{
    log_t *log = log_new();
    if (arg) {
        log_set_callbacks(log, arg, raft);
    }
    return log;
}

static void __log_free(void *log)
{
    log_free(log);
}

static void __log_reset(void *log, raft_index_t first_idx, raft_term_t term)
{
    log_clear_entries(log);
    log_clear(log);

    assert(first_idx >= 1);
    ((log_private_t*) log)->base = first_idx - 1;
}

static int __log_append(void *log, raft_entry_t *entry)
{
    return log_append_entry(log, entry);
}

static raft_entry_t *__log_get(void *log, raft_index_t idx)
{
    return log_get_at_idx(log, idx);
}

static int __log_get_batch(void *log, raft_index_t idx, int entries_n, raft_entry_t **entries)
{
    int n, i;
    raft_entry_t **r = log_get_from_idx(log, idx, &n);

    if (!r || n < 1) {
        return 0;
    }

    if (n > entries_n)
        n = entries_n;

    for (i = 0; i < n; i++) {
        entries[i] = r[i];
    }
    return n;
}

static int __log_pop(void *log, raft_index_t from_idx, func_entry_notify_f cb, void *cb_arg)
{
    return __log_delete(log, from_idx, cb, cb_arg);
}

static int __log_poll(void *log, raft_index_t first_idx)
{
    while (log_get_base(log) + 1 < first_idx) {
        raft_entry_t *ety;
        int e = log_poll(log, (void **) &ety);

        if (e < 0)
            return e;
    }

    return 0;
}

static raft_index_t __log_first_idx(void *log)
{
    return log_get_base(log) + 1;
}

static raft_index_t __log_current_idx(void *log)
{
    return log_get_current_idx(log);
}

static raft_index_t __log_count(void *log)
{
    return log_count(log);
}

const raft_log_impl_t raft_log_internal_impl = {
    .init = __log_init,
    .free = __log_free,
    .reset = __log_reset,
    .append = __log_append,
    .poll = __log_poll,
    .pop = __log_pop,
    .get = __log_get,
    .get_batch = __log_get_batch,
    .first_idx = __log_first_idx,
    .current_idx = __log_current_idx,
    .count = __log_count
};
