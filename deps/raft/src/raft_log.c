/**
 * Copyright (c) 2013, Willem-Hendrik Thiart
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 *
 * @file
 * @brief ADT for managing Raft log entries (aka entries)
 * @author Willem Thiart himself@willemthiart.com
 */

#include <string.h>
#include <assert.h>

#include "raft.h"
#include "raft_private.h"
#include "raft_log.h"

#define INITIAL_CAPACITY 10

struct raft_log {
    /* size of array */
    raft_index_t size;

    /* the amount of elements in the array */
    raft_index_t count;

    /* position of the queue */
    raft_index_t front, back;

    /* we compact the log, and thus need to increment the Base Log Index */
    raft_index_t base;

    raft_entry_t **entries;

    /* callbacks */
    raft_log_cbs_t cb;

    void *raft;
};

static raft_index_t mod(raft_index_t a, raft_index_t b)
{
    raft_index_t r = a % b;
    return r < 0 ? r + b : r;
}

static int ensure_capacity(raft_log_t *me)
{
    raft_entry_t **temp;

    if (me->count < me->size) {
        return 0;
    }

    temp = raft_calloc(1, sizeof(*temp) * me->size * 2);
    if (!temp) {
        return RAFT_ERR_NOMEM;
    }

    for (raft_index_t i = 0, j = me->front; i < me->count; i++, j++) {
        if (j == me->size) {
            j = 0;
        }
        temp[i] = me->entries[j];
    }

    /* clean up old entries */
    raft_free(me->entries);

    me->size *= 2;
    me->entries = temp;
    me->front = 0;
    me->back = me->count;

    return 0;
}

int raft_log_load_from_snapshot(raft_log_t *me,
                                raft_index_t idx,
                                raft_term_t term)
{
    (void) term;

    raft_log_clear_entries(me);
    raft_log_clear(me);
    me->base = idx;

    return 0;
}

raft_log_t *raft_log_alloc(raft_index_t initial_size)
{
    raft_log_t *me = raft_calloc(1, sizeof(*me));
    if (!me) {
        return NULL;
    }

    me->size = initial_size;
    raft_log_clear(me);

    me->entries = raft_calloc(1, sizeof(*me->entries) * me->size);
    if (!me->entries) {
        raft_free(me);
        return NULL;
    }

    return me;
}

raft_log_t *raft_log_new(void)
{
    return raft_log_alloc(INITIAL_CAPACITY);
}

void raft_log_set_callbacks(raft_log_t *me, raft_log_cbs_t *funcs, void *raft)
{
    me->raft = raft;
    me->cb = *funcs;
}

void raft_log_clear(raft_log_t *me)
{
    me->count = 0;
    me->back = 0;
    me->front = 0;
    me->base = 0;
}

void raft_log_clear_entries(raft_log_t *me)
{
    if (!me->count || !me->cb.log_clear) {
        return;
    }

    for (raft_index_t i = me->base; i <= me->base + me->count; i++) {
        me->cb.log_clear(me->raft, raft_get_udata(me->raft),
                         me->entries[(me->front + i - me->base) % me->size], i);
    }
}

int raft_log_append_entry(raft_log_t *me, raft_entry_t *ety)
{
    int e;
    raft_index_t idx = me->base + me->count + 1;

    e = ensure_capacity(me);
    if (e != 0) {
        return e;
    }

    me->entries[me->back] = ety;

    if (me->cb.log_offer) {
        void *udata = raft_get_udata(me->raft);

        e = me->cb.log_offer(me->raft, udata, me->entries[me->back], idx);
        if (e != 0) {
            return e;
        }
    }

    me->count++;
    me->back++;
    me->back = me->back % me->size;

    return 0;
}

raft_entry_t **raft_log_get_from_idx(raft_log_t *me,
                                     raft_index_t idx,
                                     raft_index_t *n_etys)
{
    raft_index_t i;

    assert(0 <= idx - 1);

    if (me->base + me->count < idx || idx <= me->base) {
        *n_etys = 0;
        return NULL;
    }

    /* idx starts at 1 */
    idx -= 1;

    i = (me->front + idx - me->base) % me->size;

    /* log entries until the end of the log */
    *n_etys = (i < me->back) ? (me->back - i) : (me->size - i);

    return &me->entries[i];
}

raft_entry_t *raft_log_get_at_idx(raft_log_t *me, raft_index_t idx)
{
    raft_index_t i;

    if (idx == 0) {
        return NULL;
    }

    if (me->base + me->count < idx || idx <= me->base) {
        return NULL;
    }

    /* idx starts at 1 */
    idx -= 1;

    i = (me->front + idx - me->base) % me->size;
    return me->entries[i];
}

raft_index_t raft_log_count(raft_log_t *me)
{
    return me->count;
}

static int log_delete(raft_log_t *me, raft_index_t idx)
{
    if (idx == 0) {
        return -1;
    }

    if (idx < me->base) {
        idx = me->base;
    }

    for (; idx <= me->base + me->count && me->count;) {
        raft_index_t idx_tmp = me->base + me->count;
        raft_index_t back = mod(me->back - 1, me->size);

        if (me->cb.log_pop) {
            int e = me->cb.log_pop(me->raft, raft_get_udata(me->raft),
                                   me->entries[back], idx_tmp);
            if (e != 0) {
                return e;
            }
        }

        raft_entry_release(me->entries[back]);

        me->back = back;
        me->count--;
    }

    return 0;
}

int raft_log_delete(raft_log_t *me, raft_index_t idx)
{
    return log_delete(me, idx);
}

int raft_log_poll(raft_log_t *me, raft_entry_t **etyp)
{
    raft_index_t idx = me->base + 1;

    if (me->count == 0) {
        return -1;
    }

    raft_entry_t *elem = me->entries[me->front];

    if (me->cb.log_poll) {
        int e = me->cb.log_poll(me->raft, raft_get_udata(me->raft),
                                me->entries[me->front], idx);
        if (e != 0) {
            return e;
        }
    }

    raft_entry_release(me->entries[me->front]);

    me->front++;
    me->front = me->front % me->size;
    me->count--;
    me->base++;

    *etyp = elem;

    return 0;
}

raft_entry_t *raft_log_peektail(raft_log_t *me)
{
    if (me->count == 0) {
        return NULL;
    }

    raft_index_t tail = (me->back == 0) ? (me->size - 1) : (me->back - 1);

    return me->entries[tail];
}

void raft_log_empty(raft_log_t *me)
{
    me->front = 0;
    me->back = 0;
    me->count = 0;
}

void raft_log_free(raft_log_t *me)
{
    raft_free(me->entries);
    raft_free(me);
}

raft_index_t raft_log_get_current_idx(raft_log_t *me)
{
    return raft_log_count(me) + me->base;
}

raft_index_t raft_log_get_base(raft_log_t *me)
{
    return me->base;
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

static void *log_init(void *raft, void *arg)
{
    raft_log_t *log = raft_log_new();
    if (arg) {
        raft_log_set_callbacks(log, arg, raft);
    }
    return log;
}

static void log_free(void *log)
{
    raft_log_free(log);
}

static void log_reset(void *log, raft_index_t first_idx, raft_term_t term)
{
    (void) term;

    raft_log_clear_entries(log);
    raft_log_clear(log);

    assert(first_idx >= 1);
    ((raft_log_t *) log)->base = first_idx - 1;
}

static int log_append(void *log, raft_entry_t *entry)
{
    raft_entry_hold(entry);
    return raft_log_append_entry(log, entry);
}

static raft_entry_t *log_get(void *log, raft_index_t idx)
{
    raft_entry_t *e = raft_log_get_at_idx(log, idx);
    if (e != NULL) {
        raft_entry_hold(e);
    }

    return e;
}

static raft_index_t log_get_batch(void *log,
                                  raft_index_t idx,
                                  raft_index_t entries_n,
                                  raft_entry_t **entries)
{
    raft_index_t n;
    raft_entry_t **r = raft_log_get_from_idx(log, idx, &n);

    if (!r || n < 1) {
        return 0;
    }

    if (n > entries_n) {
        n = entries_n;
    }

    for (raft_index_t i = 0; i < n; i++) {
        entries[i] = r[i];
        raft_entry_hold(entries[i]);
    }

    return n;
}

static int log_pop(void *log, raft_index_t from_idx)
{
    return log_delete(log, from_idx);
}

static int log_poll(void *log, raft_index_t first_idx)
{
    while (raft_log_get_base(log) + 1 < first_idx) {
        raft_entry_t *ety;
        int e = raft_log_poll(log, &ety);
        if (e < 0) {
            return e;
        }
    }

    return 0;
}

static raft_index_t log_first_idx(void *log)
{
    return raft_log_get_base(log) + 1;
}

static raft_index_t log_current_idx(void *log)
{
    return raft_log_get_current_idx(log);
}

static raft_index_t log_count(void *log)
{
    return raft_log_count(log);
}

static int log_sync(void *log)
{
    (void) log;
    return 0;
}

const raft_log_impl_t raft_log_internal_impl = {
    .init = log_init,
    .free = log_free,
    .reset = log_reset,
    .append = log_append,
    .poll = log_poll,
    .pop = log_pop,
    .get = log_get,
    .get_batch = log_get_batch,
    .first_idx = log_first_idx,
    .current_idx = log_current_idx,
    .count = log_count,
    .sync = log_sync
};
