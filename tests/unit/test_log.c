/*
 * Copyright Redis Ltd. 2020 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "../src/entrycache.h"
#include "../src/log.h"

#include <limits.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "cmocka.h"

#define LOGNAME "test.log.db"
#define DBID    "01234567890123456789012345678901"

static int setup_create_log(void **state)
{
    *state = LogCreate(LOGNAME, DBID, 1, 0, 1);
    assert_non_null(*state);
    return 0;
}

static int teardown_log(void **state)
{
    Log *log = (Log *) *state;
    LogFree(log);
    unlink(LOGNAME);
    unlink(LOGNAME ".idx");
    return 0;
}

static raft_entry_t *__make_entry_value(int id, const char *value)
{
    raft_entry_t *e = raft_entry_new(strlen(value) + 1);
    e->id = id;
    strcpy(e->data, value);
    return e;
}

static raft_entry_t *__make_entry(int id)
{
    raft_entry_t *e = raft_entry_new(50);
    e->id = id;
    snprintf(e->data, 49, "value%d\n", id);
    return e;
}

static void __append_entry(Log *log, int id)
{
    raft_entry_t *e = __make_entry(id);

    assert_int_equal(LogAppend(log, e), RR_OK);
    raft_entry_release(e);
}

static void test_log_random_access(void **state)
{
    Log *log = (Log *) *state;

    __append_entry(log, 3);
    __append_entry(log, 30);

    /* Invalid out of bound reads */
    assert_null(LogGet(log, 0));
    assert_null(LogGet(log, 3));

    raft_entry_t *e = LogGet(log, 1);
    assert_int_equal(e->id, 3);
    raft_entry_release(e);

    e = LogGet(log, 2);
    assert_int_equal(e->id, 30);
    raft_entry_release(e);
}

static void test_log_random_access_with_snapshot(void **state)
{
    Log *log = (Log *) *state;

    /* Reset log assuming last snapshot is 100 */
    LogReset(log, 100, 1);

    /* Write entries */
    __append_entry(log, 3);
    __append_entry(log, 30);

    assert_int_equal(LogFirstIdx(log), 101);

    /* Invalid out of bound reads */
    assert_null(LogGet(log, 99));
    assert_null(LogGet(log, 100));
    assert_null(LogGet(log, 103));

    raft_entry_t *e = LogGet(log, 101);
    assert_non_null(e);
    assert_int_equal(e->id, 3);
    raft_entry_release(e);

    e = LogGet(log, 102);
    assert_int_equal(e->id, 30);
    raft_entry_release(e);
}

static void test_log_load_entries(void **state)
{
    raft_entry_t *ety;
    Log *log = *state;

    __append_entry(log, 3);
    __append_entry(log, 30);

    assert_int_equal(LogLoadEntries(log), RR_OK);
    assert_int_equal(LogCount(log), 2);

    ety = LogGet(log, 1);
    assert_int_equal(ety->id, 3);
    assert_memory_equal(ety->data, "value3", strlen("value3"));
    raft_entry_release(ety);

    ety = LogGet(log, 2);
    assert_int_equal(ety->id, 30);
    assert_memory_equal(ety->data, "value30", strlen("value30"));
    raft_entry_release(ety);
}

static void test_log_index_rebuild(void **state)
{
    Log *log = (Log *) *state;
    LogReset(log, 100, 1);

    __append_entry(log, 3);
    __append_entry(log, 30);

    /* Delete index file */
    unlink(LOGNAME ".idx");

    /* Reopen the log */
    Log *log2 = LogOpen(LOGNAME, false);
    LogLoadEntries(log2);

    /* Invalid out of bound reads */
    assert_null(LogGet(log, 99));
    assert_null(LogGet(log, 100));
    assert_null(LogGet(log, 103));

    raft_entry_t *e = LogGet(log, 101);
    assert_int_equal(e->id, 3);
    raft_entry_release(e);

    e = LogGet(log, 102);
    assert_int_equal(e->id, 30);
    raft_entry_release(e);

    /* Close the log */
    LogFree(log2);
}

static void test_log_write_after_read(void **state)
{
    Log *log = (Log *) *state;

    __append_entry(log, 1);
    __append_entry(log, 2);

    raft_entry_t *e = LogGet(log, 1);
    assert_int_equal(e->id, 1);
    raft_entry_release(e);

    __append_entry(log, 3);
    e = LogGet(log, 3);
    assert_int_equal(e->id, 3);
    raft_entry_release(e);
}

static void test_log_fuzzer(void **state)
{
    Log *log = (Log *) *state;
    int idx = 0, i;

    for (i = 0; i < 10000; i++) {
        int new_entries = random() % 10;
        int j;
        for (j = 0; j < new_entries; j++) {
            __append_entry(log, ++idx);
        }

        if (idx > 10) {
            int del_entries = (random() % 5) + 1;
            idx = idx - del_entries;
            assert_int_equal(LogDelete(log, idx + 1), RR_OK);
        }

        for (j = 0; j < 20; j++) {
            int get_idx = (random() % (idx - 1)) + 1;
            raft_entry_t *e = LogGet(log, get_idx);
            assert_non_null(e);
            assert_int_equal(e->id, get_idx);
            raft_entry_release(e);
        }
    }
}

static void mock_notify_func(void *arg, raft_entry_t *ety, raft_index_t idx)
{
    int ety_id = ety->id;
    check_expected(ety_id);
    check_expected(idx);
}

static void test_log_delete(void **state)
{
    Log *log = (Log *) *state;

    char value1[] = "value1";
    raft_entry_t *entry1 = __make_entry_value(3, value1);
    char value2[] = "value22222";
    raft_entry_t *entry2 = __make_entry_value(20, value2);
    char value3[] = "value33333333333";
    raft_entry_t *entry3 = __make_entry_value(30, value3);

    /* Simulate post snapshot log */
    LogReset(log, 50, 1);

    /* Write entries */
    assert_int_equal(LogAppend(log, entry1), RR_OK);
    assert_int_equal(LogAppend(log, entry2), RR_OK);
    assert_int_equal(LogAppend(log, entry3), RR_OK);

    raft_entry_t *e = LogGet(log, 51);
    assert_non_null(e);
    assert_int_equal(e->id, 3);
    raft_entry_release(e);

    /* Try delete with improper values */
    assert_int_equal(LogDelete(log, 0), RR_ERROR);

    /* Delete last two elements */
    assert_int_equal(LogDelete(log, 52), RR_OK);

    /* Assert deleting a non-existing entry doesn't cause a problem. */
    assert_int_equal(LogDelete(log, 52), RR_OK);

    /* Check log sanity after delete */
    assert_int_equal(LogCount(log), 1);
    assert_int_equal(LogCurrentIdx(log), 51);
    assert_null(LogGet(log, 52));
    e = LogGet(log, 51);
    assert_non_null(e);
    assert_int_equal(e->id, 3);
    raft_entry_release(e);

    /* Re-add entries in reverse order, validate indexes are handled
     * properly.
     */

    assert_int_equal(LogAppend(log, entry3), RR_OK);
    e = LogGet(log, 52);
    assert_non_null(e);
    assert_int_equal(e->id, 30);
    raft_entry_release(e);

    assert_int_equal(LogAppend(log, entry2), RR_OK);
    e = LogGet(log, 53);
    assert_non_null(e);
    assert_int_equal(e->id, 20);
    raft_entry_release(e);

    raft_entry_release(entry1);
    raft_entry_release(entry2);
    raft_entry_release(entry3);
}

static void test_entry_cache_sanity(void **state)
{
    EntryCache *cache = EntryCacheNew(8);
    raft_entry_t *ety;
    int i;

    /* Insert 64 entries (cache grows) */
    for (i = 1; i <= 64; i++) {
        ety = raft_entry_new(0);
        ety->id = i;
        EntryCacheAppend(cache, ety, i);
        raft_entry_release(ety);
    }

    assert_int_equal(cache->size, 64);
    assert_int_equal(cache->len, 64);

    /* Get 64 entries */
    for (i = 1; i <= 64; i++) {
        ety = EntryCacheGet(cache, i);
        assert_non_null(ety);
        assert_int_equal(ety->id, i);
        raft_entry_release(ety);
    }

    EntryCacheFree(cache);
}

static void test_entry_cache_start_index_change(void **state)
{
    EntryCache *cache = EntryCacheNew(8);
    raft_entry_t *ety;

    /* Establish start_idx 1 */
    ety = raft_entry_new(0);
    ety->id = 1;
    EntryCacheAppend(cache, ety, 1);
    raft_entry_release(ety);

    assert_int_equal(cache->start_idx, 1);
    EntryCacheDeleteTail(cache, 1);
    assert_int_equal(cache->start_idx, 0);

    ety = raft_entry_new(0);
    ety->id = 10;
    EntryCacheAppend(cache, ety, 10);
    raft_entry_release(ety);

    assert_int_equal(cache->start_idx, 10);

    EntryCacheFree(cache);
}

static void test_entry_cache_delete_head(void **state)
{
    EntryCache *cache = EntryCacheNew(4);
    raft_entry_t *ety;
    int i;

    /* Fill up 5 entries */
    for (i = 1; i <= 5; i++) {
        ety = raft_entry_new(0);
        ety->id = i;
        EntryCacheAppend(cache, ety, i);
        raft_entry_release(ety);
    }

    assert_int_equal(cache->size, 8);
    assert_int_equal(cache->start, 0);
    assert_int_equal(cache->start_idx, 1);

    /* Test invalid deletes */
    assert_int_equal(EntryCacheDeleteHead(cache, 0), -1);

    /* Delete first entry */
    assert_int_equal(EntryCacheDeleteHead(cache, 2), 1);
    assert_null(EntryCacheGet(cache, 1));
    ety = EntryCacheGet(cache, 2);
    assert_int_equal(ety->id, 2);
    raft_entry_release(ety);

    assert_int_equal(cache->start, 1);
    assert_int_equal(cache->len, 4);
    assert_int_equal(cache->start_idx, 2);

    /* Delete and add 5 entries (6, 7, 8, 9, 10)*/
    for (i = 0; i < 5; i++) {
        assert_int_equal(EntryCacheDeleteHead(cache, 3 + i), 1);
        ety = raft_entry_new(0);
        ety->id = 6 + i;
        EntryCacheAppend(cache, ety, ety->id);
        raft_entry_release(ety);
    }

    assert_int_equal(cache->start_idx, 7);
    assert_int_equal(cache->start, 6);
    assert_int_equal(cache->size, 8);
    assert_int_equal(cache->len, 4);

    /* Add another 3 (11, 12, 13) */
    for (i = 11; i <= 13; i++) {
        ety = raft_entry_new(0);
        ety->id = i;
        EntryCacheAppend(cache, ety, i);
        raft_entry_release(ety);
    }

    assert_int_equal(cache->start, 6);
    assert_int_equal(cache->size, 8);
    assert_int_equal(cache->len, 7);

    /* Validate contents */
    for (i = 7; i <= 13; i++) {
        ety = EntryCacheGet(cache, i);
        assert_non_null(ety);
        assert_int_equal(ety->id, i);
        raft_entry_release(ety);
    }

    /* Delete multiple with an overlap */
    assert_int_equal(EntryCacheDeleteHead(cache, 10), 3);
    assert_int_equal(cache->len, 4);
    assert_int_equal(cache->start, 1);

    /* Validate contents after deletion */
    for (i = 10; i <= 13; i++) {
        ety = EntryCacheGet(cache, i);
        assert_non_null(ety);
        assert_int_equal(ety->id, i);
        raft_entry_release(ety);
    }

    EntryCacheFree(cache);
}

static void test_entry_cache_delete_tail(void **state)
{
    EntryCache *cache = EntryCacheNew(4);
    raft_entry_t *ety;
    int i;

    for (i = 100; i <= 103; i++) {
        ety = raft_entry_new(0);
        ety->id = i;
        EntryCacheAppend(cache, ety, i);
        raft_entry_release(ety);
    }

    assert_int_equal(cache->size, 4);
    assert_int_equal(cache->len, 4);

    /* Try invalid indexes */
    assert_int_equal(EntryCacheDeleteTail(cache, 104), -1);

    /* Delete last entry */
    assert_int_equal(EntryCacheDeleteTail(cache, 103), 1);
    assert_int_equal(cache->len, 3);
    assert_null(EntryCacheGet(cache, 103));
    ety = EntryCacheGet(cache, 102);
    assert_int_equal(ety->id, 102);
    raft_entry_release(ety);

    /* Delete all entries */
    assert_int_equal(EntryCacheDeleteTail(cache, 100), 3);
    assert_int_equal(cache->len, 0);

    /* Delete an index that precedes start_idx */
    ety = raft_entry_new(0);
    EntryCacheAppend(cache, ety, 100);
    raft_entry_release(ety);

    assert_int_equal(EntryCacheDeleteTail(cache, 1), 1);
    assert_int_equal(cache->len, 0);

    EntryCacheFree(cache);
}

static void test_entry_cache_fuzzer(void **state)
{
    EntryCache *cache = EntryCacheNew(4);
    raft_entry_t *ety;
    int i, j;
    raft_index_t first_index = 1;
    raft_index_t index = 0;

    srandom(time(NULL));
    for (i = 0; i < 100000; i++) {
        int new_entries = random() % 50;

        for (j = 0; j < new_entries; j++) {
            ety = raft_entry_new(0);
            ety->id = ++index;
            EntryCacheAppend(cache, ety, index);
            raft_entry_release(ety);
        }

        if (index > 5) {
            int del_head = random() % ((index + 1) / 2);
            int removed = EntryCacheDeleteHead(cache, del_head);
            if (removed > 0) {
                first_index += removed;
            }
        }

        if (index - first_index > 10) {
            int del_tail = random() % ((index - first_index) / 10);
            if (del_tail) {
                int removed = EntryCacheDeleteTail(cache, index - del_tail + 1);
                assert_int_equal(removed, del_tail);
                index -= removed;
            }
        }
    }

    /* verify */
    for (i = 1; i < first_index; i++) {
        assert_null(EntryCacheGet(cache, i));
    }
    for (i = first_index; i <= index; i++) {
        ety = EntryCacheGet(cache, i);
        assert_non_null(ety);
        assert_int_equal(i, ety->id);
        raft_entry_release(ety);
    }

    EntryCacheFree(cache);
}

static int cleanup_meta(void **state)
{
    unlink(LOGNAME ".meta");
    return 0;
}

static void test_meta_persistence(void **state)
{
    Metadata m;

    MetadataInit(&m);
    MetadataSetClusterConfig(&m, LOGNAME, DBID, 1002);

    assert_int_equal(MetadataRead(&m, LOGNAME), RR_ERROR);
    assert_int_equal(MetadataWrite(&m, 0xffffffff, INT32_MAX), RR_OK);
    assert_int_equal(MetadataRead(&m, LOGNAME), RR_OK);
    assert_string_equal(m.dbid, DBID);
    assert_int_equal(m.node_id, 1002);
    assert_int_equal(m.term, 0xffffffff);
    assert_int_equal(m.vote, INT32_MAX);

    assert_int_equal(MetadataWrite(&m, LONG_MAX, (int) -1), RR_OK);
    assert_int_equal(MetadataRead(&m, LOGNAME), RR_OK);
    assert_string_equal(m.dbid, DBID);
    assert_int_equal(m.node_id, 1002);
    assert_int_equal(m.term, LONG_MAX);
    assert_int_equal(m.vote, -1);

    /* Test overwrite */
    assert_int_equal(MetadataWrite(&m, 5, 5), RR_OK);
    assert_int_equal(MetadataWrite(&m, 6, 6), RR_OK);
    assert_string_equal(m.dbid, DBID);
    assert_int_equal(m.node_id, 1002);
    assert_int_equal(m.term, 6);
    assert_int_equal(m.vote, 6);

    MetadataTerm(&m);
}

const struct CMUnitTest log_tests[] = {
    cmocka_unit_test_setup_teardown(
        test_log_load_entries, setup_create_log, teardown_log),
    cmocka_unit_test_setup_teardown(
        test_log_random_access, setup_create_log, teardown_log),
    cmocka_unit_test_setup_teardown(
        test_log_random_access_with_snapshot, setup_create_log, teardown_log),
    cmocka_unit_test_setup_teardown(
        test_log_write_after_read, setup_create_log, teardown_log),
    cmocka_unit_test_setup_teardown(
        test_log_index_rebuild, setup_create_log, teardown_log),
    cmocka_unit_test_setup_teardown(
        test_log_delete, setup_create_log, teardown_log),
    cmocka_unit_test_setup_teardown(
        test_log_fuzzer, setup_create_log, teardown_log),
    cmocka_unit_test_setup_teardown(
        test_entry_cache_sanity, NULL, NULL),
    cmocka_unit_test_setup_teardown(
        test_entry_cache_start_index_change, NULL, NULL),
    cmocka_unit_test_setup_teardown(
        test_entry_cache_delete_head, NULL, NULL),
    cmocka_unit_test_setup_teardown(
        test_entry_cache_delete_tail, NULL, NULL),
    cmocka_unit_test_setup_teardown(
        test_entry_cache_fuzzer, NULL, NULL),
    cmocka_unit_test_setup_teardown(
        test_meta_persistence, cleanup_meta, cleanup_meta),
    {.test_func = NULL},
};
