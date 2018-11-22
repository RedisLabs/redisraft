#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <unistd.h>
#include <string.h>

#include "cmocka.h"

#include "../redisraft.h"

#define LOGNAME "test.log.db"
#define DBID "01234567890123456789012345678901"

static int setup_create_log(void **state)
{
    *state = RaftLogCreate(LOGNAME, DBID, 1, 0);
    assert_non_null(*state);
    return 0;
}

static int setup_create_log_idx100(void **state)
{
    *state = RaftLogCreate(LOGNAME, DBID, 1, 100);
    assert_non_null(*state);
    return 0;
}

static int teardown_log(void **state)
{
    RaftLog *log = (RaftLog *) *state;
    RaftLogClose(log);
    unlink(LOGNAME);
    return 0;
}

static int cmp_entry(raft_entry_t *e1, raft_entry_t *e2)
{
    return (e1->term == e2->term &&
            e1->type == e2->type &&
            e1->id == e2->id &&
            e1->data.len == e2->data.len &&
            !memcmp(e1->data.buf, e2->data.buf, e1->data.len));
}

#define MAX_FAKELOG 10
struct fakelog {
    int first;
    int last;
    raft_entry_t entries[MAX_FAKELOG];
};

static int fakelog_entries(struct fakelog *log)
{
    return log->last - log->first;
}

static int fakelog_cmp_entry(struct fakelog *log, int idx, raft_entry_t *e)
{
    return cmp_entry(&log->entries[log->first + idx - 1], e);
}

static void fakelog_clear(struct fakelog *log)
{
    log->first = log->last = 0;
}

static int read_callback(void *arg, LogEntryAction action, raft_entry_t *entry)
{
    struct fakelog *log = (struct fakelog *) arg;
    if (action == LA_APPEND) {
        assert_true(log->last < MAX_FAKELOG);
        log->entries[log->last++] = *entry;
    } else if (action == LA_REMOVE_HEAD) {
        assert_true(log->last > log->first);
        log->first++;
    } else if (action == LA_REMOVE_TAIL) {
        assert_true(log->last > log->first);
        log->last--;
    }
    return mock();
}

static void test_log_random_access(void **state)
{
    RaftLog *log = (RaftLog *) *state;

    char value1[] = "value1";
    raft_entry_t entry1 = {
        .term = 1, .type = 2, .id = 3, .data = { .buf = value1, .len = sizeof(value1)-1 }
    };
    char value2[] = "value2";
    raft_entry_t entry2 = {
        .term = 10, .type = 2, .id = 30, .data = { .buf = value2, .len = sizeof(value2)-1 }
    };

    /* Write entries */
    assert_int_equal(RaftLogAppend(log, &entry1), RR_OK);
    assert_int_equal(RaftLogAppend(log, &entry2), RR_OK);

    /* Invalid out of bound reads */
    assert_null(RaftLogGet(log, 0));
    assert_null(RaftLogGet(log, 3));

    raft_entry_t *e = RaftLogGet(log, 1);
    assert_int_equal(e->id, 3);
    raft_entry_release(e);

    e = RaftLogGet(log, 2);
    assert_int_equal(e->id, 30);
    raft_entry_release(e);
}

static void test_log_random_access_with_snapshot(void **state)
{
    RaftLog *log = (RaftLog *) *state;

    char value1[] = "value1";
    raft_entry_t entry1 = {
        .term = 1, .type = 2, .id = 3, .data = { .buf = value1, .len = sizeof(value1)-1 }
    };
    char value2[] = "value2";
    raft_entry_t entry2 = {
        .term = 10, .type = 2, .id = 30, .data = { .buf = value2, .len = sizeof(value2)-1 }
    };

    /* Write entries */
    assert_int_equal(RaftLogAppend(log, &entry1), RR_OK);
    assert_int_equal(RaftLogAppend(log, &entry2), RR_OK);

    /* Invalid out of bound reads */
    assert_null(RaftLogGet(log, 99));
    assert_null(RaftLogGet(log, 100));
    assert_null(RaftLogGet(log, 103));

    raft_entry_t *e = RaftLogGet(log, 101);
    assert_int_equal(e->id, 3);
    raft_entry_release(e);

    e = RaftLogGet(log, 102);
    assert_int_equal(e->id, 30);
    raft_entry_release(e);
}

static void test_basic_log_read_write(void **state)
{
    RaftLog *log = (RaftLog *) *state;
    struct fakelog fl = { 0 };

    char value1[] = "value1";
    raft_entry_t entry1 = {
        .term = 1, .type = 2, .id = 3, .data = { .buf = value1, .len = sizeof(value1)-1 }
    };
    char value2[] = "value2";
    raft_entry_t entry2 = {
        .term = 10, .type = 2, .id = 30, .data = { .buf = value2, .len = sizeof(value2)-1 }
    };

    /* Write entries */
    assert_int_equal(RaftLogAppend(log, &entry1), RR_OK);
    assert_int_equal(RaftLogAppend(log, &entry2), RR_OK);

    /* Load entries */
    will_return_always(read_callback, 0);
    assert_int_equal(RaftLogLoadEntries(log, &read_callback, (void *) &fl), 2);
    assert_int_equal(fakelog_entries(&fl), 2);
    assert_true(!fakelog_cmp_entry(&fl, 1, &entry1));
    assert_true(!fakelog_cmp_entry(&fl, 2, &entry2));
}

static void test_log_index_rebuild(void **state)
{
    RaftLog *log = (RaftLog *) *state;

    char value1[] = "value1";
    raft_entry_t entry1 = {
        .term = 1, .type = 2, .id = 3, .data = { .buf = value1, .len = sizeof(value1)-1 }
    };
    char value2[] = "value2";
    raft_entry_t entry2 = {
        .term = 10, .type = 2, .id = 30, .data = { .buf = value2, .len = sizeof(value2)-1 }
    };

    /* Write entries */
    assert_int_equal(RaftLogAppend(log, &entry1), RR_OK);
    assert_int_equal(RaftLogAppend(log, &entry2), RR_OK);

    /* Delete index file */
    unlink(LOGNAME ".idx");

    /* Reopen the log */
    RaftLog *log2 = RaftLogOpen(LOGNAME);
    RaftLogLoadEntries(log2, NULL, NULL);

    /* Invalid out of bound reads */
    assert_null(RaftLogGet(log, 99));
    assert_null(RaftLogGet(log, 100));
    assert_null(RaftLogGet(log, 103));

    raft_entry_t *e = RaftLogGet(log, 101);
    assert_int_equal(e->id, 3);
    raft_entry_release(e);

    e = RaftLogGet(log, 102);
    assert_int_equal(e->id, 30);
    raft_entry_release(e);

    /* Close the log */
    RaftLogClose(log2);
}


static void test_log_remove_head(void **state)
{
    struct fakelog fl = { 0 };

    RaftLog *log = (RaftLog *) *state;
    char value1[] = "value1";
    raft_entry_t entry1 = {
        .term = 1, .type = 2, .id = 3, .data = { .buf = value1, .len = sizeof(value1)-1 }
    };

    char value2[] = "value2";
    raft_entry_t entry2 = {
        .term = 1, .type = 2, .id = 4, .data = { .buf = value2, .len = sizeof(value2)-1 }
    };

    /* Create log */
    assert_int_equal(RaftLogAppend(log, &entry1), RR_OK);
    assert_int_equal(RaftLogAppend(log, &entry2), RR_OK);

    /* Remove first */
    assert_int_equal(RaftLogRemoveHead(log), RR_OK);
    will_return_always(read_callback, 0);
    assert_int_equal(RaftLogLoadEntries(log, &read_callback, &fl), 1);
    assert_int_equal(fakelog_entries(&fl), 1);
    assert_true(!fakelog_cmp_entry(&fl, 1, &entry2));

    /* Remove last */
    fakelog_clear(&fl);
    assert_int_equal(RaftLogRemoveHead(log), RR_OK);
    assert_int_equal(RaftLogLoadEntries(log, &read_callback, (void *)&fl), 0);
    assert_int_equal(fakelog_entries(&fl), 0);

    /* No more */
    assert_int_equal(RaftLogRemoveHead(log), RR_ERROR);
}

static void test_log_remove_tail(void **state)
{
    struct fakelog fl = { 0 };

    RaftLog *log = (RaftLog *) *state;
    char value1[] = "value1";
    raft_entry_t entry1 = {
        .term = 1, .type = 2, .id = 3, .data = { .buf = value1, .len = sizeof(value1)-1 }
    };

    char value2[] = "value2";
    raft_entry_t entry2 = {
        .term = 1, .type = 2, .id = 4, .data = { .buf = value2, .len = sizeof(value2)-1 }
    };

    /* Create log */
    assert_int_equal(RaftLogAppend(log, &entry1), RR_OK);
    assert_int_equal(RaftLogAppend(log, &entry2), RR_OK);

    /* Remove tail */
    assert_int_equal(RaftLogRemoveTail(log), RR_OK);
    will_return_always(read_callback, 0);
    assert_int_equal(RaftLogLoadEntries(log, &read_callback, (void *)&fl), 1);
    assert_int_equal(fakelog_entries(&fl), 1);
    assert_true(!fakelog_cmp_entry(&fl, 1, &entry1));

    /* Remove last entry */
    fakelog_clear(&fl);
    assert_int_equal(RaftLogRemoveTail(log), RR_OK);
    assert_int_equal(RaftLogLoadEntries(log, &read_callback, (void *)&fl), 0);
    assert_int_equal(fakelog_entries(&fl), 0);

    /* No more */
    assert_int_equal(RaftLogRemoveTail(log), RR_ERROR);
}

static void test_log_remove_head_and_tail(void **state)
{
    struct fakelog fl = { 0 };

    RaftLog *log = (RaftLog *) *state;
    char value1[] = "value1";
    raft_entry_t entry1 = {
        .term = 1, .type = 2, .id = 3, .data = { .buf = value1, .len = sizeof(value1)-1 }
    };

    char value2[] = "value2";
    raft_entry_t entry2 = {
        .term = 1, .type = 2, .id = 4, .data = { .buf = value2, .len = sizeof(value2)-1 }
    };

    char value3[] = "value3";
    raft_entry_t entry3 = {
        .term = 1, .type = 2, .id = 5, .data = { .buf = value3, .len = sizeof(value3)-1 }
    };

    /* Create log */
    assert_int_equal(RaftLogAppend(log, &entry1), RR_OK);
    assert_int_equal(RaftLogAppend(log, &entry2), RR_OK);
    assert_int_equal(RaftLogAppend(log, &entry3), RR_OK);

    /* Remove head and tail */
    assert_int_equal(RaftLogRemoveHead(log), RR_OK);
    assert_int_equal(RaftLogRemoveTail(log), RR_OK);
    will_return_always(read_callback, 0);
    assert_int_equal(RaftLogLoadEntries(log, &read_callback, (void *)&fl), 1);
    assert_int_equal(fakelog_entries(&fl), 1);
    assert_true(!fakelog_cmp_entry(&fl, 1, &entry2));

    /* Remove tail */
    fakelog_clear(&fl);
    assert_int_equal(RaftLogRemoveTail(log), RR_OK);
    assert_int_equal(RaftLogLoadEntries(log, &read_callback, (void *)&fl), 0);
    assert_int_equal(fakelog_entries(&fl), 0);
}


const struct CMUnitTest log_tests[] = {
    cmocka_unit_test_setup_teardown(
            test_basic_log_read_write, setup_create_log, teardown_log),
    cmocka_unit_test_setup_teardown(
            test_log_random_access, setup_create_log, teardown_log),
    cmocka_unit_test_setup_teardown(
            test_log_random_access_with_snapshot, setup_create_log_idx100, teardown_log),
    cmocka_unit_test_setup_teardown(
            test_log_index_rebuild, setup_create_log_idx100, teardown_log),
    cmocka_unit_test_setup_teardown(
            test_log_remove_head, setup_create_log, teardown_log),
    cmocka_unit_test_setup_teardown(
            test_log_remove_tail, setup_create_log, teardown_log),
    cmocka_unit_test_setup_teardown(
            test_log_remove_head_and_tail, setup_create_log, teardown_log),
    { .test_func = NULL }
};
