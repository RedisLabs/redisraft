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
    *state = RaftLogCreate(LOGNAME, DBID);
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
    assert_true(RaftLogAppend(log, &entry1));
    assert_true(RaftLogAppend(log, &entry2));

    /* Load entries */
    will_return_always(read_callback, 0);
    assert_int_equal(RaftLogLoadEntries(log, &read_callback, (void *) &fl), 2);
    assert_int_equal(fakelog_entries(&fl), 2);
    assert_true(!fakelog_cmp_entry(&fl, 1, &entry1));
    assert_true(!fakelog_cmp_entry(&fl, 2, &entry2));
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
    assert_true(RaftLogAppend(log, &entry1));
    assert_true(RaftLogAppend(log, &entry2));

    /* Remove first */
    assert_true(RaftLogRemoveHead(log));
    will_return_always(read_callback, 0);
    assert_int_equal(RaftLogLoadEntries(log, &read_callback, &fl), 1);
    assert_int_equal(fakelog_entries(&fl), 1);
    assert_true(!fakelog_cmp_entry(&fl, 1, &entry2));

    /* Remove last */
    fakelog_clear(&fl);
    assert_true(RaftLogRemoveHead(log));
    assert_int_equal(RaftLogLoadEntries(log, &read_callback, (void *)&fl), 0);
    assert_int_equal(fakelog_entries(&fl), 0);

    /* No more */
    assert_false(RaftLogRemoveHead(log));
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
    assert_true(RaftLogAppend(log, &entry1));
    assert_true(RaftLogAppend(log, &entry2));

    /* Remove tail */
    assert_true(RaftLogRemoveTail(log));
    will_return_always(read_callback, 0);
    assert_int_equal(RaftLogLoadEntries(log, &read_callback, (void *)&fl), 1);
    assert_int_equal(fakelog_entries(&fl), 1);
    assert_true(!fakelog_cmp_entry(&fl, 1, &entry1));

    /* Remove last entry */
    fakelog_clear(&fl);
    assert_true(RaftLogRemoveTail(log));
    assert_int_equal(RaftLogLoadEntries(log, &read_callback, (void *)&fl), 0);
    assert_int_equal(fakelog_entries(&fl), 0);

    /* No more */
    assert_false(RaftLogRemoveTail(log));
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
    assert_true(RaftLogAppend(log, &entry1));
    assert_true(RaftLogAppend(log, &entry2));
    assert_true(RaftLogAppend(log, &entry3));

    /* Remove head and tail */
    assert_true(RaftLogRemoveHead(log));
    assert_true(RaftLogRemoveTail(log));
    will_return_always(read_callback, 0);
    assert_int_equal(RaftLogLoadEntries(log, &read_callback, (void *)&fl), 1);
    assert_int_equal(fakelog_entries(&fl), 1);
    assert_true(!fakelog_cmp_entry(&fl, 1, &entry2));

    /* Remove tail */
    fakelog_clear(&fl);
    assert_true(RaftLogRemoveTail(log));
    assert_int_equal(RaftLogLoadEntries(log, &read_callback, (void *)&fl), 0);
    assert_int_equal(fakelog_entries(&fl), 0);
}


const struct CMUnitTest log_tests[] = {
    cmocka_unit_test_setup_teardown(
            test_basic_log_read_write, setup_create_log, teardown_log),
    cmocka_unit_test_setup_teardown(
            test_log_remove_head, setup_create_log, teardown_log),
    cmocka_unit_test_setup_teardown(
            test_log_remove_tail, setup_create_log, teardown_log),
    cmocka_unit_test_setup_teardown(
            test_log_remove_head_and_tail, setup_create_log, teardown_log),
    { .test_func = NULL }
};
