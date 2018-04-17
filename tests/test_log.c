#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <unistd.h>
#include <string.h>

#include "cmocka.h"

#include "../redisraft.h"

#define LOGNAME "test.log.db"

static int setup_create_log(void **state)
{
    *state = RaftLogCreate(LOGNAME, 1);
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

static int __check_entry(const long unsigned int a, const long unsigned int b)
{
    raft_entry_t *e1 = (raft_entry_t *) a;
    raft_entry_t *e2 = (raft_entry_t *) b;

    return (e1->term == e2->term &&
            e1->type == e2->type &&
            e1->id == e2->id &&
            e1->data.len == e2->data.len &&
            !memcmp(e1->data.buf, e2->data.buf, e1->data.len));
}

static int read_callback(void **arg, raft_entry_t *entry)
{
    check_expected(entry);
    test_free(entry->data.buf);
    return mock();
}

static void test_basic_log_read_write(void **state)
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
    assert_true(RaftLogAppend(log, &entry1));
    assert_true(RaftLogAppend(log, &entry2));

    /* Load entries */
    will_return_always(read_callback, 0);
    expect_check(read_callback, entry, __check_entry, &entry1);
    expect_check(read_callback, entry, __check_entry, &entry2);
    assert_int_equal(RaftLogLoadEntries(log, &read_callback, (void *) 0x1234), 2);
}

static void test_log_remove_head(void **state)
{
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
    expect_check(read_callback, entry, __check_entry, &entry2);
    assert_int_equal(RaftLogLoadEntries(log, &read_callback, (void *)0x1234), 1);

    /* Remove last */
    assert_true(RaftLogRemoveHead(log));
    assert_int_equal(RaftLogLoadEntries(log, &read_callback, (void *)0x1234), 0);
    assert_int_equal(lseek(log->fd, 0, SEEK_END), sizeof(RaftLogHeader));

    /* No more */
    assert_false(RaftLogRemoveHead(log));
}

static void test_log_remove_tail(void **state)
{
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
    expect_check(read_callback, entry, __check_entry, &entry1);
    assert_int_equal(RaftLogLoadEntries(log, &read_callback, (void *)0x1234), 1);

    /* Remove last entry */
    assert_true(RaftLogRemoveTail(log));
    assert_int_equal(RaftLogLoadEntries(log, &read_callback, (void *)0x1234), 0);

    /* No more */
    assert_false(RaftLogRemoveTail(log));
    assert_int_equal(lseek(log->fd, 0, SEEK_END), sizeof(RaftLogHeader));
}

static void test_log_remove_head_and_tail(void **state)
{
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
    expect_check(read_callback, entry, __check_entry, &entry2);
    assert_int_equal(RaftLogLoadEntries(log, &read_callback, (void *)0x1234), 1);

    /* Remove tail */
    assert_true(RaftLogRemoveTail(log));
    assert_int_equal(RaftLogLoadEntries(log, &read_callback, (void *)0x1234), 0);
    assert_int_equal(lseek(log->fd, 0, SEEK_END), sizeof(RaftLogHeader));
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
