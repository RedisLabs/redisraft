/*
 * Copyright Redis Ltd. 2020 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "raft.h"

#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>

#include "cmocka.h"

extern struct CMUnitTest file_tests[];
extern struct CMUnitTest log_tests[];
extern struct CMUnitTest util_tests[];
extern struct CMUnitTest serialization_tests[];

/* Redis symbols to keep linker happy */
void *rdbLoad;
void *rdbSave;

int tests_count(struct CMUnitTest *tests)
{
    int count = 0;

    while (tests[count].test_func != NULL) {
        count++;
    }
    return count;
}

/* --------------------------- Redis Module API Stubs --------------------------- */

extern FILE *redis_raft_logfile;

static void *__raft_malloc_stub(size_t size)
{
    return test_malloc(size);
}

static void __raft_free_stub(void *ptr)
{
    test_free(ptr);
}

static void *__raft_realloc_stub(void *ptr, size_t size)
{
    return test_realloc(ptr, size);
}

static void *__raft_calloc_stub(size_t nmemb, size_t size)
{
    return test_calloc(nmemb, size);
}

int main(int argc, char *argv[])
{
    raft_set_heap_functions(__raft_malloc_stub, __raft_calloc_stub,
                            __raft_realloc_stub, __raft_free_stub);

    return _cmocka_run_group_tests(
               "file", file_tests, tests_count(file_tests), NULL, NULL) ||
           _cmocka_run_group_tests(
               "log", log_tests, tests_count(log_tests), NULL, NULL) ||
           _cmocka_run_group_tests(
               "util", util_tests, tests_count(util_tests), NULL, NULL) ||
           _cmocka_run_group_tests(
               "serialization", serialization_tests, tests_count(serialization_tests), NULL, NULL);
}
