#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <unistd.h>
#include <stdio.h>

#include "cmocka.h"

extern struct CMUnitTest log_tests[];

int tests_count(struct CMUnitTest *tests)
{
    int count = 0;

    while (tests[count].test_func != NULL) count++;
    return count;
}

/* --------------------------- Redis Module API Stubs --------------------------- */

extern FILE *redis_raft_logfile;

int main(int argc, char *argv[])
{
    redis_raft_logfile = stderr;

    return _cmocka_run_group_tests(
            "log", log_tests, tests_count(log_tests), NULL, NULL);
}
