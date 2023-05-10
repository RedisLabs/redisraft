#ifndef REDISRAFT_TEST_H
#define REDISRAFT_TEST_H

#define test_run(fn)                                                       \
    do {                                                                   \
        struct timespec ts;                                                \
        unsigned long long start, end;                                     \
        printf("[ Running ] %s \n", #fn);                                  \
                                                                           \
        clock_gettime(CLOCK_MONOTONIC, &ts);                               \
        start = ts.tv_sec * (unsigned long long) 1000000000 + ts.tv_nsec;  \
                                                                           \
        fn();                                                              \
                                                                           \
        clock_gettime(CLOCK_MONOTONIC, &ts);                               \
        end = ts.tv_sec * (unsigned long long) 1000000000 + ts.tv_nsec;    \
                                                                           \
        printf("[ Passed  ] %s in %llu nanoseconds \n", #fn, end - start); \
    } while (0);

#endif
