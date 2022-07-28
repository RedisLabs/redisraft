#ifndef REDISRAFT_THREADPOOL_H
#define REDISRAFT_THREADPOOL_H

#include "queue.h"

#include <pthread.h>

/* This threadpool is used for DNS resolution only. */

struct ThreadPoolTask {
    STAILQ_ENTRY(ThreadPoolTask) entry;
    void *arg;
    void (*run)(void *arg);
};

typedef struct ThreadPool {
    pthread_t *threads;

    STAILQ_HEAD(tasks, ThreadPoolTask) tasks;

    pthread_mutex_t mtx;
    pthread_cond_t cond;
    int shutdown;
} ThreadPool;

void ThreadPoolInit(ThreadPool *pool, int thread_count);
void ThreadPoolAdd(ThreadPool *pool, void *arg, void (*run)(void *arg));

#endif
