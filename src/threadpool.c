/*
* Copyright Redis Ltd. 2022 - present
* Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
* the Server Side Public License v1 (SSPLv1).
*/

#include "redisraft.h"

#include <pthread.h>
#include <string.h>

/* Thread main loop */
static void *loop(void *arg)
{
    ThreadPool *pool = arg;

    while (true) {
        pthread_mutex_lock(&pool->mtx);
        while (sc_list_is_empty(&pool->tasks) && pool->shutdown == 0) {
            int rc = pthread_cond_wait(&pool->cond, &pool->mtx);
            RedisModule_Assert(rc == 0);
        }

        if (pool->shutdown) {
            pthread_mutex_unlock(&pool->mtx);
            return NULL;
        }

        struct sc_list *elem = sc_list_pop_head(&pool->tasks);
        struct Task *t = sc_list_entry(elem, struct Task, entry);

        pthread_mutex_unlock(&pool->mtx);

        t->run(t->arg);

        RedisModule_Free(t);
    }
}

/* Initializes thread pool with `thread_count` threads. */
void threadPoolInit(ThreadPool *pool, int thread_count)
{
    *pool = (ThreadPool){0};

    pool->threads = RedisModule_Alloc(sizeof(*pool->threads) * thread_count);
    pool->thread_count = thread_count;
    pool->mtx = (pthread_mutex_t) PTHREAD_MUTEX_INITIALIZER;
    sc_list_init(&pool->tasks);

    int rc = pthread_cond_init(&pool->cond, NULL);
    if (rc != 0) {
        PANIC("pthread_cond_init(): %s \n", strerror(rc));
    }

    for (int i = 0; i < thread_count; i++) {
        pthread_attr_t attr;

        rc = pthread_attr_init(&attr);
        if (rc != 0) {
            PANIC("pthread_attr_init(): %s", strerror(rc));
        }

        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

        rc = pthread_create(&pool->threads[i], &attr, loop, pool);
        if (rc != 0) {
            PANIC("pthread_create(): %s", strerror(rc));
        }

        pthread_attr_destroy(&attr);
    }
}

/* Add task to the pool. A random thread will call the callback with the arg
  * provided */
void threadPoolAdd(ThreadPool *pool, void *arg, void (*run)(void *arg))
{
    struct Task *t = RedisModule_Alloc(sizeof(*t));

    t->arg = arg;
    t->run = run;
    sc_list_init(&t->entry);

    pthread_mutex_lock(&pool->mtx);
    sc_list_add_tail(&pool->tasks, &t->entry);
    pthread_cond_signal(&pool->cond);
    pthread_mutex_unlock(&pool->mtx);
}

/* Shutdown threadpool gracefully, waiting each thread to join */
void threadPoolShutdown(ThreadPool *pool)
{
    pthread_mutex_lock(&pool->mtx);
    pool->shutdown = 1;
    pthread_cond_broadcast(&pool->cond);
    pthread_mutex_unlock(&pool->mtx);

    for (int i = 0; i < pool->thread_count; i++) {
        void *ret;
        int rc = pthread_join(pool->threads[i], &ret);
        if (rc != 0) {
            PANIC("pthread_join() : %s", strerror(rc));
        }
    }

    struct sc_list *it, *tmp;

    sc_list_foreach_safe (&pool->tasks, tmp, it) {
        struct Task *t = sc_list_entry(it, struct Task, entry);
        RedisModule_Free(t);
    }
}