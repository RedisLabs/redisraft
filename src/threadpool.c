#include "threadpool.h"

#include "redismodule.h"
#include "redisraft.h"

#include <pthread.h>
#include <stdbool.h>
#include <string.h>

/* Thread main loop */
static void *loop(void *arg)
{
    ThreadPool *pool = arg;

    while (true) {
        pthread_mutex_lock(&pool->mtx);
        while (STAILQ_EMPTY(&pool->tasks) && pool->shutdown == 0) {
            int rc = pthread_cond_wait(&pool->cond, &pool->mtx);
            RedisModule_Assert(rc == 0);
        }

        if (pool->shutdown) {
            pthread_mutex_unlock(&pool->mtx);
            return NULL;
        }

        struct ThreadPoolTask *t = STAILQ_FIRST(&pool->tasks);
        STAILQ_REMOVE_HEAD(&pool->tasks, entry);
        pthread_mutex_unlock(&pool->mtx);

        t->run(t->arg);

        RedisModule_Free(t);
    }
}

/* Initializes thread pool with `thread_count` threads. */
void ThreadPoolInit(ThreadPool *pool, int thread_count)
{
    *pool = (ThreadPool){0};

    pool->threads = RedisModule_Alloc(sizeof(*pool->threads) * thread_count);
    pool->mtx = (pthread_mutex_t) PTHREAD_MUTEX_INITIALIZER;
    STAILQ_INIT(&pool->tasks);

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
void ThreadPoolAdd(ThreadPool *pool, void *arg, void (*run)(void *arg))
{
    struct ThreadPoolTask *t = RedisModule_Alloc(sizeof(*t));

    t->arg = arg;
    t->run = run;

    pthread_mutex_lock(&pool->mtx);
    STAILQ_INSERT_TAIL(&pool->tasks, t, entry);
    pthread_cond_signal(&pool->cond);
    pthread_mutex_unlock(&pool->mtx);
}
