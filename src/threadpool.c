#include <string.h>
#include <pthread.h>
#include "redisraft.h"

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

        struct Task *t = STAILQ_FIRST(&pool->tasks);
        STAILQ_REMOVE_HEAD(&pool->tasks, entry);
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
void threadPoolAdd(ThreadPool *pool, void *arg, void (*run)(void *arg))
{
    struct Task *t = RedisModule_Alloc(sizeof(*t));

    t->arg = arg;
    t->run = run;

    pthread_mutex_lock(&pool->mtx);
    STAILQ_INSERT_TAIL(&pool->tasks, t, entry);
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

    struct Task *it, *tmp;
    STAILQ_FOREACH_SAFE(it, &pool->tasks, entry, tmp) {
        STAILQ_REMOVE(&pool->tasks, it, Task, entry);
        RedisModule_Free(it);
    }
}