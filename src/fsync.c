/*
 * Copyright Redis Ltd. 2022 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "redisraft.h"

#include <pthread.h>
#include <string.h>

/* Add fsync task. requested_index should be the latest entry index in the file.
 * This index will be reported on `on_complete` callback.
 *
 * If you call this function many times while fsyncThread is in fsync(), only
 * the latest requested_index will be reported. This is some sort of batching.
 * We'll call fsync() once and report the latest index that we called fsync()
 * for.
 */
void fsyncThreadAddTask(FsyncThread *th, int fd, raft_index_t requested_index)
{
    int rc;

    pthread_mutex_lock(&th->mtx);

    th->requested_index = requested_index;
    th->fd = fd;
    th->need_fsync = true;
    th->running = true;

    rc = pthread_cond_signal(&th->cond);
    RedisModule_Assert(rc == 0);

    pthread_mutex_unlock(&th->mtx);
}

/* Waits fsync thread until it completes fsync() call */
void fsyncThreadWaitUntilCompleted(FsyncThread *th)
{
    pthread_mutex_lock(&th->mtx);
    while (th->running == 1) {
        pthread_cond_wait(&th->cond, &th->mtx);
    }
    pthread_mutex_unlock(&th->mtx);
}

static void *fsyncLoop(void *arg)
{
    int rc, fd;
    raft_index_t request_idx;
    FsyncThread *th = arg;

    while (1) {
        pthread_mutex_lock(&th->mtx);
        while (!th->need_fsync) {
            rc = pthread_cond_wait(&th->cond, &th->mtx);
            RedisModule_Assert(rc == 0);
        }

        request_idx = th->requested_index;
        fd = th->fd;
        th->need_fsync = false;

        pthread_mutex_unlock(&th->mtx);

        uint64_t begin = RedisModule_MonotonicMicroseconds();
        rc = fsyncFile(fd);
        if (rc != RR_OK) {
            PANIC("fsync(): %s \n", strerror(errno));
        }

        uint64_t time = (RedisModule_MonotonicMicroseconds() - begin);

        pthread_mutex_lock(&th->mtx);

        if (!th->need_fsync) {
            th->running = false;
            pthread_cond_signal(&th->cond);
        }
        pthread_mutex_unlock(&th->mtx);

        FsyncThreadResult *rs = RedisModule_Alloc(sizeof(*rs));
        rs->time = time;
        rs->fsync_index = request_idx;
        /* Wake up Redis event loop */
        RedisModule_EventLoopAddOneShot(th->on_complete, rs);
    }
}

/* Initializes and starts fsync thread, on_complete() callback will be
 * called from Redis main thread after each fsync() operation.
 *
 * `result` parameter will point to a FsyncThreadResult object:
 *
 * typedef struct FsyncThreadResult {
 *    raft_index_t fsync_index;  // index parameter passed in fsyncThreadAddTask()
 *    uint64_t time; // Time fsync() took in microseconds
 * } FsyncThreadResult;
 *
 * User should free() the `result` object in on_complete() callback.
 */
void fsyncThreadStart(FsyncThread *th, void (*on_complete)(void *result))
{
    int rc;
    pthread_attr_t attr;

    *th = (FsyncThread){
        .mtx = (pthread_mutex_t) PTHREAD_MUTEX_INITIALIZER,
        .on_complete = on_complete,
    };

    rc = pthread_cond_init(&th->cond, NULL);
    if (rc != 0) {
        PANIC("pthread_cond_init(): %s \n", strerror(rc));
    }

    rc = pthread_attr_init(&attr);
    if (rc != 0) {
        PANIC("pthread_attr_init(): %s \n", strerror(rc));
    }

    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    rc = pthread_create(&th->id, &attr, fsyncLoop, th);
    if (rc != 0) {
        PANIC("pthread_create(): %s \n", strerror(rc));
    }

    pthread_attr_destroy(&attr);
}
