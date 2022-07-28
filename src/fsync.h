#ifndef REDISRAFT_FSYNC_H
#define REDISRAFT_FSYNC_H

#include "raft.h"

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>

typedef struct FsyncThreadResult {
    raft_index_t fsync_index;
    uint64_t time;
} FsyncThreadResult;

typedef struct FsyncThread {
    pthread_t id;
    pthread_cond_t cond;
    pthread_mutex_t mtx;

    bool need_fsync;
    bool running;

    int fd;
    raft_index_t requested_index;

    void (*on_complete)(void *result);

} FsyncThread;

void FsyncThreadStart(FsyncThread *th, void (*on_complete)(void *result));
void FsyncThreadAddTask(FsyncThread *th, int fd, raft_index_t requested_index);
void FsyncThreadWaitUntilCompleted(FsyncThread *th);

#endif
