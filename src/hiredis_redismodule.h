/*
 * Copyright Redis Ltd. 2022 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef __HIREDIS_REDISMODULE_H__
#define __HIREDIS_REDISMODULE_H__

#include "redisraft.h"

#include "hiredis/async.h"
#include "hiredis/hiredis.h"

#include <sys/types.h>

typedef struct redisAeEvents {
    redisAsyncContext *context;
    int fd;
    int reading, writing;
} redisAeEvents;

static void redisModuleReadEvent(int fd, void *privdata, int mask)
{
    ((void) fd);
    ((void) mask);

    redisAeEvents *e = (redisAeEvents *) privdata;
    redisAsyncHandleRead(e->context);
}

static void redisModuleWriteEvent(int fd, void *privdata, int mask)
{
    ((void) fd);
    ((void) mask);

    redisAeEvents *e = (redisAeEvents *) privdata;
    redisAsyncHandleWrite(e->context);
}

static void redisModuleAddRead(void *privdata)
{
    redisAeEvents *e = (redisAeEvents *) privdata;
    if (!e->reading) {
        e->reading = 1;
        RedisModule_EventLoopAdd(e->fd, REDISMODULE_EVENTLOOP_READABLE, redisModuleReadEvent, e);
    }
}

static void redisModuleDelRead(void *privdata)
{
    redisAeEvents *e = (redisAeEvents *) privdata;
    if (e->reading) {
        e->reading = 0;
        RedisModule_EventLoopDel(e->fd, REDISMODULE_EVENTLOOP_READABLE);
    }
}

static void redisModuleAddWrite(void *privdata)
{
    redisAeEvents *e = (redisAeEvents *) privdata;
    if (!e->writing) {
        e->writing = 1;
        RedisModule_EventLoopAdd(e->fd, REDISMODULE_EVENTLOOP_WRITABLE, redisModuleWriteEvent, e);
    }
}

static void redisModuleDelWrite(void *privdata)
{
    redisAeEvents *e = (redisAeEvents *) privdata;
    if (e->writing) {
        e->writing = 0;
        RedisModule_EventLoopDel(e->fd, REDISMODULE_EVENTLOOP_WRITABLE);
    }
}

static void redisModuleCleanup(void *privdata)
{
    redisAeEvents *e = (redisAeEvents *) privdata;
    redisModuleDelRead(privdata);
    redisModuleDelWrite(privdata);
    hi_free(e);
}

static int redisModuleAttach(redisAsyncContext *ac)
{
    redisContext *c = &(ac->c);
    redisAeEvents *e;

    /* Nothing should be attached when something is already attached */
    if (ac->ev.data != NULL)
        return REDIS_ERR;

    /* Create container for context and r/w events */
    e = (redisAeEvents *) hi_malloc(sizeof(*e));
    if (e == NULL)
        return REDIS_ERR;

    e->context = ac;
    e->fd = c->fd;
    e->reading = e->writing = 0;

    /* Register functions to start/stop listening for events */
    ac->ev.addRead = redisModuleAddRead;
    ac->ev.delRead = redisModuleDelRead;
    ac->ev.addWrite = redisModuleAddWrite;
    ac->ev.delWrite = redisModuleDelWrite;
    ac->ev.cleanup = redisModuleCleanup;
    ac->ev.data = e;

    return REDIS_OK;
}

#endif
