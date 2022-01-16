/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020-2021 Redis Ltd.
 *
 * RedisRaft is licensed under the Redis Source Available License (RSAL).
 */

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <string.h>
#include "cmocka.h"

#define RedisModule_Alloc(size)             test_malloc(size)
#define RedisModule_Calloc(nmemb, size)     test_calloc(nmemb, size)
#define RedisModule_Realloc(ptr, size)      test_realloc(ptr, size)
#define RedisModule_Free(ptr)               test_free(ptr)

struct RedisModuleString;

static inline const char *mock_StringPtrLen(const struct RedisModuleString *s, size_t *len)
{
    *len = strlen((char *)s);
    return (const char *) s;
}

static inline struct RedisModuleString *mock_CreateString(const char *s, size_t len)
{
    char *buf = test_malloc(len + 1);
    memcpy(buf, s, len);
    buf[len] = '\0';
    return (struct RedisModuleString *) buf;
}

#define RedisModule_StringPtrLen(__s, __len)            mock_StringPtrLen(__s, __len)
#define RedisModule_CreateString(__ctx, __s, __len)     mock_CreateString(__s, __len)
#define RedisModule_FreeString(__ctx, __s)              test_free(__s)
#define RedisModule_MonotonicMicroseconds()             0
