/*
 * Copyright Redis Ltd. 2020 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef REDISRAFT_LOG_UTILS_H
#define REDISRAFT_LOG_UTILS_H

#include "log.h"

int truncateFiles(Log *log, size_t offset, size_t idxoffset);
int writeLength(void *buf, size_t cap, char prefix, long len);
bool readLength(File *fp, char type, int *length);
int writeString(void *buf, size_t cap, const char *val);
bool readItem(File *fp, char *buf, size_t size);
int writeLong(void *buf, size_t cap, long val);
bool readLong(File *fp, long *value);
bool readInt(File *fp, int *value);

#endif //REDISRAFT_LOG_UTILS_H
