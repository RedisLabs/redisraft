/*
 * Copyright Redis Ltd. 2022 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef REDISRAFT_FILE_H
#define REDISRAFT_FILE_H

#include <stdlib.h>
#include <sys/types.h>

/* File implementation with a userspace buffer.
 *
 * Main differences over the standard library FILE implementation:
 *
 * * Works with O_APPEND mode only. It is a deliberate decision to keep the
 *   implementation simple.
 * * The standard library FILE API requires extra care when you want to use it
 *   together with POSIX IO functions, e.g., need fflush() before ftruncate().
 *   This implementation tries to do these things automatically.
 * * Tracks read/write offset internally. Getting read/write offset is fast
 *   as it doesn't make a system call for them.
 * * This implementation is not thread-safe while the standard library
 *   implementation is.
 */

typedef struct File {
    int fd;         /* File descriptor */
    char *rpos;     /* In read mode, current read position of the buffer. */
    char *rend;     /* In read mode, end position of the buffer. */
    size_t roffset; /* Read offset relative to the head of the file. */
    char *wpos;     /* In write mode, current write position of the buffer. */
    size_t woffset; /* Write offset relative to the head of the file. */
    char buf[4096]; /* Userspace buffer. */
} File;

void FileInit(File *file);
int FileTerm(File *file);

int FileOpen(File *file, const char *filename, int flags);
int FileFlush(File *file);
int FileFsync(File *file);

int FileSetReadOffset(File *file, size_t offset);
size_t FileGetReadOffset(File *file);

ssize_t FileGets(File *file, void *buf, size_t cap);
ssize_t FileRead(File *file, void *buf, size_t cap);
ssize_t FileWrite(File *file, void *buf, size_t len);
size_t FileSize(File *file);
int FileTruncate(File *file, size_t len);

#endif
