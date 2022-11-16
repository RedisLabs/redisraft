/*
 * Copyright Redis Ltd. 2022 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "metadata.h"

#include "redisraft.h"

#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>

static const char *METADATA_STR = "METADATA";
static const int METADATA_VERSION = 1;
static const int METADATA_ELEM_COUNT = 4;

static int writeLength(void *buf, size_t cap, char prefix, int len)
{
    return safesnprintf(buf, cap, "%c%d\r\n", prefix, len);
}

static int writeInteger(void *buf, size_t cap, long long val)
{
    int len = lensnprintf("%lld", val);
    return safesnprintf(buf, cap, "$%d\r\n%lld\r\n", len, val);
}

static int writeString(void *buf, size_t cap, const char *val)
{
    int len = lensnprintf("%s", val);
    return safesnprintf(buf, cap, "$%d\r\n%s\r\n", len, val);
}

static char *metadataFilename(char *buf, size_t size, const char *filename)
{
    safesnprintf(buf, size, "%s.meta", filename);
    return buf;
}

int MetadataWrite(Metadata *m, const char *filename, raft_term_t term,
                  raft_node_id_t vote)
{
    int off = 0;
    char buf[2048];
    char orig_buf[PATH_MAX];
    char tmp_file[PATH_MAX];

    off += writeLength(buf + off, sizeof(buf) - off, '*', METADATA_ELEM_COUNT);
    off += writeString(buf + off, sizeof(buf) - off, METADATA_STR);
    off += writeInteger(buf + off, sizeof(buf) - off, METADATA_VERSION);
    off += writeInteger(buf + off, sizeof(buf) - off, term);
    off += writeInteger(buf + off, sizeof(buf) - off, vote);

    char *orig_file = metadataFilename(orig_buf, sizeof(orig_buf), filename);
    safesnprintf(tmp_file, sizeof(tmp_file), "%s.tmp", orig_file);

    int fd = open(tmp_file, O_WRONLY | O_CREAT | O_TRUNC, S_IWUSR | S_IRUSR);
    if (fd < 0) {
        PANIC("open(): %s", strerror(errno));
    }

    char *data = buf;
    size_t remaining = off;

    while (remaining > 0) {
        ssize_t written = write(fd, data, remaining);
        if (written < 0) {
            PANIC("write(): %s", strerror(errno));
        }
        data += written;
        remaining -= written;
    }

    if (close(fd) != 0) {
        PANIC("close(): %s", strerror(errno));
    }

    if (rename(tmp_file, orig_file) != 0) {
        PANIC("rename(): %s", strerror(errno));
    }

    m->term = term;
    m->vote = vote;

    return RR_OK;
}

static int readItem(char *buf, size_t size, char **item)
{
    char *p = memchr(buf, '\n', size);
    if (!p || p == buf || *(p - 1) != '\r') {
        PANIC("Corrupt metadata: %s", buf);
    }

    if (item) {
        *item = buf;
    }

    return (int) (p - buf + 1);
}

static int readLength(char *buf, size_t size, char prefix, int *length)
{
    int len_bytes = readItem(buf, size, NULL);
    if (len_bytes <= 0 || buf[0] != prefix) {
        PANIC("Corrupt metadata: %s", buf);
    }

    if (!parseInt(buf + 1, NULL, length)) {
        PANIC("Corrupt metadata: %s", buf);
    }

    return len_bytes;
}

static int readString(char *buf, size_t size, char **item)
{
    int bytes, len;

    bytes = readLength(buf, size, '$', &len);
    bytes += readItem(buf + bytes, size - bytes, item);

    return bytes;
}

static int readInteger(char *buf, size_t size, long *val)
{
    int len, len_bytes;

    len_bytes = readLength(buf, size, '$', &len);
    if (!parseLong(buf + len_bytes, NULL, val)) {
        PANIC("Corrupt metadata: %s", buf);
    }

    return (int) (len_bytes + len + strlen("\r\n"));
}

int MetadataRead(Metadata *m, const char *filename)
{
    char buf[2048];
    char file[PATH_MAX];

    *m = (Metadata){
        .term = 0,
        .vote = RAFT_NODE_ID_NONE,
    };

    safesnprintf(file, sizeof(file), "%s.meta", filename);

    int fd = open(file, O_RDONLY);
    if (fd < 0) {
        if (errno != ENOENT) {
            PANIC("open(): %s", strerror(errno));
        }
        return RR_ERROR;
    }

    char *data = buf;
    size_t cap = sizeof(buf);
    size_t total_bytes = 0;
    ssize_t bytes;

    do {
        bytes = read(fd, data + total_bytes, cap);
        if (bytes < 0) {
            PANIC("read(): %s", strerror(errno));
        }
        total_bytes += bytes;
        cap -= bytes;
    } while (cap > 0 && bytes > 0);

    RedisModule_Assert((size_t) total_bytes < sizeof(buf));

    if (close(fd) != 0) {
        PANIC("close(): %s", strerror(errno));
    }

    buf[total_bytes] = '\0';

    int off = 0, len = 0;
    long version, term, vote;
    char *p = NULL;

    off += readLength(buf + off, total_bytes - off, '*', &len);
    RedisModule_Assert(len == METADATA_ELEM_COUNT);

    off += readString(buf + off, total_bytes - off, &p);
    RedisModule_Assert(strncmp(p, METADATA_STR, strlen(METADATA_STR)) == 0);

    off += readInteger(buf + off, total_bytes - off, &version);
    RedisModule_Assert(version == METADATA_VERSION);

    off += readInteger(buf + off, total_bytes - off, &term);
    RedisModule_Assert(term >= 0);
    m->term = term;

    readInteger(buf + off, total_bytes - off, &vote);
    RedisModule_Assert(vote >= INT_MIN && vote <= INT_MAX);
    m->vote = (raft_node_id_t) vote;

    return RR_OK;
}
