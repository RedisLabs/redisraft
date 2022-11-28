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

static char *metadataFilename(char *buf, size_t size, const char *filename)
{
    safesnprintf(buf, size, "%s.meta", filename);
    return buf;
}

int MetadataWrite(Metadata *m, const char *filename, raft_term_t term,
                  raft_node_id_t vote)
{
    char buf[2048], orig[PATH_MAX], tmp[PATH_MAX];
    int off = 0;
    File f;

    off += multibulkWriteLen(buf + off, sizeof(buf) - off, '*', METADATA_ELEM_COUNT);
    off += multibulkWriteStr(buf + off, sizeof(buf) - off, METADATA_STR);
    off += multibulkWriteInt(buf + off, sizeof(buf) - off, METADATA_VERSION);
    off += multibulkWriteLong(buf + off, sizeof(buf) - off, term);
    off += multibulkWriteInt(buf + off, sizeof(buf) - off, vote);

    metadataFilename(orig, sizeof(orig), filename);
    safesnprintf(tmp, sizeof(tmp), "%s.tmp", orig);

    FileInit(&f);

    if (FileOpen(&f, tmp, O_WRONLY | O_CREAT | O_TRUNC | O_APPEND) != RR_OK) {
        PANIC("FileOpen(): %s", strerror(errno));
    }

    if (FileWrite(&f, buf, off) != off) {
        PANIC("FileWrite(): %s", strerror(errno));
    }

    if (FileTerm(&f) != RR_OK) {
        PANIC("FileTerm(): %s", strerror(errno));
    }

    if (rename(tmp, orig) != 0) {
        PANIC("rename(): %s", strerror(errno));
    }

    m->term = term;
    m->vote = vote;

    return RR_OK;
}

int MetadataRead(Metadata *m, const char *filename)
{
    char buf[PATH_MAX];
    char str[128];
    int version, elem_count;
    raft_term_t term;
    raft_node_id_t vote;
    File f;

    *m = (Metadata){
        .term = 0,
        .vote = RAFT_NODE_ID_NONE,
    };

    metadataFilename(buf, sizeof(buf), filename);
    FileInit(&f);

    if (FileOpen(&f, buf, O_RDONLY) != RR_OK) {
        if (errno != ENOENT) {
            PANIC("FileOpen(): %s", strerror(errno));
        }
        return RR_ERROR;
    }

    if (!multibulkReadLen(&f, '*', &elem_count) ||
        !multibulkReadStr(&f, str, sizeof(str)) ||
        !multibulkReadInt(&f, &version) ||
        !multibulkReadLong(&f, &term) ||
        !multibulkReadInt(&f, &vote)) {
        PANIC("failed to read metadata file!");
    }

    if (FileTerm(&f) != RR_OK) {
        PANIC("FileTerm(): %s", strerror(errno));
    }

    RedisModule_Assert(elem_count == METADATA_ELEM_COUNT);
    RedisModule_Assert(strncmp(str, METADATA_STR, strlen(METADATA_STR)) == 0);
    RedisModule_Assert(version == METADATA_VERSION);

    m->term = term;
    m->vote = vote;

    return RR_OK;
}
