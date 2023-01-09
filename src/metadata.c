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
static const int METADATA_ELEM_COUNT = 6;

static char *metadataFilename(char *buf, size_t size, const char *filename)
{
    safesnprintf(buf, size, "%s.meta", filename);
    return buf;
}

void MetadataInit(Metadata *m)
{
    *m = (Metadata){
        .node_id = RAFT_NODE_ID_NONE,
        .dbid = "",
        .term = 0,
        .vote = RAFT_NODE_ID_NONE,
    };
}

void MetadataTerm(Metadata *m)
{
    RedisModule_Free(m->filename);
}

void MetadataSetClusterConfig(Metadata *m, const char *filename, char *dbid,
                              raft_node_id_t node_id)
{
    char buf[PATH_MAX];

    RedisModule_Free(m->filename);
    metadataFilename(buf, sizeof(buf), filename);
    m->filename = RedisModule_Strdup(buf);

    RedisModule_Assert(strlen(dbid) == RAFT_DBID_LEN);
    memcpy(m->dbid, dbid, RAFT_DBID_LEN);
    m->dbid[RAFT_DBID_LEN] = '\0';

    m->node_id = node_id;
}

void MetadataArchiveFile(Metadata *m)
{
    char buf[PATH_MAX];

    safesnprintf(buf, sizeof(buf), "%s.%d.bak", m->filename, m->node_id);
    rename(m->filename, buf);
}

int MetadataWrite(Metadata *m, raft_term_t term, raft_node_id_t vote)
{
    char buf[2048], tmp[PATH_MAX];
    ssize_t len;
    char *pos = buf;
    char *end = buf + sizeof(buf);
    File f;

    pos += multibulkWriteLen(pos, end - pos, '*', METADATA_ELEM_COUNT);
    pos += multibulkWriteStr(pos, end - pos, METADATA_STR);
    pos += multibulkWriteInt(pos, end - pos, METADATA_VERSION);
    pos += multibulkWriteStr(pos, end - pos, m->dbid);
    pos += multibulkWriteInt(pos, end - pos, m->node_id);
    pos += multibulkWriteLong(pos, end - pos, term);
    pos += multibulkWriteInt(pos, end - pos, vote);
    len = pos - buf;

    safesnprintf(tmp, sizeof(tmp), "%s.tmp", m->filename);

    FileInit(&f);

    if (FileOpen(&f, tmp, O_WRONLY | O_CREAT | O_TRUNC | O_APPEND) != RR_OK) {
        PANIC("FileOpen(): %s", strerror(errno));
    }

    if (FileWrite(&f, buf, len) != len) {
        PANIC("FileWrite(): %s", strerror(errno));
    }

    if (FileTerm(&f) != RR_OK) {
        PANIC("FileTerm(): %s", strerror(errno));
    }

    if (fsyncFileAt(tmp) != RR_OK ||
        syncRename(tmp, m->filename) != RR_OK) {
        return RR_ERROR;
    }

    m->term = term;
    m->vote = vote;

    return RR_OK;
}

int MetadataRead(Metadata *m, const char *filename)
{
    char buf[PATH_MAX];
    char str[128] = {0};
    char dbid[64] = {0};
    int version, elem_count;
    raft_term_t term;
    raft_node_id_t vote, node_id;
    File f;

    metadataFilename(buf, sizeof(buf), filename);
    FileInit(&f);

    if (FileOpen(&f, buf, O_RDONLY) != RR_OK) {
        if (errno != ENOENT) {
            PANIC("FileOpen(): %s", strerror(errno));
        }

        FileTerm(&f);
        return RR_ERROR;
    }

    if (!multibulkReadLen(&f, '*', &elem_count) ||
        !multibulkReadStr(&f, str, sizeof(str)) ||
        !multibulkReadInt(&f, &version) ||
        !multibulkReadStr(&f, dbid, sizeof(dbid)) ||
        !multibulkReadInt(&f, &node_id) ||
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
    RedisModule_Assert(strlen(dbid) == RAFT_DBID_LEN);

    MetadataSetClusterConfig(m, filename, dbid, node_id);
    m->term = term;
    m->vote = vote;

    return RR_OK;
}
