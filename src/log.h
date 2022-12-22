/*
 * Copyright Redis Ltd. 2022 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef REDISRAFT_LOG_H
#define REDISRAFT_LOG_H

#include "file.h"
#include "raft.h"

#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

extern raft_log_impl_t LogImpl;

typedef struct LogPage {
    char dbid[64];              /* DB unique ID, TODO: size should be RAFT_DBID_LEN + 1, will be fixed with RR-148 */
    raft_node_id_t node_id;     /* Node ID */
    raft_term_t prev_log_term;  /* Entry term that comes just before this page. */
    raft_index_t prev_log_idx;  /* Entry index that comes just before this page. */
    raft_index_t num_entries;   /* Entries in log */
    raft_index_t index;         /* Index of last entry */
    char filename[PATH_MAX];    /* Log file name */
    char idxfilename[PATH_MAX]; /* Index file name */
    File file;                  /* Log file */
    File idxfile;               /* Index file descriptor */
    long current_crc;           /* Current running crc value for the log file */
} LogPage;

typedef struct Log {
    char dbid[64];            /* DB unique ID, TODO: size should be RAFT_DBID_LEN + 1, will be fixed with RR-148 */
    raft_node_id_t node_id;   /* Node ID */
    LogPage *pages[2];        /* Log files. Second page will be created on log compaction */
    raft_index_t fsync_index; /* Last entry index included in the latest fsync() call */
    uint64_t fsync_count;     /* Count of fsync() calls */
    uint64_t fsync_max;       /* Slowest fsync() call in microseconds */
    uint64_t fsync_total;     /* Total time fsync() calls consumed in microseconds */
} Log;

void LogInit(Log *log);
void LogTerm(Log *log);

int LogCreate(Log *log, const char *filename, const char *dbid,
              raft_node_id_t node_id, raft_term_t prev_log_term,
              raft_index_t prev_log_index);
int LogOpen(Log *log, const char *filename);

raft_node_id_t LogNodeId(Log *log);
const char *LogDbid(Log *log);

int LogAppend(Log *log, raft_entry_t *entry);
int LogLoadEntries(Log *log);
int LogSync(Log *log, bool sync);
int LogFlush(Log *log);
int LogCurrentFd(Log *log);
raft_entry_t *LogGet(Log *log, raft_index_t idx);
int LogDelete(Log *log, raft_index_t from_idx);
int LogReset(Log *log, raft_index_t index, raft_term_t term);
raft_term_t LogPrevLogTerm(Log *log);
raft_index_t LogPrevLogIndex(Log *log);
raft_index_t LogCount(Log *log);
raft_index_t LogFirstIdx(Log *log);
raft_index_t LogCurrentIdx(Log *log);
size_t LogFileSize(Log *log);
void LogArchiveFiles(Log *log);

int LogCompactionBegin(Log *log);
void LogCompactionEnd(Log *log);
bool LogCompactionStarted(Log *log);
raft_index_t LogCompactionIdx(Log *log);

#endif
