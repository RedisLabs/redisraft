#ifndef REDISRAFT_LOG_H
#define REDISRAFT_LOG_H

#include "raft.h"
#include "snapshot.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#define RAFTLOG_VERSION 1

typedef struct RaftLog {
    uint32_t version;               /* Log file format version */
    raft_node_id_t node_id;         /* Id of this server */
    off_t *entry_offsets;           /* Entry positions in the log file */
    raft_index_t entry_offset_cap;  /* Capacity of entry_offsets array */
    raft_index_t num_entries;       /* Entries in log */
    raft_term_t snapshot_last_term; /* Last term included in snapshot */
    raft_index_t snapshot_last_idx; /* Last index included in snapshot */
    raft_index_t index;             /* Index of last entry */
    size_t file_size;               /* File size at the time of last write */
    const char *filename;           /* Log file name */
    FILE *file;                     /* Log file */
    raft_index_t fsync_index;       /* Last entry index included in the latest fsync() call */
} RaftLog;

struct RedisRaftCtx;
struct RedisRaftConfig;

RaftLog *RaftLogCreate(const char *filename, raft_term_t snapshot_term, raft_index_t snapshot_index, raft_node_id_t node_id);
RaftLog *RaftLogOpen(const char *filename);
void RaftLogClose(RaftLog *log);
int RaftLogAppend(RaftLog *log, raft_entry_t *entry);
int RaftLogLoadEntries(RaftLog *log, int (*callback)(void *, raft_entry_t *, raft_index_t), void *callback_arg);
int RaftLogWriteEntry(RaftLog *log, raft_entry_t *entry);
int RaftLogSync(RaftLog *log, bool sync);
raft_entry_t *RaftLogGet(RaftLog *log, raft_index_t idx);
int RaftLogDelete(RaftLog *log, raft_index_t from_idx, raft_entry_notify_f cb, void *cb_arg);
int RaftLogReset(RaftLog *log, raft_index_t index, raft_term_t term);
raft_index_t RaftLogCount(RaftLog *log);
raft_index_t RaftLogFirstIdx(RaftLog *log);
raft_index_t RaftLogCurrentIdx(RaftLog *log);
long long int RaftLogRewrite(struct RedisRaftCtx *rr, const char *filename, raft_index_t last_idx, raft_term_t last_term);

#endif
