/*
 * Copyright Redis Ltd. 2020 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "log.h"

#include "entrycache.h"
#include "file.h"
#include "log_utils.h"

#include "common/crc16.h"

#include <assert.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

static const int ENTRY_CACHE_INIT_SIZE = 512;
static const int ENTRY_ELEM_COUNT = 6;
static const char *ENTRY_STR = "ENTRY";

static const int RAFTLOG_VERSION = 1;
static const int RAFTLOG_ELEM_COUNT = 7;
static const char *RAFTLOG_STR = "RAFTLOG";

#define RAFTLOG_TRACE(fmt, ...) TRACE_MODULE(RAFTLOG, "<raftlog> " fmt, ##__VA_ARGS__)

static ssize_t generateHeader(Log *log, char *buf, size_t buf_len)
{
    size_t off = 0;

    off += writeLength(buf + off, buf_len - off, '*', RAFTLOG_ELEM_COUNT);
    off += writeString(buf + off, buf_len - off, RAFTLOG_STR);
    off += writeLong(buf + off, buf_len - off, RAFTLOG_VERSION);
    off += writeString(buf + off, buf_len - off, log->dbid);
    off += writeLong(buf + off, buf_len - off, log->node_id);
    off += writeLong(buf + off, buf_len - off, log->snapshot_last_term);
    off += writeLong(buf + off, buf_len - off, log->snapshot_last_idx);

    return off;
}

static int writeHeader(Log *log)
{
    char buf[1024];

    ssize_t off = generateHeader(log, buf, sizeof(buf));
    /* add crc to header */
    long crc = crc16_ccitt(0, buf, off);
    off += writeLong(buf + off, sizeof(buf) - off, crc);

    if (truncateFiles(log, 0, 0) != RR_OK ||
        FileWrite(&log->file, buf, off) != off ||
        LogSync(log, true) != RR_OK) {

        /* Try to delete files just in case there was a partial write. */
        truncateFiles(log, 0, 0);
        return RR_ERROR;
    }

    /* update running crc to be the crc of entire header, including crc */
    log->current_crc = crc16_ccitt(0, buf, off);
    return RR_OK;
}

static int readHeader(Log *log, long *read_crc)
{
    char str[64] = {0};
    char dbid[64] = {0};
    int num_elements;
    long version;

    if (FileSetReadOffset(&log->file, 0) != RR_OK) {
        return RR_ERROR;
    }

    if (!multibulkReadLen(&log->file, '*', &num_elements) ||
        !multibulkReadStr(&log->file, str, sizeof(str)) ||
        !multibulkReadLong(&log->file, &version) ||
        !multibulkReadStr(&log->file, dbid, sizeof(dbid))) {
        return RR_ERROR;
    }

    /* Validate */
    if (num_elements != RAFTLOG_ELEM_COUNT ||
        strncmp(RAFTLOG_STR, str, strlen(RAFTLOG_STR)) != 0 ||
        version != RAFTLOG_VERSION ||
        strlen(dbid) != RAFT_DBID_LEN) {
        return RR_ERROR;
    }

    raft_node_id_t node_id;
    raft_term_t snapshot_last_term;
    raft_index_t snapshot_last_index;
    long crc;

    if (!multibulkReadInt(&log->file, &node_id) ||
        !multibulkReadLong(&log->file, &snapshot_last_term) ||
        !multibulkReadLong(&log->file, &snapshot_last_index) ||
        !multibulkReadLong(&log->file, &crc)) {
        return RR_ERROR;
    }

    memcpy(log->dbid, dbid, sizeof(log->dbid));
    log->dbid[RAFT_DBID_LEN] = '\0';

    log->node_id = node_id;
    log->snapshot_last_term = snapshot_last_term;
    log->snapshot_last_idx = snapshot_last_index;
    log->index = snapshot_last_index;

    if (read_crc != NULL) {
        *read_crc = crc;
    }

    return RR_OK;
}

static size_t generateEntryHeader(raft_entry_t *ety, char *buf, size_t buf_len)
{
    char *pos = buf;
    char *end = buf + buf_len;

    pos += multibulkWriteLen(pos, end - pos, '*', ENTRY_ELEM_COUNT);
    pos += multibulkWriteStr(pos, end - pos, ENTRY_STR);
    pos += multibulkWriteLong(pos, end - pos, ety->term);
    pos += multibulkWriteLong(pos, end - pos, ety->id);
    pos += multibulkWriteLong(pos, end - pos, ety->type);
    pos += multibulkWriteLen(pos, end - pos, '$', (int) ety->data_len);

    return pos - buf;
}

static int writeEntry(Log *log, raft_entry_t *ety)
{
    int rc;
    char buf[1024];
    ssize_t len;

    size_t offset = FileSize(&log->file);
    size_t idxoffset = FileSize(&log->idxfile);

    /* header */
    len = generateEntryHeader(ety, buf, sizeof(buf));
    if (FileWrite(&log->file, buf, len) != len) {
        goto error;
    }

    /* data */
    if (FileWrite(&log->file, ety->data, ety->data_len) != ety->data_len ||
        FileWrite(&log->file, "\r\n", 2) != 2) {
        goto error;
    }

    /* crc */
    /* accumulating crc, so start with current crc */
    long crc = crc16_ccitt(log->current_crc, buf, len);
    crc = crc16_ccitt(crc, ety->data, ety->data_len);
    crc = crc16_ccitt(crc, "\r\n", 2);

    /* write crc as added element */
    len = writeLong(buf, sizeof(buf), crc);
    if (FileWrite(&log->file, buf, len) != len) {
        goto error;
    }

    /* accumulating crc, so add the crc value we just wrote as well */
    crc = crc16_ccitt(crc, buf, len);

    ssize_t ret = FileWrite(&log->idxfile, &offset, sizeof(offset));
    if (ret != sizeof(offset)) {
        goto error;
    }

    /* everything succeeded, so can update accumulated crc */
    log->current_crc = crc;

    return RR_OK;

error:
    /* Try to revert file changes. */
    rc = truncateFiles(log, offset, idxoffset);
    RedisModule_Assert(rc == RR_OK);

    return RR_ERROR;
}

static raft_entry_t *readEntry(Log *log, long *read_crc)
{
    char str[64] = {0};
    int num_elements;

    if (!multibulkReadLen(&log->file, '*', &num_elements) ||
        !multibulkReadStr(&log->file, str, sizeof(str))) {
        return NULL;
    }

    if (num_elements != ENTRY_ELEM_COUNT ||
        strncmp(ENTRY_STR, str, strlen(ENTRY_STR)) != 0) {
        return NULL;
    }

    raft_term_t term;
    raft_entry_id_t id;
    int type, length;

    /* header */
    if (!multibulkReadLong(&log->file, &term) ||
        !multibulkReadInt(&log->file, &id) ||
        !multibulkReadInt(&log->file, &type) ||
        !multibulkReadLen(&log->file, '$', &length)) {
        return NULL;
    }

    char crlf[2];
    raft_entry_t *e = raft_entry_new(length);

    /* data */
    if (FileRead(&log->file, e->data, length) != length ||
        FileRead(&log->file, crlf, 2) != 2) {
        goto error;
    }

    /* crc */
    long crc;
    if (!multibulkReadLong(&log->file, &crc)) {
        goto error;
    }
    if (read_crc != NULL) {
        *read_crc = crc;
    }

    e->term = term;
    e->id = id;
    e->type = (short) type;

    return e;

error:
    raft_entry_release(e);
    return NULL;
}

static Log *prepareLog(const char *filename, bool keep_index)
{
    int rc;
    Log *log = RedisModule_Calloc(1, sizeof(*log));

    safesnprintf(log->filename, sizeof(log->filename), "%s", filename);
    safesnprintf(log->idxfilename, sizeof(log->idxfilename), "%s.idx", filename);

    FileInit(&log->file);
    FileInit(&log->idxfile);

    rc = FileOpen(&log->file, filename, O_APPEND | O_RDWR | O_CREAT);
    if (rc != RR_OK) {
        goto error;
    }

    int mode = O_APPEND | O_RDWR | O_CREAT | (keep_index ? 0 : O_TRUNC);

    rc = FileOpen(&log->idxfile, log->idxfilename, mode);
    if (rc != RR_OK) {
        goto error;
    }

    return log;

error:
    LogFree(log);
    return NULL;
}

Log *LogCreate(const char *filename, const char *dbid,
               raft_term_t snapshot_term, raft_index_t snapshot_index,
               raft_node_id_t node_id)
{
    Log *log = prepareLog(filename, 0);
    if (!log) {
        return NULL;
    }

    memcpy(log->dbid, dbid, RAFT_DBID_LEN);
    log->dbid[RAFT_DBID_LEN] = '\0';

    log->index = snapshot_index;
    log->snapshot_last_idx = snapshot_index;
    log->snapshot_last_term = snapshot_term;
    log->node_id = node_id;

    if (writeHeader(log) != RR_OK) {
        LOG_WARNING("Failed to create log: %s: %s", filename, strerror(errno));
        LogFree(log);
        return NULL;
    }

    return log;
}

void LogFree(Log *log)
{
    if (!log) {
        return;
    }

    FileTerm(&log->file);
    FileTerm(&log->idxfile);
    RedisModule_Free(log);
}

Log *LogOpen(const char *filename, bool keep_index)
{
    Log *log = prepareLog(filename, keep_index);
    if (!log) {
        return NULL;
    }

    if (readHeader(log, NULL) != RR_OK) {
        LogFree(log);
        return NULL;
    }

    return log;
}

int LogReset(Log *log, raft_index_t index, raft_term_t term)
{
    log->index = index;
    log->snapshot_last_idx = index;
    log->snapshot_last_term = term;

    return writeHeader(log);
}

static bool validateEntryCRC(raft_entry_t *e, long read_crc, long current_crc, long *calc_crc)
{
    size_t off = 0;
    char buf[1024];

    /* generate the entry as it should be on disk, and calculate crc */
    off = generateEntryHeader(e, buf, sizeof(buf));
    *calc_crc = crc16_ccitt(current_crc, buf, off);
    *calc_crc = crc16_ccitt(*calc_crc, e->data, e->data_len);
    *calc_crc = crc16_ccitt(*calc_crc, "\r\n", 2);

    if (*calc_crc != read_crc) {
        return true;
    }

    return false;
}

int LogLoadEntries(Log *log)
{
    log->num_entries = 0;
    long calc_crc = 0, read_crc = 0;
    char buf[1024];
    ssize_t len;

    if (readHeader(log, &read_crc) != RR_OK) {
        return RR_ERROR;
    }

    /* validate CRC, by rebuilding header */
    len = generateHeader(log, buf, sizeof(buf));
    calc_crc = crc16_ccitt(0, buf, len);
    if (read_crc != calc_crc) {
        return RR_ERROR;
    }

    /* update running crc */
    len = writeLong(buf, sizeof(buf), calc_crc);
    log->current_crc = crc16_ccitt(calc_crc, buf, len);

    /* Read Entries */
    while (true) {
        long read_crc;
        uint64_t offset = (uint64_t) FileGetReadOffset(&log->file);
        raft_entry_t *e = readEntry(log, &read_crc);
        if (!e) {
            size_t bytes = FileSize(&log->file) - offset;
            if (bytes > 0) {
                LOG_WARNING("Found partial entry at the end of the log file. "
                            "Discarding %zu bytes.",
                            bytes);

                int rc = FileTruncate(&log->file, offset);
                RedisModule_Assert(rc == RR_OK);
            }

            return RR_OK;
        }

        bool error = validateEntryCRC(e, read_crc, log->current_crc, &calc_crc);
        raft_entry_release(e);
        if (error) {
            LOG_WARNING("Entry failed crc32 check, truncating log to "
                        "previous entry: %ld",
                        log->index);
            int rc = FileTruncate(&log->file, offset);
            RedisModule_Assert(rc == RR_OK);
            return RR_OK;
        }

        /* append crc to accumulating crc value */
        len = writeLong(buf, sizeof(buf), calc_crc);
        log->current_crc = crc16_ccitt(calc_crc, buf, len);

        log->index++;
        log->num_entries++;

        ssize_t rc = FileWrite(&log->idxfile, &offset, sizeof(offset));
        RedisModule_Assert(rc == sizeof(offset));
    }
}

size_t LogFileSize(Log *log)
{
    return FileSize(&log->file);
}

int LogSync(Log *log, bool sync)
{
    uint64_t begin = RedisModule_MonotonicMicroseconds();

    if (FileFlush(&log->file) != RR_OK) {
        return RR_ERROR;
    }

    if (sync) {
        if (FileFsync(&log->file) != RR_OK) {
            LOG_WARNING("fsync(): %s", strerror(errno));
            return RR_ERROR;
        }
    }

    uint64_t took = RedisModule_MonotonicMicroseconds() - begin;
    log->fsync_count++;
    log->fsync_total += took;
    log->fsync_max = MAX(took, log->fsync_max);
    log->fsync_index = log->index;

    return RR_OK;
}

int LogAppend(Log *log, raft_entry_t *entry)
{
    if (writeEntry(log, entry) != RR_OK) {
        return RR_ERROR;
    }

    log->index++;
    log->num_entries++;

    return RR_OK;
}

static size_t seekEntry(Log *log, raft_index_t idx)
{
    uint64_t offset;
    size_t idxoffset = (sizeof(uint64_t) * (idx - log->snapshot_last_idx - 1));

    /* Bounds check */
    if (idx <= log->snapshot_last_idx ||
        idx > log->snapshot_last_idx + log->num_entries) {
        return 0;
    }

    if (FileSetReadOffset(&log->idxfile, idxoffset) != RR_OK ||
        FileRead(&log->idxfile, &offset, sizeof(offset)) != sizeof(offset) ||
        FileSetReadOffset(&log->file, offset) != RR_OK) {
        return 0;
    }

    return offset;
}

raft_entry_t *LogGet(Log *log, raft_index_t idx)
{
    if (seekEntry(log, idx) <= 0) {
        return NULL;
    }

    raft_entry_t *ety = readEntry(log, NULL);
    RedisModule_Assert(ety != NULL);

    return ety;
}

int LogDelete(Log *log, raft_index_t from_idx)
{
    if (from_idx <= log->snapshot_last_idx) {
        return RR_ERROR;
    }

    raft_index_t count = log->index - from_idx + 1;
    if (count != 0) {
        size_t offset = seekEntry(log, from_idx);
        if (offset == 0) {
            return RR_ERROR;
        }

        raft_index_t relidx = from_idx - log->snapshot_last_idx - 1;
        size_t idxoffset = relidx * sizeof(uint64_t);

        if (truncateFiles(log, offset, idxoffset) != RR_OK) {
            PANIC("ftruncate failed: %s", strerror(errno));
        }

        log->num_entries -= count;
        log->index -= count;
        /* update running crc value */
        /* if we truncated the entire log file, it's the crc value of the header
         * otherwise, it's the crc from the last entry
         */
        if (log->num_entries == 0) { /* header */
            /* calculate running crc value by just regenerating header buf */
            char buf[1024];
            ssize_t len = generateHeader(log, buf, sizeof(buf));
            long crc = crc16_ccitt(0, buf, len);
            len += writeLong(buf + len, sizeof(buf) - len, crc);
            log->current_crc = crc16_ccitt(0, buf, len);
        } else { /* log entry */
            /* to regenerate running crc value, need to reread last entry */
            long read_crc;
            seekEntry(log, log->index);
            raft_entry_t *e = readEntry(log, &read_crc);
            raft_entry_release(e);

            /* calculate running crc with the crc value */
            char buf[1024];
            ssize_t len = writeLong(buf, sizeof(buf), read_crc);
            log->current_crc = crc16_ccitt(read_crc, buf, len);
        }
    }

    return RR_OK;
}

raft_index_t LogFirstIdx(Log *log)
{
    return log->snapshot_last_idx + 1;
}

raft_index_t LogCurrentIdx(Log *log)
{
    return log->index;
}

raft_index_t LogCount(Log *log)
{
    return log->num_entries;
}

/*
 * Log compaction.
 */

/* Rewrite the current log state into a new file:
 * 1. Latest snapshot info
 * 2. All entries
 * 3. Current term and vote
 */
Log *LogRewrite(RedisRaftCtx *rr, const char *filename,
                raft_index_t last_idx, raft_term_t last_term,
                unsigned long *num_entries)
{
    *num_entries = 0;

    Log *log = LogCreate(filename, rr->snapshot_info.dbid, last_term,
                         last_idx, rr->config.id);

    for (raft_index_t i = last_idx + 1; i <= LogCurrentIdx(rr->log); i++) {
        (*num_entries)++;
        raft_entry_t *ety = raft_get_entry_from_idx(rr->raft, i);
        if (LogAppend(log, ety) != RR_OK) {
            LogFree(log);
            return NULL;
        }
        raft_entry_release(ety);
    }

    if (LogSync(log, true) != RR_OK) {
        LogFree(log);
        return NULL;
    }

    return log;
}

void LogArchiveFiles(Log *log)
{
    char buf[PATH_MAX + 100];

    unlink(log->idxfilename);

    safesnprintf(buf, sizeof(buf), "%s.%d.bak", log->filename, log->node_id);
    rename(log->filename, buf);
}

int LogRewriteSwitch(RedisRaftCtx *rr, Log *new_log,
                     raft_index_t new_log_entries)
{
    /* Rename Raft log.  If we fail, we can assume the old log is still
     * okay and we can just cancel the operation.
     */
    if (rename(new_log->filename, rr->log->filename) < 0) {
        LOG_WARNING("Failed to switch Raft log: %s to %s: %s",
                    new_log->filename, rr->log->filename, strerror(errno));
        return RR_ERROR;
    }

    /* Rename the index.  If we fail now we're in inconsistent state, so we
     * panic and expect the index to be re-built when the process restarts.
     */
    if (rename(new_log->idxfilename, rr->log->idxfilename) < 0) {
        PANIC("Failed to switch Raft log index: %s to %s: %s",
              new_log->idxfilename, rr->log->idxfilename, strerror(errno));
    }

    strcpy(new_log->filename, rr->log->filename);
    strcpy(new_log->idxfilename, rr->log->idxfilename);
    new_log->num_entries = new_log_entries;
    new_log->index = new_log->snapshot_last_idx + new_log->num_entries;

    LogFree(rr->log);
    rr->log = new_log;

    return RR_OK;
}

/*
 * Interface to Raft library.
 */

static void *logImplInit(void *raft, void *arg)
{
    RedisRaftCtx *rr = arg;

    if (!rr->logcache) {
        rr->logcache = EntryCacheNew(ENTRY_CACHE_INIT_SIZE);
    }

    return rr;
}

static void logImplFree(void *arg)
{
    RedisRaftCtx *rr = arg;

    LogFree(rr->log);
    EntryCacheFree(rr->logcache);
}

static void logImplReset(void *arg, raft_index_t index, raft_term_t term)
{
    RedisRaftCtx *rr = arg;

    /* Note: the RaftLogImpl API specifies the specified index is the one
     * to be assigned to the *next* entry, hence the adjustments below.
     */
    assert(index >= 1);
    LogReset(rr->log, index - 1, term);

    RAFTLOG_TRACE("Reset(index=%lu,term=%lu)", index, term);

    EntryCacheFree(rr->logcache);
    rr->logcache = EntryCacheNew(ENTRY_CACHE_INIT_SIZE);
}

static int logImplAppend(void *arg, raft_entry_t *ety)
{
    RedisRaftCtx *rr = arg;

    RAFTLOG_TRACE("Append(id=%d, term=%lu) -> index %lu",
                  ety->id, ety->term, rr->log->index + 1);

    if (LogAppend(rr->log, ety) != RR_OK) {
        return -1;
    }
    EntryCacheAppend(rr->logcache, ety, rr->log->index);
    return 0;
}

static int logImplPoll(void *arg, raft_index_t first_idx)
{
    RedisRaftCtx *rr = arg;

    RAFTLOG_TRACE("Poll(first_idx=%lu)", first_idx);
    EntryCacheDeleteHead(rr->logcache, first_idx);
    return 0;
}

static int logImplPop(void *arg, raft_index_t from_idx)
{
    RedisRaftCtx *rr = arg;

    RAFTLOG_TRACE("Delete(from_idx=%lu)", from_idx);

    EntryCacheDeleteTail(rr->logcache, from_idx);
    if (LogDelete(rr->log, from_idx) != RR_OK) {
        return -1;
    }
    return 0;
}

static raft_entry_t *logImplGet(void *arg, raft_index_t idx)
{
    RedisRaftCtx *rr = arg;
    raft_entry_t *ety;

    ety = EntryCacheGet(rr->logcache, idx);
    if (ety != NULL) {
        RAFTLOG_TRACE("Get(idx=%lu) -> (cache) id=%d, term=%lu",
                      idx, ety->id, ety->term);
        return ety;
    }

    ety = LogGet(rr->log, idx);
    RAFTLOG_TRACE("Get(idx=%lu) -> (file) id=%d, term=%lu",
                  idx, ety ? ety->id : -1, ety ? ety->term : 0);
    return ety;
}

static raft_index_t logImplGetBatch(void *arg, raft_index_t idx,
                                    raft_index_t entries_n,
                                    raft_entry_t **entries)
{
    RedisRaftCtx *rr = arg;
    raft_index_t i = 0;

    while (i < entries_n) {
        raft_entry_t *e = EntryCacheGet(rr->logcache, idx + i);
        if (!e) {
            e = LogGet(rr->log, idx + i);
            if (!e) {
                break;
            }
        }

        entries[i++] = e;
    }

    RAFTLOG_TRACE("GetBatch(idx=%lu entries_n=%ld) -> %ld", idx, entries_n, i);
    return i;
}

static raft_index_t logImplFirstIdx(void *arg)
{
    RedisRaftCtx *rr = arg;
    return LogFirstIdx(rr->log);
}

static raft_index_t logImplCurrentIdx(void *arg)
{
    RedisRaftCtx *rr = arg;
    return LogCurrentIdx(rr->log);
}

static raft_index_t logImplCount(void *arg)
{
    RedisRaftCtx *rr = arg;
    return LogCount(rr->log);
}

static int logImplSync(void *arg)
{
    RedisRaftCtx *rr = arg;
    return LogSync(rr->log, rr->config.log_fsync);
}

raft_log_impl_t LogImpl = {
    .init = logImplInit,
    .free = logImplFree,
    .reset = logImplReset,
    .append = logImplAppend,
    .poll = logImplPoll,
    .pop = logImplPop,
    .get = logImplGet,
    .get_batch = logImplGetBatch,
    .first_idx = logImplFirstIdx,
    .current_idx = logImplCurrentIdx,
    .count = logImplCount,
    .sync = logImplSync,
};
