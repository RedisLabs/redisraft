/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020-21 Redis Ltd.
 *
 * RedisRaft is licensed under the Redis Source Available License (RSAL).
 */

#include "log.h"

#include "entrycache.h"
#include "redisraft.h"

#include "redismodule.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

#define ENTRY_CACHE_INIT_SIZE 512

void RaftLogClose(RaftLog *log)
{
    if (log->file) {
        fclose(log->file);
        log->file = NULL;
    }

    RedisModule_Free(log->entry_offsets);
    RedisModule_Free(log);
}

/*
 * Raw reading/writing of Raft log.
 */
static int writeBegin(FILE *logfile, int length)
{
    int n;

    if ((n = fprintf(logfile, "*%u\r\n", length)) <= 0) {
        return -1;
    }

    return n;
}

static int writeBuffer(FILE *logfile, const void *buf, size_t buf_len)
{
    static const char crlf[] = "\r\n";
    int n;

    if ((n = fprintf(logfile, "$%zu\r\n", buf_len)) <= 0 ||
        fwrite(buf, 1, buf_len, logfile) < buf_len ||
        fwrite(crlf, 1, 2, logfile) < 2) {
        return -1;
    }

    return n + buf_len + 2;
}

static int writeUnsignedInteger(FILE *logfile, unsigned long value, int pad)
{
    char buf[25];
    int n;
    assert(pad < (int) sizeof(buf));

    if (pad) {
        snprintf(buf, sizeof(buf) - 1, "%0*lu", pad, value);
    } else {
        snprintf(buf, sizeof(buf) - 1, "%lu", value);
    }

    if ((n = fprintf(logfile, "$%zu\r\n%s\r\n", strlen(buf), buf)) <= 0) {
        return -1;
    }

    return n;
}

typedef struct RawElement {
    void *ptr;
    size_t len;
} RawElement;

typedef struct RawLogEntry {
    int num_elements;
    RawElement elements[];
} RawLogEntry;

static int readEncodedLength(FILE *fp, char type, unsigned long *length)
{
    char buf[128];
    char *eptr;

    if (!fgets(buf, sizeof(buf), fp)) {
        return -1;
    }

    if (buf[0] != type) {
        return -1;
    }

    *length = strtoul(buf + 1, &eptr, 10);
    if (*eptr != '\n' && *eptr != '\r') {
        return -1;
    }

    return 0;
}

static void freeRawLogEntry(RawLogEntry *entry)
{
    int i;

    if (!entry) {
        return;
    }

    for (i = 0; i < entry->num_elements; i++) {
        if (entry->elements[i].ptr != NULL) {
            RedisModule_Free(entry->elements[i].ptr);
            entry->elements[i].ptr = NULL;
        }
    }

    RedisModule_Free(entry);
}

static int readRawLogEntry(FILE *fp, RawLogEntry **entry)
{
    unsigned long num_elements;
    int i;

    if (readEncodedLength(fp, '*', &num_elements) < 0) {
        return -1;
    }

    *entry = RedisModule_Calloc(1, sizeof(RawLogEntry) + sizeof(RawElement) * num_elements);
    (*entry)->num_elements = (int) num_elements;

    for (i = 0; i < (int) num_elements; i++) {
        unsigned long len;
        char *ptr;

        if (readEncodedLength(fp, '$', &len) < 0) {
            goto error;
        }
        (*entry)->elements[i].len = len;
        (*entry)->elements[i].ptr = ptr = RedisModule_Alloc(len + 2);

        /* Read extra CRLF */
        if (fread(ptr, 1, len + 2, fp) != len + 2) {
            goto error;
        }
        ptr[len] = '\0';
        ptr[len + 1] = '\0';
    }

    return 0;
error:
    freeRawLogEntry(*entry);
    *entry = NULL;

    return -1;
}

static int updateIndex(RaftLog *log, raft_index_t index, off_t offset)
{
    RedisModule_Assert(index > log->snapshot_last_idx);

    raft_index_t relidx = index - log->snapshot_last_idx - 1;

    if (relidx >= log->entry_offset_cap) {
        log->entry_offset_cap = (log->entry_offset_cap == 0) ? 8 : log->entry_offset_cap * 2;
        log->entry_offsets = RedisModule_Realloc(log->entry_offsets, log->entry_offset_cap * sizeof(off_t));
    }

    log->entry_offsets[relidx] = offset;
    return 0;
}

static RaftLog *prepareLog(const char *filename)
{
    FILE *file = fopen(filename, "a+");
    if (!file) {
        LOG_WARNING("Raft Log: %s: %s", filename, strerror(errno));
        return NULL;
    }

    /* Initialize struct */
    RaftLog *log = RedisModule_Calloc(1, sizeof(RaftLog));
    log->file = file;
    log->filename = filename;

    return log;
}

int writeLogHeader(FILE *logfile, RaftLog *log)
{
    if (writeBegin(logfile, 5) < 0 ||
        writeBuffer(logfile, "RAFTLOG", 7) < 0 ||
        writeUnsignedInteger(logfile, RAFTLOG_VERSION, 4) < 0 ||
        writeUnsignedInteger(logfile, log->node_id, 20) < 0 ||
        writeUnsignedInteger(logfile, log->snapshot_last_term, 20) < 0 ||
        writeUnsignedInteger(logfile, log->snapshot_last_idx, 20) < 0) {
        return -1;
    }

    return 0;
}

RaftLog *RaftLogCreate(const char *filename, raft_term_t snapshot_term,
                       raft_index_t snapshot_index, raft_node_id_t node_id)
{
    RaftLog *log = prepareLog(filename);
    if (!log) {
        return NULL;
    }

    log->index = log->snapshot_last_idx = snapshot_index;
    log->snapshot_last_term = snapshot_term;
    log->node_id = node_id;

    /* Truncate */
    if (ftruncate(fileno(log->file), 0) < 0) {
        PANIC("ftruncate failed : %s", strerror(errno));
    }

    /* Write log start */
    if (writeLogHeader(log->file, log) < 0 ||
        RaftLogSync(log, true) != RR_OK) {
        LOG_WARNING("Failed to create Raft log: %s: %s", filename, strerror(errno));
        RaftLogClose(log);
        log = NULL;
    }

    return log;
}

static raft_entry_t *parseRaftLogEntry(RawLogEntry *re)
{
    char *eptr;
    raft_entry_t *e;

    if (re->num_elements != 5) {
        LOG_WARNING("Log entry: invalid number of arguments: %d", re->num_elements);
        return NULL;
    }

    e = raft_entry_new(re->elements[4].len);
    memcpy(e->data, re->elements[4].ptr, re->elements[4].len);

    e->term = strtoul(re->elements[1].ptr, &eptr, 10);
    if (*eptr) {
        goto error;
    }

    e->id = strtoul(re->elements[2].ptr, &eptr, 10);
    if (*eptr) {
        goto error;
    }

    e->type = strtoul(re->elements[3].ptr, &eptr, 10);
    if (*eptr) {
        goto error;
    }

    return e;

error:
    raft_entry_release(e);
    return NULL;
}

static int handleHeader(RaftLog *log, RawLogEntry *re)
{
    if (re->num_elements != 5 ||
        strcmp(re->elements[0].ptr, "RAFTLOG")) {
        LOG_WARNING("Invalid Raft log header.");
        return -1;
    }

    char *eptr;
    unsigned long ver = strtoul(re->elements[1].ptr, &eptr, 10);
    if (*eptr != '\0' || ver != RAFTLOG_VERSION) {
        LOG_WARNING("Invalid Raft header version: %lu", ver);
        return -1;
    }

    log->node_id = strtoul(re->elements[2].ptr, &eptr, 10);
    if (*eptr != '\0') {
        LOG_WARNING("Invalid Raft node_id: %s", (char *) re->elements[2].ptr);
        return -1;
    }

    log->snapshot_last_term = strtoul(re->elements[3].ptr, &eptr, 10);
    if (*eptr != '\0') {
        LOG_WARNING("Invalid Raft log term: %s", (char *) re->elements[3].ptr);
        return -1;
    }

    log->index = log->snapshot_last_idx = strtoul(re->elements[4].ptr, &eptr, 10);
    if (*eptr != '\0') {
        LOG_WARNING("Invalid Raft log index: %s", (char *) re->elements[4].ptr);
        return -1;
    }

    return 0;
}

RaftLog *RaftLogOpen(const char *filename)
{
    RaftLog *log = prepareLog(filename);
    RawLogEntry *e = NULL;

    if (!log) {
        return NULL;
    }

    /* Gracefully skip an empty file */
    fseek(log->file, 0L, SEEK_END);
    if (!ftell(log->file)) {
        goto error;
    }

    /* Read start */
    fseek(log->file, 0L, SEEK_SET);

    if (readRawLogEntry(log->file, &e) < 0) {
        LOG_WARNING("Failed to read Raft log: %s",
                    errno ? strerror(errno) : "invalid data");
        goto error;
    }

    if (handleHeader(log, e) < 0) {
        goto error;
    }

    freeRawLogEntry(e);
    return log;

error:
    if (e != NULL) {
        freeRawLogEntry(e);
    }
    RaftLogClose(log);
    return NULL;
}

int RaftLogReset(RaftLog *log, raft_index_t index, raft_term_t term)
{
    log->index = log->snapshot_last_idx = index;
    log->snapshot_last_term = term;

    log->num_entries = 0;
    RedisModule_Free(log->entry_offsets);
    log->entry_offsets = NULL;
    log->entry_offset_cap = 0;

    if (ftruncate(fileno(log->file), 0) < 0 ||
        writeLogHeader(log->file, log) < 0 ||
        RaftLogSync(log, true) != RR_OK) {
        return RR_ERROR;
    }

    return RR_OK;
}

int RaftLogLoadEntries(RaftLog *log)
{
    int ret = 0;

    if (fseek(log->file, 0, SEEK_SET) < 0) {
        return -1;
    }

    log->index = 0;

    /* Read Header */
    RawLogEntry *re = NULL;
    if (readRawLogEntry(log->file, &re) < 0 || handleHeader(log, re) < 0) {
        freeRawLogEntry(re);
        LOG_NOTICE("Failed to read Raft log header");
        return -1;
    }
    freeRawLogEntry(re);

    /* Read Entries */
    do {
        raft_entry_t *e = NULL;

        long offset = ftell(log->file);
        if (readRawLogEntry(log->file, &re) < 0 || !re->num_elements) {
            break;
        }

        if (!strcasecmp(re->elements[0].ptr, "ENTRY")) {
            e = parseRaftLogEntry(re);
            if (!e) {
                freeRawLogEntry(re);
                ret = -1;
                break;
            }
            log->index++;
            ret++;

            updateIndex(log, log->index, offset);
        } else {
            LOG_WARNING("Invalid log entry: %s", (char *) re->elements[0].ptr);
            freeRawLogEntry(re);

            ret = -1;
            break;
        }

        freeRawLogEntry(re);
        raft_entry_release(e);
    } while (1);

    if (ret > 0) {
        log->num_entries = ret;
    }
    return ret;
}

int RaftLogWriteEntry(RaftLog *log, raft_entry_t *entry)
{
    size_t written = 0;
    int n;

    if ((n = writeBegin(log->file, 5)) < 0) {
        return RR_ERROR;
    }
    written += n;
    if ((n = writeBuffer(log->file, "ENTRY", 5)) < 0) {
        return RR_ERROR;
    }
    written += n;
    if ((n = writeUnsignedInteger(log->file, entry->term, 0)) < 0) {
        return RR_ERROR;
    }
    written += n;
    if ((n = writeUnsignedInteger(log->file, entry->id, 0)) < 0) {
        return RR_ERROR;
    }
    written += n;
    if ((n = writeUnsignedInteger(log->file, entry->type, 0)) < 0) {
        return RR_ERROR;
    }
    written += n;
    if ((n = writeBuffer(log->file, entry->data, entry->data_len)) < 0) {
        return RR_ERROR;
    }
    written += n;

    /* Update index */
    log->file_size = ftell(log->file);
    off_t offset = log->file_size - written;
    log->index++;
    if (updateIndex(log, log->index, offset) < 0) {
        return RR_ERROR;
    }

    return RR_OK;
}

int RaftLogSync(RaftLog *log, bool sync)
{
    if (fflush(log->file) < 0) {
        LOG_WARNING("fflush() : %s", strerror(errno));
        return RR_ERROR;
    }

    if (sync) {
        if (fsync(fileno(log->file)) < 0) {
            LOG_WARNING("fsync() : %s", strerror(errno));
            return RR_ERROR;
        }
    }

    log->fsync_index = log->index;

    return RR_OK;
}

int RaftLogAppend(RaftLog *log, raft_entry_t *entry)
{
    if (RaftLogWriteEntry(log, entry) != RR_OK) {
        return RR_ERROR;
    }

    log->num_entries++;
    return RR_OK;
}

static off_t seekEntry(RaftLog *log, raft_index_t idx)
{
    /* Bounds check */
    if (idx <= log->snapshot_last_idx) {
        return 0;
    }

    if (idx > log->snapshot_last_idx + log->num_entries) {
        return 0;
    }

    raft_index_t pos = idx - log->snapshot_last_idx - 1;
    off_t offset = (off_t) log->entry_offsets[pos];

    if (fseek(log->file, offset, SEEK_SET) < 0) {
        return 0;
    }

    return offset;
}

raft_entry_t *RaftLogGet(RaftLog *log, raft_index_t idx)
{
    if (seekEntry(log, idx) <= 0) {
        return NULL;
    }

    RawLogEntry *re;
    if (readRawLogEntry(log->file, &re) != RR_OK) {
        return NULL;
    }

    raft_entry_t *e = parseRaftLogEntry(re);
    freeRawLogEntry(re);

    if (!e) {
        return NULL;
    }

    return e;
}

int RaftLogDelete(RaftLog *log, raft_index_t from_idx)
{
    off_t offset;
    RRStatus ret = RR_OK;

    if (from_idx <= log->snapshot_last_idx) {
        return RR_ERROR;
    }

    while (log->index >= from_idx) {
        if (!(offset = seekEntry(log, log->index))) {
            return RR_ERROR;
        }

        RawLogEntry *re;
        raft_entry_t *e;

        if (readRawLogEntry(log->file, &re) < 0) {
            ret = RR_ERROR;
            break;
        }

        if (!strcasecmp(re->elements[0].ptr, "ENTRY")) {
            if ((e = parseRaftLogEntry(re)) == NULL) {
                freeRawLogEntry(re);
                ret = RR_ERROR;
                break;
            }

            log->index--;
            log->num_entries--;

            raft_entry_release(e);

            if (ftruncate(fileno(log->file), offset) < 0) {
                PANIC("ftruncate failed : %s", strerror(errno));
            }
        }

        freeRawLogEntry(re);
    }

    return ret;
}

raft_index_t RaftLogFirstIdx(RaftLog *log)
{
    return log->snapshot_last_idx;
}

raft_index_t RaftLogCurrentIdx(RaftLog *log)
{
    return log->index;
}

raft_index_t RaftLogCount(RaftLog *log)
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
long long int RaftLogRewrite(RedisRaftCtx *rr,
                             const char *filename,
                             raft_index_t last_idx,
                             raft_term_t last_term)
{
    long long int num_entries = 0;

    RaftLog *log = RaftLogCreate(filename, last_term, last_idx, rr->config.id);

    for (raft_index_t i = last_idx + 1; i <= RaftLogCurrentIdx(rr->log); i++) {
        num_entries++;

        raft_entry_t *ety = raft_get_entry_from_idx(rr->raft, i);
        if (RaftLogWriteEntry(log, ety) != RR_OK) {
            RaftLogClose(log);
            return -1;
        }
        raft_entry_release(ety);
    }

    if (RaftLogSync(log, true) != RR_OK) {
        RaftLogClose(log);
        return -1;
    }

    RaftLogClose(log);
    return num_entries;
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

static void logImplFree(void *rr_)
{
    RedisRaftCtx *rr = rr_;

    RaftLogClose(rr->log);
    EntryCacheFree(rr->logcache);
}

static void logImplReset(void *rr_, raft_index_t index, raft_term_t term)
{
    RedisRaftCtx *rr = rr_;

    /* Note: the RaftLogImpl API specifies the specified index is the one
     * to be assigned to the *next* entry, hence the adjustments below.
     */
    assert(index >= 1);
    RaftLogReset(rr->log, index - 1, term);

    EntryCacheFree(rr->logcache);
    rr->logcache = EntryCacheNew(ENTRY_CACHE_INIT_SIZE);
}

static int logImplAppend(void *rr_, raft_entry_t *ety)
{
    RedisRaftCtx *rr = rr_;

    if (RaftLogAppend(rr->log, ety) != RR_OK) {
        return -1;
    }
    EntryCacheAppend(rr->logcache, ety, rr->log->index);
    return 0;
}

static int logImplPoll(void *rr_, raft_index_t first_idx)
{
    RedisRaftCtx *rr = rr_;

    EntryCacheDeleteHead(rr->logcache, first_idx);
    return 0;
}

static int logImplPop(void *rr_, raft_index_t from_idx)
{
    RedisRaftCtx *rr = rr_;

    EntryCacheDeleteTail(rr->logcache, from_idx);
    if (RaftLogDelete(rr->log, from_idx) != RR_OK) {
        return -1;
    }
    return 0;
}

static raft_entry_t *logImplGet(void *rr_, raft_index_t idx)
{
    RedisRaftCtx *rr = rr_;
    raft_entry_t *ety;

    ety = EntryCacheGet(rr->logcache, idx);
    if (ety != NULL) {
        return ety;
    }

    ety = RaftLogGet(rr->log, idx);

    return ety;
}

static raft_index_t logImplGetBatch(void *rr_, raft_index_t idx, raft_index_t entries_n, raft_entry_t **entries)
{
    RedisRaftCtx *rr = rr_;
    raft_index_t n = 0;
    raft_index_t i = idx;

    while (n < entries_n) {
        raft_entry_t *e = EntryCacheGet(rr->logcache, i);
        if (!e) {
            e = RaftLogGet(rr->log, i);
        }
        if (!e) {
            break;
        }

        entries[n] = e;
        n++;
        i++;
    }

    return n;
}

static raft_index_t logImplFirstIdx(void *rr_)
{
    RedisRaftCtx *rr = rr_;
    return RaftLogFirstIdx(rr->log);
}

static raft_index_t logImplCurrentIdx(void *rr_)
{
    RedisRaftCtx *rr = rr_;
    return RaftLogCurrentIdx(rr->log);
}

static raft_index_t logImplCount(void *rr_)
{
    RedisRaftCtx *rr = rr_;
    return RaftLogCount(rr->log);
}

static int logImplSync(void *rr_)
{
    RedisRaftCtx *rr = rr_;
    return RaftLogSync(rr->log, true);
}

raft_log_impl_t RaftLogImpl = {
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
