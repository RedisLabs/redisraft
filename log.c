/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020 Redis Labs
 *
 * RedisRaft is dual licensed under the GNU Affero General Public License version 3
 * (AGPLv3) or the Redis Source Available License (RSAL).
 */

#include <unistd.h>
#include <string.h>
#include <strings.h>
#include <stdlib.h>
#include <sys/uio.h>

#include <assert.h>

#include "redisraft.h"

#define ENTRY_CACHE_INIT_SIZE 512

#ifdef RAFT_LOG_TRACE
#  define TRACE_LOG_OP(fmt, ...) LOG_DEBUG("Log>>" fmt, ##__VA_ARGS__)
#else
#  define TRACE_LOG_OP(fmt, ...) do {} while(0)
#endif

/*
 * Entries Cache.
 */

EntryCache *EntryCacheNew(unsigned long initial_size)
{
    EntryCache *cache = RedisModule_Calloc(1, sizeof(EntryCache));

    if (initial_size < 0) {
        initial_size = ENTRY_CACHE_INIT_SIZE;
    }
    cache->size = initial_size;
    cache->ptrs = RedisModule_Calloc(cache->size, sizeof(raft_entry_t *));

    return cache;
}

void EntryCacheFree(EntryCache *cache)
{
    unsigned long i;

    for (i = 0; i < cache->len; i++) {
        raft_entry_release(cache->ptrs[(cache->start + i) % cache->size]);
    }

    RedisModule_Free(cache->ptrs);
    RedisModule_Free(cache);
}

void EntryCacheAppend(EntryCache *cache, raft_entry_t *ety, raft_index_t idx)
{
    if (!cache->start_idx) {
        cache->start_idx = idx;
    }

    assert(cache->start_idx + cache->len == idx);

    /* Enlrage cache if necessary */
    if (cache->len == cache->size) {
        unsigned long int new_size = cache->size * 2;
        cache->ptrs = RedisModule_Realloc(cache->ptrs, new_size * sizeof(raft_entry_t *));

        if (cache->start > 0) {
            memmove(&cache->ptrs[cache->size], &cache->ptrs[0], cache->start * sizeof(raft_entry_t *));
            memset(&cache->ptrs[0], 0, cache->start * sizeof(raft_entry_t *));
        }

        cache->size = new_size;
    }

    cache->ptrs[(cache->start + cache->len) % cache->size] = ety;
    cache->len++;
    cache->entries_memsize += sizeof(raft_entry_t) + ety->data_len;
    raft_entry_hold(ety);
}

raft_entry_t *EntryCacheGet(EntryCache *cache, raft_index_t idx)
{
    if (idx < cache->start_idx) {
        return NULL;
    }

    unsigned long int relidx = idx - cache->start_idx;
    if (relidx >= cache->len) {
        return NULL;
    }

    raft_entry_t *ety = cache->ptrs[(cache->start + relidx) % cache->size];
    raft_entry_hold(ety);
    return ety;
}

long EntryCacheDeleteHead(EntryCache *cache, raft_index_t first_idx)
{
    long deleted = 0;

    if (first_idx < cache->start_idx) {
        return -1;
    }

    while (first_idx > cache->start_idx && cache->len > 0) {
        raft_entry_t *ety = cache->ptrs[cache->start];
        cache->entries_memsize -= sizeof(raft_entry_t) + ety->data_len;
        raft_entry_release(ety);

        cache->start_idx++;
        cache->ptrs[cache->start] = NULL;
        cache->start++;
        if (cache->start >= cache->size) {
            cache->start = 0;
        }
        cache->len--;
        deleted++;
    }

    if (!cache->len) {
        cache->start_idx = 0;
    }

    return deleted;
}

/* Remove entries from the tail of the cache, starting from (and including)
 * entry @ index.
 *
 * Returns the number of entries removed, or -1 if the specified index is
 * beyond the tail.
 */
long EntryCacheDeleteTail(EntryCache *cache, raft_index_t index)
{
    long deleted = 0;
    raft_index_t i;

    if (index >= cache->start_idx + cache->len) {
        return -1;
    }
    if (index < cache->start_idx) {
        index = cache->start_idx;
    }

    for (i = index; i < cache->start_idx + cache->len; i++) {
        unsigned long int relidx = i - cache->start_idx;
        unsigned long int ofs = (cache->start + relidx) % cache->size;
        raft_entry_t *ety = cache->ptrs[ofs];

        cache->entries_memsize -= sizeof(raft_entry_t) + ety->data_len;
        raft_entry_release(ety);

        cache->ptrs[ofs] = NULL;
        deleted++;
    }

    cache->len -= deleted;

    if (!cache->len) {
        cache->start_idx = 0;
    }

    return deleted;
}

long EntryCacheCompact(EntryCache *cache, size_t max_memory)
{
    long deleted = 0;

    while (cache->len > 0 && cache->entries_memsize > max_memory) {
        raft_entry_t *ety = cache->ptrs[cache->start];
        cache->entries_memsize -= sizeof(raft_entry_t) + ety->data_len;
        raft_entry_release(ety);

        cache->start_idx++;
        cache->ptrs[cache->start] = NULL;
        cache->start++;
        if (cache->start >= cache->size) {
            cache->start = 0;
        }
        cache->len--;
        deleted++;
    }

    if (!cache->len) {
        cache->start_idx = 0;
    }

    return deleted;
}

void RaftLogClose(RaftLog *log)
{
    if (log->file) {
        fclose(log->file);
        log->file = NULL;
    }
    if (log->idxfile) {
        fclose(log->idxfile);
        log->idxfile = NULL;
    }
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

static int writeEnd(FILE *logfile, bool use_fsync)
{
    if (fflush(logfile) < 0) {
        return -1;
    }
    if (!use_fsync) {
        return 0;
    }
    if (fsync(fileno(logfile)) < 0) {
        return -1;
    }

    return 0;
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
    assert(pad < sizeof(buf));

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

static int writeInteger(FILE *logfile, long value, int pad)
{
    char buf[25];
    int n;
    assert(pad < sizeof(buf));

    if (pad) {
        snprintf(buf, sizeof(buf) - 1, "%0*ld", pad, value);
    } else {
        snprintf(buf, sizeof(buf) - 1, "%ld", value);
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

static int readEncodedLength(RaftLog *log, char type, unsigned long *length)
{
    char buf[128];
    char *eptr;

    if (!fgets(buf, sizeof(buf), log->file)) {
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

static int readRawLogEntry(RaftLog *log, RawLogEntry **entry)
{
    unsigned long num_elements;
    int i;

    if (readEncodedLength(log, '*', &num_elements) < 0) {
        return -1;
    }

    *entry = RedisModule_Calloc(1, sizeof(RawLogEntry) + sizeof(RawElement) * num_elements);
    (*entry)->num_elements = num_elements;
    for (i = 0; i < num_elements; i++) {
        unsigned long len;
        char *ptr;

        if (readEncodedLength(log, '$', &len) < 0) {
            goto error;
        }
        (*entry)->elements[i].len = len;
        (*entry)->elements[i].ptr = ptr = RedisModule_Alloc(len + 2);

        /* Read extra CRLF */
        if (fread(ptr, 1, len + 2, log->file) != len + 2) {
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
    long relidx = index - log->snapshot_last_idx;

    if (fseek(log->idxfile, sizeof(off_t) * relidx, SEEK_SET) < 0 ||
            fwrite(&offset, sizeof(off_t), 1, log->idxfile) != 1) {
        return -1;
    }

    return 0;
}

static char *getIndexFilename(const char *filename)
{
    int idx_filename_len = strlen(filename) + 10;
    char *idx_filename = RedisModule_Alloc(idx_filename_len);
    snprintf(idx_filename, idx_filename_len - 1, "%s.idx", filename);
    return idx_filename;
}

static RaftLog *prepareLog(const char *filename, RedisRaftConfig *config, int flags)
{
    FILE *file = fopen(filename, "a+");
    if (!file) {
        LOG_ERROR("Raft Log: %s: %s", filename, strerror(errno));
        return NULL;
    }

    /* Index file */
    char *idx_filename = getIndexFilename(filename);
    FILE *idxfile = fopen(idx_filename, (flags & RAFTLOG_KEEP_INDEX) ? "r+" : "w+");
    if (!idxfile) {
        LOG_ERROR("Raft Log: %s: %s", idx_filename, strerror(errno));
        RedisModule_Free(idx_filename);
        fclose(file);
        return NULL;
    }
    RedisModule_Free(idx_filename);

    /* Initialize struct */
    RaftLog *log = RedisModule_Calloc(1, sizeof(RaftLog));
    log->file = file;
    log->idxfile = idxfile;
    log->filename = filename;

    /* Config */
    if (config) {
        log->fsync = config->raft_log_fsync;
    } else {
        log->fsync = true;
    }

    return log;
}

int writeLogHeader(FILE *logfile, RaftLog *log)
{
    if (writeBegin(logfile, 8) < 0 ||
        writeBuffer(logfile, "RAFTLOG", 7) < 0 ||
        writeUnsignedInteger(logfile, RAFTLOG_VERSION, 4) < 0 ||
        writeBuffer(logfile, log->dbid, strlen(log->dbid)) < 0 ||
        writeUnsignedInteger(logfile, log->node_id, 20) < 0 ||
        writeUnsignedInteger(logfile, log->snapshot_last_term, 20) < 0 ||
        writeUnsignedInteger(logfile, log->snapshot_last_idx, 20) < 0 ||
        writeUnsignedInteger(logfile, log->term, 20) < 0 ||
        writeInteger(logfile, log->vote, 11) < 0 ||
        writeEnd(logfile, log->fsync) < 0) {
            return -1;
    }

    return 0;
}

int updateLogHeader(RaftLog *log)
{
    int ret;

    /* Avoid same file open twice */
    fclose(log->file);
    log->file = NULL;

    FILE *file = fopen(log->filename, "r+");
    if (!file) {
        PANIC("Failed to update log header: %s: %s",
                log->filename, strerror(errno));
    }

    ret = writeLogHeader(file, log);
    fclose(file);

    /* Reopen */
    log->file = fopen(log->filename, "a+");
    if (!log->file) {
        PANIC("Failed to reopen log file: %s: %s",
                log->filename, strerror(errno));
    }

    return ret;
}

RaftLog *RaftLogCreate(const char *filename, const char *dbid, raft_term_t snapshot_term,
        raft_index_t snapshot_index, raft_term_t current_term, raft_node_id_t last_vote, RedisRaftConfig *config)
{
    RaftLog *log = prepareLog(filename, config, 0);
    if (!log) {
        return NULL;
    }

    log->index = log->snapshot_last_idx = snapshot_index;
    log->snapshot_last_term = snapshot_term;
    log->term = current_term;
    log->vote = last_vote;

    memcpy(log->dbid, dbid, RAFT_DBID_LEN);
    log->dbid[RAFT_DBID_LEN] = '\0';
    log->node_id = config->id;

    /* Truncate */
    ftruncate(fileno(log->file), 0);
    ftruncate(fileno(log->idxfile), 0);

    /* Write log start */
    if (writeLogHeader(log->file, log) < 0) {
        LOG_ERROR("Failed to create Raft log: %s: %s", filename, strerror(errno));
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
        LOG_ERROR("Log entry: invalid number of arguments: %d", re->num_elements);
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
    if (re->num_elements != 8 ||
        strcmp(re->elements[0].ptr, "RAFTLOG")) {
        LOG_ERROR("Invalid Raft log header.");
        return -1;
    }

    char *eptr;
    unsigned long ver = strtoul(re->elements[1].ptr, &eptr, 10);
    if (*eptr != '\0' || ver != RAFTLOG_VERSION) {
        LOG_ERROR("Invalid Raft header version: %lu", ver);
        return -1;
    }

    if (strlen(re->elements[2].ptr) > RAFT_DBID_LEN) {
        LOG_ERROR("Invalid Raft log dbid: %s", (char *) re->elements[2].ptr);
        return -1;
    }
    strcpy(log->dbid, re->elements[2].ptr);

    log->node_id = strtoul(re->elements[3].ptr, &eptr, 10);
    if (*eptr != '\0') {
        LOG_ERROR("Invalid Raft node_id: %s", (char *) re->elements[3].ptr);
        return -1;
    }

    log->snapshot_last_term = strtoul(re->elements[4].ptr, &eptr, 10);
    if (*eptr != '\0') {
        LOG_ERROR("Invalid Raft log term: %s", (char *) re->elements[4].ptr);
        return -1;
    }

    log->index = log->snapshot_last_idx = strtoul(re->elements[5].ptr, &eptr, 10);
    if (*eptr != '\0') {
        LOG_ERROR("Invalid Raft log index: %s", (char *) re->elements[5].ptr);
        return -1;
    }

    log->term = strtoul(re->elements[6].ptr, &eptr, 10);
    if (*eptr != '\0') {
        LOG_ERROR("Invalid Raft log voted term: %s", (char *) re->elements[6].ptr);
        return -1;
    }

    log->vote = strtol(re->elements[7].ptr, &eptr, 10);
    if (*eptr != '\0') {
        LOG_ERROR("Invalid Raft log vote: %s", (char *) re->elements[7].ptr);
        return -1;
    }

    return 0;
}

RaftLog *RaftLogOpen(const char *filename, RedisRaftConfig *config, int flags)
{
    RaftLog *log = prepareLog(filename, config, flags);
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

    if (readRawLogEntry(log, &e) < 0) {
        LOG_ERROR("Failed to read Raft log: %s", errno ? strerror(errno) : "invalid data");
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

RRStatus RaftLogReset(RaftLog *log, raft_index_t index, raft_term_t term)
{
    log->index = log->snapshot_last_idx = index;
    log->snapshot_last_term = term;
    if (log->term > term) {
        log->term = term;
        log->vote = -1;
    }

    if (ftruncate(fileno(log->file), 0) < 0 ||
        ftruncate(fileno(log->idxfile), 0) < 0 ||
        writeLogHeader(log->file, log) < 0) {

        return RR_ERROR;
    }

    return RR_OK;
}

int RaftLogLoadEntries(RaftLog *log, int (*callback)(void *, raft_entry_t *, raft_index_t), void *callback_arg)
{
    int ret = 0;

    if (fseek(log->file, 0, SEEK_SET) < 0) {
        return -1;
    }

    log->term = 1;
    log->index = 0;

    /* Read Header */
    RawLogEntry *re = NULL;
    if (readRawLogEntry(log, &re) < 0 || handleHeader(log, re) < 0)  {
        freeRawLogEntry(re);
        LOG_INFO("Failed to read Raft log header");
        return -1;
    }
    freeRawLogEntry(re);

    /* Read Entries */
    do {
        raft_entry_t *e = NULL;

        long offset = ftell(log->file);
        if (readRawLogEntry(log, &re) < 0 || !re->num_elements) {
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
            LOG_ERROR("Invalid log entry: %s", (char *) re->elements[0].ptr);
            freeRawLogEntry(re);

            ret = -1;
            break;
        }

        int cb_ret = 0;
        if (callback) {
            callback(callback_arg, e, log->index);
        }

        freeRawLogEntry(re);
        raft_entry_release(e);

        if (cb_ret < 0) {
            ret = cb_ret;
            break;
        }
    } while(1);

    if (ret > 0) {
        log->num_entries = ret;
    }
    return ret;
}

RRStatus RaftLogWriteEntry(RaftLog *log, raft_entry_t *entry)
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

RRStatus RaftLogSync(RaftLog *log)
{
    if (writeEnd(log->file, log->fsync) < 0) {
        return RR_ERROR;
    }
    return RR_OK;
}

RRStatus RaftLogAppend(RaftLog *log, raft_entry_t *entry)
{
    if (RaftLogWriteEntry(log, entry) != RR_OK ||
            writeEnd(log->file, log->fsync) < 0) {
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

    raft_index_t relidx = idx - log->snapshot_last_idx;
    off_t offset;
    if (fseek(log->idxfile, sizeof(off_t) * relidx, SEEK_SET) < 0 ||
            fread(&offset, sizeof(offset), 1, log->idxfile) != 1) {
        return 0;
    }

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
    if (readRawLogEntry(log, &re) != RR_OK) {
        return NULL;
    }

    raft_entry_t *e = parseRaftLogEntry(re);
    freeRawLogEntry(re);

    if (!e) {
        return NULL;
    }

    return e;
}

RRStatus RaftLogDelete(RaftLog *log, raft_index_t from_idx, func_entry_notify_f cb, void *cb_arg)
{
    off_t offset;
    RRStatus ret = RR_OK;
    unsigned long removed = 0;

    if (from_idx <= log->snapshot_last_idx) {
        return RR_ERROR;
    }

    while (log->index >= from_idx) {
        if (!(offset = seekEntry(log, log->index))) {
            return RR_ERROR;
        }

        RawLogEntry *re;
        raft_entry_t *e;

        if (readRawLogEntry(log, &re) < 0) {
            ret = RR_ERROR;
            break;
        }

        if (!strcasecmp(re->elements[0].ptr, "ENTRY")) {
            if ((e = parseRaftLogEntry(re)) == NULL) {
                freeRawLogEntry(re);
                ret = RR_ERROR;
                break;
            }
            if (cb) {
                cb(cb_arg, e, log->index);
            }

            removed++;
            log->index--;
            log->num_entries--;

            raft_entry_release(e);

            ftruncate(fileno(log->file), offset);
        }

        freeRawLogEntry(re);
    }

    return ret;
}

RRStatus RaftLogSetVote(RaftLog *log, raft_node_id_t vote)
{
    TRACE_LOG_OP("RaftLogSetVote(vote=%ld)", vote);
    log->vote = vote;
    if (updateLogHeader(log) < 0) {
        return RR_ERROR;
    }
    return RR_OK;
}

RRStatus RaftLogSetTerm(RaftLog *log, raft_term_t term, raft_node_id_t vote)
{
    TRACE_LOG_OP("RaftLogSetTerm(term=%lu,vote=%ld)", term, vote);
    log->term = term;
    log->vote = vote;
    if (updateLogHeader(log) < 0) {
        return RR_ERROR;
    }
    return RR_OK;
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
long long int RaftLogRewrite(RedisRaftCtx *rr, const char *filename, raft_index_t last_idx, raft_term_t last_term)
{
    RaftLog *log = RaftLogCreate(filename, rr->snapshot_info.dbid,
            last_term, last_idx,
            raft_get_current_term(rr->raft),
            raft_get_voted_for(rr->raft),
            rr->config);
    long long int num_entries = 0;

    raft_index_t i;
    for (i = last_idx + 1; i <= RaftLogCurrentIdx(rr->log); i++) {
        num_entries++;
        raft_entry_t *ety = raft_get_entry_from_idx(rr->raft, i);
        if (RaftLogWriteEntry(log, ety) != RR_OK) {
            RaftLogClose(log);
            return -1;
        }
        raft_entry_release(ety);
    }

    if (RaftLogSync(log) != RR_OK) {
        RaftLogClose(log);
        return -1;
    }

    RaftLogClose(log);
    return num_entries;
}

void RaftLogRemoveFiles(const char *filename)
{
    char *idx_filename = getIndexFilename(filename);

    LOG_DEBUG("Removing Raft Log files: %s", filename);
    unlink(filename);
    unlink(idx_filename);

    RedisModule_Free(idx_filename);
}

void RaftLogArchiveFiles(RedisRaftCtx *rr)
{
    char *idx_filename = getIndexFilename(rr->config->raft_log_filename);
    unlink(idx_filename);
    RedisModule_Free(idx_filename);

    char bak_filename_maxlen = strlen(rr->config->raft_log_filename) + 100;
    char bak_filename[bak_filename_maxlen];
    snprintf(bak_filename, bak_filename_maxlen - 1,
            "%s.%d.bak", rr->config->raft_log_filename, raft_get_nodeid(rr->raft));
    rename(rr->config->raft_log_filename, bak_filename);
}

RRStatus RaftLogRewriteSwitch(RedisRaftCtx *rr, RaftLog *new_log, unsigned long new_log_entries)
{
    /* Rename Raft log.  If we fail, we can assume the old log is still
     * okay and we can just cancel the operation.
     */
    if (rename(new_log->filename, rr->log->filename) < 0) {
        LOG_ERROR("Failed to switch Raft log: %s to %s: %s",
                new_log->filename, rr->log->filename, strerror(errno));
        return RR_ERROR;
    }

    /* Rename the index.  If we fail now we're in inconsistent state, so we
     * panic and expect the index to be re-built when the process restarts.
     */
    char *log_idx_filename = getIndexFilename(rr->log->filename);
    char *new_idx_filename = getIndexFilename(new_log->filename);
    if (rename(new_idx_filename, log_idx_filename) < 0) {
        PANIC("Failed to switch Raft log index: %s to %s: %s",
               new_idx_filename, log_idx_filename, strerror(errno));
    }

    RedisModule_Free(log_idx_filename);
    RedisModule_Free(new_idx_filename);

    new_log->filename = rr->log->filename;
    new_log->num_entries = new_log_entries;
    new_log->index = new_log->snapshot_last_idx + new_log->num_entries;

    RaftLogClose(rr->log);
    rr->log = new_log;

    return RR_OK;
}

/*
 * Interface to Raft library.
 */

static void *logImplInit(void *raft, void *arg)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) arg;

    if (!rr->logcache) {
        rr->logcache = EntryCacheNew(ENTRY_CACHE_INIT_SIZE);
    }

    return rr;
}

static void logImplFree(void *rr_)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) rr_;

    RaftLogClose(rr->log);
    EntryCacheFree(rr->logcache);
}

static void logImplReset(void *rr_, raft_index_t index, raft_term_t term)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) rr_;

    /* Note: the RaftLogImpl API specifies the specified index is the one
     * to be assigned to the *next* entry, hence the adjustments below.
     */
    assert(index >= 1);
    RaftLogReset(rr->log, index - 1, term);

    TRACE_LOG_OP("Reset(index=%lu,term=%lu)", index, term);

    EntryCacheFree(rr->logcache);
    rr->logcache = EntryCacheNew(ENTRY_CACHE_INIT_SIZE);
}

static int logImplAppend(void *rr_, raft_entry_t *ety)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) rr_;
    TRACE_LOG_OP("Append(id=%d, term=%lu) -> index %lu", ety->id, ety->term, rr->log->index + 1);
    if (RaftLogAppend(rr->log, ety) != RR_OK) {
        return -1;
    }
    EntryCacheAppend(rr->logcache, ety, rr->log->index);
    return 0;
}

static int logImplPoll(void *rr_, raft_index_t first_idx)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) rr_;
    TRACE_LOG_OP("Poll(first_idx=%lu)", first_idx);
    EntryCacheDeleteHead(rr->logcache, first_idx);
    return 0;
}

static int logImplPop(void *rr_, raft_index_t from_idx, func_entry_notify_f cb, void *cb_arg)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) rr_;
    TRACE_LOG_OP("Delete(from_idx=%lu)", from_idx);
    EntryCacheDeleteTail(rr->logcache, from_idx);
    if (RaftLogDelete(rr->log, from_idx, cb, cb_arg) != RR_OK) {
        return -1;
    }
    return 0;
}

static raft_entry_t *logImplGet(void *rr_, raft_index_t idx)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) rr_;
    raft_entry_t *ety;

    ety = EntryCacheGet(rr->logcache, idx);
    if (ety != NULL) {
        TRACE_LOG_OP("Get(idx=%lu) -> (cache) id=%d, term=%lu",
                idx, ety->id, ety->term);
        return ety;
    }

    ety = RaftLogGet(rr->log, idx);
    TRACE_LOG_OP("Get(idx=%lu) -> (file) id=%d, term=%lu",
            idx, ety ? ety->id : -1, ety ? ety->term : 0);
    return ety;
}

static int logImplGetBatch(void *rr_, raft_index_t idx, int entries_n, raft_entry_t **entries)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) rr_;
    int n = 0;
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

    TRACE_LOG_OP("GetBatch(idx=%lu entries_n=%d) -> %d", idx, entries_n, n);
    return n;
}

static raft_index_t logImplFirstIdx(void *rr_)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) rr_;
    return RaftLogFirstIdx(rr->log);
}

static raft_index_t logImplCurrentIdx(void *rr_)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) rr_;
    return RaftLogCurrentIdx(rr->log);
}

static raft_index_t logImplCount(void *rr_)
{
    RedisRaftCtx *rr = (RedisRaftCtx *) rr_;
    return RaftLogCount(rr->log);
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
    .count = logImplCount
};
