#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <sys/uio.h>

#include <assert.h>

#include "redisraft.h"

#define INITIAL_LOG_OFFSET  sizeof(RaftLogHeader)

/*
 * Entries Cache.
 */

EntryCache *EntryCacheNew(unsigned long initial_size, raft_index_t start_idx)
{
    EntryCache *cache = RedisModule_Calloc(1, sizeof(EntryCache));

    cache->size = initial_size;
    cache->ptrs = RedisModule_Calloc(cache->size, sizeof(raft_entry_t *));
    cache->start_idx = start_idx;

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

void EntryCacheAppend(EntryCache *cache, raft_entry_t *ety)
{
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
        cache->start_idx++;
        raft_entry_release(cache->ptrs[cache->start]);
        cache->ptrs[cache->start] = NULL;
        cache->start++;
        if (cache->start >= cache->size) {
            cache->start = 0;
        }
        cache->len--;
        deleted++;
    }

    return deleted;
}

long EntryCacheDeleteTail(EntryCache *cache, raft_index_t index)
{
    long deleted = 0;
    raft_index_t i;

    if (index >= cache->start_idx + cache->len) {
        return -1;
    }

    for (i = index; i < cache->start_idx + cache->len; i++) {
        unsigned long int relidx = i - cache->start_idx;
        unsigned long int ofs = (cache->start + relidx) % cache->size;
        raft_entry_release(cache->ptrs[ofs]);
        cache->ptrs[ofs] = NULL;
        cache->len--;
        deleted++;
    }

    return deleted;
}

void RaftLogClose(RaftLog *log)
{
    fclose(log->file);
    fclose(log->idxfile);
    RedisModule_Free(log);
}

/*
 * Raw reading/writing of Raft log.
 */

static int writeBegin(RaftLog *log, int length)
{
    if (fprintf(log->file, "*%u\r\n", length) < 0) {
        return -1;
    }

    return 0;
}

static int writeEnd(RaftLog *log)
{
    if (fflush(log->file) < 0 ||
        fsync(fileno(log->file)) < 0) {
            return -1;
    }

    return 0;
}

static int writeBuffer(RaftLog *log, const void *buf, size_t buf_len)
{
    static const char crlf[] = "\r\n";

    if (fprintf(log->file, "$%zu\r\n", buf_len) < 0 ||
        fwrite(buf, 1, buf_len, log->file) < buf_len ||
        fwrite(crlf, 1, 2, log->file) < 2) {
            return -1;
    }

    return 0;
}


static int writeInteger(RaftLog *log, long value)
{
    char buf[25];

    snprintf(buf, sizeof(buf) - 1, "%ld", value);
    if (fprintf(log->file, "$%zu\r\n%s\r\n", strlen(buf), buf) < 0) {
        return -1;
    }

    return 0;
}

static int writeUnsignedInteger(RaftLog *log, unsigned long value)
{
    char buf[25];

    snprintf(buf, sizeof(buf) - 1, "%lu", value);
    if (fprintf(log->file, "$%zu\r\n%s\r\n", strlen(buf), buf) < 0) {
        return -1;
    }

    return 0;
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

static int updateIndex(RaftLog *log, raft_index_t index, off64_t offset)
{
    long relidx = index - log->snapshot_last_idx;

    if (fseek(log->idxfile, sizeof(off64_t) * relidx, SEEK_SET) < 0 ||
            fwrite(&offset, sizeof(off64_t), 1, log->idxfile) != 1) {
        return -1;
    }

    return 0;
}

RaftLog *prepareLog(const char *filename)
{
    FILE *file = fopen(filename, "a+");
    if (!file) {
        LOG_ERROR("Raft Log: %s: %s\n", filename, strerror(errno));
        return NULL;
    }

    int idx_filename_len = strlen(filename) + 10;
    char idx_filename[idx_filename_len];
    snprintf(idx_filename, idx_filename_len - 1, "%s.idx", filename);
    FILE *idxfile = fopen(idx_filename, "w+");
    if (!file) {
        LOG_ERROR("Raft Log: %s: %s\n", idx_filename, strerror(errno));
        fclose(file);
        return NULL;
    }

    RaftLog *log = RedisModule_Calloc(1, sizeof(RaftLog));
    log->file = file;
    log->idxfile = idxfile;

    return log;
}

int writeLogHeader(RaftLog *log, raft_term_t term, raft_index_t index)
{
    if (writeBegin(log, 5) < 0 ||
        writeBuffer(log, "RAFTLOG", 7) < 0 ||
        writeUnsignedInteger(log, RAFTLOG_VERSION) < 0 ||
        writeBuffer(log, log->dbid, strlen(log->dbid)) < 0 ||
        writeUnsignedInteger(log, term) < 0 ||
        writeUnsignedInteger(log, index) < 0 ||
        writeEnd(log) < 0) {
            return -1;
    }

    return 0;
}

RaftLog *RaftLogCreate(const char *filename, const char *dbid, raft_term_t term,
        raft_index_t index)
{
    RaftLog *log = prepareLog(filename);
    if (!log) {
        return NULL;
    }

    log->index = log->snapshot_last_idx = index;
    log->snapshot_last_term = term;

    memcpy(log->dbid, dbid, RAFT_DBID_LEN);
    log->dbid[RAFT_DBID_LEN] = '\0';

    /* Truncate */
    ftruncate(fileno(log->file), 0);
    ftruncate(fileno(log->idxfile), 0);

    /* Write log start */
    if (writeLogHeader(log, term, index) < 0) {
        LOG_ERROR("Failed to create Raft log: %s: %s\n", filename, strerror(errno));
        RaftLogClose(log);
        log = NULL;
    }

    return log;
}

int readRaftLogEntryLen(RaftLog *log, size_t *entry_len)
{
    size_t nread = fread(entry_len, 1, sizeof(*entry_len), log->file);

    /* EOF */
    if (!nread) {
        return 0;
    }

    if (nread < 0) {
        LOG_ERROR("Error reading log entry: %s\n", strerror(errno));
        return -1;
    } else if (nread < sizeof(*entry_len)) {
        LOG_ERROR("Short read reading log entry: %zd/%zd\n",
                nread, sizeof(*entry_len));
        return -1;
    }

    return 1;
}

static int parseRaftLogEntry(RawLogEntry *re, raft_entry_t *e)
{
    char *eptr;

    if (re->num_elements != 5) {
        LOG_ERROR("Log entry: invalid number of arguments: %d\n", re->num_elements);
        return -1;
    }

    e->term = strtoul(re->elements[1].ptr, &eptr, 10);
    if (*eptr) {
        return -1;
    }

    e->id = strtoul(re->elements[2].ptr, &eptr, 10);
    if (*eptr) {
        return -1;
    }

    e->type = strtoul(re->elements[3].ptr, &eptr, 10);
    if (*eptr) {
        return -1;
    }

    e->data.len = re->elements[4].len;
    e->data.buf = re->elements[4].ptr;
    return 0;
}

raft_entry_t *copyFromRawEntry(raft_entry_t *target, RawLogEntry *re)
{
    if (parseRaftLogEntry(re, target) < 0) {
        return NULL;
    }

    /* Unlink buffer from raw entry, so it doesn't get freed */
    re->elements[4].ptr = NULL;
    freeRawLogEntry(re);

    return target;
}

static int handleTerm(RaftLog *log, RawLogEntry *re)
{
    char *eptr;

    if (re->num_elements != 3) {
        LOG_ERROR("Log entry: TERM: invalid number of arguments: %d\n", re->num_elements);
        return -1;
    }

    log->term = strtoul(re->elements[1].ptr, &eptr, 10);
    if (*eptr) {
        return -1;
    }
    log->vote = strtol(re->elements[2].ptr, &eptr, 10);
    if (*eptr) {
        return -1;
    }

    return 0;
}

static int handleHeader(RaftLog *log, RawLogEntry *re)
{
    if (re->num_elements != 5 ||
        strcmp(re->elements[0].ptr, "RAFTLOG")) {
        LOG_ERROR("Invalid Raft log header.");
        return -1;
    }

    char *eptr;
    unsigned long ver = strtoul(re->elements[1].ptr, &eptr, 10);
    if (*eptr != '\0' || ver != RAFTLOG_VERSION) {
        LOG_ERROR("Invalid Raft header version: %lu\n", ver);
        return -1;
    }

    if (strlen(re->elements[2].ptr) > RAFT_DBID_LEN) {
        LOG_ERROR("Invalid Raft log dbid: %s\n", re->elements[2].ptr);
        return -1;
    }
    strcpy(log->dbid, re->elements[2].ptr);

    log->term = log->snapshot_last_term = strtoul(re->elements[3].ptr, &eptr, 10);
    if (*eptr != '\0') {
        LOG_ERROR("Invalid Raft log term: %s\n", re->elements[3].ptr);
        return -1;
    }

    log->index = log->snapshot_last_idx = strtoul(re->elements[4].ptr, &eptr, 10);
    if (*eptr != '\0') {
        LOG_ERROR("Invalid Raft log index: %s\n", re->elements[4].ptr);
        return -1;
    }

    return 0;
}

static int handleVote(RaftLog *log, RawLogEntry *re)
{
    char *eptr;

    if (re->num_elements != 2) {
        LOG_ERROR("Log entry: VOTE: invalid number of arguments: %d\n", re->num_elements);
        return -1;
    }

    log->vote = strtol(re->elements[1].ptr, &eptr, 10);
    if (*eptr) {
        return -1;
    }

    return 0;
}

RaftLog *RaftLogOpen(const char *filename)
{
    RaftLog *log = prepareLog(filename);
    if (!log) {
        return NULL;
    }

    /* Read start */
    fseek(log->file, 0L, SEEK_SET);

    RawLogEntry *e = NULL;
    if (readRawLogEntry(log, &e) < 0) {
        LOG_ERROR("Failed to read Raft log: %s\n", errno ? strerror(errno) : "invalid data");
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
    RedisModule_Free(log);
    return NULL;
}

RRStatus RaftLogReset(RaftLog *log, raft_term_t term, raft_index_t index)
{
    log->index = log->snapshot_last_idx = index;
    log->snapshot_last_term = term;

    if (ftruncate(fileno(log->file), 0) < 0 ||
        ftruncate(fileno(log->idxfile), 0) < 0 ||
        writeLogHeader(log, term, index)) {

        return RR_ERROR;
    }

    return RR_OK;
}

int RaftLogLoadEntries(RaftLog *log, int (*callback)(void *, LogEntryAction, raft_entry_t *), void *callback_arg)
{
    int ret = 0;

    if (fseek(log->file, 0, SEEK_SET) < 0) {
        return -1;
    }

    log->term = 1;
    log->index = 0;

    do {
        RawLogEntry *re;
        raft_entry_t e;
        LogEntryAction action;

        long offset = ftell(log->file);
        if (readRawLogEntry(log, &re) < 0 || !re->num_elements) {
            break;
        }

        if (!strcasecmp(re->elements[0].ptr, "ENTRY")) {
            memset(&e, 0, sizeof(raft_entry_t));

            if (parseRaftLogEntry(re, &e) < 0) {
                freeRawLogEntry(re);
                ret = -1;
                break;
            }
            log->index++;
            action = LA_APPEND;
            ret++;

            updateIndex(log, log->index, offset);
        } else if (!strcasecmp(re->elements[0].ptr, "RAFTLOG")) {
            if (handleHeader(log, re) < 0) {
                return -1;
                break;
            }
            freeRawLogEntry(re);
            continue;
        } else if (!strcasecmp(re->elements[0].ptr, "TERM")) {
            int r = handleTerm(log, re);
            freeRawLogEntry(re);
            if (r < 0) {
                ret = -1;
                break;
            } else {
                continue;
            }
        } else if (!strcasecmp(re->elements[0].ptr, "VOTE")) {
            int r = handleVote(log, re);
            freeRawLogEntry(re);
            if (r < 0) {
                ret = -1;
                break;
            } else {
                continue;
            }
        } else {
            LOG_ERROR("Invalid log entry: %s\n", (char *) re->elements[0].ptr);
            freeRawLogEntry(re);

            ret = -1;
            break;
        }

        int cb_ret = 0;
        if (callback) {
            callback(callback_arg, action, action == LA_APPEND ? &e : NULL);
        }

        freeRawLogEntry(re);
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
    off64_t offset = ftell(log->file);

    if (writeBegin(log, 5) < 0 ||
        writeBuffer(log, "ENTRY", 5) < 0 ||
        writeUnsignedInteger(log, entry->term) < 0 ||
        writeUnsignedInteger(log, entry->id) < 0 ||
        writeUnsignedInteger(log, entry->type) < 0 ||
        writeBuffer(log, entry->data.buf, entry->data.len) < 0) {
        return RR_ERROR;
    }

    /* Update index */
    log->index++;
    if (updateIndex(log, log->index, offset) < 0) {
        return RR_ERROR;
    }

    return RR_OK;
}

RRStatus RaftLogSync(RaftLog *log)
{
    if (writeEnd(log) < 0) {
        return RR_ERROR;
    }
    return RR_OK;
}

RRStatus RaftLogAppend(RaftLog *log, raft_entry_t *entry)
{
    if (RaftLogWriteEntry(log, entry) != RR_OK ||
            writeEnd(log) < 0) {
        return RR_ERROR;
    }

    log->num_entries++;
    return RR_OK;
}

static off64_t seekEntry(RaftLog *log, raft_index_t idx)
{
    /* Bounds check */
    if (idx <= log->snapshot_last_idx) {
        return 0;
    }

    if (idx > log->snapshot_last_idx + log->num_entries) {
        return 0;
    }

    raft_index_t relidx = idx - log->snapshot_last_idx;
    off64_t offset;
    if (fseek(log->idxfile, sizeof(off64_t) * relidx, SEEK_SET) < 0 ||
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
    if (seekEntry(log, idx) < 0) {
        return NULL;
    }

    RawLogEntry *re;
    if (readRawLogEntry(log, &re) != RR_OK) {
        return NULL;
    }

    raft_entry_t *e = raft_entry_new();
    if (!copyFromRawEntry(e, re)) {
        raft_entry_release(e);
        freeRawLogEntry(re);
        return NULL;
    }

    return e;
}

RRStatus RaftLogDelete(RaftLog *log, raft_index_t from_idx, func_entry_notify_f cb, void *cb_arg)
{
    off64_t offset;
    raft_index_t idx = from_idx;
    RRStatus ret = RR_OK;

    if (!(offset = seekEntry(log, from_idx))) {
        return RR_ERROR;
    }

    do {
        RawLogEntry *re;
        raft_entry_t e;

        if (readRawLogEntry(log, &re) < 0) {
            break;
        }

        if (!strcasecmp(re->elements[0].ptr, "ENTRY")) {
            memset(&e, 0, sizeof(raft_entry_t));
            if (parseRaftLogEntry(re, &e) < 0) {
                freeRawLogEntry(re);
                ret = RR_ERROR;
                break;
            }

            cb(cb_arg, &e, idx);
            idx++;

            freeRawLogEntry(re);
        }
    } while(1);

    ftruncate(fileno(log->file), offset);
    unsigned long removed = log->index - from_idx + 1;
    log->num_entries -= removed;
    log->index = from_idx - 1;

    return ret;
}

RRStatus RaftLogSetVote(RaftLog *log, raft_node_id_t vote)
{
    log->vote = vote;

    if (writeBegin(log, 2) < 0 ||
        writeBuffer(log, "VOTE", 4) < 0 ||
        writeInteger(log, vote) < 0 ||
        writeEnd(log) < 0) {
        return RR_ERROR;
    }
    return RR_OK;
}

RRStatus RaftLogSetTerm(RaftLog *log, raft_term_t term, raft_node_id_t vote)
{
    log->term = term;
    log->vote = vote;

    if (writeBegin(log, 3) < 0 ||
        writeBuffer(log, "TERM", 4) < 0 ||
        writeUnsignedInteger(log, term) < 0 ||
        writeInteger(log, vote) < 0 ||
        writeEnd(log) < 0) {
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
