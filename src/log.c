/*
 * Copyright Redis Ltd. 2020 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "log.h"

#include "entrycache.h"
#include "file.h"
#include "redisraft.h"

#include "common/sc_crc32.h"

#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

static const int ENTRY_CACHE_INIT_SIZE = 512;
static const int ENTRY_ELEM_COUNT = 7;
static const char *ENTRY_STR = "ENTRY";

static const int RAFTLOG_VERSION = 1;
static const int RAFTLOG_ELEM_COUNT = 7;
static const char *RAFTLOG_STR = "RAFTLOG";

#define RAFTLOG_TRACE(fmt, ...) TRACE_MODULE(RAFTLOG, "<raftlog> " fmt, ##__VA_ARGS__)

static void pageFree(LogPage *p, bool delete_files);
static int pageSync(LogPage *p, bool sync);
static int pageTruncateFiles(LogPage *p, size_t offset, size_t idxoffset);

static size_t pageGenerateHeader(LogPage *p, unsigned char *buf, size_t len)
{
    unsigned char *pos = buf;
    unsigned char *end = buf + len;

    pos += multibulkWriteLen(pos, end - pos, '*', RAFTLOG_ELEM_COUNT);
    pos += multibulkWriteStr(pos, end - pos, RAFTLOG_STR);
    pos += multibulkWriteLong(pos, end - pos, RAFTLOG_VERSION);
    pos += multibulkWriteStr(pos, end - pos, p->dbid);
    pos += multibulkWriteLong(pos, end - pos, p->node_id);
    pos += multibulkWriteLong(pos, end - pos, p->prev_log_term);
    pos += multibulkWriteLong(pos, end - pos, p->prev_log_idx);

    return pos - buf;
}

static int pageWriteHeader(LogPage *p)
{
    unsigned char buf[1024];
    unsigned char *pos;
    unsigned char *end = buf + sizeof(buf);
    ssize_t len = pageGenerateHeader(p, buf, sizeof(buf));
    pos = buf + len;

    /* add crc to header */
    long crc = sc_crc32(0, buf, len);
    pos += multibulkWriteLong(pos, end - pos, crc);
    len = pos - buf;

    if (pageTruncateFiles(p, 0, 0) != RR_OK ||
        FileWrite(&p->file, buf, len) != len ||
        pageSync(p, true) != RR_OK) {

        /* Try to delete files just in case there was a partial write. */
        pageTruncateFiles(p, 0, 0);
        return RR_ERROR;
    }

    p->current_crc = crc;
    return RR_OK;
}

static int pageReadHeader(LogPage *p, long *read_crc)
{
    char str[64] = {0};
    char dbid[64] = {0};
    int num_elements;
    long version;

    if (FileSetReadOffset(&p->file, 0) != RR_OK) {
        return RR_ERROR;
    }

    if (!multibulkReadLen(&p->file, '*', &num_elements) ||
        !multibulkReadStr(&p->file, str, sizeof(str)) ||
        !multibulkReadLong(&p->file, &version) ||
        !multibulkReadStr(&p->file, dbid, sizeof(dbid))) {
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
    raft_term_t prev_log_term;
    raft_index_t prev_log_idx;
    long crc;

    if (!multibulkReadInt(&p->file, &node_id) ||
        !multibulkReadLong(&p->file, &prev_log_term) ||
        !multibulkReadLong(&p->file, &prev_log_idx) ||
        !multibulkReadLong(&p->file, &crc)) {
        return RR_ERROR;
    }

    memcpy(p->dbid, dbid, sizeof(p->dbid));
    p->dbid[RAFT_DBID_LEN] = '\0';

    p->node_id = node_id;
    p->prev_log_term = prev_log_term;
    p->prev_log_idx = prev_log_idx;
    p->index = prev_log_idx;

    if (read_crc != NULL) {
        *read_crc = crc;
    }

    return RR_OK;
}

static size_t generateEntryHeader(raft_entry_t *ety, unsigned char *buf, size_t buf_len)
{
    unsigned char *pos = buf;
    unsigned char *end = buf + buf_len;

    pos += multibulkWriteLen(pos, end - pos, '*', ENTRY_ELEM_COUNT);
    pos += multibulkWriteStr(pos, end - pos, ENTRY_STR);
    pos += multibulkWriteLong(pos, end - pos, ety->term);
    pos += multibulkWriteLong(pos, end - pos, ety->id);
    pos += multibulkWriteUInt64(pos, end - pos, ety->session);
    pos += multibulkWriteInt(pos, end - pos, ety->type);

    pos += multibulkWriteLen(pos, end - pos, '$', (int) ety->data_len);

    return pos - buf;
}

static int pageWriteEntry(LogPage *p, raft_entry_t *ety)
{
    int rc;
    unsigned char buf[1024];
    ssize_t len;

    size_t offset = FileSize(&p->file);
    size_t idxoffset = FileSize(&p->idxfile);

    /* header */
    len = generateEntryHeader(ety, buf, sizeof(buf));
    if (FileWrite(&p->file, buf, len) != len) {
        LOG_WARNING("FileWrite() failed for the file: %s", p->filename);
        goto error;
    }

    /* data */
    if (FileWrite(&p->file, ety->data, ety->data_len) != ety->data_len ||
        FileWrite(&p->file, "\r\n", 2) != 2) {
        LOG_WARNING("FileWrite() failed for the file: %s", p->filename);
        goto error;
    }

    /* crc */
    /* accumulating crc, so start with current crc */
    long crc = sc_crc32(p->current_crc, buf, len);
    crc = sc_crc32(crc, (unsigned char *) ety->data, ety->data_len);
    crc = sc_crc32(crc, (unsigned char *) "\r\n", 2);

    /* write crc as added element */
    len = multibulkWriteLong(buf, sizeof(buf), crc);
    if (FileWrite(&p->file, buf, len) != len) {
        LOG_WARNING("FileWrite() failed for the file: %s", p->filename);
        goto error;
    }

    ssize_t ret = FileWrite(&p->idxfile, &offset, sizeof(offset));
    if (ret != sizeof(offset)) {
        LOG_WARNING("FileWrite() failed for the file: %s", p->idxfilename);
        goto error;
    }

    /* everything succeeded, so can update accumulated crc */
    p->current_crc = crc;

    return RR_OK;

error:
    /* Try to revert file changes. */
    rc = pageTruncateFiles(p, offset, idxoffset);
    RedisModule_Assert(rc == RR_OK);

    return RR_ERROR;
}

static raft_entry_t *pageReadEntry(LogPage *p, long *read_crc)
{
    char str[64] = {0};
    int num_elements;

    if (!multibulkReadLen(&p->file, '*', &num_elements) ||
        !multibulkReadStr(&p->file, str, sizeof(str))) {
        return NULL;
    }

    if (num_elements != ENTRY_ELEM_COUNT ||
        strncmp(ENTRY_STR, str, strlen(ENTRY_STR)) != 0) {
        return NULL;
    }

    raft_term_t term;
    raft_entry_id_t id;
    raft_session_t session_id;
    int type, length;

    /* header */
    if (!multibulkReadLong(&p->file, &term) ||
        !multibulkReadInt(&p->file, &id) ||
        !multibulkReadUInt64(&p->file, &session_id) ||
        !multibulkReadInt(&p->file, &type) ||
        !multibulkReadLen(&p->file, '$', &length)) {
        return NULL;
    }

    char crlf[2];
    raft_entry_t *e = raft_entry_new(length);

    /* data */
    if (FileRead(&p->file, e->data, length) != length ||
        FileRead(&p->file, crlf, 2) != 2 ||
        crlf[0] != '\r' || crlf[1] != '\n') {
        goto error;
    }

    /* crc */
    long crc;
    if (!multibulkReadLong(&p->file, &crc)) {
        goto error;
    }
    if (read_crc != NULL) {
        *read_crc = crc;
    }

    e->term = term;
    e->id = id;
    e->session = session_id;
    e->type = type;

    return e;

error:
    raft_entry_release(e);
    return NULL;
}

static LogPage *pagePrepare(const char *filename)
{
    int rc;
    LogPage *p = RedisModule_Calloc(1, sizeof(*p));

    safesnprintf(p->filename, sizeof(p->filename), "%s", filename);
    safesnprintf(p->idxfilename, sizeof(p->idxfilename), "%s.idx", filename);

    FileInit(&p->file);
    FileInit(&p->idxfile);

    rc = FileOpen(&p->file, filename, O_APPEND | O_RDWR | O_CREAT);
    if (rc != RR_OK) {
        goto error;
    }

    int mode = O_APPEND | O_RDWR | O_CREAT | O_TRUNC;

    rc = FileOpen(&p->idxfile, p->idxfilename, mode);
    if (rc != RR_OK) {
        goto error;
    }

    return p;

error:
    pageFree(p, false);
    return NULL;
}

static LogPage *pageCreate(const char *filename, const char *dbid,
                           raft_node_id_t node_id, raft_term_t prev_log_term,
                           raft_index_t prev_log_idx)
{
    LogPage *p = pagePrepare(filename);
    if (!p) {
        return NULL;
    }

    memcpy(p->dbid, dbid, RAFT_DBID_LEN);
    p->dbid[RAFT_DBID_LEN] = '\0';

    p->index = prev_log_idx;
    p->prev_log_idx = prev_log_idx;
    p->prev_log_term = prev_log_term;
    p->node_id = node_id;

    if (pageWriteHeader(p) != RR_OK) {
        LOG_WARNING("Failed to create log: %s: %s", filename, strerror(errno));
        pageFree(p, true);
        return NULL;
    }

    /* Directory fsync() is required to make file creation operation durable. */
    fsyncDir(filename);

    return p;
}

static void pageFree(LogPage *p, bool delete_files)
{
    if (!p) {
        return;
    }

    FileTerm(&p->file);
    FileTerm(&p->idxfile);

    if (delete_files) {
        unlink(p->idxfilename);
        unlink(p->filename);

        /* Call fsync for the directory to make above unlink() durable. */
        fsyncDir(p->filename);
    }
    RedisModule_Free(p);
}

static LogPage *pageOpen(const char *filename)
{
    LogPage *p = pagePrepare(filename);
    if (!p) {
        return NULL;
    }

    long read_crc;
    if (pageReadHeader(p, &read_crc) != RR_OK) {
        LOG_WARNING("Failed to read log file header, file=%s", p->filename);
        pageFree(p, false);
        return NULL;
    }

    /* validate log header crc */
    unsigned char buf[1024];
    ssize_t len = pageGenerateHeader(p, buf, sizeof(buf));
    if (sc_crc32(0, buf, len) != read_crc) {
        LOG_WARNING("logfile fails crc check, starting from scratch");
        pageFree(p, false);
        return NULL;
    }

    return p;
}

static int pageTruncateFiles(LogPage *p, size_t offset, size_t idxoffset)
{
    if (FileTruncate(&p->file, offset) != RR_OK ||
        FileTruncate(&p->idxfile, idxoffset) != RR_OK) {
        return RR_ERROR;
    }

    return RR_OK;
}

static bool validateEntryCRC(raft_entry_t *e, long read_crc, long current_crc, long *calc_crc)
{
    size_t off = 0;
    unsigned char buf[1024];

    /* generate the entry as it should be on disk, and calculate crc */
    off = generateEntryHeader(e, buf, sizeof(buf));
    *calc_crc = sc_crc32(current_crc, buf, off);
    *calc_crc = sc_crc32(*calc_crc, (unsigned char *) e->data, e->data_len);
    *calc_crc = sc_crc32(*calc_crc, (unsigned char *) "\r\n", 2);

    if (*calc_crc != read_crc) {
        return true;
    }

    return false;
}

static int pageLoadEntries(LogPage *p)
{
    long calc_crc = 0, read_crc = 0;

    p->num_entries = 0;

    if (pageReadHeader(p, &read_crc) != RR_OK) {
        return RR_ERROR;
    }

    /* already validated crc in LogOpen, update running crc */
    p->current_crc = read_crc;

    /* Read Entries */
    while (true) {
        uint64_t offset = (uint64_t) FileGetReadOffset(&p->file);
        raft_entry_t *e = pageReadEntry(p, &read_crc);
        if (!e) {
            size_t bytes = FileSize(&p->file) - offset;
            if (bytes > 0) {
                LOG_WARNING("Found partial entry at the end of the log file. "
                            "Discarding %zu bytes.",
                            bytes);

                int rc = FileTruncate(&p->file, offset);
                if (rc != RR_OK) {
                    PANIC("FileTruncate() failed for the file: %s", p->filename);
                }
            }

            return RR_OK;
        }

        bool error = validateEntryCRC(e, read_crc, p->current_crc, &calc_crc);
        raft_entry_release(e);
        if (error) {
            LOG_WARNING("Entry failed crc32 check, truncating log to "
                        "previous entry: %ld",
                        p->index);

            int rc = FileTruncate(&p->file, offset);
            if (rc != RR_OK) {
                PANIC("FileTruncate() failed for the file: %s", p->filename);
            }

            return RR_OK;
        }

        p->current_crc = calc_crc;
        p->index++;
        p->num_entries++;

        ssize_t rc = FileWrite(&p->idxfile, &offset, sizeof(offset));
        if (rc != sizeof(offset)) {
            PANIC("FileWrite() failed for the file: %s", p->idxfilename);
        }
    }
}

static int pageSync(LogPage *p, bool sync)
{
    if (FileFlush(&p->file) != RR_OK) {
        LOG_WARNING("FileFlush() failed for the file: %s", p->filename);
        return RR_ERROR;
    }

    if (sync) {
        if (FileFsync(&p->file) != RR_OK) {
            LOG_WARNING("FileFsync() failed for the file: %s", p->filename);
            return RR_ERROR;
        }
    }

    return RR_OK;
}

static int pageAppend(LogPage *p, raft_entry_t *entry)
{
    if (pageWriteEntry(p, entry) != RR_OK) {
        return RR_ERROR;
    }

    p->index++;
    p->num_entries++;

    return RR_OK;
}

static size_t pageSeekEntry(LogPage *p, raft_index_t idx)
{
    uint64_t offset;
    size_t idxoffset = (sizeof(uint64_t) * (idx - p->prev_log_idx - 1));

    /* Bounds check */
    if (idx <= p->prev_log_idx ||
        idx > p->prev_log_idx + p->num_entries) {
        return 0;
    }

    if (FileSetReadOffset(&p->idxfile, idxoffset) != RR_OK ||
        FileRead(&p->idxfile, &offset, sizeof(offset)) != sizeof(offset) ||
        FileSetReadOffset(&p->file, offset) != RR_OK) {
        return 0;
    }

    return offset;
}

static raft_entry_t *pageGet(LogPage *p, raft_index_t idx)
{
    if (pageSeekEntry(p, idx) <= 0) {
        return NULL;
    }

    raft_entry_t *ety = pageReadEntry(p, NULL);
    RedisModule_Assert(ety != NULL);

    return ety;
}

static int pageDelete(LogPage *p, raft_index_t from_idx)
{
    if (from_idx <= p->prev_log_idx || from_idx > p->index) {
        return RR_ERROR;
    }

    raft_index_t count = p->index - from_idx + 1;
    if (count != 0) {
        size_t offset = pageSeekEntry(p, from_idx);
        if (offset == 0) {
            return RR_ERROR;
        }

        raft_index_t relidx = from_idx - p->prev_log_idx - 1;
        size_t idxoffset = relidx * sizeof(uint64_t);

        if (pageTruncateFiles(p, offset, idxoffset) != RR_OK) {
            PANIC("ftruncate failed: %s", strerror(errno));
        }

        p->num_entries -= count;
        p->index -= count;

        /* Update running crc value */
        /* If we truncated the entire log file, it's the crc value of the header
         * otherwise, it's the crc from the last entry.
         */
        if (p->num_entries == 0) { /* header */
            /* Calculate running crc value by just regenerating header buf. */
            unsigned char buf[1024];
            size_t len = pageGenerateHeader(p, buf, sizeof(buf));
            p->current_crc = sc_crc32(0, buf, len);
        } else { /* log entry */
            /* To regenerate running crc value, need to reread last entry. */
            long read_crc;

            size_t pos = pageSeekEntry(p, p->index);
            RedisModule_Assert(pos > 0);

            raft_entry_t *e = pageReadEntry(p, &read_crc);
            raft_entry_release(e);

            p->current_crc = read_crc;
        }
    }

    return RR_OK;
}

void LogInit(Log *log)
{
    *log = (Log){0};
}

void LogTerm(Log *log)
{
    pageFree(log->pages[0], false);
    pageFree(log->pages[1], false);
}

int LogCreate(Log *log, const char *filename, const char *dbid,
              raft_node_id_t node_id, raft_term_t prev_log_term,
              raft_index_t prev_log_index)
{
    RedisModule_Assert(strlen(dbid) == RAFT_DBID_LEN);

    log->pages[0] = pageCreate(filename, dbid, node_id, prev_log_term,
                               prev_log_index);
    if (!log->pages[0]) {
        return RR_ERROR;
    }

    memcpy(log->dbid, log->pages[0]->dbid, RAFT_DBID_LEN);
    log->dbid[RAFT_DBID_LEN] = '\0';
    log->node_id = node_id;

    return RR_OK;
}

static char *secondPageFileName(char *buf, size_t size, const char *filename)
{
    safesnprintf(buf, size, "%s.1", filename);
    return buf;
}

int LogOpen(Log *log, const char *filename)
{
    log->pages[0] = pageOpen(filename);
    if (!log->pages[0]) {
        return RR_ERROR;
    }

    memcpy(log->dbid, log->pages[0]->dbid, RAFT_DBID_LEN);
    log->dbid[RAFT_DBID_LEN] = '\0';
    log->node_id = log->pages[0]->node_id;

    char buf[PATH_MAX];
    secondPageFileName(buf, sizeof(buf), filename);

    if (access(buf, F_OK) != 0) {
        return RR_OK;
    }

    log->pages[1] = pageOpen(buf);
    if (!log->pages[1]) {
        return RR_ERROR;
    }

    LogPage *p0 = log->pages[0];
    LogPage *p1 = log->pages[1];

    if (p0->node_id != p1->node_id || strcmp(p0->dbid, p1->dbid) != 0) {
        PANIC("Log pages do not match: p0 node_id=%d, dbid=%s, "
              "p1 node_id=%d, dbid=%s",
              p0->node_id, p0->dbid, p1->node_id, p1->dbid);
    }

    return RR_OK;
}

size_t LogFileSize(Log *log)
{
    size_t n = 0;

    if (log->pages[0]) {
        n += FileSize(&log->pages[0]->file);
    }

    if (log->pages[1]) {
        n += FileSize(&log->pages[1]->file);
    }

    return n;
}

raft_node_id_t LogNodeId(Log *log)
{
    return log->node_id;
}

const char *LogDbid(Log *log)
{
    return log->dbid;
}

int LogAppend(Log *log, raft_entry_t *entry)
{
    LogPage *last = log->pages[1] ? log->pages[1] : log->pages[0];
    return pageAppend(last, entry);
}

int LogLoadEntries(Log *log)
{
    LogPage *p0 = log->pages[0];
    LogPage *p1 = log->pages[1];

    if (pageLoadEntries(p0) != RR_OK) {
        return RR_ERROR;
    }

    if (p1) {
        if (pageLoadEntries(p1) != RR_OK) {
            return RR_ERROR;
        }

        /* If second page is empty or if there is a gap between the first and
         * second page, delete the second page. */
        if (p1->num_entries == 0 || p1->prev_log_idx != p0->index) {
            LOG_WARNING("Deleting second log page=%s, first page index:%ld, "
                        "num_entries=%ld, prev_log_index:%ld",
                        p1->filename, p0->index, p1->num_entries,
                        p1->prev_log_idx);

            pageFree(p1, true);
            log->pages[1] = NULL;
        }
    }

    return RR_OK;
}

raft_term_t LogPrevLogTerm(Log *log)
{
    return log->pages[0]->prev_log_term;
}

raft_index_t LogPrevLogIndex(Log *log)
{
    return log->pages[0]->prev_log_idx;
}

raft_index_t LogCompactionIdx(Log *log)
{
    return log->pages[0]->index;
}

raft_index_t LogFirstIdx(Log *log)
{
    return log->pages[0]->prev_log_idx + 1;
}

raft_index_t LogCurrentIdx(Log *log)
{
    LogPage *last = log->pages[1] ? log->pages[1] : log->pages[0];
    return last->index;
}

int LogSync(Log *log, bool sync)
{
    LogPage *curr = log->pages[1] ? log->pages[1] : log->pages[0];
    uint64_t begin = RedisModule_MonotonicMicroseconds();

    if (pageSync(curr, sync) != RR_OK) {
        PANIC("pageFsync() failed for the file: %s", curr->filename);
    }

    uint64_t took = RedisModule_MonotonicMicroseconds() - begin;
    log->fsync_index = LogCurrentIdx(log);
    log->fsync_count++;
    log->fsync_total += took;
    log->fsync_max = MAX(took, log->fsync_max);

    return RR_OK;
}

int LogFlush(Log *log)
{
    LogPage *curr = log->pages[1] ? log->pages[1] : log->pages[0];

    if (FileFlush(&curr->file) != RR_OK) {
        PANIC("FileFlush() failed for the file: %s", curr->filename);
    }

    return RR_OK;
}

int LogCurrentFd(Log *log)
{
    LogPage *curr = log->pages[1] ? log->pages[1] : log->pages[0];
    return curr->file.fd;
}

raft_entry_t *LogGet(Log *log, raft_index_t idx)
{
    raft_entry_t *ety = NULL;

    if (log->pages[1]) {
        ety = pageGet(log->pages[1], idx);
    }

    if (!ety) {
        ety = pageGet(log->pages[0], idx);
    }

    return ety;
}

int LogDelete(Log *log, raft_index_t from_idx)
{
    LogPage *p0 = log->pages[0];
    LogPage *p1 = log->pages[1];

    if (LogCount(log) == 0 ||
        from_idx < LogFirstIdx(log) || from_idx > LogCurrentIdx(log)) {
        return RR_ERROR;
    }

    pageDelete(p0, from_idx);

    if (p1) {
        pageDelete(p1, from_idx);

        /* If we've deleted an entry from the first page, we should delete the
         * second page as the entries are not subsequent anymore. */
        if (p0->index != p1->prev_log_idx) {
            pageFree(p1, true);
            log->pages[1] = NULL;
        }
    }

    return RR_OK;
}

int LogReset(Log *log, raft_index_t index, raft_term_t term)
{
    if (log->pages[1]) {
        pageFree(log->pages[1], true);
        log->pages[1] = NULL;
    }

    log->pages[0]->index = index;
    log->pages[0]->prev_log_idx = index;
    log->pages[0]->prev_log_term = term;

    return pageWriteHeader(log->pages[0]);
}

raft_index_t LogCount(Log *log)
{
    raft_index_t n = log->pages[0]->num_entries;

    if (log->pages[1]) {
        n += log->pages[1]->num_entries;
    }

    return n;
}

void LogArchiveFiles(Log *log)
{
    char buf[PATH_MAX + 100];
    LogPage *p0 = log->pages[0];
    LogPage *p1 = log->pages[1];

    unlink(p0->idxfilename);
    safesnprintf(buf, sizeof(buf), "%s.%d.bak", p0->filename, p0->node_id);
    rename(p0->filename, buf);

    if (p1) {
        unlink(p1->idxfilename);
        safesnprintf(buf, sizeof(buf), "%s.%d.bak", p1->filename, p1->node_id);
        rename(p1->filename, buf);
    }
}

int LogCompactionBegin(Log *log)
{
    char tmp[PATH_MAX];
    LogPage *p0 = log->pages[0];

    if (log->pages[1]) {
        return RR_OK;
    }

    if (p0->num_entries <= 1) {
        return RR_ERROR;
    }

    secondPageFileName(tmp, sizeof(tmp), p0->filename);

    raft_index_t idx = p0->index;
    raft_entry_t *e = pageGet(p0, idx);
    raft_term_t term = e->term;
    raft_entry_release(e);

    if (pageSync(p0, true) != RR_OK) {
        PANIC("pageFsync() failed for the file: %s", p0->filename);
    }

    log->pages[1] = pageCreate(tmp, p0->dbid, p0->node_id, term, idx);
    if (!log->pages[1]) {
        return RR_ERROR;
    }

    return RR_OK;
}

/* Rename page1 over page0 */
void LogCompactionEnd(Log *log)
{
    LogPage *p0 = log->pages[0];
    LogPage *p1 = log->pages[1];

    RedisModule_Assert(p1);

    if (rename(p1->idxfilename, p0->idxfilename) != 0) {
        PANIC("rename() failed: %s", strerror(errno));
    }

    if (syncRename(p1->filename, p0->filename) != RR_OK) {
        PANIC("rename() failed.");
    }

    strcpy(p1->filename, p0->filename);
    strcpy(p1->idxfilename, p0->idxfilename);

    pageFree(p0, false);

    log->pages[0] = p1;
    log->pages[1] = NULL;
}

bool LogCompactionStarted(Log *log)
{
    return log->pages[1] != NULL;
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

    LogTerm(&rr->log);
    EntryCacheFree(rr->logcache);
}

static void logImplReset(void *arg, raft_index_t index, raft_term_t term)
{
    RedisRaftCtx *rr = arg;

    /* Note: the RaftLogImpl API specifies the specified index is the one
     * to be assigned to the *next* entry, hence the adjustments below.
     */
    RedisModule_Assert(index >= 1);
    LogReset(&rr->log, index - 1, term);

    RAFTLOG_TRACE("Reset(index=%lu,term=%lu)", index, term);

    EntryCacheFree(rr->logcache);
    rr->logcache = EntryCacheNew(ENTRY_CACHE_INIT_SIZE);
}

static int logImplAppend(void *arg, raft_entry_t *ety)
{
    RedisRaftCtx *rr = arg;

    RAFTLOG_TRACE("Append(id=%d, term=%lu) -> index %lu",
                  ety->id, ety->term, LogCurrentIdx(&rr->log) + 1);

    if (LogAppend(&rr->log, ety) != RR_OK) {
        return -1;
    }
    EntryCacheAppend(rr->logcache, ety, LogCurrentIdx(&rr->log));
    return 0;
}

static int logImplPoll(void *arg, raft_index_t first_idx)
{
    RedisRaftCtx *rr = arg;

    RAFTLOG_TRACE("Poll(first_idx=%lu)", first_idx);
    EntryCacheDeleteHead(rr->logcache, first_idx);
    LogCompactionEnd(&rr->log);
    return 0;
}

static int logImplPop(void *arg, raft_index_t from_idx)
{
    RedisRaftCtx *rr = arg;

    RAFTLOG_TRACE("Delete(from_idx=%lu)", from_idx);

    EntryCacheDeleteTail(rr->logcache, from_idx);
    if (LogDelete(&rr->log, from_idx) != RR_OK) {
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

    ety = LogGet(&rr->log, idx);
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
            e = LogGet(&rr->log, idx + i);
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
    return LogFirstIdx(&rr->log);
}

static raft_index_t logImplCurrentIdx(void *arg)
{
    RedisRaftCtx *rr = arg;
    return LogCurrentIdx(&rr->log);
}

static raft_index_t logImplCount(void *arg)
{
    RedisRaftCtx *rr = arg;
    return LogCount(&rr->log);
}

static int logImplSync(void *arg)
{
    RedisRaftCtx *rr = arg;
    LogSync(&rr->log, rr->config.log_fsync);
    return RR_OK;
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
