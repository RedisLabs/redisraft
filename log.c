#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <sys/uio.h>

#include "redisraft.h"

#define INITIAL_LOG_OFFSET  sizeof(RaftLogHeader)

void RaftLogClose(RaftLog *log)
{
    fclose(log->file);
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

static int writeBuffer(RaftLog *log, void *buf, size_t buf_len)
{
    static const char crlf[] = "\r\n";

    if (fprintf(log->file, "$%zu\r\n", buf_len) < 0 ||
        fwrite(buf, 1, buf_len, log->file) < buf_len ||
        fwrite(crlf, 1, 2, log->file) < 2) {
            return -1;
    }

    return 0;
}

static int writeInteger(RaftLog *log, unsigned long value)
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

RaftLog *RaftLogCreate(const char *filename)
{
    FILE *file = fopen(filename, "w+");
    if (!file) {
        LOG_ERROR("Failed to create Raft log: %s: %s\n", filename, strerror(errno));
        return NULL;
    }

    RaftLog *log = RedisModule_Calloc(1, sizeof(RaftLog));
    log->file = file;

    /* Write log start */
    if (writeBegin(log, 2) < 0 ||
        writeBuffer(log, "RAFTLOG", 7) < 0 ||
        writeInteger(log, RAFTLOG_VERSION) < 0 ||
        writeEnd(log) < 0) {

        LOG_ERROR("Failed to create Raft log: %s: %s\n", filename, strerror(errno));

        RedisModule_Free(log);

        log = NULL;
    }

    return log;
}

RaftLog *RaftLogOpen(const char *filename)
{
    FILE *file = fopen(filename, "r+");
    if (!file) {
        LOG_ERROR("Failed top open Raft log: %s: %s\n", filename, strerror(errno));
        return NULL;
    }

    RaftLog *log = RedisModule_Calloc(1, sizeof(RaftLog));
    log->file = file;

    /* Read start */
    RawLogEntry *e = NULL;
    if (readRawLogEntry(log, &e) < 0) {
        LOG_ERROR("Failed to read Raft log: %s\n", errno ? strerror(errno) : "invalid data");
        goto error;
    }

    if (e->num_elements != 2 ||
        strcmp(e->elements[0].ptr, "RAFTLOG")) {
        LOG_ERROR("Invalid Raft log start command.");
        goto error;
    }

    char *eptr;
    unsigned long ver = strtoul(e->elements[1].ptr, &eptr, 10);
    if (*eptr != '\0' || ver != RAFTLOG_VERSION) {
        LOG_ERROR("Invalid Raft log version: %lu\n", ver);
        goto error;
    }

    freeRawLogEntry(e);
    return log;

error:
    freeRawLogEntry(e);
    RedisModule_Free(log);
    return NULL;
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
    log->vote = strtoul(re->elements[2].ptr, &eptr, 10);
    if (*eptr) {
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

    log->vote = strtoul(re->elements[1].ptr, &eptr, 10);
    if (*eptr) {
        return -1;
    }

    return 0;
}

static int handleSnapshot(RaftLog *log, RawLogEntry *re)
{
    char *eptr;
    raft_term_t term;
    raft_index_t idx;

    if (re->num_elements != 3) {
        LOG_ERROR("Log entry: SNAPSHOT: invalid number of arguments: %d\n", re->num_elements);
        return -1;
    }

    term = strtoul(re->elements[1].ptr, &eptr, 10);
    if (*eptr) {
        return -1;
    }

    idx = strtoul(re->elements[2].ptr, &eptr, 10);
    if (*eptr) {
        return -1;
    }

    return 0;
}

int RaftLogLoadEntries(RaftLog *log, int (*callback)(void *, LogEntryAction, raft_entry_t *), void *callback_arg)
{
    int ret = 0;

    if (fseek(log->file, 0, SEEK_SET) < 0) {
        return -1;
    }

    do {
        RawLogEntry *re;
        raft_entry_t e;
        LogEntryAction action;

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
            action = LA_APPEND;
            ret++;
        } else if (!strcasecmp(re->elements[0].ptr, "REMHEAD")) {
            action = LA_REMOVE_HEAD;
            ret--;
        } else if (!strcasecmp(re->elements[0].ptr, "REMTAIL")) {
            action = LA_REMOVE_TAIL;
            ret--;
        } else if (!strcasecmp(re->elements[0].ptr, "RAFTLOG")) {
            // Silently ignore
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
        } else if (!strcasecmp(re->elements[0].ptr, "SNAPSHOT")) {
            if (handleSnapshot(log, re) < 0) {
                ret = -1;
                break;
            }
            freeRawLogEntry(re);
            continue;
        } else {
            LOG_ERROR("Invalid log entry: %s\n", (char *) re->elements[0].ptr);
            freeRawLogEntry(re);

            ret = -1;
            break;
        }

        int cb_ret = callback(callback_arg, action, action == LA_APPEND ? &e : NULL);
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

bool RaftLogWriteEntry(RaftLog *log, raft_entry_t *entry)
{
    if (writeBegin(log, 5) < 0 ||
        writeBuffer(log, "ENTRY", 5) < 0 ||
        writeInteger(log, entry->term) < 0 ||
        writeInteger(log, entry->id) < 0 ||
        writeInteger(log, entry->type) < 0 ||
        writeBuffer(log, entry->data.buf, entry->data.len) < 0) {
        return false;
    }

    return true;
}

bool RaftLogWriteSnapshotInfo(RaftLog *log, raft_term_t term, raft_index_t idx)
{
    if (writeBegin(log, 3) < 0 ||
            writeBuffer(log, "SNAPSHOT", 8) < 0 ||
            writeInteger(log, term) < 0 ||
            writeInteger(log, idx) < 0) {
        return false;
    }

    return true;
}

bool RaftLogSync(RaftLog *log)
{
    if (writeEnd(log) < 0) {
        return false;
    }
    return true;
}

bool RaftLogAppend(RaftLog *log, raft_entry_t *entry)
{
    if (!RaftLogWriteEntry(log, entry) ||
            writeEnd(log) < 0) {
        return false;
    }

    log->num_entries++;
    return true;
}

bool RaftLogRemoveHead(RaftLog *log)
{
    if (!log->num_entries) {
        return false;
    }
    if (writeBegin(log, 1) < 0 ||
        writeBuffer(log, "REMHEAD", 7) < 0 ||
        writeEnd(log) < 0) {
        return false;
    }
    log->num_entries--;

    return true;
}

bool RaftLogRemoveTail(RaftLog *log)
{
    if (!log->num_entries) {
        return false;
    }
    if (writeBegin(log, 1) < 0 ||
        writeBuffer(log, "REMHEAD", 7) < 0 ||
        writeEnd(log) < 0) {
        return false;
    }
    log->num_entries--;

    return true;
}

bool RaftLogSetVote(RaftLog *log, raft_node_id_t vote)
{
    log->vote = vote;

    if (writeBegin(log, 2) < 0 ||
        writeBuffer(log, "VOTE", 4) < 0 ||
        writeInteger(log, vote) < 0 ||
        writeEnd(log) < 0) {
        return false;
    }
    return true;
}

bool RaftLogSetTerm(RaftLog *log, raft_term_t term, raft_node_id_t vote)
{
    log->term = term;
    log->vote = vote;

    if (writeBegin(log, 3) < 0 ||
        writeBuffer(log, "TERM", 4) < 0 ||
        writeInteger(log, term) < 0 ||
        writeInteger(log, vote) < 0 ||
        writeEnd(log) < 0) {
        return false;
    }
    return true;
}
