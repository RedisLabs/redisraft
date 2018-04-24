#include <unistd.h>
#include <string.h>

#include "redisraft.h"

#define INITIAL_LOG_OFFSET  sizeof(RaftLogHeader)

void RaftLogClose(RaftLog *log)
{
    close(log->fd);
    RedisModule_Free(log->header);
    RedisModule_Free(log);
}


RaftLog *RaftLogCreate(const char *filename, uint32_t node_id)
{
    int fd = open(filename, O_CREAT|O_RDWR|O_TRUNC, S_IRWXU|S_IRWXG);
    if (fd < 0) {
        LOG_ERROR("Failed to create Raft log: %s: %s\n", filename, strerror(errno));
        return NULL;
    }

    RaftLog *log = RedisModule_Calloc(1, sizeof(RaftLog));
    log->header = RedisModule_Calloc(1, sizeof(RaftLogHeader));
    log->fd = fd;

    log->header->version = RAFTLOG_VERSION;
    log->header->node_id = node_id;
    log->header->term = 0;
    log->header->entry_offset = INITIAL_LOG_OFFSET;

    if (write(log->fd, log->header, sizeof(RaftLogHeader)) < 0 ||
        fsync(log->fd) < 0) {

        LOG_ERROR("Failed to write Raft log header: %s: %s\n", filename, strerror(errno));

        RedisModule_Free(log->header);
        RedisModule_Free(log);

        log = NULL;
    }

    return log;
}

RaftLog *RaftLogOpen(const char *filename)
{
    int fd = open(filename, O_RDWR);
    if (fd < 0) {
        LOG_ERROR("Failed top open Raft log: %s: %s\n", filename, strerror(errno));
        return NULL;
    }

    RaftLog *log = RedisModule_Calloc(1, sizeof(RaftLog));
    log->header = RedisModule_Calloc(1, sizeof(RaftLogHeader));
    log->fd = fd;

    int bytes;
    if ((bytes = read(log->fd, log->header, sizeof(RaftLogHeader))) < sizeof(RaftLogHeader)) {
        LOG_ERROR("Failed to read Raft log header: %s\n", bytes < 0 ? strerror(errno) : "file too short");
        goto error;
    }

    if (log->header->version != RAFTLOG_VERSION) {
        LOG_ERROR("Invalid Raft log version: %d\n", log->header->version);
        goto error;
    }

    return log;

error:
    RedisModule_Free(log->header);
    RedisModule_Free(log);
    return NULL;
}

int readRaftLogEntryLen(RaftLog *log, size_t *entry_len)
{
    size_t nread = read(log->fd, entry_len, sizeof(*entry_len));

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

int readRaftLogEntry(RaftLog *log, RaftLogEntry *entry)
{
    size_t nread = read(log->fd, entry, sizeof(*entry));

    /* EOF */
    if (!nread) {
        return 0;
    }

    if (nread < 0) {
        LOG_ERROR("Error reading log entry: %s\n", strerror(errno));
        return -1;
    } else if (nread < sizeof(*entry)) {
        LOG_ERROR("Short read reading log entry: %zd/%zd\n",
                nread, sizeof(*entry));
        return -1;
    }

    return 1;
}

int RaftLogLoadEntries(RaftLog *log, int (*callback)(void **, raft_entry_t *), void *callback_arg)
{
    int ret = 0;

    if (lseek(log->fd, log->header->entry_offset, SEEK_SET) < 0) {
        LOG_ERROR("Failed to read Raft log: %s\n", strerror(errno));
        return -1;
    }

    do {
        raft_entry_t raft_entry;
        RaftLogEntry e;
        int r = readRaftLogEntry(log, &e);

        /* End of file? */
        if (!r) {
            break;
        }

        /* Error? */
        if (r < 0) {
            ret = -1;
            break;
        }

        /* Set up raft_entry */
        memset(&raft_entry, 0, sizeof(raft_entry));
        raft_entry.term = e.term;
        raft_entry.id = e.id;
        raft_entry.type = e.type;
        raft_entry.data.len = e.len;
        raft_entry.data.buf = RedisModule_Alloc(e.len);

        /* Read data */
        size_t entry_len;
        struct iovec iov[2] = {
            { .iov_base = raft_entry.data.buf, .iov_len = e.len },
            { .iov_base = &entry_len, .iov_len = sizeof(entry_len) }
        };

        ssize_t nread = readv(log->fd, iov, 2);
        if (nread < e.len + sizeof(entry_len)) {
            LOG_ERROR("Failed to read Raft log entry: %s\n",
                    nread == -1 ? strerror(errno) : "truncated file");
            RedisModule_Free(raft_entry.data.buf);
            ret = -1;
            break;
        }

        size_t expected_len = sizeof(entry_len) + e.len + sizeof(RaftLogEntry);
        if (entry_len != expected_len) {
            LOG_ERROR("Invalid log entry size: %zd (expected %zd)\n", entry_len, expected_len);
            RedisModule_Free(raft_entry.data.buf);
            ret = -1;
            break;
        }

        int cb_ret = callback(callback_arg, &raft_entry);
        if (cb_ret < 0) {
            RedisModule_Free(raft_entry.data.buf);
            ret = cb_ret;
            break;
        }
        ret++;
    } while(1);

    return ret;
}

bool RaftLogUpdate(RaftLog *log, bool sync)
{
    if (lseek(log->fd, 0, SEEK_SET) < 0) {
        return false;
    }

    if (write(log->fd, log->header, sizeof(RaftLogHeader)) < sizeof(RaftLogHeader)) {
        LOG_ERROR("Failed to update Raft log: %s", strerror(errno));
        return false;
    }

    if (sync && fsync(log->fd) < 0) {
        LOG_ERROR("Failed to sync Raft log: %s", strerror(errno));
        return false;
    }

    return true;
}

bool RaftLogAppend(RaftLog *log, raft_entry_t *entry)
{
    RaftLogEntry ent = {
        .term = entry->term,
        .id = entry->id,
        .type = entry->type,
        .len = entry->data.len
    };
    size_t entry_len = sizeof(ent) + entry->data.len + sizeof(entry_len);

    off_t pos = lseek(log->fd, 0, SEEK_END);
    if (pos < 0) {
        return false;
    }

    struct iovec iov[3] = {
        { .iov_base = &ent, .iov_len = sizeof(ent) },
        { .iov_base = entry->data.buf, .iov_len = entry->data.len },
        { .iov_base = &entry_len, .iov_len = sizeof(entry_len) }
    };

    ssize_t written = writev(log->fd, iov, 3);
    if (written < entry_len) {
        if (written == -1) {
            LOG_ERROR("Error writing Raft log: %s", strerror(errno));
        } else {
            LOG_ERROR("Incomplete Raft log write: %zd/%zd bytes written", written, entry_len);
        }

        if (written != -1 && ftruncate(log->fd, pos) != -1) {
            LOG_ERROR("Failed to truncate partial entry!");
        }

        return false;
    }

    if (fsync(log->fd) < 0) {
        LOG_ERROR("Error syncing Raft log: %s", strerror(errno));
        return false;
    }

    return true;
}

bool RaftLogRemoveHead(RaftLog *log)
{
    RaftLogEntry entry;

    if (lseek(log->fd, log->header->entry_offset, SEEK_SET) < 0) {
        LOG_ERROR("Failed to seek Raft log: %s\n", strerror(errno));
        return false;
    }

    if (readRaftLogEntry(log, &entry) <= 0) {
        return false;
    }

    size_t next_off = log->header->entry_offset + sizeof(RaftLogEntry) + entry.len + sizeof(size_t);
    if (lseek(log->fd, 0, SEEK_END) == next_off) {
        log->header->entry_offset = INITIAL_LOG_OFFSET;
        RaftLogUpdate(log, true);
        ftruncate(log->fd, log->header->entry_offset);
    } else {
        log->header->entry_offset = next_off;
        RaftLogUpdate(log, true);
    }

    return true;
}

bool RaftLogRemoveTail(RaftLog *log)
{
    off_t end_off;
    if ((end_off = lseek(log->fd, 0 - sizeof(size_t), SEEK_END)) < 0) {
        LOG_ERROR("Failed to seek to Raft log end: %s\n", strerror(errno));
        return false;
    }
    if (end_off <= log->header->entry_offset + sizeof(RaftLogEntry)) {
        return false;
    }
    end_off += sizeof(size_t);

    size_t entry_len;
    if (readRaftLogEntryLen(log, &entry_len) <= 0) {
        return false;
    }

    off_t new_size = end_off - entry_len;
    if (new_size == log->header->entry_offset) {
        log->header->entry_offset = INITIAL_LOG_OFFSET;
        new_size = INITIAL_LOG_OFFSET;
        RaftLogUpdate(log, true);
    }
    ftruncate(log->fd, new_size);
    return true;
}
