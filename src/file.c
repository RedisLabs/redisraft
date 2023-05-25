/*
 * Copyright Redis Ltd. 2022 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "file.h"

#include "redisraft.h"

#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

#define FILE_BUFSIZE 4096

void FileInit(File *file)
{
    char *buf = RedisModule_Alloc(FILE_BUFSIZE);

    *file = (File){
        .fd = -1,
        .buf = buf,
        .wpos = buf,
    };
}

int FileTerm(File *file)
{
    int rc;
    int fd = file->fd;

    if (fd == -1) {
        rc = RR_OK;
        goto out;
    }

    rc = FileFlush(file);

    if (close(fd) != 0) {
        LOG_WARNING("error, fd: %d, close(): %s", file->fd, strerror(errno));
        rc = RR_ERROR;
        goto out;
    }

out:
    RedisModule_Free(file->buf);
    *file = (File){
        .fd = -1,
        .buf = NULL,
    };

    return rc;
}

int FileOpen(File *file, const char *filename, int flags)
{
    int fd;
    struct stat st;

    /* Currently, only O_APPEND mode is supported. */
    RedisModule_Assert(flags == O_RDONLY || flags & O_APPEND);

    fd = open(filename, flags, S_IWUSR | S_IRUSR);
    if (fd < 0) {
        int saved_errno = errno;
        if (errno != ENOENT) {
            LOG_WARNING("error, fd: %d, open(): %s", file->fd, strerror(errno));
        }
        errno = saved_errno;

        return RR_ERROR;
    }

    if (stat(filename, &st) != 0) {
        int saved_errno = errno;
        LOG_WARNING("error, fd: %d, stat(): %s", file->fd, strerror(errno));
        close(fd);
        errno = saved_errno;

        return RR_ERROR;
    }

    file->woffset = st.st_size;
    file->fd = fd;

    return RR_OK;
}

/* Flush buffer to the file. */
int FileFlush(File *file)
{
    char *wbegin = file->buf;

    if (file->wpos == file->buf) {
        return RR_OK;
    }

    while (true) {
        size_t count = file->wpos - wbegin;
        if (count == 0) {
            break;
        }

        ssize_t wr = write(file->fd, wbegin, count);
        if (wr < 0) {
            /* Adjust userspace buffer if there was a partial write. */
            memmove(file->buf, wbegin, count);
            file->wpos = file->buf + count;
            file->rpos = file->rend = NULL;
            LOG_WARNING("error, fd: %d, write(): %s", file->fd, strerror(errno));
            return RR_ERROR;
        }

        wbegin += wr;
    }

    /* Clear read and write buffers */
    file->wpos = file->buf;
    file->rpos = file->rend = NULL;

    return RR_OK;
}

int FileFsync(File *file)
{
    int rc = fsyncFile(file->fd);
    if (rc != RR_OK) {
        LOG_WARNING("error fd:%d, fsync(): %s", file->fd, strerror(errno));
    }
    return rc;
}

/* Set read position of the file. Required before read functions. */
int FileSetReadOffset(File *file, size_t offset)
{
    if (FileFlush(file) != RR_OK) {
        return RR_ERROR;
    }

    /* Dismiss buffers. */
    file->wpos = file->buf;
    file->rpos = file->rend = NULL;

    off_t ret = lseek(file->fd, (off_t) offset, SEEK_SET);
    if (ret != (off_t) offset) {
        LOG_WARNING("error, fd: %d, lseek(): %s", file->fd, strerror(errno));
        return RR_ERROR;
    }

    file->roffset = offset;

    return RR_OK;
}

/* Similar to fgets(), reads a line into `buf`, at most `cap` bytes.
 * Returns the number of bytes read, -1 on error. */
ssize_t FileGets(File *file, void *buf, size_t cap)
{
    size_t remaining = cap;
    char *dest = buf;

    /* Flush data first if we are switching from write-mode to read-mode. */
    if (FileFlush(file) != RR_OK) {
        return -1;
    }

    while (true) {
        ssize_t count = file->rend - file->rpos;
        if (count != 0) {
            /* Exhaust the buffer first */
            char *pos = memchr(file->rpos, '\n', count);
            size_t len = pos ? pos - file->rpos + 1 : count;
            if (len > remaining) {
                return -1;
            }

            memcpy(dest, file->rpos, len);
            file->rpos += len;
            file->roffset += len;
            dest += len;
            remaining -= len;

            if (pos) {
                return (ssize_t) (cap - remaining);
            }
        }

        ssize_t bytes = read(file->fd, file->buf, FILE_BUFSIZE);
        if (bytes <= 0) {
            return -1;
        }

        file->rpos = file->buf;
        file->rend = file->buf + bytes;
    }
}

/* Similar to fread(), reads data into `buf`, at most `cap` bytes.
 * Returns the number of bytes read, -1 on error. */
ssize_t FileRead(File *file, void *buf, size_t cap)
{
    size_t remaining = (ssize_t) cap;
    char *dest = buf;

    /* Flush data first if we are switching from write-mode to read-mode. */
    if (FileFlush(file) != RR_OK) {
        return -1;
    }

    size_t count = file->rend - file->rpos;
    if (count) {
        /* Exhaust the file buffer first. */
        count = MIN(count, remaining);
        memcpy(dest, file->rpos, count);
        remaining -= count;
        dest += count;
        file->rpos += count;
        file->roffset += count;
    }

    if (remaining == 0) {
        return (ssize_t) cap;
    }

    /* Try to fill user buffer and file buffer in a single readv() call. */
    while (true) {
        struct iovec iov[2] = {
            {.iov_base = dest,      .iov_len = remaining   },
            {.iov_base = file->buf, .iov_len = FILE_BUFSIZE},
        };

        /* readv() with iov buffers larger than INT_MAX does not work on macOS.
         * Falling back to read() on macOS to support large read operations. */
#if defined(__APPLE__)
        ssize_t n = MIN(INT_MAX, iov[0].iov_len);
        ssize_t rd = read(file->fd, iov[0].iov_base, n);
#else
        ssize_t rd = readv(file->fd, iov, 2);
#endif
        if (rd < 0) {
            return -1;
        } else if (rd == 0) {
            return (ssize_t) (cap - remaining);
        }

        if (rd >= (ssize_t) iov[0].iov_len) {
            file->roffset += iov[0].iov_len;
            file->rpos = file->buf;
            file->rend = file->buf + (rd - iov[0].iov_len);

            return (ssize_t) cap;
        }

        remaining -= rd;
        dest += rd;
        file->roffset += rd;
    }
}

/* Similar to fwrite(), write data from `buf` into the file.
 * Return the number of written bytes, -1 on error. */
ssize_t FileWrite(File *file, void *buf, size_t len)
{
    /* Clear read buffer positions as we are in write mode now. */
    file->rpos = file->rend = NULL;

    size_t cap = file->buf + FILE_BUFSIZE - file->wpos;
    if (cap >= len) {
        /* Data can fit into the file buffer. */
        memcpy(file->wpos, buf, len);
        file->wpos += len;
        file->woffset += len;

        return (ssize_t) len;
    }

    /* We have some data in the file buffer and need to write it first before
     * the user buffer. We'll write both buffers with a single writev() call.
     * We'll loop until all data is written in case of a partial write. */

#define IOV_COUNT 2

    struct iovec iov[IOV_COUNT] = {
        {.iov_base = file->buf, .iov_len = file->wpos - file->buf},
        {.iov_base = buf,       .iov_len = len                   }
    };

    size_t remaining = iov[0].iov_len + iov[1].iov_len;
    int current_iov = 0;
    ssize_t count;

    while (true) {
        /* writev() with iov buffers larger than INT_MAX does not work on macOS.
         * Falling back to write() on macOS to support large write operations.*/
#if defined(__APPLE__)
        ssize_t n = MIN(INT_MAX, iov[current_iov].iov_len);
        count = write(file->fd, iov[current_iov].iov_base, n);
#else
        count = writev(file->fd, &iov[current_iov], IOV_COUNT - current_iov);
#endif
        if (count < 0) {
            /* Adjust userspace buffer if there was a partial write. */
            memmove(file->buf, iov[0].iov_base, iov[0].iov_len);
            file->wpos = file->buf + iov[0].iov_len;

            /* Adjust write offset if there was a partial write. */
            file->woffset += len - iov[1].iov_len;
            LOG_WARNING("error, fd:%d, writev():%s", file->fd, strerror(errno));
            return -1;
        }

        if ((size_t) count == remaining) {
            file->wpos = file->buf;
            file->woffset += len;
            return (ssize_t) len;
        }

        remaining -= count;
        if ((size_t) count >= iov[current_iov].iov_len) {
            count -= (ssize_t) iov[current_iov].iov_len;
            iov[current_iov].iov_len = 0;
            current_iov++;
        }

        iov[current_iov].iov_base = (char *) iov[current_iov].iov_base + count;
        iov[current_iov].iov_len -= count;
    }
}

size_t FileSize(File *file)
{
    return file->woffset;
}

size_t FileGetReadOffset(File *file)
{
    return file->roffset;
}

int FileTruncate(File *file, size_t len)
{
    if (FileFlush(file) != RR_OK) {
        return RR_ERROR;
    }

    if (ftruncate(file->fd, (off_t) len) != 0) {
        LOG_WARNING("error, fd: %d, ftruncate(): %s", file->fd, strerror(errno));
        return RR_ERROR;
    }

    file->woffset = len;
    file->roffset = 0;
    file->rpos = file->rend = NULL;

    return RR_OK;
}
