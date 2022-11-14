#include "file.h"

#include "redisraft.h"

#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

void FileInit(File *file)
{
    *file = (File){
        .fd = -1,
        .wpos = file->buf,
    };
}

int FileTerm(File *file)
{
    int rc;

    if (file->fd == -1) {
        return RR_OK;
    }

    rc = FileFlush(file);

    if (close(file->fd) != 0) {
        return RR_ERROR;
    }
    file->fd = -1;

    return rc;
}

int FileOpen(File *file, const char *filename, int flags)
{
    int fd;
    struct stat st;

    /* Currently, only O_APPEND mode is supported. */
    RedisModule_Assert(flags & O_APPEND);

    fd = open(filename, flags, S_IWUSR | S_IRUSR);
    if (fd < 0) {
        return RR_ERROR;
    }

    if (stat(filename, &st) != 0) {
        close(fd);
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
    return fsync(file->fd) == 0 ? RR_OK : RR_ERROR;
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

        ssize_t bytes = read(file->fd, file->buf, sizeof(file->buf));
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
            {.iov_base = dest,      .iov_len = remaining        },
            {.iov_base = file->buf, .iov_len = sizeof(file->buf)},
        };

        ssize_t rd = readv(file->fd, iov, 2);
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
        dest += count;
        file->roffset += rd;
    }
}

/* Similar to fwrite(), write data from `buf` into the file.
 * Return the number of written bytes, -1 on error. */
ssize_t FileWrite(File *file, void *buf, size_t len)
{
    /* Clear read buffer positions as we are in write mode now. */
    file->rpos = file->rend = NULL;

    size_t cap = file->buf + sizeof(file->buf) - file->wpos;
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
    struct iovec iovs[2] = {
        {.iov_base = file->buf, .iov_len = file->wpos - file->buf},
        {.iov_base = buf,       .iov_len = len                   }
    };

    struct iovec *iov = iovs;
    size_t rem = iov[0].iov_len + iov[1].iov_len;
    int iovcnt = 2;
    ssize_t count;

    while (true) {
        count = writev(file->fd, iov, iovcnt);
        if (count < 0) {
            return -1;
        }

        if ((size_t) count == rem) {
            file->wpos = file->buf;
            file->woffset += len;
            return (ssize_t) len;
        }

        rem -= count;
        if ((size_t) count > iov[0].iov_len) {
            count -= (ssize_t) iov[0].iov_len;
            iov++;
            iovcnt--;
        }

        iov[0].iov_base = (char *) iov[0].iov_base + count;
        iov[0].iov_len -= count;
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
    if (FileFlush(file) != RR_OK ||
        ftruncate(file->fd, (off_t) len) != 0) {
        return RR_ERROR;
    }

    file->woffset = len;
    file->roffset = 0;
    file->rpos = file->rend = NULL;

    return RR_OK;
}
