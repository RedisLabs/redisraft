/*
 * Copyright Redis Ltd. 2020 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "log.h"

int truncateFiles(Log *log, size_t offset, size_t idxoffset)
{
    if (FileTruncate(&log->file, offset) != RR_OK ||
        FileTruncate(&log->idxfile, idxoffset) != RR_OK) {
        return RR_ERROR;
    }

    return RR_OK;
}

int writeLength(void *buf, size_t cap, char prefix, long len)
{
    return safesnprintf(buf, cap, "%c%ld\r\n", prefix, len);
}

bool readLength(File *fp, char type, int *length)
{
    char buf[64] = {0};

    ssize_t ret = FileGets(fp, buf, sizeof(buf));
    if (ret <= 0 || buf[0] != type) {
        return false;
    }

    return parseInt(buf + 1, NULL, length);
}

int writeString(void *buf, size_t cap, const char *val)
{
    int len = lensnprintf("%s", val);
    return safesnprintf(buf, cap, "$%d\r\n%s\r\n", len, val);
}

bool readItem(File *fp, char *buf, size_t size)
{
    int len;
    char crlf[2];

    if (!readLength(fp, '$', &len) ||
        len > (int) size) {
        return false;
    }

    if (FileRead(fp, buf, len) != len ||
        FileRead(fp, crlf, 2) != 2) {
        return false;
    }

    buf[size - 1] = '\0';
    return true;
}

int writeLong(void *buf, size_t cap, long val)
{
    int len = lensnprintf("%ld", val);
    return safesnprintf(buf, cap, "$%d\r\n%ld\r\n", len, val);
}

bool readLong(File *fp, long *value)
{
    char buf[64] = {0};

    if (!readItem(fp, buf, sizeof(buf)) ||
        !parseLong(buf, NULL, value)) {
        return false;
    }

    return true;
}

bool readInt(File *fp, int *value)
{
    char buf[64] = {0};

    if (!readItem(fp, buf, sizeof(buf)) ||
        !parseInt(buf, NULL, value)) {
        return false;
    }

    return true;
}
