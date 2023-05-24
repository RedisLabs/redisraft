/*
 * Copyright Redis Ltd. 2020 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "test.h"

#include "../src/redisraft.h"

#include <assert.h>
#include <fcntl.h>

#define FILENAME "test.file"

static void test_file_write_pos()
{
    int rc;
    ssize_t wr, rd;
    char tmp[8192] = {50};
    char *str = "test";
    File file;

    unlink(FILENAME);

    FileInit(&file);

    rc = FileOpen(&file, FILENAME, O_CREAT | O_RDWR | O_APPEND);
    assert(rc == RR_OK);
    assert(FileSize(&file) == 0);
    assert(FileTruncate(&file, -1) == RR_ERROR);

    /* Test short write */
    wr = FileWrite(&file, str, strlen(str));
    assert(wr == strlen(str));
    assert(FileSize(&file) == strlen(str));

    rc = FileTerm(&file);
    assert(rc == RR_OK);

    rc = FileOpen(&file, FILENAME, O_RDWR | O_APPEND);
    assert(rc == RR_OK);
    assert(FileSize(&file) == strlen(str));

    /* Test large write */
    wr = FileWrite(&file, tmp, sizeof(tmp));
    assert(wr == sizeof(tmp));
    assert(FileSize(&file) == sizeof(tmp) + strlen(str));

    rc = FileTerm(&file);
    assert(rc == RR_OK);

    rc = FileOpen(&file, FILENAME, O_RDWR | O_APPEND);
    assert(rc == RR_OK);
    assert(FileSize(&file) == sizeof(tmp) + strlen(str));

    rd = FileRead(&file, tmp, sizeof(tmp));
    assert(rd == sizeof(tmp));
    assert(FileGetReadOffset(&file) == sizeof(tmp));
    assert(FileSize(&file) == sizeof(tmp) + strlen(str));

    rc = FileTerm(&file);
    assert(rc == RR_OK);
}

static void test_file_read_pos()
{
    int rc;
    char tmp[8192] = {50};
    File file;

    unlink(FILENAME);

    FileInit(&file);

    rc = FileOpen(&file, FILENAME, O_CREAT | O_RDWR | O_APPEND);
    assert(rc == RR_OK);
    assert(FileGetReadOffset(&file) == 0);
    assert(FileRead(&file, tmp, sizeof(tmp)) == 0);

    FileWrite(&file, tmp, sizeof(tmp));

    /* Test reads less than page size (4096). */
    FileSetReadOffset(&file, 0);
    assert(FileRead(&file, tmp, 100) == 100);
    assert(FileGetReadOffset(&file) == 100);

    /* Test reads larger than page size (4096). */
    FileSetReadOffset(&file, 0);
    assert(FileRead(&file, tmp, 8000) == 8000);
    assert(FileGetReadOffset(&file) == 8000);

    /* Test reads less than buffer size */
    FileTruncate(&file, 100);
    FileSetReadOffset(&file, 0);
    assert(FileRead(&file, tmp, 8000) == 100);
    assert(FileGetReadOffset(&file) == 100);

    FileTerm(&file);
}

static void test_file_fgets()
{
    int rc;
    char tmp[8192] = {50};
    File file;

    unlink(FILENAME);

    FileInit(&file);

    rc = FileOpen(&file, FILENAME, O_CREAT | O_RDWR | O_APPEND);
    assert(rc == RR_OK);
    assert(FileGets(&file, tmp, sizeof(tmp)) == -1);

    FileWrite(&file, tmp, sizeof(tmp));
    FileSetReadOffset(&file, 0);
    assert(FileGets(&file, tmp, 100) == -1);

    /* Test reads less than page size (4096)
     * Line length will be 4001. */
    FileTruncate(&file, 4000);
    FileWrite(&file, "\n", 1);
    FileSetReadOffset(&file, 0);

    memset(tmp, 0, sizeof(tmp));
    assert(FileGets(&file, tmp, 4000) == -1);

    memset(tmp, 0, sizeof(tmp));
    assert(FileGets(&file, tmp, 4001) == 4001);

    memset(tmp, 0, sizeof(tmp));
    FileTruncate(&file, 0);

    /* Test reads larger than page size (4096)
     * Line length will be 5001. */
    FileWrite(&file, tmp, 5000);
    FileWrite(&file, "\n", 1);

    FileSetReadOffset(&file, 0);
    assert(FileGets(&file, tmp, 5000) == -1);

    FileSetReadOffset(&file, 0);
    assert(FileGets(&file, tmp, 5001) == 5001);

    FileTerm(&file);
}

static void test_file_persistence()
{
    char read[720], orig[720];
    File file;

    unlink(FILENAME);

    for (int i = 0; i < sizeof(orig); i++) {
        orig[i] = rand() & 255;
    }

    /* Write some random bytes to the file in a loop */
    FileInit(&file);
    FileOpen(&file, FILENAME, O_CREAT | O_RDWR | O_APPEND);

    for (int i = 0; i < 34; i++) {
        FileWrite(&file, orig, sizeof(orig));
    }
    FileTerm(&file);

    /* Read bytes in a loop and verify correctness */
    FileOpen(&file, FILENAME, O_RDWR | O_APPEND);
    FileSetReadOffset(&file, 0);

    for (int i = 0; i < 17; i++) {
        ssize_t ret;
        size_t half = sizeof(read) / 2;

        ret = FileRead(&file, read, half);
        assert(ret == half);
        assert(memcmp(orig, read, half) == 0);

        ret = FileRead(&file, read, half);
        assert(ret == half);
        assert(memcmp(&orig[half], read, half) == 0);
    }

    FileTerm(&file);
}

static void test_file_boundaries()
{
    char read[720], orig[720];
    ssize_t ret;
    File file;

    unlink(FILENAME);

    for (int i = 0; i < sizeof(orig); i++) {
        orig[i] = rand() & 255;
    }

    FileInit(&file);
    FileOpen(&file, FILENAME, O_CREAT | O_RDWR | O_APPEND);

    /* Test write with buffer len zero */
    ret = FileWrite(&file, orig, 0);
    assert(ret == 0);
    assert(FileSize(&file) == 0);

    /* Test read with buffer len zero */
    FileSetReadOffset(&file, 0);
    ret = FileRead(&file, read, 0);
    assert(ret == 0);
    assert(FileGetReadOffset(&file) == 0);

    /* Test read with buffer len zero after write*/
    FileWrite(&file, orig, sizeof(orig));
    FileSetReadOffset(&file, 0);
    ret = FileRead(&file, read, 0);
    assert(ret == 0);
    assert(FileGetReadOffset(&file) == 0);

    /* Fill the internal file buffer */
    ret = FileRead(&file, read, 1);
    assert(ret == 1);

    /* Read the remaining bytes */
    ret = FileRead(&file, &read[1], sizeof(read));
    assert(ret == sizeof(read) - 1);

    assert(memcmp(read, orig, sizeof(orig)) == 0);
    FileTerm(&file);
}

static void test_file_big()
{
    /* Test writing/reading big chunks in a single call. Especially, big buffer
     * reads may cause readv() call return partial reads, so this test is useful
     * for testing it. */
    ssize_t ret;
    File file;

    unlink(FILENAME);

    const size_t PARTIAL_READ_LEN = 518;
    const size_t LEN = 2UL * 1024 * 1024 * 1100;
    char *copy = malloc(LEN);

    char *orig = calloc(1, LEN);
    /* Set some random bytes. */
    for (size_t i = 0; i < 4096; i++) {
        orig[rand() % (LEN - 1)] = rand();
    }

    FileInit(&file);
    FileOpen(&file, FILENAME, O_CREAT | O_RDWR | O_APPEND);

    /* Test write */
    ret = FileWrite(&file, orig, LEN);
    assert(ret == LEN);
    assert(FileSize(&file) == LEN);

    /* Test read */
    FileSetReadOffset(&file, 0);

    /* Read partial first. */
    ret = FileRead(&file, copy, PARTIAL_READ_LEN);
    assert(ret == PARTIAL_READ_LEN);
    assert(FileGetReadOffset(&file) == PARTIAL_READ_LEN);

    /* Read rest of the data */
    ret = FileRead(&file, copy + PARTIAL_READ_LEN, LEN - PARTIAL_READ_LEN);
    assert(ret == LEN - PARTIAL_READ_LEN);
    assert(FileGetReadOffset(&file) == LEN);
    assert(memcmp(copy, orig, LEN) == 0);
    FileTerm(&file);

    /* Test read by opening file again. */
    FileInit(&file);
    FileOpen(&file, FILENAME, O_RDONLY);

    /* Read partial first. */
    ret = FileRead(&file, copy, PARTIAL_READ_LEN);
    assert(ret == PARTIAL_READ_LEN);
    assert(FileGetReadOffset(&file) == PARTIAL_READ_LEN);

    /* Read rest of the data */
    ret = FileRead(&file, copy + PARTIAL_READ_LEN, LEN - PARTIAL_READ_LEN);
    assert(ret == LEN - PARTIAL_READ_LEN);
    assert(FileGetReadOffset(&file) == LEN);
    assert(memcmp(copy, orig, LEN) == 0);

    FileTerm(&file);
    free(orig);
    free(copy);
}

void test_file()
{
    test_run(test_file_write_pos);
    test_run(test_file_read_pos);
    test_run(test_file_fgets);
    test_run(test_file_persistence);
    test_run(test_file_boundaries);
    test_run(test_file_big);
}
