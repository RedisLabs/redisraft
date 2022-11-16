/*
 * Copyright Redis Ltd. 2020 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "../src/redisraft.h"

#include <fcntl.h>
#include <stddef.h>

#include "cmocka.h"

#define FILENAME "test.file"

static void test_file_write_pos(void **state)
{
    int rc;
    ssize_t wr, rd;
    char tmp[8192] = {50};
    char *str = "test";
    File file;

    unlink(FILENAME);

    FileInit(&file);

    rc = FileOpen(&file, FILENAME, O_CREAT | O_RDWR | O_APPEND);
    assert_int_equal(rc, RR_OK);
    assert_int_equal(FileSize(&file), 0);
    assert_int_equal(FileTruncate(&file, -1), RR_ERROR);

    /* Test short write */
    wr = FileWrite(&file, str, strlen(str));
    assert_int_equal(wr, strlen(str));
    assert_int_equal(FileSize(&file), strlen(str));

    rc = FileTerm(&file);
    assert_int_equal(rc, RR_OK);

    rc = FileOpen(&file, FILENAME, O_RDWR | O_APPEND);
    assert_int_equal(rc, RR_OK);
    assert_int_equal(FileSize(&file), strlen(str));

    /* Test large write */
    wr = FileWrite(&file, tmp, sizeof(tmp));
    assert_int_equal(wr, sizeof(tmp));
    assert_int_equal(FileSize(&file), sizeof(tmp) + strlen(str));

    rc = FileTerm(&file);
    assert_int_equal(rc, RR_OK);

    rc = FileOpen(&file, FILENAME, O_RDWR | O_APPEND);
    assert_int_equal(rc, RR_OK);
    assert_int_equal(FileSize(&file), sizeof(tmp) + strlen(str));

    rd = FileRead(&file, tmp, sizeof(tmp));
    assert_int_equal(rd, sizeof(tmp));
    assert_int_equal(FileGetReadOffset(&file), sizeof(tmp));
    assert_int_equal(FileSize(&file), sizeof(tmp) + strlen(str));

    rc = FileTerm(&file);
    assert_int_equal(rc, RR_OK);
}

static void test_file_read_pos(void **state)
{
    int rc;
    char tmp[8192] = {50};
    File file;

    unlink(FILENAME);

    FileInit(&file);

    rc = FileOpen(&file, FILENAME, O_CREAT | O_RDWR | O_APPEND);
    assert_int_equal(rc, RR_OK);
    assert_int_equal(FileGetReadOffset(&file), 0);
    assert_int_equal(FileRead(&file, tmp, sizeof(tmp)), 0);

    FileWrite(&file, tmp, sizeof(tmp));

    /* Test reads less than page size (4096). */
    FileSetReadOffset(&file, 0);
    assert_int_equal(FileRead(&file, tmp, 100), 100);
    assert_int_equal(FileGetReadOffset(&file), 100);

    /* Test reads larger than page size (4096). */
    FileSetReadOffset(&file, 0);
    assert_int_equal(FileRead(&file, tmp, 8000), 8000);
    assert_int_equal(FileGetReadOffset(&file), 8000);

    /* Test reads less than buffer size */
    FileTruncate(&file, 100);
    FileSetReadOffset(&file, 0);
    assert_int_equal(FileRead(&file, tmp, 8000), 100);
    assert_int_equal(FileGetReadOffset(&file), 100);

    FileTerm(&file);
}

static void test_file_fgets(void **state)
{
    int rc;
    char tmp[8192] = {50};
    File file;

    unlink(FILENAME);

    FileInit(&file);

    rc = FileOpen(&file, FILENAME, O_CREAT | O_RDWR | O_APPEND);
    assert_int_equal(rc, RR_OK);
    assert_int_equal(FileGets(&file, tmp, sizeof(tmp)), -1);

    FileWrite(&file, tmp, sizeof(tmp));
    FileSetReadOffset(&file, 0);
    assert_int_equal(FileGets(&file, tmp, 100), -1);

    /* Test reads less than page size (4096)
     * Line length will be 4001. */
    FileTruncate(&file, 4000);
    FileWrite(&file, "\n", 1);
    FileSetReadOffset(&file, 0);

    memset(tmp, 0, sizeof(tmp));
    assert_int_equal(FileGets(&file, tmp, 4000), -1);

    memset(tmp, 0, sizeof(tmp));
    assert_int_equal(FileGets(&file, tmp, 4001), 4001);

    memset(tmp, 0, sizeof(tmp));
    FileTruncate(&file, 0);

    /* Test reads larger than page size (4096)
     * Line length will be 5001. */
    FileWrite(&file, tmp, 5000);
    FileWrite(&file, "\n", 1);

    FileSetReadOffset(&file, 0);
    assert_int_equal(FileGets(&file, tmp, 5000), -1);

    FileSetReadOffset(&file, 0);
    assert_int_equal(FileGets(&file, tmp, 5001), 5001);

    FileTerm(&file);
}

static void test_file_persistence(void **state)
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
        assert_int_equal(ret, half);
        assert_memory_equal(orig, read, half);

        ret = FileRead(&file, read, half);
        assert_int_equal(ret, half);
        assert_memory_equal(&orig[half], read, half);
    }

    FileTerm(&file);
}

static void test_file_boundaries(void **state)
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
    assert_int_equal(ret, 0);
    assert_int_equal(FileSize(&file), 0);

    /* Test read with buffer len zero */
    FileSetReadOffset(&file, 0);
    ret = FileRead(&file, read, 0);
    assert_int_equal(ret, 0);
    assert_int_equal(FileGetReadOffset(&file), 0);

    /* Test read with buffer len zero after write*/
    FileWrite(&file, orig, sizeof(orig));
    FileSetReadOffset(&file, 0);
    ret = FileRead(&file, read, 0);
    assert_int_equal(ret, 0);
    assert_int_equal(FileGetReadOffset(&file), 0);

    /* Fill the internal file buffer */
    ret = FileRead(&file, read, 1);
    assert_int_equal(ret, 1);

    /* Read the remaining bytes */
    ret = FileRead(&file, &read[1], sizeof(read));
    assert_int_equal(ret, sizeof(read) - 1);

    assert_memory_equal(read, orig, sizeof(orig));
    FileTerm(&file);
}

const struct CMUnitTest file_tests[] = {
    cmocka_unit_test(test_file_write_pos),
    cmocka_unit_test(test_file_read_pos),
    cmocka_unit_test(test_file_fgets),
    cmocka_unit_test(test_file_persistence),
    cmocka_unit_test(test_file_boundaries),
    {.test_func = NULL},
};
