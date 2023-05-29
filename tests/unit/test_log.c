/*
 * Copyright Redis Ltd. 2020 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "test.h"

#include "../src/entrycache.h"
#include "../src/log.h"
#include "../src/redisraft.h"
#include "common/sc_crc32.h"

#include <assert.h>
#include <fcntl.h>
#include <limits.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define LOGNAME "test.log.db"
#define DBID    "01234567890123456789012345678901"

static raft_entry_t *make_entry(int id, const char *value)
{
    char buf[64];

    if (!value) {
        snprintf(buf, sizeof(buf), "value%d\n", id);
        value = buf;
    }

    raft_entry_t *e = raft_entry_new(strlen(value) + 1);
    e->id = id;
    strcpy(e->data, value);
    return e;
}

static void append_entry(Log *log, int id, const char *value)
{
    raft_entry_t *e = make_entry(id, value);

    assert(LogAppend(log, e) == RR_OK);
    raft_entry_release(e);
}

static void test_log_random_access()
{
    Log log;

    LogInit(&log);
    LogCreate(&log, LOGNAME, DBID, 1, 1, 0);

    append_entry(&log, 3, NULL);
    append_entry(&log, 30, NULL);

    /* Invalid out of bound reads */
    assert(LogGet(&log, 0) == NULL);
    assert(LogGet(&log, 3) == NULL);

    raft_entry_t *e = LogGet(&log, 1);
    assert(e->id == 3);
    raft_entry_release(e);

    e = LogGet(&log, 2);
    assert(e->id == 30);
    raft_entry_release(e);

    LogTerm(&log);
}

static void test_log_random_access_with_snapshot()
{
    Log log;

    LogInit(&log);
    LogCreate(&log, LOGNAME, DBID, 1, 1, 0);

    /* Reset log assuming last snapshot is 100 */
    LogReset(&log, 100, 1);

    /* Write entries */
    append_entry(&log, 3, NULL);
    append_entry(&log, 30, NULL);

    assert(LogFirstIdx(&log) == 101);

    /* Invalid out of bound reads */
    assert(LogGet(&log, 99) == NULL);
    assert(LogGet(&log, 100) == NULL);
    assert(LogGet(&log, 103) == NULL);

    raft_entry_t *e = LogGet(&log, 101);
    assert(e != NULL);
    assert(e->id == 3);
    raft_entry_release(e);

    e = LogGet(&log, 102);
    assert(e->id == 30);
    raft_entry_release(e);

    LogTerm(&log);
}

static void test_log_load_entries()
{
    raft_entry_t *ety;
    Log log, log2;

    LogInit(&log);
    LogCreate(&log, LOGNAME, DBID, 1, 1, 0);

    append_entry(&log, 3, NULL);
    append_entry(&log, 30, NULL);

    LogTerm(&log);

    LogInit(&log2);
    assert(LogOpen(&log2, LOGNAME) == RR_OK);

    assert(LogLoadEntries(&log2) == RR_OK);
    assert(LogCount(&log2) == 2);

    ety = LogGet(&log2, 1);
    assert(ety->id == 3);
    assert(memcmp(ety->data, "value3", strlen("value3")) == 0);
    raft_entry_release(ety);

    ety = LogGet(&log2, 2);
    assert(ety->id == 30);
    assert(memcmp(ety->data, "value30", strlen("value30")) == 0);
    raft_entry_release(ety);

    LogTerm(&log2);
}

static void test_log_index_rebuild()
{
    Log log;

    LogInit(&log);
    LogCreate(&log, LOGNAME, DBID, 1, 1, 0);
    LogReset(&log, 100, 1);

    append_entry(&log, 3, NULL);
    append_entry(&log, 30, NULL);

    /* Delete index file */
    unlink(LOGNAME ".idx");
    LogTerm(&log);

    /* Reopen the log */
    LogInit(&log);
    LogOpen(&log, LOGNAME);
    LogLoadEntries(&log);

    /* Invalid out of bound reads */
    assert(LogGet(&log, 99) == NULL);
    assert(LogGet(&log, 100) == NULL);
    assert(LogGet(&log, 103) == NULL);

    raft_entry_t *e = LogGet(&log, 101);
    assert(e->id == 3);
    raft_entry_release(e);

    e = LogGet(&log, 102);
    assert(e->id == 30);
    raft_entry_release(e);

    /* Close the log */
    LogTerm(&log);
}

static void test_log_write_after_read()
{
    Log log;

    LogInit(&log);
    LogCreate(&log, LOGNAME, DBID, 1, 1, 0);

    append_entry(&log, 1, NULL);
    append_entry(&log, 2, NULL);

    raft_entry_t *e = LogGet(&log, 1);
    assert(e->id == 1);
    raft_entry_release(e);

    append_entry(&log, 3, NULL);
    e = LogGet(&log, 3);
    assert(e->id == 3);
    raft_entry_release(e);

    LogTerm(&log);
}

static void test_log_fuzzer()
{
    Log log;

    LogInit(&log);
    LogCreate(&log, LOGNAME, DBID, 1, 1, 0);
    int idx = 0, i;

    for (i = 0; i < 10000; i++) {
        int new_entries = random() % 10;
        int j;
        for (j = 0; j < new_entries; j++) {
            append_entry(&log, ++idx, NULL);
        }

        if (idx > 10) {
            int del_entries = (random() % 5) + 1;
            idx = idx - del_entries;
            assert(LogDelete(&log, idx + 1) == RR_OK);
        }

        for (j = 0; j < 20; j++) {
            int get_idx = (random() % (idx - 1)) + 1;
            raft_entry_t *e = LogGet(&log, get_idx);
            assert(e != NULL);
            assert(e->id == get_idx);
            raft_entry_release(e);
        }
    }

    LogTerm(&log);
}

static void test_log_delete()
{
    Log log;

    LogInit(&log);
    LogCreate(&log, LOGNAME, DBID, 1, 1, 0);

    char value1[] = "value1";
    raft_entry_t *entry1 = make_entry(3, value1);
    char value2[] = "value22222";
    raft_entry_t *entry2 = make_entry(20, value2);
    char value3[] = "value33333333333";
    raft_entry_t *entry3 = make_entry(30, value3);

    /* Simulate post snapshot log */
    LogReset(&log, 50, 1);

    /* Write entries */
    assert(LogAppend(&log, entry1) == RR_OK);
    assert(LogAppend(&log, entry2) == RR_OK);
    assert(LogAppend(&log, entry3) == RR_OK);

    raft_entry_t *e = LogGet(&log, 51);
    assert(e != NULL);
    assert(e->id == 3);
    raft_entry_release(e);

    /* Try to delete with improper values */
    assert(LogDelete(&log, 0) == RR_ERROR);

    /* Delete last two elements */
    assert(LogDelete(&log, 52) == RR_OK);

    /* Assert deleting a non-existing entry doesn't cause a problem. */
    assert(LogDelete(&log, 52) == RR_ERROR);

    /* Check log sanity after delete */
    assert(LogCount(&log) == 1);
    assert(LogCurrentIdx(&log) == 51);
    assert(LogGet(&log, 52) == NULL);
    e = LogGet(&log, 51);
    assert(e != NULL);
    assert(e->id == 3);
    raft_entry_release(e);

    /* Re-add entries in reverse order, validate indexes are handled
     * properly.
     */

    assert(LogAppend(&log, entry3) == RR_OK);
    e = LogGet(&log, 52);
    assert(e != NULL);
    assert(e->id == 30);
    raft_entry_release(e);

    assert(LogAppend(&log, entry2) == RR_OK);
    e = LogGet(&log, 53);
    assert(e != NULL);
    assert(e->id == 20);
    raft_entry_release(e);

    raft_entry_release(entry1);
    raft_entry_release(entry2);
    raft_entry_release(entry3);

    LogTerm(&log);
}

static void test_entry_cache_sanity()
{
    EntryCache *cache = EntryCacheNew(8);
    raft_entry_t *ety;
    int i;

    /* Insert 64 entries (cache grows) */
    for (i = 1; i <= 64; i++) {
        ety = raft_entry_new(0);
        ety->id = i;
        EntryCacheAppend(cache, ety, i);
        raft_entry_release(ety);
    }

    assert(cache->size == 64);
    assert(cache->len == 64);

    /* Get 64 entries */
    for (i = 1; i <= 64; i++) {
        ety = EntryCacheGet(cache, i);
        assert(ety != NULL);
        assert(ety->id == i);
        raft_entry_release(ety);
    }

    EntryCacheFree(cache);
}

static void test_entry_cache_start_index_change()
{
    EntryCache *cache = EntryCacheNew(8);
    raft_entry_t *ety;

    /* Establish start_idx 1 */
    ety = raft_entry_new(0);
    ety->id = 1;
    EntryCacheAppend(cache, ety, 1);
    raft_entry_release(ety);

    assert(cache->start_idx == 1);
    EntryCacheDeleteTail(cache, 1);
    assert(cache->start_idx == 0);

    ety = raft_entry_new(0);
    ety->id = 10;
    EntryCacheAppend(cache, ety, 10);
    raft_entry_release(ety);

    assert(cache->start_idx == 10);

    EntryCacheFree(cache);
}

static void test_entry_cache_delete_head()
{
    EntryCache *cache = EntryCacheNew(4);
    raft_entry_t *ety;
    int i;

    /* Fill up 5 entries */
    for (i = 1; i <= 5; i++) {
        ety = raft_entry_new(0);
        ety->id = i;
        EntryCacheAppend(cache, ety, i);
        raft_entry_release(ety);
    }

    assert(cache->size == 8);
    assert(cache->start == 0);
    assert(cache->start_idx == 1);

    /* Test invalid deletes */
    assert(EntryCacheDeleteHead(cache, 0) == -1);

    /* Delete first entry */
    assert(EntryCacheDeleteHead(cache, 2) == 1);
    assert(EntryCacheGet(cache, 1) == NULL);
    ety = EntryCacheGet(cache, 2);
    assert(ety->id == 2);
    raft_entry_release(ety);

    assert(cache->start == 1);
    assert(cache->len == 4);
    assert(cache->start_idx == 2);

    /* Delete and add 5 entries (6, 7, 8, 9, 10)*/
    for (i = 0; i < 5; i++) {
        assert(EntryCacheDeleteHead(cache, 3 + i) == 1);
        ety = raft_entry_new(0);
        ety->id = 6 + i;
        EntryCacheAppend(cache, ety, ety->id);
        raft_entry_release(ety);
    }

    assert(cache->start_idx == 7);
    assert(cache->start == 6);
    assert(cache->size == 8);
    assert(cache->len == 4);

    /* Add another 3 (11, 12, 13) */
    for (i = 11; i <= 13; i++) {
        ety = raft_entry_new(0);
        ety->id = i;
        EntryCacheAppend(cache, ety, i);
        raft_entry_release(ety);
    }

    assert(cache->start == 6);
    assert(cache->size == 8);
    assert(cache->len == 7);

    /* Validate contents */
    for (i = 7; i <= 13; i++) {
        ety = EntryCacheGet(cache, i);
        assert(ety != NULL);
        assert(ety->id == i);
        raft_entry_release(ety);
    }

    /* Delete multiple with an overlap */
    assert(EntryCacheDeleteHead(cache, 10) == 3);
    assert(cache->len == 4);
    assert(cache->start == 1);

    /* Validate contents after deletion */
    for (i = 10; i <= 13; i++) {
        ety = EntryCacheGet(cache, i);
        assert(ety != NULL);
        assert(ety->id == i);
        raft_entry_release(ety);
    }

    EntryCacheFree(cache);
}

static void test_entry_cache_delete_tail()
{
    EntryCache *cache = EntryCacheNew(4);
    raft_entry_t *ety;
    int i;

    for (i = 100; i <= 103; i++) {
        ety = raft_entry_new(0);
        ety->id = i;
        EntryCacheAppend(cache, ety, i);
        raft_entry_release(ety);
    }

    assert(cache->size == 4);
    assert(cache->len == 4);

    /* Try invalid indexes */
    assert(EntryCacheDeleteTail(cache, 104) == -1);

    /* Delete last entry */
    assert(EntryCacheDeleteTail(cache, 103) == 1);
    assert(cache->len == 3);
    assert(EntryCacheGet(cache, 103) == NULL);
    ety = EntryCacheGet(cache, 102);
    assert(ety->id == 102);
    raft_entry_release(ety);

    /* Delete all entries */
    assert(EntryCacheDeleteTail(cache, 100) == 3);
    assert(cache->len == 0);

    /* Delete an index that precedes start_idx */
    ety = raft_entry_new(0);
    EntryCacheAppend(cache, ety, 100);
    raft_entry_release(ety);

    assert(EntryCacheDeleteTail(cache, 1) == 1);
    assert(cache->len == 0);

    EntryCacheFree(cache);
}

static void test_entry_cache_fuzzer()
{
    EntryCache *cache = EntryCacheNew(4);
    raft_entry_t *ety;
    int i, j;
    raft_index_t first_index = 1;
    raft_index_t index = 0;

    srandom(time(NULL));
    for (i = 0; i < 100000; i++) {
        int new_entries = random() % 50;

        for (j = 0; j < new_entries; j++) {
            ety = raft_entry_new(0);
            ety->id = ++index;
            EntryCacheAppend(cache, ety, index);
            raft_entry_release(ety);
        }

        if (index > 5) {
            int del_head = random() % ((index + 1) / 2);
            int removed = EntryCacheDeleteHead(cache, del_head);
            if (removed > 0) {
                first_index += removed;
            }
        }

        if (index - first_index > 10) {
            int del_tail = random() % ((index - first_index) / 10);
            if (del_tail) {
                int removed = EntryCacheDeleteTail(cache, index - del_tail + 1);
                assert(removed == del_tail);
                index -= removed;
            }
        }
    }

    /* verify */
    for (i = 1; i < first_index; i++) {
        assert(EntryCacheGet(cache, i) == NULL);
    }
    for (i = first_index; i <= index; i++) {
        ety = EntryCacheGet(cache, i);
        assert(ety != NULL);
        assert(i == ety->id);
        raft_entry_release(ety);
    }

    EntryCacheFree(cache);
}

static void test_entry_cache_compact()
{
    EntryCache *cache = EntryCacheNew(4);

    for (int i = 1; i < 1000; i++) {
        raft_entry_t *ety = raft_entry_new(100);
        ety->id = i;
        EntryCacheAppend(cache, ety, i);
        raft_entry_release(ety);
    }

    /* Test index limit */
    assert(EntryCacheCompact(cache, 10, 5) == 5);
    assert(cache->len == 994);
    assert(cache->start_idx == 6);

    /* Test index limit with no deletion. */
    assert(EntryCacheCompact(cache, 10, 5) == 0);

    assert(EntryCacheCompact(cache, 10, 6) == 1);
    assert(EntryCacheCompact(cache, 10, 998) == 992);
    assert(cache->len == 1);
    assert(cache->start_idx == 999);

    /* Test smaller index than cache start index. */
    assert(EntryCacheCompact(cache, 10, 998) == 0);
    assert(EntryCacheCompact(cache, 10, 999) == 1);

    for (int i = 1000; i < 2000; i++) {
        raft_entry_t *ety = raft_entry_new(100);
        ety->id = i;
        EntryCacheAppend(cache, ety, i);
        raft_entry_release(ety);
    }

    /* Test memory limit, single entry uses 156 bytes of memory. */
    assert(EntryCacheCompact(cache, 156 * 2, 100000) == 998);

    EntryCacheFree(cache);
}

static void test_meta_persistence()
{
    Metadata m;

    unlink(LOGNAME ".meta");

    MetadataInit(&m);
    MetadataSetClusterConfig(&m, LOGNAME, DBID, 1002);

    assert(MetadataRead(&m, LOGNAME) == RR_ERROR);
    assert(MetadataWrite(&m, 0xffffffff, INT32_MAX) == RR_OK);
    assert(MetadataRead(&m, LOGNAME) == RR_OK);
    assert(strcmp(m.dbid, DBID) == 0);
    assert(m.node_id == 1002);
    assert(m.term == 0xffffffff);
    assert(m.vote == INT32_MAX);

    assert(MetadataWrite(&m, LONG_MAX, (int) -1) == RR_OK);
    assert(MetadataRead(&m, LOGNAME) == RR_OK);
    assert(strcmp(m.dbid, DBID) == 0);
    assert(m.node_id == 1002);
    assert(m.term == LONG_MAX);
    assert(m.vote == -1);

    /* Test overwrite */
    assert(MetadataWrite(&m, 5, 5) == RR_OK);
    assert(MetadataWrite(&m, 6, 6) == RR_OK);
    assert(strcmp(m.dbid, DBID) == 0);
    assert(m.node_id == 1002);
    assert(m.term == 6);
    assert(m.vote == 6);

    MetadataTerm(&m);
}

/* Sanity check for CRC32c implementation. */
static void test_crc32c()
{
    sc_crc32_init();

    /* CRC values are pre-computed. */
    assert(sc_crc32(0, "", 1) == 1383945041);
    assert(sc_crc32(0, "1", 2) == 2727214374);
}

/* Loop over log file header bytes and change one byte at a time.
 * Verify we detect the corruption when we try to read the file. */
static void test_corruption_header()
{
    Log log;

    /* Generate header on the disk. */
    LogInit(&log);
    LogCreate(&log, LOGNAME, DBID, 1, 1, 0);
    LogTerm(&log);

    struct stat st;
    stat(LOGNAME, &st);

    /* Loop over the bytes and corrupt one byte each time. */
    for (int i = 0; i < st.st_size; i++) {
        int fd = open(LOGNAME, O_RDWR, S_IWUSR | S_IRUSR);
        assert(fd > 0);

        lseek(fd, i, SEEK_SET);
        ssize_t rc = write(fd, "^", 1); /* Alter the byte */
        (void) rc;
        close(fd);

        LogInit(&log);
        int ret = LogOpen(&log, LOGNAME);
        assert(ret == RR_ERROR);

        /* Create file again. */
        unlink(LOGNAME);
        LogInit(&log);
        LogCreate(&log, LOGNAME, DBID, 1, 1, 0);
        LogTerm(&log);
    }
}

/* Loop over an entry's bytes and change one byte at a time.
 * Verify we detect the corruption when we try to read the file. */
static void test_corruption_entry()
{
    raft_entry_t *e;
    struct stat st;
    Log log;

    /* In this test, we'll create a log file with three entries and then loop
     * over the bytes of the second entry (to change one byte at a time). We
     * need to find the position of the first and last byte of the second entry
     * on the disk before going into the loop. Here, creating a log file and
     * adding two entries just to detect the entry position. */
    LogInit(&log);
    LogCreate(&log, LOGNAME, DBID, 1, 1, 0);
    append_entry(&log, 5000, "test5000");
    LogTerm(&log);

    /* Find out beginning and end bytes of the serialized entry.*/
    stat(LOGNAME, &st);
    size_t entry_begin = st.st_size;

    LogInit(&log);
    LogOpen(&log, LOGNAME);
    LogLoadEntries(&log);
    append_entry(&log, 6000, "test6000");
    LogTerm(&log);

    stat(LOGNAME, &st);
    size_t entry_end = st.st_size;

    /* Loop over the bytes and corrupt one byte each time. */
    for (size_t i = entry_begin; i < entry_end; i++) {
        /* Prepare the log file. */
        unlink(LOGNAME);
        LogInit(&log);
        LogCreate(&log, LOGNAME, DBID, 1, 1, 0);
        append_entry(&log, 5000, "test5000");
        append_entry(&log, 6000, "test6000");
        append_entry(&log, 7000, "test7000");
        LogTerm(&log);

        int fd = open(LOGNAME, O_RDWR, S_IWUSR | S_IRUSR);
        assert(fd > 0);

        lseek(fd, (off_t) i, SEEK_SET);
        ssize_t rc = write(fd, "^", 1); /* Alter the byte */
        assert(rc == 1);
        close(fd);

        LogInit(&log);
        LogOpen(&log, LOGNAME);
        LogLoadEntries(&log);

        assert(LogCount(&log) == 1);
        /* Verify entry with id 7000 does not exist. */
        e = LogGet(&log, 3);
        assert(e == NULL);

        /* Verify entry with id 6000 does not exist. */
        e = LogGet(&log, 2);
        assert(e == NULL);

        /* Verify entry with id 5000 exists. */
        e = LogGet(&log, 1);
        assert(e->id == 5000);
        assert(memcmp(e->data, "test5000", 8) == 0);
        raft_entry_release(e);
        LogTerm(&log);
    }
}

/* Simulate log compaction. Log moves to second page and then the second page
 * will be deleted when compaction ends. */
static void test_log_compaction()
{
    int idx = 0;
    Log log;

    LogInit(&log);
    LogCreate(&log, LOGNAME, DBID, 1, 1, 0);

    /* If there is no entry, it should fail. */
    assert(LogCompactionBegin(&log) == RR_ERROR);

    append_entry(&log, ++idx, NULL);

    /* If there is one entry, it should fail. */
    assert(LogCompactionBegin(&log) == RR_ERROR);

    append_entry(&log, ++idx, NULL);
    append_entry(&log, ++idx, NULL);

    LogCompactionBegin(&log);
    append_entry(&log, ++idx, NULL);
    append_entry(&log, ++idx, NULL);
    LogCompactionEnd(&log);
    append_entry(&log, ++idx, NULL);
    LogTerm(&log);

    LogInit(&log);
    LogOpen(&log, LOGNAME);
    LogLoadEntries(&log);

    raft_entry_t *e;

    e = LogGet(&log, 3);
    assert(e == NULL);

    e = LogGet(&log, 4);
    assert(e->id == 4);
    raft_entry_release(e);

    e = LogGet(&log, 5);
    assert(e->id == 5);
    raft_entry_release(e);

    e = LogGet(&log, 6);
    assert(e->id == 6);
    raft_entry_release(e);

    LogTerm(&log);
}

/* Verify that on pop, we delete second page if necessary */
static void test_log_delete_second_page()
{
    int idx = 0;
    Log log;
    raft_entry_t *e;

    LogInit(&log);
    LogCreate(&log, LOGNAME, DBID, 1, 1, 0);

    append_entry(&log, ++idx, NULL);
    append_entry(&log, ++idx, NULL);
    append_entry(&log, ++idx, NULL);

    /* First index of the second page is 4. */
    LogCompactionBegin(&log);
    append_entry(&log, ++idx, NULL);
    append_entry(&log, ++idx, NULL);

    /* Sanity check */
    assert(LogCount(&log) == 5);
    assert(LogFirstIdx(&log) == 1);
    assert(LogCurrentIdx(&log) == 5);

    e = LogGet(&log, 1);
    assert(e->id == 1);
    raft_entry_release(e);

    e = LogGet(&log, 4);
    assert(e->id == 4);
    raft_entry_release(e);

    /* Delete from index 4 and verify second page is not deleted. */
    LogDelete(&log, 4);
    assert(&log.pages[1] != NULL);

    append_entry(&log, ++idx, NULL);
    append_entry(&log, ++idx, NULL);

    /* Delete from index 3 and verify second page is deleted. */
    LogDelete(&log, 3);
    assert(log.pages[1] == NULL);
    LogTerm(&log);

    LogInit(&log);
    LogOpen(&log, LOGNAME);
    LogLoadEntries(&log);

    e = LogGet(&log, 3);
    assert(e == NULL);

    e = LogGet(&log, 1);
    assert(e->id == 1);
    raft_entry_release(e);

    e = LogGet(&log, 2);
    assert(e->id == 2);
    raft_entry_release(e);

    LogTerm(&log);
}

/* Simulate shutdown/crash in the middle of compaction process. Try to read log
 * from multiple pages. */
static void test_log_start_with_two_pages()
{
    int idx = 0;
    Log log;

    LogInit(&log);
    LogCreate(&log, LOGNAME, DBID, 1, 1, 0);

    append_entry(&log, ++idx, NULL);
    append_entry(&log, ++idx, NULL);
    append_entry(&log, ++idx, NULL);

    LogCompactionBegin(&log);
    append_entry(&log, ++idx, NULL);
    append_entry(&log, ++idx, NULL);
    LogTerm(&log);

    /* Try to read files. */
    LogInit(&log);
    LogOpen(&log, LOGNAME);
    LogLoadEntries(&log);

    raft_entry_t *e;

    assert(LogCount(&log) == 5);
    assert(LogCompactionIdx(&log) == 3);
    assert(LogCurrentIdx(&log) == 5);

    e = LogGet(&log, 5);
    assert(e->id == 5);
    raft_entry_release(e);

    e = LogGet(&log, 1);
    assert(e->id == 1);
    raft_entry_release(e);

    LogTerm(&log);
}

void test_log()
{
    test_run(test_log_load_entries);
    test_run(test_log_random_access);
    test_run(test_log_random_access_with_snapshot);
    test_run(test_log_write_after_read);
    test_run(test_log_index_rebuild);
    test_run(test_log_delete);
    test_run(test_log_fuzzer);
    test_run(test_entry_cache_sanity);
    test_run(test_entry_cache_start_index_change);
    test_run(test_entry_cache_delete_head);
    test_run(test_entry_cache_delete_tail);
    test_run(test_entry_cache_fuzzer);
    test_run(test_entry_cache_compact);
    test_run(test_meta_persistence);
    test_run(test_crc32c);
    test_run(test_corruption_header);
    test_run(test_corruption_entry);
    test_run(test_log_compaction);
    test_run(test_log_delete_second_page);
    test_run(test_log_start_with_two_pages);
}
