/*
 * Bug #29: archiveSnapshot stack buffer overflow / filename truncation
 *
 * Reproduces the exact logic from snapshot.c:882-889.
 * The buffer is allocated to strlen(rdb_filename), but snprintf writes
 * "%s.bak.%d" which needs strlen(rdb_filename) + strlen(".bak.") + digits + 1.
 * snprintf prevents memory corruption but truncates the output filename.
 *
 * Expected: backup filename = "redisraft.rdb.bak.1"
 * Actual:   backup filename = "redisraft.r" (truncated to fit buffer)
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/* Exact reproduction of archiveSnapshot buffer logic */
void archiveSnapshot_buggy(const char *rdb_filename, int node_id)
{
    /* This is the exact code from snapshot.c:884-888 */
    size_t bak_rdb_filename_maxlen = strlen(rdb_filename);
    char bak_rdb_filename[bak_rdb_filename_maxlen];

    int written = snprintf(bak_rdb_filename, bak_rdb_filename_maxlen - 1,
             "%s.bak.%d", rdb_filename, node_id);

    printf("[BUGGY]  rdb_filename       = \"%s\" (len=%zu)\n", rdb_filename, strlen(rdb_filename));
    printf("[BUGGY]  buffer size        = %zu\n", bak_rdb_filename_maxlen);
    printf("[BUGGY]  snprintf limit     = %zu\n", bak_rdb_filename_maxlen - 1);
    printf("[BUGGY]  snprintf wanted    = %d chars\n", written);
    printf("[BUGGY]  actual result      = \"%s\" (len=%zu)\n", bak_rdb_filename, strlen(bak_rdb_filename));
    printf("[BUGGY]  TRUNCATED?         = %s\n", (size_t)written >= bak_rdb_filename_maxlen - 1 ? "YES — BUG!" : "no");
    printf("\n");
}

/* Correct implementation */
void archiveSnapshot_fixed(const char *rdb_filename, int node_id)
{
    size_t bak_rdb_filename_maxlen = strlen(rdb_filename) + 32; /* enough for .bak.NNNNN */
    char bak_rdb_filename[bak_rdb_filename_maxlen];

    int written = snprintf(bak_rdb_filename, bak_rdb_filename_maxlen,
             "%s.bak.%d", rdb_filename, node_id);

    printf("[FIXED]  actual result      = \"%s\" (len=%zu)\n", bak_rdb_filename, strlen(bak_rdb_filename));
    printf("[FIXED]  TRUNCATED?         = %s\n", (size_t)written >= bak_rdb_filename_maxlen ? "YES" : "no — correct");
    printf("\n");
}

int main(void)
{
    printf("=== Bug #29: archiveSnapshot buffer overflow ===\n\n");

    /* Test with typical filenames and various node IDs */
    const char *filenames[] = {
        "redisraft.rdb",       /* typical: 14 chars */
        "raft.rdb",            /* short: 8 chars */
        "/data/dump.rdb",      /* with path: 14 chars */
    };
    int node_ids[] = {1, 12345, 100000};

    for (int i = 0; i < 3; i++) {
        for (int j = 0; j < 3; j++) {
            printf("--- filename=\"%s\", node_id=%d ---\n", filenames[i], node_ids[j]);
            archiveSnapshot_buggy(filenames[i], node_ids[j]);
            archiveSnapshot_fixed(filenames[i], node_ids[j]);
        }
    }

    /* Summary */
    printf("=== CONCLUSION ===\n");
    printf("Every call to archiveSnapshot() produces a truncated backup filename.\n");
    printf("The rename() target is wrong, so snapshot backup is lost or misplaced.\n");
    printf("snprintf prevents memory corruption, but the logic is broken.\n");

    return 0;
}
