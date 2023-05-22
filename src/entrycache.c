/*
 * Copyright Redis Ltd. 2020 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "entrycache.h"

#include "common/redismodule.h"

#include <memory.h>

EntryCache *EntryCacheNew(raft_index_t initial_size)
{
    EntryCache *cache = RedisModule_Calloc(1, sizeof(*cache));

    cache->size = initial_size;
    cache->ptrs = RedisModule_Calloc(cache->size, sizeof(*cache->ptrs));

    return cache;
}

void EntryCacheFree(EntryCache *cache)
{
    for (raft_index_t i = 0; i < cache->len; i++) {
        raft_entry_release(cache->ptrs[(cache->start + i) % cache->size]);
    }

    RedisModule_Free(cache->ptrs);
    RedisModule_Free(cache);
}

void EntryCacheAppend(EntryCache *cache, raft_entry_t *ety, raft_index_t idx)
{
    if (!cache->start_idx) {
        cache->start_idx = idx;
    }

    RedisModule_Assert(cache->start_idx + cache->len == idx);

    /* Enlarge cache if necessary */
    if (cache->len == cache->size) {
        raft_index_t new_size = cache->size * 2;
        cache->ptrs = RedisModule_Realloc(cache->ptrs,
                                          new_size * sizeof(*cache->ptrs));
        if (cache->start > 0) {
            memmove(&cache->ptrs[cache->size],
                    &cache->ptrs[0],
                    cache->start * sizeof(raft_entry_t *));
            memset(&cache->ptrs[0], 0, cache->start * sizeof(raft_entry_t *));
        }

        cache->size = new_size;
    }

    cache->ptrs[(cache->start + cache->len) % cache->size] = ety;
    cache->len++;
    cache->entries_memsize += sizeof(raft_entry_t) + ety->data_len;
    raft_entry_hold(ety);
}

raft_entry_t *EntryCacheGet(EntryCache *cache, raft_index_t idx)
{
    if (idx < cache->start_idx) {
        return NULL;
    }

    raft_index_t relidx = idx - cache->start_idx;
    if (relidx >= cache->len) {
        return NULL;
    }

    raft_entry_t *ety = cache->ptrs[(cache->start + relidx) % cache->size];
    raft_entry_hold(ety);
    return ety;
}

long EntryCacheDeleteHead(EntryCache *cache, raft_index_t first_idx)
{
    long deleted = 0;

    if (first_idx < cache->start_idx) {
        return -1;
    }

    while (first_idx > cache->start_idx && cache->len > 0) {
        raft_entry_t *ety = cache->ptrs[cache->start];
        cache->entries_memsize -= sizeof(raft_entry_t) + ety->data_len;
        raft_entry_release(ety);

        cache->start_idx++;
        cache->ptrs[cache->start] = NULL;
        cache->start++;
        if (cache->start >= cache->size) {
            cache->start = 0;
        }
        cache->len--;
        deleted++;
    }

    if (!cache->len) {
        cache->start_idx = 0;
    }

    return deleted;
}

/* Remove entries from the tail of the cache, starting from (and including)
 * entry @ index.
 *
 * Returns the number of entries removed, or -1 if the specified index is
 * beyond the tail.
 */
long EntryCacheDeleteTail(EntryCache *cache, raft_index_t index)
{
    long deleted = 0;
    raft_index_t i;

    if (index >= cache->start_idx + cache->len) {
        return -1;
    }
    if (index < cache->start_idx) {
        index = cache->start_idx;
    }

    for (i = index; i < cache->start_idx + cache->len; i++) {
        unsigned long int relidx = i - cache->start_idx;
        unsigned long int ofs = (cache->start + relidx) % cache->size;
        raft_entry_t *ety = cache->ptrs[ofs];

        cache->entries_memsize -= sizeof(raft_entry_t) + ety->data_len;
        raft_entry_release(ety);

        cache->ptrs[ofs] = NULL;
        deleted++;
    }

    cache->len -= deleted;

    if (!cache->len) {
        cache->start_idx = 0;
    }

    return deleted;
}

/* Delete entries up to index `limit` to take memory usage under `max_memory`. */
long EntryCacheCompact(EntryCache *cache, size_t max_memory, raft_index_t limit)
{
    long deleted = 0;

    while (cache->len > 0 && cache->start_idx <= limit &&
           cache->entries_memsize > max_memory) {
        raft_entry_t *ety = cache->ptrs[cache->start];
        cache->entries_memsize -= sizeof(raft_entry_t) + ety->data_len;
        raft_entry_release(ety);

        cache->start_idx++;
        cache->ptrs[cache->start] = NULL;
        cache->start++;
        if (cache->start >= cache->size) {
            cache->start = 0;
        }
        cache->len--;
        deleted++;
    }

    if (!cache->len) {
        cache->start_idx = 0;
    }

    return deleted;
}
