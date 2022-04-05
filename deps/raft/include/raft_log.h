#ifndef RAFT_LOG_H_
#define RAFT_LOG_H_

#include "raft_types.h"

typedef struct raft_log raft_log_t;

raft_log_t* raft_log_new(void);

raft_log_t* raft_log_alloc(raft_index_t initial_size);

void raft_log_set_callbacks(raft_log_t* me, raft_log_cbs_t* funcs, void* raft);

void raft_log_free(raft_log_t* me);

void raft_log_clear(raft_log_t* me);

void raft_log_clear_entries(raft_log_t* me);

/**
 * Add entry to log.
 * Don't add entry if we've already added this entry (based off ID)
 * Don't add entries with ID=0
 * @return 0 if unsuccessful; 1 otherwise */
int raft_log_append_entry(raft_log_t* me, raft_entry_t* c);

/**
 * @return number of entries held within log */
raft_index_t raft_log_count(raft_log_t* me);

/**
 * Delete all logs from this log onwards */
int raft_log_delete(raft_log_t* me, raft_index_t idx);

/**
 * Empty the queue. */
void raft_log_empty(raft_log_t * me);

/**
 * Remove oldest entry. Set *etyp to oldest entry on success. */
int raft_log_poll(raft_log_t * me, raft_entry_t **etyp);

/** Get an array of entries from this index onwards.
 * This is used for batching.
 */
raft_entry_t** raft_log_get_from_idx(raft_log_t* me, raft_index_t idx, long *n_etys);

raft_entry_t* raft_log_get_at_idx(raft_log_t* me, raft_index_t idx);

/**
 * @return youngest entry */
raft_entry_t *raft_log_peektail(raft_log_t * me);

raft_index_t raft_log_get_current_idx(raft_log_t* me);

int raft_log_load_from_snapshot(raft_log_t *me, raft_index_t idx, raft_term_t term);

raft_index_t raft_log_get_base(raft_log_t* me);

#endif /* RAFT_LOG_H_ */
