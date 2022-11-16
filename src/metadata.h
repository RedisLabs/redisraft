/*
 * Copyright Redis Ltd. 2022 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef REDISRAFT_METADATA_H
#define REDISRAFT_METADATA_H

#include "raft.h"

/* Raft metadata file to store last voted node id and the current term.*/

typedef struct Metadata {
    raft_term_t term;
    raft_node_id_t vote;
} Metadata;

int MetadataRead(Metadata *m, const char *filename);
int MetadataWrite(Metadata *m, const char *filename, raft_term_t term,
                  raft_node_id_t vote);

#endif
