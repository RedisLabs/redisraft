#ifndef REDISRAFT_META_H
#define REDISRAFT_META_H

#include "raft.h"

/* Metadata file is the part of the Raft algorithm. When term changes or if
 * this server votes for a node in an election, Raft requires us to persist
 * metadata file. */
typedef struct RaftMeta {
    raft_term_t term;    /* Last term we're aware of */
    raft_node_id_t vote; /* Our vote in the last term, or RAFT_NODE_ID_NONE */
} RaftMeta;

int RaftMetaRead(RaftMeta *meta, const char *filename);
int RaftMetaWrite(RaftMeta *meta, const char *filename, raft_term_t term, raft_node_id_t vote);

#endif
