
#ifndef RAFT_DEFS_H_
#define RAFT_DEFS_H_

/**
 * Unique entry ids are mostly used for debugging and nothing else,
 * so there is little harm if they collide.
 */
typedef int raft_entry_id_t;

/**
 * Monotonic term counter.
 */
typedef long int raft_term_t;

/**
 * Monotonic log entry index.
 *
 * This is also used to as an entry count size type.
 */
typedef long int raft_index_t;

/**
 * Id used to group entries into sessions
 */
typedef unsigned long long raft_session_t;

/**
 * Size type. This should be at least 64 bits.
 */
typedef unsigned long long raft_size_t;

/**
 * Unique node identifier.
 */
typedef int raft_node_id_t;

/**
 * Unique message identifier.
 */
typedef unsigned long raft_msg_id_t;

/**
 * Time type.
 */
typedef long long raft_time_t;

#endif  /* RAFT_DEFS_H_ */
