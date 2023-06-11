/**
 * Copyright (c) 2013, Willem-Hendrik Thiart
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 *
 * @file
 * @author Willem Thiart himself@willemthiart.com
 */

#ifndef RAFT_H_
#define RAFT_H_

#include <stddef.h>

#include "raft_types.h"

typedef enum {
    RAFT_ERR_NOT_LEADER                  = -2,
    RAFT_ERR_ONE_VOTING_CHANGE_ONLY      = -3,
    RAFT_ERR_SHUTDOWN                    = -4,
    RAFT_ERR_NOMEM                       = -5,
    RAFT_ERR_SNAPSHOT_IN_PROGRESS        = -6,
    RAFT_ERR_SNAPSHOT_ALREADY_LOADED     = -7,
    RAFT_ERR_INVALID_NODEID              = -8,
    RAFT_ERR_LEADER_TRANSFER_IN_PROGRESS = -9,
    RAFT_ERR_DONE                        = -10,
    RAFT_ERR_NOTFOUND                    = -11,
    RAFT_ERR_MISUSE                      = -12,
    RAFT_ERR_TRYAGAIN                    = -13,
} raft_error_e;

typedef enum {
    RAFT_MEMBERSHIP_ADD,
    RAFT_MEMBERSHIP_REMOVE,
} raft_membership_e;

typedef enum {
    RAFT_STATE_FOLLOWER = 1,
    RAFT_STATE_PRECANDIDATE,
    RAFT_STATE_CANDIDATE,
    RAFT_STATE_LEADER,
} raft_state_e;

typedef enum {
    RAFT_LEADER_TRANSFER_TIMEOUT,
    RAFT_LEADER_TRANSFER_UNEXPECTED_LEADER,
    RAFT_LEADER_TRANSFER_EXPECTED_LEADER,
} raft_leader_transfer_e;

typedef enum {
    RAFT_CONFIG_ELECTION_TIMEOUT = 1,
    RAFT_CONFIG_REQUEST_TIMEOUT,
    RAFT_CONFIG_AUTO_FLUSH,
    RAFT_CONFIG_LOG_ENABLED,
    RAFT_CONFIG_NONBLOCKING_APPLY,
    RAFT_CONFIG_DISABLE_APPLY,
} raft_config_e;

#define RAFT_NODE_ID_NONE                   (-1)

typedef enum {
    /** Regular log type. This is for application data intended for the FSM. */
    RAFT_LOGTYPE_NORMAL,

    /** Membership change. Non-voting nodes can't cast votes. Nodes in this
     *  non-voting state are used to catch up with the cluster, when trying to
     *  the join the cluster.
     */
    RAFT_LOGTYPE_ADD_NONVOTING_NODE,

    /** Membership change. Add a voting node.  */
    RAFT_LOGTYPE_ADD_NODE,

    /** Membership change. Remove a node */
    RAFT_LOGTYPE_REMOVE_NODE,


    /** A no-op entry appended automatically when a leader begins a new term in
     *  order to determine the current commit index.
     */
    RAFT_LOGTYPE_NO_OP,

    /** Users can piggyback the entry mechanism by specifying log types that
     *  are higher than RAFT_LOGTYPE_NUM.
     */
    RAFT_LOGTYPE_NUM = 100,
} raft_logtype_e;

typedef struct raft_server_stats {
    /** Miscellaneous */
    unsigned long long appendentries_req_with_entry;
    unsigned long long snapshots_created;
    unsigned long long snapshots_received;
    unsigned long long exec_throttled;

    /** Message types */
    unsigned long long appendentries_req_sent;
    unsigned long long appendentries_req_received;
    unsigned long long appendentries_req_failed;
    unsigned long long appendentries_resp_received;

    unsigned long long snapshot_req_sent;
    unsigned long long snapshot_req_received;
    unsigned long long snapshot_req_failed;
    unsigned long long snapshot_resp_received;

    unsigned long long requestvote_prevote_req_sent;
    unsigned long long requestvote_prevote_req_received;
    unsigned long long requestvote_prevote_req_failed;
    unsigned long long requestvote_prevote_req_granted;
    unsigned long long requestvote_prevote_resp_received;

    unsigned long long requestvote_req_sent;
    unsigned long long requestvote_req_received;
    unsigned long long requestvote_req_failed;
    unsigned long long requestvote_req_granted;
    unsigned long long requestvote_resp_received;
} raft_server_stats_t;

/** Entry that is stored in the server's entry log. */
typedef struct raft_entry
{
    /** the entry's term at the point it was created */
    raft_term_t term;

    /** the entry's unique ID */
    raft_entry_id_t id;

    /** session this entry belongs to **/
    raft_session_t session;

    /** type of entry */
    int type;

    /** number of references */
    unsigned short refs;

    /** private local data */
    void *user_data;

    /** free function, used instead of __free if specified */
    void (*free_func) (struct raft_entry *entry);

    /** data length */
    raft_size_t data_len;

    /** data */
    char data[];
} raft_entry_t;

/** Message sent from client to server.
 * The client sends this message to a server with the intention of having it
 * applied to the FSM. */
typedef raft_entry_t raft_entry_req_t;

/** Entry message response.
 * Indicates to client if entry was committed or not. */
typedef struct
{
    /** the entry's unique ID */
    raft_entry_id_t id;

    /** the entry's term */
    raft_term_t term;

    /** the entry's index */
    raft_index_t idx;
} raft_entry_resp_t;

typedef struct
{
    /** chunk offset */
    raft_size_t offset;

    /** Chunk data pointer */
    void *data;

    /** Chunk len */
    raft_size_t len;

    /** 1 if this is the last chunk */
    int last_chunk;
} raft_snapshot_chunk_t;

/** Vote request message.
 * Sent to nodes when a server wants to become leader.
 * This message could force a leader/candidate to become a follower. */
typedef struct
{
    /** 1 if this is a prevote message, 0 otherwise */
    int prevote;

    /** currentTerm, to force other leader/candidate to step down */
    raft_term_t term;

    /** candidate requesting vote */
    raft_node_id_t candidate_id;

    /** index of candidate's last log entry */
    raft_index_t last_log_idx;

    /** term of candidate's last log entry */
    raft_term_t last_log_term;
} raft_requestvote_req_t;

/** Vote request response message.
 * Indicates if node has accepted the server's vote request. */
typedef struct
{
    /** 1 if this is a prevote message, 0 otherwise */
    int prevote;

    /** term of received requestvote msg */
    raft_term_t request_term;

    /** currentTerm, for candidate to update itself */
    raft_term_t term;

    /** true means candidate received vote */
    int vote_granted;
} raft_requestvote_resp_t;

typedef struct
{
    /** currentTerm, for follower to update itself */
    raft_term_t term;

    /** used to identify the sender node. Useful when this message is received
     * from the nodes that are not part of the configuration yet. */
    raft_node_id_t leader_id;

    /** id, to make it possible to associate responses with requests. */
    raft_msg_id_t msg_id;

    /** last included index of the snapshot */
    raft_index_t snapshot_index;

    /** last included term of the snapshot */
    raft_term_t snapshot_term;

    /** snapshot chunk **/
    raft_snapshot_chunk_t chunk;

} raft_snapshot_req_t;

typedef struct
{
    /** the msg_id this response refers to */
    raft_msg_id_t msg_id;

    /** last included index of the snapshot this response refers to */
    raft_index_t snapshot_index;

    /** currentTerm, to force other leader to step down */
    raft_term_t term;

    /** indicates last acknowledged snapshot offset by the follower */
    raft_size_t offset;

    /** 1 if request is accepted */
    int success;

     /** 1 if this is a response to the final chunk */
    int last_chunk;
} raft_snapshot_resp_t;

/** Appendentries message.
 * This message is used to tell nodes if it's safe to apply entries to the FSM.
 * Can be sent without any entries as a keep alive message.
 * This message could force a leader/candidate to become a follower. */
typedef struct
{
    /** used to identify the sender node. Useful when this message is received
     * from the nodes that are not part of the configuration yet. **/
    raft_node_id_t leader_id;

    /** id, to make it possible to associate responses with requests. */
    raft_msg_id_t msg_id;

    /** currentTerm, to force other leader/candidate to step down */
    raft_term_t term;

    /** the index of the log just before the newest entry for the node who
     * receives this message */
    raft_index_t prev_log_idx;

    /** the term of the log just before the newest entry for the node who
     * receives this message */
    raft_term_t prev_log_term;

    /** the index of the entry that has been appended to the majority of the
     * cluster. Entries up to this index will be applied to the FSM */
    raft_index_t leader_commit;

    /** number of entries within this message */
    raft_index_t n_entries;

    /** array of pointers to entries within this message */
    raft_entry_req_t** entries;
} raft_appendentries_req_t;

/** Appendentries response message.
 * Can be sent without any entries as a keep alive message.
 * This message could force a leader/candidate to become a follower. */
typedef struct
{
    /** the msg_id this response refers to */
    raft_msg_id_t msg_id;

    /** currentTerm, to force other leader/candidate to step down */
    raft_term_t term;

    /** true if follower contained entry matching prevLogidx and prevLogTerm */
    int success;

    /* Non-Raft fields follow: */
    /* Having the following fields allows us to do less book keeping in
     * regards to full fledged RPC */

    /** If success, this is the highest log IDX we've received and appended to
     * our log; otherwise, this is the our currentIndex */
    raft_index_t current_idx;
} raft_appendentries_resp_t;

typedef struct raft_server raft_server_t;
typedef struct raft_node raft_node_t;

/** Callback for sending request vote messages.
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] node The node's ID that we are sending this message to
 * @param[in] msg The request vote message to be sent
 * @return 0 on success */
typedef int (
*raft_send_requestvote_f
)   (
    raft_server_t* raft,
    void *user_data,
    raft_node_t* node,
    raft_requestvote_req_t* msg
    );

/** Callback for sending append entries messages.
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] node The node's ID that we are sending this message to
 * @param[in] msg The appendentries message to be sent
 * @return 0 on success */
typedef int (
*raft_send_appendentries_f
)   (
    raft_server_t* raft,
    void *user_data,
    raft_node_t* node,
    raft_appendentries_req_t* msg
    );

/** Callback for sending snapshot messages.
 * @param[in] raft Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] node Node's ID that needs a snapshot sent to
 * @param[in] msg  Snapshot msg
 **/
typedef int (
*raft_send_snapshot_f
)   (
    raft_server_t* raft,
    void *user_data,
    raft_node_t* node,
    raft_snapshot_req_t* msg
    );

/** Callback for loading the received snapshot. User should load snapshot using
 * raft_begin_load_snapshot() and raft_end_load_snapshot();
 * e.g
 *
 * int loadsnapshot_callback()
 * {
 *      // User loads the received snapshot
 *      int rc = loadSnapshotData();
 *      if (rc != 0) {
 *          return rc;
 *      }
 *
 *      rc = raft_begin_load_snapshot(raft, snapshot_term, snapshot_index);
 *      if (rc != 0) {
 *           return -1;
 *      }
 *
 *      // User should configure nodes using configuration data in the snapshot
 *      // e.g Using raft_add_node(), raft_node_set_voting() etc.
 *      configureNodesFromSnapshot();
 *
 *      raft_end_load_snapshot(raft);
 *      return 0;
 * }
 *
 * @param[in] raft Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] snapshot_term  Received snapshot term
 * @param[in] snapshot_index Received snapshot index
 * @return 0 on success */
typedef int (
*raft_load_snapshot_f
)   (
    raft_server_t* raft,
    void *user_data,
    raft_term_t snapshot_term,
    raft_index_t snapshot_index
);

/** Callback to get a chunk from the snapshot file. This chunk will be sent
 * to the follower.
 *
 *  'chunk' struct fields should be filled with the appropriate data.
 *  To apply backpressure, return RAFT_ERR_DONE.
 * @param[in] raft Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] node Chunk will be sent to this node
 * @param[in] offset Snapshot offset we request
 * @param[in] chunk Snapshot chunk
 * @return 0 on success */
typedef int (
*raft_get_snapshot_chunk_f
)   (
    raft_server_t* raft,
    void *user_data,
    raft_node_t* node,
    raft_size_t offset,
    raft_snapshot_chunk_t* chunk
);

/** Callback to store a snapshot chunk. This chunk is received from the leader.
 * @param[in] raft Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] snapshot_index Last index of the received snapshot
 * @param[in] offset Offset of the chunk we received
 * @param[in] chunk Snapshot chunk
 * @return 0 on success */
typedef int (
*raft_store_snapshot_chunk_f
)   (
    raft_server_t* raft,
    void *user_data,
    raft_index_t snapshot_index,
    raft_size_t offset,
    raft_snapshot_chunk_t* chunk
);

/** Callback to clear incoming snapshot file. This might be called to clean up
 * a partial snapshot file. e.g While we are still receiving snapshot, leader
 * takes another snapshot and starts to send it.
 *
 * @param[in] raft Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @return 0 on success */
typedef int (
*raft_clear_snapshot_f
)   (
    raft_server_t* raft,
    void *user_data
);

/** Callback for detecting when non-voting nodes have obtained enough logs.
 * This triggers only when there are no pending configuration changes.
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] node The node
 * @return 0 does not want to be notified again; otherwise -1 */
typedef int (
*raft_node_has_sufficient_logs_f
)   (
    raft_server_t* raft,
    void *user_data,
    raft_node_t* node
    );

/** Callback for providing debug logging information.
 * This callback is optional
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] buf The buffer that was logged */
typedef void (
*raft_log_f
)    (
    raft_server_t* raft,
    void *user_data,
    const char *buf
    );

/** Callback for saving current term and vote to the disk.
 * For safety reasons this callback MUST flush the term and vote changes to
 * disk atomically.
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] term Current term
 * @param[in] vote Vote
 * @return 0 on success */
typedef int (
*raft_persist_metadata_f
)   (
    raft_server_t *raft,
    void *user_data,
    raft_term_t term,
    raft_node_id_t vote
    );

/** Callback for saving log entry changes.
 *
 * This callback is used for:
 * <ul>
 *      <li>Adding entries to the log (ie. offer)</li>
 *      <li>Removing the first entry from the log (ie. polling)</li>
 *      <li>Removing the last entry from the log (ie. popping)</li>
 *      <li>Applying entries</li>
 * </ul>
 *
 * For safety reasons this callback MUST flush the change to disk.
 *
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] entry The entry that the event is happening to.
 * @param[in] entry_idx The entries index in the log
 * @return 0 on success
 * */
typedef int (
*raft_logentry_event_f
)   (
    raft_server_t* raft,
    void *user_data,
    raft_entry_t *entry,
    raft_index_t entry_idx
);

/** Callback for determining which node this configuration log entry
 * affects. This call only applies to configuration change log entries.
 *
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] entry The entry that the event is happening to.
 * @param[in] entry_idx The entries index in the log
*  @return the node ID of the node
 * */
typedef raft_node_id_t (
*raft_get_node_id_f
)   (
    raft_server_t* raft,
    void *user_data,
    raft_entry_t *entry,
    raft_index_t entry_idx
    );

/** Callback for being notified of membership changes.
 *
 * Implementing this callback is optional.
 *
 * Remove notification happens before the node is about to be removed.
 *
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] node The node that is the subject of this log. Could be NULL.
 * @param[in] entry The entry that was the trigger for the event. Could be NULL.
 * @param[in] type The type of membership change */
typedef void (
*raft_membership_event_f
)   (
    raft_server_t* raft,
    void *user_data,
    raft_node_t *node,
    raft_entry_t *entry,
    raft_membership_e type
    );

/** Callback for being notified of state changes.
 *
 * Implementing this callback is optional.
 *
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] state The new cluster state. */
typedef void (
*raft_state_event_f
)   (
    raft_server_t* raft,
    void *user_data,
    raft_state_e state
    );

/** Call for being notified of leadership transfer events.
 *
 *  Implementing this callback is optional
 *
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] result the leadership transfer result
 */
typedef void (
*raft_transfer_event_f
)   (
    raft_server_t* raft,
    void *user_data,
    raft_leader_transfer_e result
    );

/** Callback for sending TimeoutNow RPC messages
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] node The node that we are sending this message to
 * @return 0 on success
 */
typedef int (
*raft_send_timeoutnow_f
)   (
    raft_server_t* raft,
    void *user_data,
    raft_node_t* node
    );

/** Callback to skip sending raft_appendentries_req to the node
 *
 * Implementing this callback is optional
 *
 * If there are already pending appendentries messages in flight, you may want
 * to skip sending more until you receive response for the previous ones.
 * If the node is a slow consumer and you create raft_appendentries_req for each
 * batch of new entries received, it may cause out of memory.
 *
 * Also, this way you can do batching. If new entries are received with an
 * interval, creating a new appendentries message for each one might be
 * inefficient. For each appendentries message, follower has to write entries
 * to the disk before sending the response. e.g If there are 1000 appendentries
 * message in flight, to commit a new entry, previous 1000 disk write operations
 * must be completed. Considering disk write operations are quite slow, 1000
 * write operations will take quite a time. A better approach would be limiting
 * in flight appendentries messages depending on network conditions and disk
 * performance.
 *
 * @param[in] raft The Raft server making this callback
 * @param[in] node The node that we are about to send raft_appendentries_req to
 * @return 0 to send message
 *         Any other value to skip sending message
 */
typedef int (
*raft_backpressure_f
)   (
    raft_server_t* raft,
    void *user_data,
    raft_node_t* node
    );

/** Callback for fetching entries to send in a appendentries message.
 *
 *  This callback is useful when you want to limit appendentries message size.
 *  Application is supposed to fill the `entries` array by using
 *  raft_get_entry_from_idx() and raft_get_entries_from_idx() functions. If the
 *  application wants to limit the appendentries message size, it can fill the
 *  array partially. As this callback is inside a loop, the remaining entries
 *  will be fetched and sent as another append entries message in the next
 *  callback.
 *
 * @param[in] raft The Raft server making this callback.
 * @param[in] user_data User data that is passed from Raft server.
 * @param[in] node The node that we are sending this message to.
 * @param[in] idx Index of first entry to fetch.
 * @param[in] entries_n Length of entries (max. entries to fetch).
 * @param[out] entries An initialized array of raft_entry_t*.
 * @return Number of entries fetched
 */
typedef raft_index_t (
*raft_get_entries_to_send_f
)   (
    raft_server_t *raft,
    void *user_data,
    raft_node_t *node,
    raft_index_t idx,
    raft_index_t entries_n,
    raft_entry_t **entries
    );

/** Callback to retrieve monotonic timestamp in microseconds .
 *
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @return Timestamp in microseconds
 */
typedef raft_time_t (
*raft_timestamp_f
)   (
    raft_server_t *raft,
    void *user_data
    );

typedef struct
{
    /** Callback for sending request vote messages */
    raft_send_requestvote_f send_requestvote;

    /** Callback for sending appendentries messages */
    raft_send_appendentries_f send_appendentries;

    /** Callback for sending snapshot messages */
    raft_send_snapshot_f send_snapshot;

    /** Callback for sending timeoutnow message */
    raft_send_timeoutnow_f send_timeoutnow;

    /** Callback for loading snapshot. This will be called when we complete
     * receiving snapshot from the leader */
    raft_load_snapshot_f load_snapshot;

    /** Callback to get a chunk of the snapshot file */
    raft_get_snapshot_chunk_f get_snapshot_chunk;

    /** Callback to store a chunk of the snapshot */
    raft_store_snapshot_chunk_f store_snapshot_chunk;

    /** Callback to dismiss temporary file which is used for incoming
     * snapshot chunks */
    raft_clear_snapshot_f clear_snapshot;

    /** Callback for finite state machine application
     * Return 0 on success.
     * Return RAFT_ERR_SHUTDOWN if you want the server to shutdown. */
    raft_logentry_event_f applylog;

    /** Callback for persisting term and vote data
     * For safety reasons this callback MUST flush the term and vote changes to
     * disk atomically. */
    raft_persist_metadata_f persist_metadata;

    /** Callback for determining which node this configuration log entry
     * affects. This call only applies to configuration change log entries.
     * @return the node ID of the node */
    raft_get_node_id_f get_node_id;

    /** Callback for detecting when a non-voting node has sufficient logs. */
    raft_node_has_sufficient_logs_f node_has_sufficient_logs;

    /** Callback to retrieve monotonic timestamp in microseconds */
    raft_timestamp_f timestamp;

    /** (optional) Callback for being notified of membership changes. */
    raft_membership_event_f notify_membership_event;

    /** (optional) Callback for being notified of state changes. */
    raft_state_event_f notify_state_event;

    /** (optional) Callback for being notified of transfer leadership events. */
    raft_transfer_event_f notify_transfer_event;

    /** (optional) Callback for catching debugging log messages. */
    raft_log_f log;

    /** (optional) Callback for deciding whether to send raft_appendentries_req
     * to a node. */
    raft_backpressure_f backpressure;

    /** (optional) Callback for preparing entries to send in
     * a raft_appendentries_req. */
    raft_get_entries_to_send_f get_entries_to_send;

} raft_cbs_t;

/** A callback used to notify when queued read requests can be processed.
 *
 * @param[in] arg Argument passed in the original call.
 * @param[in] can_read If non-zero, the read requests may be processed and
 *   returned to the user. Otherwise the request should be treated as if
 *   arriving to a non leader.
 */
typedef void (
*raft_read_request_callback_f
)   (
    void *arg,
    int can_read
    );

/** Generic Raft Log implementation.
 *
 * This is an abstract interface that can be used to implement pluggable
 * Raft Log implementations, unlike the built-in implementation which is
 * more opinionated (e.g. is entirely in-memory, etc.).
 *
 * The log implementation is expected to be persistent, so it must avoid
 * losing entries that have been appended to it.
 */

typedef struct raft_log_impl
{
    /** Log implementation construction, called exactly once when Raft
     * initializes.
     *
     * @param[in] raft The Raft server using the log.
     * @param[in] arg User-specified initialization argument, as passed to
     *      raft_new().
     * @return Initialized log handle.  This handle is passed as 'log' on
     *      all subsequent calls.
     *
     * @note A common pattern may involve initializing the log engine
     *      in advance and passing a handle to it as arg.  The init function
     *      can then simply return arg.
     */
    void *(*init) (void *raft, void *arg);

    /** Log implementation destruction, called exactly once when Raft
     * shuts down.
     *
     * All memory and resources allocated since init() should be released.
     *
     * @param[in] log The log handle.
     */
    void (*free) (void *log);

    /** Reset log.  All entries should be deleted, and the log is configured
     * such that the next appended log entry would be assigned with the
     * specified index.
     *
     * A log implementation that has been initialized for the first time and
     * contains no persisted data should implicitly perform reset(1).
     *
     * A reset operation with a higher first_idx is expected when the log
     * is compacted after a snapshot is taken.  In this case the log
     * implementation is expected to persist the index and term.
     *
     * @param[in] first_idx Index to assign to the first entry in the log.
     * @param[in] term Term of last applied entry, if reset is called after
     *  a snapshot.
     */
    void (*reset) (void *log, raft_index_t first_idx, raft_term_t term);

    /** Append an entry to the log.
     * @param[in] entry Entry to append.
     * @return
     *  0 on success;
     *  RAFT_ERR_SHUTDOWN server should shutdown;
     *  RAFT_ERR_NOMEM memory allocation failure.
     *
     * @note
     *  The passed raft_entry_t is expected to be allocated by raft_entry_new().
     *  The caller is expected to call raft_entry_release() after the append.
     *
     *  The log implementation shall call raft_entry_hold() in order to
     *  maintain its reference count, and call raft_entry_release() when
     *  the entry is no longer needed.
     *
     * @todo
     * 1. Batch append of multiple entries.
     * 2. Consider an async option to make it possible to implement
     *    I/O in a background thread.
     */
    int (*append) (void *log, raft_entry_t *entry);

    /** Remove entries from the start of the log, as necessary when compacting
     * the log and deleting the oldest entries.
     *
     * The log implementation must call raft_entry_release() on any removed
     * in-memory entries.
     *
     * @param[in] first_idx Index of first entry to be left in log.
     * @return
     *  0 on success;
     *  -1 on error (e.g. log is empty).
     */
    int (*poll) (void *log, raft_index_t first_idx);

    /** Remove entries from the end of the log, as necessary when rolling back
     * append operations that have not been committed.
     *
     * The log implementation must call raft_entry_release() on any removed
     * in-memory entries
     *
     * @param[in] from_idx Index of first entry to be removed.  All entries
     *  starting from and including this index shall be removed.
     * @return
     *  0 on success;
     *  -1 on error.
     */
    int (*pop) (void *log, raft_index_t from_idx);

    /** Get a single entry from the log.
     *
     * The log implementation must call raft_entry_hold() on the fetched entry
     *
     * @param[in] idx Index of entry to fetch.
     * @return
     *  Pointer to entry on success;
     *  NULL if no entry in specified index.
     *
     * @note
     *  Caller must use raft_entry_release() when no longer requiring the
     *  entry.
     */
    raft_entry_t* (*get) (void *log, raft_index_t idx);

    /** Get a batch of entries from the log.
     *
     * The log implementation must call raft_entry_hold() on the fetched entries
     *
     * @param[in] idx Index of first entry to fetch.
     * @param[in] entries_n Length of entries (max. entries to fetch).
     * @param[out] entries An initialized array of raft_entry_t*.
     * @return
     *  Number of entries fetched;
     *  -1 on error.
     *
     * @note
     *  Caller must use raft_entry_release_list() when no longer requiring
     *    the returned entries.
     */
    raft_index_t (*get_batch) (void *log, raft_index_t idx,
                              raft_index_t entries_n, raft_entry_t **entries);

    /** Get first entry's index.
     * @return
     *  Index of first entry.
     */
    raft_index_t (*first_idx) (void *log);

    /** Get current (latest) entry's index.
     * @return
     *  Index of latest entry.
     */
    raft_index_t (*current_idx) (void *log);

    /** Get number of entries in the log.
     * @return
     *  Number of entries.
     */
    raft_index_t (*count) (void *log);

    /** Persist log file to the disk. Usually, implemented as calling fsync()
     * for the log file.
     * @return 0 on success
     *        -1 on error
     */
    int (*sync) (void *log);
} raft_log_impl_t;

/** Initialise a new Raft server, using the in-memory log implementation.
 *
 * Request timeout defaults to 200 milliseconds
 * Election timeout defaults to 1000 milliseconds
 *
 * @return newly initialised Raft server */
raft_server_t* raft_new(void);

/** Initializes a new Raft server with a custom Raft Log implementation.
 *
 * @param[in] log_impl Callbacks structure for the Log implementation to use.
 * @param[in] log_arg Argument to pass to Log implementation's init().
 *
 * @return newly initialised Raft server
 */
raft_server_t* raft_new_with_log(const raft_log_impl_t *log_impl, void *log_arg);

/** De-initialise Raft server.
 * Frees all memory */
void raft_destroy(raft_server_t* me);

/** De-initialise Raft server. */
void raft_clear(raft_server_t* me);

/** Restore term and vote after reading the metadata file from the disk.
 *
 * On a restart, the application should set term and vote after reading metadata
 * file from the disk. See `raft_persist_metadata_f`.
 *
 * @param[in] raft The Raft server
 * @param[in] term term in the metadata file
 * @param[in] vote vote in the metadata file
 * @return 0 on success
 */
int raft_restore_metadata(raft_server_t *me,
                          raft_term_t term,
                          raft_node_id_t vote);

/** Set callbacks and user data.
 *
 * @param[in] funcs Callbacks
 * @param[in] user_data "User data" - user's context that's included in a callback */
void raft_set_callbacks(raft_server_t* me, raft_cbs_t* funcs, void* user_data);

/** Add node.
 *
 * If a voting node already exists the call will fail.
 *
 * @param[in] user_data The user data for the node.
 *  This is obtained using raft_node_get_udata.
 *  Examples of what this could be:
 *  - void* pointing to implementor's networking data
 *  - a (IP,Port) tuple
 * @param[in] id The integer ID of this node
 *  This is used for identifying clients across sessions.
 * @param[in] is_self Set to 1 if this "node" is this server
 * @return
 *  node if it was successfully added;
 *  NULL if a voting node already exists */
raft_node_t* raft_add_node(raft_server_t* me, void* user_data, raft_node_id_t id, int is_self);

/** Add a node which does not participate in voting.
 * If a node already exists the call will fail.
 * Parameters are identical to raft_add_node
 * @return
 *  node if it was successfully added;
 *  NULL if the node already exists */
raft_node_t* raft_add_non_voting_node(raft_server_t* me, void* udata, raft_node_id_t id, int is_self);

/** Remove node.
 * @param node The node to be removed. */
void raft_remove_node(raft_server_t* me, raft_node_t* node);

/** Process events that are dependent on time passing.
 * @return
 *  0 on success;
 *  -1 on failure;
 *  RAFT_ERR_SHUTDOWN when server MUST shutdown */
int raft_periodic(raft_server_t *me);

/** Receive an appendentries message.
 *
 * Will block (ie. by syncing to disk) if we need to append a message.
 *
 * The caller is responsible to call raft_entry_release() for all
 * included entries.
 *
 * @param[in] node The node who sent us this message
 * @param[in] req The appendentries message
 * @param[out] resp The resulting response
 * @return
 *  0 on success
 *  */
int raft_recv_appendentries(raft_server_t *me,
                            raft_node_t *node,
                            raft_appendentries_req_t *req,
                            raft_appendentries_resp_t *resp);

/** Receive a response from an appendentries message we sent.
 * @param[in] node The node who sent us this message
 * @param[in] resp The appendentries response message
 * @return
 *  0 on success;
 *  -1 on error;
 *  RAFT_ERR_NOT_LEADER server is not the leader */
int raft_recv_appendentries_response(raft_server_t *me,
                                     raft_node_t *node,
                                     raft_appendentries_resp_t *resp);

/** Receive a snapshot message.
 * @param[in] node The node who sent us this message
 * @param[in] req The snapshot message
 * @param[out] resp The resulting response
 * @return
 *  0 on success  */
int raft_recv_snapshot(raft_server_t* me,
                       raft_node_t* node,
                       raft_snapshot_req_t *req,
                       raft_snapshot_resp_t *resp);

/** Receive a response from a snapshot message we sent.
 * @param[in] node The node who sent us this message
 * @param[in] resp The snapshot response message
 * @return
 *  0 on success;
 *  -1 on error;
 *  RAFT_ERR_NOT_LEADER server is not the leader */
int raft_recv_snapshot_response(raft_server_t *me,
                                raft_node_t *node,
                                raft_snapshot_resp_t *resp);

/** Receive a requestvote message.
 * @param[in] node The node who sent us this message
 * @param[in] req The requestvote message
 * @param[out] resp The resulting response
 * @return 0 on success */
int raft_recv_requestvote(raft_server_t *me,
                          raft_node_t *node,
                          raft_requestvote_req_t *req,
                          raft_requestvote_resp_t *resp);

/** Receive a response from a requestvote message we sent.
 * @param[in] node The node this response was sent by
 * @param[in] resp The requestvote response message
 * @return
 *  0 on success;
 *  RAFT_ERR_SHUTDOWN server MUST shutdown; */
int raft_recv_requestvote_response(raft_server_t *me,
                                   raft_node_t *node,
                                   raft_requestvote_resp_t *resp);

/** Receive an entry message from the client.
 *
 * Append the entry to the log and send appendentries to followers.
 *
 * Will block (ie. by syncing to disk) if we need to append a message.
 *
 * The caller is responsible to call raft_entry_release() following this
 * call.
 *
 * Will fail:
 * <ul>
 *      <li>if the server is not the leader
 * </ul>
 *
 * @param[in] node The node who sent us this message
 * @param[in] req The entry message
 * @param[out] resp The resulting response
 * @return
 *  0 on success;
 *  RAFT_ERR_NOT_LEADER server is not the leader;
 *  RAFT_ERR_SHUTDOWN server MUST shutdown;
 *  RAFT_ERR_ONE_VOTING_CHANGE_ONLY there is a non-voting change inflight;
 *  RAFT_ERR_NOMEM memory allocation failure
 */
int raft_recv_entry(raft_server_t *me,
                    raft_entry_req_t *req,
                    raft_entry_resp_t *resp);

/**
 * @return server's node ID; RAFT_NODE_ID_NONE if it doesn't know what it is */
raft_node_id_t raft_get_nodeid(raft_server_t *me);

/**
 * @return the server's node */
raft_node_t *raft_get_my_node(raft_server_t *me);

/**
 * @return number of nodes that this server has */
int raft_get_num_nodes(raft_server_t *me);

/**
 * @return number of voting nodes that this server has */
int raft_get_num_voting_nodes(raft_server_t* me);

/**
 * @return number of items within log */
raft_index_t raft_get_log_count(raft_server_t *me);

/**
 * @return current term */
raft_term_t raft_get_current_term(raft_server_t *me);

/**
 * @return current log index */
raft_index_t raft_get_current_idx(raft_server_t *me);

/**
 * @return commit index */
raft_index_t raft_get_commit_idx(raft_server_t *me);

/**
 * @return 1 if follower; 0 otherwise */
int raft_is_follower(raft_server_t* me);

/**
 * @return 1 if leader; 0 otherwise */
int raft_is_leader(raft_server_t *me);

/**
 * @return 1 if precandidate; 0 otherwise */
int raft_is_precandidate(raft_server_t *me);

/**
 * @return 1 if candidate; 0 otherwise */
int raft_is_candidate(raft_server_t *me);

/**
 * @return index of last applied entry */
raft_index_t raft_get_last_applied_idx(raft_server_t *me);

/**
 * @return index of last applied term */
raft_term_t raft_get_last_applied_term(raft_server_t *me);

/**
 * @return this node's user data */
void* raft_node_get_udata(raft_node_t *node);

/**
 * Set this node's user data */
void raft_node_set_udata(raft_node_t *node, void *user_data);

/**
 * @param[in] idx The entry's index
 * @return entry from index */
raft_entry_t* raft_get_entry_from_idx(raft_server_t* me, raft_index_t idx);

/**
 * @param[in] idx The entry's index
 * @param[out] n_etys Number of returned entries
 * @return entry batch from index. Caller must use raft_entry_release_list(). */
raft_entry_t** raft_get_entries_from_idx(raft_server_t* me,
                                         raft_index_t idx,
                                         raft_index_t* n_etys);

/**
 * @param[in] node The node's ID
 * @return node pointed to by node ID */
raft_node_t *raft_get_node(raft_server_t *me, raft_node_id_t id);

/**
 * Used for iterating through nodes
 * @param[in] node The node's idx
 * @return node pointed to by node idx */
raft_node_t *raft_get_node_from_idx(raft_server_t *me, raft_index_t idx);

/**
 * @return node ID of who I voted for */
raft_node_id_t raft_get_voted_for(raft_server_t *me);

/** Get what this node thinks the node ID of the leader is.
 * @return node of what this node thinks is the valid leader;
 *   RAFT_NODE_ID_NONE if there is no leader */
raft_node_id_t raft_get_leader_id(raft_server_t *me);

/** Get what this node thinks the node of the leader is.
 * @return node of what this node thinks is the valid leader;
 *   NULL if there is no leader or
 *        if the leader is not part of the local configuration yet */
raft_node_t *raft_get_leader_node(raft_server_t *me);

/**
 * @return callback user data */
void *raft_get_udata(raft_server_t *me);

/** Set the current term.
 * This should be used to reload persistent state, ie. the current_term field.
 * @param[in] term The new current term
 * @return
 *  0 on success */
int raft_set_current_term(raft_server_t *me, raft_term_t term);

/** Confirm if a msg_entry_response has been committed.
 * @param[in] r The response we want to check */
int raft_msg_entry_response_committed(raft_server_t* me,
                                      const raft_entry_resp_t* r);

/** Get node's ID.
 * @return ID of node */
raft_node_id_t raft_node_get_id(raft_node_t *node);

/** Tell if we are a leader, candidate or follower.
 * @return get state of type raft_state_e. */
int raft_get_state(raft_server_t *me);

/* @return state string */
const char *raft_get_state_str(raft_server_t *me);

/* @return error string */
const char *raft_get_error_str(int err);

/** Get the most recent log's term
 * @return the last log term */
raft_term_t raft_get_last_log_term(raft_server_t *me);

/** Become leader
 * WARNING: this is a dangerous function call. It could lead to your cluster
 * losing it's consensus guarantees. */
int raft_become_leader(raft_server_t *me);

/** Begin snapshotting.
 *
 * While snapshotting, raft will:
 *  - not apply log entries
 *  - not start elections
 *
 * If RAFT_CONFIG_NONBLOCKING_APPLY config is set via raft_config(), log entries
 * will be applied during snapshot. The FSM must isolate the snapshot state and
 * guarantee these changes do not affect it.
 *
 * @return 0 on success
 *
 **/
int raft_begin_snapshot(raft_server_t *me);

/** Stop snapshotting.
 *
 * The user MUST include membership changes inside the snapshot. This means
 * that membership changes are included in the size of the snapshot. For peers
 * that load the snapshot, the user needs to deserialize the snapshot to
 * obtain the membership changes.
 *
 * The user MUST compact the log up to the commit index. This means all
 * log entries up to the commit index MUST be deleted (aka polled).
 *
 * @return
 *  0 on success
 *  -1 on failure
 **/
int raft_end_snapshot(raft_server_t *me);

/** Cancel snapshotting.
 *
 * If an error occurs during snapshotting, this function can be called instead
 * of raft_end_snapshot() to cancel the operation.
 *
 * The user MUST be sure the original snapshot is left untouched and remains
 * usable.
 */
int raft_cancel_snapshot(raft_server_t *me);

/** Check is a snapshot is in progress
 **/
int raft_snapshot_is_in_progress(raft_server_t *me);

/** Check if entries can be applied now (no snapshot in progress, or
 * RAFT_SNAPSHOT_NONBLOCKING_APPLY specified).
 **/
int raft_is_apply_allowed(raft_server_t *me);

/** Get last applied entry
 **/
raft_entry_t *raft_get_last_applied_entry(raft_server_t *me);

raft_index_t raft_get_first_entry_idx(raft_server_t* me);

/** Restore the snapshot after a restart.
 *
 * This function should be called on a restart just after application restores
 * the snapshot and configures the nodes from the snapshot.
 *
 * Correct order to restore the state after a restart:
 * 1- Restore the snapshot
 * 2- Load log entries
 * 3- Restore metadata
 *
 * @param[in] last_included_term Term of the last log of the snapshot
 * @param[in] last_included_index Index of the last log of the snapshot
 * @return
 *  0 on success
 *  RAFT_ERR_MISUSE if the server has already started
 */
int raft_restore_snapshot(raft_server_t *me,
                          raft_term_t last_included_term,
                          raft_index_t last_included_index);

/** Start loading snapshot
 *
 * This is usually the result of a snapshot being loaded.
 * We need to send an appendentries response.
 *
 * This will remove all other nodes (not ourself). The user MUST use the
 * snapshot to load the new membership information.
 *
 * @param[in] last_included_term Term of the last log of the snapshot
 * @param[in] last_included_index Index of the last log of the snapshot
 *
 * @return
 *  0 on success
 *  RAFT_ERR_MISUSE
 *  RAFT_ERR_SNAPSHOT_ALREADY_LOADED
 **/
int raft_begin_load_snapshot(raft_server_t *me,
                             raft_term_t last_included_term,
                             raft_index_t last_included_index);

/** Stop loading snapshot.
 *
 * @return
 *  0 on success
 *  -1 on failure
 **/
int raft_end_load_snapshot(raft_server_t *me);

/** Return last applied entry index that snapshot includes. */
raft_index_t raft_get_snapshot_last_idx(raft_server_t *me);

/** Return last applied entry term that snapshot includes. */
raft_term_t raft_get_snapshot_last_term(raft_server_t *me);

/** Turn a node into a voting node.
 * Voting nodes can take part in elections and in-regards to committing entries,
 * are counted in majorities. */
void raft_node_set_voting(raft_node_t *node, int voting);

/** Tell if a node is a voting node or not.
 * @return 1 if this is a voting node. Otherwise 0. */
int raft_node_is_voting(raft_node_t *node);

/** Check if a node is active.
 * Active nodes could become voting nodes.
 * This should be used for creating the membership snapshot.
 **/
int raft_node_is_active(raft_node_t *node);

/** Make the node active.
 * @param[in] active Set a node as active if this is 1
 **/
void raft_node_set_active(raft_node_t *node, int active);

/** Confirm that a node's voting status is final
 * @param[in] node The node
 * @param[in] voting Whether this node's voting status is committed or not */
void raft_node_set_voting_committed(raft_node_t *node, int voting);

/** Confirm that a node's voting status is final
 * @param[in] node The node
 * @param[in] committed Whether this node's membership is committed or not */
void raft_node_set_addition_committed(raft_node_t *node, int committed);

/** Set next entry index to deliver
 * @param[in] node The node
 * @param[in] idx Next entry index to deliver */
void raft_node_set_next_idx(raft_node_t *node, raft_index_t idx);

/** Check if a node's voting status has been committed.
 * This should be used for creating the membership snapshot.
 **/
int raft_node_is_voting_committed(raft_node_t *node);

/** Check if a node's membership to the cluster has been committed.
 * This should be used for creating the membership snapshot.
 **/
int raft_node_is_addition_committed(raft_node_t *node);

/**
 * Register custom heap management functions, to be used if an alternative
 * heap management is used.
 **/
void raft_set_heap_functions(void *(*_malloc)(size_t),
                             void *(*_calloc)(size_t, size_t),
                             void *(*_realloc)(void *, size_t),
                             void (*_free)(void *));

/** Get the log implementation handle in use.
 */
void *raft_get_log(raft_server_t* me);

/** Allocate a new Raft Log entry.
 *
 * @param[in] data_len Length of user-supplied data for which additional
 *      memory should be allocated at the end of the entry.
 * @returns
 *  Entry pointer (heap allocated).
 *
 * @note All raft_entry_t elements are reference counted and created with an
 *  initial refcount value of 1.  Calling raft_entry_release() immediately would
 *  therefore result with deallocation.
 */
raft_entry_t *raft_entry_new(raft_size_t data_len);

/** Hold the raft_entry_t, i.e. increment refcount by one.
 */
void raft_entry_hold(raft_entry_t *ety);

/** Release the raft_entry_t, i.e. decrement refcount by one and free
 * if refcount reaches zero.
 */
void raft_entry_release(raft_entry_t *ety);

/** Iterate an array of raft_entry_t* and release each element.
 *
 * @param[in] ety_list A pointer to a raft_entry_t* array.
 * @param[in] len Number of entries in the array.
 *
 * @note The array itself is not freed.
 */

void raft_entry_release_list(raft_entry_t **ety_list, size_t len);

/** Log Implementation callbacks structure for the default in-memory
 * log implementation.
 */
extern const raft_log_impl_t raft_log_internal_impl;

/** Enqueue a readonly request
 *
 * @param[in] me The Raft server
 * @param[in] cb Function to be called when it is safe to execute the request
 * @param[in] cb_arg User argument to the callback
 * @return 0 on success
 *         RAFT_ERR_NOT_LEADER server is not the leader
 */
int raft_recv_read_request(raft_server_t* me, raft_read_request_callback_f cb, void *cb_arg);

/** Invoke a leadership transfer to targeted node
 *
 * @param[in] node_id targeted node, RAFT_NODE_ID_NONE for automatic target
 *                    selection. Node with the most entries will be selected.
 * @param[in] timeout timeout in ms before this transfer is aborted.
 *                    if 0, use default election timeout
 * @return 0 on success
 *         RAFT_ERR_NOT_LEADER server is not the leader.
 *         RAFT_ERR_LEADER_TRANSFER_IN_PROGRESS if leadership transfer is
 *                                              already in progress.
 *         RAFT_ERR_INVALID_NODEID target node ID is unknown.
 */
int raft_transfer_leader(raft_server_t* me, raft_node_id_t node_id, long timeout);

/** Return leader transfer target node id
 *
 * @param[in] me The Raft server
 * @return target node id if leadership transfer is in progress, or
 *         RAFT_NODE_ID_NONE otherwise
 */
raft_node_id_t raft_get_transfer_leader(raft_server_t *me);

/** Retrieves collected raft stats
 *
 * @param[in] me The Raft raft_server_t
 * @param[out] stats a pointer to a client allocated buffer to fill collected stats
 */
void raft_get_server_stats(raft_server_t *me, raft_server_stats_t *stats);

/** Force this server to start an election
 *
 * As the last step of leader transfer, current leader calls
 * `raft_send_timeoutnow_f` to send a message to the target node. Target node
 * should start an election by calling this function when it receives timeoutnow
 * message.
 *
 * @param[in] me The Raft server
 * @return 0 on success, non-zero otherwise
 */
int raft_timeout_now(raft_server_t* me);

/** Return number of entries that can be compacted
 *
 * @param[in] me The Raft server
 * @return number of entries that can be compacted
 */
raft_index_t raft_get_num_snapshottable_logs(raft_server_t* me);

/**
 *  Library can be used in two modes:
 *
 * - Auto flush enabled: This is the default mode. In auto flush mode, after
 * each raft_recv_entry() call, raft_log_impl_t's sync() is called to verify
 * entry is persisted. Also, appendentries messages are sent for the new entry
 * immediately. Easier to use the library in this mode but performance will be
 * limited as there is no batching.
 *
 * - Auto flush disabled: To achieve better performance, we need batching.
 * We can write entries to disk in another thread and send a single
 * appendentries message for multiple entries. To do that, we disable auto flush
 * mode. Once we do that, the library user must check the newest log index by
 * calling raft_get_index_to_sync() and verify new entries up to that index are
 * written to the disk, probably in another thread. Also, users should call
 * raft_flush() often to update persisted log index and send new appendentries
 * messages.
 *
 * Example:
 *
 * To disable auto flush mode:
 *      raft_config(raft, 1, RAFT_CONFIG_AUTO_FLUSH, 0);
 *
 * void server_loop() {
 *    while (1) {
 *        HandleNetworkOperations();
 *
 *         for (int i = 0; i < new_readreq_count; i++)
 *             raft_recv_read_request(raft, read_requests[i]);
 *
 *         for (int i = 0; i < new_requests_count; i++)
 *             raft_recv_entry(raft, new_requests[i]);
 *
 *         raft_index_t current_idx = raft_get_index_to_sync(raft);
 *         if (current_idx != 0) {
 *              TriggerAsyncWriteForIndex(current_idx);
 *         }
 *
 *        raft_index_t sync_index = GetLastCompletedSyncIndex();
 *
 *        // This call will send new appendentries messages if necessary
 *        raft_flush(sync_index);
 *    }
 * }
 *
 * -------------------------------------------------------------------------
 * raft_flush():
 *  - Updates persisted index and commit index.
 *  - Applies entries.
 *  - Sends messages(e.g appendentries) to the followers.
 *
 * @param[in] sync_index Entry index of the last persisted entry. '0' to skip
 *                       updating persisted index.
 * @return
 *   0 on success
 *   RAFT_ERR_SHUTDOWN when server MUST shutdown
 */
int raft_flush(raft_server_t* me, raft_index_t sync_index);

/** Returns the latest entry index that needs to be written to the disk.
 *
 * This function is only useful when auto flush is disabled. Same index will be
 * reported once.
 *
 * If the node is not the leader, it returns zero. For followers, disk flush is
 * synchronous. sync() callback is called when an appendentries message
 * received. So, this function does not makes sense if the node is a follower.
 *
 * @param[in] me The Raft server
 * @return entry index need to be written to the disk.
 *         '0' if there is no new entry to write to the disk
 */
raft_index_t raft_get_index_to_sync(raft_server_t *me);

/** Set or get config
 *
 * Valid configurations:
 *
 * election-timeout   : Amount of time before node assumes leader is down.
 * request-timeout    : Heartbeat interval.
 * auto-flush         : See raft_flush().
 * log-enabled        : Enable / disable library logs.
 * non-blocking-apply : See raft_begin_snapshot().
 * disable-apply      : Skip applying entries. Useful for testing.
 *
 *
 * | Enum                          | Type | Valid values     | Default value   |
 * | ----------------------------- | ---- | ---------------- | --------------- |
 * | RAFT_CONFIG_ELECTION_TIMEOUT  | int  | Positive integer | 1000 millis     |
 * | RAFT_CONFIG_REQUEST_TIMEOUT   | int  | Positive integer | 200 millis      |
 * | RAFT_CONFIG_AUTO_FLUSH        | int  | 0 or 1           | 0               |
 * | RAFT_CONFIG_LOG_ENABLED       | int  | 0 or 1           | 0               |
 * | RAFT_CONFIG_NONBLOCKING_APPLY | int  | 0 or 1           | 0               |
 * | RAFT_CONFIG_DISABLE_APPLY     | int  | 0 or 1           | 0               |
 *
 * Example:
 *
 * - Set
 *      raft_config(raft, 1, RAFT_CONFIG_ELECTION_TIMEOUT, 4000);
 *
 * - Get
 *      int election_timeout;
 *      raft_config(raft, 0, RAFT_CONFIG_ELECTION_TIMEOUT, &election_timeout);
 *
 * @param[in] set 1 to set the value, 0 to get the current value.
 * @param[in] config Config enum.
 * @param[in] ... Value to set or destination variable to get the config.
 * @return 0 on success, RAFT_ERR_NOTFOUND if config is missing.
 */
int raft_config(raft_server_t *me, int set, raft_config_e config, ...);

/** Returns non-zero if there are read requests or entries ready to be executed.
 *
 *  If executing entries/read requests take longer than `request-timeout`,
 *  raft_flush() will return early. In that case, application should call
 *  raft_flush() again to continue operation later, preferably after processing
 *  messages from the network. That way, server can send/receive heartbeat
 *  messages and execute long running batch of operations without affecting
 *  cluster availability.
 *
 *  @param[in] me The Raft server
 *  @return 0 if there is no pending operations, non-zero otherwise.
 */
int raft_pending_operations(raft_server_t *me);

/** Restore log entries after a restart.
 *
 * This function should only be called after a restart. After application loads
 * the snapshot and log entries, this function should be called to rebuild
 * cluster configuration from the logs.
 *
 *  @param[in] raft The Raft server
 *  @return 0 on success
 *          RAFT_ERR_MISUSE if a misuse detected.
 */
int raft_restore_log(raft_server_t *me);

#endif /* RAFT_H_ */
