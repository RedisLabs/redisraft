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

#include "raft_types.h"

typedef enum {
    RAFT_ERR_NOT_LEADER=-2,
    RAFT_ERR_ONE_VOTING_CHANGE_ONLY=-3,
    RAFT_ERR_SHUTDOWN=-4,
    RAFT_ERR_NOMEM=-5,
    RAFT_ERR_NEEDS_SNAPSHOT=-6,
    RAFT_ERR_SNAPSHOT_IN_PROGRESS=-7,
    RAFT_ERR_SNAPSHOT_ALREADY_LOADED=-8,
    RAFT_ERR_INVALID_NODEID=-9,
    RAFT_ERR_LEADER_TRANSFER_IN_PROGRESS=-10,
    RAFT_ERR_LAST=-100,
} raft_error_e;

typedef enum {
    RAFT_MEMBERSHIP_ADD,
    RAFT_MEMBERSHIP_REMOVE,
} raft_membership_e;

typedef enum {
    RAFT_STATE_NONE,
    RAFT_STATE_FOLLOWER,
    RAFT_STATE_PRECANDIDATE,
    RAFT_STATE_CANDIDATE,
    RAFT_STATE_LEADER,
} raft_state_e;

typedef enum {
    RAFT_STATE_LEADERSHIP_TRANSFER_TIMEOUT,
    RAFT_STATE_LEADERSHIP_TRANSFER_UNEXPECTED_LEADER,
    RAFT_STATE_LEADERSHIP_TRANSFER_EXPECTED_LEADER,
} raft_transfer_state_e;

/** Allow entries to apply while taking a snapshot */
#define RAFT_SNAPSHOT_NONBLOCKING_APPLY     1
#define RAFT_NODE_ID_NONE                   (-1)

typedef enum {
    /**
     * Regular log type.
     * This is solely for application data intended for the FSM.
     */
    RAFT_LOGTYPE_NORMAL,
    /**
     * Membership change.
     * Non-voting nodes can't cast votes or start elections.
     * Nodes in this non-voting state are used to catch up with the cluster,
     * when trying to the join the cluster.
     */
    RAFT_LOGTYPE_ADD_NONVOTING_NODE,
    /**
     * Membership change.
     * Add a voting node.
     */
    RAFT_LOGTYPE_ADD_NODE,
    /**
     * Membership change.
     * Nodes become demoted when we want to remove them from the cluster
     * Nodes become demoted upon appending of the log message
     * Demoted nodes can't take part in voting or start elections.
     * Demoted nodes become inactive, as per raft_node_is_active.
     * Demoted nodes are removed from cluster when the log message is committed and applied
     */
    RAFT_LOGTYPE_REMOVE_NODE,
    /**
     * Users can piggyback the entry mechanism by specifying log types that
     * are higher than RAFT_LOGTYPE_NUM.
     */
    RAFT_LOGTYPE_NO_OP,
    /**
     * A no-op entry appended automatically when a leader begins a new term,
     * in order to determine the current commit index.
     */
    RAFT_LOGTYPE_NUM=100,
} raft_logtype_e;

/** Entry that is stored in the server's entry log. */
typedef struct raft_entry
{
    /** the entry's term at the point it was created */
    raft_term_t term;

    /** the entry's unique ID */
    raft_entry_id_t id;

    /** type of entry */
    short type;

    /** number of references */
    unsigned short refs;

    /** private local data */
    void *user_data;

    /** free function, used instead of __free if specified */
    void (*free_func) (struct raft_entry *entry);

    /** data length */
    unsigned int data_len;

    /** data */
    char data[0];
} raft_entry_t;

/** Message sent from client to server.
 * The client sends this message to a server with the intention of having it
 * applied to the FSM. */
typedef raft_entry_t msg_entry_t;

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
} msg_entry_response_t;

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

    /** tell nodes that they should allow a vote, even with a healthy leader */
    int transfer_leader;
} msg_requestvote_t;

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
} msg_requestvote_response_t;

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
    int n_entries;

    /** array of pointers to entries within this message */
    msg_entry_t** entries;
} msg_appendentries_t;

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
} msg_appendentries_response_t;

typedef void* raft_server_t;
typedef void* raft_node_t;

/** Callback for sending request vote messages.
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] node The node's ID that we are sending this message to
 * @param[in] msg The request vote message to be sent
 * @return 0 on success */
typedef int (
*func_send_requestvote_f
)   (
    raft_server_t* raft,
    void *user_data,
    raft_node_t* node,
    msg_requestvote_t* msg
    );

/** Callback for sending append entries messages.
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] node The node's ID that we are sending this message to
 * @param[in] msg The appendentries message to be sent
 * @return 0 on success */
typedef int (
*func_send_appendentries_f
)   (
    raft_server_t* raft,
    void *user_data,
    raft_node_t* node,
    msg_appendentries_t* msg
    );

/**
 * Log compaction
 * Callback for telling the user to send a snapshot.
 *
 * @param[in] raft Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] node Node's ID that needs a snapshot sent to
 **/
typedef int (
*func_send_snapshot_f
)   (
    raft_server_t* raft,
    void *user_data,
    raft_node_t* node
    );

/** Callback for detecting when non-voting nodes have obtained enough logs.
 * This triggers only when there are no pending configuration changes.
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] node The node
 * @return 0 does not want to be notified again; otherwise -1 */
typedef int (
*func_node_has_sufficient_logs_f
)   (
    raft_server_t* raft,
    void *user_data,
    raft_node_t* node
    );

#ifndef HAVE_FUNC_LOG
#define HAVE_FUNC_LOG
/** Callback for providing debug logging information.
 * This callback is optional
 * @param[in] raft The Raft server making this callback
 * @param[in] node_id The node id that is the subject of this log. If log is not
 *                    related to a node, this could be 'RAFT_NODE_ID_NONE'.
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] buf The buffer that was logged */
typedef void (
*func_log_f
)    (
    raft_server_t* raft,
    raft_node_id_t node_id,
    void *user_data,
    const char *buf
    );
#endif

/** Callback for saving who we voted for to disk.
 * For safety reasons this callback MUST flush the change to disk.
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] vote The node we voted for
 * @return 0 on success */
typedef int (
*func_persist_vote_f
)   (
    raft_server_t* raft,
    void *user_data,
    raft_node_id_t vote
    );

/** Callback for saving current term (and nil vote) to disk.
 * For safety reasons this callback MUST flush the term and vote changes to
 * disk atomically.
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] term Current term
 * @param[in] vote The node value dictating we haven't voted for anybody
 * @return 0 on success */
typedef int (
*func_persist_term_f
)   (
    raft_server_t* raft,
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
*func_logentry_event_f
)   (
    raft_server_t* raft,
    void *user_data,
    raft_entry_t *entry,
    raft_index_t entry_idx
);

/** Callback for determining which node this configuration log entry
 * affects. This call only applies to configuration change log entries.
 * @return the node ID of the node
 *
 * @param[in] raft The Raft server making this callback
 * @param[in] user_data User data that is passed from Raft server
 * @param[in] entry The entry that the event is happening to.
 * @param[in] entry_idx The entries index in the log
*  @return the node ID of the node
 * */
typedef raft_node_id_t (
*func_get_node_id_f
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
*func_membership_event_f
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
*func_state_event_f
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
 * @param[in] state the leadership transfer result
 */
typedef void (
        *func_transfer_event_f
)   (
        raft_server_t* raft,
        void *user_data,
        raft_transfer_state_e state
);

/** Callback for sending TimeoutNow RPC messages
 * @param[in] raft The Raft server making this callback
 * @param[in] node The node that we are sending this message to
 * @return 0 on success
 */
typedef int (
*func_send_timeoutnow_f
)   (
        raft_server_t* raft,
        raft_node_t* node
    );

/** Callback for determing if we should delay sending appendentries requests
 * @param[in] raft The Raft server making this callback
 * @return 0 if we shouldn't delay, 1 if we should
 */
typedef int (
*func_delay_send_appendentries_f
)   (
        raft_server_t *raft
    );

typedef struct
{
    /** Callback for sending request vote messages */
    func_send_requestvote_f send_requestvote;

    /** Callback for sending appendentries messages */
    func_send_appendentries_f send_appendentries;

    /** Callback for notifying user that a node needs a snapshot sent */
    func_send_snapshot_f send_snapshot;

    /** Callback for finite state machine application
     * Return 0 on success.
     * Return RAFT_ERR_SHUTDOWN if you want the server to shutdown. */
    func_logentry_event_f applylog;

    /** Callback for persisting vote data
     * For safety reasons this callback MUST flush the change to disk. */
    func_persist_vote_f persist_vote;

    /** Callback for persisting term (and nil vote) data
     * For safety reasons this callback MUST flush the term and vote changes to
     * disk atomically. */
    func_persist_term_f persist_term;

    /** Callback for determining which node this configuration log entry
     * affects. This call only applies to configuration change log entries.
     * @return the node ID of the node */
    func_get_node_id_f get_node_id;

    /** Callback for detecting when a non-voting node has sufficient logs. */
    func_node_has_sufficient_logs_f node_has_sufficient_logs;

    /** Callback for being notified of membership changes (optional). */
    func_membership_event_f notify_membership_event;

    /** Callback for being notified of state changes (optional). */
    func_state_event_f notify_state_event;

    /** Callbakc for notified of transfer leadership events (optional) */
    func_transfer_event_f notify_transfer_event;

    /** Callback for catching debugging log messages
     * This callback is optional */
    func_log_f log;

    /** Callback for sending TimeoutNow RPC messages to nodes */
    func_send_timeoutnow_f send_timeoutnow;

    /** Callback for enabling delaying sending of appendentries in raft_recv_entry
     * This callback is optional */
    func_delay_send_appendentries_f delay_send_appendentries;
} raft_cbs_t;

/** A generic notification callback used to allow Raft to notify caller
 * on certain log operations.
 *
 * @param[in] arg Argument passed by Raft in the original call.
 * @param[in] entry Entry for which notification is generated.
 * @param[in] entry_idx Index of entry.
 *
 * The callback *must not* modify the entry or perform any preemptive
 * log operation until it returns.
 */
typedef void (
*func_entry_notify_f
)   (
    void* arg,
    raft_entry_t *entry,
    raft_index_t entry_idx
    );

/** A callback used to notify when queued read requests can be processed.
 *
 * @param[in] arg Argument passed in the original call.
 * @param[in] can_read If non-zero, the read requests may be processed and
 *   returned to the user. Otherwise the request should be treated as if
 *   arriving to a non leader.
 */
typedef void (
*func_read_request_callback_f
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
     * @param[in] first_idx Index of first entry to be left in log.
     * @return
     *  0 on success;
     *  -1 on error (e.g. log is empty).
     */
    int (*poll) (void *log, raft_index_t first_idx);

    /** Remove entries from the end of the log, as necessary when rolling back
     * append operations that have not been committed.
     *
     * @param[in] from_idx Index of first entry to be removed.  All entries
     *  starting from and including this index shall be removed.
     * @param[in] cb Optional callback to execute for every removed entry.
     * @param[in] cb_arg Argument to pass to callback.
     * @return
     *  0 on success;
     *  -1 on error.
     */
    int (*pop) (void *log, raft_index_t from_idx, func_entry_notify_f cb, void *cb_arg);

    /** Get a single entry from the log.
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
    int (*get_batch) (void *log, raft_index_t idx, int entries_n,
            raft_entry_t **entries);

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
} raft_log_impl_t;

/** Initialise a new Raft server, using the in-memory log implementation.
 *
 * Request timeout defaults to 200 milliseconds
 * Election timeout defaults to 1000 milliseconds
 *
 * @return newly initialised Raft server */
raft_server_t* raft_new();

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
void raft_destroy(raft_server_t* me_);

/** De-initialise Raft server. */
void raft_clear(raft_server_t* me);

/** Set callbacks and user data.
 *
 * @param[in] funcs Callbacks
 * @param[in] user_data "User data" - user's context that's included in a callback */
void raft_set_callbacks(raft_server_t* me, raft_cbs_t* funcs, void* user_data);

/** Add node.
 *
 * If a voting node already exists the call will fail.
 *
 * @note The order this call is made is important.
 *  This call MUST be made in the same order as the other raft nodes.
 *  This is because the node ID is assigned depending on when this call is made
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
 *  NULL if the node already exists */
raft_node_t* raft_add_node(raft_server_t* me, void* user_data, raft_node_id_t id, int is_self);

/** Add a node which does not participate in voting.
 * If a node already exists the call will fail.
 * Parameters are identical to raft_add_node
 * @return
 *  node if it was successfully added;
 *  NULL if the node already exists */
raft_node_t* raft_add_non_voting_node(raft_server_t* me_, void* udata, raft_node_id_t id, int is_self);

/** Remove node.
 * @param node The node to be removed. */
void raft_remove_node(raft_server_t* me_, raft_node_t* node);

/** Set election timeout.
 * The amount of time that needs to elapse before we assume the leader is down
 * @param[in] msec Election timeout in milliseconds */
void raft_set_election_timeout(raft_server_t* me, int msec);

/** Set request timeout in milliseconds.
 * The amount of time before we resend an appendentries message
 * @param[in] msec Request timeout in milliseconds */
void raft_set_request_timeout(raft_server_t* me, int msec);

/** Process events that are dependent on time passing.
 * @param[in] msec_elapsed Time in milliseconds since the last call
 * @return
 *  0 on success;
 *  -1 on failure;
 *  RAFT_ERR_SHUTDOWN when server MUST shutdown */
int raft_periodic(raft_server_t* me, int msec_elapsed);

/** Receive an appendentries message.
 *
 * Will block (ie. by syncing to disk) if we need to append a message.
 *
 * The caller is responsible to call raft_entry_release() for all
 * included entries.
 *
 * @param[in] node The node who sent us this message
 * @param[in] ae The appendentries message
 * @param[out] r The resulting response
 * @return
 *  0 on success
 *  RAFT_ERR_NEEDS_SNAPSHOT
 *  */
int raft_recv_appendentries(raft_server_t* me,
                            raft_node_t* node,
                            msg_appendentries_t* ae,
                            msg_appendentries_response_t *r);

/** Receive a response from an appendentries message we sent.
 * @param[in] node The node who sent us this message
 * @param[in] r The appendentries response message
 * @return
 *  0 on success;
 *  -1 on error;
 *  RAFT_ERR_NOT_LEADER server is not the leader */
int raft_recv_appendentries_response(raft_server_t* me,
                                     raft_node_t* node,
                                     msg_appendentries_response_t* r);

/** Receive a requestvote message.
 * @param[in] node The node who sent us this message
 * @param[in] vr The requestvote message
 * @param[out] r The resulting response
 * @return 0 on success */
int raft_recv_requestvote(raft_server_t* me,
                          raft_node_t* node,
                          msg_requestvote_t* vr,
                          msg_requestvote_response_t *r);

/** Receive a response from a requestvote message we sent.
 * @param[in] node The node this response was sent by
 * @param[in] r The requestvote response message
 * @return
 *  0 on success;
 *  RAFT_ERR_SHUTDOWN server MUST shutdown; */
int raft_recv_requestvote_response(raft_server_t* me,
                                   raft_node_t* node,
                                   msg_requestvote_response_t* r);

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
 * @param[in] ety The entry message
 * @param[out] r The resulting response
 * @return
 *  0 on success;
 *  RAFT_ERR_NOT_LEADER server is not the leader;
 *  RAFT_ERR_SHUTDOWN server MUST shutdown;
 *  RAFT_ERR_ONE_VOTING_CHANGE_ONLY there is a non-voting change inflight;
 *  RAFT_ERR_NOMEM memory allocation failure
 */
int raft_recv_entry(raft_server_t* me,
                    msg_entry_t* ety,
                    msg_entry_response_t *r);

/**
 * @return server's node ID; -1 if it doesn't know what it is */
int raft_get_nodeid(raft_server_t* me);

/**
 * @return the server's node */
raft_node_t* raft_get_my_node(raft_server_t *me_);

/**
 * @return currently configured election timeout in milliseconds */
int raft_get_election_timeout(raft_server_t* me);

/**
 * @return number of nodes that this server has */
int raft_get_num_nodes(raft_server_t* me);

/**
 * @return number of voting nodes that this server has */
int raft_get_num_voting_nodes(raft_server_t* me_);

/**
 * @return number of items within log */
raft_index_t raft_get_log_count(raft_server_t* me);

/**
 * @return current term */
raft_term_t raft_get_current_term(raft_server_t* me);

/**
 * @return current log index */
raft_index_t raft_get_current_idx(raft_server_t* me);

/**
 * @return commit index */
raft_index_t raft_get_commit_idx(raft_server_t* me_);

/**
 * @return 1 if follower; 0 otherwise */
int raft_is_follower(raft_server_t* me);

/**
 * @return 1 if leader; 0 otherwise */
int raft_is_leader(raft_server_t* me);

/**
 * @return 1 if precandidate; 0 otherwise */
int raft_is_precandidate(raft_server_t* me);

/**
 * @return 1 if candidate; 0 otherwise */
int raft_is_candidate(raft_server_t* me);

/**
 * @return currently elapsed timeout in milliseconds */
int raft_get_timeout_elapsed(raft_server_t* me);

/**
 * @return request timeout in milliseconds */
int raft_get_request_timeout(raft_server_t* me);

/**
 * @return index of last applied entry */
raft_index_t raft_get_last_applied_idx(raft_server_t* me);

/**
 * @return the node's next index */
raft_index_t raft_node_get_next_idx(raft_node_t* node);

/**
 * @return this node's user data */
raft_index_t raft_node_get_match_idx(raft_node_t* me);

/**
 * @return this node's user data */
void* raft_node_get_udata(raft_node_t* me);

/**
 * Set this node's user data */
void raft_node_set_udata(raft_node_t* me, void* user_data);

/**
 * After sending the snapshot, user can set the next index for the node
 *
 * @param[in] node node
 * @param[in] idx next entry index */
void raft_node_set_next_idx(raft_node_t* me, raft_index_t idx);

/**
 * @param[in] idx The entry's index
 * @return entry from index */
raft_entry_t* raft_get_entry_from_idx(raft_server_t* me, raft_index_t idx);

/**
 * @param[in] node The node's ID
 * @return node pointed to by node ID */
raft_node_t* raft_get_node(raft_server_t* me_, raft_node_id_t id);

/**
 * Used for iterating through nodes
 * @param[in] node The node's idx
 * @return node pointed to by node idx */
raft_node_t* raft_get_node_from_idx(raft_server_t* me_, raft_index_t idx);

/**
 * @return number of votes this server has received this election */
int raft_get_nvotes_for_me(raft_server_t* me);

/**
 * @return node ID of who I voted for */
int raft_get_voted_for(raft_server_t* me);

/** Get what this node thinks the node ID of the leader is.
 * @return node of what this node thinks is the valid leader;
 *   RAFT_NODE_ID_NONE if there is no leader */
raft_node_id_t raft_get_leader_id(raft_server_t* me_);

/** Get what this node thinks the node of the leader is.
 * @return node of what this node thinks is the valid leader;
 *   NULL if there is no leader or
 *        if the leader is not part of the local configuration yet */
raft_node_t* raft_get_leader_node(raft_server_t* me_);

/**
 * @return callback user data */
void* raft_get_udata(raft_server_t* me);

/** Vote for a server.
 * This should be used to reload persistent state, ie. the voted-for field.
 * @param[in] node The server to vote for
 * @return
 *  0 on success */
int raft_vote(raft_server_t* me_, raft_node_t* node);

/** Vote for a server.
 * This should be used to reload persistent state, ie. the voted-for field.
 * @param[in] nodeid The server to vote for by nodeid
 * @return
 *  0 on success */
int raft_vote_for_nodeid(raft_server_t* me_, raft_node_id_t nodeid);

/** Set the current term.
 * This should be used to reload persistent state, ie. the current_term field.
 * @param[in] term The new current term
 * @return
 *  0 on success */
int raft_set_current_term(raft_server_t* me, raft_term_t term);

/** Set the commit idx.
 * This should be used to reload persistent state, ie. the commit_idx field.
 * @param[in] commit_idx The new commit index. */
void raft_set_commit_idx(raft_server_t* me, raft_index_t commit_idx);

/** Add an entry to the server's log.
 * This should be used to reload persistent state, ie. the commit log.
 * @param[in] ety The entry to be appended
 * @return
 *  0 on success;
 *  RAFT_ERR_SHUTDOWN server should shutdown
 *  RAFT_ERR_NOMEM memory allocation failure */
int raft_append_entry(raft_server_t* me, raft_entry_t* ety);

/** Remove the last entry from the server's log.
 * This should only be used when reloading persistent state from an append log
 * store, where removed entries are still in the log but followed by a pop
 * action.
 */
int raft_pop_entry(raft_server_t* me);

/** Confirm if a msg_entry_response has been committed.
 * @param[in] r The response we want to check */
int raft_msg_entry_response_committed(raft_server_t* me_,
                                      const msg_entry_response_t* r);

/** Get node's ID.
 * @return ID of node */
raft_node_id_t raft_node_get_id(raft_node_t* me_);

/** Tell if we are a leader, candidate or follower.
 * @return get state of type raft_state_e. */
int raft_get_state(raft_server_t* me_);

/** Get the most recent log's term
 * @return the last log term */
raft_term_t raft_get_last_log_term(raft_server_t* me_);

/** Turn a node into a voting node.
 * Voting nodes can take part in elections and in-regards to committing entries,
 * are counted in majorities. */
void raft_node_set_voting(raft_node_t* node, int voting);

/** Tell if a node is a voting node or not.
 * @return 1 if this is a voting node. Otherwise 0. */
int raft_node_is_voting(raft_node_t* me_);

/** Check if a node has sufficient logs to be able to join the cluster.
 **/
int raft_node_has_sufficient_logs(raft_node_t* me_);

/** Apply all entries up to the commit index
 * @return
 *  0 on success;
 *  RAFT_ERR_SHUTDOWN when server MUST shutdown */
int raft_apply_all(raft_server_t* me_);

/** Become leader
 * WARNING: this is a dangerous function call. It could lead to your cluster
 * losing it's consensus guarantees. */
int raft_become_leader(raft_server_t* me);

/** Become follower. This may be used to give up leadership. It does not change
 * currentTerm. */
void raft_become_follower(raft_server_t* me);

/** Determine if entry is voting configuration change.
 * @param[in] ety The entry to query.
 * @return 1 if this is a voting configuration change. */
int raft_entry_is_voting_cfg_change(raft_entry_t* ety);

/** Determine if entry is configuration change.
 * @param[in] ety The entry to query.
 * @return 1 if this is a configuration change. */
int raft_entry_is_cfg_change(raft_entry_t* ety);

/** Begin snapshotting.
 *
 * While snapshotting, raft will:
 *  - not apply log entries
 *  - not start elections
 *
 * If the RAFT_SNAPSHOT_NONBLOCKING_APPLY flag is specified, log entries will
 * be applied during snapshot.  The FSM must isolate the snapshot state and
 * guarantee these changes do not affect it.
 *
 * @return 0 on success
 *
 **/
int raft_begin_snapshot(raft_server_t *me_, int flags);

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
int raft_end_snapshot(raft_server_t *me_);

/** Cancel snapshotting.
 *
 * If an error occurs during snapshotting, this function can be called instead
 * of raft_end_snapshot() to cancel the operation.
 *
 * The user MUST be sure the original snapshot is left untouched and remains
 * usable.
 */
int raft_cancel_snapshot(raft_server_t *me_);

/** Get the entry index of the entry that was snapshotted
 **/
raft_index_t raft_get_snapshot_entry_idx(raft_server_t *me_);

/** Check is a snapshot is in progress
 **/
int raft_snapshot_is_in_progress(raft_server_t *me_);

/** Check if entries can be applied now (no snapshot in progress, or
 * RAFT_SNAPSHOT_NONBLOCKING_APPLY specified).
 **/
int raft_is_apply_allowed(raft_server_t* me_);

/** Remove the first log entry.
 * This should be used for compacting logs.
 * @return 0 on success
 **/
int raft_poll_entry(raft_server_t* me_);

/** Get last applied entry
 **/
raft_entry_t *raft_get_last_applied_entry(raft_server_t *me_);

raft_index_t raft_get_first_entry_idx(raft_server_t* me_);

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
 *  -1 on failure
 *  RAFT_ERR_SNAPSHOT_ALREADY_LOADED
 **/
int raft_begin_load_snapshot(raft_server_t *me_,
                       raft_term_t last_included_term,
		       raft_index_t last_included_index);

/** Stop loading snapshot.
 *
 * @return
 *  0 on success
 *  -1 on failure
 **/
int raft_end_load_snapshot(raft_server_t *me_);

raft_index_t raft_get_snapshot_last_idx(raft_server_t *me_);

raft_term_t raft_get_snapshot_last_term(raft_server_t *me_);

void raft_set_snapshot_metadata(raft_server_t *me_, raft_term_t term, raft_index_t idx);

/** Check if a node is active.
 * Active nodes could become voting nodes.
 * This should be used for creating the membership snapshot.
 **/
int raft_node_is_active(raft_node_t* me_);

/** Make the node active.
 *
 * The user sets this to 1 between raft_begin_load_snapshot and
 * raft_end_load_snapshot.
 *
 * @param[in] active Set a node as active if this is 1
 **/
void raft_node_set_active(raft_node_t* me_, int active);

/** Check if a node's voting status has been committed.
 * This should be used for creating the membership snapshot.
 **/
int raft_node_is_voting_committed(raft_node_t* me_);

/** Check if a node's membership to the cluster has been committed.
 * This should be used for creating the membership snapshot.
 **/
int raft_node_is_addition_committed(raft_node_t* me_);

/**
 * Register custom heap management functions, to be used if an alternative
 * heap management is used.
 **/
void raft_set_heap_functions(void *(*_malloc)(size_t),
                             void *(*_calloc)(size_t, size_t),
                             void *(*_realloc)(void *, size_t),
                             void (*_free)(void *));

/** Confirm that a node's voting status is final
 * @param[in] node The node
 * @param[in] voting Whether this node's voting status is committed or not */
void raft_node_set_voting_committed(raft_node_t* me_, int voting);

/** Confirm that a node's voting status is final
 * @param[in] node The node
 * @param[in] committed Whether this node's membership is committed or not */
void raft_node_set_addition_committed(raft_node_t* me_, int committed);

/** Check if a voting change is in progress
 * @param[in] raft The Raft server
 * @return 1 if a voting change is in progress */
int raft_voting_change_is_in_progress(raft_server_t* me_);

/** Get the log implementation handle in use.
 */
void *raft_get_log(raft_server_t* me_);

/** Backward compatible callbacks for log events, implemented by the
 * default in-memory log implementation.
 *
 */

typedef struct {
    /** Callback for adding an entry to the log
     * For safety reasons this callback MUST flush the change to disk.
     * Return 0 on success.
     * Return RAFT_ERR_SHUTDOWN if you want the server to shutdown. */

    func_logentry_event_f log_offer;

    /** Callback for removing the oldest entry from the log
     * For safety reasons this callback MUST flush the change to disk.
     * @note The callback does not need to call raft_entry_release() as
     *   no references are implicitly held.  If access to the entry is
     *   desired after the callback returns, raft_entry_hold() should be
     *   used.
     */
    func_logentry_event_f log_poll;

    /** Callback for removing the youngest entry from the log
     * For safety reasons this callback MUST flush the change to disk.
     * @note The callback does not need to call raft_entry_release() as
     *   no references are implicitly held.  If access to the entry is
     *   desired after the callback returns, raft_entry_hold() should be
     *   used.
     */
    func_logentry_event_f log_pop;

    /** Callback called for every existing log entry when clearing the log.
     * If memory was malloc'd in log_offer and the entry doesn't get a chance
     * to go through log_poll or log_pop, this is the last chance to free it.
     */
    func_logentry_event_f log_clear;
} raft_log_cbs_t;

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
raft_entry_t *raft_entry_new(unsigned int data_len);

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

void raft_handle_append_cfg_change(raft_server_t* me_, raft_entry_t* ety, raft_index_t idx);

void raft_queue_read_request(raft_server_t* me_, func_read_request_callback_f cb, void *cb_arg);

/** Attempt to process read queue.
 */
void raft_process_read_queue(raft_server_t* me_);

/*
 * invoke a leadership transfer to targeted node
 * node_id = targeted node
 * timeout = timeout in ms before this transfer is aborted.  if 0, use default election timeout
 */
int raft_transfer_leader(raft_server_t* me_, raft_node_id_t node_id, long timeout);

/* get the targeted node_id if a leadership transfer is in progress, or RAFT_NODE_ID_NONE if not */
raft_node_id_t raft_get_transfer_leader(raft_server_t* me_);

/* cause this server to force an election on its next raft_periodic function call */
void raft_set_timeout_now(raft_server_t* me_);

#endif /* RAFT_H_ */
