WATCH / UNWATCH Design
======================

This document describes a practical design for implementing `WATCH` and `UNWATCH` in RedisRaft using the session scaffolding that already exists in the codebase.

Goals
-----

* Support Redis-style optimistic transactions on the current leader.
* Preserve atomic `MULTI/EXEC` behavior across all nodes.
* Keep the implementation deterministic under Raft log replication.
* Clean up client watch state on `UNWATCH`, `EXEC`, `DISCARD`, disconnect, and leader change.

Non-Goals
---------

* Full support for `WATCH` in follower proxy mode.
* Cross-leader preservation of watch state without an explicit session model.
* Reproducing every internal Redis implementation detail if a smaller compatible surface is enough.

Current State
-------------

The codebase already contains partial support intended for client sessions:

* `ClientState.watched` tracks whether a client ever used watch/session semantics and is used to decide whether to append a disconnect cleanup entry.
* `handleWatch()` marks the local client state before append time.
* `handleUnwatch()` already appends a `RAFT_LOGTYPE_END_SESSION` entry.
* `raft.c` already has a `client_session_dict`, a `getClientSession()` helper that creates sessions on `WATCH`, and `handleEndClientSession()` to tear them down.

What is missing is the actual watched-key state and the `EXEC` abort decision based on whether watched keys changed.

Required Semantics
------------------

Redis-compatible behavior should be:

* `WATCH key [key ...]` registers keys for the client session and returns `OK`.
* `UNWATCH` clears all watched keys for the client session and returns `OK`.
* `DISCARD` clears queued transaction commands and also clears watch state.
* `EXEC` aborts and returns a null reply when any watched key was modified after `WATCH` and before `EXEC`.
* Successful or aborted `EXEC` ends the watch state for that session.
* If the client disconnects, watch state is eventually removed on all nodes.
* If leadership changes, local sessions are cleared, and local clients using sessions are disconnected.

Design Constraints
------------------

### 1. The abort decision must be replicated safely

The important question is not whether the leader can observe dirty keys locally. The important question is whether every node can reach the same `EXEC` decision deterministically.

A purely local check at append time is not sufficient by itself because:

* other nodes do not share the same in-memory watch bookkeeping;
* follower apply must match leader behavior;
* the result must survive client disconnects and leadership changes.

### 2. The watch state must be represented in the replicated state machine

The existing `client_session_dict` is the right anchor for replicated session existence, but it currently does not track:

* which keys are watched by each session;
* whether the session has been dirtied since the last `WATCH`.

### 3. Local client lifecycle still matters before apply

The existing append-time `ClientState.watched` flag should remain. It solves a real problem: the client can disconnect after append but before apply, and the leader still needs to append a cleanup entry later.

State Model
-----------

Extend the replicated `ClientSession` object to include:

* `client_id`
* `local`
* `watched_keys`: a set of normalized key names
* `dirty`: boolean

Suggested structure:

```c
typedef struct ClientSession {
    raft_session_t client_id;
    bool local;
    bool dirty;
    RedisModuleDict *watched_keys;
} ClientSession;
```

Key points:

* `watched_keys` is state-machine data, not local-only state.
* `dirty` is replicated state-machine data derived from applied writes.
* `local` remains a local convenience used when clearing sessions on leader loss.

Command Handling Model
----------------------

### WATCH

`WATCH` should continue to be appended as a normal Raft command, not applied only locally.

On append:

* mark `ClientState.watched = true` using the existing `handleWatch()` path;
* set `entry->session = RedisModule_GetClientId(ctx)` as normal.

On apply:

* create or fetch the replicated `ClientSession`;
* add each watched key to `watched_keys`;
* clear `dirty = false` only if Redis semantics require a fresh watch set to reset prior dirty state.

Recommended behavior:

* match Redis semantics by treating a fresh `WATCH` call as replacing nothing but extending the currently watched key set while leaving the session dirty if it was already dirtied.

### UNWATCH

`UNWATCH` should keep the current high-level strategy: append a dedicated end-of-session log entry instead of executing `UNWATCH` directly on Redis.

On append:

* use the existing `handleUnwatch()` path to append `RAFT_LOGTYPE_END_SESSION`.

On apply:

* delete the replicated `ClientSession`;
* clear any local `MultiState` if needed only on the requesting node;
* reply `OK`.

### MULTI

No fundamental change is needed to queueing. `MULTI` remains local request state in `MultiState`.

### EXEC

This is the critical path.

Current code swaps the queued commands into the request and appends them as one Raft entry. To support watch semantics safely, `EXEC` needs a replicated precondition.

Recommended design:

* introduce a dedicated Raft log entry type for `EXEC`, for example `RAFT_LOGTYPE_EXEC_TRANSACTION`;
* serialize:
  * client/session id;
  * queued command array;
  * a boolean `expect_clean_watch`;
* on the leader, set `expect_clean_watch = true` whenever a replicated `ClientSession` exists for the client;
* on apply, before executing the transaction:
  * look up the session;
  * if `expect_clean_watch` is true and the session is missing or `dirty == true`, abort `EXEC` with a null reply;
  * otherwise execute the queued command array atomically;
* in both abort and success cases, end the client session after the `EXEC` decision.

Why a dedicated entry type:

* It moves the abort decision into deterministic apply-time state rather than local append-time state.
* It avoids trying to encode watch semantics indirectly into a plain normal command array.
* It makes the transaction precondition explicit and testable.

Alternative:

* encode the watch expectation inside `RaftRedisCommandArray` metadata instead of adding a new log type.

This is possible, but a separate `EXEC` log type is cleaner because `EXEC` has state-machine semantics beyond “execute these commands.”

### DISCARD

Redis semantics require `DISCARD` to clear watch state.

Current `MultiStateReset()` only clears queued commands and error flags. The design should change `DISCARD` handling so that it also ends the watch session.

Recommended approach:

* when `DISCARD` is received and a replicated `ClientSession` exists or local `ClientState.watched` is set, append `RAFT_LOGTYPE_END_SESSION`;
* only reply `OK` when that entry applies;
* if no watch session exists, keep the current fast local behavior.

This keeps session cleanup deterministic across nodes.

Dirtying Rules
--------------

A session becomes dirty when an applied command modifies any watched key.

Implementation rule:

* after applying a write command, extract its key set using the same routing/key-spec logic already used for sharding validation where possible;
* scan active client sessions;
* if any watched key intersects the command key set, set `session->dirty = true`.

This should happen after the write is accepted for application and before subsequent `EXEC` entries are evaluated.

Notes:

* It is acceptable to mark sessions dirty conservatively.
* False positives are better than false negatives because they preserve correctness at the expense of extra transaction aborts.
* Commands with non-trivial key access patterns, scripts, and functions should be treated carefully.

Handling Scripts and Functions
------------------------------

This is the hardest compatibility corner.

Baseline approach:

* if the command metadata cannot reliably expose all touched keys for `EVAL`, `EVALSHA`, `FCALL`, or `FCALL_RO`, conservatively dirty every active watch session.

This is pessimistic but safe.

Possible future optimization:

* only dirty sessions when the script declares keys and those keys intersect the watch set.

That can come later; the first implementation should prefer correctness.

Persistence and Recovery
------------------------

The current session model does not appear to serialize watch session state into snapshots. That is acceptable if session state is intentionally ephemeral and always cleared on restart or leader change.

Recommended rule:

* do not persist `ClientSession` watch state across restart;
* clear all sessions on load and term changes, which is already aligned with the existing `clearClientSessions()` model.

Rationale:

* watch state is a client-session concern, not durable dataset state;
* restoring it across restarts would be difficult to make meaningful because the client connection context is gone.

Follower Proxy Mode
-------------------

Do not claim support for `WATCH` in follower proxy mode in the first version.

Recommended behavior:

* reject `WATCH` and `UNWATCH` on followers when follower proxy mode is enabled, unless the node is leader;
* keep the existing documentation warning that stateful proxying is not safe.

This matches the current design direction in `Development.md`.

Failure and Cleanup
-------------------

### Client Disconnect

Keep the current behavior:

* if a client that has used watch/session semantics disconnects while the node is leader, append `RAFT_LOGTYPE_END_SESSION`.

This remains necessary even if the disconnect happens before prior `WATCH` entries apply.

### Leader Change

Keep the current session clearing model:

* on leader loss / term reset, clear all local client sessions;
* close local clients that have active session state.

This prevents stale watch state from surviving across leadership changes.

### UNWATCH / EXEC / DISCARD Idempotence

Session end should be idempotent:

* deleting a missing session must succeed without error;
* duplicate end-session entries must be harmless.

Implementation Plan
-------------------

1. Extend `ClientSession` with watched-key and dirty-state fields.
2. Implement apply-time handling for `WATCH` to populate the replicated session.
3. Mark sessions dirty when applied writes touch watched keys.
4. Introduce a dedicated replicated `EXEC` precondition so abort decisions happen deterministically on apply.
5. Make `DISCARD` clear watch state via replicated session teardown.
6. Keep `UNWATCH` and disconnect cleanup on the existing `RAFT_LOGTYPE_END_SESSION` path.
7. Reject or clearly document unsupported follower-proxy watch behavior.

Testing Plan
------------

Required integration coverage:

* `WATCH key`, then unrelated write, then `EXEC` succeeds.
* `WATCH key`, then related write, then `EXEC` aborts.
* `WATCH` with multiple keys.
* repeated `WATCH` calls extend the watch set.
* `UNWATCH` clears state and allows `EXEC` to proceed normally.
* `DISCARD` clears watch state.
* disconnect after `WATCH` cleans up replicated session state.
* leader change clears local session state and does not leak watch sessions.
* scripts/functions conservatively dirty watches.
* duplicate cleanup entries are harmless.

Open Questions
--------------

* Should `DISCARD` always be replicated when any watch/session state exists, or should it remain partly local with a best-effort cleanup append?
* Should `EXEC` use a new log type, or should the watch precondition be encoded into existing command-array metadata?
* Is there enough existing key-spec extraction to avoid a separate watch dirtying key parser for complex commands?

Recommendation
--------------

Implement the first version with conservative dirtying and a dedicated replicated `EXEC` precondition.

That is the simplest path that is correct under Raft:

* watch state exists in the replicated state machine,
* the `EXEC` abort decision is deterministic at apply time,
* session cleanup uses the log-based teardown path the codebase already started to build.

