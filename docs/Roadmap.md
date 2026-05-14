RedisRaft Roadmap
=================

This roadmap ranks missing or incomplete RedisRaft features by likely user impact rather than implementation difficulty.

The intent is to make tradeoffs explicit:

* Features that block common Redis application patterns come first.
* Features required for operating larger clusters come next.
* Lower-level compatibility and observability gaps follow after that.

Priority 1: Transaction Compatibility
-------------------------------------

### WATCH / UNWATCH

Impact:

* Blocks optimistic transactions built around `WATCH ... MULTI ... EXEC`.
* Prevents full compatibility with a common Redis concurrency control pattern.
* Limits safe use of `MULTI/EXEC` in follower proxy mode.

Current state:

* Explicitly unsupported in [Using.md](Using.md).
* The codebase already contains partial client-session scaffolding intended for `WATCH`.

Exit criteria:

* `WATCH` and `UNWATCH` work on the leader.
* `EXEC` aborts with Redis-compatible behavior when watched keys are dirtied.
* Sessions are cleaned up correctly on `UNWATCH`, `EXEC`, `DISCARD`, disconnect, and leader change.
* Redis compatibility tests for watch-based transactions pass or have documented exceptions.

Why first:

* This is the most visible transactional correctness gap relative to standalone Redis.

Priority 2: Streams
-------------------

### Redis Streams and Consumer Groups

Impact:

* Blocks event-driven applications that rely on `XADD`, `XREAD`, `XREADGROUP`, `XGROUP`, and related commands.
* Prevents use in several queueing and messaging workloads.

Current state:

* Stream commands are explicitly rejected in the command table.
* The Redis test suite has a substantial set of skipped stream tests.

Exit criteria:

* Core stream commands execute correctly and deterministically under Raft replication.
* Consumer group state is replicated consistently.
* Compatibility tests cover stream creation, reads, acks, pending state, trimming, and group operations.

Why second:

* This is the largest unsupported Redis feature family by application surface area.

Priority 3: Resharding
----------------------

### Dynamic Slot Migration and Resharding

Impact:

* Static slot ownership is a hard operational limit for real multi-shard deployments.
* Prevents online capacity expansion and controlled balancing of hot shards.

Current state:

* External sharding only accepts `STABLE` slot types today.
* The internal shardgroup update path still does not support slot remapping.

Exit criteria:

* Slot states `STABLE`, `MIGRATING`, and `IMPORTING` are supported end to end.
* `RAFT.SHARDGROUP REPLACE` or equivalent can safely reassign slots.
* Redirect and import flows are consistent during migration.
* Failure handling is defined for interrupted migrations and leader changes.

Why third:

* This is the biggest blocker to operating RedisRaft as a practical sharded system rather than a static deployment.

Priority 4: Follower Proxy Robustness
-------------------------------------

### Stateful Follower Proxy Support

Impact:

* Reduces client complexity by avoiding `-MOVED` handling.
* Matters for transparent failover behavior and compatibility with simpler clients.

Current state:

* Follower proxy mode exists but is marked experimental.
* Stateful command sequences are not safely preserved across proxying.

Exit criteria:

* Proxying preserves client session semantics where supported.
* Behavior for `WATCH`, `MULTI/EXEC`, blocking commands, and leader changes is clearly defined.
* Performance and connection-management limitations are documented and tested.

Why fourth:

* Useful, but less fundamental than implementing the transactional and data-model gaps themselves.

Priority 5: Redis Compatibility Gaps
------------------------------------

### Script Control, MONITOR, SLOWLOG, and ACL Edge Cases

Impact:

* Affects observability, debugging, and operational parity more than core correctness.
* Important for production polish and for passing more upstream Redis tests.

Current state:

* Skipped tests call out gaps around `SCRIPT KILL`, timed-out script behavior, `MONITOR`, `SLOWLOG`, and script ACL handling.

Exit criteria:

* User-visible command reporting matches Redis expectations closely enough for tooling.
* Script control behavior is documented and as compatible as the replication model allows.
* Remaining incompatibilities are explicit and intentional.

Why fifth:

* Important for operator experience, but not usually the first blocker for adoption.

Priority 6: Multi-DB Compatibility
----------------------------------

### Multiple Databases / `SELECT`

Impact:

* Blocks applications that depend on Redis logical databases.
* Less important for many modern deployments, which already standardize on DB 0.

Current state:

* Multiple databases are explicitly unsupported.

Exit criteria:

* Either support `SELECT` with well-defined replication semantics, or document a permanent product constraint.

Why sixth:

* Compatibility gap, but lower practical impact than streams, watch-based transactions, or resharding.

Priority 7: Unsupported Native Redis Replication and Persistence Commands
-------------------------------------------------------------------------

### `SYNC`, `PSYNC`, `REPLICAOF`, `SAVE`, `BGSAVE`, and Related Commands

Impact:

* Mostly affects tooling that assumes stock Redis replication or persistence controls.
* Less surprising, because RedisRaft intentionally replaces those mechanisms.

Current state:

* These commands are explicitly marked unsupported.

Exit criteria:

* Keep as intentional product constraints unless a clear operational need appears.
* Improve error messages and documentation where necessary.

Why seventh:

* These are intentional incompatibilities rather than the highest-value missing features.

Recommended Delivery Order
--------------------------

1. Finish `WATCH` / `UNWATCH`.
2. Close the remaining transaction/session cleanup gaps around `EXEC`, `DISCARD`, disconnect, and leader change.
3. Implement Redis Streams incrementally, starting with the core non-group command set.
4. Make shard slot remapping first-class before investing further in advanced sharding ergonomics.
5. Tighten follower proxy behavior once session semantics are solid.
6. Reduce Redis test suite skips in observability and scripting areas.

