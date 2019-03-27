MULTI/EXEC Support
==================

Basic Flow
----------

When `RAFT MULTI` is issued, an entry is created in the `multi_clients` dictionary
for the current client.  When an entry exists, every new command is append to
the entry and a `QUEUED` response is generated.

When `RAFT EXEC` is issued and an entry exists for the client, a
`RaftRedisCommandArray` with all commands is created and processed as a regular
command.

We need a **new Module API** to catch disconnected clients and clean up their
state.

WATCH
-----

`WATCH` needs to be implemented by the Raft module itself, because we need to
detect and fail an `EXEC` before creating an entry and propagating it.

Supporting `WATCH` in proxying mode is difficult because of the need to
propagate the watched keys state between nodes on leader election (in case
re-election takes place between the `WATCH` and `EXEC`).

To support watch:
1. Every `WATCH` command should be propagated to Redis directly.
2. When `MULTI` and `EXEC` are performed, we need a **new Module API** to query
   Redis and determine if `EXEC` would succeed, and fail early if not.

Proxying
--------

Proxying should work for the simple `MULTI` case, but there are issues with
WATCH:

1. We need to be sure we maintain a dedicated proxy connection per client,
   because `WATCH` lives in a connection context.
2. If a new leader is elected between `WATCH` and `EXEC`, we must not proxy
   commands as the `WATCH` state will be invalid on the new leader.


