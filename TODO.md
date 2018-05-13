To-do list
==========

Raft Module
-----------

[] Node/redis connection memory free fixes
[] Persistence strategy, integrated with RDB and/or AOF
[] Make persistent log really safe (or kill it)
[] Better cluster membership UX
[] Automatic proxying
[] MULTI command handling
[] Improved read command handling

Raft Library
------------

[] Make it possible to set up callbacks immediately, fix anything that breaks
   because of it now.
[] Configurable types, avoid 32 bit limits for node id, log index, etc.
[] Bug?: Membership management and log rollbacks.  Maybe need a single
   complete log entry for configuration.
[] Bug?: Off-by-one voting configuration change index handling.
[] Improve callback API to prevent unnecessary allocations/deallocations.
