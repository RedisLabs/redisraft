To-do list
==========

Raft Module
-----------

[] Node/redis connection memory free and use-after-free fixes
[] Persistence strategy, integrated with RDB and/or AOF
[] Better cluster membership UX
[] Automatic proxying
[] MULTI command handling
[] Improved read command handling

Raft Library
------------

[] Make it possible to set up callbacks immediately, fix anything that breaks
   because of it now.
[] Improve callback API to prevent unnecessary allocations/deallocations.
