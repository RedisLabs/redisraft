To-do list
==========

Raft Module
-----------

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
