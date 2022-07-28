# Redismodule Raft Server Example

### Building

To build:

    mkdir build && cd build
    cmake ..
    make

`redisraft.so` will be created under the project directory.

### Creating a cluster

**Note**: RedisRaft requires Redis 7.0 or above.

To create a three-node cluster, start the first node:

    redis-server \
        --port 5001 \
        --loadmodule <path-to>/redisraft.so \
        --raft.log-filename raftlog1.db \
        --raft.snapshot-filename snapshot1.db \
        --raft.addr localhost:5001

Then initialize the cluster:

    redis-cli -p 5001 raft.cluster init

Now start the second node, and run the `RAFT.CLUSTER JOIN` command to join it to the existing cluster:

    redis-server \
        --port 5002 \
        --loadmodule <path-to>/redisraft.so \
        --raft.log-filename raftlog2.db \
        --raft.snapshot-filename snapshot2.db \
        --raft.addr localhost:5002

    redis-cli -p 5002 RAFT.CLUSTER JOIN localhost:5001

Now add the third node in the same way:

    redis-server \
        --port 5003 \
        --loadmodule <path-to>/redisraft.so \
        --raft.log-filename raftlog3.db 
        --raft.snapshot-filename snapshot3.db \
        --raft.addr localhost:5003

    redis-cli -p 5003 RAFT.CLUSTER JOIN localhost:5001

To query the cluster state, run the `INFO raft` command:

    redis-cli -p 5001 INFO raft

> **Warning**
> Be careful with `--raft.snapshot-filename` and `--raft.log-filename` configs. 
> If multiple servers are using the same directory, these filenames must be unique to nodes. 

## Documentation

- Data-set is a single string key. There are just two commands:
  - Write command:
    ```
    redis-cli -p 5001 raft.write newvalue
    ```

  - Read command:
    ```
    redis-cli -p 5001 raft.read
    ```
    
- Quick description of the files:
  
| File                 | Description                                                                                                                                   |
|----------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| connection.c         | Connection handling between nodes                                                                                                             |
| entrycache.c         | Raft log file is on the disk. We also keep some of the raft log entries in memory for faster access. This is just a performance optimization. | 
| fsync.c              | A separate thread just to call fsync() for the raft log file. This is just a performance optimization                                         |
| hiredis_redismodule.h | hiredis is used for sending messages to other nodes. This is an adapter which makes hiredis to use RedisModule EventLoop API                 | 
| log.c                | Raft log implementation.                                                                                                                      |
| meta.c               | Raft metadata file. Raft requires `term` and last `vote` info to be persisted.                                                                |
| node.c               | A node object for each cluster node                                                                                                           |                                                                                                                   |
| redisraft.c          | Main raft logic                                                                                                                               | 
| serialization.c      | Serialization of an operation into a raft entry                                                                                               |
| snapshot.c           | Snapshot impl. Currently, it dumps data-set which is a single key, along with used node ids and cluster node configuration.                   | 
| threadpool.c         | This is just used for DNS resolution to prevent blocking the main thread.                                                                     |
| util.c               | Helper functions                                                                                                                              |  

## License

RedisRaft is licensed under the [Redis Source Available License (RSAL)](LICENSE.rsal).
