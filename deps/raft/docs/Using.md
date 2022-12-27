## Table of Contents

- [Introduction](#Introduction)
- [Implementing callbacks](#Implementing-callbacks)
    * [Raft library callbacks](#Raft-library-callbacks)
    * [Log file callbacks](#Log-file-callbacks)
- [Library Initialization](#Library-Initialization)
- [Cluster Initialization](#Cluster-Initialization)
- [Submit Requests](#Submit-requests)
    * [Submit entries](#Submit-entries)
    * [Submit read-only requests](#Submit-read-only-requests)
- [Log compaction](#Log-compaction)
- [Restore state after a restart](#Restore-state-after-a-restart)
    * [Restoring from snapshot](#Restoring-from-snapshot)
    * [Restoring log entries](#Restoring-log-entries)
    * [Restoring metadata](#Restoring-metadata)
    * [State restore example](#State-restore-example)
- [Sending a snapshot and loading a snapshot as a follower](#Sending-a-snapshot-and-loading-a-snapshot-as-a-follower)
    * [Sending a snapshot](#Sending-a-snapshot)
    * [Receiving a snapshot](#Receiving-a-snapshot)
    * [Loading the received snapshot file](#Loading-the-received-snapshot-file)
- [Adding and removing nodes](#Adding-and-removing-nodes)
    * [Adding a node](#Adding-a-node)
    * [Removing a node](#Removing-a-node)


Introduction
===============

This document contains examples of how to integrate the Raft library.

> :bulb: In the code snippets, error handling is skipped. You should check the return code of the library functions all the time.

> :bulb: In the code snippets, functions that need to be implemented by the application will use the "app_" prefix. e.g. app_function()


Implementing callbacks
----------------

Raft library only provides core raft logic. The application should implement some set of callbacks for networking and storage.


### Raft library callbacks
You provide your callbacks to the Raft server using `raft_set_callbacks()`.
The application must implement all the mandatory callbacks. You can find more detailed info in the `raft.h`.

```c
typedef struct
{
    int (*raft_appylog_f) (raft_server_t* r, void *user_data, raft_entry_t *entry, raft_index_t entry_idx);
    int (*raft_persist_metadata_f) (raft_server_t *r, void *user_data, raft_term_t term, raft_node_id_t vote);
    raft_node_id_t (*raft_get_node_id_f) (raft_server_t *r, void *user_data, raft_entry_t *entry, raft_index_t entry_idx);
    int (*raft_node_has_sufficient_logs_f) (raft_server_t *r, void *user_data, raft_node_t *node);
    raft_time_t (*raft_timestamp_f) (raft_server_t *r, void *user_data);
    
    /* Networking */
    int (*raft_send_requestvote_f) (raft_server_t *r, void *user_data, raft_node_t *node, raft_requestvote_req_t *req);
    int (*raft_send_appendentries_f) (raft_server_t *r, void *user_data, raft_node_t *node, raft_appendentries_req_t *req);
    int (*raft_send_snapshot_f) (raft_server_t *r, void *user_data, raft_node_t *node, raft_snapshot_req_t *req);
    int (*raft_send_timeoutnow_f) (raft_server_t *r, void *user_data, raft_node_t *node);
    
    /* Snapshot handling */
    int (*raft_load_snapshot_f) (raft_server_t *r, void *user_data, raft_term_t snapshot_term, raft_index_t snapshot_index);
    int (*raft_get_snapshot_chunk_f) (raft_server_t *r, void *user_data, raft_node_t *node, raft_size_t offset, raft_snapshot_chunk_t *chunk);
    int (*raft_store_snapshot_chunk_f) (raft_server_t *r, void *user_data, raft_index_t snapshot_index, raft_size_t offset, raft_snapshot_chunk_t* chunk);
    int (*raft_clear_snapshot_f) (raft_server_t *r, void *user_data);
    
    /** 
     * Optional callbacks: 
     * 
     * Notification on events, debug logging, message flow control etc.
     */
    void (*raft_membership_event_f) (raft_server_t *r,void *user_data,raft_node_t *node,raft_entry_t *entry,raft_membership_e type);
    void (*raft_state_event_f) (raft_server_t *r, void *user_data, raft_state_e state);
    void (*raft_transfer_event_f) (raft_server_t *r, void *user_data, raft_leader_transfer_e result);
    void (*raft_log_f) (raft_server_t *r, void *user_data, const char *buf);
    int (*raft_backpressure_f) (raft_server_t* r, void *user_data, raft_node_t *node);
    raft_index_t (*raft_get_entries_to_send_f) (raft_server_t *r, void *user_data, raft_node_t *node, raft_index_t idx, raft_index_t entries_n, raft_entry_t **entries);
    
} raft_cbs_t;
```

### Log file callbacks

The application must provide a log implementation and pass that implementation as a parameter to `raft_new_with_log()` call.
Log implementation must implement all the callbacks. You can find more detailed info in the `raft.h`.

```c
typedef struct raft_log_impl
{
    void *(*init) (void *raft, void *arg);
    void (*free) (void *log);
    void (*reset) (void *log, raft_index_t first_idx, raft_term_t term);
    int (*append) (void *log, raft_entry_t *entry);
    int (*poll) (void *log, raft_index_t first_idx);
    int (*pop) (void *log, raft_index_t from_idx);
    raft_entry_t* (*get) (void *log, raft_index_t idx);
    raft_index_t (*get_batch) (void *log, raft_index_t idx, raft_index_t entries_n, raft_entry_t **entries);
    raft_index_t (*first_idx) (void *log);
    raft_index_t (*current_idx) (void *log);
    raft_index_t (*count) (void *log);
    int (*sync) (void *log);
} raft_log_impl_t;
```


Library Initialization
----------------

You should initialize the Raft library on all nodes:

```c
raft_server_t *r = raft_new_with_log(&log_impl, log_impl_arg);
raft_set_callbacks(r, &callback_impls, app_arg);

// Add own node as non-voting, e.g. node ID here is 999999, application generates node IDs.
raft_add_non_voting_node(r, user_data_ptr, 999999, 1); 
```

and call `raft_periodic()` at periodic intervals:

```
//e.g. Call every 100 milliseconds
raft_periodic(r);
```


Cluster Initialization
------------------

You need to follow these steps when you want to initialize a cluster. (Preparing the cluster for the first time use)
- You need to start a single node first. This node will be the first node in the cluster. Other nodes can join this node later.
- Initialize the node, set term 1, and make the node leader.
- Submit the configuration entry of the node.


```c

// Raft Library initialization
raft_server_t *r = raft_new_with_log(&log_impl, arg);
raft_set_callbacks(r, &callback_impls, arg);
raft_add_non_voting_node(r, NULL, 9999999, 1); // Add own node as non-voting

// Bootstrap a cluster. 
// Become the leader and append the configuration entry for the own node.
raft_set_current_term(r, 1);
raft_become_leader(r);

struct app_config cfg = {
    .id = 9999999,
    .addr = ....,  // You should also put node address into the config entry
};

raft_entry_t *ety = raft_entry_new(sizeof(cfg));
ety->type = RAFT_LOGTYPE_ADD_NODE;
memcpy(ety->data, &cfg, sizeof(cfg));

raft_recv_entry(r, ety, NULL);
raft_entry_release(ety);

```

Submit requests
------------------

#### Submit entries

You can submit entries by calling `raft_recv_entry()`:

```c
void app_submit(raft_server_t *r, void *client_request, size_t client_request_len)
{
    raft_entry_t *ety = raft_entry_new(client_request_len);
    
    memcpy(ety->data, client_request, client_request_len);
    ety->type = RAFT_LOGTYPE_NORMAL;
    
    raft_recv_entry(r, ety, NULL);
    raft_entry_release(ety);
}
```

When the Raft library commits the entry, it will call `applylog` callback for the entry:

```c
int app_appylog_callback_impl(raft_server_t *r,
                              void *user_data,
                              raft_entry_t *entry,
                              raft_index_t entry_index)
{
    void *client_request = entry->data;
    size_t client_request_len = entry->data_len;
    
    app_execute_request(client_request, client_request_len);
}
```

#### Submit read-only requests

If your operation is read-only, you can execute it without writing to the log or without replicating it to the other nodes. Still, the Raft library must communicate with the other nodes to verify that local node is still the leader. For that reason, there is another API function to submit read-only requests. When it is safe to execute a request, Raft library will call the callback you provided:

```c
void app_submit_readonly(raft_server_t *r, void *client_request) 
{
   raft_recv_read_request(r, app_readonly_op_callback, request); 
}

void app_readonly_op_callback(void *arg, int can_read)
{
    void *client_request = arg;
    
    if (can_read == 0) {
        // Cluster is down or this node is not the leader anymore. 
        // We cannot execute the command.
        return;
    }
    
    app_execute_readonly(request);
}

```


Log compaction
------------------

Over time, the log file will grow. The library does not initiate log compaction itself. The application should decide when to take a snapshot. (e.g. if the log file grows over a limit)

These are the steps when you want to save a snapshot:

- Call `raft_begin_snapshot()`. 
- Save `last_applied_term` and `last_applied_index` and node configuration into the snapshot.
- Save the snapshot to the disk.
- Call `raft_end_snapshot()`

e.g
```c
void app_take_snapshot(raft_server_t *r)
{
    raft_begin_snapshot(r);

    app_serialize_into_snapshot(raft_get_last_applied_term());
    app_serialize_into_snapshot(raft_get_last_applied_index());

    for (int i = 0; i < raft_get_num_nodes(r); i++) {
        raft_node_t *n = raft_get_node_from_idx(r, i);
        /* Skip uncommitted nodes from the snapshot */
        if (!raft_node_is_addition_committed(n)) {
            continue;
        }
        raft_node_id_t id = raft_get_node_id(n);
        int voting = raft_is_voting_committed(n);
        
        // You may also want to store address info for the node but skipping that
        // part in this example as it is not part of the library
        serialize_node_into_snapshot(id, voting);
    }
    
    app_save_snapshot_to_disk();

    raft_end_snapshot(r);
}

```

Restore state after a restart
------------------

Raft stores all its data in three files: Snapshot, log, and the metadata file (see `raft_persist_metadata_f` in `raft.h` for details).

After a restart, we need to restore the state. The correct order would be:

- Initialize the library (described above)
- Restore snapshot (skip this step if you don't have a snapshot)
- Restore log entries
- Restore metadata


### Restoring from snapshot

The application saves `last_applied_term` and `last_applied_index` along with node configuration into the snapshots.
After loading the state into your application on a restart, you need to configure the Raft library from the configuration info in the snapshot and call `raft_restore_snapshot()`.
e.g: 

```c

// As an example, assuming you read a list of nodes from the snapshot
struct config {
    raft_node_id_t id;
    int voting;
    struct config *next;
};
....

void app_configure_from_snapshot(raft_server_t *r, struct config *head)
{
    struct config *cfg = head;
    
    while (cfg != NULL) {
        if (cfg->id == raft_get_nodeid(r)) {
            // This is our own node configuration
            raft_node_t *n = raft_get_node(r, cfg->id);
            raft_node_set_voting(n, cfg->voting);
        } else {
            if (cfg->voting) {
                raft_add_node(r, n, cfg->id, 0);
            } else {
                raft_add_non_voting_node(r, n, cfg->id, 0);
            }
        }
        cfg = cfg->next;
    }
}

void app_restore_snapshot(raft_server_t *r, 
                          struct config *head,
                          raft_term_t last_applied_term
                          raft_index_t last_applied_index)
{
    app_configure_from_snapshot(r, head);
    raft_restore_snapshot(r, last_applied_term, last_applied_index);
}

```

### Restoring log entries
After restoring the log file (restoring in your log implementation), you need to call one function:

```c
raft_restore_log(r);
```


### Restoring metadata
As the final step, read the term and vote info from the metadata file and then call one library function:
```
raft_restore_metadata(r, term, vote);
```

### State restore example

If we put all steps together:

```c
void app_restore_raft_library()
{
    // Raft Library initialization
    raft_server_t *r = raft_new_with_log(..., ...);
    raft_add_non_voting_node(r, NULL, 1, 1);

    // Assuming you loaded snapshot into your application,
    // extracted node configuration list,
    // extracted last_applied_term and last_applied_index
    // See the example implementation above for this function.
    app_configure_from_snapshot(r, cfg);
    raft_restore_snapshot(r, snapshot_last_term, snapshot_last_index);
    
    app_load_logs_to_impl(); // Load log entries in your log implementation
    raft_restore_log();
    
    app_read_metadata_file(); // Read metadata file and extract term and vote
    raft_restore_metadata(r, term, vote);
}


```



Sending a snapshot and loading a snapshot as a follower
------------------

#### Sending a snapshot

The leader node can send a snapshot to the follower node if the follower lags behind. When that happens, the Raft library will call `raft_get_snapshot_chunk_f` callback and require a chunk from the snapshot from the application. An example implementation would be:

```c

// Assuming you have a pointer to snapshot 
// (e.g. you can use mmap'ed file or if the snapshot is small, you can have a
// copy of the snapshot in memory)
size_t snapshot_file_len;
void *ptr_to_snapshot_file;

int app_get_snapshot_chunk_impl(raft_server_t* raft,
                                void *user_data, 
                                raft_node_t *node, 
                                raft_size_t offset, 
                                raft_snapshot_chunk_t *chunk)
{
    // If you want to limit snapshot_req messages count in flight, you can 
    // return RAFT_ERR_DONE here immediately. (backpressure). 
    // e.g if (nodeimpl->msg_in_flight > 32) { return RAFT_ERR_DONE; }
    
    const int max_chunk_size = 32000;
    
    const raft_size_t remaining_bytes = snapshot_file_len - offset;
    chunk->len = MIN(max_chunk_size, remaining_bytes);
    if (chunk->len == 0) {
        /* All chunks are sent */
        return RAFT_ERR_DONE;
    }

    chunk->data = (char *) ptr_to_snapshot_file + offset;
    chunk->last_chunk = (offset + chunk->len == snapshot_file_len);

    return 0;
}
```

#### Receiving a snapshot

The leader node can send a snapshot to the follower node. Raft library will call required callbacks when that happens:

```c
// This callback is to clear the temporary snapshot file on the disk.
// Remember, the leader can take a newer snapshot and start sending it.
// In that case, the Raft library will instruct application to delete the partial 
// file of the previous snapshot.
int raft_clear_snapshot_f(raft_server_t *raft, void *user_data); 

int raft_store_snapshot_chunk_f(raft_server_t *raft, void *user_data, raft_index_t snapshot_index, raft_size_t offset, raft_snapshot_chunk_t *chunk);
```

An example implementation of `raft_store_snapshot_chunk_f` would be:

```c

int app_raft_store_snapshot_impl(raft_server_t *r, 
                                 void *user_data,
                                 raft_index_t snapshot_index,
                                 raft_size_t offset,
                                 raft_snapshot_chunk_t *chunk)
{
    int flags = O_WRONLY | O_CREAT;
    
    if (offset == 0) {
        flags |= O_TRUNC;
    }

    int fd = open("temp_snapshot_file.db", flags, S_IWUSR | S_IRUSR);
    if (fd == -1) {
        return -1;
    }

    off_t ret_offset = lseek(fd, offset, SEEK_SET);
    if (ret_offset != (off_t) offset) {
        close(fd);
        return -1;
    }

    size_t len = write(fd, chunk->data, chunk->len);
    if (len != chunk->len) {
        close(fd);
        return -1;
    }

    close(fd);
    return 0;
}

```

#### Loading the received snapshot file

When a snapshot is received fully, the Raft library will call `raft_load_snapshot_f` callback.

These are the steps when you want to load the received snapshot. Inside the callback:

- Call `raft_begin_load_snapshot()`
- Read the snapshot into your application
- Configure the Raft library from the application.
- Call `raft_end_load_snapshot()`

An example implementation would be:

```c
int app_raft_load_snapshot(raft_server_t *r,
                           void *user_data,
                           raft_term_t snapshot_term,
                           raft_index_t snapshot_index)
{
    // See the function app_raft_store_snapshot() above. The assumption is 
    // application stores incoming chunks in a file called "temp_snapshot_file.db".
    // If we are inside this callback, it means we've received all the chunks, and 
    // we can rename it as the current snapshot file.
    int ret = rename("temp_snapshot_file.db", "final_snapshot_file.db");
    if (ret != 0) {
        return -1;
    }

    ret = raft_begin_load_snapshot(r, snapshot_term, snapshot_index);
    if (ret != 0) {
        return ret;
    }
    
    // Configure the Raft library from the configuration in the snapshot
    // See the example implementation above for more details about this function
    app_configure_from_snapshot();
    raft_end_load_snapshot(rr->raft);
    
    // Probably, you need to create mmap of the snapshot file or read it into the
    // memory to be prepared for the `raft_get_snapshot_chunk_f` callback.
    // See the example above for the `raft_get_snapshot_chunk_f` callback for more details.
    
    return 0;
}
```

Adding and removing nodes
------------------

You can add or remove a node by submitting configuration change entries. Once the entry is applied, you can submit another configuration change entry. Only one configuration change is allowed at a time.

#### Adding a node
Adding a node is a two-step operation. In the first step, you wait until the new node catches up with the leader. A node with a slow connection may not catch up with the leader. In this case, adding the node without waiting to obtain enough logs would cause unavailability. To prevent that, adding a node is a two-step operation.

e.g. 
A single node cluster adds a new node directly. The majority in the cluster becomes two. To commit an entry, we need to replicate it to both nodes, but we cannot replicate a new entry until the new node gets all the existing entries. So, the cluster cannot commit an entry until the new node catches up.


Add a node in two steps:

- Submit an entry with the type `RAFT_LOGTYPE_ADD_NONVOTING_NODE`.
- The Raft Library will call `raft_node_has_sufficient_logs_f` callback once the new node obtains enough log entries and catches up with the leader.
- Inside that callback, you can submit an entry with the type `RAFT_LOGTYPE_ADD_NODE`.
- When that entry is applied, the configuration change will be final.

e.g:

```c

struct app_node_config {
    raft_node_id_t id;
    int port;
    char host[128];
};

// Step-0 This is an example implementation of `raft_get_node_id_f` callback.
raft_node_id_t app_log_get_node_id(raft_server_t *r,
                                   void *user_data,
                                   raft_entry_t *entry,
                                   raft_index_t entry_idx)
{
    struct app_node_config *cfg = (struct app_node_config *) entry->data;
    return cfg->id;
}

// Step-1
void app_add_node(raft_server_t *r)
{
    struct app_node_config cfg = { 
        .id = 9999999,
        .port = 5000,
        .host = "localhost"
    };
    
    raft_entry_req_t *entry = raft_entry_new(sizeof(cfg));
    entry->type = RAFT_LOGTYPE_ADD_NONVOTING_NODE;
    memcpy(entry->data, &cfg, sizeof(cfg));
    
    int e = raft_recv_entry(r, entry, NULL);
    if (e != 0) {
        // handle failure
    }
    raft_entry_release(entry);
}

// Step-2 (Callback will be called when the new node catches up)
int app_node_has_sufficient_logs(raft_server_t *r,
                                 void *user_data, 
                                 raft_node_t *raft_node)
{
    struct app_node_config cfg = {
        .id = 9999999,
        .port = 5000,
        .host = "localhost"
    };
    
    raft_entry_req_t *entry = raft_entry_new(sizeof(cfg));
    entry->type = RAFT_LOGTYPE_ADD_NODE;
    memcpy(entry->data, &cfg, sizeof(cfg));
    
    int e = raft_recv_entry(r, entry, &response);
    raft_entry_release(entry);

    return e;
}

// Step-3 This is an example implementation of `applylog` callback.
int app_applylog(raft_server_t *r, 
                 void *user_data, 
                 raft_entry_t *entry, 
                 raft_index_t entry_idx)
{
    struct app_node_config *cfg;

    switch (entry->type) {
        case RAFT_LOGTYPE_ADD_NONVOTING_NODE:
            cfg = (struct app_node_config *) entry->data;
            printf("node_id:%d added as non-voting node", cfg->id);
            break;
        case RAFT_LOGTYPE_REMOVE_NODE:
            cfg = (struct app_node_config *) entry->data;
            printf("node_id:%d added as voting node", cfg->id);
            break;

    return 0;
}

```

#### Removing a node

- Submit an entry with the type `RAFT_LOGTYPE_REMOVE_NODE`
- Node will be removed when entry is applied.

```c

struct app_node_config {
    raft_node_id_t id;
};

// Step-0 This is an example implementation of `raft_get_node_id_f` callback.
raft_node_id_t app_log_get_node_id(raft_server_t *r,
                                   void *user_data,
                                   raft_entry_t *entry,
                                   raft_index_t entry_idx)
{
    struct app_node_config *cfg = (struct app_node_config *) entry->data;
    return cfg->id;
}

// Step-1
void app_remove_node()
{
    raft_entry_req_t *entry = raft_entry_new(sizeof(cfg));
    entry->type = RAFT_LOGTYPE_REMOVE_NODE;
    memcpy(entry->data, &cfg, sizeof(cfg));

    int e = raft_recv_entry(r, entry, &resp);
    if (e != 0) {
        // handle error
    }
    raft_entry_release(entry);
} 

// Step-2 This is an example implementation of `applylog` callback.
int app_applylog(raft_server_t *r, 
                 void *user_data, 
                 raft_entry_t *entry, 
                 raft_index_t entry_idx)
{
    struct app_node_config *cfg;

    switch (entry->type) {
        case RAFT_LOGTYPE_REMOVE_NODE:
            cfg = (struct app_node_config *) entry->data;
            printf("node_id:%d has been removed", cfg->id);
            break;
    
    return 0;
}

```