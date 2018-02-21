#include <string.h>
#include <stdlib.h>
#include <assert.h>

#include "redisraft.h"

/*
 * Serialization/Deserialization of argv/argc into/from a raft_entry_data_t.
 */

void redis_raft_serialize(raft_entry_data_t *target, RedisModuleString **argv, int argc)
{
    size_t sz = sizeof(size_t) * (argc + 1);
    size_t len;
    int i;
    char *p;

    /* Compute sizes */
    for (i = 0; i < argc; i++) {
        RedisModule_StringPtrLen(argv[i], &len);
        sz += len;
    }

    /* Serialize argc */
    p = target->buf = RedisModule_Alloc(sz);
    target->len = sz;

    *(size_t *)p = argc;
    p += sizeof(size_t);

    /* Serialize argumnets */
    for (i = 0; i < argc; i++) {
        const char *d = RedisModule_StringPtrLen(argv[i], &len);
        *(size_t *)p = len;
        p += sizeof(size_t);
        memcpy(p, d, len);
        p += len;
    }
}

bool redis_raft_deserialize(RedisModuleCtx *ctx, 
        raft_rediscommand_t *target, raft_entry_data_t *source)
{
    char *p = source->buf;
    size_t argc = *(size_t *)p;
    p += sizeof(size_t);

    target->argv = RedisModule_Calloc(argc, sizeof(RedisModuleString *));
    target->argc = argc;

    int i;
    for (i = 0; i < argc; i++) {
        size_t len = *(size_t *)p;
        p += sizeof(size_t);

        target->argv[i] = RedisModule_CreateString(ctx, p, len);
        p += len;
    }

    return true;
}

void raft_rediscommand_free(RedisModuleCtx *ctx, raft_rediscommand_t *r)
{
    int i;

    for (i = 0; i < r->argc; i++) {
        RedisModule_FreeString(ctx, r->argv[i]);
    }
    RedisModule_Free(r->argv);
}

/*
 * Execution of Raft log on the local instance.  There are two variants:
 * 1) Execution of a raft entry received from another node.
 * 2) Execution of a locally initiated command.
 */

static void execute_log_entry(redis_raft_t *rr, raft_entry_t *entry)
{
    raft_rediscommand_t rcmd;
    redis_raft_deserialize(rr->ctx, &rcmd, &entry->data);

    size_t cmdlen;
    const char *cmd = RedisModule_StringPtrLen(rcmd.argv[0], &cmdlen);

    RedisModule_ThreadSafeContextLock(rr->ctx);
    RedisModuleCallReply *reply = RedisModule_Call(
            rr->ctx, cmd, "v",
            &rcmd.argv[1],
            rcmd.argc - 1);
    RedisModule_FreeCallReply(reply);
    RedisModule_ThreadSafeContextUnlock(rr->ctx);

    raft_rediscommand_free(rr->ctx, &rcmd);
}

static void execute_committed_req(raft_req_t *req)
{
    RedisModuleString *argv;
    int argc;

    size_t cmdlen;
    const char *cmd = RedisModule_StringPtrLen(req->r.raft.argv[0], &cmdlen);

    RedisModule_ThreadSafeContextLock(req->ctx);
    RedisModuleCallReply *reply = RedisModule_Call(
            req->ctx, cmd, "v",
            &req->r.raft.argv[1],
            req->r.raft.argc - 1);
    RedisModule_ThreadSafeContextUnlock(req->ctx);

    RedisModule_ReplyWithCallReply(req->ctx, reply);
    RedisModule_FreeCallReply(reply);
    RedisModule_FreeThreadSafeContext(req->ctx);
    RedisModule_UnblockClient(req->client, NULL);
    req->ctx = NULL;
}

/* Iterate commit queue and execute commands whose entries were committed */
static void iterate_cqueue(redis_raft_t *rr)
{
    while (!STAILQ_EMPTY(&rr->cqueue)) {
        raft_req_t *req = STAILQ_FIRST(&rr->cqueue);
        if (!raft_msg_entry_response_committed(rr->raft, &req->r.raft.response)) {
            return;
        }

        /* Execute and reply */
        execute_committed_req(req);
        STAILQ_REMOVE_HEAD(&rr->cqueue, entries);
        raft_req_free(req);
    }
}

/*
 * Callbacks to handle async Redis commands we send to remote peers.
 */

static void requestvote_response_handler(redisAsyncContext *c, void *r, void *privdata)
{
    node_t *node = privdata;
    redis_raft_t *rr = node->rr;

    redisReply *reply = r;
    if (!reply || reply->type != REDIS_REPLY_ARRAY || reply->elements != 2 ||
            reply->element[0]->type != REDIS_REPLY_INTEGER ||
            reply->element[1]->type != REDIS_REPLY_INTEGER) {
        LOG_NODE(node, "invalid RAFT.REQUESTVOTE reply\n");
        return;
    }

    msg_requestvote_response_t response = {
        .term = reply->element[0]->integer,
        .vote_granted = reply->element[1]->integer
    };

    raft_node_t *raft_node = raft_get_node(rr->raft, node->id);
    assert(raft_node != NULL);

    int ret;
    if ((ret = raft_recv_requestvote_response(
            rr->raft,
            raft_node,
            &response)) != 0) {
        LOG("raft_recv_requestvote_response failed => %d\n", ret);
    }
    LOG_NODE(node, "received requestvote response\n");
}


static void appendentries_response_handler(redisAsyncContext *c, void *r, void *privdata)
{
    node_t *node = privdata;
    redis_raft_t *rr = node->rr;

    redisReply *reply = r;
    if (!reply || reply->type != REDIS_REPLY_ARRAY || reply->elements != 4 ||
            reply->element[0]->type != REDIS_REPLY_INTEGER ||
            reply->element[1]->type != REDIS_REPLY_INTEGER ||
            reply->element[2]->type != REDIS_REPLY_INTEGER ||
            reply->element[3]->type != REDIS_REPLY_INTEGER) {
        LOG_NODE(node, "invalid RAFT.APPENDENTRIES reply\n");
        return;
    }

    msg_appendentries_response_t response = {
        .term = reply->element[0]->integer,
        .success = reply->element[1]->integer,
        .current_idx = reply->element[2]->integer,
        .first_idx = reply->element[3]->integer
    };

    raft_node_t *raft_node = raft_get_node(rr->raft, node->id);
    assert(raft_node != NULL);

    int ret;
    if ((ret = raft_recv_appendentries_response(
            rr->raft,
            raft_node,
            &response)) != 0) {
        LOG_NODE(node, "raft_recv_appendentries_response failed => %d\n", ret);
    }
    LOG_NODE(node, "received appendentries response\n");

    /* Maybe we have pending stuff to apply now */
    iterate_cqueue(rr); 
}

/*
 * Callbacks we provide to the Raft library
 */

static int __raft_send_requestvote(raft_server_t *raft, void *user_data,
        raft_node_t *raft_node, msg_requestvote_t *msg)
{
    node_t *node = (node_t *) raft_node_get_udata(raft_node);
    redis_raft_t *rr = user_data;

    if (!(node->state & NODE_CONNECTED)) {
        node_connect(node, rr);
        LOG_NODE(node, "not connected, state=%u\n", node->state);
        return 0;
    }

    /* RAFT.REQUESTVOTE <src_node_id> <term> <candidate_id> <last_log_idx> <last_log_term> */
    if (redisAsyncCommand(node->rc, requestvote_response_handler,
                node, "RAFT.REQUESTVOTE %d %d:%d:%d:%d",
                raft_get_nodeid(raft),
                msg->term,
                msg->candidate_id,
                msg->last_log_idx,
                msg->last_log_term) != REDIS_OK) {
        LOG_NODE(node, "failed requestvote");
    }

    return 0;
}

static int __raft_send_appendentries(raft_server_t *raft, void *user_data,
        raft_node_t *raft_node, msg_appendentries_t *msg)
{
    node_t *node = (node_t *) raft_node_get_udata(raft_node);
    redis_raft_t *rr = user_data;

    int argc = 4 + msg->n_entries * 2;
    char *argv[argc];
    size_t argvlen[argc];

    if (!(node->state & NODE_CONNECTED)) {
        node_connect(node, rr);
        LOG_NODE(node, "not connected, state=%u\n", node->state);
        return 0;
    }

    argv[0] = "RAFT.APPENDENTRIES";
    argvlen[0] = strlen(argv[0]);
    argvlen[1] = asprintf(&argv[1], "%d", raft_get_nodeid(raft));
    argvlen[2] = asprintf(&argv[2], "%d:%d:%d:%d",
            msg->term, 
            msg->prev_log_idx,
            msg->prev_log_term,
            msg->leader_commit);
    argvlen[3] = asprintf(&argv[3], "%d", msg->n_entries);

    int i;
    for (i = 0; i < msg->n_entries; i++) {
        raft_entry_t *e = &msg->entries[i];
        argvlen[4 + i*2] = asprintf(&argv[4 + i*2], "%d:%d:%d", e->term, e->id, e->type);
        argvlen[5 + i*2] = e->data.len;
        argv[5 + i*2] = e->data.buf;
    }

    if (redisAsyncCommandArgv(node->rc, appendentries_response_handler,
                node, argc, (const char **)argv, argvlen) != REDIS_OK) {
        LOG_NODE(node, "failed appendentries");
    }

    free(argv[1]);
    free(argv[2]);
    free(argv[3]);
    for (i = 0; i < msg->n_entries; i++) {
        free(argv[4 + i*2]);
    }
    return 0;
}

static int __raft_persist_vote(raft_server_t *raft, void *user_data, int vote)
{
    return 0;
}

static int __raft_persist_term(raft_server_t *raft, void *user_data, int term, int vote)
{
    return 0;
}

static void __raft_log(raft_server_t *raft, raft_node_t *node, void *user_data, const char *buf)
{
    fprintf(stderr, "[%d] raft log>> %s\n", raft_get_nodeid(raft), buf);
}

static int __raft_log_offer(raft_server_t *raft, void *user_data, raft_entry_t *entry, int entry_idx)
{
    return 0;
}

static int __raft_log_pop(raft_server_t *raft, void *user_data, raft_entry_t *entry, int entry_idx)
{
    return 0;
}

static int __raft_applylog(raft_server_t *raft, void *user_data, raft_entry_t *entry, int entry_idx)
{
    redis_raft_t *rr = user_data;
    execute_log_entry(rr, entry);
    return 0;
}

raft_cbs_t redis_raft_callbacks = {
    .send_requestvote = __raft_send_requestvote,
    .send_appendentries = __raft_send_appendentries,
    .persist_vote = __raft_persist_vote,
    .persist_term = __raft_persist_term,
    .log_offer = __raft_log_offer,
    .log_pop = __raft_log_pop,
    .log = __raft_log,
    .applylog = __raft_applylog,
};

/*
 * Handling of the Redis Raft context, including its own thread and
 * async I/O loop.
 */

static void redis_raft_timer(uv_timer_t *handle)
{
    redis_raft_t *rr = (redis_raft_t *) uv_handle_get_data((uv_handle_t *) handle);

    raft_periodic(rr->raft, 500);
}

static void redis_raft_thread(void *arg)
{
    redis_raft_t *rr = (redis_raft_t *) arg;

    rr->loop = RedisModule_Alloc(sizeof(uv_loop_t));
    uv_loop_init(rr->loop);

    uv_async_init(rr->loop, &rr->rqueue_sig, raft_req_handle_rqueue);
    uv_handle_set_data((uv_handle_t *) &rr->rqueue_sig, rr);

    uv_timer_init(rr->loop, &rr->ptimer);
    uv_handle_set_data((uv_handle_t *) &rr->ptimer, rr);
    uv_timer_start(&rr->ptimer, redis_raft_timer, 500, 500);

    rr->running = true;
    uv_run(rr->loop, UV_RUN_DEFAULT);
}

int redis_raft_init(RedisModuleCtx *ctx, redis_raft_t *rr, redis_raft_config_t *config)
{
    memset(rr, 0, sizeof(redis_raft_t));
    STAILQ_INIT(&rr->rqueue);
    STAILQ_INIT(&rr->cqueue);
    rr->ctx = RedisModule_GetThreadSafeContext(NULL);

    /* Initialize raft library */
    rr->raft = raft_new();
    if (!raft_add_node(rr->raft, NULL, config->id, 1)) {
        RedisModule_Log(ctx, LOGLEVEL_WARNING, "Failed to initialize raft_node");
        return REDISMODULE_ERR;
    }
    raft_set_callbacks(rr->raft, &redis_raft_callbacks, rr);

    /* Create nodes.  Connections will be established when Raft library callbacks
     * hit them.
     */
    node_config_t *nc = config->nodes;
    while (nc != NULL) {
        node_t *node = node_init(nc->id, &nc->addr);

        raft_node_t *raft_node;
        if (!(raft_node = raft_add_node(rr->raft, node, node->id, 0))) {
            RedisModule_Log(ctx, LOGLEVEL_WARNING, "Failed to add node, id %d exists", node->id);
            return REDISMODULE_ERR;
        }
        raft_node_set_udata(raft_node, node);

        nc = nc->next;
    }

    return REDISMODULE_OK;
}

int redis_raft_start(RedisModuleCtx *ctx, redis_raft_t *rr)
{
    /* Start Raft thread */
    if (uv_thread_create(&rr->thread, redis_raft_thread, rr) < 0) {
        RedisModule_Log(ctx, LOGLEVEL_WARNING, "Failed to initialize redis_raft thread");
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}


/*
 * Raft Requests, which are exchanged between the Redis main thread
 * and the Raft thread over the requests queue.
 */

void raft_req_free(raft_req_t *req)
{
    switch (req->type) {
        case RAFT_REQ_ADDNODE:
            node_addr_free(&req->r.addnode.addr);
            break;
        case RAFT_REQ_APPENDENTRIES:
            if (req->r.appendentries.msg.entries) {
                RedisModule_Free(req->r.appendentries.msg.entries);
                req->r.appendentries.msg.entries = NULL;
            }
            break;
        break;
    }
    RedisModule_Free(req);
}

raft_req_t *raft_req_init(RedisModuleCtx *ctx, enum raft_req_type type)
{
    raft_req_t *req = RedisModule_Calloc(1, sizeof(raft_req_t));
    if (ctx != NULL) {
        req->client = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
        req->ctx = RedisModule_GetThreadSafeContext(req->client);
    }
    req->type = type;
    
    return req;
}

void raft_req_submit(redis_raft_t *rr, raft_req_t *req)
{
    STAILQ_INSERT_TAIL(&rr->rqueue, req, entries);
    if (rr->running) {
        uv_async_send(&rr->rqueue_sig);
    }
}

void raft_req_handle_rqueue(uv_async_t *handle)
{
    redis_raft_t *rr = (redis_raft_t *) uv_handle_get_data((uv_handle_t *) handle);

    while (!STAILQ_EMPTY(&rr->rqueue)) {
        raft_req_t *req = STAILQ_FIRST(&rr->rqueue);
        raft_req_callbacks[req->type](rr, req);
        STAILQ_REMOVE_HEAD(&rr->rqueue, entries);
        if (!(req->flags & RAFT_REQ_PENDING_COMMIT)) {
            raft_req_free(req);
        }
    }
}


/*
 * Implementation of specific request types.
 */

static int __raft_requestvote(redis_raft_t *rr, raft_req_t *req)
{
    msg_requestvote_response_t response;

    if (raft_recv_requestvote(rr->raft,
                raft_get_node(rr->raft, req->r.requestvote.src_node_id),
                &req->r.requestvote.msg,
                &response) != 0) {
        RedisModule_ReplyWithError(req->ctx, "operation failed"); // TODO: Identify cases
        goto exit;
    }

    RedisModule_ReplyWithArray(req->ctx, 2);
    RedisModule_ReplyWithLongLong(req->ctx, response.term);
    RedisModule_ReplyWithLongLong(req->ctx, response.vote_granted);

exit:
    RedisModule_FreeThreadSafeContext(req->ctx);
    RedisModule_UnblockClient(req->client, NULL);
    req->ctx = NULL;

    return REDISMODULE_OK;
}


static int __raft_appendentries(redis_raft_t *rr, raft_req_t *req)
{
    msg_appendentries_response_t response;

    if (raft_recv_appendentries(rr->raft,
                raft_get_node(rr->raft, req->r.appendentries.src_node_id),
                &req->r.appendentries.msg,
                &response) != 0) {
        RedisModule_ReplyWithError(req->ctx, "operation failed"); // TODO: Identify cases
        goto exit;
    }

    RedisModule_ReplyWithArray(req->ctx, 4);
    RedisModule_ReplyWithLongLong(req->ctx, response.term);
    RedisModule_ReplyWithLongLong(req->ctx, response.success);
    RedisModule_ReplyWithLongLong(req->ctx, response.current_idx);
    RedisModule_ReplyWithLongLong(req->ctx, response.first_idx);

exit:
    RedisModule_FreeThreadSafeContext(req->ctx);
    RedisModule_UnblockClient(req->client, NULL);
    req->ctx = NULL;

    return REDISMODULE_OK;
}

static int __raft_addnode(redis_raft_t *rr, raft_req_t *req)
{
    node_t *node = node_init(req->r.addnode.id, &req->r.addnode.addr);

    /* Before attempting to connect, try to add the node */
    raft_node_t *raft_node;
    if (!(raft_node = raft_add_node(rr->raft, node, node->id, 0))) {
        if (req->ctx) RedisModule_ReplyWithError(req->ctx, "node id exists");
        node_free(node);
        goto exit;
    }

    /* Connect */
    node_connect(node, rr);
    if (!req->ctx) return REDISMODULE_OK;

    RedisModule_ReplyWithSimpleString(req->ctx, "OK");

exit:
    RedisModule_FreeThreadSafeContext(req->ctx);
    RedisModule_UnblockClient(req->client, NULL);
    req->ctx = NULL;
    return REDISMODULE_OK;
}

static int __raft_rediscommand(redis_raft_t *rr,raft_req_t *req)
{
    raft_node_t *leader = raft_get_current_leader_node(rr->raft);
    if (!leader) {
        RedisModule_ReplyWithError(req->ctx, "-NOLEADER");
        goto exit;
    }
    if (raft_node_get_id(leader) != raft_get_nodeid(rr->raft)) {
        node_t *l = raft_node_get_udata(leader);
        char *reply;
       
        asprintf(&reply, "LEADERIS %s:%u", l->addr.host, l->addr.port);

        RedisModule_ReplyWithError(req->ctx, reply);
        free(reply);
        goto exit;
    }

    raft_entry_t entry = {
        .id = rand(),
        .type = RAFT_LOGTYPE_NORMAL,
    };

    redis_raft_serialize(&entry.data, req->r.raft.argv, req->r.raft.argc);
    int e = raft_recv_entry(rr->raft, &entry, &req->r.raft.response);
    if (e) {
        // todo handle errors
        RedisModule_Free(entry.data.buf);
        RedisModule_ReplyWithSimpleString(req->ctx, "ERROR");
        goto exit;
    }

    // We're now waiting 
    req->flags |= RAFT_REQ_PENDING_COMMIT;
    STAILQ_INSERT_TAIL(&rr->cqueue, req, entries);

    return REDISMODULE_OK;

exit:
    RedisModule_FreeThreadSafeContext(req->ctx);
    RedisModule_UnblockClient(req->client, NULL);
    req->ctx = NULL;
    return REDISMODULE_OK;

}

static int __raft_info(redis_raft_t *rr, raft_req_t *req)
{
    size_t slen = 1024;
    char *s = RedisModule_Calloc(1, slen);

    char role[10];
    switch (raft_get_state(rr->raft)) {
        case RAFT_STATE_FOLLOWER:
            strcpy(role, "follower");
            break;
        case RAFT_STATE_LEADER:
            strcpy(role, "leader");
            break;
        case RAFT_STATE_CANDIDATE:
            strcpy(role, "candidate");
            break;
        default:
            strcpy(role, "(none)");
            break;
    }

    s = catsnprintf(s, &slen,
            "# Nodes\n"
            "node_id:%d\n"
            "role:%s\n"
            "leader_id:%d\n"
            "current_term:%d\n",
            raft_get_nodeid(rr->raft),
            role,
            raft_get_current_leader(rr->raft),
            raft_get_current_term(rr->raft));
    
    int i;
    for (i = 0; i < raft_get_num_nodes(rr->raft); i++) {
        raft_node_t *rnode = raft_get_node_from_idx(rr->raft, i);
        node_t *node = raft_node_get_udata(rnode);
        if (!node) {
            continue;
        }

        s = catsnprintf(s, &slen,
                "node%d: id=%d,addr=%s,port=%d\n",
                i, node->id, node->addr.host, node->addr.port);
    }

    s = catsnprintf(s, &slen,
            "\n# Log\n"
            "log_entries:%d\n"
            "current_index:%d\n"
            "commit_index:%d\n"
            "last_applied_index:%d\n",
            raft_get_log_count(rr->raft),
            raft_get_current_idx(rr->raft),
            raft_get_commit_idx(rr->raft),
            raft_get_last_applied_idx(rr->raft));

    RedisModule_ReplyWithSimpleString(req->ctx, s);
    RedisModule_FreeThreadSafeContext(req->ctx);
    RedisModule_UnblockClient(req->client, NULL);
    req->ctx = NULL;

    return REDISMODULE_OK;
}


raft_req_callback_t raft_req_callbacks[] = {
    NULL,
    __raft_addnode,
    __raft_appendentries,
    __raft_requestvote,
    __raft_rediscommand,
    __raft_info,
    NULL
};


