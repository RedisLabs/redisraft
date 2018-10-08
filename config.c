#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "redisraft.h"

static int parseBool(const char *value, bool *result)
{
    if (!strcmp(value, "yes")) {
        *result = true;
    } else if (!strcmp(value, "no")) {
        *result = false;
    } else {
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}

int processConfigParam(const char *keyword, const char *value,
        RedisRaftConfig *target, bool on_init, char *errbuf, int errbuflen)
{
    /* Parameters we don't accept as config set */
    if (!on_init && (!strcmp(keyword, "id") || !strcmp(keyword, "join") ||
                !strcmp(keyword, "addr") || !strcmp(keyword, "init") ||
                !strcmp(keyword, "raftlog"))) {
        snprintf(errbuf, errbuflen-1, "'%s' only supported at load time", keyword);
        return REDISMODULE_ERR;
    }

    /* Process flags without values */
    if (!strcmp(keyword, "init")) {
        target->init = true;
        return REDISMODULE_OK;
    }

    if (!value) {
        snprintf(errbuf, errbuflen-1, "'%s' requires a value", keyword);
        return REDISMODULE_ERR;
    }

    if (!strcmp(keyword, "id")) {
        char *errptr;
        unsigned long idval = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || !idval) {
            snprintf(errbuf, errbuflen-1, "invalid 'id' value");
            return REDISMODULE_ERR;
        }
        target->id = idval;
    } else if (!strcmp(keyword, "join")) {
        NodeAddrListElement *n = RedisModule_Alloc(sizeof(NodeAddrListElement));
        if (!NodeAddrParse(value, strlen(value), &n->addr)) {
            RedisModule_Free(n);
            snprintf(errbuf, errbuflen-1, "invalid join address '%s'", value);
            return REDISMODULE_ERR;
        }
        n->next = target->join;
        target->join = n;
    } else if (!strcmp(keyword, "addr")) {
        if (!NodeAddrParse(value, strlen(value), &target->addr)) {
            snprintf(errbuf, errbuflen-1, "invalid addr '%s'", value);
            return REDISMODULE_ERR;
        }
    } else if (!strcmp(keyword, "raftlog")) {
        if (target->raftlog) {
            RedisModule_Free(target->raftlog);
        }
        target->raftlog = RedisModule_Strdup(value);
    } else if (!strcmp(keyword, "raft_interval")) {
        char *errptr;
        unsigned long val = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || !val) {
            snprintf(errbuf, errbuflen-1, "invalid 'raft_interval' value");
            return REDISMODULE_ERR;
        }
        target->raft_interval = val;
    } else if (!strcmp(keyword, "request_timeout")) {
        char *errptr;
        unsigned long val = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || val <= 0) {
            snprintf(errbuf, errbuflen-1, "invalid 'request_timeout' value");
            return REDISMODULE_ERR;
        }
        target->request_timeout = val;
    } else if (!strcmp(keyword, "election_timeout")) {
        char *errptr;
        unsigned long val = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || val <= 0) {
            snprintf(errbuf, errbuflen-1, "invalid 'election_timeout' value");
            return REDISMODULE_ERR;
        }
        target->election_timeout = val;
    } else if (!strcmp(keyword, "reconnect_interval")) {
        char *errptr;
        unsigned long val = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || val <= 0) {
            snprintf(errbuf, errbuflen-1, "invalid 'reconnect_interval' value");
            return REDISMODULE_ERR;
        }
        target->reconnect_interval = val;
    } else if (!strcmp(keyword, "max_log_entries")) {
        char *errptr;
        unsigned long val = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || val <= 0) {
            snprintf(errbuf, errbuflen-1, "invalid 'max_log_entries' value");
            return REDISMODULE_ERR;
        }
        target->max_log_entries = val;
    } else {
        snprintf(errbuf, errbuflen-1, "invalid parameter '%s'", keyword);
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}

int handleConfigSet(RedisModuleCtx *ctx, RedisRaftConfig *config, RedisModuleString **argv, int argc)
{
    size_t key_len;
    const char *key = RedisModule_StringPtrLen(argv[2], &key_len);
    char keybuf[key_len + 1];
    memcpy(keybuf, key, key_len);
    keybuf[key_len] = '\0';

    size_t value_len;
    const char *value = RedisModule_StringPtrLen(argv[3], &value_len);
    char valuebuf[value_len + 1];
    memcpy(valuebuf, value, value_len);
    valuebuf[value_len] = '\0';

    char errbuf[256] = "ERR ";
    if (processConfigParam(keybuf, valuebuf, config, false,
                errbuf + strlen(errbuf), sizeof(errbuf) - strlen(errbuf)) == REDISMODULE_OK) {
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    } else {
        RedisModule_ReplyWithError(ctx, errbuf);
    }

    return REDISMODULE_OK;
}

static void replyConfigStr(RedisModuleCtx *ctx, const char *name, const char *str)
{
    RedisModule_ReplyWithStringBuffer(ctx, name, strlen(name));
    RedisModule_ReplyWithStringBuffer(ctx, str, strlen(str));
}

static void replyConfigInt(RedisModuleCtx *ctx, const char *name, int val)
{
    char str[64];
    snprintf(str, sizeof(str) - 1, "%d", val);

    RedisModule_ReplyWithStringBuffer(ctx, name, strlen(name));
    RedisModule_ReplyWithStringBuffer(ctx, str, strlen(str));
}

int handleConfigGet(RedisModuleCtx *ctx, RedisRaftConfig *config, RedisModuleString **argv, int argc)
{
    int len = 0;
    size_t pattern_len;
    const char *pattern = RedisModule_StringPtrLen(argv[2], &pattern_len);

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    if (stringmatch(pattern, "raftlog", 1)) {
        len++;
        replyConfigStr(ctx, "raftlog", config->raftlog ? config->raftlog : "");
    }
    if (stringmatch(pattern, "raft_interval", 1)) {
        len++;
        replyConfigInt(ctx, "raft_interval", config->raft_interval);
    }
    if (stringmatch(pattern, "request_timeout", 1)) {
        len++;
        replyConfigInt(ctx, "request_timeout", config->request_timeout);
    }
    if (stringmatch(pattern, "election_timeout", 1)) {
        len++;
        replyConfigInt(ctx, "election_timeout", config->election_timeout);
    }
    if (stringmatch(pattern, "reconnect_interval", 1)) {
        len++;
        replyConfigInt(ctx, "reconnect_interval", config->reconnect_interval);
    }
    if (stringmatch(pattern, "max_log_entries", 1)) {
        len++;
        replyConfigInt(ctx, "max_log_entries", config->max_log_entries);
    }

    RedisModule_ReplySetArrayLength(ctx, len * 2);
    return REDISMODULE_OK;
}

int ConfigInit(RedisModuleCtx *ctx, RedisRaftConfig *config)
{
    memset(config, 0, sizeof(RedisRaftConfig));

    config->raft_interval = REDIS_RAFT_DEFAULT_INTERVAL;
    config->request_timeout = REDIS_RAFT_DEFAULT_REQUEST_TIMEOUT;
    config->election_timeout = REDIS_RAFT_DEFAULT_ELECTION_TIMEOUT;
    config->reconnect_interval = REDIS_RAFT_DEFAULT_RECONNECT_INTERVAL;
    config->max_log_entries = REDIS_RAFT_DEFAULT_MAX_LOG_ENTRIES;
}

int ConfigReadFromRedis(RedisRaftCtx *rr)
{
    /* Query RDB filename */
    size_t len;
    const char *str;
    RedisModuleCallReply *reply = RedisModule_Call(rr->ctx, "CONFIG", "cc", "GET", "dbfilename");
    assert(reply != NULL);
    assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY);

    RedisModuleCallReply *reply_name = RedisModule_CallReplyArrayElement(reply, 1);
    assert(RedisModule_CallReplyType(reply_name) == REDISMODULE_REPLY_STRING);

    str = RedisModule_CallReplyStringPtr(reply_name, &len);
    assert(len>1);
    rr->config->rdb_filename = RedisModule_Alloc(len+1);
    memcpy(rr->config->rdb_filename, str, len);
    rr->config->rdb_filename[len] = '\0';
    RedisModule_FreeCallReply(reply_name);
    RedisModule_FreeCallReply(reply);

    return REDISMODULE_OK;
}

int ConfigParseArgs(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, RedisRaftConfig *target)
{
    int i;

    for (i = 0; i < argc; i++) {
        size_t arglen;
        const char *arg = RedisModule_StringPtrLen(argv[i], &arglen);

        char argbuf[arglen + 1];
        memcpy(argbuf, arg, arglen);
        argbuf[arglen] = '\0';

        const char *val = NULL;
        char *eq = strchr(argbuf, '=');
        if (eq != NULL) {
            *eq = '\0';
            val = eq + 1;
        }

        char errbuf[256];
        if (processConfigParam(argbuf, val, target, true,
                    errbuf, sizeof(errbuf)) != REDISMODULE_OK) {
            RedisModule_Log(ctx, REDIS_WARNING, errbuf);
            return REDISMODULE_ERR;
        }
    }

    return REDISMODULE_OK;
}

int ConfigValidate(RedisModuleCtx *ctx, RedisRaftConfig *config)
{
    if (config->init && config->join) {
        RedisModule_Log(ctx, REDIS_WARNING, "'init' and 'join' are mutually exclusive");
        return REDISMODULE_ERR;
    }
    if (config->init) {
        if (!config->addr.port) {
            RedisModule_Log(ctx, REDIS_WARNING, "'init' specified without an 'addr'");
            return REDISMODULE_ERR;
        }
        if (!config->id) {
            RedisModule_Log(ctx, REDIS_WARNING, "'init' requires an 'id'");
            return REDISMODULE_ERR;
        }
    }
    if (config->join) {
        if (!config->addr.port) {
            RedisModule_Log(ctx, REDIS_WARNING, "'join' specified without an 'addr'");
            return REDISMODULE_ERR;
        }
    }

    return REDISMODULE_OK;
}

