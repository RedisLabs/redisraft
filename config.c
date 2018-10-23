#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "redisraft.h"

static RRStatus parseBool(const char *value, bool *result)
{
    if (!strcmp(value, "yes")) {
        *result = true;
    } else if (!strcmp(value, "no")) {
        *result = false;
    } else {
        return RR_ERROR;
    }
    return RR_OK;
}

static RRStatus processConfigParam(const char *keyword, const char *value,
        RedisRaftConfig *target, bool on_init, char *errbuf, int errbuflen)
{
    /* Parameters we don't accept as config set */
    if (!on_init && (!strcmp(keyword, "id"))) {
        snprintf(errbuf, errbuflen-1, "'%s' only supported at load time", keyword);
        return RR_ERROR;
    }

    if (!value) {
        snprintf(errbuf, errbuflen-1, "'%s' requires a value", keyword);
        return RR_ERROR;
    }

    if (!strcmp(keyword, "id")) {
        char *errptr;
        unsigned long idval = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || !idval) {
            snprintf(errbuf, errbuflen-1, "invalid 'id' value");
            return RR_ERROR;
        }
        target->id = idval;
    } else if (!strcmp(keyword, "addr")) {
        if (!NodeAddrParse(value, strlen(value), &target->addr)) {
            snprintf(errbuf, errbuflen-1, "invalid addr '%s'", value);
            return RR_ERROR;
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
            return RR_ERROR;
        }
        target->raft_interval = val;
    } else if (!strcmp(keyword, "request_timeout")) {
        char *errptr;
        unsigned long val = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || val <= 0) {
            snprintf(errbuf, errbuflen-1, "invalid 'request_timeout' value");
            return RR_ERROR;
        }
        target->request_timeout = val;
    } else if (!strcmp(keyword, "election_timeout")) {
        char *errptr;
        unsigned long val = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || val <= 0) {
            snprintf(errbuf, errbuflen-1, "invalid 'election_timeout' value");
            return RR_ERROR;
        }
        target->election_timeout = val;
    } else if (!strcmp(keyword, "reconnect_interval")) {
        char *errptr;
        unsigned long val = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || val <= 0) {
            snprintf(errbuf, errbuflen-1, "invalid 'reconnect_interval' value");
            return RR_ERROR;
        }
        target->reconnect_interval = val;
    } else if (!strcmp(keyword, "max_log_entries")) {
        char *errptr;
        unsigned long val = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || val <= 0) {
            snprintf(errbuf, errbuflen-1, "invalid 'max_log_entries' value");
            return RR_ERROR;
        }
        target->max_log_entries = val;
    } else {
        snprintf(errbuf, errbuflen-1, "invalid parameter '%s'", keyword);
        return RR_ERROR;
    }

    return RR_OK;
}

void handleConfigSet(RedisModuleCtx *ctx, RedisRaftConfig *config, RedisModuleString **argv, int argc)
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
                errbuf + strlen(errbuf), sizeof(errbuf) - strlen(errbuf)) == RR_OK) {
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    } else {
        RedisModule_ReplyWithError(ctx, errbuf);
    }
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

void handleConfigGet(RedisModuleCtx *ctx, RedisRaftConfig *config, RedisModuleString **argv, int argc)
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
    if (stringmatch(pattern, "addr", 1)) {
        len++;
        char buf[300];
        snprintf(buf, sizeof(buf)-1, "%s:%u", config->addr.host, config->addr.port);
        replyConfigStr(ctx, "addr", buf);
    }

    RedisModule_ReplySetArrayLength(ctx, len * 2);
}

void ConfigInit(RedisModuleCtx *ctx, RedisRaftConfig *config)
{
    memset(config, 0, sizeof(RedisRaftConfig));

    config->raft_interval = REDIS_RAFT_DEFAULT_INTERVAL;
    config->request_timeout = REDIS_RAFT_DEFAULT_REQUEST_TIMEOUT;
    config->election_timeout = REDIS_RAFT_DEFAULT_ELECTION_TIMEOUT;
    config->reconnect_interval = REDIS_RAFT_DEFAULT_RECONNECT_INTERVAL;
    config->max_log_entries = REDIS_RAFT_DEFAULT_MAX_LOG_ENTRIES;
}

static char *getRedisConfig(RedisModuleCtx *ctx, const char *name)
{
    size_t len;
    const char *str;
    char *buf = NULL;
    RedisModuleCallReply *reply = NULL, *reply_name = NULL;

    if (!(reply = RedisModule_Call(ctx, "CONFIG", "cc", "GET", name))) {
        goto exit;
    }

    if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_ARRAY ||
            RedisModule_CallReplyLength(reply) < 2) {
        goto exit;
    }

    reply_name = RedisModule_CallReplyArrayElement(reply, 1);
    if (!reply_name || RedisModule_CallReplyType(reply_name) != REDISMODULE_REPLY_STRING) {
        goto exit;
    }

    str = RedisModule_CallReplyStringPtr(reply_name, &len);
    buf = RedisModule_Alloc(len + 1);
    memcpy(buf, str, len);
    buf[len] = '\0';

exit:
    if (reply_name) {
        RedisModule_FreeCallReply(reply_name);
    }
    if (reply) {
        RedisModule_FreeCallReply(reply);
    }

    return buf;
}

static RRStatus getInterfaceAddr(NodeAddr *addr)
{
    struct sockaddr *sa = NULL;
    uv_interface_address_t *ifaddr = NULL;
    int ifaddr_count, i, ret;

    if (uv_interface_addresses(&ifaddr, &ifaddr_count) < 0 || !ifaddr_count) {
        if (ifaddr) {
            uv_free_interface_addresses(ifaddr, ifaddr_count);
        }
        return RR_ERROR;
    }

    /* Try to find a non-internal one, otherwise return just the first one */
    sa = (struct sockaddr *) &ifaddr[0].address;
    for (i = 0; i < ifaddr_count; i++) {
        if (!ifaddr[i].is_internal) {
            sa = (struct sockaddr *) &ifaddr[i].address;
            break;
        }
    }

    ret = getnameinfo(sa,
            sa->sa_family == AF_INET ? sizeof(struct sockaddr_in) : sizeof(struct sockaddr_in6),
            addr->host, sizeof(addr->host), NULL, 0, NI_NUMERICHOST);
    uv_free_interface_addresses(ifaddr, ifaddr_count);

    return ret < 0 ? RR_ERROR : RR_OK;
}

RRStatus ConfigReadFromRedis(RedisRaftCtx *rr)
{
    int r;

    rr->config->rdb_filename = getRedisConfig(rr->ctx, "dbfilename");
    assert(rr->config->rdb_filename != NULL);

    /* If 'addr' was not set explicitly, try to guess it */
    if (!rr->config->addr.host[0]) {
        /* Get port from Redis */
        char *port_str = getRedisConfig(rr->ctx, "port");
        assert(port_str != NULL);

        rr->config->addr.port = strtoul(port_str, NULL, 10);
        RedisModule_Free(port_str);

        /* Get address from first non-internal interface */
        if (getInterfaceAddr(&rr->config->addr) == RR_ERROR) {
            PANIC("Failed to determine local address, please use addr=.");
            return RR_ERROR;
        }
    }

    return RR_OK;
}

RRStatus ConfigParseArgs(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, RedisRaftConfig *target)
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
                    errbuf, sizeof(errbuf)) != RR_OK) {
            RedisModule_Log(ctx, REDIS_WARNING, errbuf);
            return RR_ERROR;
        }
    }

    return RR_OK;
}

RRStatus ConfigValidate(RedisModuleCtx *ctx, RedisRaftConfig *config)
{
    if (!config->id) {
        RedisModule_Log(ctx, REDIS_WARNING, "'id' is required");
        return RR_ERROR;
    }

    return RR_OK;
}

