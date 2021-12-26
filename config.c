/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020-21 Redis Ltd.
 *
 * RedisRaft is licensed under the Redis Source Available License (RSAL).
 */

#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <assert.h>

#include "redisraft.h"

static const char *CONF_ID = "id";
static const char *CONF_ADDR = "addr";
static const char *CONF_RAFT_INTERVAL = "raft-interval";
static const char *CONF_REQUEST_TIMEOUT = "request-timeout";
static const char *CONF_ELECTION_TIMEOUT = "election-timeout";
static const char *CONF_CONNECTION_TIMEOUT = "connection-timeout";
static const char *CONF_JOIN_TIMEOUT = "join-timeout";
static const char *CONF_RAFT_RESPONSE_TIMEOUT = "raft-response-timeout";
static const char *CONF_PROXY_RESPONSE_TIMEOUT = "proxy-response-timeout";
static const char *CONF_RECONNECT_INTERVAL = "reconnect-interval";
static const char *CONF_RAFT_LOG_FILENAME = "raft-log-filename";
static const char *CONF_RAFT_LOG_MAX_CACHE_SIZE = "raft-log-max-cache-size";
static const char *CONF_RAFT_LOG_MAX_FILE_SIZE = "raft-log-max-file-size";
static const char *CONF_RAFT_LOG_FSYNC = "raft-log-fsync";
static const char *CONF_FOLLOWER_PROXY = "follower-proxy";
static const char *CONF_QUORUM_READS = "quorum-reads";
static const char *CONF_LOGLEVEL = "loglevel";
static const char *CONF_SHARDING = "sharding";
static const char *CONF_SLOT_CONFIG = "slot-config";
static const char *CONF_SHARDGROUP_UPDATE_INTERVAL = "shardgroup-update-interval";
static const char *CONF_IGNORED_COMMANDS = "ignored-commands";
static const char *CONF_EXTERNAL_SHARDING = "external-sharding";

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

static char *loglevels[] = {
    "error",
    "info",
    "verbose",
    "debug",
    NULL
};

static int parseLogLevel(const char *value)
{
    int i;
    for (i = 0; loglevels[i] != NULL; i++) {
        if (!strcasecmp(value, loglevels[i])) {
            return i;
        }
    }
    return -1;
}

static const char *getLoglevelName(int level)
{
    assert(level >= 0 && level <= LOGLEVEL_DEBUG);
    return loglevels[level];
}

int validSlotConfig(char *slot_config) {
    int ret = 0;
    char *tmp = RedisModule_Strdup(slot_config);
    char *pos = tmp;
    char *endptr;
    int val_l, val_h;
    if ((pos = strchr(tmp, ':'))) {
        *pos = '\0';
        val_l = strtoul(tmp, &endptr, 10);
        if (*endptr != 0) {
            goto exit;
        }
        val_h = strtoul(pos+1, &endptr, 10);
        if (*endptr != 0) {
            goto exit;
        }
        if (!HashSlotRangeValid(val_l, val_h)) {
            goto exit;
        }
    } else {
        val_l = val_h = strtoul(tmp, &endptr, 10);
        if (*endptr != 0 || !HashSlotValid(val_l)) {
            goto exit;
        }
    }

    ret = 1;

exit:
    RedisModule_Free(tmp);
    return ret;
}

static RRStatus processConfigParam(const char *keyword, const char *value,
        RedisRaftConfig *target, bool on_init, char *errbuf, int errbuflen)
{
    /* Parameters we don't accept as config set */
    if (!on_init && (!strcmp(keyword, CONF_ID) ||
                !strcmp(keyword, CONF_RAFT_LOG_FILENAME) ||
                !strcmp(keyword, CONF_SLOT_CONFIG) ||
                !strcmp(keyword, CONF_EXTERNAL_SHARDING))) {
        snprintf(errbuf, errbuflen-1, "'%s' only supported at load time", keyword);
        return RR_ERROR;
    }

    if (!value) {
        snprintf(errbuf, errbuflen-1, "'%s' requires a value", keyword);
        return RR_ERROR;
    }

    if (!strcmp(keyword, CONF_ID)) {
        char *errptr;
        unsigned long idval = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || !idval || idval > INT32_MAX)
            goto invalid_value;
        target->id = (raft_node_id_t) idval;
    } else if (!strcmp(keyword, CONF_ADDR)) {
        if (!NodeAddrParse(value, strlen(value), &target->addr))
            goto invalid_value;
    } else if (!strcmp(keyword, CONF_RAFT_LOG_FILENAME)) {
        if (target->raft_log_filename) {
            RedisModule_Free(target->raft_log_filename);
        }
        target->raft_log_filename = RedisModule_Strdup(value);
    } else if (!strcmp(keyword, CONF_RAFT_INTERVAL)) {
        char *errptr;
        unsigned long val = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || !val)
            goto invalid_value;
        target->raft_interval = (int)val;
    } else if (!strcmp(keyword, CONF_REQUEST_TIMEOUT)) {
        char *errptr;
        unsigned long val = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || val <= 0)
            goto invalid_value;
        target->request_timeout = (int)val;
    } else if (!strcmp(keyword, CONF_ELECTION_TIMEOUT)) {
        char *errptr;
        unsigned long val = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || val <= 0)
            goto invalid_value;
        target->election_timeout = (int)val;
    } else if (!strcmp(keyword, CONF_CONNECTION_TIMEOUT)) {
        char *errptr;
        unsigned long val = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || val <= 0)
            goto invalid_value;
        target->connection_timeout = (int)val;
    } else if (!strcmp(keyword, CONF_JOIN_TIMEOUT)) {
        char *errptr;
        unsigned long val = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || val <= 0)
            goto invalid_value;
        target->join_timeout = (int)val;
    } else if (!strcmp(keyword, CONF_RAFT_RESPONSE_TIMEOUT)) {
        char *errptr;
        unsigned long val = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || val <= 0)
            goto invalid_value;
        target->raft_response_timeout = (int)val;
    } else if (!strcmp(keyword, CONF_PROXY_RESPONSE_TIMEOUT)) {
        char *errptr;
        unsigned long val = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || val <= 0)
            goto invalid_value;
        target->proxy_response_timeout = (int)val;
    } else if (!strcmp(keyword, CONF_RECONNECT_INTERVAL)) {
        char *errptr;
        unsigned long val = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || val <= 0)
            goto invalid_value;
        target->reconnect_interval = (int)val;
    } else if (!strcmp(keyword, CONF_RAFT_LOG_MAX_CACHE_SIZE)) {
        unsigned long val;
        if (parseMemorySize(value, &val) != RR_OK)
            goto invalid_value;
        target->raft_log_max_cache_size = (int)val;
    } else if (!strcmp(keyword, CONF_RAFT_LOG_MAX_FILE_SIZE)) {
        unsigned long val;
        if (parseMemorySize(value, &val) != RR_OK)
            goto invalid_value;
        target->raft_log_max_file_size = (int)val;
    } else if (!strcmp(keyword, CONF_RAFT_LOG_FSYNC)) {
        bool val;
        if (parseBool(value, &val) != RR_OK)
            goto invalid_value;
        target->raft_log_fsync = val;
    } else if (!strcmp(keyword, CONF_FOLLOWER_PROXY)) {
        bool val;
        if (parseBool(value, &val) != RR_OK)
            goto invalid_value;
        target->follower_proxy = val;
    } else if (!strcmp(keyword, CONF_QUORUM_READS)) {
        bool val;
        if (parseBool(value, &val) != RR_OK)
            goto invalid_value;
        target->quorum_reads = val;
    } else if (!strcmp(keyword, CONF_LOGLEVEL)) {
        int loglevel = parseLogLevel(value);
        if (loglevel < 0) {
            snprintf(errbuf, errbuflen-1,
                     "invalid '%s', must be 'error', 'info', 'verbose' or 'debug'", keyword);
            return RR_ERROR;
        }
        redis_raft_loglevel = loglevel;
    } else if (!strcmp(keyword, CONF_SHARDING)) {
        bool val;
        if (parseBool(value, &val) != RR_OK)
            goto invalid_value;
        target->sharding = val;
    } else if (!strcmp(keyword, CONF_EXTERNAL_SHARDING)) {
        bool val;
        if (parseBool(value, &val) != RR_OK)
            goto invalid_value;
        target->external_sharding = val;
    } else if (!strcmp(keyword, CONF_SLOT_CONFIG)) {
        target->slot_config = RedisModule_Strdup(value);
        if (!validSlotConfig(target->slot_config)) {
            snprintf(errbuf, errbuflen-1, "invalid 'slot_config' value");
            return RR_ERROR;
        }
    } else if (!strcmp(keyword, CONF_SHARDGROUP_UPDATE_INTERVAL)) {
        char *errptr;
        unsigned long val = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || val < 0)
            goto invalid_value;
        target->shardgroup_update_interval = (int) val;
    } else if (!strcmp(keyword, CONF_IGNORED_COMMANDS)) {
        if (target->ignored_commands) {
            RedisModule_Free(target->ignored_commands);
        }
        target->ignored_commands = RedisModule_Strdup(value);
    } else {
        snprintf(errbuf, errbuflen-1, "invalid parameter '%s'", keyword);
        return RR_ERROR;
    }

    return RR_OK;

invalid_value:
    snprintf(errbuf, errbuflen-1, "invalid '%s' value", keyword);
    return RR_ERROR;
}

void handleConfigSet(RedisRaftCtx *rr, RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if (argc != 4) {
        RedisModule_WrongArity(ctx);
        return;
    }

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
    if (processConfigParam(keybuf, valuebuf, rr->config, false,
                errbuf + strlen(errbuf), (int)(sizeof(errbuf) - strlen(errbuf))) == RR_OK) {
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

static void replyConfigMemSize(RedisModuleCtx *ctx, const char *name, unsigned long val)
{
    char str[64];
    formatExactMemorySize(val, str, sizeof(str));

    RedisModule_ReplyWithStringBuffer(ctx, name, strlen(name));
    RedisModule_ReplyWithStringBuffer(ctx, str, strlen(str));
}


static void replyConfigBool(RedisModuleCtx *ctx, const char *name, bool val)
{
    replyConfigStr(ctx, name, val ? "yes" : "no");
}

void handleConfigGet(RedisModuleCtx *ctx, RedisRaftConfig *config, RedisModuleString **argv, int argc)
{
    if (argc != 3) {
        RedisModule_WrongArity(ctx);
        return;
    }

    int len = 0;
    size_t pattern_len;
    const char *pattern = RedisModule_StringPtrLen(argv[2], &pattern_len);

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    if (stringmatch(pattern, CONF_ID, 1)) {
        len++;
        replyConfigInt(ctx, CONF_ID, config->id);
    }
    if (stringmatch(pattern, CONF_RAFT_LOG_FILENAME, 1)) {
        len++;
        replyConfigStr(ctx, CONF_RAFT_LOG_FILENAME, config->raft_log_filename);
    }
    if (stringmatch(pattern, CONF_RAFT_INTERVAL, 1)) {
        len++;
        replyConfigInt(ctx, CONF_RAFT_INTERVAL, config->raft_interval);
    }
    if (stringmatch(pattern, CONF_REQUEST_TIMEOUT, 1)) {
        len++;
        replyConfigInt(ctx, CONF_REQUEST_TIMEOUT, config->request_timeout);
    }
    if (stringmatch(pattern, CONF_ELECTION_TIMEOUT, 1)) {
        len++;
        replyConfigInt(ctx, CONF_ELECTION_TIMEOUT, config->election_timeout);
    }
    if (stringmatch(pattern, CONF_CONNECTION_TIMEOUT, 1)) {
        len++;
        replyConfigInt(ctx, CONF_CONNECTION_TIMEOUT, config->connection_timeout);
    }
    if (stringmatch(pattern, CONF_JOIN_TIMEOUT, 1)) {
        len++;
        replyConfigInt(ctx, CONF_JOIN_TIMEOUT, config->join_timeout);
    }
    if (stringmatch(pattern, CONF_RAFT_RESPONSE_TIMEOUT, 1)) {
        len++;
        replyConfigInt(ctx, CONF_RAFT_RESPONSE_TIMEOUT, config->raft_response_timeout);
    }
    if (stringmatch(pattern, CONF_PROXY_RESPONSE_TIMEOUT, 1)) {
        len++;
        replyConfigInt(ctx, CONF_PROXY_RESPONSE_TIMEOUT, config->proxy_response_timeout);
    }
    if (stringmatch(pattern, CONF_RECONNECT_INTERVAL, 1)) {
        len++;
        replyConfigInt(ctx, CONF_RECONNECT_INTERVAL, config->reconnect_interval);
    }
    if (stringmatch(pattern, CONF_RAFT_LOG_MAX_CACHE_SIZE, 1)) {
        len++;
        replyConfigMemSize(ctx, CONF_RAFT_LOG_MAX_CACHE_SIZE, config->raft_log_max_cache_size);
    }
    if (stringmatch(pattern, CONF_RAFT_LOG_MAX_FILE_SIZE, 1)) {
        len++;
        replyConfigMemSize(ctx, CONF_RAFT_LOG_MAX_FILE_SIZE, config->raft_log_max_file_size);
    }
    if (stringmatch(pattern, CONF_RAFT_LOG_FSYNC, 1)) {
        len++;
        replyConfigBool(ctx, CONF_RAFT_LOG_FSYNC, config->raft_log_fsync);
    }
    if (stringmatch(pattern, CONF_FOLLOWER_PROXY, 1)) {
        len++;
        replyConfigBool(ctx, CONF_FOLLOWER_PROXY, config->follower_proxy);
    }
    if (stringmatch(pattern, CONF_QUORUM_READS, 1)) {
        len++;
        replyConfigBool(ctx, CONF_QUORUM_READS, config->quorum_reads);
    }
    if (stringmatch(pattern, CONF_ADDR, 1)) {
        len++;
        char buf[300];
        snprintf(buf, sizeof(buf)-1, "%s:%u", config->addr.host, config->addr.port);
        replyConfigStr(ctx, CONF_ADDR, buf);
    }
    if (stringmatch(pattern, CONF_LOGLEVEL, 1)) {
        len++;
        replyConfigStr(ctx, CONF_LOGLEVEL, getLoglevelName(redis_raft_loglevel));
    }
    if (stringmatch(pattern, CONF_SHARDING, 1)) {
        len++;
        replyConfigBool(ctx, CONF_SHARDING, config->sharding);
    }
    if (stringmatch(pattern, CONF_EXTERNAL_SHARDING, 1)) {
        len++;
        replyConfigBool(ctx, CONF_EXTERNAL_SHARDING, config->external_sharding);
    }
    if (stringmatch(pattern, CONF_SLOT_CONFIG, 1)) {
        len++;
        replyConfigStr(ctx, CONF_SLOT_CONFIG, config->slot_config);
    }
    if (stringmatch(pattern, CONF_SHARDGROUP_UPDATE_INTERVAL, 1)) {
        len++;
        replyConfigInt(ctx, CONF_SHARDGROUP_UPDATE_INTERVAL, config->shardgroup_update_interval);
    }
    if (stringmatch(pattern, CONF_IGNORED_COMMANDS, 1)) {
        len++;
        replyConfigStr(ctx, CONF_IGNORED_COMMANDS, config->ignored_commands);
    }
    RedisModule_ReplySetArrayLength(ctx, len * 2);
}

void ConfigInit(RedisModuleCtx *ctx, RedisRaftConfig *config)
{
    UNUSED(ctx);

    memset(config, 0, sizeof(RedisRaftConfig));

    config->raft_log_filename = RedisModule_Strdup(REDIS_RAFT_DEFAULT_LOG_FILENAME);
    config->raft_interval = REDIS_RAFT_DEFAULT_INTERVAL;
    config->request_timeout = REDIS_RAFT_DEFAULT_REQUEST_TIMEOUT;
    config->election_timeout = REDIS_RAFT_DEFAULT_ELECTION_TIMEOUT;
    config->connection_timeout = REDIS_RAFT_DEFAULT_CONNECTION_TIMEOUT;
    config->join_timeout = REDIS_RAFT_DEFAULT_JOIN_TIMEOUT;
    config->reconnect_interval = REDIS_RAFT_DEFAULT_RECONNECT_INTERVAL;
    config->raft_response_timeout = REDIS_RAFT_DEFAULT_RAFT_RESPONSE_TIMEOUT;
    config->proxy_response_timeout = REDIS_RAFT_DEFAULT_PROXY_RESPONSE_TIMEOUT;
    config->raft_log_max_cache_size = REDIS_RAFT_DEFAULT_LOG_MAX_CACHE_SIZE;
    config->raft_log_max_file_size = REDIS_RAFT_DEFAULT_LOG_MAX_FILE_SIZE;
    config->raft_log_fsync = true;
    config->quorum_reads = true;
    config->sharding = false;
    config->external_sharding = false;
    config->slot_config = "0:16383",
    config->shardgroup_update_interval = REDIS_RAFT_DEFAULT_SHARDGROUP_UPDATE_INTERVAL;
}

static RRStatus setRedisConfig(RedisModuleCtx *ctx, const char *param, const char *value)
{
    size_t len;
    const char *str;
    RedisModuleCallReply *reply = NULL;
    RRStatus ret = RR_OK;

    if (!(reply = RedisModule_Call(ctx, "CONFIG", "ccc", "SET", param, value))) {
        ret = RR_ERROR;
        goto exit;
    }

    if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_STRING) {
        ret = RR_ERROR;
        goto exit;
    }

    str = RedisModule_CallReplyStringPtr(reply, &len);
    if (len != 2 || memcmp(str, "OK", 2) != 0) {
        ret = RR_ERROR;
        goto exit;
    }

exit:
    if (reply) {
        RedisModule_FreeCallReply(reply);
    }

    return ret;
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
        }
    }

    return RR_OK;
}

RRStatus ConfigureRedis(RedisModuleCtx *ctx)
{
    if (setRedisConfig(ctx, "save", "") != RR_OK) {
        return RR_ERROR;
    }

    return RR_OK;
}

RRStatus ConfigParseArgs(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, RedisRaftConfig *target)
{
    int i;

    for (i = 0; i < argc; i++) {
        size_t kwlen;
        const char *kw = RedisModule_StringPtrLen(argv[i], &kwlen);

        if (i + 1 >= argc) {
            RedisModule_Log(ctx, REDIS_WARNING, "No argument specified for keyword '%.*s'",
                (int) kwlen, kw);
            return RR_ERROR;
        }

        size_t vallen;
        const char *val = RedisModule_StringPtrLen(argv[i + 1], &vallen);
        i++;

        char errbuf[256];
        if (processConfigParam(kw, val, target, true,
                    errbuf, sizeof(errbuf)) != RR_OK) {
            RedisModule_Log(ctx, REDIS_WARNING, "%s", errbuf);
            return RR_ERROR;
        }
    }

    return RR_OK;
}

