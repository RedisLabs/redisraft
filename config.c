/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020 Redis Labs
 *
 * RedisRaft is dual licensed under the GNU Affero General Public License version 3
 * (AGPLv3) or the Redis Source Available License (RSAL).
 */

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
        if (!strcmp(value, loglevels[i])) {
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

static RRStatus processConfigParam(const char *keyword, const char *value,
        RedisRaftConfig *target, bool on_init, char *errbuf, int errbuflen)
{
    /* Parameters we don't accept as config set */
    if (!on_init && (!strcmp(keyword, "id") ||
                !strcmp(keyword, "raft-log-filename") ||
                !strcmp(keyword, "cluster-start-hslot") ||
                !strcmp(keyword, "cluster-end-hslot"))) {
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
    } else if (!strcmp(keyword, "raft-log-filename")) {
        if (target->raft_log_filename) {
            RedisModule_Free(target->raft_log_filename);
        }
        target->raft_log_filename = RedisModule_Strdup(value);
    } else if (!strcmp(keyword, "raft-interval")) {
        char *errptr;
        unsigned long val = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || !val) {
            snprintf(errbuf, errbuflen-1, "invalid 'raft-interval' value");
            return RR_ERROR;
        }
        target->raft_interval = (int)val;
    } else if (!strcmp(keyword, "request-timeout")) {
        char *errptr;
        unsigned long val = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || val <= 0) {
            snprintf(errbuf, errbuflen-1, "invalid 'request-timeout' value");
            return RR_ERROR;
        }
        target->request_timeout = (int)val;
    } else if (!strcmp(keyword, "election-timeout")) {
        char *errptr;
        unsigned long val = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || val <= 0) {
            snprintf(errbuf, errbuflen-1, "invalid 'election-timeout' value");
            return RR_ERROR;
        }
        target->election_timeout = (int)val;
    } else if (!strcmp(keyword, "raft-response-timeout")) {
        char *errptr;
        unsigned long val = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || val <= 0) {
            snprintf(errbuf, errbuflen-1, "invalid 'raft-response-timeout' value");
            return RR_ERROR;
        }
        target->raft_response_timeout = (int)val;
    } else if (!strcmp(keyword, "proxy-response-timeout")) {
        char *errptr;
        unsigned long val = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || val <= 0) {
            snprintf(errbuf, errbuflen-1, "invalid 'proxy-response-timeout' value");
            return RR_ERROR;
        }
        target->proxy_response_timeout = (int)val;
    } else if (!strcmp(keyword, "reconnect-interval")) {
        char *errptr;
        unsigned long val = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || val <= 0) {
            snprintf(errbuf, errbuflen-1, "invalid 'reconnect-interval' value");
            return RR_ERROR;
        }
        target->reconnect_interval = (int)val;
    } else if (!strcmp(keyword, "raft-log-max-cache-size")) {
        unsigned long val;
        if (parseMemorySize(value, &val) != RR_OK) {
            snprintf(errbuf, errbuflen-1, "invalid 'raft-log-max-cache-size' value");
            return RR_ERROR;
        }
        target->raft_log_max_cache_size = (int)val;
    } else if (!strcmp(keyword, "raft-log-max-file-size")) {
        unsigned long val;
        if (parseMemorySize(value, &val) != RR_OK) {
            snprintf(errbuf, errbuflen-1, "invalid 'raft-log-max-file-size' value");
            return RR_ERROR;
        }
        target->raft_log_max_file_size = (int)val;
    } else if (!strcmp(keyword, "raft-log-fsync")) {
        bool val;
        if (parseBool(value, &val) != RR_OK) {
            snprintf(errbuf, errbuflen-1, "invalid 'raft-log-fsync' value");
            return RR_ERROR;
        }
        target->raft_log_fsync = val;
    } else if (!strcmp(keyword, "follower-proxy")) {
        bool val;
        if (parseBool(value, &val) != RR_OK) {
            snprintf(errbuf, errbuflen-1, "invalid 'follower-proxy' value");
            return RR_ERROR;
        }
        target->follower_proxy = val;
    } else if (!strcmp(keyword, "quorum-reads")) {
        bool val;
        if (parseBool(value, &val) != RR_OK) {
            snprintf(errbuf, errbuflen-1, "invalid 'quorum-reads' value");
            return RR_ERROR;
        }
        target->quorum_reads = val;
    } else if (!strcmp(keyword, "raftize-all-commands")) {
        bool val;
        if (parseBool(value, &val) != RR_OK) {
            snprintf(errbuf, errbuflen-1, "invalid 'raftize-all-commands' value");
            return RR_ERROR;
        }
        target->raftize_all_commands = val;
    } else if (!strcmp(keyword, "loglevel")) {
        int loglevel = parseLogLevel(value);
        if (loglevel < 0) {
            snprintf(errbuf, errbuflen-1, "invalid 'loglevel', must be 'error', 'info', 'verbose', 'debug' or 'trace'");
            return RR_ERROR;
        }
        redis_raft_loglevel = loglevel;
    } else if (!strcmp(keyword, "cluster-mode")) {
        bool val;
        if (parseBool(value, &val) != RR_OK) {
            snprintf(errbuf, errbuflen-1, "invalid 'cluster-mode' value");
            return RR_ERROR;
        }
        target->cluster_mode = val;
    } else if (!strcmp(keyword, "cluster-start-hslot")) {
        char *errptr;
        unsigned long val = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || !REDIS_RAFT_VALID_HASH_SLOT(val)) {
            snprintf(errbuf, errbuflen-1, "invalid 'cluster-start-hslot' value");
            return RR_ERROR;
        }
        target->cluster_start_hslot = (int)val;
    } else if (!strcmp(keyword, "cluster-end-hslot")) {
        char *errptr;
        unsigned long val = strtoul(value, &errptr, 10);
        if (*errptr != '\0' || !REDIS_RAFT_VALID_HASH_SLOT(val)) {
            snprintf(errbuf, errbuflen-1, "invalid 'cluster-end-hslot' value");
            return RR_ERROR;
        }
        target->cluster_end_hslot = (int)val;
    } else {
        snprintf(errbuf, errbuflen-1, "invalid parameter '%s'", keyword);
        return RR_ERROR;
    }

    return RR_OK;
}

void handleConfigSet(RedisRaftCtx *rr, RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
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

    RedisRaftConfig old_config = *rr->config;

    char errbuf[256] = "ERR ";
    if (processConfigParam(keybuf, valuebuf, rr->config, false,
                errbuf + strlen(errbuf), (int)(sizeof(errbuf) - strlen(errbuf))) == RR_OK) {

        /* Special cases -- when configuration needs to be actively applied */
        if (old_config.raftize_all_commands != rr->config->raftize_all_commands) {
            if (setRaftizeMode(rr, ctx, rr->config->raftize_all_commands) != RR_OK) {
                RedisModule_ReplyWithError(ctx, "ERR not supported in this Redis version.");
                rr->config->raftize_all_commands = old_config.raftize_all_commands;
                return;
            }
        }

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
    int len = 0;
    size_t pattern_len;
    const char *pattern = RedisModule_StringPtrLen(argv[2], &pattern_len);

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    if (stringmatch(pattern, "id", 1)) {
        len++;
        replyConfigInt(ctx, "id", config->id);
    }
    if (stringmatch(pattern, "raft-log-filename", 1)) {
        len++;
        replyConfigStr(ctx, "raft-log-filename", config->raft_log_filename);
    }
    if (stringmatch(pattern, "raft-interval", 1)) {
        len++;
        replyConfigInt(ctx, "raft-interval", config->raft_interval);
    }
    if (stringmatch(pattern, "request-timeout", 1)) {
        len++;
        replyConfigInt(ctx, "request-timeout", config->request_timeout);
    }
    if (stringmatch(pattern, "election-timeout", 1)) {
        len++;
        replyConfigInt(ctx, "election-timeout", config->election_timeout);
    }
    if (stringmatch(pattern, "raft-response-timeout", 1)) {
        len++;
        replyConfigInt(ctx, "raft-response-timeout", config->raft_response_timeout);
    }
    if (stringmatch(pattern, "proxy-response-timeout", 1)) {
        len++;
        replyConfigInt(ctx, "proxy-response-timeout", config->proxy_response_timeout);
    }
    if (stringmatch(pattern, "reconnect-interval", 1)) {
        len++;
        replyConfigInt(ctx, "reconnect-interval", config->reconnect_interval);
    }
    if (stringmatch(pattern, "raft-log-max-cache-size", 1)) {
        len++;
        replyConfigMemSize(ctx, "raft-log-max-cache-size", config->raft_log_max_cache_size);
    }
    if (stringmatch(pattern, "raft-log-max-file-size", 1)) {
        len++;
        replyConfigMemSize(ctx, "raft-log-max-file-size", config->raft_log_max_file_size);
    }
    if (stringmatch(pattern, "raft-log-fsync", 1)) {
        len++;
        replyConfigBool(ctx, "raft-log-fsync", config->raft_log_fsync);
    }
    if (stringmatch(pattern, "follower-proxy", 1)) {
        len++;
        replyConfigBool(ctx, "follower-proxy", config->follower_proxy);
    }
    if (stringmatch(pattern, "quorum-reads", 1)) {
        len++;
        replyConfigBool(ctx, "quorum-reads", config->quorum_reads);
    }
    if (stringmatch(pattern, "raftize-all-commands", 1)) {
        len++;
        replyConfigBool(ctx, "raftize-all-commands", config->raftize_all_commands);
    }
    if (stringmatch(pattern, "addr", 1)) {
        len++;
        char buf[300];
        snprintf(buf, sizeof(buf)-1, "%s:%u", config->addr.host, config->addr.port);
        replyConfigStr(ctx, "addr", buf);
    }
    if (stringmatch(pattern, "loglevel", 1)) {
        len++;
        replyConfigStr(ctx, "loglevel", getLoglevelName(redis_raft_loglevel));
    }
    if (stringmatch(pattern, "cluster-mode", 1)) {
        len++;
        replyConfigBool(ctx, "cluster-mode", config->cluster_mode);
    }
    if (stringmatch(pattern, "cluster-start-hslot", 1)) {
        len++;
        replyConfigInt(ctx, "cluster-start-hslot", config->cluster_start_hslot);
    }
    if (stringmatch(pattern, "cluster-end-hslot", 1)) {
        len++;
        replyConfigInt(ctx, "cluster-end-hslot", config->cluster_end_hslot);
    }
    RedisModule_ReplySetArrayLength(ctx, len * 2);
}

void ConfigInit(RedisModuleCtx *ctx, RedisRaftConfig *config)
{
    memset(config, 0, sizeof(RedisRaftConfig));

    config->raft_log_filename = RedisModule_Strdup(REDIS_RAFT_DEFAULT_LOG_FILENAME);
    config->raft_interval = REDIS_RAFT_DEFAULT_INTERVAL;
    config->request_timeout = REDIS_RAFT_DEFAULT_REQUEST_TIMEOUT;
    config->election_timeout = REDIS_RAFT_DEFAULT_ELECTION_TIMEOUT;
    config->reconnect_interval = REDIS_RAFT_DEFAULT_RECONNECT_INTERVAL;
    config->raft_response_timeout = REDIS_RAFT_DEFAULT_RAFT_RESPONSE_TIMEOUT;
    config->proxy_response_timeout = REDIS_RAFT_DEFAULT_PROXY_RESPONSE_TIMEOUT;
    config->raft_log_max_cache_size = REDIS_RAFT_DEFAULT_LOG_MAX_CACHE_SIZE;
    config->raft_log_max_file_size = REDIS_RAFT_DEFAULT_LOG_MAX_FILE_SIZE;
    config->raft_log_fsync = true;
    config->quorum_reads = true;
    config->raftize_all_commands = true;
    config->cluster_mode = false;
    config->cluster_start_hslot = REDIS_RAFT_HASH_MIN_SLOT;
    config->cluster_end_hslot = REDIS_RAFT_HASH_MAX_SLOT;
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
            RedisModule_Log(ctx, REDIS_WARNING, "%s", errbuf);
            return RR_ERROR;
        }
    }

    return RR_OK;
}

