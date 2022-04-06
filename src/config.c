/*
 * This file is part of RedisRaft.
 *
 * Copyright (c) 2020-21 Redis Ltd.
 *
 * RedisRaft is licensed under the Redis Source Available License (RSAL).
 */

#if defined(__APPLE__) && !defined(_DARWIN_C_SOURCE)
/* Required for net/if.h and IFF_LOOPBACK  */
#define _DARWIN_C_SOURCE
#endif

#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <net/if.h>
#include <ifaddrs.h>
#include <netdb.h>

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
static const char *CONF_TRACE = "trace";
static const char *CONF_SHARDING = "sharding";
static const char *CONF_SLOT_CONFIG = "slot-config";
static const char *CONF_SHARDGROUP_UPDATE_INTERVAL = "shardgroup-update-interval";
static const char *CONF_IGNORED_COMMANDS = "ignored-commands";
static const char *CONF_EXTERNAL_SHARDING = "external-sharding";
static const char *CONF_MAX_APPEND_REQ_IN_FLIGHT = "max-append-req-in-flight";
static const char *CONF_TLS_ENABLED = "tls-enabled";
static const char *CONF_CLUSTER_USER = "cluster-user";
static const char *CONF_CLUSTER_PASSWORD = "cluster-password";

static RRStatus setRedisConfig(RedisModuleCtx *ctx, const char *param, const char *value)
{
    size_t len;
    const char *str;
    RedisModuleCallReply *reply = NULL;
    RRStatus ret = RR_OK;

    enterRedisModuleCall();
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
    exitRedisModuleCall();
    if (reply) {
        RedisModule_FreeCallReply(reply);
    }

    return ret;
}

char *getRedisConfig(RedisModuleCtx *ctx, const char *name)
{
    size_t len;
    const char *str;
    char *buf = NULL;
    RedisModuleCallReply *reply = NULL, *reply_name = NULL;

    enterRedisModuleCall();
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
    exitRedisModuleCall();
    if (reply_name) {
        RedisModule_FreeCallReply(reply_name);
    }
    if (reply) {
        RedisModule_FreeCallReply(reply);
    }

    return buf;
}

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

static int setLogLevel(const char *value)
{
    for (int i = 0; i < LOG_LEVEL_COUNT; i++) {
        if (!strcasecmp(value, redis_raft_log_levels[i])) {
            redis_raft_loglevel = i;
            return 0;
        }
    }

    return -1;
}

static const char *getLoglevelName(int level)
{
    assert(level >= 0 && level < LOG_LEVEL_COUNT);
    return redis_raft_log_levels[level];
}

static int setTrace(const char *value)
{
    if (!strcasecmp(value, "off")) {
        redis_raft_trace = TRACE_OFF;
    } else if (!strcasecmp(value, "node")) {
        redis_raft_trace ^= TRACE_NODE;
    } else if (!strcasecmp(value, "conn")) {
        redis_raft_trace ^= TRACE_CONN;
    } else if (!strcasecmp(value, "raftlog")) {
        redis_raft_trace ^= TRACE_RAFTLOG;
    } else if (!strcasecmp(value, "raftlib")) {
        redis_raft_trace ^= TRACE_RAFTLIB;
    } else if (!strcasecmp(value, "generic")) {
        redis_raft_trace ^= TRACE_GENERIC;
    } else if (!strcasecmp(value, "all")) {
        redis_raft_trace = TRACE_ALL;
    }  else {
        return -1;
    }

    return 0;
}

static const char *getTraceFlags(char *buf, size_t size)
{
    RedisModule_Assert(size >= 128);

    size_t wr = 0;

    if (redis_raft_trace == TRACE_OFF) {
        *buf = '\0';
        return buf;
    }

    if (redis_raft_trace & TRACE_NODE) {
        wr += snprintf(buf + wr, size - wr, "node ");
    }
    if (redis_raft_trace & TRACE_CONN) {
        wr += snprintf(buf + wr, size - wr, "conn ");
    }
    if (redis_raft_trace & TRACE_RAFTLOG) {
        wr += snprintf(buf + wr, size - wr, "raftlog ");
    }
    if (redis_raft_trace & TRACE_RAFTLIB) {
        wr += snprintf(buf + wr, size - wr, "raftlib ");
    }
    if (redis_raft_trace & TRACE_GENERIC) {
        wr += snprintf(buf + wr, size - wr, "generic ");
    }

    /* Trim extra space at the end */
    if (wr) {
        buf[wr - 1] = '\0';
    }

    return buf;
}

int validSlotConfig(char *slot_config) {
    if (*slot_config == 0) {
        return 1;
    }

    int ret = 0;
    char *tmp = RedisModule_Strdup(slot_config);
    char *pos = tmp;
    char *endptr;
    long val_l, val_h;
    if ((pos = strchr(tmp, ':'))) {
        *pos = '\0';
        val_l = strtol(tmp, &endptr, 10);
        if (*endptr != 0) {
            goto exit;
        }
        val_h = strtol(pos+1, &endptr, 10);
        if (*endptr != 0) {
            goto exit;
        }
        if (!HashSlotRangeValid(val_l, val_h)) {
            goto exit;
        }
    } else {
        val_l = strtol(tmp, &endptr, 10);
        if (*endptr != 0 || !HashSlotValid(val_l)) {
            goto exit;
        }
    }

    ret = 1;

exit:
    RedisModule_Free(tmp);
    return ret;
}

static RRStatus processConfigParam(const char *keyword, const char *value, RedisRaftConfig *target,
                                   bool on_init, bool uninitialized, char *errbuf, int errbuflen)
{
    /* Parameters we don't accept as config set */
    if (!on_init && (!strcmp(keyword, CONF_RAFT_LOG_FILENAME) ||
                !strcmp(keyword, CONF_SLOT_CONFIG) ||
                !strcmp(keyword, CONF_EXTERNAL_SHARDING))) {
        snprintf(errbuf, errbuflen-1, "'%s' only supported at load time", keyword);
        return RR_ERROR;
    }

    if (!uninitialized && !strcmp(keyword, CONF_ID)) {
        snprintf(errbuf, errbuflen-1, "'%s' only supported at before cluster init/join", keyword);
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
        if (setLogLevel(value) != 0) {
            snprintf(errbuf, errbuflen,
                     "invalid '%s', must be 'warning', 'notice', 'verbose' or 'debug'", keyword);
            return RR_ERROR;
        }
    } else if (!strcmp(keyword, CONF_TRACE)) {
        if (setTrace(value) != 0) {
            snprintf(errbuf, errbuflen,
                     "invalid '%s', must be 'node', 'conn', 'raftlog', 'raftlib', 'generic', 'all', or 'off'", keyword);
            return RR_ERROR;
        }
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
        if (*errptr != '\0')
            goto invalid_value;
        target->shardgroup_update_interval = (int) val;
    } else if (!strcmp(keyword, CONF_IGNORED_COMMANDS)) {
        if (target->ignored_commands) {
            RedisModule_Free(target->ignored_commands);
        }
        target->ignored_commands = RedisModule_Strdup(value);
    } else if (!strcmp(keyword, CONF_MAX_APPEND_REQ_IN_FLIGHT)) {
        char *errptr;
        unsigned long val = strtoul(value, &errptr, 10);
        if (*errptr != '\0')
            goto invalid_value;
        target->max_appendentries_inflight = (int) val;
    } else if (!strcmp(keyword, CONF_TLS_ENABLED)) {
        bool val;
        if (parseBool(value, &val) != RR_OK)
            goto invalid_value;
        target->tls_enabled = val;
    } else if (!strcmp(keyword, CONF_CLUSTER_PASSWORD)) {
        if (target->cluster_password) {
            RedisModule_Free(target->cluster_password);
            target->cluster_password = NULL;
        }
        if (strlen(value) > 0) {
            target->cluster_password = RedisModule_Strdup(value);
        }
    } else if (!strcmp(keyword, CONF_CLUSTER_USER)) {
        if (target->cluster_user) {
            RedisModule_Free(target->cluster_user);
            target->cluster_user = NULL;
        }
        if (strlen(value) > 0) {
            target->cluster_user = RedisModule_Strdup(value);
        }
    } else {
        snprintf(errbuf, errbuflen-1, "invalid parameter '%s'", keyword);
        return RR_ERROR;
    }

    return RR_OK;

invalid_value:
    snprintf(errbuf, errbuflen-1, "invalid '%s' value", keyword);
    return RR_ERROR;
}

void ConfigSet(RedisRaftCtx *rr, RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    char errbuf[256] = "ERR ";
    char *pos = errbuf + strlen(errbuf);
    const int cap  = (int)(sizeof(errbuf) - strlen(errbuf));
    bool st = rr->state == REDIS_RAFT_UNINITIALIZED;

    if (argc != 4) {
        RedisModule_WrongArity(ctx);
        return;
    }

    char *key = StrCreateFromString(argv[2]);
    char *value = StrCreateFromString(argv[3]);

    if (processConfigParam(key, value, rr->config, false, st, pos, cap) != RR_OK) {
        RedisModule_ReplyWithError(ctx, errbuf);
        goto out;
    }

    RedisModule_ReplyWithSimpleString(ctx, "OK");

out:
    RedisModule_Free(key);
    RedisModule_Free(value);
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

void ConfigGet(RedisRaftCtx *rr, RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    RedisRaftConfig *config = rr->config;

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
    if (stringmatch(pattern, CONF_TRACE, 1)) {
        len++;
        char buf[512];
        replyConfigStr(ctx, CONF_TRACE, getTraceFlags(buf, sizeof(buf)));
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
        replyConfigStr(ctx, CONF_IGNORED_COMMANDS, config->ignored_commands ? config->ignored_commands : "");
    }
    if (stringmatch(pattern, CONF_MAX_APPEND_REQ_IN_FLIGHT, 1)) {
        len++;
        replyConfigInt(ctx, CONF_MAX_APPEND_REQ_IN_FLIGHT, config->max_appendentries_inflight);
    }
    if (stringmatch(pattern, CONF_TLS_ENABLED, 1)) {
        len++;
        replyConfigBool(ctx, CONF_TLS_ENABLED, config->tls_enabled);
    }
    if (stringmatch(pattern, CONF_CLUSTER_USER, 1)) {
        len++;
        replyConfigStr(ctx, CONF_CLUSTER_USER, config->cluster_user ? config->cluster_user : "");
    }
    RedisModule_ReplySetArrayLength(ctx, len * 2);
}

void updateTLSConfig(RedisModuleCtx *ctx, RedisRaftConfig *config)
{
    bool client_key = false;

    if (config->tls_ca_cert) {
        RedisModule_Free(config->tls_ca_cert);
    }
    config->tls_ca_cert = getRedisConfig(ctx, "tls-ca-cert-file");
    if (config->tls_key) {
        RedisModule_Free(config->tls_key);
    }
    config->tls_key = getRedisConfig(ctx, "tls-key-file");
    char *key = getRedisConfig(ctx, "tls-client-key-file");
    if (key) {
        if (strcmp("", key)) {
            client_key = true;
            RedisModule_Free(config->tls_key);
            config->tls_key = key;
        } else {
            RedisModule_Free(key);
        }
    }

    if (config->tls_key_pass) {
        RedisModule_Free(config->tls_key_pass);
    }
    if (!client_key) {
        config->tls_key_pass = getRedisConfig(ctx, "tls-key-file-pass");
    } else {
        config->tls_key_pass = getRedisConfig(ctx, "tls-client-key-file-pass");
    }

    if (config->tls_cert) {
        RedisModule_Free(config->tls_cert);
    }
    if (!client_key) {
        config->tls_cert = getRedisConfig(ctx, "tls-cert-file");
    } else {
        config->tls_cert = getRedisConfig(ctx, "tls-client-cert-file");
    }
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
    config->slot_config = "0:16383";
    config->shardgroup_update_interval = REDIS_RAFT_DEFAULT_SHARDGROUP_UPDATE_INTERVAL;
    config->ignored_commands = NULL;
    config->max_appendentries_inflight = REDIS_RAFT_DEFAULT_MAX_APPENDENTRIES;
#ifdef HAVE_TLS
    updateTLSConfig(ctx, config);
#endif
    config->cluster_user = RedisModule_Strdup("default");
    config->cluster_password = NULL;
}


static RRStatus getInterfaceAddr(NodeAddr *addr)
{
    struct ifaddrs *addrs, *ent = NULL;

    if (getifaddrs(&addrs) != 0) {
        return RR_ERROR;
    }

    for (ent = addrs; ent != NULL; ent = ent->ifa_next) {
        /* Skip loopback and non-IP interfaces */
        if (!(ent->ifa_flags & IFF_LOOPBACK) &&
             (ent->ifa_addr->sa_family == AF_INET ||
              ent->ifa_addr->sa_family == AF_INET6)) {
            break;
        }
    }

    if (!ent) {
        return RR_ERROR;
    }

    size_t size = ent->ifa_addr->sa_family == AF_INET ?
                      sizeof(struct sockaddr_in) : sizeof(struct sockaddr_in6);

    int ret = getnameinfo(ent->ifa_addr, size, addr->host, sizeof(addr->host),
                          NULL, 0, NI_NUMERICHOST);
    freeifaddrs(addrs);

    return ret != 0 ? RR_ERROR : RR_OK;
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
            LOG_WARNING("No argument specified for keyword '%.*s'",
                        (int) kwlen, kw);
            return RR_ERROR;
        }

        size_t vallen;
        const char *val = RedisModule_StringPtrLen(argv[i + 1], &vallen);
        i++;

        char errbuf[256];
        if (processConfigParam(kw, val, target, true, true,
                    errbuf, sizeof(errbuf)) != RR_OK) {
            LOG_WARNING("%s", errbuf);
            return RR_ERROR;
        }
    }

    return RR_OK;
}

