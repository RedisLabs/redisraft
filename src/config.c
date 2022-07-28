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

#include "config.h"

#include "redismodule.h"
#include "redisraft.h"

#include <ifaddrs.h>
#include <limits.h>
#include <net/if.h>
#include <netdb.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>

static const char *conf_id = "id";
static const char *conf_addr = "addr";
static const char *conf_periodic_interval = "periodic-interval";
static const char *conf_request_timeout = "request-timeout";
static const char *conf_election_timeout = "election-timeout";
static const char *conf_connection_timeout = "connection-timeout";
static const char *conf_join_timeout = "join-timeout";
static const char *conf_response_timeout = "response-timeout";
static const char *conf_reconnect_interval = "reconnect-interval";
static const char *conf_snapshot_filename = "snapshot-filename";
static const char *conf_log_filename = "log-filename";
static const char *conf_log_max_cache_size = "log-max-cache-size";
static const char *conf_log_max_file_size = "log-max-file-size";
static const char *conf_loglevel = "loglevel";
static const char *conf_append_req_max_count = "append-req-max-count";
static const char *conf_append_req_max_size = "append-req-max-size";
static const char *conf_snapshot_req_max_count = "snapshot-req-max-count";
static const char *conf_snapshot_req_max_size = "snapshot-req-max-size";

const char *err_init = "Configuration change is not allowed after init/join for this parameter";

static int setRedisConfig(RedisModuleCtx *ctx,
                          const char *param,
                          const char *value)
{
    RRStatus ret = RR_OK;
    RedisModuleCallReply *reply = NULL;

    reply = RedisModule_Call(ctx, "CONFIG", "ccc", "SET", param, value);

    if (!reply) {
        return RR_ERROR;
    }

    size_t len;
    const char *str = RedisModule_CallReplyStringPtr(reply, &len);

    if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_STRING ||
        len != 2 || memcmp(str, "OK", 2) != 0) {
        ret = RR_ERROR;
    }

    RedisModule_FreeCallReply(reply);
    return ret;
}

static char *getRedisConfig(RedisModuleCtx *ctx, const char *name)
{
    size_t len;
    const char *str;
    char *buf = NULL;
    RedisModuleCallReply *reply, *reply_name = NULL;

    reply = RedisModule_Call(ctx, "CONFIG", "cc", "GET", name);

    if (!reply) {
        return NULL;
    }

    if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_ARRAY ||
        RedisModule_CallReplyLength(reply) < 2) {
        goto exit;
    }

    reply_name = RedisModule_CallReplyArrayElement(reply, 1);
    if (!reply_name ||
        RedisModule_CallReplyType(reply_name) != REDISMODULE_REPLY_STRING) {
        goto exit;
    }

    str = RedisModule_CallReplyStringPtr(reply_name, &len);
    if (len == 0) {
        goto exit;
    }
    buf = RedisModule_Alloc(len + 1);
    memcpy(buf, str, len);
    buf[len] = '\0';

exit:
    if (reply_name) {
        RedisModule_FreeCallReply(reply_name);
    }
    RedisModule_FreeCallReply(reply);

    return buf;
}

static RedisModuleString *getString(const char *name, void *privdata)
{
    RedisRaftConfig *c = privdata;
    RedisModuleString *ret = NULL;

    if (strcasecmp(name, conf_snapshot_filename) == 0) {
        ret = RedisModule_CreateStringPrintf(NULL, "%s", c->snapshot_filename);
    } else if (strcasecmp(name, conf_log_filename) == 0) {
        ret = RedisModule_CreateStringPrintf(NULL, "%s", c->log_filename);
    } else if (strcasecmp(name, conf_addr) == 0) {
        ret = RedisModule_CreateStringPrintf(NULL, "%s:%u", c->addr.host, c->addr.port);
    }

    if (ret) {
        /*
         * Config API assumes string configurations are RedisModuleStrings
         * inside the module. Modules are supposed to return a pointer to
         * a config object on each "get" callback. In RedisRaft, we prefer
         * C strings for string configurations as it is simpler to use. So, we
         * create RedisModuleString on the fly in this callback. Config API
         * wouldn't free this object as it assumes we return a pointer to a
         * config variable. Due to this limitation, we need to keep a pointer in
         * a temporary variable to avoid leaks. On each call, we deallocate the
         * previous value and assign a new config object we are about to return.
         * Related issue: https://github.com/redis/redis/issues/10749
         */
        RedisModule_FreeString(NULL, c->str_conf_ref);
        c->str_conf_ref = ret;
    }

    return ret;
}

static int setString(const char *name,
                     RedisModuleString *val,
                     void *privdata,
                     RedisModuleString **err)
{
    RedisRaftCtx *rr = &redis_raft;
    RedisRaftConfig *c = privdata;

    if (rr->state != REDIS_RAFT_UNINITIALIZED) {
        if (!strcasecmp(name, conf_addr) ||
            !strcasecmp(name, conf_log_filename)) {
            *err = RedisModule_CreateString(NULL, err_init, strlen(err_init));
            return REDISMODULE_ERR;
        }
    }

    size_t len;
    const char *value = RedisModule_StringPtrLen(val, &len);

    if (strcasecmp(name, conf_snapshot_filename) == 0) {
        RedisModule_Free(c->snapshot_filename);
        c->snapshot_filename = RedisModule_Strdup(value);
    } else if (strcasecmp(name, conf_log_filename) == 0) {
        RedisModule_Free(c->log_filename);
        c->log_filename = RedisModule_Strdup(value);
    } else if (strcasecmp(name, conf_addr) == 0) {
        if (*value == '\0') {
            c->addr = (NodeAddr){0};
        } else if (!NodeAddrParse(value, len, &c->addr)) {
            *err = RedisModule_CreateStringPrintf(NULL, "Address is invalid. It must be in the form of 10.0.0.3:8000");
            return REDISMODULE_ERR;
        }
    } else {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}

static int getEnum(const char *name, void *privdata)
{
    (void) privdata;

    if (strcasecmp(name, conf_loglevel) == 0) {
        return redisraft_loglevel;
    }

    return REDISMODULE_ERR;
}

static int setEnum(const char *name, int val, void *privdata, RedisModuleString **err)
{
    (void) err;
    (void) privdata;

    if (strcasecmp(name, conf_loglevel) == 0) {
        redisraft_loglevel = val;
    } else {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}

static long long getNumeric(const char *name, void *privdata)
{
    RedisRaftConfig *c = privdata;

    if (strcasecmp(name, conf_id) == 0) {
        return c->id;
    } else if (strcasecmp(name, conf_periodic_interval) == 0) {
        return c->periodic_interval;
    } else if (strcasecmp(name, conf_request_timeout) == 0) {
        return c->request_timeout;
    } else if (strcasecmp(name, conf_election_timeout) == 0) {
        return c->election_timeout;
    } else if (strcasecmp(name, conf_connection_timeout) == 0) {
        return c->connection_timeout;
    } else if (strcasecmp(name, conf_join_timeout) == 0) {
        return c->join_timeout;
    } else if (strcasecmp(name, conf_response_timeout) == 0) {
        return c->response_timeout;
    }  else if (strcasecmp(name, conf_reconnect_interval) == 0) {
        return c->reconnect_interval;
    } else if (strcasecmp(name, conf_log_max_file_size) == 0) {
        return (long long) c->log_max_file_size;
    } else if (strcasecmp(name, conf_log_max_cache_size) == 0) {
        return (long long) c->log_max_cache_size;
    } else if (strcasecmp(name, conf_append_req_max_count) == 0) {
        return c->append_req_max_count;
    } else if (strcasecmp(name, conf_append_req_max_size) == 0) {
        return c->append_req_max_size;
    } else if (strcasecmp(name, conf_snapshot_req_max_count) == 0) {
        return c->snapshot_req_max_count;
    } else if (strcasecmp(name, conf_snapshot_req_max_size) == 0) {
        return c->snapshot_req_max_size;
    }

    return REDISMODULE_ERR;
}

static int setNumeric(const char *name, long long val, void *priv,
                      RedisModuleString **err)
{
    RedisRaftCtx *rr = &redis_raft;
    RedisRaftConfig *c = priv;

    if (rr->state != REDIS_RAFT_UNINITIALIZED) {
        if (!strcasecmp(name, conf_id)) {
            *err = RedisModule_CreateString(NULL, err_init, strlen(err_init));
            return REDISMODULE_ERR;
        }
    }

    if (strcasecmp(name, conf_id) == 0) {
        c->id = (raft_node_id_t) val;
    } else if (strcasecmp(name, conf_periodic_interval) == 0) {
        c->periodic_interval = (int) val;
    } else if (strcasecmp(name, conf_request_timeout) == 0) {
        int rc;
        int timeout = (int) val;

        if (rr->state == REDIS_RAFT_UP) {
            rc = raft_config(rr->raft, 1, RAFT_CONFIG_REQUEST_TIMEOUT, timeout);
            if (rc != 0) {
                *err = RedisModule_CreateStringPrintf(NULL, "Failed to configure request timeout: internal error");
                return REDISMODULE_ERR;
            }
        }
        c->request_timeout = timeout;
    } else if (strcasecmp(name, conf_election_timeout) == 0) {
        int rc;
        int timeout = (int) val;

        if (rr->state == REDIS_RAFT_UP) {
            rc = raft_config(rr->raft, 1, RAFT_CONFIG_ELECTION_TIMEOUT, timeout);
            if (rc != 0) {
                *err = RedisModule_CreateStringPrintf(NULL, "Failed to configure election timeout: internal error");
                return REDISMODULE_ERR;
            }
        }
        c->election_timeout = timeout;
    } else if (strcasecmp(name, conf_connection_timeout) == 0) {
        c->connection_timeout = (int) val;
    } else if (strcasecmp(name, conf_join_timeout) == 0) {
        c->join_timeout = (int) val;
    } else if (strcasecmp(name, conf_response_timeout) == 0) {
        c->response_timeout = (int) val;
    } else if (strcasecmp(name, conf_reconnect_interval) == 0) {
        c->reconnect_interval = (int) val;
    } else if (strcasecmp(name, conf_log_max_cache_size) == 0) {
        c->log_max_cache_size = val;
    } else if (strcasecmp(name, conf_log_max_file_size) == 0) {
        c->log_max_file_size = val;
    } else if (strcasecmp(name, conf_append_req_max_count) == 0) {
        c->append_req_max_count = val;
    } else if (strcasecmp(name, conf_append_req_max_size) == 0) {
        c->append_req_max_size = val;
    } else if (strcasecmp(name, conf_snapshot_req_max_count) == 0) {
        c->snapshot_req_max_count = val;
    } else if (strcasecmp(name, conf_snapshot_req_max_size) == 0) {
        c->snapshot_req_max_size = val;
    } else {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}

/* Get address from first non-internal interface */
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
                      sizeof(struct sockaddr_in) :
                      sizeof(struct sockaddr_in6);

    int ret = getnameinfo(ent->ifa_addr, size, addr->host, sizeof(addr->host),
                          NULL, 0, NI_NUMERICHOST);
    freeifaddrs(addrs);

    return ret != 0 ? RR_ERROR : RR_OK;
}

static int validateRedisConfig(RedisModuleCtx *ctx)
{
    if (setRedisConfig(ctx, "save", "") != RR_OK) {
        LOG_WARNING("Failed to set Redis 'save' configuration!");
        return RR_ERROR;
    }

    return RR_OK;
}

/* If 'addr' was not set explicitly, try to guess it */
static int getListenAddr(RedisRaftConfig *c, RedisModuleCtx *ctx)
{
    char *port = getRedisConfig(ctx, "port");
    if (!port || *port == '\0' || getInterfaceAddr(&c->addr) != RR_OK) {
        RedisModule_Free(port);
        LOG_WARNING("Failed to determine local address, please set 'raft.addr'.");
        return RR_ERROR;
    }
    c->addr.port = strtoul(port, NULL, 10);

    RedisModule_Free(port);
    return RR_OK;
}

int ConfigInit(RedisRaftConfig *c, RedisModuleCtx *ctx)
{
    int ret = 0;

    *c = (RedisRaftConfig){
        .str_conf_ref = RedisModule_CreateString(NULL, "", 0),
    };

    if (validateRedisConfig(ctx) != RR_OK) {
        goto err;
    }

    /* clang-format off */
                                                  /* name */                   /* default-value */   /* flags */               /* min - max value */
    ret |= RedisModule_RegisterNumericConfig(ctx, conf_id,                         0,                REDISMODULE_CONFIG_DEFAULT,   0, INT_MAX,   getNumeric, setNumeric, NULL, c);
    ret |= RedisModule_RegisterNumericConfig(ctx, conf_periodic_interval,          100,              REDISMODULE_CONFIG_DEFAULT,   1, INT_MAX,   getNumeric, setNumeric, NULL, c);
    ret |= RedisModule_RegisterNumericConfig(ctx, conf_request_timeout,            200,              REDISMODULE_CONFIG_DEFAULT,   1, INT_MAX,   getNumeric, setNumeric, NULL, c);
    ret |= RedisModule_RegisterNumericConfig(ctx, conf_election_timeout,           1000,             REDISMODULE_CONFIG_DEFAULT,   1, INT_MAX,   getNumeric, setNumeric, NULL, c);
    ret |= RedisModule_RegisterNumericConfig(ctx, conf_connection_timeout,         3000,             REDISMODULE_CONFIG_DEFAULT,   1, INT_MAX,   getNumeric, setNumeric, NULL, c);
    ret |= RedisModule_RegisterNumericConfig(ctx, conf_join_timeout,               120000,           REDISMODULE_CONFIG_DEFAULT,   1, INT_MAX,   getNumeric, setNumeric, NULL, c);
    ret |= RedisModule_RegisterNumericConfig(ctx, conf_response_timeout,           1000,             REDISMODULE_CONFIG_DEFAULT,   1, INT_MAX,   getNumeric, setNumeric, NULL, c);
    ret |= RedisModule_RegisterNumericConfig(ctx, conf_reconnect_interval,         100,              REDISMODULE_CONFIG_DEFAULT,   1, INT_MAX,   getNumeric, setNumeric, NULL, c);
    ret |= RedisModule_RegisterNumericConfig(ctx, conf_append_req_max_count,       2,                REDISMODULE_CONFIG_DEFAULT,   1, INT_MAX,   getNumeric, setNumeric, NULL, c);
    ret |= RedisModule_RegisterNumericConfig(ctx, conf_append_req_max_size,        2097152,          REDISMODULE_CONFIG_MEMORY,    1, INT_MAX,   getNumeric, setNumeric, NULL, c);
    ret |= RedisModule_RegisterNumericConfig(ctx, conf_snapshot_req_max_count,     32,               REDISMODULE_CONFIG_DEFAULT,   1, INT_MAX,   getNumeric, setNumeric, NULL, c);
    ret |= RedisModule_RegisterNumericConfig(ctx, conf_snapshot_req_max_size,      65536,            REDISMODULE_CONFIG_MEMORY,    1, INT_MAX,   getNumeric, setNumeric, NULL, c);
    ret |= RedisModule_RegisterNumericConfig(ctx, conf_log_max_cache_size,         64000000,         REDISMODULE_CONFIG_MEMORY,    0, LLONG_MAX, getNumeric, setNumeric, NULL, c);
    ret |= RedisModule_RegisterNumericConfig(ctx, conf_log_max_file_size,          128000000,        REDISMODULE_CONFIG_MEMORY,    0, LLONG_MAX, getNumeric, setNumeric, NULL, c);

                                                  /* name */                   /* default-value */   /* flags */
    ret |= RedisModule_RegisterStringConfig(ctx,  conf_snapshot_filename,          "raft.snapshot",  REDISMODULE_CONFIG_DEFAULT,                 getString,  setString,  NULL, c);
    ret |= RedisModule_RegisterStringConfig(ctx,  conf_log_filename,               "raft.db",        REDISMODULE_CONFIG_DEFAULT,                 getString,  setString,  NULL, c);
    ret |= RedisModule_RegisterStringConfig(ctx,  conf_addr,                       "",               REDISMODULE_CONFIG_DEFAULT,                 getString,  setString,  NULL, c);

                                                   /* name */                  /* default-value */   /* flags */
    ret |= RedisModule_RegisterEnumConfig(ctx,    conf_loglevel,                   LOG_LEVEL_NOTICE, REDISMODULE_CONFIG_DEFAULT,  redisraft_loglevels, redisraft_loglevel_enums, LOG_LEVEL_COUNT,  getEnum, setEnum, NULL, c);
    /* clang-format on */

    ret |= RedisModule_LoadConfigs(ctx);
    if (ret != REDISMODULE_OK) {
        LOG_WARNING("Failed to register configurations.");
        goto err;
    }

    if (c->addr.host[0] == '\0') {
        if (getListenAddr(c, ctx) != RR_OK) {
            goto err;
        }
    }

    return RR_OK;

err:
    ConfigFree(c);
    return RR_ERROR;
}

void ConfigFree(RedisRaftConfig *c)
{
    if (c->str_conf_ref) {
        RedisModule_FreeString(NULL, c->str_conf_ref);
    }

    RedisModule_Free(c->snapshot_filename);
    RedisModule_Free(c->log_filename);

    *c = (RedisRaftConfig){0};
}
