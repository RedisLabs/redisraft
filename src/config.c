/*
 * Copyright Redis Ltd. 2020 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#if defined(__APPLE__) && !defined(_DARWIN_C_SOURCE)
/* Required for net/if.h and IFF_LOOPBACK  */
#define _DARWIN_C_SOURCE
#endif

#include "redisraft.h"

#include <ifaddrs.h>
#include <limits.h>
#include <net/if.h>
#include <netdb.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>

static const char *trace_names[] = {
    "off",
    "node",
    "conn",
    "raftlib",
    "raftlog",
    "generic",
    "migration",
    "all",
};

static const int trace_flags[] = {
    TRACE_OFF,
    TRACE_NODE,
    TRACE_CONN,
    TRACE_RAFTLIB,
    TRACE_RAFTLOG,
    TRACE_GENERIC,
    TRACE_MIGRATION,
    TRACE_ALL,
};

#define TRACE_FLAG_COUNT (sizeof(trace_flags) / sizeof(int))

/* Migration debug configs */
static const char *migration_debug_names[] = {
    "none",
    "fail-connect",
    "fail-import",
    "fail-unlock",
};

static const int migration_debug_enums[] = {
    DEBUG_MIGRATION_NONE,
    DEBUG_MIGRATION_EMULATE_CONNECT_FAILED,
    DEBUG_MIGRATION_EMULATE_IMPORT_FAILED,
    DEBUG_MIGRATION_EMULATE_UNLOCK_FAILED,
};

#define MIGRATION_CONF_COUNT (sizeof(migration_debug_enums) / sizeof(int))

static const char *conf_id = "id";
static const char *conf_addr = "addr";
static const char *conf_periodic_interval = "periodic-interval";
static const char *conf_request_timeout = "request-timeout";
static const char *conf_election_timeout = "election-timeout";
static const char *conf_connection_timeout = "connection-timeout";
static const char *conf_join_timeout = "join-timeout";
static const char *conf_response_timeout = "response-timeout";
static const char *conf_proxy_response_timeout = "proxy-response-timeout";
static const char *conf_reconnect_interval = "reconnect-interval";
static const char *conf_log_filename = "log-filename";
static const char *conf_log_max_cache_size = "log-max-cache-size";
static const char *conf_log_max_file_size = "log-max-file-size";
static const char *conf_log_fsync = "log-fsync";
static const char *conf_follower_proxy = "follower-proxy";
static const char *conf_quorum_reads = "quorum-reads";
static const char *conf_loglevel = "loglevel";
static const char *conf_trace = "trace";
static const char *conf_sharding = "sharding";
static const char *conf_slot_config = "slot-config";
static const char *conf_shardgroup_update_interval = "shardgroup-update-interval";
static const char *conf_ignored_commands = "ignored-commands";
static const char *conf_external_sharding = "external-sharding";
static const char *conf_append_req_max_count = "append-req-max-count";
static const char *conf_append_req_max_size = "append-req-max-size";
static const char *conf_snapshot_req_max_count = "snapshot-req-max-count";
static const char *conf_snapshot_req_max_size = "snapshot-req-max-size";
static const char *conf_scan_size = "scan-size";
static const char *conf_tls_enabled = "tls-enabled";
static const char *conf_cluster_user = "cluster-user";
static const char *conf_cluster_password = "cluster-password";
static const char *conf_log_delay_apply = "log-delay-apply";
static const char *conf_log_disable_apply = "log-disable-apply";
static const char *conf_snapshot_delay = "snapshot-delay";
static const char *conf_snapshot_fail = "snapshot-fail";
static const char *conf_snapshot_disable = "snapshot-disable";
static const char *conf_snapshot_disable_load = "snapshot-disable-load";
static const char *conf_migration_debug = "migration-debug";

const char *err_init = "Configuration change is not allowed after init/join for this parameter";
const char *err_raft = "Failed to configure raft: internal error";

static int setRedisConfig(RedisModuleCtx *ctx,
                          const char *param,
                          const char *value)
{
    RRStatus ret = RR_OK;
    RedisModuleCallReply *reply = NULL;

    enterRedisModuleCall();
    reply = RedisModule_Call(ctx, "CONFIG", "ccc", "SET", param, value);
    exitRedisModuleCall();

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

    enterRedisModuleCall();
    reply = RedisModule_Call(ctx, "CONFIG", "cc", "GET", name);
    exitRedisModuleCall();

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

static bool validSlotConfig(const char *slot_config)
{
    long val_l, val_h;
    char *end, *pos;

    if (*slot_config == '\0') {
        return true;
    }

    pos = strchr(slot_config, ':');
    if (pos) {
        errno = 0;
        val_l = strtol(slot_config, &end, 10);
        if (errno != 0 || end != pos) {
            return false;
        }

        errno = 0;
        val_h = strtol(pos + 1, &end, 10);
        if (errno != 0 || *end != '\0' || !HashSlotRangeValid(val_l, val_h)) {
            return false;
        }
    } else {
        errno = 0;
        val_l = strtol(slot_config, &end, 10);
        if (errno != 0 || *end != '\0' || !HashSlotValid(val_l)) {
            return false;
        }
    }

    return true;
}

#ifdef HAVE_TLS
/* Callback for passing a keyfile password stored as a char* to OpenSSL */
static int tlsPasswordCallback(char *buf, int size, int rwflag, void *u)
{
    (void) rwflag;
    const char *pass = u;
    size_t pass_len;

    if (!pass) {
        return -1;
    }

    pass_len = strlen(pass);
    if (pass_len > (size_t) size) {
        return -1;
    }
    memcpy(buf, pass, pass_len);

    return (int) pass_len;
}

static SSL_CTX *generateSSLContext(const char *cacert_filename,
                                   const char *capath,
                                   const char *cert,
                                   const char *key,
                                   const char *keypass)
{
    if ((cert != NULL && key == NULL) || (key != NULL && cert == NULL)) {
        LOG_WARNING("'tls-cert-file' or 'tls-key-file' is not configured.");
        return NULL;
    }

    SSL_CTX *ctx = SSL_CTX_new(SSLv23_client_method());
    if (!ctx) {
        LOG_WARNING("SSL_CTX_new(): %s",
                    ERR_reason_error_string(ERR_peek_last_error()));
        return NULL;
    }

    SSL_CTX_set_options(ctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3);
    SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, NULL);

    if (keypass && *keypass != '\0') {
        SSL_CTX_set_default_passwd_cb(ctx, tlsPasswordCallback);
        SSL_CTX_set_default_passwd_cb_userdata(ctx, RedisModule_Strdup(keypass));
    }

    if (!SSL_CTX_load_verify_locations(ctx, cacert_filename, capath)) {
        LOG_WARNING("SSL_CTX_load_verify_locations(): %s",
                    ERR_reason_error_string(ERR_peek_last_error()));
        goto error;
    }

    if (!SSL_CTX_use_certificate_chain_file(ctx, cert)) {
        LOG_WARNING("SSL_CTX_use_certificate_chain_file(): %s",
                    ERR_reason_error_string(ERR_peek_last_error()));
        goto error;
    }

    if (!SSL_CTX_use_PrivateKey_file(ctx, key, SSL_FILETYPE_PEM)) {
        LOG_WARNING("SSL_CTX_use_PrivateKey_file(): %s",
                    ERR_reason_error_string(ERR_peek_last_error()));
        goto error;
    }

    return ctx;

error:
    SSL_CTX_free(ctx);
    return NULL;
}

static RRStatus updateTLSConfig(RedisRaftConfig *config, RedisModuleCtx *ctx)
{
    int ret = RR_OK;
    char *clientkey, *key, *keypass, *cacert_filename, *capath, *cert;

    cacert_filename = getRedisConfig(ctx, "tls-ca-cert-file");
    capath = getRedisConfig(ctx, "tls-ca-cert-dir");

    clientkey = getRedisConfig(ctx, "tls-client-key-file");
    if (clientkey && *clientkey != '\0') {
        key = getRedisConfig(ctx, "tls-client-key-file");
        cert = getRedisConfig(ctx, "tls-client-cert-file");
        keypass = getRedisConfig(ctx, "tls-client-key-file-pass");
    } else {
        key = getRedisConfig(ctx, "tls-key-file");
        cert = getRedisConfig(ctx, "tls-cert-file");
        keypass = getRedisConfig(ctx, "tls-key-file-pass");
    }

    SSL_CTX *ssl = generateSSLContext(cacert_filename, capath, cert, key, keypass);
    if (!ssl) {
        ret = RR_ERROR;
        goto out;
    }

    if (config->ssl) {
        RedisModule_Free(SSL_CTX_get_default_passwd_cb_userdata(config->ssl));
        SSL_CTX_free(config->ssl);
    }
    config->ssl = ssl;

out:
    RedisModule_Free(cacert_filename);
    RedisModule_Free(capath);
    RedisModule_Free(cert);
    RedisModule_Free(key);
    RedisModule_Free(keypass);
    RedisModule_Free(clientkey);
    return ret;
}

#endif

static RedisModuleString *getString(const char *name, void *privdata)
{
    RedisRaftConfig *c = privdata;
    RedisModuleString *ret = NULL;

    if (strcasecmp(name, conf_log_filename) == 0) {
        ret = RedisModule_CreateStringPrintf(NULL, "%s", c->log_filename);
    } else if (strcasecmp(name, conf_addr) == 0) {
        ret = RedisModule_CreateStringPrintf(NULL, "%s:%u", c->addr.host, c->addr.port);
    } else if (strcasecmp(name, conf_slot_config) == 0) {
        ret = RedisModule_CreateStringPrintf(NULL, "%s", c->slot_config);
    } else if (strcasecmp(name, conf_ignored_commands) == 0) {
        ret = RedisModule_CreateStringPrintf(NULL, "%s", c->ignored_commands);
    } else if (strcasecmp(name, conf_cluster_user) == 0) {
        ret = RedisModule_CreateStringPrintf(NULL, "%s", c->cluster_user);
    } else if (strcasecmp(name, conf_cluster_password) == 0) {
        ret = RedisModule_CreateStringPrintf(NULL, "%s", c->cluster_password);
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
            !strcasecmp(name, conf_slot_config) ||
            !strcasecmp(name, conf_log_filename)) {
            *err = RedisModule_CreateString(NULL, err_init, strlen(err_init));
            return REDISMODULE_ERR;
        }
    }

    size_t len;
    const char *value = RedisModule_StringPtrLen(val, &len);

    if (strcasecmp(name, conf_log_filename) == 0) {
        RedisModule_Free(c->log_filename);
        c->log_filename = RedisModule_Strdup(value);
    } else if (strcasecmp(name, conf_addr) == 0) {
        if (*value == '\0') {
            c->addr = (NodeAddr){0};
        } else if (!NodeAddrParse(value, len, &c->addr)) {
            *err = RedisModule_CreateStringPrintf(NULL, "Address is invalid. It must be in the form of 10.0.0.3:8000");
            return REDISMODULE_ERR;
        }
    } else if (strcasecmp(name, conf_slot_config) == 0) {
        if (!validSlotConfig(value)) {
            *err = RedisModule_CreateStringPrintf(NULL, "Not a valid slot config");
            return REDISMODULE_ERR;
        }
        RedisModule_Free(c->slot_config);
        c->slot_config = RedisModule_Strdup(value);
    } else if (strcasecmp(name, conf_ignored_commands) == 0) {
        CommandSpecTableRebuild(rr->ctx, rr->commands_spec_table, value);
        RedisModule_Free(c->ignored_commands);
        c->ignored_commands = RedisModule_Strdup(value);
    } else if (strcasecmp(name, conf_cluster_user) == 0) {
        RedisModule_Free(c->cluster_user);
        c->cluster_user = RedisModule_Strdup(value);
    } else if (strcasecmp(name, conf_cluster_password) == 0) {
        RedisModule_Free(c->cluster_password);
        c->cluster_password = RedisModule_Strdup(value);
    } else {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}

static int getEnum(const char *name, void *privdata)
{
    (void) privdata;
    RedisRaftCtx *rr = &redis_raft;

    if (strcasecmp(name, conf_loglevel) == 0) {
        return redisraft_loglevel;
    } else if (strcasecmp(name, conf_trace) == 0) {
        return redisraft_trace;
    } else if (strcasecmp(name, conf_migration_debug) == 0) {
        return rr->config.migration_debug;
    }

    return REDISMODULE_ERR;
}

static int setEnum(const char *name, int val, void *privdata, RedisModuleString **err)
{
    (void) err;
    (void) privdata;
    RedisRaftCtx *rr = &redis_raft;

    if (strcasecmp(name, conf_loglevel) == 0) {
        redisraft_loglevel = val;
    } else if (strcasecmp(name, conf_trace) == 0) {
        if (val == TRACE_OFF || val == TRACE_ALL) {
            redisraft_trace = val;
        } else {
            redisraft_trace ^= val;
        }

        if (rr->state == REDIS_RAFT_UP) {
            int flag = redisraft_trace & TRACE_RAFTLIB ? 1 : 0;
            raft_config(rr->raft, 1, RAFT_CONFIG_LOG_ENABLED, flag);
        }
    } else if (strcasecmp(name, conf_migration_debug) == 0) {
        rr->config.migration_debug = val;
    } else {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}

static int getBool(const char *name, void *privdata)
{
    RedisRaftConfig *c = privdata;

    if (strcasecmp(name, conf_log_fsync) == 0) {
        return c->log_fsync;
    } else if (strcasecmp(name, conf_follower_proxy) == 0) {
        return c->follower_proxy;
    } else if (strcasecmp(name, conf_quorum_reads) == 0) {
        return c->quorum_reads;
    } else if (strcasecmp(name, conf_sharding) == 0) {
        return c->sharding;
    } else if (strcasecmp(name, conf_external_sharding) == 0) {
        return c->external_sharding;
    } else if (strcasecmp(name, conf_tls_enabled) == 0) {
        return c->tls_enabled;
    } else if (strcasecmp(name, conf_log_disable_apply) == 0) {
        return c->log_disable_apply;
    } else if (strcasecmp(name, conf_snapshot_fail) == 0) {
        return c->snapshot_fail;
    } else if (strcasecmp(name, conf_snapshot_disable) == 0) {
        return c->snapshot_disable;
    } else if (strcasecmp(name, conf_snapshot_disable_load) == 0) {
        return c->snapshot_disable_load;
    }

    return REDISMODULE_ERR;
}

static const char *handleTlsEnabled(RedisRaftConfig *conf, int enabled)
{
#ifdef HAVE_TLS
    RedisRaftCtx *rr = &redis_raft;

    if (enabled) {
        if (updateTLSConfig(conf, rr->ctx) != RR_OK) {
            return "Failed to configure TLS";
        }
    } else {
        if (conf->ssl) {
            RedisModule_Free(SSL_CTX_get_default_passwd_cb_userdata(conf->ssl));
            SSL_CTX_free(conf->ssl);
            conf->ssl = NULL;
        }
    }
#else
    (void) conf;

    if (enabled) {
        return "Build RedisRaft with TLS to enable it";
    }
#endif

    return NULL;
}

static int setBool(const char *name, int val, void *pr, RedisModuleString **err)
{
    RedisRaftCtx *rr = &redis_raft;
    RedisRaftConfig *c = pr;

    if (rr->state != REDIS_RAFT_UNINITIALIZED) {
        if (!strcasecmp(name, conf_external_sharding)) {
            *err = RedisModule_CreateString(NULL, err_init, strlen(err_init));
            return REDISMODULE_ERR;
        }
    }

    if (strcasecmp(name, conf_log_fsync) == 0) {
        c->log_fsync = val;
    } else if (strcasecmp(name, conf_follower_proxy) == 0) {
        c->follower_proxy = val;
    } else if (strcasecmp(name, conf_quorum_reads) == 0) {
        c->quorum_reads = val;
    } else if (strcasecmp(name, conf_sharding) == 0) {
        c->sharding = val;
    } else if (strcasecmp(name, conf_external_sharding) == 0) {
        c->external_sharding = val;
    } else if (strcasecmp(name, conf_tls_enabled) == 0) {
        const char *errmsg = handleTlsEnabled(c, val);
        if (errmsg) {
            *err = RedisModule_CreateString(NULL, errmsg, strlen(errmsg));
            return REDISMODULE_ERR;
        }
        c->tls_enabled = val;
    } else if (strcasecmp(name, conf_log_disable_apply) == 0) {
        if (rr->state == REDIS_RAFT_UP) {
            if (raft_config(rr->raft, 1, RAFT_CONFIG_DISABLE_APPLY, val) != 0) {
                *err = RedisModule_CreateString(NULL, err_raft, strlen(err_raft));
                return REDISMODULE_ERR;
            }
        }
        c->log_disable_apply = val;
    } else if (strcasecmp(name, conf_snapshot_fail) == 0) {
        c->snapshot_fail = val;
    } else if (strcasecmp(name, conf_snapshot_disable) == 0) {
        c->snapshot_disable = val;
    } else if (strcasecmp(name, conf_snapshot_disable_load) == 0) {
        c->snapshot_disable_load = val;
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
    } else if (strcasecmp(name, conf_proxy_response_timeout) == 0) {
        return c->proxy_response_timeout;
    } else if (strcasecmp(name, conf_reconnect_interval) == 0) {
        return c->reconnect_interval;
    } else if (strcasecmp(name, conf_log_max_file_size) == 0) {
        return (long long) c->log_max_file_size;
    } else if (strcasecmp(name, conf_log_max_cache_size) == 0) {
        return (long long) c->log_max_cache_size;
    } else if (strcasecmp(name, conf_shardgroup_update_interval) == 0) {
        return c->shardgroup_update_interval;
    } else if (strcasecmp(name, conf_append_req_max_count) == 0) {
        return c->append_req_max_count;
    } else if (strcasecmp(name, conf_append_req_max_size) == 0) {
        return c->append_req_max_size;
    } else if (strcasecmp(name, conf_snapshot_req_max_count) == 0) {
        return c->snapshot_req_max_count;
    } else if (strcasecmp(name, conf_snapshot_req_max_size) == 0) {
        return c->snapshot_req_max_size;
    } else if (strcasecmp(name, conf_scan_size) == 0) {
        return c->scan_size;
    } else if (strcasecmp(name, conf_log_delay_apply) == 0) {
        return c->log_delay_apply;
    } else if (strcasecmp(name, conf_snapshot_delay) == 0) {
        return c->snapshot_delay;
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
                *err = RedisModule_CreateString(NULL, err_raft, strlen(err_raft));
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
                *err = RedisModule_CreateString(NULL, err_raft, strlen(err_raft));
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
    } else if (strcasecmp(name, conf_proxy_response_timeout) == 0) {
        c->proxy_response_timeout = (int) val;
    } else if (strcasecmp(name, conf_reconnect_interval) == 0) {
        c->reconnect_interval = (int) val;
    } else if (strcasecmp(name, conf_log_max_cache_size) == 0) {
        c->log_max_cache_size = val;
    } else if (strcasecmp(name, conf_log_max_file_size) == 0) {
        c->log_max_file_size = val;
    } else if (strcasecmp(name, conf_shardgroup_update_interval) == 0) {
        c->shardgroup_update_interval = (int) val;
    } else if (strcasecmp(name, conf_append_req_max_count) == 0) {
        c->append_req_max_count = val;
    } else if (strcasecmp(name, conf_append_req_max_size) == 0) {
        c->append_req_max_size = val;
    } else if (strcasecmp(name, conf_snapshot_req_max_count) == 0) {
        c->snapshot_req_max_count = val;
    } else if (strcasecmp(name, conf_snapshot_req_max_size) == 0) {
        c->snapshot_req_max_size = val;
    } else if (strcasecmp(name, conf_scan_size) == 0) {
        c->scan_size = val;
    } else if (strcasecmp(name, conf_log_delay_apply) == 0) {
        c->log_delay_apply = val;
    } else if (strcasecmp(name, conf_snapshot_delay) == 0) {
        c->snapshot_delay = val;
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
        if (ent->ifa_addr == NULL) {
            continue;
        }

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

    char *cfg = getRedisConfig(ctx, "cluster_enabled");
    if (cfg && *cfg != '0') {
        LOG_WARNING("RedisRaft requires Redis not be started with 'cluster_enabled'!");
        RedisModule_Free(cfg);
        return RR_ERROR;
    }
    RedisModule_Free(cfg);

    char *filename = getRedisConfig(ctx, "dbfilename");
    if (!filename || *filename == '\0') {
        LOG_WARNING("'dbfilename' configuration is missing!");
        RedisModule_Free(filename);
        return RR_ERROR;
    }
    RedisModule_Free(filename);

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

RRStatus ConfigInit(RedisModuleCtx *ctx, RedisRaftConfig *c)
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
    ret |= RedisModule_RegisterNumericConfig(ctx, conf_proxy_response_timeout,     10000,            REDISMODULE_CONFIG_DEFAULT,   1, INT_MAX,   getNumeric, setNumeric, NULL, c);
    ret |= RedisModule_RegisterNumericConfig(ctx, conf_reconnect_interval,         100,              REDISMODULE_CONFIG_DEFAULT,   1, INT_MAX,   getNumeric, setNumeric, NULL, c);
    ret |= RedisModule_RegisterNumericConfig(ctx, conf_shardgroup_update_interval, 5000,             REDISMODULE_CONFIG_DEFAULT,   1, INT_MAX,   getNumeric, setNumeric, NULL, c);
    ret |= RedisModule_RegisterNumericConfig(ctx, conf_append_req_max_count,       2,                REDISMODULE_CONFIG_DEFAULT,   1, INT_MAX,   getNumeric, setNumeric, NULL, c);
    ret |= RedisModule_RegisterNumericConfig(ctx, conf_append_req_max_size,        2097152,          REDISMODULE_CONFIG_MEMORY,    1, INT_MAX,   getNumeric, setNumeric, NULL, c);
    ret |= RedisModule_RegisterNumericConfig(ctx, conf_snapshot_req_max_count,     32,               REDISMODULE_CONFIG_DEFAULT,   1, INT_MAX,   getNumeric, setNumeric, NULL, c);
    ret |= RedisModule_RegisterNumericConfig(ctx, conf_snapshot_req_max_size,      65536,            REDISMODULE_CONFIG_MEMORY,    1, INT_MAX,   getNumeric, setNumeric, NULL, c);
    ret |= RedisModule_RegisterNumericConfig(ctx, conf_log_max_cache_size,         64000000,         REDISMODULE_CONFIG_MEMORY,    0, LLONG_MAX, getNumeric, setNumeric, NULL, c);
    ret |= RedisModule_RegisterNumericConfig(ctx, conf_log_max_file_size,          128000000,        REDISMODULE_CONFIG_MEMORY,    0, LLONG_MAX, getNumeric, setNumeric, NULL, c);
    ret |= RedisModule_RegisterNumericConfig(ctx, conf_scan_size,                  1000,             REDISMODULE_CONFIG_DEFAULT,   1, LLONG_MAX, getNumeric, setNumeric, NULL, c);
    ret |= RedisModule_RegisterNumericConfig(ctx, conf_log_delay_apply,            0,                REDISMODULE_CONFIG_HIDDEN,    0, LLONG_MAX, getNumeric, setNumeric, NULL, c);
    ret |= RedisModule_RegisterNumericConfig(ctx, conf_snapshot_delay,             0,                REDISMODULE_CONFIG_HIDDEN,    0, LLONG_MAX, getNumeric, setNumeric, NULL, c);

                                                  /* name */                   /* default-value */   /* flags */
    ret |= RedisModule_RegisterBoolConfig(ctx,    conf_log_fsync,                  true,             REDISMODULE_CONFIG_DEFAULT,                 getBool,    setBool,    NULL, c);
    ret |= RedisModule_RegisterBoolConfig(ctx,    conf_follower_proxy,             false,            REDISMODULE_CONFIG_DEFAULT,                 getBool,    setBool,    NULL, c);
    ret |= RedisModule_RegisterBoolConfig(ctx,    conf_quorum_reads,               true,             REDISMODULE_CONFIG_DEFAULT,                 getBool,    setBool,    NULL, c);
    ret |= RedisModule_RegisterBoolConfig(ctx,    conf_sharding,                   false,            REDISMODULE_CONFIG_DEFAULT,                 getBool,    setBool,    NULL, c);
    ret |= RedisModule_RegisterBoolConfig(ctx,    conf_external_sharding,          false,            REDISMODULE_CONFIG_DEFAULT,                 getBool,    setBool,    NULL, c);
    ret |= RedisModule_RegisterBoolConfig(ctx,    conf_tls_enabled,                false,            REDISMODULE_CONFIG_DEFAULT,                 getBool,    setBool,    NULL, c);
    ret |= RedisModule_RegisterBoolConfig(ctx,    conf_log_disable_apply,          false,            REDISMODULE_CONFIG_HIDDEN,                  getBool,    setBool,    NULL, c);
    ret |= RedisModule_RegisterBoolConfig(ctx,    conf_snapshot_fail,              false,            REDISMODULE_CONFIG_HIDDEN,                  getBool,    setBool,    NULL, c);
    ret |= RedisModule_RegisterBoolConfig(ctx,    conf_snapshot_disable,           false,            REDISMODULE_CONFIG_HIDDEN,                  getBool,    setBool,    NULL, c);
    ret |= RedisModule_RegisterBoolConfig(ctx,    conf_snapshot_disable_load,      false,            REDISMODULE_CONFIG_HIDDEN,                  getBool,    setBool,    NULL, c);

                                                  /* name */                   /* default-value */   /* flags */
    ret |= RedisModule_RegisterStringConfig(ctx,  conf_log_filename,               "redisraft.db",   REDISMODULE_CONFIG_DEFAULT,                 getString,  setString,  NULL, c);
    ret |= RedisModule_RegisterStringConfig(ctx,  conf_addr,                       "",               REDISMODULE_CONFIG_DEFAULT,                 getString,  setString,  NULL, c);
    ret |= RedisModule_RegisterStringConfig(ctx,  conf_slot_config,                "0:16383",        REDISMODULE_CONFIG_DEFAULT,                 getString,  setString,  NULL, c);
    ret |= RedisModule_RegisterStringConfig(ctx,  conf_ignored_commands,           "",               REDISMODULE_CONFIG_DEFAULT,                 getString,  setString,  NULL, c);
    ret |= RedisModule_RegisterStringConfig(ctx,  conf_cluster_user,               "default",        REDISMODULE_CONFIG_DEFAULT,                 getString,  setString,  NULL, c);
    ret |= RedisModule_RegisterStringConfig(ctx,  conf_cluster_password,           "",               REDISMODULE_CONFIG_SENSITIVE |
                                                                                                     REDISMODULE_CONFIG_HIDDEN,                  getString,  setString,  NULL, c);

                                                   /* name */                  /* default-value */   /* flags */
    ret |= RedisModule_RegisterEnumConfig(ctx,    conf_loglevel,               LOG_LEVEL_NOTICE,     REDISMODULE_CONFIG_DEFAULT,  redisraft_loglevels,   redisraft_loglevel_enums, LOG_LEVEL_COUNT,      getEnum, setEnum, NULL, c);
    ret |= RedisModule_RegisterEnumConfig(ctx,    conf_trace,                  TRACE_OFF,            REDISMODULE_CONFIG_BITFLAGS, trace_names,           trace_flags,              TRACE_FLAG_COUNT,     getEnum, setEnum, NULL, c);
    ret |= RedisModule_RegisterEnumConfig(ctx,    conf_migration_debug,        DEBUG_MIGRATION_NONE, REDISMODULE_CONFIG_HIDDEN,   migration_debug_names, migration_debug_enums,    MIGRATION_CONF_COUNT, getEnum, setEnum, NULL, c);
    /* clang-format on */

    ret |= RedisModule_LoadConfigs(ctx);
    if (ret != REDISMODULE_OK) {
        LOG_WARNING("Failed to register configurations.");
        goto err;
    }

    c->rdb_filename = getRedisConfig(ctx, "dbfilename");

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

    RedisModule_Free(c->rdb_filename);
    RedisModule_Free(c->log_filename);
    RedisModule_Free(c->ignored_commands);
    RedisModule_Free(c->cluster_user);
    RedisModule_Free(c->cluster_password);
    RedisModule_Free(c->slot_config);

#ifdef HAVE_TLS
    if (c->ssl) {
        RedisModule_Free(SSL_CTX_get_default_passwd_cb_userdata(c->ssl));
        SSL_CTX_free(c->ssl);
    }
#endif

    *c = (RedisRaftConfig){0};
}

void ConfigRedisEventCallback(RedisModuleCtx *ctx,
                              RedisModuleEvent eid,
                              uint64_t event,
                              void *data)
{
    if (eid.id != REDISMODULE_EVENT_CONFIG ||
        event != REDISMODULE_SUBEVENT_CONFIG_CHANGE) {
        return;
    }

#ifndef HAVE_TLS
    (void) ctx;
    (void) data;
#else
    RedisRaftCtx *rr = &redis_raft;

    if (!rr->config.tls_enabled) {
        return;
    }

    RedisModuleConfigChangeV1 *ei = data;

    for (unsigned int i = 0; i < ei->num_changes; i++) {
        const char *conf = ei->config_names[i];

        if (!strcmp(conf, "tls-ca-cert-file") ||
            !strcmp(conf, "tls-ca-cert-dir") ||
            !strcmp(conf, "tls-cert-file") ||
            !strcmp(conf, "tls-key-file") ||
            !strcmp(conf, "tls-key-file-pass") ||
            !strcmp(conf, "tls-client-cert-file") ||
            !strcmp(conf, "tls-client-key-file") ||
            !strcmp(conf, "tls-client-key-file-pass")) {

            updateTLSConfig(&rr->config, ctx);
            break;
        }
    }
#endif
}
