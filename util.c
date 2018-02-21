#include <string.h>
#include "redisraft.h"

int rmstring_to_int(RedisModuleString *str, int *value)
{
    long long tmpll;
    if (RedisModule_StringToLongLong(str, &tmpll) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    if (tmpll < INT32_MIN || tmpll > INT32_MAX) {
        return REDISMODULE_ERR;
    }

    *value = tmpll;
    return REDISMODULE_OK;
}


char *catsnprintf(char *strbuf, size_t *strbuf_len, const char *fmt, ...)
{
    va_list ap;
    size_t len;
    size_t used = strlen(strbuf);
    size_t avail = *strbuf_len - used;

    va_start(ap, fmt);
    len = vsnprintf(strbuf + used, avail, fmt, ap);

    if (len >= avail) {
        if (len - avail > 4096) {
            *strbuf_len += (len + 1);
        } else {
            *strbuf_len += 4096;
        }

        /* "Rewind" va_arg(); Apparently this is required by older versions (rhel6) */
        va_end(ap);
        va_start(ap, fmt);

        strbuf = RedisModule_Realloc(strbuf, *strbuf_len);
        len = vsnprintf(strbuf + used, *strbuf_len - used, fmt, ap);
    }
    va_end(ap);

    return strbuf;
}
