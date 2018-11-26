#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmocka.h"

#define RedisModule_Alloc(size)             test_malloc(size)
#define RedisModule_Calloc(nmemb, size)     test_calloc(nmemb, size)
#define RedisModule_Realloc(ptr, size)      test_realloc(ptr, size)
#define RedisModule_Free(ptr)               test_free(ptr)
