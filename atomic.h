/*
 * Copyright (c) 2015, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef _REDISRAFT_ATOMIC_H
#define _REDISRAFT_ATOMIC_H

#if (__i386 || __amd64 || __powerpc__) && __GNUC__
    #define GNUC_VERSION (__GNUC__ * 10000 + __GNUC_MINOR__ * 100 + __GNUC_PATCHLEVEL__)

    #if defined(__clang__)
        #define HAVE_ATOMIC
    #endif

    #if (defined(__GLIBC__) && defined(__GLIBC_PREREQ))
        #if (GNUC_VERSION >= 40100 && __GLIBC_PREREQ(2, 6))
            #define HAVE_ATOMIC
        #endif
    #endif
#endif

#if !defined(__ATOMIC_VAR_FORCE_SYNC_MACROS) && defined(__STDC_VERSION__) && \
    (__STDC_VERSION__ >= 201112L) && !defined(__STDC_NO_ATOMICS__)

    #include <stdatomic.h>

    #define redisAtomic _Atomic
    #define atomicSetRelaxed(var, value) atomic_store_explicit(&var, value, memory_order_relaxed)
    #define atomicGetRelaxed(var) atomic_load_explicit(&var, memory_order_relaxed)

#elif !defined(__ATOMIC_VAR_FORCE_SYNC_MACROS) && \
    (!defined(__clang__) || !defined(__APPLE__) || __apple_build_version__ > 4210057) && \
    defined(__ATOMIC_RELAXED) && defined(__ATOMIC_SEQ_CST)

    #define redisAtomic
    #define atomicSetRelaxed(var, value) __atomic_store_n(&var, value, __ATOMIC_RELAXED)
    #define atomicGetRelaxed(var, value) __atomic_load_n(&var, __ATOMIC_RELAXED)

#elif defined(HAVE_ATOMIC)

    #define redisAtomic
    #define atomicSetRelaxed(var, value) while(!__sync_bool_compare_and_swap(&var,var,value))
    #define atomicGetRelaxed(var, value) __sync_sub_and_fetch(&var, 0)

#else
    #error "Unable to determine atomic operations for your platform"
#endif

#endif
