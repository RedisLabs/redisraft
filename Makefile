# This file is part of RedisRaft.
#
# Copyright (c) 2020-2021 Redis Ltd.
#
# RedisRaft is licensed under the Redis Source Available License (RSAL).

OS := $(shell sh -c 'uname -s 2>/dev/null || echo none')
BUILDDIR := $(CURDIR)/.build

ifeq ($(OS),Linux)
    ARCH_CFLAGS := -fPIC
    ARCH_LDFLAGS := -shared -Wl,-Bsymbolic-functions
else
    ARCH_CFLAGS := -dynamic
    ARCH_LDFLAGS := -bundle -undefined dynamic_lookup
endif

CC = gcc
CPPFLAGS = -D_POSIX_C_SOURCE=200112L -D_GNU_SOURCE
OPTIMIZATION?=-O3

ifdef SANITIZER
ifeq ($(SANITIZER),address)
    CFLAGS += -fsanitize=address -fno-sanitize-recover=all -fno-omit-frame-pointer
    LDFLAGS += -fsanitize=address -static-libasan
else
ifeq ($(SANITIZER),undefined)
    CFLAGS += -fsanitize=undefined -fno-sanitize-recover=all -fno-omit-frame-pointer
    LDFLAGS += -fsanitize=undefined
else
ifeq ($(SANITIZER),thread)
    CFLAGS += -fsanitize=thread -fno-sanitize-recover=all -fno-omit-frame-pointer
    LDFLAGS += -fsanitize=thread
else
    $(error "unknown sanitizer=${SANITIZER}")
endif
endif
endif
endif

ifneq ($(TRACE),)
    CPPFLAGS += -DENABLE_TRACE
endif
CFLAGS += -g -Wall -Werror $(OPTIMIZATION) -std=c99 -I$(BUILDDIR)/include $(ARCH_CFLAGS) -D_POSIX_C_SOURCE=200112L -D_GNU_SOURCE
LDFLAGS += $(ARCH_LDFLAGS)

LIBS = \
       $(BUILDDIR)/lib/libraft.a \
       $(BUILDDIR)/lib/libhiredis.a \
       -lpthread

OBJECTS = \
	  cluster.o \
	  commands.o \
	  common.o \
	  config.o \
	  connection.o \
	  crc16.o \
	  fsync.o \
	  join.o \
	  log.o \
	  node.o \
	  node_addr.o \
	  proxy.o \
	  raft.o \
	  redisraft.o \
	  serialization.o \
	  snapshot.o \
	  sort.o \
	  threadpool.o \
	  util.o

ifeq ($(COVERAGE),1)
CFLAGS += -fprofile-arcs -ftest-coverage
LIBS += -lgcov
endif

ifeq ($(BUILD_TLS),yes)
	CFLAGS += -DHAVE_TLS
	LIBSSL_PKGCONFIG := $(shell $(PKG_CONFIG) --exists libssl && echo $$?)
ifeq ($(LIBSSL_PKGCONFIG),0)
	LIBSSL_LIBS=$(shell $(PKG_CONFIG) --libs libssl)
else
	LIBSSL_LIBS=-lssl
endif
	LIBCRYPTO_PKGCONFIG := $(shell $(PKG_CONFIG) --exists libcrypto && echo $$?)
ifeq ($(LIBCRYPTO_PKGCONFIG),0)
	LIBCRYPTO_LIBS=$(shell $(PKG_CONFIG) --libs libcrypto)
else
	LIBCRYPTO_LIBS=-lcrypto
endif
	LIBS += $(BUILDDIR)/lib/libhiredis_ssl.a $(LIBSSL_LIBS) $(LIBCRYPTO_LIBS)
endif

.PHONY: all
all: redisraft.so

src/buildinfo.h:
	GIT_SHA1=`(git show-ref --head --hash=8 2>/dev/null || echo 00000000) | head -n1` && \
	echo "#define REDISRAFT_GIT_SHA1 \"$$GIT_SHA1\"" > src/buildinfo.h

OBJECTS := $(addprefix $(BUILDDIR)/src/, $(OBJECTS))

$(OBJECTS): $(BUILDDIR)/%.o : %.c | $(BUILDDIR)/.deps_installed src/buildinfo.h
	$(CC) $(CFLAGS) -c -o $@ $<

redisraft.so: $(OBJECTS)
	$(CC) $(LDFLAGS) -o $@ $(OBJECTS) $(LIBS)

clean: clean-tests
	rm -f redisraft.so buildinfo.h $(OBJECTS) $(TEST_OBJECTS)

cleanall: clean
	rm -rf $(BUILDDIR)
	$(MAKE) -C deps clean PREFIX=$(BUILDDIR)

distclean: cleanall

# ----------------------------- Unit Tests -----------------------------

DUT_CPPFLAGS = $(CPPFLAGS) -include tests/dut_premble.h
ifeq ($(OS),Linux)
    DUT_CFLAGS = $(CFLAGS) -fprofile-arcs -ftest-coverage
    DUT_LIBS = -lgcov
else
    DUT_CFLAGS = $(CFLAGS)
    DUT_LIBS =
endif
TEST_OBJECTS = \
	main.o \
	test_log.o \
	test_util.o \
	test_serialization.o
DUT_OBJECTS = \
	$(patsubst $(BUILDDIR)/src/%.o,$(BUILDDIR)/tests/test-%.o,$(OBJECTS))
TEST_LIBS = $(BUILDDIR)/lib/libcmocka-static.a $(DUT_LIBS) -lpthread -ldl
TEST_OBJECTS := $(addprefix $(BUILDDIR)/tests/, $(TEST_OBJECTS))

.PHONY: clean-tests
clean-tests:
	-rm -rf tests/tests_main $(DUT_OBJECTS) $(TEST_OBJECTS) *.gcno *.gcda tests/*.gcno tests/*.gcda tests/*.gcov tests/*lcov.info tests/.*lcov_html

tests/test-%.o: src/%.c
	$(CC) -c $(DUT_CFLAGS) $(DUT_CPPFLAGS) -o $@ $<

.PHONY: tests
tests: all unit-tests integration-tests

.PHONY: unit-tests
ifeq ($(OS),Linux)
unit-tests: tests/tests_main
	./tests/tests_main && \
		lcov --rc lcov_branch_coverage=1 -c -d . -d ./tests --no-external -o tests/lcov.info && \
		lcov --rc lcov_branch_coverage=1 --summary tests/lcov.info
else
unit-tests: tests/tests_main
	./tests/tests_main
endif

.PHONY: tests/tests_main
$(TEST_OBJECTS): $(BUILDDIR)/%.o : %.c
	$(CC) $(DUT_CFLAGS) $(DUT_CPPFLAGS) -c -o $@ $<

$(DUT_OBJECTS): $(BUILDDIR)/tests/test-%.o : src/%.c
	$(CC) $(DUT_CFLAGS) $(DUT_CPPFLAGS) -c -o $@ $<

tests/tests_main: $(TEST_OBJECTS) $(DUT_OBJECTS)
	$(CC) -o tests/tests_main $(TEST_OBJECTS) $(DUT_OBJECTS) $(LIBS) $(TEST_LIBS)

.PHONY: unit-lcov-report
unit-lcov-report: tests/lcov.info
	mkdir -p tests/.lcov_html
	genhtml --branch-coverage -o tests/.lcov_html tests/lcov.info
	xdg-open tests/.lcov_html/index.html >/dev/null 2>&1

# ----------------------------- Integration Tests -----------------------------

PYTEST_OPTS ?= -v

.PHONY: integration-tests
integration-tests:
	pytest tests/integration $(PYTEST_OPTS)

.PHONY: valgrind-tests
valgrind-tests:
	pytest tests/integration $(PYTEST_OPTS) --valgrind

.PHONY: integration-lcov-report
integration-lcov-report:
	lcov --rc lcov_branch_coverage=1 -c -d . --no-external -o tests/integration-lcov.info && \
	lcov --rc lcov_branch_coverage=1 --summary tests/integration-lcov.info
	mkdir -p tests/.integration-lcov_html
	genhtml --branch-coverage -o tests/.integration-lcov_html tests/integration-lcov.info
	xdg-open tests/.integration-lcov_html/index.html >/dev/null 2>&1

# ------------------------- Build dependencies -------------------------

$(BUILDDIR)/.deps_installed:
	mkdir -p $(BUILDDIR)
	mkdir -p $(BUILDDIR)/lib
	mkdir -p $(BUILDDIR)/include
	mkdir -p $(BUILDDIR)/src
	mkdir -p $(BUILDDIR)/tests
	$(MAKE) -C deps PREFIX=$(BUILDDIR)
	touch $(BUILDDIR)/.deps_installed
