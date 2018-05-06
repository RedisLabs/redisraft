OS := $(shell sh -c 'uname -s 2>/dev/null || echo none')
BUILDDIR := $(CURDIR)/.build

ifeq ($(OS),Linux)
    ARCH_CFLAGS := -fPIC
    ARCH_LDFLAGS := -shared -Bsymbolic-functions
else
    ARCH_CFLAGS := -dynamic
    ARCH_LDFLAGS := -bundle -undefined dynamic_lookup
endif

CPPFLAGS = -D_POSIX_C_SOURCE=200112L -D_GNU_SOURCE # -DUSE_COMMAND_FILTER
CFLAGS = -g -std=c99 -I$(BUILDDIR)/include $(ARCH_CFLAGS)
LDFLAGS = $(ARCH_LDFLAGS)

LIBS = \
       $(BUILDDIR)/lib/libraft.a \
       $(BUILDDIR)/lib/libhiredis.a \
       $(BUILDDIR)/lib/libuv.a \
       -lpthread

OBJECTS = \
	  redisraft.o \
	  node.o \
	  util.o \
	  config.o \
	  raft.o \
	  snapshot.o \
	  log.o

redisraft.so: deps $(OBJECTS)
	$(LD) $(LDFLAGS) -o $@ $(OBJECTS) $(LIBS)

clean:	clean-tests
	rm -f redisraft.so $(OBJECTS)

cleanall: clean
	rm -rf $(BUILDDIR)
	$(MAKE) -C deps clean PREFIX=$(BUILDDIR)

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
	tests/main.o \
	tests/test_log.o \
	tests/test_util.o
DUT_OBJECTS = \
	$(patsubst %.o,tests/test-%.o,$(OBJECTS))
TEST_LIBS = $(BUILDDIR)/lib/libcmocka.a $(DUT_LIBS) -lpthread

.PHONY: clean-tests
clean-tests:
	-rm -rf tests/tests_main $(DUT_OBJECTS) $(TEST_OBJECTS) tests/*.gcno tests/*.gcda tests/*.gcov tests/lcov.info tests/.lcov_html

tests/test-%.o: %.c
	$(CC) -c $(DUT_CFLAGS) $(DUT_CPPFLAGS) -o $@ $<

.PHONY: tests
tests: unit-tests integration-tests

.PHONY: integration-tests
integration-tests:
	PATH=../redis/src:${PATH} nosetests tests/integration -v

.PHONY: valgrind-tests
valgrind-tests:
	PATH=../redis/src:${PATH} SANDBOX_CONFIG=ValgrindConfig nosetests tests/integration -v

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

.PHONY: lcov-report
lcov-report: tests/lcov.info
	mkdir -p tests/.lcov_html
	genhtml --branch-coverage -o tests/.lcov_html tests/lcov.info
	xdg-open tests/.lcov_html/index.html >/dev/null 2>&1

.PHONY: tests/tests_main
tests/tests_main: $(TEST_OBJECTS) $(DUT_OBJECTS)
	$(CC) -o tests/tests_main $(TEST_OBJECTS) $(DUT_OBJECTS) $(LIBS) $(TEST_LIBS)

# ------------------------- Build dependencies -------------------------

.PHONY: deps
deps: $(BUILDDIR)/.deps_installed

$(BUILDDIR)/.deps_installed:
	mkdir -p $(BUILDDIR)
	$(MAKE) -C deps PREFIX=$(BUILDDIR)
	touch $(BUILDDIR)/.deps_installed
