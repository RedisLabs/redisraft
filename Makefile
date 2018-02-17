
BUILDDIR := $(CURDIR)/.build
CFLAGS = -g -std=c99 -I$(BUILDDIR)/include -fPIC
LDFLAGS = -shared
LIBS = $(BUILDDIR)/lib/libraft.a $(BUILDDIR)/lib/libuv.a -lpthread

OBJECTS = redisraft.o

redisraft.so: deps $(OBJECTS)
	$(LD) $(LDFLAGS) -o $@ $(OBJECTS) $(LIBS)

clean:
	rm -f redisraft.so redisraft.o

cleanall: clean
	rm -rf $(BUILDDIR)
	$(MAKE) -C deps clean PREFIX=$(BUILDDIR)

.PHONY: deps
deps: $(BUILDDIR)/.deps_installed

$(BUILDDIR)/.deps_installed:
	mkdir -p $(BUILDDIR)
	$(MAKE) -C deps PREFIX=$(BUILDDIR)
	touch $(BUILDDIR)/.deps_installed
