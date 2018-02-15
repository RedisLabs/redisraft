ifeq ($(PREFIX),)
    $(error PREFIX was not specified)
endif

all:	libuv hiredis raft tpl

clean:
	$(MAKE) -C hiredis clean
	$(MAKE) -C libuv distclean
	$(MAKE) -C raft clean

##

.PHONY: hiredis
hiredis: hiredis/Makefile
	$(MAKE) -C hiredis && $(MAKE) -C hiredis install PREFIX=$(PREFIX)

##

.PHONY: libuv
libuv: libuv/Makefile
	$(MAKE) -C libuv && $(MAKE) -C libuv install

libuv/Makefile: libuv/configure
	cd libuv && ./configure --with-pic --prefix=$(PREFIX)

libuv/configure: libuv/autogen.sh
	cd libuv && ./autogen.sh

##

.PHONY: raft
raft: raft/Makefile
	cd raft && \
	    make static GCOV_CCFLAGS= && \
	    cp libraft.a $(PREFIX)/lib && \
	    cp include/raft.h $(PREFIX)/include

##

.PHONY: tpl
tpl: tpl/Makefile
	$(MAKE) -C tpl && $(MAKE) -C tpl install

tpl/Makefile: tpl/configure
	cd tpl && ./configure --with-pic --prefix=$(PREFIX)

tpl/configure: tpl/bootstrap
	cd tpl && ./bootstrap

