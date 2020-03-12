ifeq ($(PREFIX),)
    $(error PREFIX was not specified)
endif

all:	libuv hiredis raft cmocka

clean:
	-$(MAKE) -C hiredis clean
	-$(MAKE) -C libuv distclean
	-$(MAKE) -C raft clean
	-rm -rf cmocka/build

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
	    make static GCOV_CCFLAGS= ASANFLAGS= && \
	    cp libraft.a $(PREFIX)/lib && \
	    cp include/raft.h include/raft_types.h $(PREFIX)/include

##

.PHONY: cmocka
cmocka: cmocka/build/Makefile
	cd cmocka/build && \
	    make && \
	    make install

cmocka/build/Makefile:
	mkdir -p cmocka/build
	cd cmocka/build && \
	    cmake -DWITH_STATIC_LIB=true -DCMAKE_INSTALL_PREFIX=$(PREFIX) ..
