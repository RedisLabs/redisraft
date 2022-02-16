ifeq ($(PREFIX),)
    $(error PREFIX was not specified)
endif

all:	hiredis raft cmocka

clean:
	-$(MAKE) -C hiredis clean
	-$(MAKE) -C raft clean
	-rm -rf cmocka/build

##

USE_SSL ?= 0
ifeq ($(BUILD_TLS),yes)
	USE_SSL=1
endif

.PHONY: hiredis
hiredis: hiredis/Makefile
	$(MAKE) -C hiredis && $(MAKE) USE_SSL=$(USE_SSL) -C hiredis install PREFIX=$(PREFIX)

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
