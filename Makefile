
CFLAGS = -g -std=c99 -I../raft/include -I$(HOME)/.local/include -fPIC
LDFLAGS = -shared
LIBS = ../raft/libraft.a $(HOME)/.local/lib/libuv.a -lpthread

rafter.so: rafter.o
	$(LD) $(LDFLAGS) -o $@ $< $(LIBS)

clean:
	rm -f rafter.so rafter.o
