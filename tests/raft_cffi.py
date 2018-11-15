import cffi
import subprocess

ffi = cffi.FFI()
ffi.set_source(
    "tests",
    """
        #include "raft.h"
        raft_entry_t *raft_entry_newdata(void *data) {
            raft_entry_t *e = raft_entry_new(sizeof(void *));
            *(void **) e->data = data;
            e->refs = 100;
            return e;
        }
        void *raft_entry_getdata(raft_entry_t *ety) {
            return *(void **) ety->data;
        }
        raft_entry_t **raft_entry_array_deepcopy(raft_entry_t **src, int len) {
            raft_entry_t **t = malloc(len * sizeof(raft_entry_t *));
            int i;
            for (i = 0; i < len; i++) {
                int sz = sizeof(raft_entry_t) + src[i]->data_len;
                t[i] = malloc(sz);
                memcpy(t[i], src[i], sz);
            }
            return t;
        }
    """,
    sources="""
        src/raft_log.c
        src/raft_server.c
        src/raft_server_properties.c
        src/raft_node.c
        """.split(),
    include_dirs=["include"],
    extra_compile_args=["-UNDEBUG"]
    )
library = ffi.compile()

ffi = cffi.FFI()
lib = ffi.dlopen(library)

def load(fname):
    return '\n'.join(
        [line for line in subprocess.check_output(
            ["gcc", "-E", fname]).decode('utf-8').split('\n')])


ffi.cdef('void *malloc(size_t __size);')
ffi.cdef(load('include/raft.h'))
ffi.cdef(load('include/raft_log.h'))

ffi.cdef('raft_entry_t *raft_entry_newdata(void *data);')
ffi.cdef('void *raft_entry_getdata(raft_entry_t *);')
ffi.cdef('raft_entry_t **raft_entry_array_deepcopy(raft_entry_t **src, int len);')
