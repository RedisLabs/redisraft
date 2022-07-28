import argparse
import os
import subprocess
import cffi


def load(fname):
    tmpfile = 'tests/raft_cffi_tmp.h'
    # Strip C standard library headers as cffi cannot parse them
    with open(fname, "r") as f:
        lines = f.readlines()
    with open(tmpfile, "w+") as f:
        for line in lines:
            if '#include <std' not in line:
                f.write(line)

    output = subprocess.check_output(["gcc", "-Iinclude/", "-E", tmpfile])
    os.unlink(tmpfile)
    return '\n'.join([line for line in output.decode('utf-8').split('\n')])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--libdir', type=str, default='.')
    parser.add_argument('--libname', type=str, default='raft')
    parser.add_argument('--includedir', type=str, default='include')
    parser.add_argument('--tmpdir', type=str, default='.')
    args = parser.parse_args()

    ffibuilder = cffi.FFI()
    ffibuilder.set_source(
        "tests.raft_cffi",
        """
            #include "raft.h"
            #include "raft_private.h"
            #include "raft_log.h"

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
        libraries=[args.libname],
        include_dirs=[args.includedir],
        extra_compile_args=["-UNDEBUG"],
        extra_link_args=["-L{}".format(args.libdir)]
    )

    ffibuilder.cdef('void *malloc(size_t __size);')
    ffibuilder.cdef(load('include/raft.h'))
    ffibuilder.cdef(load('include/raft_private.h'))
    ffibuilder.cdef(load('include/raft_log.h'))

    ffibuilder.cdef('raft_entry_t *raft_entry_newdata(void *data);')
    ffibuilder.cdef('void *raft_entry_getdata(raft_entry_t *);')
    ffibuilder.cdef('raft_entry_t **raft_entry_array_deepcopy(raft_entry_t **src, int len);')

    ffibuilder.compile(tmpdir=args.tmpdir, verbose=True)
