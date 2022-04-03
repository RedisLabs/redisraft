import subprocess
import unittest

import raft_cffi

from hypothesis import given
from hypothesis.strategies import lists, just, integers, one_of


commands = one_of(
    just('append'),
    just('poll'),
    integers(min_value=1, max_value=10),
)


class Log(object):
    def __init__(self):
        self.entries = []
        self.base = 0

    def append(self, ety):
        self.entries.append(ety)

    def poll(self):
        self.base += 1
        return self.entries.pop(0)

    def delete(self, idx):
        idx -= 1
        if idx < self.base:
            idx = self.base
        idx = max(idx - self.base, 0)
        del self.entries[idx:]

    def count(self):
        return len(self.entries)


class CoreTestCase(unittest.TestCase):
    def setUp(self):
        super(CoreTestCase, self).setUp()
        self.r = raft_cffi

    @given(lists(commands))
    def test_sanity_check(self, commands):
        r = self.r.lib

        unique_id = 1
        l = r.raft_log_alloc(1)

        log = Log()

        for cmd in commands:
            if cmd == 'append':
                entry = r.raft_entry_new(0)
                entry.id = unique_id
                unique_id += 1

                ret = r.raft_log_append_entry(l, entry)
                assert ret == 0

                log.append(entry)

            elif cmd == 'poll':
                entry_ptr = self.r.ffi.new('raft_entry_t**')

                if log.entries:
                    ret = r.raft_log_poll(l, entry_ptr)
                    assert ret == 0

                    ety_expected = log.poll()
                    assert entry_ptr[0].id == ety_expected.id

            elif isinstance(cmd, int):
                if log.entries:
                    log.delete(cmd)
                    ret = r.raft_log_delete(l, cmd)
                    assert ret == 0

            else:
                assert False

            self.assertEqual(r.raft_log_count(l), log.count())


if __name__ == '__main__':
    unittest.main()
