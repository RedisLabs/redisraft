import sys
import time
import sandbox
from nose.tools import eq_, ok_
from test_tools import with_setup_args

def _setup():
    return [sandbox.Cluster()], {}

def _teardown(c):
    c.destroy()

@with_setup_args(_setup, _teardown)
def test_compaction_thresholds(c):
    """
    Log compaction behaves according to configuration
    """

    r1 = c.add_node()
    eq_(r1.raft_info()['log_entries'], 1)
    ok_(r1.raft_config_set('max_log_entries', 5))
    for x in range(10):
        ok_(r1.raft_exec('SET', 'testkey', x))
    time.sleep(1)
    eq_(r1.raft_info()['log_entries'], 5)
