#!/usr/bin/env python
"""
virtraft2 - Simulate a raft network

Some quality checks we do:
 - Log Matching (servers must have matching logs)
 - State Machine Safety (applied entries have the same ID)
 - Election Safety (only one valid leader per term)
 - Current Index Validity (does current index have an existing entry?)
 - Entry ID Monotonicity (entries aren't appended out of order)
 - Committed entry popping (committed entries are not popped from the log)
 - Log Accuracy (does the server's log match mirror an independent log?)
 - Deadlock detection (does the cluster continuously make progress?)

Some chaos we generate:
 - Random bi-directional partitions between nodes
 - Message dropping
 - Message duplication
 - Message re-ordering

Usage:
  virtraft --servers SERVERS [-d RATE] [-D RATE] [-c RATE] [-C RATE] [-m RATE]
                             [-P RATE] [-s SEED] [-i ITERS] [-p] [--tsv] [--rqm MULTI]
                             [-q] [-v] [-l LEVEL] [-j] [-L LOGFILE] [--duplex_partition]
                             [--auto_flush]
  virtraft --version
  virtraft --help

Options:
  -n --servers SERVERS       Number of servers
  -d --drop_rate RATE        Message drop rate 0-100 [default: 0]
  -D --dupe_rate RATE        Message duplication rate 0-100 [default: 0]
  -c --client_rate RATE      Rate entries are received from the client 0-100
                             [default: 100]
  -C --compaction_rate RATE  Rate that log compactions occur 0-100 [default: 0]
  -m --member_rate RATE      Membership change rate 0-100000 [default: 0]
  -P --partition_rate RATE   Rate that partitions occur or are healed 0-100
                             [default: 0]
  -p --no_random_period      Don't use a random period
  -s --seed SEED             The simulation's seed [default: 0]
  -q --quiet                 No output at end of run
  -i --iterations ITERS      Number of iterations before the simulation ends
                             [default: 1000]
  --tsv                      Output node status tab separated values at exit
  --rqm MULTI                multiplier to use for node_id in read_queue test [default: 10000000000]
  -v --verbose               Show debug logs
  -j --json_output           JSON output for diagnostics
  -l --log_level LEVEL       Set log level
  -L --log_file LOGFILE      Set log file
  -V --version               Display version.
  -h --help                  Prints a short usage summary.
  --duplex_partition         On partition, prevent traffic from flowing in both directions
  --auto_flush               Use libraft with auto_flush option

Examples:

  Output a node status table:
    virtraft --servers 3 --iterations 1000 --tsv | column -t
"""
import collections
import colorama
import coloredlogs
import docopt
import logging
import random
import sys
import terminaltables
import traceback
import os

from raft_cffi import ffi, lib


logger = logging.getLogger('raft')


NODE_DISCONNECTED = 0
NODE_CONNECTING = 1
NODE_CONNECTED = 2
NODE_DISCONNECTING = 3

SNAPSHOT_SIZE = 41 * 1023

class ServerDoesNotExist(Exception):
    pass


def logtype2str(log_type):
    if log_type == lib.RAFT_LOGTYPE_NORMAL:
        return 'normal'
    elif log_type == lib.RAFT_LOGTYPE_REMOVE_NODE:
        return 'remove'
    elif log_type == lib.RAFT_LOGTYPE_ADD_NONVOTING_NODE:
        return 'add_nonvoting'
    elif log_type == lib.RAFT_LOGTYPE_ADD_NODE:
        return 'add'
    elif log_type == lib.RAFT_LOGTYPE_NO_OP:
        return 'no_op'
    else:
        return 'unknown'


def state2str(state):
    if state == lib.RAFT_STATE_LEADER:
        return colorama.Fore.GREEN + 'leader' + colorama.Style.RESET_ALL
    elif state == lib.RAFT_STATE_CANDIDATE:
        return 'candidate'
    elif state == lib.RAFT_STATE_PRECANDIDATE:
        return 'pre-candidate'
    elif state == lib.RAFT_STATE_FOLLOWER:
        return 'follower'
    else:
        return 'unknown'


def connectstatus2str(connectstatus):
    return {
        NODE_DISCONNECTED: colorama.Fore.RED + 'DISCONNECTED' + colorama.Style.RESET_ALL,
        NODE_CONNECTING: 'CONNECTING',
        NODE_CONNECTED: colorama.Fore.GREEN + 'CONNECTED' + colorama.Style.RESET_ALL,
        NODE_DISCONNECTING: colorama.Fore.YELLOW + 'DISCONNECTING' + colorama.Style.RESET_ALL,
    }[connectstatus]


def err2str(err):
    return {
        lib.RAFT_ERR_NOT_LEADER: 'RAFT_ERR_NOT_LEADER',
        lib.RAFT_ERR_ONE_VOTING_CHANGE_ONLY: 'RAFT_ERR_ONE_VOTING_CHANGE_ONLY',
        lib.RAFT_ERR_SHUTDOWN: 'RAFT_ERR_SHUTDOWN',
        lib.RAFT_ERR_NOMEM: 'RAFT_ERR_NOMEM',
        lib.RAFT_ERR_SNAPSHOT_IN_PROGRESS: 'RAFT_ERR_SNAPSHOT_IN_PROGRESS',
        lib.RAFT_ERR_SNAPSHOT_ALREADY_LOADED: 'RAFT_ERR_SNAPSHOT_ALREADY_LOADED',
        lib.RAFT_INVALID_NODEID: 'RAFT_INVALID_NODEID',
        lib.RAFT_ERR_LEADER_TRANSFER_IN_PROGRESS: 'RAFT_ERR_LEADER_TRANSFER_IN_PROGRESS',
        lib.RAFT_ERR_DONE: 'RAFT_ERR_DONE',
        lib.RAFT_ERR_LAST: 'RAFT_ERR_LAST',
    }[err]


class ChangeRaftEntry(object):
    def __init__(self, node_id):
        self.node_id = node_id


class SetRaftEntry(object):
    def __init__(self, key, val):
        self.key = key
        self.val = val


class RaftEntry(object):
    def __init__(self, entry):
        self.term = entry.term
        self.id = entry.id


class Snapshot(object):
    def __init__(self):
        self.members = []


SnapshotMember = collections.namedtuple('SnapshotMember', ['id', 'voting'])


def raft_send_requestvote(raft, udata, node, msg):
    server = ffi.from_handle(udata)
    dst_server = ffi.from_handle(lib.raft_node_get_udata(node))
    server.network.enqueue_msg(msg, server, dst_server)
    return 0


def raft_send_appendentries(raft, udata, node, msg):
    server = ffi.from_handle(udata)
    assert node
    dst_server = ffi.from_handle(lib.raft_node_get_udata(node))
    server.network.enqueue_msg(msg, server, dst_server)

    # Collect statistics
    if server.network.max_entries_in_ae < msg.n_entries:
        server.network.max_entries_in_ae = msg.n_entries

    return 0


def raft_send_snapshot(raft, udata, node, msg):
    server = ffi.from_handle(udata)
    dst_server = ffi.from_handle(lib.raft_node_get_udata(node))
    server.network.enqueue_msg(msg, server, dst_server)
    return 0


def raft_load_snapshot(raft, udata, term, index):
    return ffi.from_handle(udata).load_snapshot(term, index)


def raft_clear_snapshot(raft, udata):
    return ffi.from_handle(udata).clear_snapshot()


def raft_get_snapshot_chunk(raft, udata, node, offset, chunk):
    server = ffi.from_handle(udata)
    chunk.len = min(4096, len(server.snapshot_buf) - offset)
    chunk.last_chunk = offset + chunk.len == len(server.snapshot_buf)
    chunk.data = ffi.from_buffer("char*", server.snapshot_buf, 0) + offset

    target_node = ffi.from_handle(lib.raft_node_get_udata(node))

    # If receiver node has more than 8 messages to be consumed, apply some
    # backpressure by returning RAFT_ERR_DONE
    count = 0
    for message in target_node.network.messages:
        if message.sendee == target_node:
            count += 1

    if count > 8 or chunk.len == 0:
        return lib.RAFT_ERR_DONE

    return 0


def raft_store_snapshot_chunk(raft, udata, index, offset, chunk):
    buf = ffi.buffer(chunk.data, chunk.len)
    return ffi.from_handle(udata).store_snapshot(offset, buf)


def raft_applylog(raft, udata, ety, idx):
    try:
        return ffi.from_handle(udata).entry_apply(ety, idx)
    except:
        logger.error(f"raft_applylog: failure on {lib.raft_get_nodeid(raft)}")
        logger.error(traceback.format_exc())
        # we really should want to exit here, but can't figure out how to
        return lib.RAFT_ERR_SHUTDOWN


def raft_persist_metadata(raft, udata, term, vote):
    return ffi.from_handle(udata).persist_metadata(term, vote)


def raft_logentry_offer(raft, udata, ety, ety_idx):
    return ffi.from_handle(udata).entry_append(ety, ety_idx)


def raft_logentry_poll(raft, udata, ety, ety_idx):
    return ffi.from_handle(udata).entry_poll(ety, ety_idx)


def raft_logentry_pop(raft, udata, ety, ety_idx):
    return ffi.from_handle(udata).entry_pop(ety, ety_idx)


def raft_get_node_id(raft, udata, ety, ety_idx):
    change_entry = ffi.from_handle(lib.raft_entry_getdata(ety))
    assert isinstance(change_entry, ChangeRaftEntry)
    return change_entry.node_id


def raft_node_has_sufficient_logs(raft, udata, node):
    return ffi.from_handle(udata).node_has_sufficient_entries(node)


def raft_notify_membership_event(raft, udata, node, ety, event_type):
    return ffi.from_handle(udata).notify_membership_event(node, ety, event_type)


def find_leader():
    max_term = -1
    leader = None
    for s in net.servers:
        if lib.raft_is_leader(s.raft):
            term = lib.raft_get_current_term(s.raft)
            if term > max_term:
                max_term = term
                leader = s

    return leader


def get_voting_node_ids(server):
    voting_nodes_ids = []

    for i in range(0, lib.raft_get_num_nodes(server.raft)):
        node = lib.raft_get_node_from_idx(server.raft, i)
        if node == ffi.NULL:
            continue

        if lib.raft_node_is_voting(node) != 0:
            voting_nodes_ids.append(lib.raft_node_get_id(node))

    return voting_nodes_ids


def verify_read(arg):
    node_id = int(arg / net.rqm)
    arg = arg % net.rqm
    leader = net.servers[node_id - 1]

    voter_ids = get_voting_node_ids(leader)
    num_nodes = len(voter_ids)

    # primary verification logic.  we always need to count more than the required, looking to see that for voters,
    # the read_queue_id they have recorded is equal to our greater than the arg (i.e. the read_queue id this was sent on
    required = int(num_nodes / 2) + 1
    count = 0

    for i in voter_ids:
        if net.servers[i-1] == leader:
            count += 1
            continue

        node = lib.raft_get_node(net.servers[i-1].raft, leader.id)
        if node == ffi.NULL:
            continue

        msg_id = lib.raft_node_get_max_seen_msg_id(node)
        if msg_id >= arg:
            count += 1

    if count < required:
        logger.error(f"verify_read failed {count} < {required}")
        os._exit(-1)


def handle_read_queue(arg, can_read):
    val = int(ffi.cast("long", arg))
    if can_read != 0:
        verify_read(val)
        logger.debug(f"handling read_request {val}")
        net.last_seen_read_queue_msg_id = val
    else:
        logger.debug(f"ignoring read_request {val}")


def raft_log(raft, udata, buf):
    server = ffi.from_handle(lib.raft_get_udata(raft))
    # if server.id in [1] or (node and node.id in [1]):
    logger.info('{0}>  {1}: {2}'.format(
        server.network.iteration,
        server.id,
        ffi.string(buf).decode('utf8'),
    ))


class Message(object):
    def __init__(self, msg, sendor, sendee):
        self.data = msg
        self.sendor = sendor
        self.sendee = sendee


class Network(object):
    def __init__(self, seed=0):
        self.servers = []
        self.messages = []
        self.drop_rate = 0
        self.dupe_rate = 0
        self.partition_rate = 0
        self.iteration = 0
        self.leader = -1
        self.ety_id = 0
        self.entries = []
        self.random = random.Random(seed)
        self.partitions = set()
        self.no_random_period = False
        self.duplex_partition = False
        self.auto_flush = False
        self.last_seen_read_queue_msg_id = -1
        self.rqm = 10000000000

        self.server_id = 0

        # Information
        self.max_entries_in_ae = 0
        self.leadership_changes = 0
        self.log_pops = 0
        self.num_unique_nodes = 0
        self.num_membership_changes = 0
        self.num_compactions = 0
        self.latest_applied_log_idx = 0

    def add_entry(self, etype, data):
        data_handle = ffi.new_handle(data)
        ety = lib.raft_entry_newdata(data_handle)
        ety.term = 0
        ety.id = self.new_entry_id()
        ety.type = etype
        self.entries.append((ety, data_handle, data))
        return ety

    def add_server(self, server):
        self.server_id += 1
        server.id = self.server_id
        assert server.id not in set([s.id for s in self.servers])
        server.set_network(self)
        self.servers.append(server)

    def push_set_entry(self, k, v):
        for sv in self.active_servers:
            if lib.raft_is_leader(sv.raft):
                ety = self.add_entry(lib.RAFT_LOGTYPE_NORMAL,
                                     SetRaftEntry(k, v))
                e = sv.recv_entry(ety)
                assert e == 0 or e == lib.RAFT_ERR_NOT_LEADER
                break

    def push_read(self):
        for sv in self.active_servers:
            if lib.raft_is_leader(sv.raft):
                msg_id = lib.raft_get_msg_id(sv.raft) + 1
                arg = sv.id * net.rqm + msg_id
                lib.raft_recv_read_request(sv.raft, sv.handle_read_queue, ffi.cast("void *", arg))

    def id2server(self, id):
        for server in self.servers:
            if server.id == id:
                return server
            # if lib.raft_get_nodeid(server.raft) == id:
            #     return server
        raise ServerDoesNotExist('Could not find server: {}'.format(id))

    def add_partition(self):
        if len(self.active_servers) <= 1:
            return

        nodes = list(self.active_servers)
        self.random.shuffle(nodes)
        n1 = nodes.pop()
        n2 = nodes.pop()
        logger.debug(f"adding partition from {n1.id} to {n2.id}")

        self.partitions.add((n1, n2))

    def remove_partition(self):
        parts = sorted(list(self.partitions))
        self.random.shuffle(parts)
        part = parts.pop()
        logger.debug(f"removing partition from {part[0].id} to {part[1].id}")
        self.partitions.remove(part)

    def periodic(self):
        if self.random.randint(1, 100) < self.member_rate:
            if 20 < self.random.randint(1, 100):
                self.add_member()
            else:
                self.remove_member()

        if self.random.randint(1, 100) < self.partition_rate:
            self.add_partition()

        if self.partitions and self.random.randint(1, 100) < self.partition_rate:
            self.remove_partition()

        if self.random.randint(1, 100) < self.client_rate:
            self.push_set_entry(self.random.randint(1, 10), self.random.randint(1, 10))

        for server in self.active_servers:
            if self.no_random_period:
                server.periodic(100)
            else:
                server.periodic(self.random.randint(1, 100))

            if not self.auto_flush:
                # Pretend like async disk write operation is completed. Also,
                # call raft_flush() often to trigger sending appendentries.
                idx = lib.raft_get_index_to_sync(server.raft)
                assert lib.raft_flush(server.raft, idx) == 0

        # Deadlock detection
        if self.client_rate != 0 and self.latest_applied_log_idx != 0 and self.latest_applied_log_iteration + 5000 < self.iteration:
            logger.error("deadlock detected iteration:{0} appliedidx:{1}\n".format(
                self.latest_applied_log_iteration,
                self.latest_applied_log_idx,
                ))
            self.diagnostic_info()
            sys.exit(1)

        leader = find_leader()
        if leader and self.last_seen_read_queue_msg_id + 5000 < lib.raft_get_msg_id(self.servers[leader.id - 1].raft):
            logger.error("deadlock detected in handling reads: last seen read queue msg_id {0} current at {1}, iter: {2}\n".format(
                self.last_seen_read_queue_msg_id, lib.raft_get_msg_id(leader.raft), self.iteration,
            ))
            self.diagnostic_info()
            sys.exit(1)

        # Count leadership changes
        assert len(self.active_servers) > 0, self.diagnostic_info()
        leader = lib.raft_get_leader_id(self.active_servers[0].raft)
        if leader != -1:
            if self.leader != leader:
                self.leadership_changes += 1
                logger.info(f"old leader: {self.leader}")
                logger.info(f"leader change: {leader}")

                self.diagnostic_info()
            self.leader = leader


    def enqueue_msg(self, msg, sendor, sendee):
        logger.debug(f"enqueue_msg: {sendor.id} to {sendee.id}")
        # Drop message if this edge is partitioned
        for partition in self.partitions:
            # Partitions are in one direction
            if partition[0] is sendor and partition[1] is sendee:
                logger.info(f"dropping message from {sendor.id} to {sendee.id}")
                return
            # duplex partitions prevent both directions from sending traffic
            if self.duplex_partition and (
                        (partition[0] is sendor and partition[1] is sendee) or
                        (partition[1] is sendor and partition[0] is sendee)
                ):
                return

        if self.random.randint(1, 100) < self.drop_rate:
            return

        while self.random.randint(1, 100) < self.dupe_rate:
            self._enqueue_msg(msg, sendor, sendee)

        self._enqueue_msg(msg, sendor, sendee)

    def _enqueue_msg(self, msg, sendor, sendee):
        msg_type = ffi.getctype(ffi.typeof(msg))

        msg_size = ffi.sizeof(msg[0])
        new_msg = ffi.cast(ffi.typeof(msg), lib.malloc(msg_size))
        ffi.memmove(new_msg, msg, msg_size)

        if msg_type == 'raft_appendentries_req_t *':
            new_msg.entries = lib.raft_entry_array_deepcopy(msg.entries, msg.n_entries)

        self.messages.append(Message(new_msg, sendor, sendee))

    def poll_message(self, msg):
        msg_type = ffi.getctype(ffi.typeof(msg.data))

        logger.debug(f"poll_message: {msg.sendor.id} -> {msg.sendee.id} ({msg_type}")

        if msg_type == 'raft_appendentries_req_t *':
            node = lib.raft_get_node(msg.sendee.raft, msg.sendor.id)
            response = ffi.new('raft_appendentries_resp_t*')
            e = lib.raft_recv_appendentries(msg.sendee.raft, node, msg.data, response)
            if lib.RAFT_ERR_SHUTDOWN == e:
                logger.error('Catastrophic')
                logger.error(msg.sendor.debug_log())
                logger.error(msg.sendee.debug_log())
                self.diagnostic_info()
                sys.exit(1)
            else:
                self.enqueue_msg(response, msg.sendee, msg.sendor)

        elif msg_type == 'raft_appendentries_resp_t *':
            node = lib.raft_get_node(msg.sendee.raft, msg.sendor.id)
            lib.raft_recv_appendentries_response(msg.sendee.raft, node, msg.data)

        elif msg_type == 'raft_snapshot_req_t *':
            response = ffi.new('raft_snapshot_resp_t *')
            node = lib.raft_get_node(msg.sendee.raft, msg.sendor.id)
            lib.raft_recv_snapshot(msg.sendee.raft, node, msg.data, response)
            self.enqueue_msg(response, msg.sendee, msg.sendor)

        elif msg_type == 'raft_snapshot_resp_t *':
            node = lib.raft_get_node(msg.sendee.raft, msg.sendor.id)
            lib.raft_recv_snapshot_response(msg.sendee.raft, node, msg.data)

        elif msg_type == 'raft_requestvote_req_t *':
            response = ffi.new('raft_requestvote_resp_t*')
            node = lib.raft_get_node(msg.sendee.raft, msg.sendor.id)
            lib.raft_recv_requestvote(msg.sendee.raft, node, msg.data, response)
            self.enqueue_msg(response, msg.sendee, msg.sendor)

        elif msg_type == 'raft_requestvote_resp_t *':
            node = lib.raft_get_node(msg.sendee.raft, msg.sendor.id)
            e = lib.raft_recv_requestvote_response(msg.sendee.raft, node, msg.data)
            if lib.RAFT_ERR_SHUTDOWN == e:
                logger.debug(f"shutting down {msg.sendee.id} because of bad requestvote?")
                msg.sendor.shutdown()

        else:
            assert False

    def poll_messages(self):
        msgs = self.messages

        # Chaos: re-ordered messages
        # self.random.shuffle(msgs)

        self.messages = []
        for msg in msgs:
            self.poll_message(msg)
            for server in self.active_servers:
                if hasattr(server, 'abort_exception'):
                    raise server.abort_exception
                self._check_current_idx_validity(server)
            self._check_election_safety()

    def _check_current_idx_validity(self, server):
        """
        Check that current idx is valid, ie. it exists
        """
        ci = lib.raft_get_current_idx(server.raft)
        if 0 < ci and not lib.raft_get_snapshot_last_idx(server.raft) == ci:
            ety = lib.raft_get_entry_from_idx(server.raft, ci)
            try:
                assert ety
                lib.raft_entry_release(ety)
            except Exception:
                print('current idx ', ci)
                print('count', lib.raft_get_log_count(server.raft))
                print('last snapshot', lib.raft_get_snapshot_last_idx(server.raft))
                print(server.debug_log())
                raise

    def _check_election_safety(self):
        """
        FIXME: this is O(n^2)
        Election Safety
        At most one leader can be elected in a given term.
        """

        for i, sv1 in enumerate(net.active_servers):
            if not lib.raft_is_leader(sv1.raft):
                continue
            for sv2 in net.active_servers[i + 1:]:
                term1 = lib.raft_get_current_term(sv1.raft)
                term2 = lib.raft_get_current_term(sv2.raft)
                if lib.raft_is_leader(sv2.raft) and term1 == term2:
                    logger.error("election safety invalidated")
                    print(sv1, sv2, term1)
                    print('partitions:', self.partitions)
                    self.diagnostic_info()
                    sys.exit(1)

    def commit_static_configuration(self):
        for server in net.active_servers:
            server.connection_status = NODE_CONNECTED
            for sv in net.active_servers:
                is_self = 1 if sv.id == server.id else 0
                node = lib.raft_add_node(server.raft, sv.udata, sv.id, is_self)

                # FIXME: it's a bit much to expect to set these too
                lib.raft_node_set_voting_committed(node, 1)
                lib.raft_node_set_addition_committed(node, 1)
                lib.raft_node_set_active(node, 1)

    def prep_dynamic_configuration(self):
        """
        Add configuration change for leader's node
        """
        server = self.active_servers[0]
        self.leader = server.id

        server.set_connection_status(NODE_CONNECTING)
        lib.raft_add_non_voting_node(server.raft, server.udata, server.id, 1)
        lib.raft_become_leader(server.raft)
        e = lib.raft_set_current_term(server.raft, 1)
        assert e == 0

        # Configuration change entry to bootstrap other nodes
        ety = self.add_entry(lib.RAFT_LOGTYPE_ADD_NODE,
                             ChangeRaftEntry(server.id))
        e = server.recv_entry(ety)
        assert e == 0

        lib.raft_set_commit_idx(server.raft, 2)
        e = lib.raft_apply_all(server.raft)
        assert e == 0

    def new_entry_id(self):
        self.ety_id += 1
        return self.ety_id

    def remove_server(self, server):
        server.removed = True
        # self.servers = [s for s in self.servers if s is not server]

    @property
    def active_servers(self):
        return [s for s in self.servers if not getattr(s, 'removed', False)]

    def add_member(self):
        if net.num_of_servers <= len(self.active_servers):
            return

        if self.leader == -1:
            logger.info('add_member: no leader')
            return

        leader = self.leader
        leader_node = None

        for server in self.active_servers:
            if server.id == leader:
                leader_node = server

        if leader_node is None:
            return

        if not lib.raft_is_leader(leader_node.raft):
            return

        if lib.raft_voting_change_is_in_progress(leader_node.raft):
            logger.info('{} voting change in progress'.format(leader_node.id))
            return

        server = RaftServer(self)
        logger.info(f"adding node {server.id}")

        # Create a new configuration entry to be processed by the leader
        ety = self.add_entry(lib.RAFT_LOGTYPE_ADD_NONVOTING_NODE,
                             ChangeRaftEntry(server.id))
        assert(lib.raft_entry_is_cfg_change(ety))

        e = leader_node.recv_entry(ety)
        if 0 != e:
            logger.error(err2str(e))
            return
        else:
            self.num_membership_changes += 1

        # Wake up new node
        server.set_connection_status(NODE_CONNECTING)
        assert server.udata
        added_node = lib.raft_add_non_voting_node(server.raft, server.udata, server.id, 1)
        assert added_node

    def remove_member(self):
        if self.leader == -1:
            logger.error('no leader')
            return

        leader = self.leader
        server = self.random.choice(self.active_servers)
        leader_node = None

        for sv in self.active_servers:
            if sv.id == leader:
                leader_node = sv

        if leader_node is None:
            return

        if not lib.raft_is_leader(leader_node.raft):
            return

        if lib.raft_voting_change_is_in_progress(leader_node.raft):
            logger.info('{} voting change in progress'.format(server))
            return

        if leader_node == server:
            logger.info('can not remove leader')
            return

        if server.connection_status in [NODE_CONNECTING, NODE_DISCONNECTING]:
            logger.info('can not remove server that is changing connection status')
            return

        if NODE_DISCONNECTED == server.connection_status:
            self.remove_server(server)
            return

        # Create a new configuration entry to be processed by the leader
        ety = self.add_entry(lib.RAFT_LOGTYPE_REMOVE_NODE,
                             ChangeRaftEntry(server.id))
        assert server.connection_status == NODE_CONNECTED
        assert(lib.raft_entry_is_cfg_change(ety))

        e = leader_node.recv_entry(ety)
        if 0 != e:
            logger.error(err2str(e))
            return
        else:
            self.num_membership_changes += 1

        logger.info(f"trying to remove follower: node {lib.raft_get_nodeid(server.raft)}")

        # Wake up new node
        server.set_connection_status(NODE_DISCONNECTING)

    def diagnostic_info(self):
        print()

        info = {
            "Maximum appendentries size": self.max_entries_in_ae,
            "Leadership changes": self.leadership_changes,
            "Log pops": self.log_pops,
            "Unique nodes": self.num_unique_nodes,
            "Membership changes": self.num_membership_changes,
            "Compactions": self.num_compactions,
            "Number of Active Servers": len(self.active_servers),
            "Active Server Ids": [server.id for server in self.active_servers],
        }

        print('partitions:', ['{} -> {}'.format(*map(str, p)) for p in self.partitions])

        for k, v in info.items():
            print(k, v)

        print()

        # Servers
        keys = net.servers[0].debug_statistic_keys()
        data = [list(keys)] + [
            [s.debug_statistics()[key] for key in keys]
            for s in net.servers
            ]
        table = terminaltables.AsciiTable(data)
        print(table.table)


class RaftServer(object):
    def __init__(self, network):

        self.connection_status = NODE_DISCONNECTED

        self.raft = lib.raft_new()
        self.udata = ffi.new_handle(self)
        self.removed = False
        self.old_status = None

        network.add_server(self)

        self.load_callbacks()
        cbs = ffi.new('raft_cbs_t*')
        cbs.send_requestvote = self.raft_send_requestvote
        cbs.send_appendentries = self.raft_send_appendentries
        cbs.send_snapshot = self.raft_send_snapshot
        cbs.load_snapshot = self.raft_load_snapshot
        cbs.clear_snapshot = self.raft_clear_snapshot
        cbs.get_snapshot_chunk = self.raft_get_snapshot_chunk
        cbs.store_snapshot_chunk = self.raft_store_snapshot_chunk
        cbs.applylog = self.raft_applylog
        cbs.persist_metadata = self.raft_persist_metadata
        cbs.get_node_id = self.raft_get_node_id
        cbs.node_has_sufficient_logs = self.raft_node_has_sufficient_logs
        cbs.notify_membership_event = self.raft_notify_membership_event
        cbs.log = self.raft_log

        log_cbs = ffi.new('raft_log_cbs_t*')
        log_cbs.log_offer = self.raft_logentry_offer
        log_cbs.log_poll = self.raft_logentry_poll
        log_cbs.log_pop = self.raft_logentry_pop

        lib.raft_set_callbacks(self.raft, cbs, self.udata)
        lib.raft_log_set_callbacks(lib.raft_get_log(self.raft), log_cbs, self.raft)

        lib.raft_config(self.raft, 1, lib.RAFT_CONFIG_ELECTION_TIMEOUT,
                        ffi.cast("int", 500))
        lib.raft_config(self.raft, 1, lib.RAFT_CONFIG_LOG_ENABLED,
                        ffi.cast("int", 1))
        lib.raft_config(self.raft, 1, lib.RAFT_CONFIG_AUTO_FLUSH,
                        ffi.cast("int", network.auto_flush))

        self.fsm_dict = {}
        self.fsm_log = []

        self.random = random.Random(self.id)

        # Dummy snapshot file for this node
        self.snapshot_buf = bytearray(SNAPSHOT_SIZE)

        # Save incoming snapshot chunks to this buffer
        self.snapshot_recv_buf = bytearray(SNAPSHOT_SIZE)

    def __str__(self):
        return '<Server: {0}>'.format(self.id)

    def __repr__(self):
        return 'sv:{0}'.format(self.id)

    def __lt__(self, other):
        return self.id < other.id

    def set_connection_status(self, new_status):
        assert(not (self.connection_status == NODE_CONNECTED and new_status == NODE_CONNECTING))
        # logger.warning('{}: {} -> {}'.format(
        #     self,
        #     connectstatus2str(self.connection_status),
        #     connectstatus2str(new_status)))
        if new_status == NODE_DISCONNECTING and self.old_status is not None:
            self.old_status = self.connection_status

        self.connection_status = new_status

    def debug_log(self):
        first_idx = lib.raft_get_snapshot_last_idx(self.raft)
        return [(i + first_idx, l.term, l.id) for i, l in enumerate(self.fsm_log)]

    def do_compaction(self):
        # logger.warning('{} snapshotting'.format(self))
        # entries_before = lib.raft_get_log_count(self.raft)

        e = lib.raft_begin_snapshot(self.raft)
        if e != 0:
            return

        assert(lib.raft_snapshot_is_in_progress(self.raft))

        # Generate a dummy snapshot
        self.snapshot_buf = bytearray(self.random.randbytes(SNAPSHOT_SIZE))

        e = lib.raft_end_snapshot(self.raft)
        assert(e == 0)
        if e != 0:
            return

        self.do_membership_snapshot()
        self.snapshot.image = dict(self.fsm_dict)
        self.snapshot.last_term = lib.raft_get_snapshot_last_term(self.raft)
        self.snapshot.last_idx = lib.raft_get_snapshot_last_idx(self.raft)

        self.network.num_compactions += 1

        # logger.warning('{} entries compacted {}'.format(
        #     self,
        #     entries_before - lib.raft_get_log_count(self.raft)
        #     ))

    def periodic(self, msec):
        if self.network.random.randint(1, 100000) < self.network.compaction_rate:
            self.do_compaction()

        e = lib.raft_periodic_internal(self.raft, msec)
        if lib.RAFT_ERR_SHUTDOWN == e:
            self.shutdown()

        # e = lib.raft_apply_all(self.raft)
        # if lib.RAFT_ERR_SHUTDOWN == e:
        #     self.shutdown()
        #     return

        if hasattr(self, 'abort_exception'):
            raise self.abort_exception

    def shutdown(self):
        logger.error('{} shutting down'.format(self))
        self.set_connection_status(NODE_DISCONNECTED)
        self.network.remove_server(self)

    def set_network(self, network):
        self.network = network

    def load_callbacks(self):
        self.raft_send_requestvote = ffi.callback("int(raft_server_t*, void*, raft_node_t*, raft_requestvote_req_t*)", raft_send_requestvote)
        self.raft_send_appendentries = ffi.callback("int(raft_server_t*, void*, raft_node_t*, raft_appendentries_req_t*)", raft_send_appendentries)
        self.raft_send_snapshot = ffi.callback("int(raft_server_t*, void* , raft_node_t*, raft_snapshot_req_t*)", raft_send_snapshot)
        self.raft_load_snapshot = ffi.callback("int(raft_server_t*, void*, raft_term_t, raft_index_t)", raft_load_snapshot)
        self.raft_clear_snapshot = ffi.callback("int(raft_server_t*, void*)", raft_clear_snapshot)
        self.raft_get_snapshot_chunk = ffi.callback("int(raft_server_t*, void*, raft_node_t*, raft_size_t offset, raft_snapshot_chunk_t*)", raft_get_snapshot_chunk)
        self.raft_store_snapshot_chunk = ffi.callback("int(raft_server_t*, void*, raft_index_t index, raft_size_t offset, raft_snapshot_chunk_t*)", raft_store_snapshot_chunk)
        self.raft_applylog = ffi.callback("int(raft_server_t*, void*, raft_entry_t*, raft_index_t)", raft_applylog)
        self.raft_persist_metadata = ffi.callback("int(raft_server_t*, void*, raft_term_t, raft_node_id_t)", raft_persist_metadata)
        self.raft_logentry_offer = ffi.callback("int(raft_server_t*, void*, raft_entry_t*, raft_index_t)", raft_logentry_offer)
        self.raft_logentry_poll = ffi.callback("int(raft_server_t*, void*, raft_entry_t*, raft_index_t)", raft_logentry_poll)
        self.raft_logentry_pop = ffi.callback("int(raft_server_t*, void*, raft_entry_t*, raft_index_t)", raft_logentry_pop)
        self.raft_get_node_id = ffi.callback("int(raft_server_t*, void*, raft_entry_t*, raft_index_t)", raft_get_node_id)
        self.raft_node_has_sufficient_logs = ffi.callback("int(raft_server_t* raft, void *user_data, raft_node_t* node)", raft_node_has_sufficient_logs)
        self.raft_notify_membership_event = ffi.callback("void(raft_server_t* raft, void *user_data, raft_node_t* node, raft_entry_t* ety, raft_membership_e)", raft_notify_membership_event)
        self.raft_log = ffi.callback("void(raft_server_t*, void*, const char* buf)", raft_log)
        self.handle_read_queue = ffi.callback("void(void *arg, int can_read)", handle_read_queue)

    def recv_entry(self, ety):
        # FIXME: leak
        response = ffi.new('raft_entry_resp_t*')
        return lib.raft_recv_entry(self.raft, ety, response)

    def get_entry(self, idx):
        idx = idx - lib.raft_get_snapshot_last_idx(self.raft)
        if idx < 0:
            raise IndexError
        try:
            return self.fsm_log[idx]
        except:
            # self.abort_exception = e
            raise

    def _check_log_matching(self, our_log, idx):
        """
        Quality:

        Log Matching: if two logs contain an entry with the same index and
        term, then the logs are identical in all entries up through the given
        index. §5.3

        State Machine Safety: if a server has applied a log entry at a given
        index to its state machine, no other server will ever apply a
        different log entry for the same index. §5.4.3
        """
        for server in self.network.active_servers:
            if server is self:
                # no point in comparing ourselves against ourselves
                continue

            their_commit_idx = lib.raft_get_commit_idx(server.raft)
            their_snapshot_last_idx = lib.raft_get_snapshot_last_idx(server.raft)

            if lib.raft_get_commit_idx(self.raft) <= their_commit_idx and idx <= their_commit_idx:
                their_log = lib.raft_get_entry_from_idx(server.raft, idx)

                if their_log == ffi.NULL:
                    # if there's no log entry to retrieve, it must be because it was snapshotted away
                    # therefore make sure that this entry idx is <= what was snapshotted
                    assert idx <= lib.raft_get_snapshot_last_idx(server.raft), f"idx = {idx}, their_commit_idx = {their_commit_idx}, snapshot_last_idx = {their_snapshot_last_idx}"
                    # as in snapshot and not in log, can't compare, so just go to next server
                    continue

                # we only compare RAFT_LOGTYPE_NORMAL for now, probably have to improve this, to also compare log.data
                # and other LOGTYPES for complete equality
                if their_log.type in [lib.RAFT_LOGTYPE_NORMAL]:
                    try:
                        assert their_log.term == our_log.term
                        assert their_log.id == our_log.id
                    except Exception as e:
                        ety1 = lib.raft_get_entry_from_idx(self.raft, idx)
                        ety2 = lib.raft_get_entry_from_idx(server.raft, idx)
                        logger.error('ids', ety1.id, ety2.id)
                        logger.error('{0}vs{1} idx:{2} terms:{3} {4} ids:{5} {6}'.format(
                            self, server,
                            idx,
                            our_log.term, their_log.term,
                            our_log.id, their_log.id))
                        self.abort_exception = e

                        logger.error(f"{self.id}: {self.debug_log()}")
                        logger.error(f"{server.id}: {server.debug_log()}")
                        self.network.diagnostic_info()
                        return lib.RAFT_ERR_SHUTDOWN
                lib.raft_entry_release(their_log)

    def entry_apply(self, ety, idx):
        # collect stats
        if self.network.latest_applied_log_idx < idx:
            self.network.latest_applied_log_idx = idx
            self.network.latest_applied_log_iteration = self.network.iteration

        e = self._check_log_matching(ety, idx)
        if e is not None:
            logger.error(f"{self.id} failed log matching normally, not exception thrown: {e}")
            return e

        if ety.type == lib.RAFT_LOGTYPE_NO_OP:
            return 0

        change = ffi.from_handle(lib.raft_entry_getdata(ety))

        if ety.type == lib.RAFT_LOGTYPE_NORMAL:
            self.fsm_dict[change.key] = change.val

        elif ety.type == lib.RAFT_LOGTYPE_REMOVE_NODE:
            if change.node_id == lib.raft_get_nodeid(self.raft):
                logger.info("{} shutting down because of removal".format(self))
                return lib.RAFT_ERR_SHUTDOWN

        elif ety.type == lib.RAFT_LOGTYPE_ADD_NODE:
            if change.node_id == self.id:
                self.set_connection_status(NODE_CONNECTED)

        elif ety.type == lib.RAFT_LOGTYPE_ADD_NONVOTING_NODE:
            pass

        return 0

    def do_membership_snapshot(self):
        self.snapshot = Snapshot()
        for i in range(0, lib.raft_get_num_nodes(self.raft)):
            n = lib.raft_get_node_from_idx(self.raft, i)
            id = lib.raft_node_get_id(n)
            if 0 == lib.raft_node_is_addition_committed(n):
                id = -1

            self.snapshot.members.append(
                SnapshotMember(id, lib.raft_node_is_voting_committed(n)))

    def clear_snapshot(self):
        self.snapshot_recv_buf = bytearray(SNAPSHOT_SIZE)
        return 0

    def store_snapshot(self, offset, data):
        for i in range(0, len(data)):
            self.snapshot_recv_buf[offset + i] = bytes(data)[i]

        return 0

    def load_snapshot(self, term, index):
        logger.debug('{} loading snapshot'.format(self))

        leader = find_leader()
        if not leader:
            return 0

        leader_snapshot = leader.snapshot_buf

        # Copy received snapshot as our snapshot and clear the temp buf
        self.snapshot_buf[:] = self.snapshot_recv_buf
        self.snapshot_recv_buf = bytearray(SNAPSHOT_SIZE)

        # Validate received snapshot against the leader's snapshot
        for i in range(len(self.snapshot_buf)):
            assert self.snapshot_buf[i] == leader_snapshot[i]

        e = lib.raft_begin_load_snapshot(self.raft, term, index)
        logger.debug(f"return value from raft_begin_load_snapshot = {e}")
        if e == -1:
            return 0
        elif e == lib.RAFT_ERR_SNAPSHOT_ALREADY_LOADED:
            return 0
        elif e == 0:
            pass
        else:
            assert False

        snapshot_info = leader.snapshot
        node_id = lib.raft_get_nodeid(self.raft)

        # set membership configuration according to snapshot
        for member in snapshot_info.members:
            if -1 == member.id:
                continue

            node = lib.raft_get_node(self.raft, member.id)

            if not node:
                udata = ffi.NULL
                try:
                    node_sv = self.network.id2server(member.id)
                    udata = node_sv.udata
                except ServerDoesNotExist:
                    pass

                node = lib.raft_add_node(self.raft, udata, member.id, member.id == node_id)

            lib.raft_node_set_active(node, 1)

            if member.voting and not lib.raft_node_is_voting(node):
                lib.raft_node_set_voting(node, 1)
            elif not member.voting and lib.raft_node_is_voting(node):
                lib.raft_node_set_voting(node, 0)

            if node_id != member.id:
                assert node

        # TODO: this is quite ugly
        # we should have a function that removes all nodes by ourself
        # if (!raft_get_my_node(self->raft)) */
        #     raft_add_non_voting_node(self->raft, NULL, node_id, 1); */

        e = lib.raft_end_load_snapshot(self.raft)
        assert(0 == e)
        assert(lib.raft_get_log_count(self.raft) == 0)

        self.do_membership_snapshot()
        self.snapshot.image = dict(snapshot_info.image)
        self.snapshot.last_term = snapshot_info.last_term
        self.snapshot.last_idx = snapshot_info.last_idx

        assert(lib.raft_get_my_node(self.raft))
        # assert(sv->snapshot_fsm);

        self.fsm_dict = dict(snapshot_info.image)

        # logger.warning('{} loaded snapshot t:{} idx:{}'.format(
        #     self, snapshot.last_term, snapshot.last_idx))
        return 0

    def persist_metadata(self, term, vote):
        # TODO: add disk simulation
        return 0

    def _check_id_monoticity(self, ety):
        """
        Check last entry has smaller ID than new entry.
        This is a virtraft specific check to make sure entry passing is
        working correctly.
        """
        if ety.type == lib.RAFT_LOGTYPE_NO_OP:
            return

        ci = lib.raft_get_current_idx(self.raft)
        if 0 < ci and not lib.raft_get_snapshot_last_idx(self.raft) == ci:
            other_id = None
            try:
                prev_ety = lib.raft_get_entry_from_idx(self.raft, ci)
                assert prev_ety
                if prev_ety.type == lib.RAFT_LOGTYPE_NO_OP:
                    if lib.raft_get_snapshot_last_idx(self.raft) != ci - 1:
                        lib.raft_entry_release(prev_ety)
                        prev_ety = lib.raft_get_entry_from_idx(self.raft, ci - 1)
                        assert prev_ety
                    else:
                        lib.raft_entry_release(prev_ety)
                        return
                other_id = prev_ety.id
                assert other_id < ety.id
                lib.raft_entry_release(prev_ety)
            except Exception as e:
                logger.error(other_id, ety.id)
                self.abort_exception = e
                raise

    def entry_append(self, ety, ety_idx):
        try:
            assert not self.fsm_log or self.fsm_log[-1].term <= ety.term, f"node {self.id}'s term {self.fsm_log[-1].term} > to entry term {ety.term}"
        except Exception as e:
            self.abort_exception = e
            # FIXME: consider returning RAFT_ERR_SHUTDOWN
            raise

        self._check_id_monoticity(ety)

        self.fsm_log.append(RaftEntry(ety))

        return 0

    def entry_poll(self, ety, ety_idx):
        self.fsm_log.pop(0)
        return 0

    def _check_committed_entry_popping(self, ety_idx):
        """
        Check we aren't popping a committed entry
        """
        try:
            assert lib.raft_get_commit_idx(self.raft) < ety_idx
        except Exception as e:
            self.abort_exception = e
            logger.error(f"{self.id}: failed in _check_committed_entry_popping")
            logger.error(f"{traceback.format_exc()}")
            return lib.RAFT_ERR_SHUTDOWN
        return 0

    def entry_pop(self, ety, ety_idx):
        logger.debug("POP {} {} {}".format(self, ety_idx, ety.type))

        e = self._check_committed_entry_popping(ety_idx)
        if e != 0:
            return e

        self.fsm_log.pop()
        self.network.log_pops += 1

        if ety.type == lib.RAFT_LOGTYPE_NO_OP:
            return 0

        if (ety.type == lib.RAFT_LOGTYPE_REMOVE_NODE or
                ety.type == lib.RAFT_LOGTYPE_ADD_NONVOTING_NODE or
                ety.type == lib.RAFT_LOGTYPE_ADD_NODE):

            change = ffi.from_handle(lib.raft_entry_getdata(ety))
            server = self.network.id2server(change.node_id)

            if ety.type == lib.RAFT_LOGTYPE_REMOVE_NODE:
                if server.old_status is not None:
                    server.set_connection_status(self.old_status)
                    server.old_status = None
            elif ety.type == lib.RAFT_LOGTYPE_ADD_NONVOTING_NODE:
                logger.error("POP disconnect {} {}".format(self, ety_idx))
                server.set_connection_status(NODE_DISCONNECTED)

            elif ety.type == lib.RAFT_LOGTYPE_ADD_NODE:
                server.set_connection_status(NODE_CONNECTING)

        return 0

    def node_has_sufficient_entries(self, node):
        assert(not lib.raft_node_is_voting(node))

        logger.debug(f"node_has_sufficient_entries: adding {lib.raft_node_get_id(node)}")

        ety = self.network.add_entry(
            lib.RAFT_LOGTYPE_ADD_NODE,
            ChangeRaftEntry(lib.raft_node_get_id(node)))
        assert(lib.raft_entry_is_cfg_change(ety))
        # FIXME: leak
        e = self.recv_entry(ety)
        # print(err2str(e))
#        assert e == 0
        return e

    def notify_membership_event(self, node, ety, event_type):
        # Convenience: Ensure that added node has udata set
        if event_type == lib.RAFT_MEMBERSHIP_ADD:
            node_id = lib.raft_node_get_id(node)
            try:
                server = self.network.id2server(node_id)
            except ServerDoesNotExist:
                pass
            else:
                node = lib.raft_get_node(self.raft, node_id)
                lib.raft_node_set_udata(node, server.udata)
        elif event_type == lib.RAFT_MEMBERSHIP_REMOVE:
            node_id = lib.raft_node_get_id(node)
            try:
                server = self.network.id2server(node_id)
                server.set_connection_status(NODE_DISCONNECTED)
            except ServerDoesNotExist:
                pass



    def debug_statistic_keys(self):
        return ["node", "state", "leader", "status", "removed", "current", "last_log_term", "term", "committed", "applied",
                "log_count", "#peers", "#voters", "cfg_change", "snapshot", "partitioned", "voters"]

    def debug_statistics(self):
        partitioned_from = []
        for partition in self.network.partitions:
            if partition[0] is self:
                partitioned_from.append(partition[1].id)

        return {
            "node": lib.raft_get_nodeid(self.raft),
            "state": state2str(lib.raft_get_state(self.raft)),
            "current": lib.raft_get_current_idx(self.raft),
            "leader": lib.raft_get_leader_id(self.raft),
            "last_log_term": lib.raft_get_last_log_term(self.raft),
            "term": lib.raft_get_current_term(self.raft),
            "committed": lib.raft_get_commit_idx(self.raft),
            "applied": lib.raft_get_last_applied_idx(self.raft),
            "log_count": lib.raft_get_log_count(self.raft),
            "#peers": lib.raft_get_num_nodes(self.raft),
            "#voters": lib.raft_get_num_voting_nodes(self.raft),
            "status": connectstatus2str(self.connection_status),
            "cfg_change": lib.raft_voting_change_is_in_progress(self.raft),
            "snapshot": lib.raft_get_snapshot_last_idx(self.raft),
            "removed": getattr(self, 'removed', False),
            "partitioned": partitioned_from,
            "voters": get_voting_node_ids(self),
        }


if __name__ == '__main__':
    try:
        args = docopt.docopt(__doc__, version='virtraft 0.1')
    except docopt.DocoptExit as e:
        print(e)
        sys.exit()

    if args['--verbose'] or args['--log_level']:
        coloredlogs.install(fmt='%(asctime)s %(levelname)s %(message)s')

        if args['--log_level']:
            level = int(args['--log_level'])
        else:
            level = logging.DEBUG

        logger.setLevel(level)

        if args['--log_file']:
            fh = logging.FileHandler(args['--log_file'])
            fh.setLevel(level)
            logger.addHandler(fh)
            logger.propagate = False

    global net
    net = Network(int(args['--seed']))

    net.dupe_rate = int(args['--dupe_rate'])
    net.drop_rate = int(args['--drop_rate'])
    net.client_rate = int(args['--client_rate'])
    net.member_rate = int(args['--member_rate'])
    net.compaction_rate = int(args['--compaction_rate'])
    net.partition_rate = int(args['--partition_rate'])
    net.no_random_period = 1 == int(args['--no_random_period'])
    net.duplex_partition = 1 == int(args['--duplex_partition'])
    net.rqm = int(args['--rqm'])
    net.auto_flush = 1 == int(args['--auto_flush'])

    net.num_of_servers = int(args['--servers'])

    if net.member_rate == 0:
        for i in range(0, int(args['--servers'])):
            RaftServer(net)
        net.commit_static_configuration()
    else:
        RaftServer(net)
        net.prep_dynamic_configuration()

    for i in range(0, int(args['--iterations'])):
        net.iteration += 1
        try:
            net.periodic()
            net.poll_messages()
            net.push_read()
            net.poll_messages()
        except:
            # for server in net.servers:
            #     print(server, [l.term for l in server.fsm_log])
            raise

    if args['--json_output']:
        pass

    if not args['--quiet']:
        net.diagnostic_info()
