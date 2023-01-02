"""
Copyright Redis Ltd. 2020 - present
Licensed under your choice of the Redis Source Available License 2.0 (RSALv2)
or the Server Side Public License v1 (SSPLv1).
"""

import sys
import os
import struct
import logging
from enum import Enum


class RawEntry(object):
    # Log entries
    RAFTLOG = 'RAFTLOG'
    ENTRY = 'ENTRY'

    def __init__(self, args, locations):
        self.args = args.copy()
        self.locations = locations.copy()

    def kind(self):
        return str(self.args[0], encoding='ascii')

    @classmethod
    def from_file(cls, _file):
        mb_line = str(_file.readline().rstrip(), encoding='ascii')
        if not mb_line:
            raise EOFError('End of file reading multi-bulk')
        if not mb_line.startswith('*'):
            raise RuntimeError('Invalid multi-bulk line')
        elements = int(mb_line[1:])

        args = []
        locations = []

        for _ in range(elements):
            hdr = str(_file.readline().rstrip(), encoding='ascii')
            if not hdr.startswith('$'):
                raise RuntimeError('Missing/invalid bulk header')
            _len = int(hdr[1:])
            locations.append(_file.tell())
            data = _file.read(_len)
            args.append(data)

            eol = _file.read(2)
            if eol != b'\r\n':
                raise RuntimeError('Missing CRLF after bulk data')

        if str(args[0], encoding='ascii') == cls.ENTRY:
            return LogEntry(args, locations)
        if str(args[0], encoding='ascii') == cls.RAFTLOG:
            return LogHeader(args, locations)
        return RawEntry(args, locations)

    def __str__(self):
        return '<RawEntry:kind=%s>' % self.kind()

    def __repr__(self):
        return '<RawEntry:%s>' % ','.join(str(x, encoding='ascii')
                                          for x in self.args)


class LogHeader(RawEntry):
    def version(self):
        return int(self.args[1])

    def dbid(self):
        return self.args[2]

    def dbid_location(self):
        return int(self.locations[2])

    def node_id(self):
        return self.args[3]

    def snapshot_term(self):
        return int(self.args[4])

    def snapshot_index(self):
        return int(self.args[5])

    def crc(self):
        return int(self.args[6])

    def crc_location(self):
        return int(self.locations[6])

    def __repr__(self):
        return '<LogHeader:version=%s,dbid=%s,node_id=%s,' \
               'snapshot=<term:%s,index:%s>' % (
                   self.version(), self.dbid(), self.node_id(),
                   self.snapshot_term(), self.snapshot_index())

    def __str__(self):
        return '#### node_id={} dbid={} version={}\n' \
               '#### snapshot-term={} snapshot-index={}'.format(
                self.node_id().decode(encoding='ascii'),
                self.dbid().decode(encoding='ascii'),
                self.version(),
                self.snapshot_term(), self.snapshot_index())


class LogEntry(RawEntry):
    class LogType(Enum):
        NORMAL = 0
        ADD_NONVOTING_NODE = 1
        ADD_NODE = 2
        REMOVE_NODE = 3
        NO_OP = 4
        ADD_SHARDGROUP = 101

    def term(self):
        return int(self.args[1])

    def id(self):
        return int(self.args[2])

    def session(self):
        return int(self.args[3])

    def type(self):
        return self.LogType(int(self.args[4]))

    def type_is_cfgchange(self):
        _type = self.type()
        return _type in (self.LogType.ADD_NONVOTING_NODE,
                         self.LogType.ADD_NODE,
                         self.LogType.REMOVE_NODE)

    @staticmethod
    def parse_cfgchange(data):
        node_id, port, addr = struct.unpack_from('iH255s', data)
        return '<CfgChange:node_id=%s,port=%s,addr=%s>' % (
            node_id, port, addr.decode('ascii').split('\0', 1)[0])

    @staticmethod
    def parse_add_shardgroup(data):
        data_lines = data.decode('ascii').split('\n')
        hdr = data_lines[0].split(':')
        return '<ShardGroup:slots=%s-%s,nodes=%s>' % (
            hdr[0], hdr[1],
            ','.join(['<id={},addr={}:{}'.format(*e.split(':'))
                      for e in data_lines[1:] if len(e) > 0]))

    @staticmethod
    def parse_cmdlist(data):
        cmds = []
        cmd_count = int(data[0][1:])
        i = 1
        for _ in range(cmd_count):
            args_count = int(data[i][1:])
            i_end = i + 1 + args_count*2
            cmds.append(' '.join(data[i+2:i_end:2]))
            i = i_end
        return '|'.join(cmds)

    def data(self, decode=False):
        value = self.args[5]
        if self.type_is_cfgchange():
            return self.parse_cfgchange(value)
        elif self.type() == self.LogType.ADD_SHARDGROUP:
            return self.parse_add_shardgroup(value)
        else:
            if decode:
                if not value:
                    return ''
                return self.parse_cmdlist(value.decode(encoding='ascii').
                                          split('\n'))
            else:
                return value

    def data_location(self):
        return int(self.locations[4])

    def crc(self):
        return int(self.args[5])

    def crc_location(self):
        return int(self.locations[5])

    def __repr__(self):
        return '<LogEntry:%s:id=%s,term=%s,session=%s,data=%s>' % (
            self.type(), self.id(), self.term(), self.session(), self.data())

    def __str__(self):
        return '{:4d} {:10d} {} {} {}'.format(
            self.term(), self.id(), self.session(), self.type().name,
            self.data(decode=True))


class RaftLog(object):
    def __init__(self, filename):
        self.logfile = open(filename, 'rb')
        self.entries = []
        self.indexes = []

    def reset(self):
        self.entries = []
        self.indexes = []
        self.logfile.seek(0, os.SEEK_SET)

    def read(self):
        while True:
            offset = self.logfile.tell()
            try:
                entry = RawEntry.from_file(self.logfile)
            except EOFError:
                break
            self.entries.append(entry)
            self.indexes.append(offset)
        self.dump()

    def header(self) -> LogHeader:
        return self.entries[0]

    def last_entry(self):
        return self.entries[-1]

    def entry_count(self, _type=None):
        count = 0
        for entry in self.entries:
            if not isinstance(entry, LogEntry):
                continue
            if _type is None or _type == entry.type():
                count += 1
        return count

    def dump(self):
        logging.info('===== Begin Raft Log Dump =====')
        for entry in self.entries:
            logging.info(repr(entry))
        logging.info('===== End Raft Log Dump =====')


def main():
    log = RaftLog(sys.argv[1])
    log.read()
    i = 0
    for entry in log.entries:
        print('{:7d} {}'.format(i, str(entry)))
        i += 1


if __name__ == '__main__':
    main()
