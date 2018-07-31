import logging
from enum import Enum

class RawEntry(object):
    # Log entries
    RAFTLOG = 'RAFTLOG'
    ENTRY = 'ENTRY'
    REMHEAD = 'REMHEAD'
    REMTAIL = 'REMTAIL'
    TERM = 'TERM'
    VOTE = 'VOTE'
    SNAPSHOT = 'SNAPSHOT'

    def __init__(self, args):
        self.args = args.copy()

    def kind(self):
        return self.args[0]

    @classmethod
    def from_file(cls, f):
        mb_line = str(f.readline().rstrip(), encoding='ascii')
        if not mb_line:
            raise EOFError('End of file reading multi-bulk')
        if not mb_line.startswith('*'):
            raise RuntimeError('Invalid multi-bulk line')
        elements = int(mb_line[1:])

        args = []
        for _ in range(elements):
            hdr = str(f.readline().rstrip(), encoding='ascii')
            if not hdr.startswith('$'):
                raise RuntimeError('Missing/invalid bulk header')
            len = int(hdr[1:])
            data = f.read(len)
            args.append(data)

            eol = f.read(2)
            if eol != b'\r\n':
                raise RuntimeError('Missing CRLF after bulk data')

        if str(args[0], encoding='ascii') == cls.ENTRY:
            return LogEntry(args)
        else:
            return RawEntry(args)

    def __str__(self):
        return '<RawEntry:kind=%s>' % self.kind()

    def __repr__(self):
        return '<RawEntry:%s>' % ','.join(str(x, encoding='ascii')
                                          for x in self.args)

class LogEntry(RawEntry):
    class LogType(Enum):
        NORMAL = 0
        ADD_NONVOTING_NODE = 1
        ADD_NODE = 2
        DEMOTE_NODE = 2
        REMOVE_NODE = 2

    def term(self):
        return int(self.args[1])
    def id(self):
        return int(self.args[2])
    def type(self):
        return self.LogType(int(self.args[3]))
    def data(self):
        return self.args[4]

    def __repr__(self):
        return '<LogEntry:%s:id=%s,term=%s,data=%s>' % (
            self.type(), self.id(), self.term(), self.data())

class RaftLog(object):
    def __init__(self, filename):
        self.logfile = open(filename, 'rb')
        self.entries = []

    def read(self):
        while True:
            try:
                entry = RawEntry.from_file(self.logfile)
            except EOFError:
                break
            self.entries.append(entry)
        self.dump()

    def entry_count(self, type=None):
        count = 0
        for entry in self.entries:
            if not isinstance(entry, LogEntry):
                continue
            if type is None or type == entry.type():
                count += 1
        return count

    def dump(self):
        logging.info('===== Begin Raft Log Dump =====')
        for entry in self.entries:
            logging.info(repr(entry))
        logging.info('===== End Raft Log Dump =====')

