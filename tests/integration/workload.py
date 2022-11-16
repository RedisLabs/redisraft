"""
Copyright Redis Ltd. 2020 - present
Licensed under your choice of the Redis Source Available License 2.0 (RSALv2)
or the Server Side Public License v1 (SSPLv1).
"""

import threading
from redis import RedisError


class AbstractWork(object):
    def __init__(self, thread_id, client):
        self.thread_id = thread_id
        self.client = client


class MultiWithLargeReply(AbstractWork):
    def __init__(self, *args, **kwargs):
        super(MultiWithLargeReply, self).__init__(*args, **kwargs)
        self.counter_key = 'counter-%s' % self.thread_id
        self.list_key = 'list-%s' % self.thread_id

    def do_work(self, iteration):
        try:
            pipeline = self.client.pipeline(transaction=True)
            pipeline.incr(self.counter_key)
            pipeline.rpush(self.list_key, iteration)
            for _ in range(1):
                pipeline.lrange(self.list_key, 0, -1)
            reply = pipeline.execute()
            return reply[0] == reply[1]     # count == length of list
        except RedisError:
            pass
        return True


class MonotonicIncrCheck(AbstractWork):
    def __init__(self, *args, **kwargs):
        super(MonotonicIncrCheck, self).__init__(*args, **kwargs)
        self.counter_key = 'counter-%s' % self.thread_id
        self.val = 0

    def do_work(self, _iteration):
        try:
            new_val = self.client.incr(self.counter_key)
            if new_val <= self.val:
                return False
            self.val = new_val
        except RedisError:
            pass
        return True


class Workload(object):
    def __init__(self):
        self._terminate = False
        self._threads = []
        self._iters = 0
        self._failures = 0

    def start(self, thread_count, cluster, work_class):
        for thread_id in range(thread_count):
            _thread = threading.Thread(target=self._worker,
                                       args=[thread_id + 1,
                                             work_class,
                                             cluster.random_node().client])
            _thread.start()
            self._threads.append(_thread)

    def _worker(self, thread_id, work_class, client):
        worker = work_class(thread_id, client)
        iteration = 1
        while not self._terminate:
            if not worker.do_work(iteration):
                self._failures += 1
            self._iters += 1
            iteration += 1

    def terminate(self):
        self._terminate = True
        while self._threads:
            self._threads.pop().join()

    def stop(self):
        self.terminate()
        assert self._iters > 0
        assert self._failures == 0

    def stats(self):
        return 'Iterations: %s; Failures: %s' % (self._iters, self._failures)
