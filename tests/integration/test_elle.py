"""
This file is part of RedisRaft.

Copyright (c) 2020-2021 Redis Ltd.

RedisRaft is licensed under the Redis Source Available License (RSAL).
"""
import pytest as pytest

from .sandbox import ElleWorker


@pytest.mark.skipif("not config.getoption('elle')")
def test_elle_sanity(elle, cluster):
    cluster.create(3)

    worker = ElleWorker(elle, elle.map_addresses_to_clients([cluster]))

    for i in range(10):
        ops = worker.generate_ops(["key"])
        worker.do_ops(ops)
