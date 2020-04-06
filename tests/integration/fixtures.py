import pytest
from sandbox import Cluster
from workload import Workload

@pytest.fixture
def cluster():
    """
    A fixture for a sandbox Cluster()
    """

    _cluster = Cluster()
    yield _cluster
    _cluster.destroy()

@pytest.fixture
def workload():
    """
    A fixture for a Workload.
    """

    _workload = Workload()
    yield _workload
    _workload.terminate()
