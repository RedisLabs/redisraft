import pytest
from sandbox import Cluster

@pytest.fixture
def cluster():
    """
    A fixture for a sandbox Cluster()
    """

    _cluster = Cluster()
    yield _cluster
    _cluster.destroy()
