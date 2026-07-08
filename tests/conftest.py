import os
import sys
import pytest

# make glue/lib importable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from glue.lib.spark_session import get_local_spark  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    s = get_local_spark("de-gp-tests")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def data_dir():
    """Directory holding the three real CSVs. Set DATA_DIR to enable integration tests."""
    return os.environ.get("DATA_DIR")
