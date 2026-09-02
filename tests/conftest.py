"""Fixtures for the load job and the header check.

`awsglue` ships only inside a Glue job, so importing `load_table` on a laptop
fails at that line. A stub registered before the import lets everything below it
be tested locally and in CI, which is where the reader options actually get
proven — a deployed run is a slow and expensive place to find out that `escape`
was wrong.
"""

import gzip
import pathlib
import sys
import types

import pytest

_awsglue = types.ModuleType('awsglue')
_utils = types.ModuleType('awsglue.utils')
_utils.getResolvedOptions = lambda argv, names: {}
_awsglue.utils = _utils
sys.modules.setdefault('awsglue', _awsglue)
sys.modules.setdefault('awsglue.utils', _utils)

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))


@pytest.fixture(scope='session')
def spark():
    """A local session, matching the Spark version Glue 5.0 runs.

    Yields:
        The session, stopped when the test session ends.
    """
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName('gz-csv-bulk-load-tests')
        .master('local[2]')
        .config('spark.sql.shuffle.partitions', '2')
        .config('spark.ui.enabled', 'false')
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def gz_csv(tmp_path):
    """Write a gzipped CSV and return its path.

    Returns:
        A callable taking a file name and the lines to write, returning the path.
    """

    def write(name: str, lines: list[str]) -> pathlib.Path:
        path = tmp_path / name
        path.write_bytes(gzip.compress(('\n'.join(lines) + '\n').encode()))
        return path

    return write
