"""Shared Spark session for the tests: local, two cores, small shuffles, UTC, no UI."""
from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """Delta Lake is enabled so the per-month write behaviour the jobs rely on is tested, not assumed."""
    from delta import configure_spark_with_delta_pip
    builder = (SparkSession.builder.master("local[2]").appName("epd-tests")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    s = configure_spark_with_delta_pip(builder).getOrCreate()
    yield s
    s.stop()


@pytest.fixture(scope="session")
def bronze(spark):
    """Real rows from the NHSBSA API: three Bolton products over two months, plus two placeholder-prescriber rows."""
    from epd_lakehouse.bronze import read_batch
    return read_batch(spark, str(FIXTURES / "*.csv"))
