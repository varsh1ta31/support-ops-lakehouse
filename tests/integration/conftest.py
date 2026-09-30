"""Shared local Spark/Delta session: install .[spark-test] and supply Java 17+."""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from support_ops.transformations import silver


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Any]:
    pytest.importorskip("pyspark")
    delta = pytest.importorskip("delta")
    from pyspark.sql import SparkSession

    root = tmp_path_factory.mktemp("lakehouse")
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("support-ops-integration-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(root / "warehouse"))
        .config("spark.jars.ivy", str(Path(os.environ.get("SPARK_TEST_IVY", root / "ivy"))))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )
    session = delta.configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    for name in ("bronze", "silver", "ops"):
        session.sql(f"CREATE DATABASE {name} LOCATION '{root / name}'")
    for entity in ("accounts", "products"):
        silver.create_tables(session, "spark_catalog", entity)
    yield session
    session.stop()
