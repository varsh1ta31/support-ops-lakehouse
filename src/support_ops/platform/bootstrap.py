"""Create the governed namespace and Volume structure required by the project."""

from __future__ import annotations

import argparse
import re
from collections.abc import Sequence
from typing import Any, Protocol, cast

SCHEMAS = ("raw", "bronze", "silver", "gold", "ml", "ops")
VOLUMES = (("raw", "landing"), ("raw", "ticket_events"), ("ops", "checkpoints"))
IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


class SqlExecutor(Protocol):
    def sql(self, query: str) -> Any: ...


def quote_identifier(value: str) -> str:
    """Validate and quote a Unity Catalog identifier."""

    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"Invalid catalog identifier: {value!r}")
    return f"`{value}`"


def bootstrap_statements(catalog: str) -> tuple[str, ...]:
    """Return idempotent SQL in dependency order."""

    quoted_catalog = quote_identifier(catalog)
    statements = [f"CREATE CATALOG IF NOT EXISTS {quoted_catalog}"]
    statements.extend(
        f"CREATE SCHEMA IF NOT EXISTS {quoted_catalog}.{quote_identifier(schema)}"
        for schema in SCHEMAS
    )
    statements.extend(
        "CREATE VOLUME IF NOT EXISTS "
        f"{quoted_catalog}.{quote_identifier(schema)}.{quote_identifier(volume)}"
        for schema, volume in VOLUMES
    )
    return tuple(statements)


def bootstrap_catalog(spark: SqlExecutor, catalog: str) -> int:
    """Execute the namespace bootstrap and return the number of statements run."""

    statements = bootstrap_statements(catalog)
    for statement in statements:
        spark.sql(statement)
    return len(statements)


def _active_spark_session() -> SqlExecutor:
    try:
        from pyspark.sql import SparkSession
    except ImportError as error:
        raise RuntimeError("PySpark is required to bootstrap the catalog") from error
    return cast(SqlExecutor, SparkSession.builder.getOrCreate())


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    arguments = parser.parse_args(argv)
    count = bootstrap_catalog(_active_spark_session(), arguments.catalog)
    print(f"Executed {count} idempotent bootstrap statements for {arguments.catalog}")
    return 0
