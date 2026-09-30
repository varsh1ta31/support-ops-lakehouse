from __future__ import annotations

from typing import Any

import pytest

from support_ops.ingestion import batch


class FakeFrame:
    def __init__(self, count: int, unique_count: int | None = None) -> None:
        self._count = count
        self._unique_count = count if unique_count is None else unique_count
        self.view_name: str | None = None

    def dropDuplicates(self, columns: list[str]) -> FakeFrame:  # noqa: N802 - PySpark API
        assert columns == ["_record_hash"]
        return FakeFrame(self._unique_count)

    def count(self) -> int:
        return self._count

    def createOrReplaceTempView(self, name: str) -> None:  # noqa: N802 - PySpark API
        self.view_name = name


class FakeCatalog:
    def __init__(self) -> None:
        self.dropped: list[str] = []

    def dropTempView(self, view_name: str) -> bool:  # noqa: N802 - PySpark API
        self.dropped.append(view_name)
        return True


class CountFrame:
    def __init__(self, value: int) -> None:
        self.value = value

    def count(self) -> int:
        return self.value


class FakeSpark:
    def __init__(self, before: int, inserted: int) -> None:
        self.catalog = FakeCatalog()
        self.read: Any = None
        self.current_count = before
        self.inserted = inserted
        self.statements: list[str] = []

    def sql(self, query: str) -> None:
        self.statements.append(query)
        if query.startswith("MERGE INTO"):
            self.current_count += self.inserted

    def table(self, table_name: str) -> CountFrame:
        return CountFrame(self.current_count)


def test_bronze_ddl_keeps_source_values_raw() -> None:
    ddl = batch.bronze_table_ddl("support_dev", batch.SOURCES["accounts"])

    assert ddl.startswith("CREATE TABLE IF NOT EXISTS `support_dev`.`bronze`.`accounts`")
    assert "`annual_contract_value` STRING" in ddl
    assert "`_ingested_at` TIMESTAMP NOT NULL" in ddl
    assert ddl.endswith("USING DELTA")


def test_hash_columns_follow_source_schema_order() -> None:
    columns = batch.record_hash_columns(batch.SOURCES["tickets"])

    assert columns[:3] == ("ticket_id", "customer_id", "product_id")
    assert columns[-1] == "agent_id"


def test_ingestion_merges_unique_hashes_and_reports_metrics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spark = FakeSpark(before=50, inserted=7)
    prepared = FakeFrame(count=10, unique_count=8)
    monkeypatch.setattr(batch, "_prepare_batch", lambda *args: prepared)

    result = batch.ingest_batch(
        spark,
        entity="accounts",
        source_path="/landing/accounts",
        catalog="support_dev",
        pipeline_run_id="run-42",
    )

    assert result.records_read == 10
    assert result.records_written == 7
    assert result.duplicate_records == 3
    assert result.target_table == "support_dev.bronze.accounts"
    assert any(statement.startswith("MERGE INTO") for statement in spark.statements)
    assert spark.catalog.dropped[0].startswith("incoming_accounts_")


@pytest.mark.parametrize("entity", ["unknown", "ticket_events"])
def test_ingestion_rejects_unsupported_entity(entity: str) -> None:
    with pytest.raises(ValueError, match="Unsupported batch entity"):
        batch.ingest_batch(
            FakeSpark(0, 0),
            entity=entity,
            source_path="/landing",
            catalog="support_dev",
            pipeline_run_id="run",
        )


def test_ingestion_requires_run_id() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        batch.ingest_batch(
            FakeSpark(0, 0),
            entity="products",
            source_path="/landing",
            catalog="support_dev",
            pipeline_run_id=" ",
        )


def test_table_identifier_validation() -> None:
    with pytest.raises(ValueError, match="Invalid identifier"):
        batch.qualified_bronze_table("support-dev", "tickets")
