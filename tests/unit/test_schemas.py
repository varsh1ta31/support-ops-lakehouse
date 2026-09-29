import pytest

from support_ops.schemas import (
    ACCOUNTS,
    INGESTION_METADATA,
    SOURCE_SCHEMAS,
    TICKETS,
    DataType,
    Field,
    RecordSchema,
)


def test_all_source_schemas_have_unique_fields() -> None:
    for schema in SOURCE_SCHEMAS.values():
        names = [field.name for field in schema.fields]
        assert len(names) == len(set(names))


def test_required_identifier_is_not_nullable() -> None:
    assert not TICKETS.field("ticket_id").nullable
    assert not ACCOUNTS.field("customer_id").nullable


def test_schema_renders_spark_compatible_ddl() -> None:
    assert ACCOUNTS.as_ddl().startswith("customer_id STRING, customer_name STRING")


def test_ingestion_metadata_contract_is_complete() -> None:
    assert {field.name for field in INGESTION_METADATA} == {
        "_ingested_at",
        "_source_file",
        "_pipeline_run_id",
        "_record_hash",
    }


def test_duplicate_schema_fields_are_rejected() -> None:
    duplicate = Field("id", DataType.STRING, False)

    with pytest.raises(ValueError, match="duplicate"):
        RecordSchema("bad", (duplicate, duplicate))


def test_unknown_schema_field_is_explicit() -> None:
    with pytest.raises(KeyError, match="missing"):
        TICKETS.field("missing")
