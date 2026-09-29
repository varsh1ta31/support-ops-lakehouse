import pytest

from support_ops.contracts import Layer, TableRef, table


def test_registered_table_has_qualified_name() -> None:
    reference = table("support_dev", Layer.OPS, "invalid_records")

    assert reference.qualified_name == "support_dev.ops.invalid_records"


def test_invalid_identifier_is_rejected() -> None:
    with pytest.raises(ValueError, match="catalog"):
        TableRef("support-dev", Layer.BRONZE, "tickets")


def test_unknown_table_is_rejected() -> None:
    with pytest.raises(ValueError, match="Unknown gold table"):
        table("support_dev", Layer.GOLD, "made_up")
