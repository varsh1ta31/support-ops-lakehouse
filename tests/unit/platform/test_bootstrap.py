from typing import Any

import pytest

from support_ops.platform.bootstrap import (
    SCHEMAS,
    VOLUMES,
    bootstrap_catalog,
    bootstrap_statements,
    main,
    quote_identifier,
)


class FakeSpark:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def sql(self, query: str) -> None:
        self.statements.append(query)


def test_bootstrap_statements_are_idempotent_and_dependency_ordered() -> None:
    statements = bootstrap_statements("support_dev")

    assert statements[0] == "CREATE CATALOG IF NOT EXISTS `support_dev`"
    assert len(statements) == 1 + len(SCHEMAS) + len(VOLUMES)
    assert all("IF NOT EXISTS" in statement for statement in statements)
    assert statements[-1].endswith("`support_dev`.`ops`.`checkpoints`")


def test_bootstrap_executes_every_statement() -> None:
    spark = FakeSpark()

    count = bootstrap_catalog(spark, "support_dev")

    assert count == len(spark.statements)
    assert spark.statements == list(bootstrap_statements("support_dev"))


@pytest.mark.parametrize("identifier", ["support-dev", "1support", "support dev", ""])
def test_identifier_validation_blocks_unsafe_names(identifier: str) -> None:
    with pytest.raises(ValueError, match="Invalid catalog identifier"):
        quote_identifier(identifier)


def test_main_uses_active_session(monkeypatch: pytest.MonkeyPatch, capsys: Any) -> None:
    spark = FakeSpark()
    monkeypatch.setattr("support_ops.platform.bootstrap._active_spark_session", lambda: spark)

    assert main(["--catalog", "support_dev"]) == 0
    assert "Executed 10" in capsys.readouterr().out
