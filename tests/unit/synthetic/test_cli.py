from pathlib import Path

from support_ops.synthetic.cli import main


def test_cli_generates_profile(tmp_path: Path, capsys: object) -> None:
    profile = tmp_path / "profile.toml"
    output = tmp_path / "output"
    profile.write_text(
        """\
number_of_customers = 2
number_of_products = 2
number_of_tickets = 3
events_per_ticket = 1
seed = 1

[date_range]
start = 2025-01-01
end = 2025-01-02
""",
        encoding="utf-8",
    )

    result = main(["--profile", str(profile), "--output", str(output)])

    assert result == 0
    assert (output / "manifest.json").is_file()
