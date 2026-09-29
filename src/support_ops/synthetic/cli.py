"""Command-line interface for reproducible dataset generation."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path

from support_ops.synthetic.config import load_generation_config
from support_ops.synthetic.generate import generate_dataset


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", type=Path, required=True, help="Generator TOML profile")
    parser.add_argument("--output", type=Path, required=True, help="Output directory")
    parser.add_argument(
        "--overwrite", action="store_true", help="Replace generator-managed files if present"
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    config = load_generation_config(arguments.profile)
    manifest = generate_dataset(config, arguments.output, overwrite=arguments.overwrite)
    print(json.dumps(manifest["files"], indent=2, sort_keys=True))
    return 0
