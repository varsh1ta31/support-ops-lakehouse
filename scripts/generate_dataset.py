"""Compatibility wrapper for the installed ``support-ops-generate`` command."""

from support_ops.synthetic.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
