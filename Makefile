.PHONY: install format lint test build check

install:
	python3 -m pip install -e '.[dev]'

format:
	python3 -m ruff format .
	python3 -m ruff check --fix .

lint:
	python3 -m ruff format --check .
	python3 -m ruff check .
	python3 -m mypy

test:
	python3 -m pytest

build:
	python3 -m build --wheel

check: lint test
