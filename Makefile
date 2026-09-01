.PHONY: install lint fmt fmt-check test check

install:
	pip install -e ".[dev]"

lint:
	ruff check .

fmt:
	ruff format .

fmt-check:
	ruff format --check .

test:
	pytest -q

check: lint fmt-check test
