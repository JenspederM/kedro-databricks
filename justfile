new-dev name="develop-eggs":
    uv run ./scripts/mkdev.py new {{ name }}

sync-dev name="develop-eggs":
    uv run ./scripts/mkdev.py sync {{ name }}

run-dev *args:
    uv run ./scripts/dev_flow.py {{ args }}

test:
    uv run --group test pytest tests

lint:
    uv run --group dev ruff check --fix

format:
    uv run --group dev ruff format
