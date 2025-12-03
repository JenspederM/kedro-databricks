new-dev name="develop-eggs" *args="":
    uv run ./scripts/mkdev.py new {{ name }} {{ args }}

sync-dev name="develop-eggs" *args="":
    uv run ./scripts/mkdev.py sync {{ name }} {{ args }}

run-dev *args:
    uv run ./scripts/dev_flow.py {{ args }}

unittest:
    uv run --group test pytest tests/unit

test:
    uv run --group test pytest tests

lint:
    uv run --group dev ruff check --fix

format:
    uv run --group dev ruff format
