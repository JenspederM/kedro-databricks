#!/usr/bin/env bash
result=$(uv run ruff check --fix .)
if [ $? -ne 0 ]; then
    echo "Lint failed"
    echo "$result"
    exit 1
fi
echo "$result"
result=$(uv run ruff format .)
if [ $? -ne 0 ]; then
    echo "Lint failed"
    echo "$result"
    exit 1
fi
echo "$result"
