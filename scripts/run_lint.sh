#!/usr/bin/env bash

source .venv/bin/activate
result=$(rye lint --fix)
if [ $? -ne 0 ]; then
    echo "Lint failed"
    echo "$result"
    exit 1
fi
echo "$result"
