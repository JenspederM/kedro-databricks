#!/usr/bin/env bash

CUR_PATH=$(pwd)
VERSION=$(grep -m 1 version pyproject.toml | tr -s ' ' | tr -d '"' | tr -d "'" | cut -d' ' -f3)
WHL=kedro_databricks-$VERSION-py3-none-any.whl

if [ -z "$1" ]; then
  echo "Usage: $0 <project_name>"
  exit 1
fi

if test -d "$CUR_PATH/$1"; then
  echo "Directory $1 already exists."
  read -p "Do you want to remove is? (y/n)?" choice
  case "$choice" in
    y|Y ) echo "yes";;
    n|N ) echo "no"; exit 0;;
    * ) echo "invalid"; exit 1;;
  esac
  rm -rf "$CUR_PATH/$1"
fi

# Build package
rye build

# Create a new project
kedro new --starter="databricks-iris" --name="$1"
# kedro new --starter=databricks-iris --name="$1"

# Databricks needs Java
echo "java openjdk-21" >> "$CUR_PATH/$1/.tool-versions"

# Copy the package to the project directory
cp "dist/$WHL" "$CUR_PATH/$1/$WHL"

# Move to the project directory
pushd "$CUR_PATH/$1"
# Create a virtual environment
python3 -m venv .venv
# Activate the virtual environment
source .venv/bin/activate
# Install the project dependencies
pip install --upgrade uv
uv pip install -r requirements.txt
uv pip install $WHL --force-reinstall
code .
popd
