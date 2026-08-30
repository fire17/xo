#!/bin/sh
set -eu

repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
tmp=${TMPDIR:-/tmp}/xo-python-matrix-$$
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
mkdir -p "$tmp"

for version in 3.11 3.12 3.13 3.14; do
  env_dir="$tmp/$version"
  uv venv --python "$version" "$env_dir"
  uv pip install --python "$env_dir/bin/python" "$repo"
  "$env_dir/bin/python" "$repo/scripts/verify_python.py"
  PYTHONPATH="$repo/src" "$env_dir/bin/python" "$repo/benchmarks/check_budgets.py"
done
