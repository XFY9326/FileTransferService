#!/usr/bin/env bash

scriptDir="$(dirname "$(realpath "$0")")"
cd "$scriptDir" || exit

echo "Current working dir: $scriptDir"

echo "Launching script..."

uv run src/fts/sender.py

# shellcheck disable=SC2162
read -p "Press any key to continue..."
