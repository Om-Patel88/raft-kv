#!/usr/bin/env bash
# Builds the browser demo into web/: raftsim.wasm plus the matching
# wasm_exec.js from the local Go install. Serve web/ with any static server.
set -euo pipefail

cd "$(dirname "$0")/.."

GOOS=js GOARCH=wasm go build -trimpath -ldflags="-s -w" -o web/raftsim.wasm ./cmd/raftsim

goroot="$(go env GOROOT)"
src="$goroot/lib/wasm/wasm_exec.js"
if [ ! -f "$src" ]; then
	src="$goroot/misc/wasm/wasm_exec.js" # Go 1.23 and older
fi
cp "$src" web/wasm_exec.js

size=$(wc -c < web/raftsim.wasm)
echo "web/raftsim.wasm $((size / 1024)) KiB"
