#!/usr/bin/env bash
# Builds raftkv and kvctl and runs a local three-node cluster on
# 127.0.0.1:8001-8003 until Ctrl+C. Data and logs go under ./data/.
#
#   scripts/cluster.sh          start
#   scripts/cluster.sh clean    wipe ./data first
set -euo pipefail

cd "$(dirname "$0")/.."
if [[ "${1:-}" == "clean" ]]; then
	rm -rf data
	echo "removed ./data"
fi

echo "building..."
go build -o bin/ ./cmd/raftkv ./cmd/kvctl
mkdir -p data

peers="n1=127.0.0.1:8001,n2=127.0.0.1:8002,n3=127.0.0.1:8003"
pids=()
stop() {
	echo
	echo "stopping..."
	kill "${pids[@]}" 2>/dev/null || true
	wait "${pids[@]}" 2>/dev/null || true
}
trap stop EXIT INT TERM

for i in 1 2 3; do
	id="n$i"
	./bin/raftkv -id "$id" -addr "127.0.0.1:800$i" -peers "$peers" -data "data/$id" 2>"data/$id.log" &
	pids+=($!)
	echo "started $id on 127.0.0.1:800$i (pid $!, log data/$id.log)"
done

cat <<EOF

in another terminal:
  export RAFTKV_SERVERS=127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003
  ./bin/kvctl put greeting hello
  ./bin/kvctl get greeting
  ./bin/kvctl status

Ctrl+C stops all three nodes.
EOF
wait
