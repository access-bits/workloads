#!/bin/bash
set -e

WORKLOAD=""
CONFIG="ycsb-config.json"

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --workload)
      WORKLOAD="$2"
      shift 2
      ;;
    --config)
      CONFIG="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1"
      echo "Usage: $0 --workload <workload> --config <ycsb-config.json>"
      exit 1
      ;;
  esac
done

# Validate required arguments
if [[ -z "$WORKLOAD" ]]; then
  echo "Error: --workload is required (e.g. workloada, workloadb, workloadc)"
  exit 1
fi

if [[ ! -f "$CONFIG" ]]; then
  echo "Error: config file '$CONFIG' not found"
  exit 1
fi

# Read values from config.json
THREADCOUNT=$(python3 -c "import json; d=json.load(open('$CONFIG')); print(d['threadcount'])")
HOST=$(python3 -c "import json; d=json.load(open('$CONFIG')); print(d['memcached-host'])")
PORT=$(python3 -c "import json; d=json.load(open('$CONFIG')); print(d['memcached-port'])")
RECORDCOUNT=$(python3 -c "import json; d=json.load(open('$CONFIG')); print(d['${WORKLOAD}-recordcount'])")

echo "=== YCSB Load ==="
echo "Workload    : $WORKLOAD"
echo "RecordCount : $RECORDCOUNT"
echo "Threads     : $THREADCOUNT"
echo "Host        : $HOST:$PORT"
echo "================="

./bin/ycsb load memcached -s \
  -P workloads/$WORKLOAD \
  -p threadcount=$THREADCOUNT \
  -p memcached.hosts=$HOST:$PORT \
  -p recordcount=$RECORDCOUNT
