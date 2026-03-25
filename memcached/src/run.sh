#!/bin/bash
set -e

WORKLOAD=""
CONFIG="config.json"

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
      echo "Usage: $0 --workload <workload> --config <config.json>"
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
OPERATIONCOUNT=$(python3 -c "import json; d=json.load(open('$CONFIG')); print(d['${WORKLOAD}-operationcount'])")

echo "=== YCSB Run ==="
echo "Workload       : $WORKLOAD"
echo "OperationCount : $OPERATIONCOUNT"
echo "Threads        : $THREADCOUNT"
echo "Host           : $HOST:$PORT"
echo "Percentiles    : 50p 75p 90p 95p 99p 99.9p 99.99p"
echo "================"

./bin/ycsb run memcached -s \
  -P workloads/$WORKLOAD \
  -p measurementtype=hdrhistogram \
  -p hdrhistogram.output=true \
  -p hdrhistogram.percentiles=50,75,90,95,99,99.9,99.99 \
  -p threadcount=$THREADCOUNT \
  -p memcached.hosts=$HOST:$PORT \
  -p operationcount=$OPERATIONCOUNT
