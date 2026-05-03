#!/usr/bin/env bash
set -euo pipefail

SF="${SF:-8}"
DATA_DIR="${TPCH_DATA_DIR:-/data}"

mkdir -p "$DATA_DIR"

# Check if data already exists (lineitem.tbl is the largest — a good indicator)
if [[ -f "$DATA_DIR/lineitem.tbl" ]]; then
    echo "[generate] Data already present at $DATA_DIR (SF=$SF). Skipping dbgen."
    exit 0
fi

echo "[generate] Generating TPC-H data at scale factor SF=$SF into $DATA_DIR ..."

export DSS_CONFIG=/tpch-spark/dbgen
cd "$DATA_DIR"
/tpch-spark/dbgen/dbgen -s "$SF" -f -v

echo "[generate] Done. Files:"
ls -lh "$DATA_DIR/"*.tbl
