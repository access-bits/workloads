#!/usr/bin/env bash
set -euo pipefail

SF="${SF:-8}"
QUERY="${TPCH_QUERY:-}"        # empty = run all 22
DATA_DIR="${TPCH_DATA_DIR:-/data}"
OUTPUT_DIR="${TPCH_OUTPUT_DIR:-/results/output}"
TIMES_FILE="${TPCH_EXECUTION_TIMES:-/results/execution_times.txt}"

JAR=$(find /tpch-spark/target -name "*.jar" | head -1)

if [[ -z "$JAR" ]]; then
    echo "[run] ERROR: No jar found under /tpch-spark/target. Was the image built correctly?"
    exit 1
fi

if [[ ! -f "$DATA_DIR/lineitem.tbl" ]]; then
    echo "[run] ERROR: TPC-H data not found at $DATA_DIR. Run the generate step first."
    exit 1
fi

mkdir -p "$OUTPUT_DIR" "$(dirname "$TIMES_FILE")"

export TPCH_INPUT_DATA_DIR="file://$DATA_DIR"
export TPCH_QUERY_OUTPUT_DIR="file://$OUTPUT_DIR"
export TPCH_EXECUTION_TIMES="$TIMES_FILE"

echo "[run] SF=$SF  JAR=$JAR"
echo "[run] Data dir:    $DATA_DIR"
echo "[run] Output dir:  $OUTPUT_DIR"
echo "[run] Timings:     $TIMES_FILE"
[[ -n "$QUERY" ]] && echo "[run] Query: Q$QUERY" || echo "[run] Queries: all 1-22"

spark-submit \
    --master "local[*]" \
    --driver-memory "${SPARK_DRIVER_MEMORY:-8g}" \
    --conf "spark.sql.shuffle.partitions=${SPARK_SHUFFLE_PARTITIONS:-32}" \
    --conf "spark.ui.enabled=false" \
    --class "main.scala.TpchQuery" \
    "$JAR" \
    ${QUERY}

echo "[run] Complete. Timing results at $TIMES_FILE"
