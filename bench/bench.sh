#!/usr/bin/env bash
# Times every ingest path against the same table and the same 5M-row dataset.
# Each run truncates first, and the row count is checked afterwards, so a failed
# insert can never be reported as a fast one.
set -euo pipefail
export LC_ALL=C                    # one locale for printf and Python floats
cd "$(dirname "$0")/.."

OUT=results/runs.csv
RUNS=${RUNS:-3}
ROWS=${ROWS:-5000000}
[ -f "$OUT" ] || echo "approach,rows,seconds,rows_per_s" > "$OUT"

ch()   { docker compose exec -T clickhouse clickhouse-client "$@"; }
rec()  {                              # rec <name> <seconds> <expected rows>
  local n rps; n=$(ch -q "SELECT count() FROM bench.events")
  if [ "$n" != "$3" ]; then echo "!! $1 inserted $n of $3 rows — not counted" >&2; return 1; fi
  rps=$(python3 -c "print(round($3/$2))")
  echo "$1,$3,$2,$rps" >> "$OUT"
  printf '%-24s %8.2fs  %10s rows/s\n' "$1" "$2" "$rps"
}
time_ch() {                           # time_ch <name> <query>
  ch -q "TRUNCATE TABLE bench.events"
  local t0 t1; t0=$(date +%s.%N)
  ch -q "$2"
  t1=$(date +%s.%N)
  rec "$1" "$(python3 -c "print(round($t1-$t0,3))")" "$ROWS"
}
time_stdin() {                        # time_stdin <name> <query> <file>
  ch -q "TRUNCATE TABLE bench.events"  # truncate BEFORE the file is attached to stdin:
  local t0 t1; t0=$(date +%s.%N)       # otherwise the client eats the stream first
  ch -q "$2" < "$3"
  t1=$(date +%s.%N)
  rec "$1" "$(python3 -c "print(round($t1-$t0,3))")" "$ROWS"
}
time_py() {                           # time_py <name> <args...>
  ch -q "TRUNCATE TABLE bench.events"
  local out; out=$(docker compose run --rm -T bench python /app/bench/bench_py.py "${@:2}")
  # the script reports its own elapsed time: docker/container startup is not ingest time
  local secs; secs=$(python3 -c "import json,sys;print(json.loads(sys.argv[1])['seconds'])" "$out")
  rec "$1" "$secs" "$ROWS"
}

for i in $(seq 1 "$RUNS"); do
  echo "--- run $i/$RUNS ---"
  time_ch server_parquet_one   "INSERT INTO bench.events SELECT * FROM file('bench/big.parquet', Parquet)"
  time_ch server_parquet_glob10 "INSERT INTO bench.events SELECT * FROM file('bench/parts/*.parquet', Parquet)"
  time_ch server_parquet_glob200 "INSERT INTO bench.events SELECT * FROM file('bench/many/parts/*.parquet', Parquet)"
  time_stdin client_stdin_parquet "INSERT INTO bench.events FORMAT Parquet" data/big.parquet
  time_py py_arrow  --mode arrow  --rows "$ROWS"
  time_py py_pandas --mode pandas --rows "$ROWS"
  time_py py_arrow_manyfiles --mode arrow --rows "$ROWS" --file /data/many/parts
done

python3 bench/summarize.py results/runs.csv | tee results/summary.txt
