.PHONY: up down data schema bench clean
up:      ; docker compose up -d clickhouse
down:    ; docker compose down
data:    ; docker compose run --rm -T bench python /app/scripts/gen_parquet.py --rows 5000000 --parts 10 --out /data
data200: ; docker compose run --rm -T bench python /app/scripts/gen_parquet.py --rows 5000000 --parts 200 --out /data/many
schema:  ; docker compose exec -T clickhouse clickhouse-client --multiquery < bench/schema.sql
bench:   ; RUNS=3 ./bench/bench.sh
clean:   ; docker compose down -v && rm -rf data results/runs.csv
