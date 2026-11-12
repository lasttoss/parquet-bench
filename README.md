# parquet-bench

Does reading Parquet on the ClickHouse server actually beat a Python script? This
repo measures it instead of repeating the claim.

Same dataset, same table, same machine. Each path truncates the table first, and
the row count is verified afterwards, so a failed insert cannot be reported as a
fast one. Every number below is the median of 3 runs.

## Dataset

5,000,000 event rows, 13 columns, zstd parquet, ~266 MB total (46 bytes/row after
compression, ~106 bytes/row before):

```
event_id UInt64 · ts DateTime64(3) · user_id UInt64 · session_id UInt64
game_id, event, platform, country  LowCardinality(String)  (dictionary-encoded in parquet)
level UInt16 · session_ms UInt32 · revenue_cents UInt64 · is_payer UInt8 · props String
```

Three layouts of the *same* rows, so file count is the only variable:

| layout | files | file size |
|---|---|---|
| `big.parquet` | 1 | 133 MB |
| `parts/` | 10 | ~13 MB each |
| `many/parts/` | 200 | ~0.7 MB each |

## Machine and versions

```
16 vCPU · 22 GB RAM · local NVMe (no network storage)
ClickHouse 24.8.14.39, single node, default settings (only max_server_memory_usage_to_ram_ratio=0.6)
Python 3.12 · pyarrow 17.0 · clickhouse-connect 0.8.6 · pandas 2.2.3
Server, client and script all on the same docker network: no port forwarding, no network hop
```

## Results (median of 3, 5M rows)

| approach | what runs where | seconds | rows/s | MB/s | vs best |
|---|---|---:|---:|---:|---:|
| `server_parquet_glob10` | `INSERT … SELECT * FROM file('parts/*.parquet')` | 2.99 | 1,671,123 | 76.9 | 1.01x |
| `server_parquet_one` | `INSERT … SELECT * FROM file('big.parquet')` | 3.10 | 1,615,509 | 74.3 | 1.04x |
| `server_parquet_glob200` | same, 200 files | 3.22 | 1,552,313 | 71.4 | 1.09x |
| `py_arrow` | script → `clickhouse-connect.insert_arrow`, 200k-row batches | 3.25 | 1,540,832 | 70.9 | 1.09x |
| `client_stdin_parquet` | `clickhouse-client … FORMAT Parquet < big.parquet` | 3.30 | 1,513,775 | 69.6 | 1.11x |
| `py_arrow_manyfiles` | script, opening 200 small files one by one | 3.50 | 1,428,571 | 65.7 | 1.18x |
| `py_pandas` | script → `insert_df`, 200k-row batches | 10.47 | 477,464 | 22.0 | 3.53x |

Separately measured, because it is too slow to finish in the same loop:

| approach | rows | seconds | rows/s |
|---|---:|---:|---:|
| `py_naive` — Python objects, 10k rows per INSERT | 500,000 | 5.56 | 90,016 |

Data integrity check: server-side `file()` and Arrow insert produce identical
`count()`, `sum(session_ms)`, `sum(revenue_cents)` and `uniqExact(user_id)`.

## Scale test: 50M rows (the number that matters in practice)

Same table, same machine, one 1.18 GB parquet file (10× the rows). Throughput is
flat, and memory with chunked writes is flat too — measured peak of the container,
not self-reported:

| approach | rows | seconds | rows/s | peak container RAM |
|---|---:|---:|---:|---:|
| `server file()` | 50,000,000 | 30.7 | 1,630,056 | — (server side) |
| `insert_arrow`, 200k batches | 50,000,000 | 34.6 | 1,446,930 | **1,096 MB** |
| `insert_df` (pandas), 200k batches | 50,000,000 | 105.6 | 473,407 | **1,113 MB** |
| `insert_df`, whole file in one DataFrame (5M rows) | 5,000,000 | 9.1 | 548,431 | 1,049 MB |

Memory does not grow with the file when the reader is chunked: 1.1 GB at both 5M and
50M rows. It grows with how much a script holds at once — the unchunked 5M run
already costs 1.05 GB (~210 bytes/row in pandas), so a 350M-row backfill loaded in
one DataFrame would need roughly 73 GB. That matches the "72 GB of RAM" experience
people report; it is a property of the script holding everything, not of Parquet and
not of ClickHouse.

## Batch size: where the "10k rows/s" belief comes from

Measured on 2M rows (and 10k rows for the last row, because it is far too slow to
run at scale), same table, same machine, only the batch size changing:

| rows per INSERT | rows/s | note |
|---:|---:|---|
| 1 | 475 | one INSERT per row: 10,000 rows took 21s |
| 1,000 | 368,141 | |
| 10,000 | 1,163,980 | |
| 100,000 | 1,507,603 | |
| 1,000,000 | 1,543,072 | |
| one `file()` call (5M) | 1,630,056 | no client-side batching at all |

The "10,000–100,000" figure that circulates is a recommendation for **how many rows to
put in one INSERT**, not a throughput ceiling. Throughput is bounded by the number of
INSERT statements (each one creates a part that has to be merged later), not by the
number of rows: 25 inserts of 200k rows created 17 active parts and ran at 1.5M rows/s,
while 10,000 inserts of one row each ran at 475 rows/s.

Relevant defaults measured on this build: `max_insert_block_size=1,048,449`,
`max_partitions_per_insert_block=100`, `async_insert=0`.

## Parallel load

4 files × 5M rows, same table, on this 16-vCPU box:

| mode | seconds | rows/s | speedup |
|---|---:|---:|---:|
| one INSERT after another | 12.48 | 1,603,191 | 1.00x |
| 4 INSERTs concurrently | 4.34 | 4,603,873 | 2.87x |

Throughput scales with concurrent INSERT statements, not with the size of one
statement, so a backfill is limited by how many loader streams the disk and CPU can
feed — not by ClickHouse's single-insert rate.

## Findings

1. **Server-side Parquet is only ~5% faster than an Arrow insert** (1.62M vs 1.54M
   rows/s). The "read parquet on the server and it is 10x faster" claim does not
   survive measurement when the client is already sending Arrow batches over
   HTTP on the same host.
2. **The gap that is real is pandas.** `insert_df` is 3.5x slower than
   `insert_arrow` (0.48M vs 1.54M rows/s), on identical data and identical
   batches. The conversion to Python objects costs more than the insert itself.
3. **Row-by-row inserts are ~18x slower** (90k rows/s). At that rate 5M rows takes
   ~55s instead of ~3s. This is extrapolation from a measured 500k rows, not a
   measured 5M run.
4. **File count did not matter on local NVMe.** 1 file, 10 files and 200 files all
   land within 8% of each other, because the server reads files in parallel across
   cores. The small-file penalty people warn about is a property of object storage
   (one GET per object), which this repo does **not** measure — no S3/GCS
   credentials were used.
5. **Streaming parquet through the client is not slower here** (1.51M rows/s),
   because client and server run on the same host. Over a real network the
   compression and the transfer are paid twice, once by the client and once by
   the server.

## What this does not prove

- Single node, no replication, no `ReplicatedMergeTree`, no materialized views.
- 13 flat columns with no nested/array types; nested structures change parsing cost.
- No concurrent queries or writes while loading; a busy cluster behaves differently.
- Local NVMe only. S3/GCS reads, remote object storage and network latency are untested.
- Python paths use HTTP (`clickhouse-connect`), not the native TCP port or `clickhouse-local`.
- One insert per approach per run: numbers describe throughput for a single writer,
  not steady-state compaction behaviour.

## Reproduce

```bash
docker compose up -d clickhouse
docker compose run --rm -T bench python /app/scripts/gen_parquet.py --rows 5000000 --parts 10 --out /data
docker compose run --rm -T bench python /app/scripts/gen_parquet.py --rows 5000000 --parts 200 --out /data/many
docker compose exec -T clickhouse clickhouse-client --multiquery < bench/schema.sql
RUNS=3 ./bench/bench.sh          # writes results/runs.csv and prints results/summary.txt
```

## Layout

```
docker-compose.yml   clickhouse + bench python image, no host ports (so port forwarding is never measured)
Dockerfile.bench     pyarrow + clickhouse-connect + pandas
bench/schema.sql     the target table
bench/bench_py.py    script-side paths: arrow, pandas, naive
bench/bench.sh       truncates, times, verifies row count, records
bench/summarize.py   median per approach + ratio against the fastest
results/             summary.txt kept; raw csv is gitignored and regenerated
```

MIT.
