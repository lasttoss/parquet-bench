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

## Reading straight from GCS (no download)

Both functions are present in 24.8 and were verified against a public GCS object
(`cloud-samples-data/bigquery/us-states/us-states.parquet`, 50 rows):

```sql
-- native GCS function
SELECT count() FROM gcs('https://storage.googleapis.com/cloud-samples-data/bigquery/us-states/us-states.parquet', NOSIGN, 'Parquet');

-- GCS through its S3-compatible endpoint (works with wildcards and s3Cluster)
INSERT INTO bench.us_states (name, abbreviation)
SELECT name, abbreviation
FROM s3('https://storage.googleapis.com/cloud-samples-data/bigquery/us-states/*.parquet', NOSIGN, 'Parquet');
```

Notes taken from actually running this:

- `SELECT *` inside `INSERT … SELECT` from a parquet URL raised a syntax error on
  this build; listing the columns explicitly works.
- Private buckets need **HMAC interoperability keys** (GCS console → Settings →
  Interoperability), not a service-account JSON file.
- `NOSIGN` works for public objects, which is enough for a smoke test.
- Wildcards over a prefix are what makes this parallel: ClickHouse reads objects
  concurrently and merges the parsed blocks.
- Egress from GCS is billed and is usually the real limit; running ClickHouse in the
  same region removes that cost. Streaming was verified for correctness here, not
  for throughput: the public test object is ~1 KB, so no MB/s number is claimed.

## Real-world parquet over the internet (public samples)

Two public datasets were loaded straight from HTTPS into ClickHouse, no local copy
(the same mechanism as reading from GCS):

| source | parquet | rows | seconds | rows/s | MB/s |
|---|---:|---:|---:|---:|---:|
| `datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_1.parquet` | 174 MB | 1,000,000 | 18.5 | 54,000 | 9.4 |
| `.../trip-data/yellow_tripdata_2023-01.parquet` (NYC TLC) | 47.9 MB | 3,066,766 | 5.8 | 528,000 | 8.1 |
| three monthly NYC files, 3 concurrent inserts | 143.7 MB | 9,605,971 | 17.2 | 559,000 | 8.4 |
| raw `curl` of one NYC file, for reference | 47.9 MB | — | 4.9 | — | 9.8 |

The link saturates at ~9 MB/s (~75 Mbit/s): three concurrent streams give 8.4 MB/s
total, i.e. no scaling. Parallel loading only helps once the data is local, or when
ClickHouse runs in the same cloud region as the bucket.

Consequence for a backfill: 500M rows at 40-100 bytes/row compressed is 20-50 GB,
which is 40-100 minutes over this link and 2-5 minutes from a local disk. Streaming
saves disk space, not wall-clock time, unless the network is fast.

## Real data from local disk (what a backfill actually costs)

Public datasets downloaded once, then loaded from disk with `INSERT … SELECT FROM file()`.
Throughput is reported in MB/s because rows/s depends entirely on row width:

| dataset | rows | parquet | B/row | streams | ORDER BY | seconds | rows/s | MB/s |
|---|---:|---:|---:|---:|---|---:|---:|---:|
| NYC TLC yellow taxi, 20 months | 64,698,405 | 1.10 GB | 17 | 1 | `tuple()` | 17.1 | 3,783,532 | 65 |
| same | 64,698,405 | 1.10 GB | 17 | 4 | `tuple()` | 6.7 | 9,656,478 | 165 |
| same | 64,698,405 | 1.10 GB | 17 | 4 | `(VendorID, tpep_pickup_datetime)` | 10.5 | 6,137,682 | 105 |
| ClickHouse `hits`, 4 files | 4,000,000 | 740 MB | 185 | 1 | `tuple()` | 6.0 | 666,667 | 123 |
| same | 4,000,000 | 740 MB | 185 | 4 | `tuple()` | 2.6 | 1,538,462 | 285 |

Planning rule from these numbers, for 4 concurrent inserts with a real sorting key:
**minutes ≈ gigabytes / 6.6** (105 MB/s). With `ORDER BY tuple()` divide by 10.

| rows | at 17 B/row | at 50 B/row | at 100 B/row | at 185 B/row |
|---|---:|---:|---:|---:|
| 50M | 0.1 min | 0.4 min | 0.8 min | 1.4 min |
| 500M | 1.3 min | 3.8 min | 7.6 min | 14.0 min |

So 500M rows fits in 5 minutes when the parquet is under ~30 GB, which is what a
50-60 B/row event schema produces. Wide rows (nested `event_params`) do not: a
sorting key costs 1.6x on top, and the honest way to plan is to measure bytes/row on
one sample file and divide.

Caveats: files were in page cache (written minutes earlier), single node, no
replication, background merges still running afterwards (`system.parts` showed 38
active parts on the taxi table), and an in-memory filesystem or faster disk would move
these numbers more than any ClickHouse setting here.

## Nested schema (Firebase / GA4 shape)

`scripts/gen_firebase.py` builds the shape a BigQuery `EXPORT DATA` of
`analytics_<id>.events_*` produces: `event_params` as a repeated record of
key/value(struct of string/int/float/double), plus `user_properties`, and
`device`/`geo`/`traffic_source` structs, 8-14 params per event. Measured 96 bytes per
row after zstd — a "1 KB record" in JSON compresses to roughly a tenth of that in
parquet, because the keys repeat.

| layout | rows | parquet | streams | ORDER BY | seconds | rows/s | MB/s |
|---|---:|---:|---:|---|---:|---:|---:|
| nested | 2,000,000 | 192 MB | 1 | `tuple()` | 3.88 | 515,000 | 49 |
| nested | 2,000,000 | 192 MB | 4 | `tuple()` | 1.76 | 1,136,000 | 109 |
| nested | 2,000,000 | 192 MB | 4 | `(event_name, event_timestamp)` | 2.75 | 727,000 | 70 |

Nested parsing costs roughly 1.5x against a flat schema at the same byte count, and
the table on disk is 1.86x the parquet size (357 MiB vs 192 MB). ClickHouse inferred
every column as `Nullable(...)` from the parquet metadata, which is what BigQuery
exports produce; those columns are slower and bigger than their non-nullable
equivalents, so a `CREATE TABLE` with explicit non-nullable types is worth the effort.

### Where a 30-minute load actually goes

Parsing the JSON form of these records, not inserting them, is usually the real cost.
Measured with the stdlib `json` module on 1.5 KB records: **185 MB/s, 117,000 records/s**,
so 50M records (79 GB of JSON) is 7.1 minutes just to parse — before building Python
objects, converting to a DataFrame, or inserting anything. Add those and 20-35 minutes
is expected, which matches a "30 minutes for 50M records" experience. Going
JSON -> parquet -> ClickHouse removes that step entirely.

## CPU and RAM of the load itself (cgroup, not self-reported)

Sampled from the ClickHouse container's cgroup v2 (`cpu.stat`, `memory.stat`) every
50 ms. `anon` is the heap; page cache is reported separately because `file()` reads
through it and it inflates the naive "container memory" number.

| load | rows | seconds | rows/s | CPU avg | CPU-seconds | RAM anon peak | with page cache |
|---|---:|---:|---:|---:|---:|---:|---:|
| flat, 1 INSERT `file()` | 50,000,000 | 37.9 | 1,317,703 | 2.4 cores | 90 | 2,530 MB | 8,166 MB |
| flat, 4 concurrent INSERTs | 50,000,000 | 17.1 | 2,929,040 | 6.2 cores | 107 | 5,124 MB | 9,726 MB |
| nested (Firebase shape), 4 concurrent | 2,000,000 | 3.4 | 588,000 | 6.6 cores | 22 | 6,525 MB | — |

Rows were verified with `count()` and `sum(session_ms)` on both flat runs (identical),
so the faster run is not a partial load.

Three things follow from this:

1. **CPU work is constant, parallelism only redistributes it.** 90 vs 107 CPU-seconds
   for the same 50M rows, while wall clock halves. A 5-minute budget for 50M rows needs
   about 0.3 cores on average; the limit is never CPU on this machine.
2. **RAM scales with concurrent streams, not with total rows.** One stream peaks at
   2.5 GB of heap, four streams at 5.1 GB. Chunked client-side writes hold 1.1 GB flat
   at 50M rows (measured earlier).
3. **Nested nullable columns are memory-hungry.** 2M rows of `Array(Tuple(Nullable ...))`
   already peak at 6.5 GB with four streams, because every key and value arrives as
   `Nullable`. Capping `max_threads` and the number of concurrent inserts is the
   cheapest way to bound it.

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
