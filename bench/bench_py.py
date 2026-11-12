"""Script-side ingest paths: the ones people actually write.

Run inside the bench container:
    python /app/bench/bench_py.py --mode arrow|pandas|naive --rows 5000000
Every mode reads the same parquet file and inserts into the same table, so the
only thing that changes is how the rows travel: Arrow batches, pandas frames, or
one small INSERT per batch with Python objects.
"""
import argparse, json, time
import clickhouse_connect
import pyarrow.parquet as pq

HOST = "clickhouse"


def peak_mb():
    """Peak memory of this container, measured by the kernel (cgroup v2), not by self-reported RSS."""
    for path in ("/sys/fs/cgroup/memory.peak", "/sys/fs/cgroup/memory/memory.max_usage_in_bytes"):
        try:
            v = int(open(path).read().split()[0])
            return round(v / 1e6)
        except Exception:
            continue
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["arrow", "pandas", "naive", "pandas_full", "arrow_transform"],
                    required=True)
    ap.add_argument("--rows", type=int, default=5_000_000)
    ap.add_argument("--chunk", type=int, default=200_000)
    ap.add_argument("--file", default="/data/big.parquet")
    a = ap.parse_args()

    client = clickhouse_connect.get_client(host=HOST, port=8123, compress=False)
    if a.mode == "arrow_transform":
        # the realistic ETL: read a batch, transform it with Arrow compute, insert it.
        # Nothing is ever fully materialised, so memory is a function of batch size only.
        import pyarrow as pa, pyarrow.compute as pc
        t0 = time.perf_counter()
        kept = 0
        for batch in pq.ParquetFile(a.file).iter_batches(batch_size=a.chunk):
            t = pa.Table.from_batches([batch])
            t = t.append_column("event_hour", pc.hour(pc.cast(t["ts"].combine_chunks(), pa.timestamp("s"), safe=False)))
            t = t.append_column("is_revenue", pc.greater(t["revenue_cents"], 0))
            t = t.filter(pc.greater(t["session_ms"], 0))          # drop clock-skew rows
            t = t.drop_columns(["props"])                          # not needed downstream
            client.insert_arrow("bench.events", t)
            kept += t.num_rows
            if kept >= a.rows:
                break
        el = time.perf_counter() - t0
        print(json.dumps({"mode": a.mode, "rows": kept, "seconds": round(el, 3),
                          "rows_per_s": round(kept / el), "peak_mb": peak_mb()}))
        return
    if a.mode == "pandas_full":
        # what a script does when nobody told it to chunk: the whole file as one DataFrame
        import pandas as pd, pyarrow.parquet as _pq
        t0 = time.perf_counter()
        df = _pq.read_table(a.file).to_pandas()
        client.insert_df("bench.events", df)
        el = time.perf_counter() - t0
        print(json.dumps({"mode": a.mode, "rows": len(df), "seconds": round(el, 3),
                          "rows_per_s": round(len(df) / el), "peak_mb": peak_mb()}))
        return
    # a directory means "many small files": the script opens them one by one, which is
    # exactly the case where server-side file() and a script stop behaving the same.
    import os, glob
    files = sorted(glob.glob(os.path.join(a.file, "*.parquet"))) if os.path.isdir(a.file) else [a.file]
    iterator = []
    for f in files:
        iterator.append(pq.ParquetFile(f))
    todo = a.rows
    t0 = time.perf_counter()
    inserted = 0
    def batches():
        for pf in iterator:
            step = a.chunk if not os.path.isdir(a.file) else 1 << 30   # dir mode: one insert per file
            for b in pf.iter_batches(batch_size=step):
                yield b

    for batch in batches():
        if todo <= 0:
            break
        if batch.num_rows > todo:
            batch = batch.slice(0, todo)
        if a.mode == "arrow":
            client.insert_arrow("bench.events", batch)
        elif a.mode == "pandas":
            client.insert_df("bench.events", batch.to_pandas())
        else:                      # naive: per-row objects, 10k at a time
            cols = batch.to_pydict()
            names = list(cols)
            step = 10_000
            for i in range(0, batch.num_rows, step):
                block = [[cols[c][i + j] for c in names] for j in range(min(step, batch.num_rows - i))]
                client.insert("bench.events", block, column_names=names)
        inserted += batch.num_rows
        todo -= batch.num_rows
    elapsed = time.perf_counter() - t0
    print(json.dumps({"mode": a.mode, "rows": inserted, "seconds": round(elapsed, 3),
                      "rows_per_s": round(inserted / elapsed), "peak_mb": peak_mb()}))


if __name__ == "__main__":
    main()
