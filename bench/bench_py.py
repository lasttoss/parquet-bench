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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["arrow", "pandas", "naive"], required=True)
    ap.add_argument("--rows", type=int, default=5_000_000)
    ap.add_argument("--chunk", type=int, default=200_000)
    ap.add_argument("--file", default="/data/big.parquet")
    a = ap.parse_args()

    client = clickhouse_connect.get_client(host=HOST, port=8123, compress=False)
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
                      "rows_per_s": round(inserted / elapsed)}))


if __name__ == "__main__":
    main()
