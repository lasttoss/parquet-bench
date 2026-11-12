"""Generate the same event dataset twice: one big file, and 10 small part files.

Both layouts carry identical rows so the benchmark can compare "one file" against
"many small files" without changing anything else.
"""
import argparse, os, numpy as np, pyarrow as pa, pyarrow.parquet as pq

GAMES = ["screw_land", "knit_away", "camo_guys", "panthera", "dicegeon"]
EVENTS = ["level_start", "level_end", "iap", "reward_claim", "session_ping", "ad_impression"]
PLATFORMS = ["ios", "android"]
COUNTRIES = ["VN", "US", "JP", "KR", "DE", "BR", "ID", "TH"]


def build(start: int, n: int, rng) -> pa.Table:
    ts0 = np.int64(1_760_000_000_000)
    return pa.table({
        "event_id": pa.array(np.arange(start, start + n, dtype=np.uint64)),
        "ts": pa.array(ts0 + np.sort(rng.integers(0, 86_400_000 * 3, n)).astype(np.int64) + np.arange(n),
                       type=pa.timestamp("ms")),
        "user_id": pa.array(rng.integers(1, 2_000_000, n, dtype=np.uint64)),
        "session_id": pa.array(rng.integers(1, 8_000_000, n, dtype=np.uint64)),
        "game_id": pa.array(rng.choice(GAMES, n)).dictionary_encode(),
        "event": pa.array(rng.choice(EVENTS, n)).dictionary_encode(),
        "platform": pa.array(rng.choice(PLATFORMS, n)).dictionary_encode(),
        "country": pa.array(rng.choice(COUNTRIES, n)).dictionary_encode(),
        "level": pa.array(rng.integers(1, 500, n, dtype=np.uint16)),
        "session_ms": pa.array(rng.integers(0, 3_600_000, n, dtype=np.uint32)),
        "revenue_cents": pa.array(np.where(rng.random(n) < 0.03, rng.integers(99, 9999, n), 0).astype(np.uint64)),
        "is_payer": pa.array((rng.random(n) < 0.07).astype(np.uint8)),
        "props": pa.array(rng.choice(['{"ab":"a","src":"push"}', '{"ab":"b"}', '{}', '{"src":"organic"}'], n)).dictionary_encode(),
    })


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=5_000_000)
    ap.add_argument("--parts", type=int, default=10)
    ap.add_argument("--out", default="/data")
    a = ap.parse_args()
    rng = np.random.default_rng(42)
    os.makedirs(f"{a.out}/parts", exist_ok=True)

    chunk = max(1, a.rows // 50)              # 50 write batches keeps memory flat
    per_part = a.rows // a.parts
    big = pq.ParquetWriter(f"{a.out}/big.parquet", build(0, 1, rng).schema, compression="zstd")
    part, part_rows, written = 0, 0, 0
    pw = pq.ParquetWriter(f"{a.out}/parts/part-00.parquet", big.schema, compression="zstd")
    while written < a.rows:
        n = min(chunk, a.rows - written)
        t = build(written, n, rng)
        big.write_table(t, row_group_size=200_000)
        # split the same stream into part files of equal size
        off = 0
        while off < t.num_rows:
            take = min(per_part - part_rows, t.num_rows - off)
            pw.write_table(t.slice(off, take), row_group_size=200_000)
            part_rows += take; off += take
            if part_rows >= per_part and part < a.parts - 1:
                pw.close(); part += 1; part_rows = 0
                pw = pq.ParquetWriter(f"{a.out}/parts/part-{part:02d}.parquet", big.schema, compression="zstd")
        written += n
    pw.close(); big.close()

    total_mb = 0
    for root, _, files in os.walk(a.out):
        for f in files:
            if f.endswith(".parquet"):
                total_mb += os.path.getsize(os.path.join(root, f))
    print(f"rows={a.rows} files={a.parts + 1} parquet_mb={total_mb/1e6:.1f}")


if __name__ == "__main__":
    main()
