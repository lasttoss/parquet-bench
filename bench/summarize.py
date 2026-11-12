"""Median of each approach, with the ratio against the fastest one."""
import csv, statistics, sys, collections

rows = collections.defaultdict(list)
for r in csv.DictReader(open(sys.argv[1])):
    rows[r["approach"]].append((float(r["seconds"]), int(r["rows_per_s"])))

best = max(min(v, key=lambda t: t[0])[1] for v in rows.values())
print(f"{'approach':26} {'median s':>9} {'rows/s':>12} {'MB/s':>7} {'vs best':>8}  runs")
for name, v in sorted(rows.items(), key=lambda kv: statistics.median(t[0] for t in kv[1])):
    secs = statistics.median(t[0] for t in v)
    rps = statistics.median(t[1] for t in v)
    print(f"{name:26} {secs:9.2f} {int(rps):12,} {rps*46/1e6:7.1f} {best/rps:8.2f}x  {len(v)}")
