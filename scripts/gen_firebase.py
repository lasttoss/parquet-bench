"""Generate a GA4/Firebase-shaped parquet: nested event_params (repeated key/value
records), user_properties, device/geo/app_info/traffic_source structs. Same shape as a
BigQuery EXPORT DATA of analytics_<id>.events_*, so parse cost is representative."""
import argparse, os, random, time
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

RNG = random.Random(7)
NRNG = np.random.default_rng(7)

PARAMS = [
    ("firebase_event_origin", "str"), ("firebase_screen", "str"), ("firebase_screen_class", "str"),
    ("ga_session_id", "int"), ("ga_session_number", "int"), ("engagement_time_msec", "int"),
    ("level", "int"), ("score", "int"), ("item_id", "str"), ("item_name", "str"),
    ("value", "dbl"), ("currency", "str"), ("board_size", "int"), ("move_count", "int"),
]
INT64 = pa.int64(); F64 = pa.float64(); STR = pa.string()
VALUE_T = pa.struct([("string_value", STR), ("int_value", INT64), ("float_value", F64), ("double_value", F64)])
PARAM_T = pa.struct([("key", STR), ("value", VALUE_T)])
SCHEMA = pa.schema([
    ("event_date", STR), ("event_timestamp", INT64), ("event_name", STR),
    ("event_value_in_usd", F64), ("user_id", STR), ("user_pseudo_id", STR),
    ("user_first_touch_timestamp", INT64),
    ("device", pa.struct([("category", STR), ("mobile_brand_name", STR), ("mobile_model_name", STR),
                          ("operating_system", STR), ("operating_system_version", STR),
                          ("language", STR), ("time_zone_offset_seconds", pa.int32())])),
    ("geo", pa.struct([("continent", STR), ("city", STR), ("country", STR), ("region", STR)])),
    ("traffic_source", pa.struct([("name", STR), ("medium", STR), ("source", STR)])),
    ("stream_id", STR), ("platform", STR),
    ("user_properties", pa.list_(pa.struct([("key", STR), ("value", VALUE_T)]))),
    ("event_params", pa.list_(PARAM_T)),
    ("items", pa.list_(pa.struct([
        ("item_id", STR), ("item_name", STR), ("item_brand", STR), ("item_variant", STR),
        ("item_category", STR), ("price", F64), ("quantity", INT64), ("currency", STR),
    ]))),
])

EVENTS = ["level_start", "level_end", "purchase", "screen_view", "session_start", "ad_impression"]
BRANDS = ["Apple", "samsung", "Xiaomi", "OPPO"]
OSES = ["iOS", "Android"]
LANGS = ["vi", "en", "ja", "ko"]
CITIES = ["Hanoi", "Tokyo", "Seoul", "Bangkok"]
COUNTRIES = ["Vietnam", "Japan", "South Korea", "Thailand"]
CONT = ["Asia", "Europe", "Americas"]


def make_params(kind, key):
    if kind == "str":
        return (key, (f"v_{RNG.randint(1, 40)}", None, None, None))
    if kind == "int":
        return (key, (None, int(NRNG.integers(0, 100000)), None, None))
    return (key, (None, None, None, float(NRNG.random() * 100)))


def make_items(k):
    if k <= 0:
        return []
    return [(f"item_{RNG.randint(1, 500)}", f"Pack {RNG.randint(1, 99)}",
             ["Nintendo", "Sony", "Tencent", "VNG"][RNG.randint(0, 3)],
             ["basic", "plus", "pro"][RNG.randint(0, 2)],
             ["iap", "bundle", "cosmetic"][RNG.randint(0, 2)],
             round(RNG.random() * 50, 2), int(NRNG.integers(1, 4)), "USD")
            for _ in range(k)]


def make_batch(n, pmin=8, pmax=14, items=0):
    rows = []
    for _ in range(n):
        k = int(NRNG.integers(pmin, min(pmax, len(PARAMS)) + 1))
        chosen = RNG.sample(PARAMS, k)
        params = [make_params(kind, key) for key, kind in chosen]
        props = [make_params("str", "first_open_time"), make_params("str", "language")]
        rows.append({
            "event_date": f"2026{int(NRNG.integers(1, 13)):02d}{int(NRNG.integers(1, 29)):02d}",
            "event_timestamp": 1760000000000000 + int(NRNG.integers(0, 86_400_000_000)),
            "event_name": EVENTS[int(NRNG.integers(0, len(EVENTS)))],
            "event_value_in_usd": float(NRNG.random() * 20),
            "user_id": None,
            "user_pseudo_id": f"{int(NRNG.integers(0, 2**48)):012x}",
            "user_first_touch_timestamp": int(NRNG.integers(1700000000000000, 1760000000000000)),
            "device": ("mobile", BRANDS[int(NRNG.integers(0, 4))], f"model_{int(NRNG.integers(1, 60))}",
                       OSES[int(NRNG.integers(0, 2))], f"{int(NRNG.integers(12, 18))}.{int(NRNG.integers(0, 10))}",
                       LANGS[int(NRNG.integers(0, 4))], int(NRNG.integers(-36000, -14400))),
            "geo": (CONT[int(NRNG.integers(0, 3))], CITIES[int(NRNG.integers(0, 4))],
                    COUNTRIES[int(NRNG.integers(0, 4))], f"region_{int(NRNG.integers(1, 40))}"),
            "traffic_source": (["google", "organic", "(direct)"][int(NRNG.integers(0, 3))],
                               ["organic", "cpc", "(none)"][int(NRNG.integers(0, 3))], "google"),
            "stream_id": f"{int(NRNG.integers(1000000000, 2000000000))}",
            "platform": ["ANDROID", "IOS"][int(NRNG.integers(0, 2))],
            "user_properties": props,
            "event_params": params,
            "items": make_items(items),
        })
    return pa.Table.from_pylist(rows, schema=SCHEMA)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=5_000_000)
    ap.add_argument("--out", default="/data/fb/fb.parquet")
    ap.add_argument("--batch", type=int, default=50_000)
    ap.add_argument("--param-min", type=int, default=8)
    ap.add_argument("--param-max", type=int, default=14)
    ap.add_argument("--items", type=int, default=0)
    a = ap.parse_args()
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    t0 = time.time(); done = 0; w = None
    while done < a.rows:
        n = min(a.batch, a.rows - done)
        t = make_batch(n, a.param_min, a.param_max, a.items)
        if w is None:
            w = pq.ParquetWriter(a.out, t.schema, compression="zstd")
        w.write_table(t, row_group_size=n)
        done += n
        if done % 500_000 == 0:
            print(f"  {done:,} rows  {os.path.getsize(a.out)/1e6:.0f} MB", flush=True)
    w.close()
    sz = os.path.getsize(a.out)
    print(f"rows={done} parquet={sz/1e6:.0f} MB bytes_per_row={sz/done:.0f} {time.time()-t0:.0f}s")


main()
