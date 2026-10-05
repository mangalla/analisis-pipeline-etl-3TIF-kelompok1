"""Satu kali run pipeline ETL. JANGAN dijalankan langsung; dipanggil oleh jalankan_eksperimen.py.

Setiap run dijalankan sebagai proses Python terpisah supaya pengukuran memori
setiap run bersih (tidak terbawa dari run sebelumnya).
Pemakaian: python run_one.py <library> <format> <path_input> <path_output>
Mencetak satu baris JSON berisi hasil pengukuran.
"""
import json
import platform
import sys
import time

import psutil

SISTEM = platform.system()
_proc = psutil.Process()


def rss_mb():
    """Memori yang sedang dipakai proses ini (MB)."""
    return _proc.memory_info().rss / 1024 / 1024


def peak_mb():
    """Puncak memori yang pernah dipakai proses ini (MB)."""
    if SISTEM == "Windows":
        return _proc.memory_info().peak_wset / 1024 / 1024
    if SISTEM == "Linux":
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) / 1024
    import resource  # macOS: ru_maxrss dalam byte
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024 / 1024


def run_pandas(fmt, src, dst):
    import pandas as pd
    base = rss_mb()
    t0 = time.perf_counter()
    # EXTRACT
    if fmt == "CSV":
        df = pd.read_csv(src)
    elif fmt == "JSON":
        df = pd.read_json(src, lines=True)
    else:
        df = pd.read_parquet(src)
    t1 = time.perf_counter()
    # TRANSFORM
    df = df[df["status"] == "SELESAI"].copy()
    df["total_harga"] = df["jumlah"] * df["harga_satuan"]
    ringkas = df.groupby("kategori", as_index=False)["total_harga"].sum()
    df = df.merge(ringkas.rename(columns={"total_harga": "total_kategori"}), on="kategori")
    t2 = time.perf_counter()
    # LOAD
    if fmt == "CSV":
        df.to_csv(dst, index=False)
    elif fmt == "JSON":
        df.to_json(dst, orient="records", lines=True)
    else:
        df.to_parquet(dst, index=False)
    t3 = time.perf_counter()
    return base, t0, t1, t2, t3


def run_polars(fmt, src, dst):
    import polars as pl
    base = rss_mb()
    t0 = time.perf_counter()
    # EXTRACT
    if fmt == "CSV":
        df = pl.read_csv(src)
    elif fmt == "JSON":
        df = pl.read_ndjson(src)
    else:
        df = pl.read_parquet(src)
    t1 = time.perf_counter()
    # TRANSFORM
    df = df.filter(pl.col("status") == "SELESAI").with_columns(
        (pl.col("jumlah") * pl.col("harga_satuan")).alias("total_harga")
    )
    ringkas = df.group_by("kategori").agg(pl.col("total_harga").sum().alias("total_kategori"))
    df = df.join(ringkas, on="kategori")
    t2 = time.perf_counter()
    # LOAD
    if fmt == "CSV":
        df.write_csv(dst)
    elif fmt == "JSON":
        df.write_ndjson(dst)
    else:
        df.write_parquet(dst)
    t3 = time.perf_counter()
    return base, t0, t1, t2, t3


if __name__ == "__main__":
    lib, fmt, src, dst = sys.argv[1:5]
    fn = run_pandas if lib == "Pandas" else run_polars
    base, t0, t1, t2, t3 = fn(fmt, src, dst)
    print(json.dumps({
        "waktu_baca": t1 - t0,
        "waktu_transformasi": t2 - t1,
        "waktu_tulis": t3 - t2,
        "memori_puncak_mb": max(peak_mb() - base, 0.0),
    }))
