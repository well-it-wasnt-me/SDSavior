#!/usr/bin/env python3
"""
Benchmark append throughput/latency for SDSavior and baselines.

Backends:
  - sdsavior : SDSavior ring buffer
  - file     : plain JSONL append
  - mmap     : preallocated mmap file with newline-delimited JSON records

Outputs newline-delimited JSON (JSONL), one object per scenario/run.
Optionally writes a Markdown summary table.
"""
from __future__ import annotations

import argparse
import json
import mmap
import os
import platform
import random
import shutil
import statistics
import tempfile
import time
from pathlib import Path
from typing import Any

from tqdm import tqdm

from sdsavior import SDSavior


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", action="append", choices=["sdsavior", "file", "mmap"],
                        default=["sdsavior", "file", "mmap"])
    parser.add_argument("--capacity-mib", type=int, nargs="+", default=[8, 64])
    parser.add_argument("--payload-bytes", type=int, nargs="+", default=[128, 1024, 4096, 16384])
    parser.add_argument("--records", type=int, default=20000)
    parser.add_argument("--warmup-records", type=int, default=2000)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument(
        "--durability-mode",
        action="append",
        choices=["none", "meta_only", "full_fsync", "group_fsync"],
        default=["none", "meta_only", "full_fsync", "group_fsync"],
        help=(
            "none=file/mmap no fsync, meta_only=sdsavior only, "
            "full_fsync=all backends fsync/msync, "
            "group_fsync=sdsavior full fsync batched every 16 records"
        ),
    )
    parser.add_argument("--json-out", type=Path, default=None)
    parser.add_argument("--markdown-out", type=Path, default=None)
    return parser.parse_args()


def make_payload(target_size: int, seed: int) -> dict[str, Any]:
    rnd = random.Random(seed)
    payload = {
        "device": "sensor-a",
        "seq_hint": seed,
        "ts_ns": time.time_ns(),
        "value": rnd.random(),
        "ok": True,
    }
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    filler_overhead = len('"blob":""') + 2
    extra = max(0, target_size - len(encoded) - filler_overhead)
    payload["blob"] = "x" * extra
    return payload


def percentile(sorted_values: list[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    idx = round((p / 100.0) * (len(sorted_values) - 1))
    return sorted_values[int(idx)]


class BaseWriter:
    def append_bytes(self, data: bytes) -> None:
        raise NotImplementedError

    def close(self) -> None:
        pass


class SDSaviorWriter(BaseWriter):
    def __init__(self, data_path: Path, meta_path: Path, capacity_bytes: int, durability_mode: str):
        group_commit_records = None
        if durability_mode == "none":
            fsync_data = False
            fsync_meta = False
        elif durability_mode == "meta_only":
            fsync_data = False
            fsync_meta = True
        elif durability_mode == "full_fsync":
            fsync_data = True
            fsync_meta = True
        elif durability_mode == "group_fsync":
            fsync_data = True
            fsync_meta = True
            group_commit_records = 16
        else:
            raise ValueError(f"Unsupported durability mode for SDSavior: {durability_mode}")

        self._rb = SDSavior(
            data_path=str(data_path),
            meta_path=str(meta_path),
            capacity_bytes=capacity_bytes,
            fsync_data=fsync_data,
            fsync_meta=fsync_meta,
            group_commit_records=group_commit_records,
        )
        self._rb.open()

    def append_bytes(self, data: bytes) -> None:
        self._rb.append_json_bytes(data)

    def close(self) -> None:
        close = getattr(self._rb, "close", None)
        if callable(close):
            close()


class FileWriter(BaseWriter):
    def __init__(self, path: Path, durability_mode: str):
        self._f = open(path, "ab", buffering=0)
        self._durability_mode = durability_mode

    def append_bytes(self, data: bytes) -> None:
        self._f.write(data + b"\n")
        if self._durability_mode == "full_fsync":
            self._f.flush()
            os.fsync(self._f.fileno())

    def close(self) -> None:
        self._f.close()


class MmapWriter(BaseWriter):
    def __init__(self, path: Path, capacity_bytes: int, durability_mode: str):
        self._f = open(path, "w+b")
        self._f.truncate(capacity_bytes)
        self._mm = mmap.mmap(self._f.fileno(), capacity_bytes)
        self._capacity_bytes = capacity_bytes
        self._durability_mode = durability_mode
        self._pos = 0

    def append_bytes(self, data: bytes) -> None:
        line = data + b"\n"
        needed = len(line)
        if needed > self._capacity_bytes:
            raise ValueError(
                f"Record of {needed} bytes exceeds mmap capacity {self._capacity_bytes}"
            )
        if self._pos + needed > self._capacity_bytes:
            self._pos = 0
        self._mm[self._pos:self._pos + needed] = line
        self._pos += needed
        if self._durability_mode == "full_fsync":
            self._mm.flush()

    def close(self) -> None:
        self._mm.close()
        self._f.close()


def create_writer(
    tmpdir: Path,
    backend: str,
    capacity_bytes: int,
    durability_mode: str,
) -> BaseWriter:
    if backend == "sdsavior":
        return SDSaviorWriter(
            data_path=tmpdir / "data.ring",
            meta_path=tmpdir / "data.meta",
            capacity_bytes=capacity_bytes,
            durability_mode=durability_mode,
        )
    if backend == "file":
        return FileWriter(tmpdir / "data.jsonl", durability_mode=durability_mode)
    if backend == "mmap":
        return MmapWriter(
            tmpdir / "data.mmap",
            capacity_bytes=capacity_bytes,
            durability_mode=durability_mode,
        )
    raise ValueError(f"Unknown backend: {backend}")


def is_supported_combo(backend: str, durability_mode: str) -> bool:
    if backend == "sdsavior":
        return durability_mode in {"none", "meta_only", "full_fsync", "group_fsync"}
    return durability_mode in {"none", "full_fsync"}


def append_n(
    writer: BaseWriter,
    n_records: int,
    payload_size: int,
    desc: str,
) -> tuple[list[float], int]:
    latencies_us: list[float] = []
    total_bytes = 0

    for i in tqdm(range(n_records), desc=desc, leave=False):
        payload = make_payload(payload_size, seed=i)
        encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        total_bytes += len(encoded) + 1

        t0 = time.perf_counter()
        writer.append_bytes(encoded)
        t1 = time.perf_counter()
        latencies_us.append((t1 - t0) * 1_000_000)

    return latencies_us, total_bytes


def run_once(
    backend: str,
    capacity_mib: int,
    payload_size: int,
    n_records: int,
    durability_mode: str,
    run_index: int,
    warmup: bool,
) -> dict[str, Any]:
    capacity_bytes = capacity_mib * 1024 * 1024
    tmpdir = Path(tempfile.mkdtemp(prefix=f"sdsavior-bench-{backend}-"))

    try:
        writer = create_writer(
            tmpdir=tmpdir,
            backend=backend,
            capacity_bytes=capacity_bytes,
            durability_mode=durability_mode,
        )
        try:
            start = time.perf_counter()
            run_label = "warmup" if warmup else f"run{run_index + 1}"
            latencies_us, total_bytes = append_n(
                writer,
                n_records=n_records,
                payload_size=payload_size,
                desc=f"{backend} {payload_size}B {durability_mode} {run_label}",
            )
            end = time.perf_counter()
        finally:
            writer.close()

        elapsed_s = end - start
        latencies_us.sort()

        return {
            "benchmark": "append",
            "backend": backend,
            "warmup": warmup,
            "run_index": run_index,
            "durability_mode": durability_mode,
            "capacity_mib": capacity_mib,
            "capacity_bytes": capacity_bytes,
            "payload_bytes": payload_size,
            "n_records": n_records,
            "elapsed_s": elapsed_s,
            "records_per_sec": (n_records / elapsed_s) if elapsed_s else 0.0,
            "mb_per_sec": ((total_bytes / (1024 * 1024)) / elapsed_s) if elapsed_s else 0.0,
            "mean_us": statistics.fmean(latencies_us) if latencies_us else 0.0,
            "p50_us": percentile(latencies_us, 50),
            "p95_us": percentile(latencies_us, 95),
            "p99_us": percentile(latencies_us, 99),
            "python_version": platform.python_version(),
            "platform": platform.platform(),
        }
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def aggregate(measured_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple, list[dict[str, Any]]] = {}
    for row in measured_rows:
        key = (row["backend"], row["capacity_mib"], row["payload_bytes"], row["durability_mode"])
        grouped.setdefault(key, []).append(row)

    summary: list[dict[str, Any]] = []
    for (backend, capacity_mib, payload_bytes, durability_mode), rows in sorted(grouped.items()):
        summary.append({
            "backend": backend,
            "capacity_mib": capacity_mib,
            "payload_bytes": payload_bytes,
            "durability_mode": durability_mode,
            "runs": len(rows),
            "records_per_sec_mean": statistics.fmean(r["records_per_sec"] for r in rows),
            "mb_per_sec_mean": statistics.fmean(r["mb_per_sec"] for r in rows),
            "p50_us_mean": statistics.fmean(r["p50_us"] for r in rows),
            "p95_us_mean": statistics.fmean(r["p95_us"] for r in rows),
            "p99_us_mean": statistics.fmean(r["p99_us"] for r in rows),
        })
    return summary


def markdown_table(summary_rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Append benchmark summary",
        "",
        "| backend | capacity (MiB) | payload (B) | durability | runs | rec/s | "
        "MB/s | p50 us | p95 us | p99 us |",
        "|---|---:|---:|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in summary_rows:
        lines.append(
            "| {backend} | {capacity_mib} | {payload_bytes} | {durability_mode} | {runs} | "
            "{records_per_sec_mean:.0f} | {mb_per_sec_mean:.2f} | "
            "{p50_us_mean:.1f} | {p95_us_mean:.1f} | {p99_us_mean:.1f} |".format(**row)
        )
    lines.extend([
        "",
        "Durability notes:",
        "",
        "- `none`: no fsync or flush-heavy durability step after each append.",
        "- `meta_only`: SDSavior only, `fsync_data=False, fsync_meta=True`.",
        "- `full_fsync`: strongest per-append durability mode for each backend.",
        "- `group_fsync`: SDSavior only, full durability fsyncs batched every 16 records.",
    ])
    return "\n".join(lines) + "\n"


def main() -> None:
    args = parse_args()
    json_rows: list[dict[str, Any]] = []

    scenarios = [
        (backend, capacity_mib, payload_size, durability_mode)
        for backend in args.backend
        for capacity_mib in args.capacity_mib
        for payload_size in args.payload_bytes
        for durability_mode in args.durability_mode
        if is_supported_combo(backend, durability_mode)
    ]

    for backend, capacity_mib, payload_size, durability_mode in tqdm(
        scenarios,
        desc="Append scenarios",
    ):
        for warmup_index in range(args.warmups):
            row = run_once(
                backend=backend,
                capacity_mib=capacity_mib,
                payload_size=payload_size,
                n_records=args.warmup_records,
                durability_mode=durability_mode,
                run_index=warmup_index,
                warmup=True,
            )
            json_rows.append(row)

        for run_index in range(args.runs):
            row = run_once(
                backend=backend,
                capacity_mib=capacity_mib,
                payload_size=payload_size,
                n_records=args.records,
                durability_mode=durability_mode,
                run_index=run_index,
                warmup=False,
            )
            json_rows.append(row)
            print(json.dumps(row))

    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        with args.json_out.open("w", encoding="utf-8") as f:
            for row in json_rows:
                f.write(json.dumps(row) + "\n")

    measured_rows = [r for r in json_rows if not r["warmup"]]
    summary_rows = aggregate(measured_rows)

    if args.markdown_out:
        args.markdown_out.parent.mkdir(parents=True, exist_ok=True)
        args.markdown_out.write_text(markdown_table(summary_rows), encoding="utf-8")


if __name__ == "__main__":
    main()
