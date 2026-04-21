#!/usr/bin/env python3
"""
Benchmark crash recovery for SDSavior and simple baselines.

Backends:
  - sdsavior : reopen with SDSavior and count iter_records()
  - file     : append JSONL, kill process, count readable lines after reopen
  - mmap     : preallocated mmap file, kill process, count newline-terminated records

Outputs newline-delimited JSON (JSONL), one object per scenario/run.
Optionally writes a Markdown summary table.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from tqdm import tqdm

from sdsavior import SDSavior

WRITER_CODE = r"""
import json
import mmap
import os
import time
import random
from sdsavior import SDSavior

backend = os.environ["BACKEND"]
path = os.environ["DATA_PATH"]
meta_path = os.environ.get("META_PATH", "")
capacity_bytes = int(os.environ["CAPACITY_BYTES"])
durability_mode = os.environ["DURABILITY_MODE"]

rnd = random.Random(12345)

def record(i: int) -> bytes:
    payload = {
        "seq_hint": i,
        "ts_ns": time.time_ns(),
        "value": rnd.random(),
        "blob": "x" * 512,
    }
    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8") + b"\n"

if backend == "sdsavior":
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
        raise ValueError(durability_mode)

    rb = SDSavior(
        data_path=path,
        meta_path=meta_path,
        capacity_bytes=capacity_bytes,
        fsync_data=fsync_data,
        fsync_meta=fsync_meta,
        group_commit_records=group_commit_records,
    )
    rb.open()

    i = 0
    while True:
        rb.append_json_bytes(record(i))
        i += 1

elif backend == "file":
    f = open(path, "ab", buffering=0)
    i = 0
    while True:
        f.write(record(i))
        if durability_mode == "full_fsync":
            f.flush()
            os.fsync(f.fileno())
        i += 1

elif backend == "mmap":
    f = open(path, "w+b")
    f.truncate(capacity_bytes)
    mm = mmap.mmap(f.fileno(), capacity_bytes)
    pos = 0
    i = 0
    while True:
        line = record(i)
        if len(line) > capacity_bytes:
            raise ValueError("record exceeds capacity")
        if pos + len(line) > capacity_bytes:
            pos = 0
        mm[pos:pos + len(line)] = line
        pos += len(line)
        if durability_mode == "full_fsync":
            mm.flush()
        i += 1

else:
    raise ValueError(backend)
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", action="append", choices=["sdsavior", "file", "mmap"],
                        default=["sdsavior", "file", "mmap"])
    parser.add_argument("--capacity-mib", type=int, nargs="+", default=[8, 64])
    parser.add_argument("--kill-after-seconds", type=float, default=1.5)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument(
        "--durability-mode",
        action="append",
        choices=["none", "meta_only", "full_fsync", "group_fsync"],
        default=["none", "meta_only", "full_fsync", "group_fsync"],
    )
    parser.add_argument("--json-out", type=Path, default=None)
    parser.add_argument("--markdown-out", type=Path, default=None)
    return parser.parse_args()


def is_supported_combo(backend: str, durability_mode: str) -> bool:
    if backend == "sdsavior":
        return durability_mode in {"none", "meta_only", "full_fsync", "group_fsync"}
    return durability_mode in {"none", "full_fsync"}


def count_file_records(path: Path) -> int:
    if not path.exists():
        return 0
    count = 0
    with path.open("rb") as f:
        for line in f:
            if line.endswith(b"\n"):
                count += 1
    return count


def count_mmap_records(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("rb") as f:
        data = f.read()
    count = 0
    for line in data.split(b"\n"):
        if not line:
            continue
        try:
            json.loads(line.decode("utf-8"))
            count += 1
        except Exception:
            continue
    return count


def recover(
    backend: str,
    data_path: Path,
    meta_path: Path,
    capacity_bytes: int,
    durability_mode: str,
) -> tuple[int, float]:
    t0 = time.perf_counter()

    if backend == "sdsavior":
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
            raise ValueError(durability_mode)

        rb = SDSavior(
            data_path=str(data_path),
            meta_path=str(meta_path),
            capacity_bytes=capacity_bytes,
            fsync_data=fsync_data,
            fsync_meta=fsync_meta,
            group_commit_records=group_commit_records,
        )
        rb.open()
        try:
            records = list(rb.iter_records())
            recovered = len(records)
        finally:
            close = getattr(rb, "close", None)
            if callable(close):
                close()

    elif backend == "file":
        recovered = count_file_records(data_path)

    elif backend == "mmap":
        recovered = count_mmap_records(data_path)

    else:
        raise ValueError(backend)

    t1 = time.perf_counter()
    return recovered, t1 - t0


def run_once(
    backend: str,
    capacity_mib: int,
    durability_mode: str,
    run_index: int,
    kill_after_seconds: float,
) -> dict[str, Any]:
    capacity_bytes = capacity_mib * 1024 * 1024
    tmpdir = Path(tempfile.mkdtemp(prefix=f"sdsavior-bench-recovery-{backend}-"))
    data_path = tmpdir / "data.bin"
    meta_path = tmpdir / "data.meta"

    env = os.environ.copy()
    env["BACKEND"] = backend
    env["DATA_PATH"] = str(data_path)
    env["META_PATH"] = str(meta_path)
    env["CAPACITY_BYTES"] = str(capacity_bytes)
    env["DURABILITY_MODE"] = durability_mode

    proc = None
    try:
        proc = subprocess.Popen([sys.executable, "-c", WRITER_CODE], env=env)
        time.sleep(kill_after_seconds)
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait()

        recovered, recovery_s = recover(
            backend=backend,
            data_path=data_path,
            meta_path=meta_path,
            capacity_bytes=capacity_bytes,
            durability_mode=durability_mode,
        )

        return {
            "benchmark": "recovery",
            "backend": backend,
            "run_index": run_index,
            "durability_mode": durability_mode,
            "capacity_mib": capacity_mib,
            "capacity_bytes": capacity_bytes,
            "kill_after_seconds": kill_after_seconds,
            "recovery_s": recovery_s,
            "recovered_records": recovered,
            "python_version": platform.python_version(),
            "platform": platform.platform(),
        }
    finally:
        if proc is not None and proc.poll() is None:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
        shutil.rmtree(tmpdir, ignore_errors=True)


def aggregate(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple, list[dict[str, Any]]] = {}
    for row in rows:
        key = (row["backend"], row["capacity_mib"], row["durability_mode"])
        grouped.setdefault(key, []).append(row)

    summary: list[dict[str, Any]] = []
    for (backend, capacity_mib, durability_mode), group in sorted(grouped.items()):
        summary.append({
            "backend": backend,
            "capacity_mib": capacity_mib,
            "durability_mode": durability_mode,
            "runs": len(group),
            "recovery_ms_mean": statistics.fmean(r["recovery_s"] * 1000.0 for r in group),
            "recovered_records_mean": statistics.fmean(r["recovered_records"] for r in group),
            "recovered_records_min": min(r["recovered_records"] for r in group),
            "recovered_records_max": max(r["recovered_records"] for r in group),
        })
    return summary


def markdown_table(summary_rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Recovery benchmark summary",
        "",
        "| backend | capacity (MiB) | durability | runs | recovery ms | "
        "recovered records mean | recovered records min | recovered records max |",
        "|---|---:|---|---:|---:|---:|---:|---:|",
    ]
    for row in summary_rows:
        lines.append(
            "| {backend} | {capacity_mib} | {durability_mode} | {runs} | "
            "{recovery_ms_mean:.2f} | {recovered_records_mean:.0f} | "
            "{recovered_records_min} | {recovered_records_max} |".format(**row)
        )
    lines.extend([
        "",
        "Interpretation notes:",
        "",
        "- `file` and `mmap` recovery means how much valid data can be read after a forced kill.",
        "- `sdsavior` recovery measures reopen plus `iter_records()` after the same crash pattern.",
        "- `group_fsync` is SDSavior only, with full durability fsyncs batched every 16 records.",
    ])
    return "\n".join(lines) + "\n"


def main() -> None:
    args = parse_args()
    rows: list[dict[str, Any]] = []

    scenarios = [
        (backend, capacity_mib, durability_mode, run_index)
        for backend in args.backend
        for capacity_mib in args.capacity_mib
        for durability_mode in args.durability_mode
        if is_supported_combo(backend, durability_mode)
        for run_index in range(args.runs)
    ]

    for backend, capacity_mib, durability_mode, run_index in tqdm(scenarios, desc="Recovery runs"):
        row = run_once(
            backend=backend,
            capacity_mib=capacity_mib,
            durability_mode=durability_mode,
            run_index=run_index,
            kill_after_seconds=args.kill_after_seconds,
        )
        rows.append(row)
        print(json.dumps(row))

    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        with args.json_out.open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")

    if args.markdown_out:
        summary_rows = aggregate(rows)
        args.markdown_out.parent.mkdir(parents=True, exist_ok=True)
        args.markdown_out.write_text(markdown_table(summary_rows), encoding="utf-8")


if __name__ == "__main__":
    main()
