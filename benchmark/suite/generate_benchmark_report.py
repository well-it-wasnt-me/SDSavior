#!/usr/bin/env python3
"""
Merge append/recovery JSONL outputs into a single README-friendly Markdown report.
"""
from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--append-json", type=Path, required=True)
    parser.add_argument("--recovery-json", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    return parser.parse_args()


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def summarize_append(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [r for r in rows if r.get("benchmark") == "append" and not r.get("warmup", False)]
    grouped: dict[tuple, list[dict[str, Any]]] = {}
    for row in rows:
        key = (row["backend"], row["capacity_mib"], row["payload_bytes"], row["durability_mode"])
        grouped.setdefault(key, []).append(row)

    summary: list[dict[str, Any]] = []
    for (backend, capacity_mib, payload_bytes, durability_mode), group in sorted(grouped.items()):
        summary.append({
            "backend": backend,
            "capacity_mib": capacity_mib,
            "payload_bytes": payload_bytes,
            "durability_mode": durability_mode,
            "records_per_sec": statistics.fmean(r["records_per_sec"] for r in group),
            "mb_per_sec": statistics.fmean(r["mb_per_sec"] for r in group),
            "p50_us": statistics.fmean(r["p50_us"] for r in group),
            "p95_us": statistics.fmean(r["p95_us"] for r in group),
            "p99_us": statistics.fmean(r["p99_us"] for r in group),
        })
    return summary


def summarize_recovery(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [r for r in rows if r.get("benchmark") == "recovery"]
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
            "recovery_ms": statistics.fmean(r["recovery_s"] * 1000.0 for r in group),
            "recovered_records_mean": statistics.fmean(r["recovered_records"] for r in group),
        })
    return summary


def render_report(append_summary: list[dict[str, Any]], recovery_summary: list[dict[str, Any]]) -> str:
    lines: list[str] = [
        "# Benchmarks",
        "",
        "## Append",
        "",
        "| backend | capacity (MiB) | payload (B) | durability | rec/s | MB/s | p50 us | p95 us | p99 us |",
        "|---|---:|---:|---|---:|---:|---:|---:|---:|",
    ]
    for row in append_summary:
        lines.append(
            "| {backend} | {capacity_mib} | {payload_bytes} | {durability_mode} | "
            "{records_per_sec:.0f} | {mb_per_sec:.2f} | {p50_us:.1f} | {p95_us:.1f} | {p99_us:.1f} |".format(**row)
        )

    lines.extend([
        "",
        "## Recovery after forced kill",
        "",
        "| backend | capacity (MiB) | durability | recovery ms | recovered records mean |",
        "|---|---:|---|---:|---:|",
    ])
    for row in recovery_summary:
        lines.append(
            "| {backend} | {capacity_mib} | {durability_mode} | {recovery_ms:.2f} | {recovered_records_mean:.0f} |".format(**row)
        )

    lines.extend([
        "",
        "## Notes",
        "",
        "- `none`: no per-append durability step.",
        "- `meta_only`: SDSavior only, `fsync_data=False, fsync_meta=True`.",
        "- `full_fsync`: strongest per-append durability mode for each backend.",
        "- `file` and `mmap` are baselines, not feature-equivalent replacements for SDSavior.",
    ])
    return "\n".join(lines) + "\n"


def main() -> None:
    args = parse_args()
    append_rows = load_jsonl(args.append_json)
    recovery_rows = load_jsonl(args.recovery_json)
    report = render_report(
        summarize_append(append_rows),
        summarize_recovery(recovery_rows),
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(report, encoding="utf-8")


if __name__ == "__main__":
    main()
