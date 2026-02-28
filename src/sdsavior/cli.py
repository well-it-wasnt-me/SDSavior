from __future__ import annotations

import argparse
from pathlib import Path

from .ring import SDSavior


def main() -> int:
    """Run the command-line interface and execute the selected subcommand."""
    p = argparse.ArgumentParser(prog="sdsavior", description="SDSavior utilities")
    sub = p.add_subparsers(dest="cmd", required=True)

    exp = sub.add_parser("export", help="Export ring contents to JSONL")
    exp.add_argument("--data", required=True, help="Path to data file")
    exp.add_argument("--meta", required=True, help="Path to meta file")
    exp.add_argument("--capacity", required=True, type=int, help="Capacity in bytes (must match)")
    exp.add_argument("--out", required=True, help="Output JSONL path")
    exp.add_argument("--from-seq", type=int, default=None, help="Start exporting from seq")

    args = p.parse_args()

    if args.cmd == "export":
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        with SDSavior(args.data, args.meta, args.capacity) as rb:
            rb.export_jsonl(str(out), from_seq=args.from_seq)
        return 0

    return 2
