# CLI

SDSavior exposes a single command group via `sdsavior`.

## Export Command

Export current ring contents to JSONL:

```bash
sdsavior export \
  --data data.ring \
  --meta data.meta \
  --capacity 8388608 \
  --out out.jsonl
```

### Options

- `--data`: path to the ring data file.
- `--meta`: path to the ring metadata file.
- `--capacity`: ring capacity in bytes; must match file creation capacity.
- `--out`: output JSONL path (parent directories are auto-created).
- `--from-seq`: optional starting sequence number.

Example with sequence filter:

```bash
sdsavior export --data data.ring --meta data.meta --capacity 8388608 --out out.jsonl --from-seq 500
```
