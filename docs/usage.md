# Usage

## Open and Append Records

```python
from sdsavior import SDSavior

rb = SDSavior("data.ring", "data.meta", 8 * 1024 * 1024)
rb.open()
try:
    rb.append({"event": "boot"})
    rb.append({"event": "sample", "value": 123})
finally:
    rb.close()
```

Context-manager usage is recommended:

```python
with SDSavior("data.ring", "data.meta", 8 * 1024 * 1024) as rb:
    rb.append({"ok": True})
```

If the record is already serialized as compact JSON, append the bytes directly:

```python
with SDSavior("data.ring", "data.meta", 8 * 1024 * 1024) as rb:
    rb.append_json_bytes(b'{"ok":true}')
```

## Read Records

```python
for seq, ts_ns, obj in rb.iter_records():
    print(seq, ts_ns, obj)
```

Filter by sequence:

```python
for seq, ts_ns, obj in rb.iter_records(from_seq=200):
    ...
```

## Export JSONL

```python
with SDSavior("data.ring", "data.meta", 8 * 1024 * 1024) as rb:
    rb.export_jsonl("out/export.jsonl")
```

## Durability Controls

- `fsync_meta=True` (default): metadata is `fsync`'d after writes.
- `fsync_data=False` (default): data pages are not `fsync`'d on every append.
- `recover_scan_limit_bytes=None` (default): scan up to capacity during recovery.
- `recovery_checkpoint_interval_records=None` (default): do not add periodic recovery checkpoints.
- `group_commit_records=None` (default): do not batch durability fsync work.

Use `fsync_data=True` when stronger durability is required and throughput tradeoffs are acceptable.
Use `group_commit_records=N` to batch durability work across `N` appends, or call `commit()`
to flush the current batch explicitly.
