# API Reference

## `MetaState`

Dataclass storing persisted pointer state:

- `capacity`
- `head`
- `tail`
- `seq_next`
- `commit`
- `recover_start`

## `SDSavior`

### Constructor

`SDSavior(data_path, meta_path, capacity_bytes, *, fsync_data=False, fsync_meta=True, json_dumps_kwargs=None, recover_scan_limit_bytes=None, recovery_checkpoint_interval_records=None, group_commit_records=None)`

- `capacity_bytes` must be a multiple of 8 and at least 16 KiB.
- `json_dumps_kwargs` is copied internally.
- `recover_scan_limit_bytes` can cap recovery scanning.
- `recovery_checkpoint_interval_records` can periodically checkpoint recovery start positions.
- `group_commit_records` batches data and/or metadata fsync work across multiple appends.

### Lifecycle

- `open()`: initialize files, validate/load metadata, recover pointers.
- `close()`: persist metadata and release mmap/fd resources.
- Context manager support: `__enter__()` and `__exit__()`.

### Data Operations

- `append(obj) -> int`: append JSON object and return assigned sequence.
- `append_json_bytes(data) -> int`: append already-encoded JSON bytes and return assigned sequence.
- `commit()`: flush pending grouped writes according to the configured durability flags.
- `iter_records(from_seq=None)`: iterate `(seq, ts_ns, obj)` from tail to head.
- `export_jsonl(out_path, from_seq=None)`: write records to JSONL file.

### Internal Mechanics (for whomever wish to contribute)

- `_write_wrap_marker(off)`: writes wrap marker and optional data `fsync`.
- `_make_space(need)`: advances tail to free capacity.
- `_load_meta()` / `_write_meta()`: two-slot metadata persistence with CRC.
- `_recover()`: validates stream and truncates to last known good offset.

## Public Import Surface

```python
from sdsavior import SDSavior, MetaState
```
