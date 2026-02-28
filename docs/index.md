# SDSavior

SDSavior is a crash-recoverable, memory-mapped ring buffer for JSON records.
It is designed for durability and/or sensitive environments where the life of an SD card is important.

## Highlights

- Fixed-size data file with ring semantics.
- Double-buffered metadata with CRC and commit counter.
- Crash recovery that validates record headers, CRC, and payload decoding.
- Optional data `fsync` and metadata `fsync` controls.

## Quick Start

```python
from sdsavior import SDSavior

with SDSavior("data.ring", "data.meta", 8 * 1024 * 1024) as rb:
    rb.append({"sensor": "temp", "value": 23.4})
    rb.append({"whatever": "something", "value": 51})

    for seq, ts_ns, obj in rb.iter_records():
        print(seq, ts_ns, obj)
```

## Documentation Map

- [Installation](installation.md): setup, dev dependencies, doc tooling.
- [Usage](usage.md): lifecycle, appends, iteration, export.
- [CLI](cli.md): command reference and examples.
- [API Reference](api.md): class and method behavior.
- [Recovery and Format](recovery.md): file layouts and recovery model.
- [Development](development.md): tests, linting, typing, docs build.
