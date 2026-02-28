# sdsavior
Most likely i have re-invented the wheel here. But i was to lazy to find something like this. 
So, here you go...

A crash-recoverable, memory-mapped ring buffer for JSON records, designed for SD-card-ish environments
(where constant small writes are a fun way to speedrun media death).

## Install

```bash
pip install sdsavior
```

## Quick Start

```python
from sdsavior import SDSavior

rb = SDSavior(
    data_path="data.ring",
    meta_path="data.meta",
    capacity_bytes=8 * 1024 * 1024,
)

rb.open()
rb.append({"hello": "pi"})
rb.append({"n": 123})

for seq, ts_ns, obj in rb.iter_records():
    print(seq, ts_ns, obj)

rb.close()
```

Or with a context manager:

```python
from sdsavior import SDSavior

with SDSavior("data.ring", "data.meta", 8 * 1024 * 1024) as rb:
    rb.append({"ok": True})
```

## CLI
Export current contents to a JSONL

```bash
sdsavior export --data data.ring --meta data.meta --capacity 8388608 --out out.jsonl
```

## IMPORTANT NOTES
- Records are stored as JSON lines (`\\n` appended).
- Metadata is double-buffered with CRC and a commit counter for crash recovery.
- By default it does not fsync data pages on every append (to reduce wear); metadata is fsync'd.

# TODO
- fix ci for pypy publishing...