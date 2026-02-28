# Recovery and Format

## Files

- **Data file**: fixed-size ring region.
- **Meta file**: two metadata slots with CRC and commit counters.

## Data Records

Each record stores:

- `total_len` (`u32`)
- `crc32` (`u32`)
- `seq` (`u64`)
- `ts_ns` (`u64`)
- `payload_len` (`u32`)
- `reserved` (`u32`, must be `0`)
- payload bytes (`utf-8` JSON line)
- alignment padding to 8-byte boundary

Special `total_len` value `0xFFFFFFFF` marks wrap points.

## Metadata Layout

Each slot stores:

- magic and version
- ring capacity
- `head`, `tail`, `seq_next`
- `commit`
- `crc32`

On load, the newest valid slot (highest commit) is used.

## Recovery Strategy

On `open()`:

1. Load the newest valid metadata slot.
2. Scan records from `tail` toward `head`.
3. Stop and truncate `head` at the last valid offset if corruption is found.
4. If scan limit is reached before `head`, truncate at last validated offset.
5. Adjust `seq_next` if needed.

This keeps unreadable partial writes out of normal iteration.
