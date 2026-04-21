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
- recovery checkpoint offset
- `crc32`

On load, the newest valid slot (highest commit) is used.

## Recovery Strategy

On `open()`:

1. Load the newest valid metadata slot.
2. Scan records from the recovery checkpoint, or from `tail` when no checkpoint is trusted.
3. Stop and truncate `head` at the last valid offset if corruption is found.
4. If scan limit is reached before `head`, truncate at last validated offset.
5. Adjust `seq_next` if needed.

This keeps unreadable partial writes out of normal iteration.

`fsync_data=True` advances the checkpoint after each durable append. The optional
`recovery_checkpoint_interval_records` setting can also create periodic checkpoints
by flushing data pages and storing the current `head`.

When `group_commit_records` is set, those durability and checkpoint updates are
batched. Crash consistency is still protected by record CRC validation and metadata
CRC slots, but the strongest durability point becomes the last completed group commit
or explicit `commit()`.
