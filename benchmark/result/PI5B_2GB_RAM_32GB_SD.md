# Benchmarks

## Append

| backend  | capacity (MiB) | payload (B) | durability | rec/s |  MB/s | p50 us | p95 us | p99 us |
|----------|---------------:|------------:|------------|------:|------:|-------:|-------:|-------:|
| file     |              8 |         256 | full_fsync | 43246 | 10.56 |    2.0 |    2.0 |    3.6 |
| file     |              8 |         256 | none       | 44586 | 10.89 |    1.3 |    1.4 |    3.0 |
| file     |              8 |        1024 | full_fsync | 37764 | 36.88 |    2.2 |    3.8 |    4.6 |
| file     |              8 |        1024 | none       | 38965 | 38.05 |    1.5 |    3.1 |    3.9 |
| mmap     |              8 |         256 | full_fsync | 45083 | 11.01 |    1.1 |    1.2 |    3.7 |
| mmap     |              8 |         256 | none       | 46502 | 11.35 |    0.6 |    0.7 |    3.2 |
| mmap     |              8 |        1024 | full_fsync | 39464 | 38.54 |    1.4 |    1.7 |    4.2 |
| mmap     |              8 |        1024 | none       | 40536 | 39.59 |    0.9 |    1.2 |    3.7 |
| sdsavior |              8 |         256 | full_fsync | 23797 |  5.81 |   20.2 |   20.6 |   24.0 |
| sdsavior |              8 |         256 | meta_only  | 24193 |  5.91 |   19.5 |   19.9 |   23.3 |
| sdsavior |              8 |         256 | none       | 24625 |  6.01 |   18.8 |   19.3 |   22.6 |
| sdsavior |              8 |        1024 | full_fsync | 19345 | 18.89 |   26.9 |   27.4 |   30.6 |
| sdsavior |              8 |        1024 | meta_only  | 19587 | 19.13 |   26.2 |   26.7 |   29.9 |
| sdsavior |              8 |        1024 | none       | 19899 | 19.43 |   25.5 |   26.1 |   29.2 |

## Recovery after forced kill

| backend  | capacity (MiB) | durability | recovery ms | recovered records mean |
|----------|---------------:|------------|------------:|-----------------------:|
| file     |              8 | full_fsync |       46.20 |                 154545 |
| file     |              8 | none       |       49.72 |                 166618 |
| mmap     |              8 | full_fsync |       66.83 |                  14044 |
| mmap     |              8 | none       |       63.85 |                  14043 |
| sdsavior |              8 | full_fsync |      111.04 |                   8201 |
| sdsavior |              8 | meta_only  |      129.00 |                   9424 |
| sdsavior |              8 | none       |      139.25 |                  10400 |

## Notes

- `none`: no per-append durability step.
- `meta_only`: SDSavior only, `fsync_data=False, fsync_meta=True`.
- `full_fsync`: strongest per-append durability mode for each backend.
- `file` and `mmap` are baselines, not feature-equivalent replacements for SDSavior.
