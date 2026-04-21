# Benchmarks

## Append

| backend  | capacity (MiB) | payload (B) | durability | rec/s | MB/s | p50 us | p95 us | p99 us |
|----------|---------------:|------------:|------------|------:|-----:|-------:|-------:|-------:|
| file     |              8 |         256 | full_fsync |  5768 | 1.41 |   14.5 |   22.1 |   29.0 |
| file     |              8 |         256 | none       |  5944 | 1.45 |   10.2 |   17.5 |   22.0 |
| file     |              8 |        1024 | full_fsync |  5184 | 5.06 |   16.5 |   24.1 |   39.1 |
| file     |              8 |        1024 | none       |  5319 | 5.19 |   12.2 |   19.4 |   33.8 |
| mmap     |              8 |         256 | full_fsync |  5984 | 1.46 |    9.2 |   22.8 |   27.6 |
| mmap     |              8 |         256 | none       |  6125 | 1.50 |    5.9 |   19.6 |   23.2 |
| mmap     |              8 |        1024 | full_fsync |  5379 | 5.25 |   10.8 |   23.4 |   29.0 |
| mmap     |              8 |        1024 | none       |  5504 | 5.37 |    7.3 |   20.3 |   24.1 |
| sdsavior |              8 |         256 | full_fsync |  2907 | 0.71 |  171.9 |  204.8 |  229.6 |
| sdsavior |              8 |         256 | meta_only  |  2939 | 0.72 |  167.9 |  200.9 |  229.5 |
| sdsavior |              8 |         256 | none       |  2992 | 0.73 |  163.4 |  194.2 |  218.5 |
| sdsavior |              8 |        1024 | full_fsync |  2496 | 2.44 |  212.4 |  242.6 |  273.0 |
| sdsavior |              8 |        1024 | meta_only  |  2523 | 2.46 |  208.5 |  238.7 |  269.1 |
| sdsavior |              8 |        1024 | none       |  2550 | 2.49 |  204.9 |  234.0 |  263.3 |

## Recovery after forced kill

| backend  | capacity (MiB) | durability | recovery ms | recovered records mean |
|----------|---------------:|------------|------------:|-----------------------:|
| file     |              8 | full_fsync |       56.97 |                  16890 |
| file     |              8 | none       |       60.71 |                  17986 |
| mmap     |              8 | full_fsync |      464.07 |                  14079 |
| mmap     |              8 | none       |      468.38 |                  14077 |
| sdsavior |              8 | full_fsync |      486.07 |                   4586 |
| sdsavior |              8 | meta_only  |      535.95 |                   4650 |
| sdsavior |              8 | none       |      556.72 |                   5206 |

## Notes

- `none`: no per-append durability step.
- `meta_only`: SDSavior only, `fsync_data=False, fsync_meta=True`.
- `full_fsync`: strongest per-append durability mode for each backend.
- `file` and `mmap` are baselines, not feature-equivalent replacements for SDSavior.
