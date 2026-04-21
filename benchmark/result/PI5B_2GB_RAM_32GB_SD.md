
# Benchmarks
**Python Version**: 3.13.5
**Platform**: Linux-6.12.62+rpt-rpi-2712-aarch64-with-glibc2.41
**Board**: Raspberry Pi 5 b
**RAM**: 2 GB
**SD**: 32 GB

## Append

| backend  | capacity (MiB) | payload (B) | durability | rec/s |  MB/s | p50 us | p95 us | p99 us |
|----------|---------------:|------------:|------------|------:|------:|-------:|-------:|-------:|
| file     |              8 |         256 | full_fsync | 43090 | 10.52 |    2.0 |    2.0 |    3.7 |
| file     |              8 |         256 | none       | 44458 | 10.85 |    1.3 |    1.4 |    3.0 |
| file     |              8 |        1024 | full_fsync | 37650 | 36.77 |    2.2 |    3.9 |    4.7 |
| file     |              8 |        1024 | none       | 38822 | 37.91 |    1.5 |    3.1 |    4.0 |
| mmap     |              8 |         256 | full_fsync | 44970 | 10.98 |    1.1 |    1.2 |    3.7 |
| mmap     |              8 |         256 | none       | 46414 | 11.33 |    0.6 |    0.7 |    3.2 |
| mmap     |              8 |        1024 | full_fsync | 39385 | 38.46 |    1.4 |    1.7 |    4.3 |
| mmap     |              8 |        1024 | none       | 40468 | 39.52 |    0.9 |    1.2 |    3.8 |
| sdsavior |              8 |         256 | full_fsync | 33087 |  8.08 |    8.5 |    8.7 |   11.6 |
| sdsavior |              8 |         256 | meta_only  | 34499 |  8.42 |    7.3 |    7.5 |   10.3 |
| sdsavior |              8 |         256 | none       | 38418 |  9.38 |    4.8 |    5.0 |    7.7 |
| sdsavior |              8 |        1024 | full_fsync | 27721 | 27.07 |   12.2 |   12.8 |   13.2 |
| sdsavior |              8 |        1024 | meta_only  | 28747 | 28.07 |   11.0 |   11.5 |   11.9 |
| sdsavior |              8 |        1024 | none       | 31610 | 30.87 |    8.4 |    8.9 |    9.2 |

## Recovery after forced kill

| backend  | capacity (MiB) | durability | recovery ms | recovered records mean |
|----------|---------------:|------------|------------:|-----------------------:|
| file     |              8 | full_fsync |       47.68 |                 154823 |
| file     |              8 | none       |       50.37 |                 166769 |
| mmap     |              8 | full_fsync |       63.50 |                  14044 |
| mmap     |              8 | none       |       63.52 |                  14043 |
| sdsavior |              8 | full_fsync |       95.89 |                  13272 |
| sdsavior |              8 | meta_only  |       93.11 |                  11061 |
| sdsavior |              8 | none       |      118.83 |                  13272 |

## Notes

- `none`: no per-append durability step.
- `meta_only`: SDSavior only, `fsync_data=False, fsync_meta=True`.
- `full_fsync`: strongest per-append durability mode for each backend.
- `file` and `mmap` are baselines, not feature-equivalent replacements for SDSavior.
