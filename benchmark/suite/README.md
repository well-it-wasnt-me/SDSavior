# SDSavior Benchmark Guide

This document explains how to run and interpret the SDSavior benchmark suite, including the meaning of all reported metrics.

---

# What this benchmark does

The benchmark compares three storage approaches:

* **sdsavior** -> your ring buffer with crash recovery
* **file** -> plain JSON lines appended to a file
* **mmap** -> memory-mapped file with manual writes

The goal is not just "speed", but understanding **trade-offs**:

* raw performance
* latency behavior
* durability guarantees
* recovery characteristics

---

# How to run

```bash
pip install tqdm

python bench_append.py \
  --backend sdsavior --backend file --backend mmap \
  --capacity-mib 8 \
  --payload-bytes 256 1024 \
  --runs 3 \
  --markdown-out results.md
```

---

# Output example

| backend  | cap | payload | mode      | rps   | p95us |
|----------|-----|---------|-----------|-------|-------|
| sdsavior | 8   | 1024    | meta_only | 5200  | 340   |
| file     | 8   | 1024    | none      | 18000 | 40    |
| mmap     | 8   | 1024    | none      | 15000 | 60    |

---

# Understanding the metrics

## 1. backend

Which implementation is being tested:

* **sdsavior** -> safe, recoverable, structured
* **file** -> simplest possible approach
* **mmap** -> low-level memory-mapped writes

---

## 2. capacity (cap)

Size of the storage buffer in MiB.

Example:

* `8` -> 8 MiB ring buffer or file

Why it matters:

* affects wrap-around behavior
* influences cache and IO patterns

---

## 3. payload

Approximate size (in bytes) of each JSON record written.

Examples:

* 256 -> small telemetry
* 1024 -> typical structured logs

Why it matters:

* larger payloads increase serialization cost
* affects write amplification

---

## 4. mode (durability mode)

Defines how aggressively data is flushed to disk.

### `none`

* no fsync
* fastest
* **data loss likely on crash**

### `meta_only` (SDSavior only)

* metadata is flushed
* data pages are not always flushed
* **balanced safety/performance trade-off**

### `full_fsync`

* every write is flushed to disk
* slowest
* **strongest durability guarantee**

---

## 5. rps (records per second)

Number of records written per second.

Formula:

```
rps = total_records / total_time
```

Example:

* 10,000 records in 2 seconds -> 5000 rps

Interpretation:

* higher is better
* measures overall throughput

Important:

* hides latency spikes
* should not be used alone

---

## 6. p95 (95th percentile latency)

Latency below which 95% of operations fall.

Example:

* p95 = 300 µs -> 95% of writes completed in under 300 microseconds

Why it matters:

* shows **tail latency**, not just average
* reveals spikes and jitter

---

## ⚠Why p95 matters more than average

Average latency can lie.

Example:

* 99 writes at 10 µs
* 1 write at 10 ms

Average ≈ 110 µs -> looks fine

But p95 exposes reality:

* p95 ≈ 10 µs
* p99 ≈ 10 ms (spike!)

This is critical for:

* real-time systems
* logging pipelines
* embedded systems (like Raspberry Pi)

---

# What to expect

## file backend

* very high rps
* very low latency
* poor durability

## mmap backend

* fast
* slightly higher latency
* weak safety guarantees

## sdsavior

* lower rps
* higher latency
* **predictable + recoverable**

---

# Real-world interpretation

## If you care about speed only

Use:

* file (no fsync)

## If you care about durability

Use:

* sdsavior (meta_only or full_fsync)

## If you care about correctness after crashes

Use:

* sdsavior

Everything else is gambling.

---

# Common pitfalls

## 1. SD card throttling

Raspberry Pi storage slows down over time.

## 2. OS caching

Results may look faster than reality.

## 3. background processes

They introduce noise in results.

---

# Final takeaway

This benchmark is not about proving SDSavior is "fast".

It is about showing:

* what you gain (recovery, structure)
* what you pay (latency, throughput)

A good benchmark doesn’t make you look good.

It makes the trade-offs impossible to ignore.

# Commands to execute

```bash
$ python3 bench_append.py \
  --backend sdsavior --backend file --backend mmap \
  --capacity-mib 8 \
  --payload-bytes 256 1024 \
  --runs 3 \
  --json-out append.jsonl \
  --markdown-out APPEND.md
```
```bash
python3 bench_recovery.py \
  --backend sdsavior --backend file --backend mmap \
  --capacity-mib 8 \
  --runs 3 \
  --json-out recovery.jsonl \
  --markdown-out RECOVERY.md
```

```bash
python3 generate_benchmark_report.py \
  --append-json append.jsonl \
  --recovery-json recovery.jsonl \
  --out BENCHMARKS.md
```
