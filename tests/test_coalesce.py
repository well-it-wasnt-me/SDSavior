from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from sdsavior import SDSavior
from sdsavior.ring import RECORD_HDR_SIZE

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _record_offsets(rb: SDSavior) -> list[int]:
    """Return offsets for currently readable records from tail to head."""
    assert rb._state is not None
    offsets: list[int] = []
    off = rb._state.tail
    while off != rb._state.head:
        rec = rb._read_record(off)
        if rec is None:
            break
        kind, next_off, *_ = rec
        if kind == "rec":
            offsets.append(off)
        off = next_off
    return offsets


# ---------------------------------------------------------------------------
# Write coalescing
# ---------------------------------------------------------------------------

def test_coalesce_disabled_by_default(tmp_path: Path) -> None:
    """Default SDSavior writes immediately with no pending buffer."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    with SDSavior(str(data), str(meta), 256 * 1024) as rb:
        rb.append({"n": 1})
        assert len(rb._pending) == 0
        rows = list(rb.iter_records())
        assert len(rows) == 1


def test_coalesce_buffers_records(tmp_path: Path) -> None:
    """Records are buffered when coalescing is enabled."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    with SDSavior(
        str(data), str(meta), 256 * 1024,
        coalesce_max_records=10,
    ) as rb:
        s1 = rb.append({"n": 1})
        s2 = rb.append({"n": 2})
        assert s2 == s1 + 1
        assert len(rb._pending) == 2
        # Records readable via iter (includes pending)
        rows = list(rb.iter_records())
        assert [r[2]["n"] for r in rows] == [1, 2]


def test_coalesce_count_threshold(tmp_path: Path) -> None:
    """Auto-flush when max_records threshold is reached."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    with SDSavior(
        str(data), str(meta), 256 * 1024,
        coalesce_max_records=3,
    ) as rb:
        rb.append({"n": 1})
        rb.append({"n": 2})
        assert len(rb._pending) == 2
        rb.append({"n": 3})  # triggers flush
        assert len(rb._pending) == 0
        rows = list(rb.iter_records())
        assert [r[2]["n"] for r in rows] == [1, 2, 3]


def test_coalesce_time_threshold(tmp_path: Path) -> None:
    """Auto-flush when max_seconds has elapsed since last flush."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    fake_time = [100.0]

    with SDSavior(
        str(data), str(meta), 256 * 1024,
        coalesce_max_seconds=5.0,
    ) as rb:
        with patch("sdsavior.ring.time.monotonic", side_effect=lambda: fake_time[0]):
            rb._last_flush_time = 100.0
            rb.append({"n": 1})
            assert len(rb._pending) == 1  # not flushed yet

            fake_time[0] = 106.0  # 6 seconds later
            rb.append({"n": 2})
            assert len(rb._pending) == 0  # flushed


def test_coalesce_explicit_flush(tmp_path: Path) -> None:
    """Manual flush() writes all pending records."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    with SDSavior(
        str(data), str(meta), 256 * 1024,
        coalesce_max_records=100,  # high threshold
    ) as rb:
        rb.append({"n": 1})
        rb.append({"n": 2})
        assert len(rb._pending) == 2
        rb.flush()
        assert len(rb._pending) == 0


def test_coalesce_close_flushes_pending(tmp_path: Path) -> None:
    """close() persists buffered records."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"

    with SDSavior(
        str(data), str(meta), 256 * 1024,
        coalesce_max_records=100,
    ) as rb:
        rb.append({"n": 1})
        rb.append({"n": 2})
        assert len(rb._pending) == 2
    # Reopen and verify
    with SDSavior(str(data), str(meta), 256 * 1024) as rb2:
        rows = list(rb2.iter_records())
        assert [r[2]["n"] for r in rows] == [1, 2]


def test_coalesce_iter_includes_pending(tmp_path: Path) -> None:
    """iter_records yields both flushed and pending records."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    with SDSavior(
        str(data), str(meta), 256 * 1024,
        coalesce_max_records=100,
    ) as rb:
        rb.append({"n": 1})
        rb.flush()
        rb.append({"n": 2})  # still pending
        assert len(rb._pending) == 1
        rows = list(rb.iter_records())
        assert [r[2]["n"] for r in rows] == [1, 2]


def test_coalesce_reopen_recovers(tmp_path: Path) -> None:
    """Close + reopen preserves all coalesced records."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"

    with SDSavior(
        str(data), str(meta), 256 * 1024,
        coalesce_max_records=5,
    ) as rb:
        for i in range(10):
            rb.append({"n": i})

    with SDSavior(str(data), str(meta), 256 * 1024) as rb2:
        rows = list(rb2.iter_records())
        assert [r[2]["n"] for r in rows] == list(range(10))


def test_coalesce_single_meta_write_per_flush(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify meta is written once per flush batch, not per record."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    meta_writes: list[int] = []

    with SDSavior(
        str(data), str(meta), 256 * 1024,
        coalesce_max_records=100,
        fsync_meta=False,
    ) as rb:
        original_write_meta = rb._write_meta

        def counting_write_meta(st):
            meta_writes.append(1)
            original_write_meta(st)

        monkeypatch.setattr(rb, "_write_meta", counting_write_meta)
        for i in range(5):
            rb.append({"n": i})
        meta_writes.clear()
        rb.flush()
        # One meta write for the entire batch
        assert len(meta_writes) == 1


# ---------------------------------------------------------------------------
# Sector-aligned writes
# ---------------------------------------------------------------------------

def test_sector_aligned_flush(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify _sector_flush is called with aligned params during coalesced flush."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    sector_flush_calls: list[tuple] = []

    with SDSavior(
        str(data), str(meta), 256 * 1024,
        coalesce_max_records=100,
        sector_size=4096,
    ) as rb:
        original_sector_flush = rb._sector_flush

        def capture_sector_flush(mm, fd, write_start, write_end):
            sector_flush_calls.append((write_start, write_end))
            return original_sector_flush(mm, fd, write_start, write_end)

        monkeypatch.setattr(rb, "_sector_flush", capture_sector_flush)

        for i in range(5):
            rb.append({"n": i})
        rb.flush()

        # _sector_flush was called during the coalesced flush
        assert len(sector_flush_calls) >= 1


# ---------------------------------------------------------------------------
# Write stats
# ---------------------------------------------------------------------------

def test_write_stats_tracking(tmp_path: Path) -> None:
    """Verify logical_appends and physical_flushes counters."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    with SDSavior(
        str(data), str(meta), 256 * 1024,
        coalesce_max_records=3,
    ) as rb:
        for i in range(5):
            rb.append({"n": i})
        stats = rb.write_stats
        assert stats["logical_appends"] == 5
        assert stats["physical_flushes"] >= 1
        assert stats["bytes_written"] > 0


def test_write_stats_default(tmp_path: Path) -> None:
    """Stats work even without coalescing."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    with SDSavior(str(data), str(meta), 256 * 1024) as rb:
        rb.append({"n": 1})
        stats = rb.write_stats
        assert stats["logical_appends"] == 1
        assert stats["physical_flushes"] == 1
        assert stats["bytes_written"] > 0


# ---------------------------------------------------------------------------
# CRC skip
# ---------------------------------------------------------------------------

def test_skip_corrupt_continues(tmp_path: Path) -> None:
    """Corrupt middle record in open ring, skip_corrupt=True continues past it."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    capacity = 256 * 1024

    with SDSavior(str(data), str(meta), capacity) as rb:
        rb.append({"n": 1})
        rb.append({"n": 2})
        rb.append({"n": 3})
        offsets = _record_offsets(rb)
        assert len(offsets) >= 3

        # Corrupt the second record's payload while ring is still open
        off2 = offsets[1]
        assert rb._data_mm is not None
        payload_off = off2 + RECORD_HDR_SIZE
        rb._data_mm[payload_off] = rb._data_mm[payload_off] ^ 0x01

        # Without skip_corrupt: stops at corruption
        rows_no_skip = list(rb.iter_records(skip_corrupt=False))
        assert [r[2]["n"] for r in rows_no_skip] == [1]

        # With skip_corrupt: continues past corrupt record
        rows_skip = list(rb.iter_records(skip_corrupt=True))
        nums = [r[2]["n"] for r in rows_skip]
        assert 1 in nums
        assert 3 in nums


def test_skip_corrupt_false_stops(tmp_path: Path) -> None:
    """Default behavior preserved: stops at first corrupt record."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    capacity = 256 * 1024

    with SDSavior(str(data), str(meta), capacity) as rb:
        rb.append({"n": 1})
        rb.append({"n": 2})
        rb.append({"n": 3})
        offsets = _record_offsets(rb)

        off2 = offsets[1]
        assert rb._data_mm is not None
        payload_off = off2 + RECORD_HDR_SIZE
        rb._data_mm[payload_off] = rb._data_mm[payload_off] ^ 0x01

        rows = list(rb.iter_records())
        assert [r[2]["n"] for r in rows] == [1]


def test_skip_corrupt_tracks_count(tmp_path: Path) -> None:
    """_last_iter_skipped has correct count."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    capacity = 256 * 1024

    with SDSavior(str(data), str(meta), capacity) as rb:
        rb.append({"n": 1})
        rb.append({"n": 2})
        rb.append({"n": 3})
        offsets = _record_offsets(rb)

        off2 = offsets[1]
        assert rb._data_mm is not None
        payload_off = off2 + RECORD_HDR_SIZE
        rb._data_mm[payload_off] = rb._data_mm[payload_off] ^ 0x01

        list(rb.iter_records(skip_corrupt=True))
        assert rb._last_iter_skipped >= 1


def test_flush_noop_when_empty(tmp_path: Path) -> None:
    """flush() on non-coalescing ring is a safe no-op."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    with SDSavior(str(data), str(meta), 256 * 1024) as rb:
        rb.flush()  # should not raise
