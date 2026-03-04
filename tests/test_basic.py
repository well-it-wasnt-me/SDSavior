from __future__ import annotations

from pathlib import Path

import pytest

from sdsavior import SDSavior
from sdsavior.ring import DATA_START


def test_append_and_iter(tmp_path: Path) -> None:
    """Verify append order and basic iteration output."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"

    rb = SDSavior(str(data), str(meta), 256 * 1024)
    rb.open()
    try:
        s1 = rb.append({"a": 1})
        s2 = rb.append({"b": "x"})
        assert s2 == s1 + 1

        rows = list(rb.iter_records())
        assert len(rows) == 2
        assert rows[0][0] == s1
        assert rows[0][2] == {"a": 1}
        assert rows[1][2] == {"b": "x"}
    finally:
        rb.close()


def test_reopen_recovers(tmp_path: Path) -> None:
    """Verify records remain readable after closing and reopening files."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"

    with SDSavior(str(data), str(meta), 256 * 1024) as rb:
        rb.append({"n": 1})
        rb.append({"n": 2})

    with SDSavior(str(data), str(meta), 256 * 1024) as rb2:
        rows = list(rb2.iter_records())
        assert [r[2]["n"] for r in rows] == [1, 2]


def test_capacity_must_be_aligned(tmp_path: Path) -> None:
    """Validate that non-8-byte-aligned capacities are rejected."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"

    with pytest.raises(ValueError, match="multiple of 8"):
        SDSavior(str(data), str(meta), (16 * 1024) + 1)


def test_json_kwargs_are_copied(tmp_path: Path) -> None:
    """Ensure caller-owned JSON kwargs are copied, not referenced."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    kwargs = {"ensure_ascii": True}

    rb = SDSavior(str(data), str(meta), 256 * 1024, json_dumps_kwargs=kwargs)
    kwargs["ensure_ascii"] = False

    assert rb.json_dumps_kwargs["ensure_ascii"] is True


def test_open_rejects_existing_invalid_magic(tmp_path: Path) -> None:
    """Ensure existing non-ring files are not silently overwritten on open."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    capacity = 256 * 1024

    data.write_bytes(b"BAD!" + b"\x00" * (capacity - 4))
    meta.write_bytes(b"")
    size_before = data.stat().st_size

    rb = SDSavior(str(data), str(meta), capacity)
    with pytest.raises(ValueError, match="invalid magic"):
        rb.open()
    assert data.stat().st_size == size_before


def test_recover_truncates_when_scan_limit_is_reached(tmp_path: Path) -> None:
    """Ensure limited recovery scans truncate unvalidated tail data."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    capacity = 256 * 1024

    with SDSavior(str(data), str(meta), capacity) as rb:
        rb.append({"n": 1})
        rb.append({"n": 2})
        rb.append({"n": 3})

    with SDSavior(str(data), str(meta), capacity, recover_scan_limit_bytes=1) as rb_limited:
        rows = list(rb_limited.iter_records())
        assert [r[2]["n"] for r in rows] == [1]


def test_iter_records_ignores_recover_scan_limit_after_open(tmp_path: Path) -> None:
    """Ensure iteration is not implicitly capped by recovery-only scan limits."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    capacity = 256 * 1024

    with SDSavior(str(data), str(meta), capacity) as rb:
        rb.append({"n": 1})
        rb.append({"n": 2})
        rb.append({"n": 3})

    with SDSavior(str(data), str(meta), capacity) as rb2:
        rb2.recover_scan_limit_bytes = 1
        rows = list(rb2.iter_records())
        assert [r[2]["n"] for r in rows] == [1, 2, 3]


def test_write_wrap_marker_fsyncs_data_when_enabled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify wrap-marker writes call ``fsync`` when data syncing is enabled."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    fsync_calls: list[int] = []

    def fake_fsync(fd: int) -> None:
        """Capture fsync file descriptors for assertion."""
        fsync_calls.append(fd)

    monkeypatch.setattr("sdsavior.ring.os.fsync", fake_fsync)

    with SDSavior(str(data), str(meta), 256 * 1024, fsync_data=True, fsync_meta=False) as rb:
        assert rb._data_fd is not None
        data_fd = rb._data_fd
        rb._write_wrap_marker(DATA_START)
        assert data_fd in fsync_calls


def test_make_space_batches_commit_increment(tmp_path: Path) -> None:
    """Ensure tail-eviction updates the commit counter once per space-making pass."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"

    with SDSavior(str(data), str(meta), 16 * 1024) as rb:
        for i in range(64):
            rb.append({"n": i, "payload": "x" * 64})

        assert rb._state is not None
        before_commit = rb._state.commit
        before_tail = rb._state.tail

        rb._make_space(rb.capacity - DATA_START - 8)

        assert rb._state.commit == before_commit + 1
        assert rb._state.tail != before_tail
