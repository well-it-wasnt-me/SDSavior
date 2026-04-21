from __future__ import annotations

import json
import os
import struct
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from sdsavior import SDSavior, cli
from sdsavior.ring import (
    DATA_START,
    META_FILE_SIZE,
    META_HDR_SIZE,
    RECORD_HDR,
    RECORD_HDR_SIZE,
    WRAP_MARKER,
    MetaState,
    _crc32_bytes,
)


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


def _read_jsonl(path: Path) -> list[dict]:
    """Read a JSONL file into a list of dictionaries."""
    rows: list[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            rows.append(json.loads(line))
    return rows


def test_wraparound_end_to_end_via_public_api(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Force wrap-around through append and verify ordered readable output."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    wrap_calls = 0
    original = SDSavior._write_wrap_marker

    def count_wrap_calls(self: SDSavior, off: int) -> None:
        """Count wrap marker writes while preserving original behavior."""
        nonlocal wrap_calls
        wrap_calls += 1
        original(self, off)

    monkeypatch.setattr(SDSavior, "_write_wrap_marker", count_wrap_calls)

    with SDSavior(str(data), str(meta), 16 * 1024) as rb:
        for i in range(300):
            rb.append({"n": i, "payload": "x" * 96})

        rows = list(rb.iter_records())

    assert wrap_calls > 0
    assert rows
    seqs = [row[0] for row in rows]
    nums = [row[2]["n"] for row in rows]
    assert seqs == list(range(seqs[0], seqs[0] + len(seqs)))
    assert nums == list(range(nums[0], nums[0] + len(nums)))
    assert nums[-1] == 299


def test_wraparound_preserves_records_before_and_after_wrap(tmp_path: Path) -> None:
    """Ensure wrapping does not discard the whole pre-wrap segment."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"

    with SDSavior(str(data), str(meta), 16 * 1024, fsync_meta=False) as rb:
        previous_head = DATA_START
        for i in range(100):
            rb.append({"n": i, "payload": "x" * 512})
            assert rb._state is not None
            if rb._state.head < previous_head:
                rows = list(rb.iter_records())
                nums = [row[2]["n"] for row in rows]

                assert len(nums) > 1
                assert nums[-1] == i
                assert nums[0] < i
                assert nums == list(range(nums[0], nums[-1] + 1))
                break
            previous_head = rb._state.head
        else:
            pytest.fail("append workload did not trigger wraparound")


def test_recover_truncates_on_crc_corruption(tmp_path: Path) -> None:
    """Corrupt a record payload byte and verify recovery truncates before it."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    capacity = 256 * 1024

    with SDSavior(str(data), str(meta), capacity) as rb:
        rb.append({"n": 1})
        rb.append({"n": 2})
        rb.append({"n": 3})
        offsets = _record_offsets(rb)
        assert len(offsets) >= 3

        off2 = offsets[1]
        assert rb._data_mm is not None
        payload_off = off2 + RECORD_HDR_SIZE
        rb._data_mm[payload_off] = rb._data_mm[payload_off] ^ 0x01
        rb._data_mm.flush()

    with SDSavior(str(data), str(meta), capacity) as rb2:
        assert [row[2]["n"] for row in rb2.iter_records()] == [1]


def test_recover_truncates_on_reserved_field_corruption(tmp_path: Path) -> None:
    """Corrupt the reserved field and verify recovery truncates before the bad record."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    capacity = 256 * 1024

    with SDSavior(str(data), str(meta), capacity) as rb:
        rb.append({"n": 1})
        rb.append({"n": 2})
        rb.append({"n": 3})
        offsets = _record_offsets(rb)
        assert len(offsets) >= 3

        off2 = offsets[1]
        assert rb._data_mm is not None
        struct.pack_into("<I", rb._data_mm, off2 + 28, 1)
        rb._data_mm.flush()

    with SDSavior(str(data), str(meta), capacity) as rb2:
        assert [row[2]["n"] for row in rb2.iter_records()] == [1]


def test_recover_truncates_on_invalid_json_payload(tmp_path: Path) -> None:
    """Write payload with valid CRC but invalid UTF-8 and verify truncation."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    capacity = 256 * 1024

    with SDSavior(str(data), str(meta), capacity) as rb:
        rb.append({"n": 1})
        rb.append({"n": 2, "s": "abc"})
        rb.append({"n": 3})
        offsets = _record_offsets(rb)
        assert len(offsets) >= 3

        off2 = offsets[1]
        assert rb._data_mm is not None
        total_len, _crc, seq, ts_ns, payload_len, _reserved = RECORD_HDR.unpack_from(
            rb._data_mm,
            off2,
        )
        assert total_len >= RECORD_HDR_SIZE + payload_len

        bad_payload = b"\xff" * payload_len
        payload_off = off2 + RECORD_HDR_SIZE
        rb._data_mm[payload_off:payload_off + payload_len] = bad_payload
        hdr = struct.pack("<QQII", seq, ts_ns, payload_len, 0)
        new_crc = _crc32_bytes(hdr + bad_payload)
        struct.pack_into("<I", rb._data_mm, off2 + 4, new_crc)
        rb._data_mm.flush()

    with SDSavior(str(data), str(meta), capacity) as rb2:
        assert [row[2]["n"] for row in rb2.iter_records()] == [1]


def test_append_rejects_oversized_record(tmp_path: Path) -> None:
    """Reject a single payload that cannot fit in the ring."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"

    with SDSavior(str(data), str(meta), 16 * 1024) as rb:
        with pytest.raises(ValueError, match="too large"):
            rb.append({"blob": "x" * (64 * 1024)})


def test_open_rejects_capacity_mismatch(tmp_path: Path) -> None:
    """Reject opening existing files when requested capacity does not match meta."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    original_capacity = 256 * 1024

    with SDSavior(str(data), str(meta), original_capacity) as rb:
        rb.append({"n": 1})

    size_before = data.stat().st_size
    with pytest.raises(ValueError, match="Data file size"):
        with SDSavior(str(data), str(meta), 16 * 1024) as rb2:
            rb2.iter_records()
    assert data.stat().st_size == size_before


def test_open_twice_requires_close(tmp_path: Path) -> None:
    """Reject calling open on an already-open instance."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    rb = SDSavior(str(data), str(meta), 256 * 1024)

    rb.open()
    try:
        with pytest.raises(RuntimeError, match="already open"):
            rb.open()
    finally:
        rb.close()


def test_closed_state_operations_raise_runtime_error(tmp_path: Path) -> None:
    """Require open state for append, iteration, and export operations."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    rb = SDSavior(str(data), str(meta), 256 * 1024)

    with pytest.raises(RuntimeError, match="not open"):
        rb.append({"n": 1})
    with pytest.raises(RuntimeError, match="not open"):
        list(rb.iter_records())
    with pytest.raises(RuntimeError, match="not open"):
        rb.export_jsonl(str(tmp_path / "out.jsonl"))


def test_close_is_idempotent(tmp_path: Path) -> None:
    """Allow repeated close calls without error."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    rb = SDSavior(str(data), str(meta), 256 * 1024)
    rb.open()
    rb.close()
    rb.close()


def test_cli_export_writes_expected_jsonl(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Run the CLI export command and verify JSONL output content."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    out = tmp_path / "out.jsonl"
    capacity = 256 * 1024

    with SDSavior(str(data), str(meta), capacity) as rb:
        rb.append({"n": 1})
        rb.append({"n": 2})
        rb.append({"n": 3})

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "sdsavior",
            "export",
            "--data",
            str(data),
            "--meta",
            str(meta),
            "--capacity",
            str(capacity),
            "--out",
            str(out),
            "--from-seq",
            "2",
        ],
    )
    assert cli.main() == 0
    assert _read_jsonl(out) == [{"n": 2}, {"n": 3}]


def test_open_cleans_up_handles_when_internal_error_occurs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure open leaves no leaked handles if an internal error occurs mid-open."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    rb = SDSavior(str(data), str(meta), 256 * 1024)

    def raise_on_load_meta(self: SDSavior) -> None:
        """Fail after mappings exist so open() executes cleanup paths."""
        raise RuntimeError("forced load_meta failure")

    monkeypatch.setattr(SDSavior, "_load_meta", raise_on_load_meta)

    with pytest.raises(RuntimeError, match="forced load_meta failure"):
        rb.open()

    assert rb._data_fd is None
    assert rb._meta_fd is None
    assert rb._data_mm is None
    assert rb._meta_mm is None
    assert rb._state is None


def test_open_rejects_invalid_meta_size(tmp_path: Path) -> None:
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    capacity = 16 * 1024

    with open(data, "wb") as f:
        f.truncate(capacity)
    with open(meta, "wb") as f:
        f.truncate(1)

    rb = SDSavior(str(data), str(meta), capacity)
    with pytest.raises(ValueError, match="Meta file size"):
        rb.open()


def test_open_rejects_meta_capacity_mismatch(tmp_path: Path) -> None:
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    capacity = 32 * 1024

    with SDSavior(str(data), str(meta), capacity) as rb:
        rb.append({"n": 1})

    other_capacity = 16 * 1024
    state = MetaState(
        capacity=other_capacity,
        head=DATA_START,
        tail=DATA_START,
        seq_next=1,
        commit=1,
    )
    raw = rb._pack_meta(state)
    meta.write_bytes(raw + b"\x00" * (META_FILE_SIZE - len(raw)))

    rb2 = SDSavior(str(data), str(meta), capacity)
    with pytest.raises(ValueError, match="Meta capacity"):
        rb2.open()


def test_meta_double_buffer_keeps_previous_valid_slot(tmp_path: Path) -> None:
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"

    with SDSavior(str(data), str(meta), 16 * 1024, fsync_meta=False) as rb:
        rb.append({"n": 1})
        assert rb._meta_mm is not None
        assert rb._current_meta_slot is not None

        latest_start = rb._current_meta_slot * META_HDR_SIZE
        original = rb._meta_mm[latest_start]
        rb._meta_mm[latest_start] = original ^ 0xFF
        try:
            loaded = rb._load_meta()
            assert loaded is not None
            assert loaded.commit == 1
        finally:
            rb._meta_mm[latest_start] = original


def test_iter_records_breaks_on_corrupt_record(tmp_path: Path) -> None:
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"

    with SDSavior(str(data), str(meta), 16 * 1024) as rb:
        rb.append({"n": 1})
        assert rb._data_mm is not None
        struct.pack_into("<I", rb._data_mm, DATA_START, 0)
        assert list(rb.iter_records()) == []


def test_iter_records_handles_wrap_marker(tmp_path: Path) -> None:
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"

    with SDSavior(str(data), str(meta), 16 * 1024) as rb:
        assert rb._state is not None
        off = DATA_START + 64
        rb._write_wrap_marker(off)
        rb._state.tail = off
        rb._state.head = DATA_START
        assert list(rb.iter_records()) == []


def test_read_record_invalid_cases(tmp_path: Path) -> None:
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"

    with SDSavior(str(data), str(meta), 16 * 1024) as rb:
        assert rb._data_mm is not None
        mm = rb._data_mm

        assert rb._read_record(0) is None

        struct.pack_into("<I", mm, DATA_START, WRAP_MARKER)
        assert rb._read_record(DATA_START) is None

        struct.pack_into("<I", mm, DATA_START, 0)
        assert rb._read_record(DATA_START) is None

        off = rb.capacity - DATA_START
        struct.pack_into("<I", mm, off, RECORD_HDR_SIZE)
        assert rb._read_record(off) is None

        RECORD_HDR.pack_into(
            mm,
            DATA_START,
            RECORD_HDR_SIZE,
            0,
            1,
            1,
            8,
            0,
        )
        assert rb._read_record(DATA_START) is None


def test_ensure_file_size_expands(tmp_path: Path) -> None:
    path = tmp_path / "size.dat"
    fd = os.open(path, os.O_RDWR | os.O_CREAT)
    try:
        SDSavior._ensure_file_size(fd, 128)
        assert os.fstat(fd).st_size == 128
    finally:
        os.close(fd)


def test_unpack_meta_rejects_invalid_buffers(tmp_path: Path) -> None:
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    rb = SDSavior(str(data), str(meta), 16 * 1024)

    assert rb._unpack_meta(b"short") is None

    state = MetaState(
        capacity=rb.capacity,
        head=DATA_START,
        tail=DATA_START,
        seq_next=1,
        commit=1,
    )
    raw = rb._pack_meta(state)
    corrupt_crc = bytearray(raw)
    corrupt_crc[-8] ^= 0xFF
    assert rb._unpack_meta(bytes(corrupt_crc)) is None

    bad_state = MetaState(
        capacity=rb.capacity,
        head=0,
        tail=0,
        seq_next=1,
        commit=1,
    )
    bad_raw = rb._pack_meta(bad_state)
    assert rb._unpack_meta(bad_raw) is None


def test_make_space_handles_corrupt_tail_record(tmp_path: Path) -> None:
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"

    with SDSavior(str(data), str(meta), 16 * 1024) as rb:
        rb.append({"n": 1})
        assert rb._state is not None
        assert rb._data_mm is not None
        struct.pack_into("<I", rb._data_mm, rb._state.tail, 0)
        rb._make_space((rb.capacity - DATA_START) + 8)
        assert rb._state.tail == rb._state.head


def test_recover_handles_non_advancing_record(
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch
) -> None:
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"

    with SDSavior(str(data), str(meta), 16 * 1024) as rb:
        rb.append({"n": 1})
        assert rb._state is not None
        start_commit = rb._state.commit

        def stuck(_off: int):
            return ("rec", _off, 1, 0, {})

        monkeypatch.setattr(rb, "_read_record", stuck)
        rb._recover()
        assert rb._state.commit == start_commit + 1
        assert rb._state.head == rb._state.tail


def test_recover_wrap_marker_advances(tmp_path: Path) -> None:
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"

    with SDSavior(str(data), str(meta), 16 * 1024) as rb:
        assert rb._state is not None
        off = DATA_START + 64
        rb._write_wrap_marker(off)
        rb._state.tail = off
        rb._state.head = DATA_START
        rb._recover()
        assert rb._state.tail == off
        assert rb._state.head == DATA_START


def test_recover_updates_seq_next(tmp_path: Path) -> None:
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"

    with SDSavior(str(data), str(meta), 16 * 1024) as rb:
        rb.append({"n": 1})
        last_seq = rb.append({"n": 2})
        assert rb._state is not None
        rb._state.seq_next = 1
        rb._recover()
        assert rb._state.seq_next == last_seq + 1


def test_cli_returns_nonzero_for_unknown_command(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        cli.argparse.ArgumentParser,
        "parse_args",
        lambda _self: SimpleNamespace(cmd="noop"),
    )
    assert cli.main() == 2


def test_recover_treats_wrap_marker_at_data_start_as_corruption(tmp_path: Path) -> None:
    """Ensure wrap markers at DATA_START do not cause recovery/iteration hangs."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    capacity = 256 * 1024

    with SDSavior(str(data), str(meta), capacity) as rb:
        rb.append({"n": 1})

    with SDSavior(str(data), str(meta), capacity) as rb:
        assert rb._data_mm is not None
        struct.pack_into("<I", rb._data_mm, 4, 0xFFFFFFFF)
        rb._data_mm.flush()

    with SDSavior(str(data), str(meta), capacity) as rb2:
        assert list(rb2.iter_records()) == []


def test_close_cleans_up_handles_when_meta_write_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure close releases resources even if metadata persistence raises."""
    data = tmp_path / "ring.dat"
    meta = tmp_path / "ring.meta"
    rb = SDSavior(str(data), str(meta), 256 * 1024)
    rb.open()

    def raise_on_write_meta(_st) -> None:
        """Force close() to execute cleanup via the finally branch."""
        raise RuntimeError("forced write_meta failure")

    monkeypatch.setattr(rb, "_write_meta", raise_on_write_meta)

    with pytest.raises(RuntimeError, match="forced write_meta failure"):
        rb.close()

    assert rb._data_fd is None
    assert rb._meta_fd is None
    assert rb._data_mm is None
    assert rb._meta_mm is None
    assert rb._state is None
