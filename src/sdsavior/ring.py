from __future__ import annotations

import json
import mmap
import os
import struct
import time
import zlib
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, Literal

# -----------------------------
# Low-level formats / constants
# -----------------------------

META_MAGIC = b"RBM1"
DATA_MAGIC = b"RBD1"
META_VERSION = 1
DATA_START = len(DATA_MAGIC)
assert DATA_START == 4, "DATA_MAGIC must be 4 bytes for u32 alignment."

# Record layout:
# total_len   u32   (includes header + payload + padding; special value WRAP_MARKER)
# crc32       u32   (crc of (seq, ts_ns, payload_len, reserved, payload))
# seq         u64
# ts_ns       u64
# payload_len u32
# reserved    u32   (0)
# payload     bytes (utf-8)
# padding     0..7  (to 8-byte alignment)
RECORD_HDR = struct.Struct("<IIQQII")  # 4+4+8+8+4+4 = 32 bytes
RECORD_HDR_SIZE = RECORD_HDR.size      # 32
ALIGN = 8
WRAP_MARKER = 0xFFFFFFFF
ZERO_PADDING = b"\x00" * (ALIGN - 1)

# Meta header layout (one slot):
# magic[4], version u32, capacity u64,
# head u64, tail u64, seq_next u64,
# commit u64, reserved u64,
# crc32 u32, pad u32
META_HDR = struct.Struct("<4sIQQQQQQII")
META_HDR_SIZE = META_HDR.size
META_SLOTS = 2
META_FILE_SIZE = META_HDR_SIZE * META_SLOTS

WrapRecord = tuple[Literal["wrap"], int, int, int, None]
DataRecord = tuple[Literal["rec"], int, int, int, Any]
ParsedRecord = WrapRecord | DataRecord


def _align_up(n: int, a: int = ALIGN) -> int:
    """Round ``n`` up to the next ``a``-byte boundary."""
    return (n + (a - 1)) & ~(a - 1)


def _crc32_bytes(b: bytes) -> int:
    """Return an unsigned CRC32 checksum for ``b``."""
    return zlib.crc32(b) & 0xFFFFFFFF


@dataclass(slots=True)
class MetaState:
    """Recovered/persisted ring-buffer pointer state stored in the meta file."""
    capacity: int
    head: int
    tail: int
    seq_next: int
    commit: int


class SDSavior:
    """
    Crash-recoverable, memory-mapped ring buffer for JSON records.

    Files:
      - data file: fixed-size, contains records in a ring
      - meta file: small, two-slot header with CRC + commit counter

    Behavior:
      - append(obj) writes JSON into ring; overwrites oldest if needed
      - on open(), loads latest valid meta, then scans to recover consistent head/tail

    Thread safety:
      - instances are not thread-safe; synchronize externally if sharing across threads
    """

    def __init__(
        self,
        data_path: str,
        meta_path: str,
        capacity_bytes: int,
        *,
        fsync_data: bool = False,
        fsync_meta: bool = True,
        json_dumps_kwargs: dict[str, Any] | None = None,
        recover_scan_limit_bytes: int | None = None,
    ):
        """Configure file paths, durability options, and recovery behavior for a ring instance."""
        capacity = int(capacity_bytes)
        if capacity < 1024 * 16:
            raise ValueError("capacity_bytes is too small to be useful.")
        if capacity % ALIGN != 0:
            raise ValueError(f"capacity_bytes must be a multiple of {ALIGN}.")

        self.data_path = data_path
        self.meta_path = meta_path
        self.capacity = capacity
        self.fsync_data = bool(fsync_data)
        self.fsync_meta = bool(fsync_meta)
        if json_dumps_kwargs is None:
            self.json_dumps_kwargs = {
                "separators": (",", ":"),
                "ensure_ascii": False,
            }
        else:
            self.json_dumps_kwargs = dict(json_dumps_kwargs)
        self.recover_scan_limit_bytes = recover_scan_limit_bytes

        self._data_fd: int | None = None
        self._meta_fd: int | None = None
        self._data_mm: mmap.mmap | None = None
        self._meta_mm: mmap.mmap | None = None
        self._state: MetaState | None = None

    def __enter__(self) -> SDSavior:
        """Open the ring buffer when entering a ``with`` block."""
        self.open()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        """Close the ring buffer when leaving a ``with`` block."""
        self.close()

    # ---------- public API ----------

    def open(self) -> None:
        """Open/create ring files, map them into memory, load metadata, and recover state."""
        if self._data_fd is not None or self._meta_fd is not None:
            raise RuntimeError("Ring buffer is already open(). Call close() before open().")

        os.makedirs(os.path.dirname(self.data_path) or ".", exist_ok=True)
        os.makedirs(os.path.dirname(self.meta_path) or ".", exist_ok=True)

        data_fd: int | None = None
        meta_fd: int | None = None
        data_mm: mmap.mmap | None = None
        meta_mm: mmap.mmap | None = None

        try:
            data_fd = os.open(self.data_path, os.O_RDWR | os.O_CREAT)
            meta_fd = os.open(self.meta_path, os.O_RDWR | os.O_CREAT)

            data_size = os.fstat(data_fd).st_size
            meta_size = os.fstat(meta_fd).st_size
            is_new_data_file = data_size == 0

            if data_size not in (0, self.capacity):
                raise ValueError(
                    f"Data file size ({data_size}) != requested capacity ({self.capacity}). "
                    "Use the same capacity or create new files."
                )
            if meta_size not in (0, META_FILE_SIZE):
                raise ValueError(
                    f"Meta file size ({meta_size}) is invalid. "
                    f"Expected 0 or {META_FILE_SIZE} bytes."
                )

            if is_new_data_file:
                os.ftruncate(data_fd, self.capacity)
            if meta_size == 0:
                os.ftruncate(meta_fd, META_FILE_SIZE)

            data_mm = mmap.mmap(data_fd, self.capacity, access=mmap.ACCESS_WRITE)
            meta_mm = mmap.mmap(meta_fd, META_FILE_SIZE, access=mmap.ACCESS_WRITE)

            self._data_fd = data_fd
            self._meta_fd = meta_fd
            self._data_mm = data_mm
            self._meta_mm = meta_mm
            data_fd = None
            meta_fd = None
            data_mm = None
            meta_mm = None

            if not is_new_data_file and self._data_mm[:DATA_START] != DATA_MAGIC:
                raise ValueError(
                    f"Data file {self.data_path!r} has invalid magic; "
                    "refusing to overwrite existing data."
                )
            if is_new_data_file:
                self._data_mm[:DATA_START] = DATA_MAGIC
                self._data_mm.flush()
                if self.fsync_data:
                    os.fsync(self._data_fd)

            loaded = self._load_meta()
            if loaded is None:
                self._state = MetaState(
                    capacity=self.capacity,
                    head=DATA_START,
                    tail=DATA_START,
                    seq_next=1,
                    commit=1,
                )
                self._write_meta(self._state)
            else:
                if loaded.capacity != self.capacity:
                    raise ValueError(
                        f"Meta capacity ({loaded.capacity}) != requested "
                        f"capacity ({self.capacity}). "
                        f"Use the same capacity or create new files."
                    )
                self._state = loaded

            self._recover()
        except Exception:
            if data_mm is not None and data_mm is not self._data_mm:
                data_mm.close()
            if meta_mm is not None and meta_mm is not self._meta_mm:
                meta_mm.close()
            if data_fd is not None and data_fd != self._data_fd:
                os.close(data_fd)
            if meta_fd is not None and meta_fd != self._meta_fd:
                os.close(meta_fd)
            self._cleanup_open_handles()
            raise

    def close(self) -> None:
        """Persist metadata and release mmap/file descriptors."""
        try:
            if self._state is not None:
                self._write_meta(self._state)
        finally:
            if self._data_mm is not None:
                self._data_mm.flush()
                self._data_mm.close()
                self._data_mm = None

            if self._meta_mm is not None:
                self._meta_mm.flush()
                self._meta_mm.close()
                self._meta_mm = None

            if self._data_fd is not None:
                os.close(self._data_fd)
                self._data_fd = None

            if self._meta_fd is not None:
                os.close(self._meta_fd)
                self._meta_fd = None

            self._state = None

    def _cleanup_open_handles(self) -> None:
        """Best-effort cleanup of mapped files and fds without persisting metadata."""
        if self._data_mm is not None:
            self._data_mm.close()
            self._data_mm = None

        if self._meta_mm is not None:
            self._meta_mm.close()
            self._meta_mm = None

        if self._data_fd is not None:
            os.close(self._data_fd)
            self._data_fd = None

        if self._meta_fd is not None:
            os.close(self._meta_fd)
            self._meta_fd = None

        self._state = None

    def append(self, obj: Any) -> int:
        """
        Append a JSON object. Returns the sequence number written.
        Overwrites oldest records if needed.
        """
        self._require_open()
        assert self._state is not None
        assert self._data_mm is not None
        assert self._data_fd is not None
        s = self._state
        mm = self._data_mm
        data_fd = self._data_fd
        dump_kwargs: Any = self.json_dumps_kwargs

        payload = (json.dumps(obj, **dump_kwargs) + "\n").encode("utf-8")
        payload_len = len(payload)
        total_len = _align_up(RECORD_HDR_SIZE + payload_len, ALIGN)

        if total_len > self.capacity - DATA_START:
            raise ValueError("Single record is too large for the ring buffer capacity.")

        self._make_space(total_len)

        head = s.head
        if head + total_len > self.capacity:
            self._write_wrap_marker(head)
            head = DATA_START
            s.head = head
            self._make_space(total_len)

        seq = s.seq_next
        ts_ns = time.time_ns()

        reserved = 0
        hdr_wo_total_crc = struct.pack("<QQII", seq, ts_ns, payload_len, reserved)
        crc = _crc32_bytes(hdr_wo_total_crc + payload)

        hdr = RECORD_HDR.pack(total_len, crc, seq, ts_ns, payload_len, reserved)

        mm[head:head + RECORD_HDR_SIZE] = hdr
        mm[head + RECORD_HDR_SIZE:head + RECORD_HDR_SIZE + payload_len] = payload

        pad_start = head + RECORD_HDR_SIZE + payload_len
        pad_end = head + total_len
        if pad_end > pad_start:
            mm[pad_start:pad_end] = ZERO_PADDING[:pad_end - pad_start]

        s.head = head + total_len
        if s.head >= self.capacity:
            s.head = DATA_START

        s.seq_next = seq + 1
        s.commit += 1

        self._write_meta(s)

        if self.fsync_data:
            mm.flush()  # flush entire mapping (simple + safe)
            os.fsync(data_fd)

        return seq

    def iter_records(self, *, from_seq: int | None = None) -> Iterator[tuple[int, int, Any]]:
        """
        Iterate records from tail -> head, yielding (seq, ts_ns, obj).
        If from_seq is provided, skips older sequences.
        """
        self._require_open()
        assert self._state is not None
        s = self._state

        off = s.tail
        scanned = 0
        limit = self.capacity

        while off != s.head and scanned < limit:
            rec = self._read_record(off)
            if rec is None:
                break
            kind, next_off, seq, ts_ns, obj = rec

            step = self._distance(off, next_off)
            if step <= 0:
                break
            scanned += step

            if kind == "wrap":
                off = next_off
                continue

            if from_seq is None or seq >= from_seq:
                yield (seq, ts_ns, obj)

            off = next_off

    def export_jsonl(self, out_path: str, *, from_seq: int | None = None) -> None:
        """Write current records to a JSONL file, optionally starting from a sequence number."""
        os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
        dump_kwargs: Any = self.json_dumps_kwargs
        with open(out_path, "wb") as f:
            for _seq, _ts_ns, obj in self.iter_records(from_seq=from_seq):
                line = (json.dumps(obj, **dump_kwargs) + "\n").encode("utf-8")
                f.write(line)

    # ---------- internals ----------

    def _require_open(self) -> None:
        """Ensure all runtime handles exist before operations that require an open ring."""
        if (
            self._state is None
            or self._data_mm is None
            or self._meta_mm is None
            or self._data_fd is None
            or self._meta_fd is None
        ):
            raise RuntimeError("Ring buffer is not open(). Call open() first.")

    @staticmethod
    def _ensure_file_size(fd: int, size: int) -> None:
        """Resize a file descriptor to an exact size if it differs."""
        st = os.fstat(fd)
        if st.st_size != size:
            os.ftruncate(fd, size)

    def _write_wrap_marker(self, off: int) -> None:
        """Write a wrap marker at ``off`` so readers restart from ``DATA_START``."""
        assert self._data_mm is not None
        assert self._data_fd is not None
        mm = self._data_mm
        data_fd = self._data_fd

        mm[off:off + DATA_START] = struct.pack("<I", WRAP_MARKER)
        if off + RECORD_HDR_SIZE <= self.capacity:
            mm[off + DATA_START:off + RECORD_HDR_SIZE] = b"\x00" * (RECORD_HDR_SIZE - DATA_START)
        # Some platforms require page-aligned offsets for flush/msync.
        # Wrap marker writes are tiny, so flush the whole mapping.
        mm.flush()
        if self.fsync_data:
            os.fsync(data_fd)

    def _read_record(self, off: int) -> ParsedRecord | None:
        """Parse and validate a record at ``off``; return ``None`` on any corruption."""
        assert self._data_mm is not None
        mm = self._data_mm

        if off < DATA_START or off + DATA_START > self.capacity:
            return None

        (total_len,) = struct.unpack_from("<I", mm, off)
        if total_len == WRAP_MARKER:
            if off == DATA_START:
                return None
            return ("wrap", DATA_START, 0, 0, None)

        if total_len < RECORD_HDR_SIZE or total_len > self.capacity:
            return None
        if off + total_len > self.capacity:
            return None

        total_len, crc, seq, ts_ns, payload_len, reserved = RECORD_HDR.unpack_from(mm, off)
        if reserved != 0:
            return None
        if (RECORD_HDR_SIZE + payload_len) > total_len:
            return None

        payload = mm[off + RECORD_HDR_SIZE: off + RECORD_HDR_SIZE + payload_len]
        hdr_wo_total_crc = struct.pack("<QQII", seq, ts_ns, payload_len, 0)
        if _crc32_bytes(hdr_wo_total_crc + payload) != crc:
            return None

        try:
            obj = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None

        next_off = off + total_len
        if next_off >= self.capacity:
            next_off = DATA_START
        return ("rec", next_off, seq, ts_ns, obj)

    def _distance(self, a: int, b: int) -> int:
        """Return ring distance from offset ``a`` to ``b`` respecting wrap-around."""
        if b >= a:
            return b - a
        return (self.capacity - a) + (b - DATA_START)

    def _used_bytes(self) -> int:
        """Return currently used bytes between tail and head."""
        assert self._state is not None
        s = self._state
        return self._distance(s.tail, s.head)

    def _make_space(self, need: int) -> None:
        """Advance tail until at least ``need`` bytes are free for a new record."""
        assert self._state is not None
        s = self._state
        free = (self.capacity - DATA_START) - self._used_bytes()
        if need <= free:
            return

        tail_changed = False
        while need > ((self.capacity - DATA_START) - self._used_bytes()):
            rec = self._read_record(s.tail)
            if rec is None:
                if s.tail != s.head:
                    s.tail = s.head
                    tail_changed = True
                break
            kind, next_off, *_ = rec
            s.tail = next_off
            tail_changed = True

        if tail_changed:
            s.commit += 1

    # ---------- metadata: double-buffered header with CRC ----------

    def _pack_meta(self, st: MetaState) -> bytes:
        """Serialize metadata state with CRC for durable double-buffered commits."""
        reserved = 0
        crc_placeholder = 0
        pad = 0
        raw = META_HDR.pack(
            META_MAGIC,
            META_VERSION,
            st.capacity,
            st.head,
            st.tail,
            st.seq_next,
            st.commit,
            reserved,
            crc_placeholder,
            pad,
        )
        crc = _crc32_bytes(raw[:-8])
        return META_HDR.pack(
            META_MAGIC,
            META_VERSION,
            st.capacity,
            st.head,
            st.tail,
            st.seq_next,
            st.commit,
            reserved,
            crc,
            pad,
        )

    def _unpack_meta(self, buf: bytes) -> MetaState | None:
        """Deserialize metadata and validate format, version, ranges, and CRC."""
        try:
            magic, ver, cap, head, tail, seq_next, commit, reserved, crc, pad = META_HDR.unpack(buf)
        except struct.error:
            return None
        if magic != META_MAGIC or ver != META_VERSION:
            return None
        if _crc32_bytes(buf[:-8]) != crc:
            return None
        if not (DATA_START <= head < cap and DATA_START <= tail < cap):
            return None
        return MetaState(capacity=cap, head=head, tail=tail, seq_next=seq_next, commit=commit)

    def _load_meta(self) -> MetaState | None:
        """Load the newest valid metadata slot, or ``None`` if both are invalid."""
        assert self._meta_mm is not None
        mm = self._meta_mm
        slot0 = mm[0:META_HDR_SIZE]
        slot1 = mm[META_HDR_SIZE: 2 * META_HDR_SIZE]

        st0 = self._unpack_meta(slot0)
        st1 = self._unpack_meta(slot1)

        if st0 and st1:
            return st0 if st0.commit >= st1.commit else st1
        return st0 or st1

    def _write_meta(self, st: MetaState) -> None:
        """Persist metadata into the alternate slot and optionally fsync it."""
        assert self._meta_mm is not None
        assert self._meta_fd is not None
        mm = self._meta_mm
        cur = self._load_meta()
        if cur is None:
            target_slot = 0
        else:
            target_slot = 1 if (cur.commit % 2 == 0) else 0

        raw = self._pack_meta(st)
        start = target_slot * META_HDR_SIZE
        mm[start:start + META_HDR_SIZE] = raw
        # Some platforms require page-aligned offsets for flush/msync.
        # Meta is tiny, so flush the whole mapping.
        mm.flush()
        if self.fsync_meta:
            os.fsync(self._meta_fd)

    # ---------- crash recovery scan ----------

    def _recover(self) -> None:
        """Scan records after open and truncate pointers to the last known good position."""
        assert self._state is not None
        s = self._state

        if s.tail == s.head:
            return

        off = s.tail
        last_good_off = off
        last_seq: int | None = None
        scanned = 0
        if self.recover_scan_limit_bytes is None:
            limit = self.capacity
        else:
            limit = self.recover_scan_limit_bytes
        truncated = False

        while off != s.head and scanned < limit:
            rec = self._read_record(off)
            if rec is None:
                s.head = last_good_off
                s.commit += 1
                self._write_meta(s)
                truncated = True
                break

            kind, next_off, seq, _ts_ns, _obj = rec
            step = self._distance(off, next_off)
            if step <= 0:
                s.head = last_good_off
                s.commit += 1
                self._write_meta(s)
                truncated = True
                break

            if kind == "wrap":
                scanned += step
                off = next_off
                last_good_off = off
                continue

            last_seq = seq
            scanned += step
            off = next_off
            last_good_off = off

        if not truncated and scanned >= limit and off != s.head:
            s.head = last_good_off
            s.commit += 1
            self._write_meta(s)

        if last_seq is not None and s.seq_next <= last_seq:
            s.seq_next = last_seq + 1
            s.commit += 1
            self._write_meta(s)
