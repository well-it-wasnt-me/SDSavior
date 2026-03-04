from __future__ import annotations

import asyncio
import concurrent.futures
from typing import Any

from .ring import SDSavior


class AsyncSDSavior:
    """Async wrapper around SDSavior using run_in_executor for I/O operations."""

    def __init__(
        self,
        *args: Any,
        executor: concurrent.futures.Executor | None = None,
        **kwargs: Any,
    ):
        self._ring = SDSavior(*args, **kwargs)
        self._executor = executor

    async def _run(self, func: Any, *args: Any) -> Any:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, func, *args)

    async def open(self) -> None:
        await self._run(self._ring.open)

    async def close(self) -> None:
        await self._run(self._ring.close)

    async def append(self, obj: Any) -> int:
        return await self._run(self._ring.append, obj)

    async def flush(self) -> None:
        """Flush coalesced records. No-op if coalescing is not enabled."""
        await self._run(self._ring.flush)

    async def iter_records(self, **kwargs: Any) -> list[tuple[int, int, Any]]:
        """Return all records as a list (collected in executor thread).

        Returns a list rather than an async iterator to avoid holding
        mmap resources across await boundaries.
        """

        def _collect() -> list[tuple[int, int, Any]]:
            return list(self._ring.iter_records(**kwargs))

        return await self._run(_collect)

    async def export_jsonl(self, out_path: str, *, from_seq: int | None = None) -> None:
        def _export() -> None:
            self._ring.export_jsonl(out_path, from_seq=from_seq)

        await self._run(_export)

    @property
    def write_stats(self) -> dict[str, int]:
        """Delegate to underlying ring's write_stats."""
        return self._ring.write_stats

    async def __aenter__(self) -> AsyncSDSavior:
        await self.open()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.close()
