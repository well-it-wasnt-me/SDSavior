from __future__ import annotations

import asyncio
import concurrent.futures
from pathlib import Path

import pytest

from sdsavior import AsyncSDSavior


@pytest.fixture
def ring_paths(tmp_path: Path):
    return str(tmp_path / "ring.dat"), str(tmp_path / "ring.meta")


class TestAsyncSDSavior:
    def test_async_append_and_iter(self, ring_paths):
        data, meta = ring_paths

        async def run():
            async with AsyncSDSavior(data, meta, 256 * 1024) as rb:
                s1 = await rb.append({"a": 1})
                s2 = await rb.append({"b": "x"})
                assert s2 == s1 + 1
                rows = await rb.iter_records()
                assert len(rows) == 2
                assert rows[0][2] == {"a": 1}
                assert rows[1][2] == {"b": "x"}

        asyncio.run(run())

    def test_async_context_manager(self, ring_paths):
        data, meta = ring_paths

        async def run():
            async with AsyncSDSavior(data, meta, 256 * 1024) as rb:
                await rb.append({"n": 1})
                rows = await rb.iter_records()
                assert len(rows) == 1

        asyncio.run(run())

    def test_async_export_jsonl(self, ring_paths, tmp_path):
        data, meta = ring_paths
        out = str(tmp_path / "out.jsonl")

        async def run():
            async with AsyncSDSavior(data, meta, 256 * 1024) as rb:
                await rb.append({"n": 1})
                await rb.append({"n": 2})
                await rb.export_jsonl(out)
            import json

            with open(out) as f:
                lines = [json.loads(line) for line in f]
            assert lines == [{"n": 1}, {"n": 2}]

        asyncio.run(run())

    def test_async_flush_noop_without_coalescing(self, ring_paths):
        data, meta = ring_paths

        async def run():
            async with AsyncSDSavior(data, meta, 256 * 1024) as rb:
                await rb.append({"n": 1})
                await rb.flush()  # should not raise
                rows = await rb.iter_records()
                assert len(rows) == 1

        asyncio.run(run())

    def test_async_custom_executor(self, ring_paths):
        data, meta = ring_paths
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

        async def run():
            async with AsyncSDSavior(data, meta, 256 * 1024, executor=executor) as rb:
                await rb.append({"n": 1})
                rows = await rb.iter_records()
                assert len(rows) == 1

        asyncio.run(run())
        executor.shutdown(wait=True)

    def test_async_write_stats(self, ring_paths):
        data, meta = ring_paths

        async def run():
            async with AsyncSDSavior(data, meta, 256 * 1024) as rb:
                await rb.append({"n": 1})
                stats = rb.write_stats
                # write_stats may be empty dict if sync ring doesn't have it yet
                assert isinstance(stats, dict)

        asyncio.run(run())

    def test_async_reopen(self, ring_paths):
        data, meta = ring_paths

        async def run():
            async with AsyncSDSavior(data, meta, 256 * 1024) as rb:
                await rb.append({"n": 1})
                await rb.append({"n": 2})
            async with AsyncSDSavior(data, meta, 256 * 1024) as rb2:
                rows = await rb2.iter_records()
                assert [r[2]["n"] for r in rows] == [1, 2]

        asyncio.run(run())

    def test_async_iter_with_from_seq(self, ring_paths):
        data, meta = ring_paths

        async def run():
            async with AsyncSDSavior(data, meta, 256 * 1024) as rb:
                await rb.append({"n": 1})
                s2 = await rb.append({"n": 2})
                await rb.append({"n": 3})
                rows = await rb.iter_records(from_seq=s2)
                assert [r[2]["n"] for r in rows] == [2, 3]

        asyncio.run(run())
