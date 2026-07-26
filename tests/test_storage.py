from pathlib import Path

import pyarrow.parquet as pq

from polymarket_raw_log_collector.storage import RawLogWriter


def rpc_log(block: int, log_index: int = 0) -> dict[str, object]:
    return {
        "address": "0x" + "11" * 20,
        "blockNumber": hex(block),
        "blockHash": "0x" + "22" * 32,
        "transactionHash": "0x" + f"{block:064x}",
        "transactionIndex": "0x1",
        "logIndex": hex(log_index),
        "removed": False,
        "topics": ["0x" + "33" * 32],
        "data": "0x1234",
    }


def test_writer_sorts_deduplicates_and_writes_parquet(tmp_path: Path) -> None:
    writer = RawLogWriter(tmp_path, chain_id=137, segment_blocks=10_000)
    artifact = writer.write(
        [
            (100, 199, [rpc_log(102), rpc_log(101), rpc_log(102)]),
        ],
        from_block=100,
        to_block=199,
    )
    assert artifact is not None
    assert artifact.row_count == 2
    table = pq.read_table(tmp_path / artifact.relative_path)
    assert table.column("block_number").to_pylist() == [101, 102]


def test_writer_skips_empty_segments(tmp_path: Path) -> None:
    writer = RawLogWriter(tmp_path, chain_id=137, segment_blocks=10_000)
    assert writer.write([], from_block=100, to_block=199) is None
