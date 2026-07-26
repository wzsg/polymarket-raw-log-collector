from pathlib import Path

import pytest

from polymarket_raw_log_collector.checkpoint import CheckpointError, CheckpointStore


def test_checkpoint_advances_contiguously(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite"
    with CheckpointStore(path) as store:
        state = store.initialize(
            chain_id=137,
            config_fingerprint="a" * 64,
            config_snapshot={"version": 1},
            start_block=100,
        )
        assert state.next_block == 100
        state = store.commit_segment(
            from_block=100,
            to_block=199,
            to_block_hash="0x" + "12" * 32,
            safe_head=500,
            raw_log_count=0,
            rpc_request_count=2,
            artifact=None,
        )
        assert state.next_block == 200
        assert state.last_scanned_block_hash == "0x" + "12" * 32


def test_checkpoint_rejects_config_change(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite"
    with CheckpointStore(path) as store:
        store.initialize(
            chain_id=137,
            config_fingerprint="a" * 64,
            config_snapshot={},
            start_block=100,
        )
        with pytest.raises(CheckpointError, match="configuration changed"):
            store.initialize(
                chain_id=137,
                config_fingerprint="b" * 64,
                config_snapshot={},
                start_block=100,
            )
