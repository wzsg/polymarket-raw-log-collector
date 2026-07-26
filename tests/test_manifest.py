import json
from pathlib import Path

import pytest

from polymarket_raw_log_collector.checkpoint import CheckpointError, CheckpointStore
from polymarket_raw_log_collector.config import load_config
from polymarket_raw_log_collector.manifest import export_manifest
from polymarket_raw_log_collector.storage import Artifact


def write_config(path: Path) -> None:
    path.write_text(
        """
[collector]
chain_id = 137
start_block = 100
block_window = 100
segment_blocks = 100
confirmations = 10
workers = 2
request_timeout_seconds = 60
retries = 3
output_dir = "data"
state_db = "state/checkpoint.sqlite"

[rpc]
url_env = "POLYGON_RPC_URL"

[[filters]]
name = "test"
address = "0x1111111111111111111111111111111111111111"
valid_from_block = 100
topics = ["0x3333333333333333333333333333333333333333333333333333333333333333"]
""",
        encoding="utf-8",
    )


def completed_config(tmp_path: Path):
    config_path = tmp_path / "config.toml"
    write_config(config_path)
    config = load_config(config_path)
    artifact_path = config.output_dir / "raw_chain_logs/block_group=000001/test.parquet"
    artifact_path.parent.mkdir(parents=True)
    artifact_path.write_bytes(b"parquet")
    artifact = Artifact(
        relative_path="raw_chain_logs/block_group=000001/test.parquet",
        sha256="a" * 64,
        size_bytes=7,
        row_count=3,
        from_block=100,
        to_block=199,
    )
    with CheckpointStore(config.state_db) as store:
        store.initialize(
            chain_id=137,
            config_fingerprint=config.fingerprint,
            config_snapshot=config.snapshot,
            start_block=100,
        )
        store.commit_segment(
            from_block=100,
            to_block=199,
            to_block_hash="0x" + "12" * 32,
            safe_head=199,
            raw_log_count=3,
            rpc_request_count=2,
            artifact=artifact,
        )
    return config


def test_export_manifest_verifies_and_lists_artifacts(tmp_path: Path) -> None:
    config = completed_config(tmp_path)
    output = config.output_dir / "manifests/db.json"
    value = export_manifest(
        config,
        worker="db",
        expected_to_block=199,
        output=output,
    )
    assert value["artifact_count"] == 1
    assert value["raw_log_count"] == 3
    assert json.loads(output.read_text(encoding="utf-8"))["worker"] == "db"


def test_export_manifest_rejects_incomplete_range(tmp_path: Path) -> None:
    config = completed_config(tmp_path)
    with pytest.raises(CheckpointError, match="incomplete"):
        export_manifest(
            config,
            worker="db",
            expected_to_block=299,
            output=config.output_dir / "manifests/db.json",
        )
