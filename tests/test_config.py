from pathlib import Path

import pytest

from polymarket_raw_log_collector.config import ConfigError, event_topic, load_config


def write_config(path: Path, *, start: int = 10) -> None:
    path.write_text(
        f"""
[collector]
chain_id = 137
start_block = {start}
block_window = 100
segment_blocks = 10000
confirmations = 256
workers = 4
request_timeout_seconds = 60
retries = 3
output_dir = "data"
state_db = "state/checkpoint.sqlite"

[rpc]
url_env = "POLYGON_RPC_URL"

[[filters]]
name = "ctf"
address = "0x1111111111111111111111111111111111111111"
valid_from_block = 10
event_signatures = ["TransferSingle(address,address,address,uint256,uint256)"]
""",
        encoding="utf-8",
    )


def test_load_config_resolves_paths_and_topics(tmp_path: Path) -> None:
    path = tmp_path / "config.toml"
    write_config(path)
    config = load_config(path)
    assert config.start_block == 10
    assert config.output_dir == tmp_path / "data"
    assert config.filters[0].topics == (
        event_topic("TransferSingle(address,address,address,uint256,uint256)"),
    )
    assert len(config.fingerprint) == 64


def test_start_before_all_filters_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "config.toml"
    write_config(path, start=9)
    with pytest.raises(ConfigError, match="precedes every filter"):
        load_config(path)
