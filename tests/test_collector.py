import threading
import time
from pathlib import Path

from polymarket_raw_log_collector.checkpoint import CheckpointStore
from polymarket_raw_log_collector.collector import run_collection
from polymarket_raw_log_collector.config import load_config
from polymarket_raw_log_collector.rpc import BlockHeader


def write_config(path: Path) -> None:
    path.write_text(
        """
[collector]
chain_id = 137
start_block = 100
block_window = 100
segment_blocks = 200
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


def rpc_log(block: int) -> dict[str, object]:
    return {
        "address": "0x" + "11" * 20,
        "blockNumber": hex(block),
        "blockHash": "0x" + f"{block + 1:064x}",
        "transactionHash": "0x" + f"{block + 2:064x}",
        "transactionIndex": "0x0",
        "logIndex": "0x0",
        "removed": False,
        "topics": ["0x" + "33" * 32],
        "data": "0x",
    }


class FakeRpc:
    def __init__(self) -> None:
        self.http_requests = 0
        self.max_active = 0
        self._active = 0
        self._lock = threading.Lock()

    def chain_id(self) -> int:
        return 137

    def safe_head(self, _confirmations: int) -> BlockHeader:
        return self.block_header(299)

    def block_header(self, block: int) -> BlockHeader:
        return BlockHeader(
            number=block,
            block_hash="0x" + f"{block:064x}",
            parent_hash="0x" + f"{max(0, block - 1):064x}",
            timestamp=block,
        )

    def get_logs(
        self,
        from_block: int,
        _to_block: int,
        *,
        addresses: list[str],
        topics: list[str],
    ) -> list[dict[str, object]]:
        assert addresses
        assert topics
        with self._lock:
            self.http_requests += 1
            self._active += 1
            self.max_active = max(self.max_active, self._active)
        time.sleep(0.03)
        with self._lock:
            self._active -= 1
        return [rpc_log(from_block)]


def test_windows_run_concurrently_but_checkpoint_commits_once(tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    write_config(config_path)
    config = load_config(config_path)
    rpc = FakeRpc()
    messages: list[str] = []

    summary = run_collection(
        config,
        "unused",
        to_block=299,
        workers=2,
        progress=messages.append,
        rpc=rpc,  # type: ignore[arg-type]
    )

    assert rpc.max_active == 2
    assert summary.segments == 1
    assert summary.raw_logs == 2
    with CheckpointStore(config.state_db) as store:
        state = store.load()
    assert state.next_block == 300
    assert len(messages) == 1
