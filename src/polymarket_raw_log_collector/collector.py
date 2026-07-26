from __future__ import annotations

import json
import os
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any

from .checkpoint import CheckpointError, CheckpointStore
from .config import CollectorConfig
from .rpc import PolygonRpc, RpcError, parse_quantity
from .storage import RawLogWriter

Progress = Callable[[str], None]


@dataclass(frozen=True)
class RunSummary:
    from_block: int
    to_block: int
    segments: int
    raw_logs: int
    rpc_requests: int


def _write_metadata(config: CollectorConfig) -> None:
    directory = config.output_dir / "metadata"
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / "collection_config.json"
    payload = {
        **config.snapshot,
        "config_fingerprint": config.fingerprint,
    }
    encoded = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode()
    if target.exists():
        existing = target.read_bytes()
        if existing != encoded:
            raise RuntimeError(
                f"metadata snapshot differs from active configuration: {target}"
            )
        return
    temporary = target.with_suffix(".json.tmp")
    temporary.write_bytes(encoded)
    os.replace(temporary, target)


def _window_filter(config: CollectorConfig, start: int, end: int) -> tuple[list[str], list[str]]:
    active = config.active_filters(start, end)
    addresses = list(dict.fromkeys(item.address for item in active))
    topics = list(dict.fromkeys(topic for item in active for topic in item.topics))
    return addresses, topics


def _fetch_window(
    rpc: PolygonRpc,
    config: CollectorConfig,
    start: int,
    end: int,
) -> tuple[int, int, list[dict[str, Any]]]:
    addresses, topics = _window_filter(config, start, end)
    logs = rpc.get_logs(start, end, addresses=addresses, topics=topics)
    accepted: list[dict[str, Any]] = []
    for log in logs:
        address = str(log.get("address", "")).lower()
        topics_value = log.get("topics")
        if not isinstance(topics_value, list) or not topics_value:
            raise RpcError("RPC returned a log without topics")
        topic0 = str(topics_value[0]).lower()
        block = parse_quantity(log.get("blockNumber"), "log.blockNumber")
        if config.accepts_log(address, topic0, block):
            accepted.append(log)
    return start, end, accepted


def run_collection(
    config: CollectorConfig,
    rpc_url: str,
    *,
    to_block: int | None = None,
    workers: int | None = None,
    max_segments: int | None = None,
    progress: Progress = print,
    rpc: PolygonRpc | None = None,
) -> RunSummary:
    worker_count = workers if workers is not None else config.workers
    if worker_count <= 0:
        raise ValueError("workers must be positive")
    if to_block is not None and to_block < 0:
        raise ValueError("to_block must be non-negative")
    if max_segments is not None and max_segments <= 0:
        raise ValueError("max_segments must be positive")

    _write_metadata(config)
    owns_rpc = rpc is None
    client = rpc or PolygonRpc(
        rpc_url,
        timeout_seconds=config.request_timeout_seconds,
        retries=config.retries,
        workers=worker_count,
    )
    try:
        chain_id = client.chain_id()
        if chain_id != config.chain_id:
            raise RpcError(f"RPC chain ID {chain_id} differs from config {config.chain_id}")
        safe_head = client.safe_head(config.confirmations)
        target = safe_head.number if to_block is None else min(to_block, safe_head.number)
        writer = RawLogWriter(
            config.output_dir,
            chain_id=config.chain_id,
            segment_blocks=config.segment_blocks,
        )
        with CheckpointStore(config.state_db) as checkpoint:
            state = checkpoint.initialize(
                chain_id=config.chain_id,
                config_fingerprint=config.fingerprint,
                config_snapshot=config.snapshot,
                start_block=config.start_block,
            )
            if state.last_scanned_block_hash is not None:
                observed = client.block_header(state.last_scanned_block)
                if observed.block_hash != state.last_scanned_block_hash:
                    raise CheckpointError(
                        "checkpoint block hash changed; the stored chain segment may have reorged"
                    )
            initial = state.next_block
            if initial > target:
                progress(
                    f"无需采集：checkpoint={initial:,}，安全链头={safe_head.number:,}"
                )
                return RunSummary(initial, initial - 1, 0, 0, 0)

            segments = 0
            raw_logs = 0
            requests_before_run = client.http_requests
            next_block = initial
            while next_block <= target:
                if max_segments is not None and segments >= max_segments:
                    break
                segment_end = min(
                    next_block + config.segment_blocks - 1,
                    target,
                )
                windows = [
                    (start, min(start + config.block_window - 1, segment_end))
                    for start in range(next_block, segment_end + 1, config.block_window)
                ]
                requests_before_segment = client.http_requests
                completed: dict[int, tuple[int, int, list[dict[str, Any]]]] = {}
                with ThreadPoolExecutor(max_workers=worker_count) as executor:
                    futures = {
                        executor.submit(_fetch_window, client, config, start, end): start
                        for start, end in windows
                    }
                    for future in as_completed(futures):
                        result = future.result()
                        completed[result[0]] = result
                ordered = [completed[start] for start, _end in windows]
                segment_logs = sum(len(item[2]) for item in ordered)
                header = client.block_header(segment_end)
                artifact = writer.write(
                    ordered,
                    from_block=next_block,
                    to_block=segment_end,
                )
                segment_requests = client.http_requests - requests_before_segment
                checkpoint.commit_segment(
                    from_block=next_block,
                    to_block=segment_end,
                    to_block_hash=header.block_hash,
                    safe_head=safe_head.number,
                    raw_log_count=segment_logs,
                    rpc_request_count=segment_requests,
                    artifact=artifact,
                )
                segments += 1
                raw_logs += segment_logs
                progress(
                    f"已提交 {next_block:,}..{segment_end:,}："
                    f"{segment_logs:,} 条日志，{segment_requests:,} 次 RPC"
                )
                next_block = segment_end + 1

            return RunSummary(
                from_block=initial,
                to_block=next_block - 1,
                segments=segments,
                raw_logs=raw_logs,
                rpc_requests=client.http_requests - requests_before_run,
            )
    finally:
        if owns_rpc:
            client.close()
