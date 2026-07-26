from __future__ import annotations

import hashlib
import json
import re
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from eth_hash.auto import keccak

_ADDRESS_RE = re.compile(r"^0x[0-9a-f]{40}$")
_TOPIC_RE = re.compile(r"^0x[0-9a-f]{64}$")


class ConfigError(ValueError):
    """Raised when collector configuration is unsafe or incomplete."""


def event_topic(signature: str) -> str:
    signature = signature.strip()
    if not signature or "(" not in signature or not signature.endswith(")"):
        raise ConfigError(f"invalid event signature: {signature!r}")
    try:
        encoded = signature.encode("ascii")
    except UnicodeEncodeError as exc:
        raise ConfigError(f"event signature must be ASCII: {signature!r}") from exc
    return "0x" + keccak(encoded).hex()


def _address(value: Any, field: str) -> str:
    result = str(value).lower()
    if not _ADDRESS_RE.fullmatch(result):
        raise ConfigError(f"{field} must be a 20-byte 0x-prefixed address")
    return result


def _topic(value: Any, field: str) -> str:
    result = str(value).lower()
    if not _TOPIC_RE.fullmatch(result):
        raise ConfigError(f"{field} must be a 32-byte 0x-prefixed topic")
    return result


def _positive_int(value: Any, field: str, *, allow_zero: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{field} must be an integer")
    minimum = 0 if allow_zero else 1
    if value < minimum:
        raise ConfigError(f"{field} must be >= {minimum}")
    return value


@dataclass(frozen=True)
class FilterSpec:
    name: str
    address: str
    valid_from_block: int
    valid_to_block: int | None
    topics: tuple[str, ...]
    event_signatures: tuple[str, ...]

    def active_in(self, from_block: int, to_block: int) -> bool:
        end = self.valid_to_block if self.valid_to_block is not None else 2**63 - 1
        return self.valid_from_block <= to_block and from_block <= end

    def accepts(self, address: str, topic0: str, block_number: int) -> bool:
        return (
            address == self.address
            and topic0 in self.topics
            and block_number >= self.valid_from_block
            and (self.valid_to_block is None or block_number <= self.valid_to_block)
        )


@dataclass(frozen=True)
class CollectorConfig:
    path: Path
    chain_id: int
    start_block: int
    block_window: int
    segment_blocks: int
    confirmations: int
    workers: int
    request_timeout_seconds: float
    retries: int
    output_dir: Path
    state_db: Path
    rpc_url_env: str
    filters: tuple[FilterSpec, ...]
    fingerprint: str
    snapshot: dict[str, Any]

    def active_filters(self, from_block: int, to_block: int) -> tuple[FilterSpec, ...]:
        return tuple(item for item in self.filters if item.active_in(from_block, to_block))

    def accepts_log(self, address: str, topic0: str, block_number: int) -> bool:
        return any(item.accepts(address, topic0, block_number) for item in self.filters)


def _parse_filter(raw: Any, index: int) -> FilterSpec:
    if not isinstance(raw, dict):
        raise ConfigError(f"filters[{index}] must be a table")
    name = str(raw.get("name", "")).strip()
    if not name:
        raise ConfigError(f"filters[{index}].name is required")
    address = _address(raw.get("address"), f"filters[{index}].address")
    valid_from = _positive_int(
        raw.get("valid_from_block"), f"filters[{index}].valid_from_block", allow_zero=True
    )
    raw_valid_to = raw.get("valid_to_block")
    valid_to = (
        None
        if raw_valid_to is None
        else _positive_int(raw_valid_to, f"filters[{index}].valid_to_block", allow_zero=True)
    )
    if valid_to is not None and valid_to < valid_from:
        raise ConfigError(f"filters[{index}] has an invalid validity interval")

    signatures_value = raw.get("event_signatures", [])
    topics_value = raw.get("topics", [])
    if not isinstance(signatures_value, list) or not all(
        isinstance(item, str) for item in signatures_value
    ):
        raise ConfigError(f"filters[{index}].event_signatures must be an array of strings")
    if not isinstance(topics_value, list) or not all(
        isinstance(item, str) for item in topics_value
    ):
        raise ConfigError(f"filters[{index}].topics must be an array of strings")
    signatures = tuple(dict.fromkeys(item.strip() for item in signatures_value))
    topics = tuple(
        dict.fromkeys(
            [
                *(event_topic(signature) for signature in signatures),
                *(_topic(item, f"filters[{index}].topics") for item in topics_value),
            ]
        )
    )
    if not topics:
        raise ConfigError(f"filters[{index}] must define event_signatures or topics")
    return FilterSpec(
        name=name,
        address=address,
        valid_from_block=valid_from,
        valid_to_block=valid_to,
        topics=topics,
        event_signatures=signatures,
    )


def load_config(path: Path | str) -> CollectorConfig:
    config_path = Path(path).resolve()
    try:
        with config_path.open("rb") as handle:
            raw = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ConfigError(f"cannot read config {config_path}: {exc}") from exc

    collector = raw.get("collector")
    rpc = raw.get("rpc")
    filters_value = raw.get("filters")
    if not isinstance(collector, dict):
        raise ConfigError("[collector] table is required")
    if not isinstance(rpc, dict):
        raise ConfigError("[rpc] table is required")
    if not isinstance(filters_value, list) or not filters_value:
        raise ConfigError("at least one [[filters]] table is required")

    chain_id = _positive_int(collector.get("chain_id"), "collector.chain_id")
    start_block = _positive_int(
        collector.get("start_block"), "collector.start_block", allow_zero=True
    )
    block_window = _positive_int(collector.get("block_window"), "collector.block_window")
    segment_blocks = _positive_int(
        collector.get("segment_blocks"), "collector.segment_blocks"
    )
    if segment_blocks < block_window:
        raise ConfigError("collector.segment_blocks must be >= collector.block_window")
    confirmations = _positive_int(
        collector.get("confirmations"), "collector.confirmations", allow_zero=True
    )
    workers = _positive_int(collector.get("workers"), "collector.workers")
    retries = _positive_int(collector.get("retries"), "collector.retries")
    timeout = collector.get("request_timeout_seconds")
    if isinstance(timeout, bool) or not isinstance(timeout, int | float) or timeout <= 0:
        raise ConfigError("collector.request_timeout_seconds must be positive")

    base = config_path.parent
    output_dir = (base / str(collector.get("output_dir", "data"))).resolve()
    state_db = (base / str(collector.get("state_db", "state/collector.sqlite"))).resolve()
    rpc_url_env = str(rpc.get("url_env", "")).strip()
    if not rpc_url_env:
        raise ConfigError("rpc.url_env is required")

    filters = tuple(_parse_filter(item, index) for index, item in enumerate(filters_value))
    if len({item.name for item in filters}) != len(filters):
        raise ConfigError("filter names must be unique")
    if not any(item.active_in(start_block, start_block) for item in filters):
        earliest = min(item.valid_from_block for item in filters)
        if start_block < earliest:
            raise ConfigError(
                f"collector.start_block {start_block} precedes every filter; use {earliest}"
            )

    snapshot = {
        "schema_version": 1,
        "chain_id": chain_id,
        "start_block": start_block,
        "block_window": block_window,
        "segment_blocks": segment_blocks,
        "confirmations": confirmations,
        "filters": [
            {
                "name": item.name,
                "address": item.address,
                "valid_from_block": item.valid_from_block,
                "valid_to_block": item.valid_to_block,
                "topics": list(item.topics),
                "event_signatures": list(item.event_signatures),
            }
            for item in filters
        ],
    }
    canonical = json.dumps(snapshot, sort_keys=True, separators=(",", ":")).encode()
    fingerprint = hashlib.sha256(canonical).hexdigest()
    return CollectorConfig(
        path=config_path,
        chain_id=chain_id,
        start_block=start_block,
        block_window=block_window,
        segment_blocks=segment_blocks,
        confirmations=confirmations,
        workers=workers,
        request_timeout_seconds=float(timeout),
        retries=retries,
        output_dir=output_dir,
        state_db=state_db,
        rpc_url_env=rpc_url_env,
        filters=filters,
        fingerprint=fingerprint,
        snapshot=snapshot,
    )
