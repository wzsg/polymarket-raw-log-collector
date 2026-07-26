from __future__ import annotations

import hashlib
import os
import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .rpc import RpcError, parse_quantity

_HASH_RE = re.compile(r"^0x[0-9a-f]{64}$")
_ADDRESS_RE = re.compile(r"^0x[0-9a-f]{40}$")

RAW_LOG_SCHEMA = pa.schema(
    [
        pa.field("chain_id", pa.uint64(), nullable=False),
        pa.field("contract_address", pa.string(), nullable=False),
        pa.field("block_number", pa.uint64(), nullable=False),
        pa.field("block_hash", pa.string(), nullable=False),
        pa.field("transaction_hash", pa.string(), nullable=False),
        pa.field("transaction_index", pa.uint32(), nullable=False),
        pa.field("log_index", pa.uint32(), nullable=False),
        pa.field("removed", pa.bool_(), nullable=False),
        pa.field("topics", pa.list_(pa.string()), nullable=False),
        pa.field("data", pa.string(), nullable=False),
        pa.field("scan_from_block", pa.uint64(), nullable=False),
        pa.field("scan_to_block", pa.uint64(), nullable=False),
    ]
)


@dataclass(frozen=True)
class Artifact:
    relative_path: str
    sha256: str
    size_bytes: int
    row_count: int
    from_block: int
    to_block: int


def _fixed_hex(value: Any, pattern: re.Pattern[str], field: str) -> str:
    result = str(value).lower()
    if not pattern.fullmatch(result):
        raise RpcError(f"{field} has invalid hexadecimal data")
    return result


def normalize_log(
    value: dict[str, Any],
    *,
    chain_id: int,
    scan_from_block: int,
    scan_to_block: int,
) -> dict[str, Any]:
    topics_value = value.get("topics")
    if not isinstance(topics_value, list) or not topics_value:
        raise RpcError("log.topics must be a non-empty array")
    topics = [_fixed_hex(item, _HASH_RE, "log.topic") for item in topics_value]
    data = str(value.get("data", "")).lower()
    if not data.startswith("0x"):
        raise RpcError("log.data must be 0x-prefixed")
    try:
        bytes.fromhex(data[2:])
    except ValueError as exc:
        raise RpcError("log.data contains invalid hexadecimal data") from exc
    block_number = parse_quantity(value.get("blockNumber"), "log.blockNumber")
    if not scan_from_block <= block_number <= scan_to_block:
        raise RpcError(
            f"log block {block_number} is outside scan range "
            f"{scan_from_block}..{scan_to_block}"
        )
    removed = value.get("removed", False)
    if not isinstance(removed, bool):
        raise RpcError("log.removed must be a boolean")
    return {
        "chain_id": chain_id,
        "contract_address": _fixed_hex(value.get("address"), _ADDRESS_RE, "log.address"),
        "block_number": block_number,
        "block_hash": _fixed_hex(value.get("blockHash"), _HASH_RE, "log.blockHash"),
        "transaction_hash": _fixed_hex(
            value.get("transactionHash"), _HASH_RE, "log.transactionHash"
        ),
        "transaction_index": parse_quantity(
            value.get("transactionIndex"), "log.transactionIndex"
        ),
        "log_index": parse_quantity(value.get("logIndex"), "log.logIndex"),
        "removed": removed,
        "topics": topics,
        "data": data,
        "scan_from_block": scan_from_block,
        "scan_to_block": scan_to_block,
    }


class RawLogWriter:
    def __init__(self, output_dir: Path, *, chain_id: int, segment_blocks: int) -> None:
        self.output_dir = output_dir.resolve()
        self.chain_id = chain_id
        self.segment_blocks = segment_blocks

    def write(
        self,
        logs_by_window: list[tuple[int, int, list[dict[str, Any]]]],
        *,
        from_block: int,
        to_block: int,
    ) -> Artifact | None:
        rows: list[dict[str, Any]] = []
        for window_from, window_to, logs in logs_by_window:
            rows.extend(
                normalize_log(
                    item,
                    chain_id=self.chain_id,
                    scan_from_block=window_from,
                    scan_to_block=window_to,
                )
                for item in logs
            )
        rows.sort(
            key=lambda row: (
                row["block_number"],
                row["transaction_index"],
                row["log_index"],
                row["transaction_hash"],
            )
        )
        unique: list[dict[str, Any]] = []
        seen: set[tuple[str, int]] = set()
        for row in rows:
            key = (row["transaction_hash"], row["log_index"])
            if key in seen:
                continue
            seen.add(key)
            unique.append(row)
        if not unique:
            return None

        group = from_block // self.segment_blocks
        relative_dir = Path("raw_chain_logs") / f"block_group={group:06d}"
        final_dir = self.output_dir / relative_dir
        final_dir.mkdir(parents=True, exist_ok=True)
        temporary = final_dir / f".part-{uuid.uuid4().hex}.tmp"
        table = pa.Table.from_pylist(unique, schema=RAW_LOG_SCHEMA)
        pq.write_table(
            table,
            temporary,
            compression="zstd",
            use_dictionary=True,
            write_statistics=True,
        )
        digest = hashlib.sha256(temporary.read_bytes()).hexdigest()
        filename = f"part-b{from_block:09d}-b{to_block:09d}-{digest[:12]}.parquet"
        final_path = final_dir / filename
        if final_path.exists():
            existing = hashlib.sha256(final_path.read_bytes()).hexdigest()
            if existing != digest:
                temporary.unlink(missing_ok=True)
                raise RuntimeError(f"existing artifact hash mismatch: {final_path}")
            temporary.unlink(missing_ok=True)
        else:
            os.replace(temporary, final_path)
        parquet = pq.ParquetFile(final_path)
        if parquet.metadata.num_rows != len(unique):
            raise RuntimeError(f"written row count mismatch: {final_path}")
        return Artifact(
            relative_path=(relative_dir / filename).as_posix(),
            sha256=digest,
            size_bytes=final_path.stat().st_size,
            row_count=len(unique),
            from_block=from_block,
            to_block=to_block,
        )
