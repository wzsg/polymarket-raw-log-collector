from __future__ import annotations

import json
import os
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .checkpoint import CheckpointError, CheckpointStore
from .config import CollectorConfig


def export_manifest(
    config: CollectorConfig,
    *,
    worker: str,
    expected_to_block: int,
    output: Path,
) -> dict[str, Any]:
    worker = worker.strip()
    if not worker:
        raise ValueError("worker name is required")
    if expected_to_block < config.start_block:
        raise ValueError("expected_to_block precedes configured start block")

    with CheckpointStore(config.state_db) as store:
        state = store.load()
    if state.config_fingerprint != config.fingerprint:
        raise CheckpointError("checkpoint config fingerprint does not match active config")
    if state.last_scanned_block != expected_to_block:
        raise CheckpointError(
            f"collection is incomplete: last scanned block {state.last_scanned_block}, "
            f"expected {expected_to_block}"
        )
    if state.next_block != expected_to_block + 1:
        raise CheckpointError("checkpoint next_block is not contiguous")
    if state.last_scanned_block_hash is None:
        raise CheckpointError("completed checkpoint has no terminal block hash")

    db = sqlite3.connect(config.state_db)
    try:
        artifacts_rows = db.execute(
            """
            SELECT relative_path, sha256, size_bytes, row_count, from_block, to_block
            FROM artifacts
            ORDER BY from_block, to_block, relative_path
            """
        ).fetchall()
        segment_row = db.execute(
            """
            SELECT COUNT(*), COALESCE(SUM(raw_log_count), 0),
                   COALESCE(SUM(rpc_request_count), 0),
                   MIN(from_block), MAX(to_block)
            FROM scan_segments
            """
        ).fetchone()
    finally:
        db.close()

    artifacts = [
        {
            "relative_path": row[0],
            "sha256": row[1],
            "size_bytes": row[2],
            "row_count": row[3],
            "from_block": row[4],
            "to_block": row[5],
        }
        for row in artifacts_rows
    ]
    missing = [
        item["relative_path"]
        for item in artifacts
        if not (config.output_dir / item["relative_path"]).is_file()
    ]
    if missing:
        raise CheckpointError(f"manifest references missing artifacts: {missing[:3]}")

    segment_count, raw_log_count, rpc_request_count, min_block, max_block = segment_row
    if min_block != config.start_block or max_block != expected_to_block:
        raise CheckpointError(
            f"segment coverage is incomplete: {min_block}..{max_block}, "
            f"expected {config.start_block}..{expected_to_block}"
        )
    manifest = {
        "schema_version": 1,
        "worker": worker,
        "chain_id": config.chain_id,
        "config_fingerprint": config.fingerprint,
        "from_block": config.start_block,
        "to_block": expected_to_block,
        "to_block_hash": state.last_scanned_block_hash,
        "next_block": state.next_block,
        "segment_count": segment_count,
        "artifact_count": len(artifacts),
        "raw_log_count": raw_log_count,
        "rpc_request_count": rpc_request_count,
        "total_size_bytes": sum(item["size_bytes"] for item in artifacts),
        "generated_at": datetime.now(UTC).isoformat(),
        "artifacts": artifacts,
    }
    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    encoded = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode()
    temporary = output.with_name(f".{output.name}.tmp")
    temporary.write_bytes(encoded)
    os.replace(temporary, output)
    return manifest
