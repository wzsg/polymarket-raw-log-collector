from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .storage import Artifact


class CheckpointError(RuntimeError):
    """Raised when a checkpoint cannot be advanced safely."""


def _now() -> str:
    return datetime.now(UTC).isoformat()


@dataclass(frozen=True)
class CheckpointState:
    chain_id: int
    config_fingerprint: str
    next_block: int
    last_scanned_block: int
    last_scanned_block_hash: str | None
    safe_head: int


class CheckpointStore:
    def __init__(self, path: Path) -> None:
        self.path = path.resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(self.path)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._create_schema()

    def close(self) -> None:
        self._db.close()

    def __enter__(self) -> CheckpointStore:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def _create_schema(self) -> None:
        self._db.executescript(
            """
            CREATE TABLE IF NOT EXISTS collector_state (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                schema_version INTEGER NOT NULL,
                chain_id INTEGER NOT NULL,
                config_fingerprint TEXT NOT NULL,
                config_json TEXT NOT NULL,
                next_block INTEGER NOT NULL,
                last_scanned_block INTEGER NOT NULL,
                last_scanned_block_hash TEXT,
                safe_head INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS scan_segments (
                from_block INTEGER NOT NULL,
                to_block INTEGER NOT NULL,
                to_block_hash TEXT NOT NULL,
                safe_head INTEGER NOT NULL,
                raw_log_count INTEGER NOT NULL,
                rpc_request_count INTEGER NOT NULL,
                artifact_path TEXT,
                completed_at TEXT NOT NULL,
                PRIMARY KEY (from_block, to_block)
            );
            CREATE TABLE IF NOT EXISTS artifacts (
                relative_path TEXT PRIMARY KEY,
                sha256 TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                row_count INTEGER NOT NULL,
                from_block INTEGER NOT NULL,
                to_block INTEGER NOT NULL,
                created_at TEXT NOT NULL
            );
            """
        )
        self._db.commit()

    def initialize(
        self,
        *,
        chain_id: int,
        config_fingerprint: str,
        config_snapshot: dict[str, Any],
        start_block: int,
    ) -> CheckpointState:
        row = self._db.execute(
            """
            SELECT chain_id, config_fingerprint, next_block, last_scanned_block,
                   last_scanned_block_hash, safe_head
            FROM collector_state WHERE singleton = 1
            """
        ).fetchone()
        if row is None:
            self._db.execute(
                """
                INSERT INTO collector_state (
                    singleton, schema_version, chain_id, config_fingerprint,
                    config_json, next_block, last_scanned_block,
                    last_scanned_block_hash, safe_head, updated_at
                ) VALUES (1, 1, ?, ?, ?, ?, ?, NULL, ?, ?)
                """,
                (
                    chain_id,
                    config_fingerprint,
                    json.dumps(config_snapshot, sort_keys=True),
                    start_block,
                    start_block - 1,
                    start_block - 1,
                    _now(),
                ),
            )
            self._db.commit()
            return self.load()
        state = self.load()
        if state.chain_id != chain_id:
            raise CheckpointError(
                f"checkpoint chain ID {state.chain_id} differs from config {chain_id}"
            )
        if state.config_fingerprint != config_fingerprint:
            raise CheckpointError(
                "collector configuration changed; use a new state/output directory "
                "or intentionally rebuild from the configured start block"
            )
        return state

    def load(self) -> CheckpointState:
        row = self._db.execute(
            """
            SELECT chain_id, config_fingerprint, next_block, last_scanned_block,
                   last_scanned_block_hash, safe_head
            FROM collector_state WHERE singleton = 1
            """
        ).fetchone()
        if row is None:
            raise CheckpointError("checkpoint is not initialized")
        return CheckpointState(*row)

    def commit_segment(
        self,
        *,
        from_block: int,
        to_block: int,
        to_block_hash: str,
        safe_head: int,
        raw_log_count: int,
        rpc_request_count: int,
        artifact: Artifact | None,
    ) -> CheckpointState:
        state = self.load()
        if from_block != state.next_block:
            raise CheckpointError(
                f"segment starts at {from_block}, checkpoint expects {state.next_block}"
            )
        if to_block < from_block or to_block > safe_head:
            raise CheckpointError("segment has an invalid or unsafe block range")
        artifact_path = artifact.relative_path if artifact is not None else None
        completed_at = _now()
        try:
            self._db.execute("BEGIN IMMEDIATE")
            if artifact is not None:
                self._db.execute(
                    """
                    INSERT OR IGNORE INTO artifacts (
                        relative_path, sha256, size_bytes, row_count,
                        from_block, to_block, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        artifact.relative_path,
                        artifact.sha256,
                        artifact.size_bytes,
                        artifact.row_count,
                        artifact.from_block,
                        artifact.to_block,
                        completed_at,
                    ),
                )
            self._db.execute(
                """
                INSERT INTO scan_segments (
                    from_block, to_block, to_block_hash, safe_head,
                    raw_log_count, rpc_request_count, artifact_path, completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    from_block,
                    to_block,
                    to_block_hash,
                    safe_head,
                    raw_log_count,
                    rpc_request_count,
                    artifact_path,
                    completed_at,
                ),
            )
            updated = self._db.execute(
                """
                UPDATE collector_state
                SET next_block = ?, last_scanned_block = ?,
                    last_scanned_block_hash = ?, safe_head = ?, updated_at = ?
                WHERE singleton = 1 AND next_block = ?
                """,
                (
                    to_block + 1,
                    to_block,
                    to_block_hash,
                    safe_head,
                    completed_at,
                    from_block,
                ),
            )
            if updated.rowcount != 1:
                raise CheckpointError("checkpoint changed concurrently")
            self._db.commit()
        except Exception:
            self._db.rollback()
            raise
        return self.load()
