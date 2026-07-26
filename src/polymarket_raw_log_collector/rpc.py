from __future__ import annotations

import itertools
import threading
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import httpx


class RpcError(RuntimeError):
    """Raised when a JSON-RPC request cannot be completed safely."""


class SplitRequired(RpcError):
    """Raised when a log query should be divided into smaller ranges."""


def parse_quantity(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, str) or not value.startswith("0x"):
        raise RpcError(f"{field} is not a hexadecimal quantity")
    try:
        result = int(value, 16)
    except ValueError as exc:
        raise RpcError(f"{field} is not a hexadecimal quantity") from exc
    if result < 0:
        raise RpcError(f"{field} must be non-negative")
    return result


@dataclass(frozen=True)
class BlockHeader:
    number: int
    block_hash: str
    parent_hash: str
    timestamp: int


class PolygonRpc:
    def __init__(
        self,
        endpoint: str,
        *,
        timeout_seconds: float,
        retries: int,
        workers: int,
        client: httpx.Client | None = None,
    ) -> None:
        self.endpoint = endpoint
        self.timeout_seconds = timeout_seconds
        self.retries = retries
        self._ids = itertools.count(1)
        self._counter_lock = threading.Lock()
        self._http_requests = 0
        self._owns_client = client is None
        self._client = client or httpx.Client(
            timeout=httpx.Timeout(timeout_seconds),
            limits=httpx.Limits(
                max_connections=max(2, workers + 1),
                max_keepalive_connections=max(2, workers + 1),
            ),
        )

    @property
    def http_requests(self) -> int:
        with self._counter_lock:
            return self._http_requests

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> PolygonRpc:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def _post(self, method: str, params: Sequence[Any]) -> Any:
        payload = {
            "jsonrpc": "2.0",
            "id": next(self._ids),
            "method": method,
            "params": list(params),
        }
        last_error: Exception | None = None
        for attempt in range(1, self.retries + 1):
            try:
                with self._counter_lock:
                    self._http_requests += 1
                response = self._client.post(self.endpoint, json=payload)
                if response.status_code == 413:
                    raise SplitRequired("RPC response is too large")
                if response.status_code == 429 or response.status_code >= 500:
                    raise RpcError(f"RPC returned HTTP {response.status_code}")
                response.raise_for_status()
                body = response.json()
                if not isinstance(body, Mapping):
                    raise RpcError("RPC response is not an object")
                if body.get("error") is not None:
                    error = body["error"]
                    message = (
                        str(error.get("message", ""))
                        if isinstance(error, Mapping)
                        else str(error)
                    )
                    code = error.get("code") if isinstance(error, Mapping) else None
                    lowered = message.lower()
                    if code in {-32005, -32062} or any(
                        hint in lowered
                        for hint in (
                            "block range",
                            "range is too large",
                            "response size",
                            "too many result",
                            "payload too large",
                        )
                    ):
                        raise SplitRequired(f"RPC rejected log range: {message}")
                    raise RpcError(f"JSON-RPC error {code}: {message}")
                if "result" not in body:
                    raise RpcError("RPC response has no result")
                return body["result"]
            except SplitRequired:
                raise
            except (httpx.HTTPError, ValueError, RpcError) as exc:
                last_error = exc
                if attempt >= self.retries:
                    break
                time.sleep(min(0.5 * (2 ** (attempt - 1)), 8.0))
        raise RpcError(f"{method} failed after {self.retries} attempts: {last_error}")

    def chain_id(self) -> int:
        return parse_quantity(self._post("eth_chainId", []), "chainId")

    def latest_block_number(self) -> int:
        return parse_quantity(self._post("eth_blockNumber", []), "blockNumber")

    def block_header(self, block: int | str) -> BlockHeader:
        tag = hex(block) if isinstance(block, int) else block
        value = self._post("eth_getBlockByNumber", [tag, False])
        if not isinstance(value, Mapping):
            raise RpcError(f"block {tag} was not found")
        number = parse_quantity(value.get("number"), "block.number")
        if isinstance(block, int) and number != block:
            raise RpcError(f"requested block {block}, received block {number}")
        block_hash = str(value.get("hash", "")).lower()
        parent_hash = str(value.get("parentHash", "")).lower()
        if len(block_hash) != 66 or len(parent_hash) != 66:
            raise RpcError("block header contains an invalid hash")
        return BlockHeader(
            number=number,
            block_hash=block_hash,
            parent_hash=parent_hash,
            timestamp=parse_quantity(value.get("timestamp"), "block.timestamp"),
        )

    def safe_head(self, confirmations: int) -> BlockHeader:
        latest = self.latest_block_number()
        return self.block_header(max(0, latest - confirmations))

    def _get_logs_once(
        self,
        from_block: int,
        to_block: int,
        *,
        addresses: Sequence[str],
        topics: Sequence[str],
    ) -> list[dict[str, Any]]:
        value = self._post(
            "eth_getLogs",
            [
                {
                    "fromBlock": hex(from_block),
                    "toBlock": hex(to_block),
                    "address": list(addresses),
                    "topics": [list(topics)],
                }
            ],
        )
        if not isinstance(value, list) or not all(isinstance(item, Mapping) for item in value):
            raise RpcError("eth_getLogs returned invalid data")
        return [dict(item) for item in value]

    def get_logs(
        self,
        from_block: int,
        to_block: int,
        *,
        addresses: Sequence[str],
        topics: Sequence[str],
    ) -> list[dict[str, Any]]:
        if not addresses or not topics:
            return []
        try:
            return self._get_logs_once(
                from_block, to_block, addresses=addresses, topics=topics
            )
        except SplitRequired:
            if from_block == to_block:
                raise
            middle = (from_block + to_block) // 2
            return [
                *self.get_logs(
                    from_block, middle, addresses=addresses, topics=topics
                ),
                *self.get_logs(
                    middle + 1, to_block, addresses=addresses, topics=topics
                ),
            ]
