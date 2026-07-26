---
pretty_name: Polymarket Raw Chain Logs
language:
  - en
  - zh
tags:
  - polymarket
  - polygon
  - blockchain
  - event-logs
  - parquet
---

# Polymarket Raw Chain Logs

Raw Polygon EVM logs selected for reconstructing Polymarket positions and PnL.
The dataset preserves log address, all topics, data, block and transaction
coordinates without ABI decoding.

目标事件包括：

- Conditional Tokens lifecycle events;
- Conditional Tokens ERC-1155 `TransferSingle` and `TransferBatch`;
- Polymarket NegRisk market, resolution, split, merge, conversion and redemption events.

Files are immutable ZSTD Parquet shards partitioned by block group. Collection
manifests under `manifests/` record the exact block coverage and SHA-256 of every
artifact. Collection is currently in progress.

Source code: <https://github.com/wzsg/polymarket-raw-log-collector>
