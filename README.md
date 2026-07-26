# Polymarket 原始链上日志采集器

从 Polygon 按配置的合约地址、topic0 和生效区块采集原始 EVM 日志。采集器不做
ABI 解码，原始日志写入 ZSTD Parquet；SQLite checkpoint 仅在连续区块段完整
落盘后推进。

## 特性

- 默认每个 `eth_getLogs` 请求扫描 100 个区块；
- 多线程并发请求，线程数可配置或通过 CLI 覆盖；
- 一个请求合并当前区块范围内所有有效合约地址和 topic0；
- Provider 拒绝或响应过大时自动二分当前 100 区块窗口；
- 原始日志按较大的区块段合并写入，避免产生海量小文件；
- SQLite checkpoint、区块哈希续跑校验和配置指纹保护；
- Parquet 临时写入、校验后原子重命名；
- 同时支持可读的事件签名和直接配置 topic0。

## 初始化

```powershell
cd D:\yuce\polymarket-raw-log-collector
Copy-Item config.example.toml config.toml
uv sync
uv run polymarket-raw-log-collector validate-config --config config.toml
```

RPC 地址只从环境变量读取，不写入 TOML：

```powershell
$env:POLYGON_RPC_URL = "你的 Polygon RPC 地址"
uv run polymarket-raw-log-collector collect --config config.toml
```

也可以读取现有机密文件：

```powershell
uv run polymarket-raw-log-collector collect `
  --config config.toml `
  --env-file D:\yuce\polymarket-analytics\.env.secrets
```

限制采集终点、临时覆盖线程数或只运行一个存储分片：

```powershell
uv run polymarket-raw-log-collector collect `
  --config config.toml `
  --to-block 50515402 `
  --workers 8 `
  --max-segments 1
```

`--to-block` 不会越过 `latest - confirmations` 得到的安全链头。

查看 checkpoint：

```powershell
uv run polymarket-raw-log-collector status --config config.toml
```

完整范围采集结束后，可验证 checkpoint、分片覆盖和本地 artifact，并生成上传清单：

```powershell
uv run polymarket-raw-log-collector manifest `
  --config config.toml `
  --worker db `
  --expected-to-block 81053170 `
  --output data/manifests/db.json
```

`deploy/run-worker.sh` 用于服务器的一次性采集与自动上传：只有采集命令成功退出且
manifest 验证通过后，才会调用宿主机 HF CLI 上传 `raw_chain_logs/` 和
`manifests/`。同一脚本可由 `deploy/polymarket-raw-collector.service` 托管，
失败时从 SQLite checkpoint 重试。

## 配置

`config.example.toml` 已包含：

- Polygon Conditional Tokens 合约；
- CTF 市场创建、结算、拆分、合并和赎回；
- ERC-1155 `TransferSingle`、`TransferBatch`；
- NegRisk Adapter V1 市场、问题、结果、拆分、合并、转换和赎回。

默认总扫描起点为 CTF Exchange V1 部署区块 `33,605,403`。底层 Conditional
Tokens 合约虽然更早部署，但 Polymarket OrderFilled 历史从区块 `35,896,869`
开始；使用 Exchange 部署区块作为起点可以覆盖首批市场准备和仓位事件，同时避免
扫描大量与目标交易历史无关的早期区块。

每个过滤器可设置：

```toml
[[filters]]
name = "example"
address = "0x..."
valid_from_block = 123
valid_to_block = 456 # 可省略
event_signatures = ["EventName(uint256)"]
topics = ["0x..."]  # 可与 event_signatures 同时使用
```

修改地址、主题、起始区块、窗口或分片参数会改变配置指纹。已有 checkpoint 不会
在新指纹下静默续跑，因为新增过滤条件后从旧 checkpoint 继续会漏掉历史事件。

## 数据组织

```text
data/
├── metadata/
│   └── collection_config.json
└── raw_chain_logs/
    └── block_group=000402/
        └── part-b004023686-b004033685-<sha12>.parquet

state/
└── collector.sqlite
```

RPC 扫描窗口与 Parquet 分片不同。默认 100 个区块一次 RPC，但累计 10,000 个
区块才提交一个存储分片。程序异常时 checkpoint 不会越过尚未完整落盘的分片，
重新运行最多重复请求当前分片。

原始表字段：

```text
chain_id
contract_address
block_number
block_hash
transaction_hash
transaction_index
log_index
removed
topics[]
data
scan_from_block
scan_to_block
```

区块时间戳不属于 `eth_getLogs` 返回值，本项目的第一阶段不补充时间戳。后续解码
任务可按唯一命中区块批量请求区块头，再生成按 UTC 月份分区的业务表。

## 验证

```powershell
uv run pytest
uv run ruff check .
```
