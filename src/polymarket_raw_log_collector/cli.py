from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from .checkpoint import CheckpointStore
from .collector import run_collection
from .config import ConfigError, load_config
from .manifest import export_manifest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="polymarket-raw-log-collector",
        description="并发采集 Polygon 原始 EVM 日志并写入 Parquet",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    collect = subparsers.add_parser("collect", help="开始或继续采集")
    collect.add_argument("--config", type=Path, default=Path("config.toml"))
    collect.add_argument("--env-file", type=Path)
    collect.add_argument("--to-block", type=int)
    collect.add_argument("--workers", type=int)
    collect.add_argument("--max-segments", type=int)

    validate = subparsers.add_parser("validate-config", help="只校验配置")
    validate.add_argument("--config", type=Path, default=Path("config.toml"))

    status = subparsers.add_parser("status", help="查看本地 checkpoint")
    status.add_argument("--config", type=Path, default=Path("config.toml"))

    manifest = subparsers.add_parser("manifest", help="验证完成范围并导出上传清单")
    manifest.add_argument("--config", type=Path, default=Path("config.toml"))
    manifest.add_argument("--worker", required=True)
    manifest.add_argument("--expected-to-block", type=int, required=True)
    manifest.add_argument("--output", type=Path, required=True)
    return parser


def _load_env(path: Path | None) -> None:
    if path is not None:
        if not path.is_file():
            raise FileNotFoundError(f"env file does not exist: {path}")
        load_dotenv(path, override=False)


def main(argv: list[str] | None = None) -> None:
    args = _parser().parse_args(argv)
    try:
        config = load_config(args.config)
        if args.command == "validate-config":
            topic_count = len({topic for item in config.filters for topic in item.topics})
            print(
                f"配置有效：{len(config.filters)} 个过滤器，"
                f"{topic_count} 个唯一 topic0，指纹 {config.fingerprint[:12]}"
            )
            return
        if args.command == "status":
            if not config.state_db.exists():
                print(f"checkpoint 尚未创建：{config.state_db}")
                return
            with CheckpointStore(config.state_db) as store:
                state = store.load()
            print(
                f"chain_id={state.chain_id} next_block={state.next_block} "
                f"last_scanned_block={state.last_scanned_block} safe_head={state.safe_head}"
            )
            return
        if args.command == "manifest":
            value = export_manifest(
                config,
                worker=args.worker,
                expected_to_block=args.expected_to_block,
                output=args.output,
            )
            print(
                f"manifest 已生成：worker={value['worker']}，"
                f"范围={value['from_block']:,}..{value['to_block']:,}，"
                f"文件={value['artifact_count']:,}，日志={value['raw_log_count']:,}"
            )
            return

        _load_env(args.env_file)
        rpc_url = os.environ.get(config.rpc_url_env, "").strip()
        if not rpc_url:
            raise RuntimeError(
                f"环境变量 {config.rpc_url_env} 未设置；"
                "请设置环境变量或通过 --env-file 指定机密文件"
            )
        summary = run_collection(
            config,
            rpc_url,
            to_block=args.to_block,
            workers=args.workers,
            max_segments=args.max_segments,
        )
        print(
            f"完成：区块 {summary.from_block:,}..{summary.to_block:,}，"
            f"{summary.segments:,} 个分片，{summary.raw_logs:,} 条日志，"
            f"{summary.rpc_requests:,} 次 RPC"
        )
    except (ConfigError, RuntimeError, OSError, ValueError) as exc:
        print(f"错误：{exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
