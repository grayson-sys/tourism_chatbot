from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

from app.rag.ingest import DEFAULT_SEEDS, ingest_urls
from app.settings import get_settings


def _configure_logging(log_file: str) -> None:
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    for handler in list(root.handlers):
        root.removeHandler(handler)

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    root.addHandler(stream_handler)
    root.addHandler(file_handler)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run long-form ingest crawler.")
    parser.add_argument("--max-pages", type=int, default=None)
    parser.add_argument("--rate-limit", type=float, default=None)
    parser.add_argument("--log-file", default="ingest.log")
    parser.add_argument("--log-every", type=int, default=25)
    parser.add_argument("--per-host-cap", type=int, default=None)
    parser.add_argument("--commit-every", type=int, default=50)
    parser.add_argument("--smoke", action="store_true", help="Fetch only 5 pages and exit.")
    parser.add_argument("--seed", action="append", dest="seeds", default=None)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    settings = get_settings()

    log_file = args.log_file
    if not Path(log_file).is_absolute():
        log_file = str(settings.project_root / log_file)

    _configure_logging(log_file)

    max_pages = args.max_pages
    if args.smoke:
        max_pages = 5

    seeds = args.seeds or DEFAULT_SEEDS
    banner = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "max_pages": max_pages or settings.crawl_max_pages,
        "rate_limit": args.rate_limit or 1.5,
        "seeds": seeds,
        "log_file": log_file,
        "smoke": args.smoke,
    }
    print(f"STARTING INGEST {json.dumps(banner, ensure_ascii=True)}", flush=True)

    stats = ingest_urls(
        seeds,
        max_pages=max_pages,
        rate_limit_seconds=args.rate_limit,
        log_every=args.log_every,
        per_host_cap=args.per_host_cap,
        commit_every=args.commit_every,
        logger=logging.getLogger("ingest"),
    )
    print(f"INGEST DONE {json.dumps(stats, ensure_ascii=True)}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
