"""voitta-gateway --config gateway.yaml"""
from __future__ import annotations

import argparse
import asyncio
import logging

from . import config


def main() -> None:
    try:
        from .app import run
    except ImportError as e:
        raise SystemExit(f"voitta-gateway: the service needs the 'service' extra "
                         f"(pip install 'voitta-gateway[service]'): {e}")
    ap = argparse.ArgumentParser(prog="voitta-gateway")
    ap.add_argument("--config", "-c", default="gateway.yaml")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()
    logging.basicConfig(level=args.log_level.upper(),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    cfg = config.load(args.config)
    try:
        asyncio.run(run(cfg))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
