"""voitta-ask --config ask.yaml"""
from __future__ import annotations

import argparse
import logging

import uvicorn

from voitta_gateway.config import host_port

from .service import AskService, build_app, load


def main() -> None:
    ap = argparse.ArgumentParser(prog="voitta-ask")
    ap.add_argument("--config", "-c", default="ask.yaml")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()
    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    cfg = load(args.config)
    svc = AskService(cfg)                      # loads the models before accepting anything
    host, port = host_port(cfg.listen)
    uvicorn.run(build_app(svc), host=host, port=port, log_level="info", ws_max_size=2 ** 22)


if __name__ == "__main__":
    main()
