from __future__ import annotations

import argparse
from pathlib import Path

import uvicorn

from .dispatcher.app import make_app


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Durable Campaign Dispatcher")
    parser.add_argument("--fixtures", type=Path, default=root / "assignment-b-workflow" / "fixtures" / "campaigns.json")
    parser.add_argument("--database", type=Path, default=root / "dispatcher.sqlite3")
    parser.add_argument("--output", type=Path, default=root / "final_state.csv")
    parser.add_argument("--provider-url", default="http://127.0.0.1:8099")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8200)
    args = parser.parse_args()
    callback_url = f"http://{args.host}:{args.port}/webhooks/telephony"
    app = make_app(args.fixtures, args.database, args.provider_url, callback_url, args.output)
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
