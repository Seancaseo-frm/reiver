#!/usr/bin/env python3
"""Flow Gateway example client: call the website gateway (e.g. /api/gateway/v1/chat/completions)."""

import argparse
import os
import sys
import time

import dotenv

# Load .env so GATEWAY_URL, API_KEY, PROJECT_ID are set
dotenv.load_dotenv()

SCENARIOS = ("basic_chat", "streaming", "session", "cache", "prompt_config", "all")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run Flow Gateway example scenarios against the website gateway."
    )
    parser.add_argument(
        "scenario",
        choices=SCENARIOS,
        help="Scenario to run",
    )
    parser.add_argument(
        "--gateway-url",
        default=os.environ.get("GATEWAY_URL", "http://localhost:3003"),
        help="Base URL of the website (default: GATEWAY_URL or http://localhost:3003)",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("API_KEY", ""),
        help="Project API key (default: API_KEY from env)",
    )
    parser.add_argument(
        "--project-id",
        default=os.environ.get("PROJECT_ID", ""),
        help="Optional project ID",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run the scenario once and exit (default: run until Ctrl+C)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Seconds to wait between runs when looping (default: 1.0)",
    )
    args = parser.parse_args()
    api_key = (args.api_key or "").strip()
    if not api_key:
        print("error: API_KEY is required (set in .env or pass --api-key)", file=sys.stderr)
        sys.exit(1)
    project_id = (args.project_id or "").strip() or None
    mod = __import__(f"scenarios.{args.scenario}", fromlist=["run"])

    if args.once:
        mod.run(args.gateway_url, api_key, project_id)
        return

    run_num = 0
    try:
        while True:
            run_num += 1
            print(f"\n--- Run {run_num} ---", flush=True)
            mod.run(args.gateway_url, api_key, project_id)
            time.sleep(args.delay)
    except KeyboardInterrupt:
        print(f"\nStopped after {run_num} run(s).", flush=True)
        sys.exit(0)


if __name__ == "__main__":
    main()
