"""Run basic_chat, streaming, session, cache. Optionally prompt_config if RUN_PROMPT_CONFIG=1."""

import os
from typing import Optional

from . import basic_chat, cache, prompt_config, session, streaming


def run(gateway_url: str, api_key: str, project_id: Optional[str] = None) -> None:
    print("=== basic_chat ===")
    basic_chat.run(gateway_url, api_key, project_id)
    print()
    print("=== streaming ===")
    streaming.run(gateway_url, api_key, project_id)
    print()
    print("=== session ===")
    session.run(gateway_url, api_key, project_id)
    print()
    print("=== cache ===")
    cache.run(gateway_url, api_key, project_id)
    if os.environ.get("RUN_PROMPT_CONFIG") == "1":
        print()
        print("=== prompt_config ===")
        prompt_config.run(gateway_url, api_key, project_id)
    print()
    print("all: done")
