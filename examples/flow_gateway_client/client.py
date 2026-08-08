"""
Thin HTTP client for the Flow gateway via the website proxy.

POST to GATEWAY_URL/api/gateway/v1/chat/completions with Authorization: Bearer <API_KEY>.
Supports non-streaming (returns JSON + headers) and streaming (SSE, yields content + headers).
"""

from __future__ import annotations

import json
import re
from typing import Any, Iterator

import httpx

# Headers we care about from the gateway response
HEADERS_OF_INTEREST = (
    "x-reiver-provider",
    "x-reiver-model-used",
    "x-reiver-cache",
    "x-reiver-fallback-used",
)


def _normalized_headers(response: httpx.Response) -> dict[str, str]:
    out = {}
    for name in HEADERS_OF_INTEREST:
        val = response.headers.get(name)
        if val is not None:
            out[name] = val
    return out


def post_completion(
    base_url: str,
    api_key: str,
    body: dict[str, Any],
    extra_headers: dict[str, str] | None = None,
) -> tuple[dict[str, Any], dict[str, str]]:
    """
    POST a chat completion (non-streaming). Returns (response_json, headers_of_interest).
    """
    url = f"{base_url.rstrip('/')}/api/gateway/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    if extra_headers:
        headers.update(extra_headers)
    with httpx.Client(timeout=120.0) as client:
        resp = client.post(url, json=body, headers=headers)
        if not resp.is_success:
            try:
                err_body = resp.text
                print("Gateway error response:", err_body or "(empty)", flush=True)
            except Exception:
                print("Gateway error response: (could not read body)", flush=True)
        resp.raise_for_status()
        data = resp.json()
        return data, _normalized_headers(resp)


def post_completion_stream(
    base_url: str,
    api_key: str,
    body: dict[str, Any],
    extra_headers: dict[str, str] | None = None,
) -> Iterator[tuple[str, dict[str, str]]]:
    """
    POST a chat completion with stream=True. Yields (content_delta, headers).
    Headers are only non-empty on the first yield (from the response). Content deltas
    concatenate to the full assistant message.
    """
    url = f"{base_url.rstrip('/')}/api/gateway/v1/chat/completions"
    req_headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    if extra_headers:
        req_headers.update(extra_headers)
    body = {**body, "stream": True}
    with httpx.Client(timeout=120.0) as client:
        with client.stream("POST", url, json=body, headers=req_headers) as resp:
            if not resp.is_success:
                try:
                    err_body = resp.read().decode("utf-8", errors="replace")
                    if err_body:
                        print("Gateway error response:", err_body, flush=True)
                except Exception:
                    pass
            resp.raise_for_status()
            response_headers = _normalized_headers(resp)
            first = True
            for line in resp.iter_lines():
                if not line or not line.startswith("data: "):
                    continue
                payload = line[6:].strip()
                if payload == "[DONE]":
                    return
                try:
                    chunk = json.loads(payload)
                except json.JSONDecodeError:
                    continue
                choices = chunk.get("choices") or []
                if not choices:
                    continue
                delta = choices[0].get("delta") or {}
                content = delta.get("content") or ""
                h = response_headers if first else {}
                first = False
                yield content, h
