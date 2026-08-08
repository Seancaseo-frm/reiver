"""POST with stream=True; parse SSE, accumulate content; log provider/model-used."""

from typing import Optional

from client import post_completion_stream


def run(gateway_url: str, api_key: str, project_id: Optional[str] = None) -> None:
    body = {
        "model": "gpt-4o",
        "messages": [{"role": "user", "content": "Count from 1 to 3."}],
    }
    parts = []
    out_headers = {}
    for content, headers in post_completion_stream(gateway_url, api_key, body):
        if content:
            parts.append(content)
        if headers:
            out_headers = headers
    full = "".join(parts)
    assert full.strip(), "expected non-empty streamed content"
    print("Streamed content:", full.strip())
    print("Headers:", out_headers)
    print("streaming: OK (200, non-empty content)")
