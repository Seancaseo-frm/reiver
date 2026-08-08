"""Send 2–3 requests with same x-reiver-session-id (and optional x-reiver-user-id). Verify 200 each time."""

import uuid
from typing import Optional

from client import post_completion


def run(gateway_url: str, api_key: str, project_id: Optional[str] = None) -> None:
    session_id = str(uuid.uuid4())
    user_id = "example-user-1"
    extra_headers = {
        "x-reiver-session-id": session_id,
        "x-reiver-user-id": user_id,
    }
    for i in range(3):
        body = {
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": f"Request {i + 1}: say OK."}],
        }
        data, headers = post_completion(gateway_url, api_key, body, extra_headers=extra_headers)
        assert data.get("choices"), f"request {i + 1}: expected choices"
        content = (data["choices"][0].get("message") or {}).get("content") or ""
        assert content.strip(), f"request {i + 1}: expected non-empty content"
        print(f"  Request {i + 1}: 200, content length={len(content)}")
    print("session: OK (3 requests with same session-id; check LLM Sessions in UI)")
