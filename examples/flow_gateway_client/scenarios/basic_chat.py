"""Single non-streaming completion; log response body and headers (provider, model-used, fallback-used)."""

from typing import Optional

from client import post_completion


def run(gateway_url: str, api_key: str, project_id: Optional[str] = None) -> None:
    body = {
        "model": "gpt-4o",
        "messages": [{"role": "user", "content": "Say hello in one short sentence."}],
    }
    data, headers = post_completion(gateway_url, api_key, body)
    assert data.get("choices"), "expected choices in response"
    content = (data["choices"][0].get("message") or {}).get("content") or ""
    assert content.strip(), "expected non-empty content"
    print("Response:", content.strip())
    print("Headers:", headers)
    print("basic_chat: OK (200, non-empty content)")
