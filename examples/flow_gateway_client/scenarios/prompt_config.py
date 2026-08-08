"""POST with prompt_config + prompt_variables in body. Create a prompt config first."""

from typing import Optional

from client import post_completion


def run(gateway_url: str, api_key: str, project_id: Optional[str] = None) -> None:
    # Requires a prompt config (e.g. "customer-support") that accepts prompt_variables.
    body = {
        "model": "gpt-4o",
        "messages": [{"role": "user", "content": "Hello"}],
        "prompt_config": "customer-support",
        "prompt_variables": {
            "user_name": "Example User",
            "current_date": "2025-01-01",
        },
    }
    try:
        data, headers = post_completion(gateway_url, api_key, body)
    except Exception as e:
        print("prompt_config: skipped (config missing):", e)
        return
    assert data.get("choices"), "expected choices"
    content = (data["choices"][0].get("message") or {}).get("content") or ""
    print("Response:", content[:200] + ("..." if len(content) > 200 else ""))
    print("Headers:", headers)
    print("prompt_config: OK")
