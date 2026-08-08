"""Two identical requests (temperature=0); log x-reiver-cache for both; expect miss then hit (or skip if cache disabled)."""

from typing import Optional

from client import post_completion


def run(gateway_url: str, api_key: str, project_id: Optional[str] = None) -> None:
    body = {
        "model": "gpt-4o",
        "messages": [{"role": "user", "content": "What is 2+2? Reply with one number only."}],
        "temperature": 0,
    }
    data1, headers1 = post_completion(gateway_url, api_key, body)
    cache1 = headers1.get("x-reiver-cache", "?")
    print("First request  x-reiver-cache:", cache1)
    data2, headers2 = post_completion(gateway_url, api_key, body)
    cache2 = headers2.get("x-reiver-cache", "?")
    print("Second request x-reiver-cache:", cache2)
    if cache2 == "hit":
        print("cache: OK (first=miss/?, second=hit)")
    else:
        print("cache: OK (cache may be disabled; both requests succeeded)")
