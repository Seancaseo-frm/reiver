#!/usr/bin/env python3
"""
Minimal mock server for Flow LLM gateway providers (OpenAI, Anthropic, Google).
Serves on three ports so dev/staging can run without real API keys or cost.

Start: python server.py
Then set env and run Flow (e.g. make dev):
  GATEWAY_OPENAI_BASE_URL=http://127.0.0.1:8090
  GATEWAY_ANTHROPIC_BASE_URL=http://127.0.0.1:8091
  GATEWAY_GOOGLE_BASE_URL=http://127.0.0.1:8092/v1beta
"""

import json
import threading
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
import uvicorn

# Fixed ports matching .env.mock.example
OPENAI_PORT = 8090
ANTHROPIC_PORT = 8091
GOOGLE_PORT = 8092

MOCK_CONTENT = "Hello from gateway mock (no real API call)."


def openai_completion(model: str = "gpt-4o") -> dict:
    return {
        "id": "chatcmpl-mock123",
        "object": "chat.completion",
        "created": 1699000000,
        "model": model,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": MOCK_CONTENT},
                "finish_reason": "stop",
            }
        ],
        "usage": {"prompt_tokens": 10, "completion_tokens": 10, "total_tokens": 20},
    }


def openai_sse_chunks(model: str = "gpt-4o") -> list[str]:
    chunks = [
        json.dumps(
            {
                "id": "chatcmpl-stream",
                "object": "chat.completion.chunk",
                "created": 1699000000,
                "model": model,
                "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}],
            }
        ),
        json.dumps(
            {
                "id": "chatcmpl-stream",
                "object": "chat.completion.chunk",
                "created": 1699000000,
                "model": model,
                "choices": [{"index": 0, "delta": {"content": MOCK_CONTENT}, "finish_reason": None}],
            }
        ),
        json.dumps(
            {
                "id": "chatcmpl-stream",
                "object": "chat.completion.chunk",
                "created": 1699000000,
                "model": model,
                "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
            }
        ),
    ]
    return [f"data: {c}\n\n" for c in chunks] + ["data: [DONE]\n\n"]


def anthropic_message() -> dict:
    return {
        "id": "msg_mock123",
        "type": "message",
        "role": "assistant",
        "content": [{"type": "text", "text": MOCK_CONTENT}],
        "model": "claude-3-5-sonnet-20241022",
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 10, "output_tokens": 10},
    }


def google_generate_content() -> dict:
    return {
        "candidates": [
            {
                "content": {
                    "role": "model",
                    "parts": [{"text": MOCK_CONTENT}],
                },
                "finishReason": "STOP",
                "index": 0,
            }
        ],
        "usageMetadata": {
            "promptTokenCount": 10,
            "candidatesTokenCount": 10,
            "totalTokenCount": 20,
        },
    }


# ---------- OpenAI app (port 8090) ----------
openai_app = FastAPI(title="Gateway mock (OpenAI)")


@openai_app.get("/health")
def openai_health():
    return {"status": "ok", "provider": "openai"}


@openai_app.post("/chat/completions")
async def openai_chat_completions(request: Request):
    body = await request.json()
    model = body.get("model", "gpt-4o")
    if body.get("stream"):
        def stream():
            for chunk in openai_sse_chunks(model):
                yield chunk
        return StreamingResponse(
            stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
        )
    return JSONResponse(content=openai_completion(model))


# ---------- Anthropic app (port 8091) ----------
anthropic_app = FastAPI(title="Gateway mock (Anthropic)")


@anthropic_app.get("/health")
def anthropic_health():
    return {"status": "ok", "provider": "anthropic"}


@anthropic_app.post("/messages")
async def anthropic_messages(request: Request):
    body = await request.json()
    if body.get("stream"):
        # Minimal SSE for Anthropic (Flow may parse event types)
        def stream():
            yield f"event: message_start\ndata: {json.dumps({'type': 'message_start'})}\n\n"
            yield f"event: content_block_delta\ndata: {json.dumps({'type': 'content_block_delta', 'delta': {'type': 'text_delta', 'text': MOCK_CONTENT}})}\n\n"
            yield "event: message_stop\ndata: {\"type\": \"message_stop\"}\n\n"
        return StreamingResponse(
            stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
        )
    return JSONResponse(content=anthropic_message())


# ---------- Google app (port 8092) ----------
google_app = FastAPI(title="Gateway mock (Google)")


@google_app.get("/health")
def google_health():
    return {"status": "ok", "provider": "google"}


@google_app.post("/v1beta/models/{path:path}")
async def google_generate(path: str, request: Request):
    body = await request.json()
    if path.endswith(":streamGenerateContent"):
        def stream():
            yield json.dumps({"candidates": [{"content": {"parts": [{"text": MOCK_CONTENT}]}}]}) + "\n"
        return StreamingResponse(
            stream(),
            media_type="application/json",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
        )
    if not path.endswith(":generateContent"):
        return JSONResponse(content={"error": "not found"}, status_code=404)
    return JSONResponse(content=google_generate_content())


def run_uvicorn(app: FastAPI, port: int, name: str):
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="info")
    server = uvicorn.Server(config)
    print(f"[gateway-mock] {name} listening on http://127.0.0.1:{port}")
    server.run()


if __name__ == "__main__":
    print("Gateway mock: start Flow/website with GATEWAY_OPENAI_BASE_URL, GATEWAY_ANTHROPIC_BASE_URL, GATEWAY_GOOGLE_BASE_URL from .env.mock.example")
    t1 = threading.Thread(target=run_uvicorn, args=(openai_app, OPENAI_PORT, "OpenAI"), daemon=True)
    t2 = threading.Thread(target=run_uvicorn, args=(anthropic_app, ANTHROPIC_PORT, "Anthropic"), daemon=True)
    t3 = threading.Thread(target=run_uvicorn, args=(google_app, GOOGLE_PORT, "Google"), daemon=True)
    t1.start()
    t2.start()
    t3.start()
    t1.join()
    t2.join()
    t3.join()
