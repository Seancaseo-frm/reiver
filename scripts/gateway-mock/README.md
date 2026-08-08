# Gateway mock (no-cost dev testing)

Minimal mock server for the Flow LLM gateway providers (OpenAI, Anthropic, Google). Use it to run the stack and the example app **without real API keys or paid AI requests**.

## Quick start

1. **Start the mock** (in one terminal):

   ```bash
   cd scripts/gateway-mock
   pip install -r requirements.txt
   python server.py
   ```

   The mock listens on:

   - **8090** — OpenAI (`/chat/completions`)
   - **8091** — Anthropic (`/messages`)
   - **8092** — Google (`/v1beta/models/...:generateContent`)

2. **Point Flow at the mock** and start the stack (in another terminal). Either:

   - Export the env vars, then run your usual dev command:

     ```bash
     export GATEWAY_OPENAI_BASE_URL=http://127.0.0.1:8090
     export GATEWAY_ANTHROPIC_BASE_URL=http://127.0.0.1:8091
     export GATEWAY_GOOGLE_BASE_URL=http://127.0.0.1:8092/v1beta
     make dev
     ```

   - Or copy `.env.mock.example` to the repo root as `.env.mock` and run:

     ```bash
     set -a && source .env.mock && set +a && make dev
     ```

3. In the project (Prompt Hub / Settings > Integrations), you can set **dummy** provider keys (e.g. `sk-test-openai`); the mock does not validate them.

## Env vars

| Env var | Value |
|--------|--------|
| `GATEWAY_OPENAI_BASE_URL` | `http://127.0.0.1:8090` |
| `GATEWAY_ANTHROPIC_BASE_URL` | `http://127.0.0.1:8091` |
| `GATEWAY_GOOGLE_BASE_URL` | `http://127.0.0.1:8092/v1beta` |

Flow reads these from core config (`GATEWAY_*_BASE_URL`) and uses them instead of the real provider URLs, so all gateway traffic goes to this mock.

## Health checks

- `GET http://127.0.0.1:8090/health` — OpenAI mock
- `GET http://127.0.0.1:8091/health` — Anthropic mock  
- `GET http://127.0.0.1:8092/health` — Google mock

Each returns `{"status": "ok", "provider": "..."}`.

## Response behaviour

- **Non-streaming**: All providers return a minimal valid JSON body with a fixed text: `"Hello from gateway mock (no real API call)."`
- **Streaming**: OpenAI returns minimal SSE; Anthropic and Google return minimal stream-shaped payloads so the gateway pipeline runs without error.

The mock does not validate API keys, token counts, or request shape beyond what is needed for Flow to parse the response.
