# Flow Gateway Example Client

Example app that calls the Flow LLM gateway **via the website** (not Flow directly). Use it to verify chat completions, streaming, sessions, prompt config, caching, and response headers.

## Prerequisites

- **Website and Flow running**  
  From repo root: `make dev` (or `make dev-quick`). This starts the website on `http://localhost:3003` and Flow with the **gateway mock**, so no real OpenAI/Anthropic API keys are required and no API cost is incurred.

- **Project and API key**  
  Run `make seed` from repo root to create a dev user, project, and project API key. Copy the printed **Project API Key** into `.env` as `API_KEY`.  
  With **gateway mock** (`make dev`), the gateway uses default provider keys so the project does not need its own; the client app only needs the project API key.

## Setup

```bash
cd examples/flow_gateway_client
pip install -r requirements.txt
cp .env.example .env
# Edit .env and set API_KEY to the key from `make seed`
```

## Configuration

| Variable     | Default                 | Description                    |
| ------------ | ----------------------- | ------------------------------ |
| `GATEWAY_URL`| `http://localhost:3003` | Base URL of the website       |
| `API_KEY`    | (required)              | Project API key from settings  |
| `PROJECT_ID` | (optional)              | Project ID for logging        |

## Run scenarios

```bash
python main.py basic_chat      # Single non-streaming completion
python main.py streaming       # Stream=True, parse SSE
python main.py session         # 3 requests with same session-id
python main.py cache           # Two identical requests (expect cache hit on second)
python main.py prompt_config   # prompt_config + prompt_variables (explicit mode)
python main.py all             # basic_chat, streaming, session, cache; optionally prompt_config if RUN_PROMPT_CONFIG=1
```

Example with explicit URL and key:

```bash
python main.py basic_chat --gateway-url http://localhost:3003 --api-key "YOUR_PROJECT_API_KEY"
```

**Sustained load:** Use `--delay` to throttle. A very low delay (e.g. `0.0001`) can overload the local stack (Postgres, Redis, Kafka), cause timeouts and slow queries, and the website UI may become unusable. For sustained runs use at least `--delay 0.1` (10 req/s) or higher.

## Session budget (optional)

The **session** scenario sends multiple requests with the same `x-reiver-session-id`. These count toward the project’s session budget. To test 429 when over budget, set a low budget in the UI and run `python main.py session`.

## Prompt config scenario

**prompt_config** requires the project to be in **explicit** prompt mode and a prompt config (e.g. `customer-support`) that accepts `prompt_variables`. Enable it in the **all** run with:

```bash
RUN_PROMPT_CONFIG=1 python main.py all
```
