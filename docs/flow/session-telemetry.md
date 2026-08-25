# Session and Identity Contract

Before adding headers or OpenTelemetry attributes, decide what one **session** and one **user** mean in the application. This is a product decision, not only an instrumentation detail.

A Reiver session should be the smallest meaningful unit that the owner wants to evaluate, label, budget, replay, and improve. It should be long enough to contain an outcome, but narrow enough that the outcome is unambiguous.

## Decide the contract before writing code

Confirm these seven decisions with the application owner. An MCP-connected coding agent should infer them from Reiver's Agent Soul and the application first, then ask only about material gaps or conflicts.

| Decision | What to define |
|---|---|
| Session unit | The business episode Reiver should evaluate |
| Start | The application event that creates or selects its ID |
| Successful end | The event that means the episode achieved its purpose |
| Other ends | Abandonment, cancellation, handoff, failure, topic switch, or another terminal state |
| Idle fallback | What to do if the application never emits a terminal event |
| Stable user ID | The pseudonymous application identifier reused across sessions |
| Anonymous/tenant policy | How anonymous users are represented and how IDs avoid cross-tenant collisions |

Use this reusable block in Reiver's existing **Agents → Agent Soul** settings:

```text
Session and Identity Contract
- Session unit:
- Starts when:
- Ends successfully when:
- Also ends when:
- Idle fallback:
- Stable user ID source:
- Anonymous-user policy:
- Tenant scoping:
```

Store the confirmed contract in Agent Soul, normally in `custom_instructions` with the relevant business purpose in `project_description` and implementation facts in `tech_context`. Through MCP, read it from `gateway_settings.agent_soul`; when authorised to write, merge it with the existing Agent Soul through `gateway update_settings`. Do not erase useful existing context. Because Agent Soul belongs to the Reiver project, a later Claude, Codex, or other MCP session can reuse the decision instead of asking the owner again.

## Common session boundaries

| Application | Usually one session | Starts | Ends |
|---|---|---|---|
| Maths tutor | One problem, exercise, or coherent learning episode | Learner opens or asks the first question about the problem | Solved, abandoned, topic changed, or learner finishes |
| Sales agent | One lead conversation | First contact or resumed conversation | Qualified, disqualified, handed off, or abandoned |
| Support agent | One issue or ticket | Issue is opened | Resolved, escalated, closed, or abandoned |
| Internal copilot | One user task | Task begins | Completed, cancelled, or failed |
| Document processor | One processing job | Job is accepted | Succeeded, failed, or cancelled |
| Multi-agent workflow | One business case | Case is opened | Goal reached, case closed, or terminal failure |
| One-off LLM job | One request or job | Request accepted | Response or terminal error returned |

These are defaults, not universal rules. A long-lived account, browser login, or learner identity is usually **not** a session. A new session for every LLM turn is also usually wrong unless each turn is genuinely the unit the business evaluates.

## How Reiver starts and ends sessions

Reiver does not require a separate session-start endpoint. The first accepted Flow request carrying a new `x-reiver-session-id` becomes the first recorded activity for that session.

The application is responsible for creating or selecting the ID at its real business start event. Common delivery patterns are:

- reuse the application's existing conversation, ticket, task, or job ID;
- have the backend create an opaque ID when the episode begins;
- use a frontend conversation ID and pass it to the backend with every related request.

When the business episode reaches a terminal state, explicitly end it:

```bash
curl --request POST \
  "https://reiver.ai/api/gateway/v1/sessions/conversation-123/end" \
  --header "Authorization: Bearer $REIVER_FLOW_API_KEY"
```

The endpoint is idempotent. It returns `202` and schedules evaluation after an approximately 30-second ingestion buffer; `202` confirms that scheduling, not that evaluation has completed. If the scheduler dependency is unavailable, the endpoint returns `503 service_unavailable`; retry the call because scheduling was not confirmed. Reiver also discovers sessions after 30 minutes without a request. Treat that idle evaluator as protection for crashes, disconnects, and abandoned sessions—not as the application's primary definition of success or completion.

Restart recovery: the explicit-end path holds a short per-session reservation before its 30-second delay. If Flow restarts in that window, the immediate enqueue is lost, but the reservation expires well before the 30-minute idle threshold so idle discovery can enqueue the session normally. Always verify that an ended session becomes queryable before treating the integration as accepted.

## User identity rules

Use a stable pseudonymous internal ID, not an email address, name, phone number, or raw external identifier. Reuse it across that user's sessions so Reiver can support per-user routing and analytics now, and longitudinal user understanding later.

- Namespace or scope the ID by tenant when two tenants could issue the same local identifier.
- Define a deliberate anonymous-user policy. An installation- or browser-scoped pseudonymous ID may be appropriate; a single shared value such as `anonymous` destroys useful correlation.
- Do not use the user ID as the session ID. One user can have many sessions.
- Do not silently change how identities are merged. If an anonymous user later signs in, preserve the application's documented linking policy rather than inventing one during instrumentation.

Agent Soul describes what the application and business are. Session labels describe what happened in one episode. The stable user ID identifies who participated across episodes. Keeping those roles separate prevents labels, prompts, and future user profiles from being built on the wrong unit.

## Identifier transport

Flow groups LLM requests by stable identifiers. Watch stores application traces, logs, and metrics with OpenTelemetry attributes. Use the exact same values at both layers.

| Purpose | Flow request | Application telemetry |
|---|---|---|
| Conversation/session | `x-reiver-session-id` header | `gen_ai.conversation.id` |
| Reiver compatibility | same header | `gen_ai.session.id` |
| User-sticky routing | `x-reiver-user-id` header | `gen_ai.user.id` |
| Per-user gateway analytics | OpenAI-compatible `user` body field | `gen_ai.user.id` |

`gen_ai.conversation.id` is the current OpenTelemetry GenAI conversation attribute. `gen_ai.user.id` is a Reiver correlation attribute, not a current OTel semantic-convention field. Reiver's LLM processor also accepts the deprecated `gen_ai.session.id`. Emit both session values during the compatibility period. Older customer telemetry may contain `gen_ai.session_id` or `llm.session_id`, but new integrations should not introduce them.

## Python

```python
from opentelemetry import trace

session_id = "conversation-123"
user_id = "tenant-a:user-456"
tracer = trace.get_tracer(__name__)

with tracer.start_as_current_span("math_tutor.turn") as span:
    span.set_attribute("gen_ai.conversation.id", session_id)
    span.set_attribute("gen_ai.session.id", session_id)  # Reiver compatibility
    span.set_attribute("gen_ai.user.id", user_id)

    response = client.chat.completions.create(
        model="claude-sonnet-5",
        user=user_id,
        messages=messages,
        extra_headers={
            "x-reiver-session-id": session_id,
            "x-reiver-user-id": user_id,
        },
    )
```

Use a logging bridge and attach the same attributes to structured log records. Emit diagnostic logs inside the active span so their OTLP records also carry the trace and span IDs.

## Node.js

```javascript
const tracer = trace.getTracer("math-tutor");

await tracer.startActiveSpan("math_tutor.turn", async (span) => {
  span.setAttribute("gen_ai.conversation.id", sessionId);
  span.setAttribute("gen_ai.session.id", sessionId);
  span.setAttribute("gen_ai.user.id", userId);

  try {
    await client.chat.completions.create(
      {
        model: "claude-sonnet-5",
        user: userId,
        messages,
      },
      {
        headers: {
          "x-reiver-session-id": sessionId,
          "x-reiver-user-id": userId,
        },
      },
    );
  } finally {
    span.end();
  }
});
```

## Agent verification

An onboarding agent should prove the contract, not merely add fields:

1. State the confirmed session unit, start/end decisions, fallback, stable user source, and anonymous/tenant policy.
2. Run one successful episode and, where practical, one abandonment or failure episode.
3. Find the Flow session and one gateway request for the expected session/user.
4. Find the application trace under the expected `service.name`.
5. Find a structured log using `gen_ai.conversation.id` or its trace ID.
6. Confirm the exact identifier values match across Flow and Watch.
7. Confirm the explicit end call returned `202` and the session became queryable.
8. Confirm a second session for the same test user keeps the user ID but receives a new session ID.

With MCP, use `list` for sessions and traces, `get` for the session and its requests, and `search` for LLM requests and logs. Use attribute-key discovery before filtering. A Flow session and Watch telemetry are separate stored signals; matching identifiers are the correlation contract.

## Privacy

Prompt/output capture and application logs may contain sensitive content. Configure capture, redaction, access, and retention for the application's requirements. The Session and Identity Contract belongs in Agent Soul, but credentials, personal data, and other secrets do not.
