# Session and Identity Contract

A session is the smallest meaningful business episode the customer wants to evaluate and improve. It may be one support case, booking attempt, research task, or other outcome-bearing unit. It is not automatically a browser visit, login, chat window, or arbitrary timeout.

## Agree the contract first

The customer or delegated agent must define:

| Decision | Question to answer |
|---|---|
| Session unit | What single business episode should Reiver evaluate? |
| Start | Which accepted application event begins that episode? |
| Successful end | What proves the intended outcome happened? |
| Other endings | Which events mean failure or abandonment? |
| Inactivity fallback | When is an episode considered abandoned if no end event arrives? |
| Stable user | Which pseudonymous ID remains stable for the same person? |
| Anonymous users | Is an anonymous ID retained, rotated, or deliberately omitted? |
| Tenant scope | How are identical user IDs prevented from correlating across tenants? |
| Privacy boundary | Which prompts, outputs, logs, attributes, and identifiers may Reiver receive? |

Do not use an email address, name, provider credential, SDK key, agent token, or other secret as a user or session ID. Prefer an application-generated pseudonymous value scoped so two tenants cannot collide.

## Lifecycle

The first accepted Flow request carrying `x-reiver-session-id` starts recorded activity for that ID. Reuse that ID only for requests belonging to the same agreed business episode.

When the episode reaches a success, failure, or abandonment ending, the application should explicitly call:

```text
POST https://reiver.ai/api/gateway/v1/sessions/{session_id}/end
Authorization: Bearer <SDK key>
```

The current endpoint returns `202 Accepted` and schedules evaluation after a short ingestion buffer. Reiver also discovers sessions after 30 minutes without a request; that inactivity timeout is fallback protection, not the application's primary end signal.

For a second business episode, create a new session ID. Retain the same stable pseudonymous user ID when it is still the same person within the agreed tenant and privacy boundary.

## Flow headers

Send the identifiers on every request in the episode:

```python
response = client.chat.completions.create(
    model=model_name,
    messages=messages,
    user=pseudonymous_user_id,
    extra_headers={
        "x-reiver-session-id": session_id,
        "x-reiver-user-id": pseudonymous_user_id,
    },
)
```

The OpenAI-compatible `user` field is recorded as the request's user context. `x-reiver-user-id` is used for stable user-based rollout selection. Send the same agreed pseudonymous value in both when you need both recording and sticky rollout behavior, rather than creating unrelated identities in different layers.

## Watch correlation

When Watch is part of the selected track, place the same values on application telemetry:

| Meaning | OpenTelemetry attribute |
|---|---|
| Session | `gen_ai.session_id` |
| Stable pseudonymous user | `gen_ai.user.id` |

Current Reiver session telemetry views recognise `gen_ai.session_id` and the legacy `llm.session_id` spelling on spans and logs. Use `gen_ai.session_id` for new application correlation. Flow's generated GenAI telemetry also uses newer dotted semantic-convention attributes internally; do not substitute those for the session-view attribute unless the current view is updated to consume them.

An attribute does not create a telemetry pipeline. Follow [Watch](/watch/) to initialize and verify traces, structured logs, and metrics independently.

## Definition of done

1. The nine contract decisions above are written down without sensitive content.
2. One real episode uses one session ID across its Flow requests.
3. The application sends an explicit end request and receives `202`.
4. If Watch is selected, its trace and structured log carry the agreed correlation values.
5. A second episode uses a new session ID and retains the same pseudonymous user ID.
6. No credential or disallowed customer content appears in identifiers, telemetry, or the evidence report.
