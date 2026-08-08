# Privacy Policy

**Effective date:** April 9, 2026

Reiver ("we", "us", "our") operates the Reiver platform at
**reiver.ai** and related services. This policy describes what
information we collect, how we use it, and your choices regarding that
information.

## 1. Information We Collect

### Account information

When you create an account we collect your name, email address, and
authentication credentials from the identity provider you use to sign in
(Google, GitHub, or Microsoft).

### Telemetry data

When you send observability data to Reiver — traces, logs, metrics,
exceptions, and profiles — we store it on your behalf. This data is
scoped to your project and is not shared with other customers.

### LLM request data

If you use the Prompt Hub or LLM Gateway, we process the prompts and
completions you route through the gateway so we can provide cost tracking,
caching, and analytics. We do not use your prompts or completions to train
models.

### Third-party integrations

When you connect integrations such as Slack, GitHub, PagerDuty, or cloud
providers, we store the OAuth tokens and configuration required to operate
the integration. Tokens are encrypted at rest using AES-256-GCM.

### Usage and diagnostic data

We collect standard web analytics (page views, feature usage) and server
logs to maintain and improve the platform. This data is aggregated and
does not include the content of your telemetry or LLM requests.

## 2. How We Use Your Information

We use the information we collect to:

- Operate, maintain, and improve the Reiver platform.
- Authenticate your identity and enforce access controls.
- Deliver alerts, notifications, and integration messages you configure.
- Calculate billing and usage metrics.
- Diagnose and fix technical issues.

We do **not** sell your personal information or telemetry data to third
parties.

## 3. Data Sharing

We share information only in the following circumstances:

- **Infrastructure providers.** We use cloud infrastructure (compute,
  storage, networking) to host the platform. Your data is processed on
  these providers' systems under our instructions.
- **LLM providers.** When you route requests through the LLM Gateway, the
  prompts and completions are sent to the upstream model provider you
  selected (e.g. OpenAI, Anthropic, Google, AWS Bedrock).
- **Integration targets.** When you connect an integration (Slack, PagerDuty,
  etc.), we send the messages and payloads you configure to that service.
- **Legal requirements.** We may disclose information if required by law,
  regulation, or legal process.

## 4. Data Retention

- **Telemetry data** is retained according to your project's retention
  settings and plan tier.
- **Account data** is retained for as long as your account is active.
- **Integration tokens** are deleted when you remove the integration or
  delete your account.

You can request deletion of your account and all associated data by
contacting us at **privacy@reiver.ai**.

## 5. Security

We protect your data with:

- Encryption in transit (TLS) and at rest (AES-256-GCM for secrets).
- Role-based access controls and project-scoped data isolation.
- Regular infrastructure patching and monitoring.

## 6. Cookies

We use a session cookie to keep you signed in. We do not use third-party
tracking cookies.

## 7. Your Rights

Depending on your jurisdiction you may have the right to access, correct,
or delete your personal data, or to object to its processing. To exercise
these rights, contact us at **privacy@reiver.ai**.

## 8. Changes to This Policy

We may update this policy from time to time. When we make material
changes we will update the effective date at the top of this page and, if
appropriate, notify you via email or in-product notice.

## 9. Contact

If you have questions about this policy, contact us at:

- **Email:** privacy@reiver.ai
