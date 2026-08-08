# Using Flow with OpenClaw

Flow is an LLM gateway that sits between your OpenClaw agents and AI providers like OpenAI, Anthropic, and Google. Every request your agents make passes through Flow, which adds cost controls, safety guardrails, and a dashboard where you can see exactly what your agents are doing.

## Why use Flow?

- **Stop runaway costs** -- set a spending limit per agent task so a stuck loop can't drain your account
- **Keep private data private** -- Flow automatically strips personal information (emails, phone numbers, credit cards) from prompts before they reach any AI provider
- **Change prompts without editing config files** -- use a web dashboard to tweak what your agents say, test different versions, and roll out changes gradually
- **See what your agents do** -- a full log of every request with cost, speed, which model was used, and quality scores

## Setup

### Option A: Install the plugin (recommended)

```bash
openclaw plugins install openclaw-flow
```

Then add your API key to `openclaw.json`:

```json5
{
  plugins: {
    "flow-gateway": {
      apiKey: "flow_your_project_key"
    }
  }
}
```

Set your agent to use Flow:

```json5
{
  agent: {
    model: { primary: "flow/auto" }
  }
}
```

Done. `flow/auto` picks the best available model for each request automatically.

### Option B: Manual configuration

If you prefer not to install the plugin, add Flow as a custom provider directly:

```json5
{
  models: {
    mode: "merge",
    providers: {
      flow: {
        baseUrl: "https://reiver.ai/api/gateway/v1",
        apiKey: "flow_your_project_key",
        api: "openai-completions",
        models: [
          { id: "auto", name: "Auto (best available)", reasoning: false, input: ["text"], contextWindow: 128000, maxTokens: 16384 },
          { id: "gpt-4o", name: "GPT-4o", reasoning: false, input: ["text", "image"], contextWindow: 128000, maxTokens: 16384 },
          { id: "claude-sonnet-4-5", name: "Claude Sonnet 4.5", reasoning: true, input: ["text", "image"], contextWindow: 200000, maxTokens: 8192 },
          { id: "gemini-2.5-pro", name: "Gemini 2.5 Pro", reasoning: true, input: ["text", "image"], contextWindow: 1000000, maxTokens: 65536 }
        ]
      }
    }
  },
  agent: {
    model: { primary: "flow/auto" }
  }
}
```

## Example: Cost budgets

**The problem:** You set up a cron job that checks your system health every 5 minutes. A bug causes it to loop, firing hundreds of LLM requests. By morning you have a $200 bill.

**The fix:** In the Flow dashboard, set a session budget of $5. Every agent task includes a session ID automatically. Once the $5 limit is hit, Flow rejects further requests before they reach any AI provider. Your morning bill stays at $5.

To enable this, go to your project settings in the Flow dashboard and set `Session Budget (USD)` to your desired cap. No code changes needed -- Flow enforces the limit at the gateway level.

## Example: Prompt management

**The problem:** You want to improve how your code-review agent writes its feedback. Right now the prompt lives in a YAML skill file, so every change means editing the file, restarting OpenClaw, and hoping it works.

**The fix:** Move the prompt to Flow's dashboard. Your skill just sends a name:

```json5
// In the request body (handled by Flow, not OpenClaw config)
{
  "prompt_config": "code-reviewer",
  "prompt_variables": {
    "language": "python",
    "strictness": "high"
  }
}
```

Now you can:

1. Edit the prompt in a web UI with syntax highlighting
2. Test it against live models in the playground
3. Roll it out to 10% of traffic first (canary deployment)
4. Automatically roll back if quality scores drop

No restarts, no file editing, no risk of breaking your running agents.

## Example: Guardrails and PII protection

**The problem:** Your email-drafting agent has access to your inbox. It sends the full email thread -- including phone numbers, email addresses, and account details -- straight to OpenAI.

**The fix:** Flow's PII masking scans every prompt and replaces sensitive data with `[REDACTED]` before it leaves your infrastructure. The AI provider never sees the real data, but your agent still gets a useful response.

What gets caught automatically:

- Email addresses
- Phone numbers
- Credit card numbers
- Social Security numbers
- AWS keys and API tokens
- Bank account and routing numbers

You can also set up keyword blocklists (e.g., block any prompt mentioning "delete all" or "rm -rf") and token limits to prevent agents from sending enormous context windows that cost a fortune.

All of this is configured in the Flow dashboard -- toggle switches and text fields. Nothing to code.

## Getting your API key

1. Sign up at [reiver.ai](https://reiver.ai)
2. Create a project
3. Go to project settings and copy the API key
4. Paste it into your OpenClaw config as shown above

## Need help?

- [Flow README](../flow/README.md) -- full feature reference
- [Plugin README](../integrations/openclaw-plugin/README.md) -- plugin setup details
- [GitHub Issues](https://github.com/your-org/reiver/issues) -- bug reports and feature requests
