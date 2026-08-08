<template>
  <div class="min-h-screen bg-paper text-ink antialiased font-body">
    <MarketingNav />
    <main>
      <section class="max-w-[1080px] mx-auto px-6 pt-14 pb-1">
        <span class="font-mono text-xs tracking-[0.14em] uppercase text-accent font-medium">Quickstart</span>
        <h1 class="font-display font-semibold text-[clamp(2.1rem,4vw,2.95rem)] leading-[1.04] tracking-tight mt-3 max-w-[17ch]">
          From signup to first trace in about 30 minutes.
        </h1>
        <p class="text-lg text-muted mt-4 max-w-[60ch]">
          Reiver has two setup paths: hand your coding agent the docs and a key and let it wire everything in, or do it yourself in a few lines. Both start the same way. This guide covers the gateway, observability over OpenTelemetry, managed prompts and sessions.
        </p>
        <div class="flex gap-2 mt-5 flex-wrap">
          <span class="font-mono text-xs text-[#454c58] bg-white border border-line rounded-full px-3.5 py-1.5">Let your agent do it</span>
          <span class="font-mono text-xs text-[#454c58] bg-white border border-line rounded-full px-3.5 py-1.5">Or wire it yourself</span>
        </div>
      </section>

      <div class="max-w-[1080px] mx-auto px-6 pt-9 pb-2">
        <div class="max-w-[760px]">
          <!-- First, the pieces -->
          <section class="scroll-mt-20">
            <h2 class="font-display font-semibold text-2xl">First, the pieces</h2>
            <p class="text-base text-[#3f4651] mt-2">Three credentials, two of them keys. Knowing which is which saves you the most common 403.</p>
            <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mt-6 mb-1">
              <div v-for="key in keyCards" :key="key.title" class="bg-white border border-line rounded-xl p-5">
                <h4 class="font-display font-semibold text-base mb-2">{{ key.title }}</h4>
                <span class="inline-flex items-center font-mono text-xs text-[#454c58] bg-paper border border-line rounded-lg px-2.5 py-1">{{ key.location }}</span>
                <p class="text-muted text-sm mt-2">{{ key.desc }}</p>
              </div>
            </div>
            <div class="bg-[#FCF3E9] border border-[#F1DCC4] border-l-[3px] border-l-[#C9802E] rounded-[10px] p-3.5 mt-4 text-sm text-[#5a4a33] leading-relaxed">
              <span class="block font-mono text-[0.66rem] tracking-[0.1em] uppercase text-[#B0651C] mb-1">The 403 trap</span>
              The SDK key and the agent token are not interchangeable. The gateway and OpenTelemetry use the <code class="font-mono text-[0.85em] bg-black/5 px-1 py-px rounded">SDK key</code>. MCP uses the <code class="font-mono text-[0.85em] bg-black/5 px-1 py-px rounded">agent token</code>, and rejects SDK keys with a 403. Mixing them is the most common setup error.
            </div>
          </section>

          <!-- Steps -->
          <section v-for="step in steps" :key="step.num" class="py-9 border-t border-line scroll-mt-20">
            <div class="flex items-center gap-3.5">
              <span class="flex-none w-[30px] h-[30px] rounded-lg bg-ink text-white font-mono text-sm flex items-center justify-center">{{ step.num }}</span>
              <h2 class="font-display font-semibold text-2xl">{{ step.title }}</h2>
            </div>
            <div v-for="(block, bidx) in step.blocks" :key="bidx" class="mt-3">
              <p v-if="block.type === 'text'" class="text-[#3f4651] text-base leading-relaxed" v-html="block.content"></p>
              <h3 v-if="block.type === 'subtitle'" class="font-display font-semibold text-base mt-5 mb-0.5">{{ block.content }}</h3>
              <div v-if="block.type === 'code'" class="bg-panel border border-panel-line rounded-xl overflow-hidden my-3.5">
                <div class="flex items-center justify-between px-3.5 py-2 border-b border-panel-line bg-panel-2">
                  <span class="font-mono text-[0.68rem] tracking-[0.08em] uppercase text-panel-muted">{{ block.lang }}</span>
                </div>
                <pre class="p-4 overflow-x-auto"><code class="font-mono text-sm leading-[1.75] text-panel-fg whitespace-pre">{{ block.content }}</code></pre>
              </div>
              <div v-if="block.type === 'note'" class="bg-white border border-line border-l-[3px] border-l-accent rounded-[10px] p-3.5 my-4 text-sm text-[#3f4651] leading-relaxed">
                <span class="block font-mono text-[0.66rem] tracking-[0.1em] uppercase text-accent mb-1">{{ block.label }}</span>
                {{ block.content }}
              </div>
              <div v-if="block.type === 'warn'" class="bg-[#FCF3E9] border border-[#F1DCC4] border-l-[3px] border-l-[#C9802E] rounded-[10px] p-3.5 my-4 text-sm text-[#5a4a33] leading-relaxed">
                <span class="block font-mono text-[0.66rem] tracking-[0.1em] uppercase text-[#B0651C] mb-1">{{ block.label }}</span>
                {{ block.content }}
              </div>
            </div>
          </section>

          <!-- Where to next -->
          <section class="py-9 border-t border-line">
            <h2 class="font-display font-semibold text-2xl">Where to next</h2>
            <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3.5 mt-4">
              <a v-for="card in nextCards" :key="card.title" :href="card.href"
                 class="block bg-white border border-line rounded-xl p-4 transition-all hover:border-[#cfd3db] hover:-translate-y-0.5 hover:shadow-card">
                <span class="font-mono text-[0.68rem] tracking-[0.06em] uppercase text-accent">{{ card.tag }}</span>
                <h4 class="font-display font-semibold text-base mt-2 mb-1">{{ card.title }}</h4>
                <p class="text-muted text-sm">{{ card.desc }}</p>
              </a>
            </div>
          </section>
        </div>
      </div>
    </main>
    <MarketingFooter />
  </div>
</template>

<script setup>
import MarketingNav from '../Home/MarketingNav.vue';
import MarketingFooter from '../Home/MarketingFooter.vue';

const keyCards = [
  { title: 'Provider keys', location: 'Prompt Hub › Settings', desc: 'Your OpenAI, Anthropic, Gemini or Bedrock keys. Reiver stores them and calls providers for you, so they never touch your app code.' },
  { title: 'SDK key', location: 'Settings › General › SDK keys', desc: 'Your application key. One key authenticates both the Flow gateway and OpenTelemetry ingestion.' },
  { title: 'Agent token', location: 'Settings › Agent Tokens', desc: 'For operating Reiver over MCP. Give it write scopes if you want the agent to set things up, not just read.' },
];

const steps = [
  {
    num: '1',
    title: 'Connect a provider',
    blocks: [
      { type: 'text', content: 'Add at least one provider key so the gateway has a model to call. In <strong>Prompt Hub › Settings</strong>, paste your OpenAI or Anthropic key, click <strong>Test</strong> to confirm the connection is live, then try a call in the <strong>Playground</strong>.' },
      { type: 'note', label: 'Why this is separate', content: 'Provider keys live in Reiver, not in your app. Your code only ever carries the SDK key, so rotating a provider key never means a redeploy.' },
    ],
  },
  {
    num: '2',
    title: 'Grab your keys',
    blocks: [
      { type: 'subtitle', content: 'SDK key' },
      { type: 'text', content: 'Create an SDK key in <strong>Settings › General › SDK keys</strong> and store it as an environment variable. This one key covers the gateway and observability.' },
      { type: 'code', lang: 'bash', content: '# your application key: gateway + OpenTelemetry\nexport REIVER_API_KEY="paste-your-sdk-key"' },
      { type: 'subtitle', content: 'Agent token' },
      { type: 'text', content: 'Create an agent token in <strong>Settings › Agent Tokens</strong> for MCP. Tokens default to read-only, so add <code>llm:write</code> and <code>observability:write</code> scopes if you want the agent to configure prompts, dashboards or alerts.' },
      { type: 'code', lang: 'bash', content: '# for MCP only (give it write scopes to configure)\nexport REIVER_AGENT_TOKEN="paste-your-agent-token"' },
      { type: 'note', label: 'Write scopes', content: 'Write access may allow connected agents to modify prompts, settings, routing, dashboards, alerts, capture rules, retention settings, or other project resources. Grant them carefully.' },
    ],
  },
  {
    num: '3',
    title: 'Set it up',
    blocks: [
      { type: 'text', content: 'Two ways to do the same thing. Let your coding agent wire it in (recommended), or do it by hand.' },
      { type: 'subtitle', content: 'Connect your agent to MCP' },
      { type: 'text', content: 'Point your coding agent at Reiver\'s MCP server with your agent token.' },
      { type: 'code', lang: 'bash · Claude Code', content: 'claude mcp add --transport http reiver https://reiver.ai/mcp \\\n  --header "Authorization: Bearer $REIVER_AGENT_TOKEN"' },
      { type: 'code', lang: 'json · Cursor and other MCP clients', content: '{\n  "mcpServers": {\n    "reiver": {\n      "url": "https://reiver.ai/mcp",\n      "headers": { "Authorization": "Bearer YOUR_AGENT_TOKEN" }\n    }\n  }\n}' },
      { type: 'warn', label: 'Connected agents act as you', content: 'You can connect external agents, IDEs, MCP clients, automation tools or other software using customer-created credentials or scoped agent tokens. Actions taken through those credentials are treated as your actions. Only connect agents and tools you trust.' },
      { type: 'subtitle', content: 'Hand it the docs and your SDK key' },
      { type: 'text', content: 'With <code>REIVER_API_KEY</code> set in your environment, paste this to your agent:' },
      { type: 'code', lang: 'paste to your agent', content: 'Read the Reiver docs at https://docs.reiver.ai, then instrument this app:\n\n1. Route all LLM calls through the Reiver Flow gateway at\n   https://reiver.ai/api/gateway/v1, authenticating with my SDK key\n   in REIVER_API_KEY. Keep the code OpenAI-compatible.\n2. Send OpenTelemetry traces, metrics and logs to Reiver Watch at\n   https://reiver.ai/api/watch/ingest using the same SDK key.\n3. Where I reference a managed prompt by name, send it as prompt_config\n   so Flow injects the managed version.\n4. Add an x-reiver-session-id header to group requests into sessions,\n   and tag spans with the gen_ai.session_id attribute.\n\nUse environment variables for all secrets. Do not hardcode keys.' },
      { type: 'subtitle', content: 'Or wire it yourself' },
      { type: 'text', content: 'Point your client at the gateway base URL and authenticate with your SDK key.' },
      { type: 'code', lang: 'python · OpenAI SDK', content: 'import os\nfrom openai import OpenAI\n\nclient = OpenAI(\n    base_url="https://reiver.ai/api/gateway/v1",\n    api_key=os.environ["REIVER_API_KEY"],\n)\n\nresp = client.chat.completions.create(\n    model="gpt-4o",\n    messages=[{"role": "user", "content": "Hello from Reiver"}],\n)\nprint(resp.choices[0].message.content)' },
      { type: 'code', lang: 'bash · environment', content: 'export OTEL_EXPORTER_OTLP_ENDPOINT="https://reiver.ai/api/watch/ingest"\nexport OTEL_EXPORTER_OTLP_HEADERS="Authorization=Bearer $REIVER_API_KEY"\nexport OTEL_EXPORTER_OTLP_PROTOCOL="http/protobuf"\nexport OTEL_SERVICE_NAME="my-app"' },
    ],
  },
  {
    num: '4',
    title: 'Use managed prompts',
    blocks: [
      { type: 'text', content: 'Version prompts in Hub and reference them by name. Flow injects the managed prompt at request time, so you change prompts without shipping code.' },
      { type: 'code', lang: 'python', content: 'resp = client.chat.completions.create(\n    model="gpt-4o",\n    messages=[{"role": "user", "content": user_input}],\n    extra_body={"prompt_config": "Reiver GTM / Onboarding Prep"},\n)' },
      { type: 'note', label: 'The one rule', content: 'Flow injects a managed prompt only when your request has no system message. Send your own system message and Reiver stays a transparent proxy. Pass template values with prompt_variables or x-reiver-var-* headers.' },
    ],
  },
  {
    num: '5',
    title: 'Capture sessions',
    blocks: [
      { type: 'text', content: 'Group related requests into a session by sending the same session id on each gateway call.' },
      { type: 'code', lang: 'python', content: 'resp = client.chat.completions.create(\n    model="gpt-4o",\n    messages=[{"role": "user", "content": user_input}],\n    extra_headers={"x-reiver-session-id": session_id},\n)' },
      { type: 'text', content: 'To stitch your own application spans into the same session, tag them with the OpenTelemetry attribute:' },
      { type: 'code', lang: 'python', content: 'span.set_attribute("gen_ai.session_id", session_id)' },
      { type: 'note', label: 'Good to know', content: 'Sessions appear in the dashboard about 30 minutes after the last request, or call the end-session endpoint to evaluate in around 30 seconds. Organise and filter them with Session Profiles and Session Labels.' },
      { type: 'warn', label: 'What sessions can contain', content: 'Captured sessions may include prompts, system prompts, user prompts, model inputs and outputs, request and response bodies, traces, tool calls, errors, labels, identifiers, token counts, costs and latency, depending on your configuration. You control capture rules, access, deletion and retention.' },
    ],
  },
  {
    num: '6',
    title: 'Verify',
    blocks: [
      { type: 'text', content: 'Make one request, then check: <strong>Watch › Tracing and Errors</strong> for the live trace, <strong>Sessions</strong> for your session id, <strong>Playground</strong> to replay a managed prompt, or ask Moodeng over MCP: <em>show me the gateway overview for the last 7 days</em>.' },
      { type: 'note', label: 'About Moodeng', content: 'Moodeng is Reiver\'s native platform assistant. In Private Early Access it may use a Reiver-controlled DeepSeek provider account to answer questions and assist with platform operations. It is experimental and may produce inaccurate answers.' },
      { type: 'text', content: 'That is the full loop: routed, observed, prompt-managed and queryable through your agent.' },
    ],
  },
];

const nextCards = [
  { tag: 'Flow', title: 'Gateway', desc: 'Routing, fallback, caching, API reference.', href: 'https://docs.reiver.ai/flow/getting-started.html' },
  { tag: 'Watch', title: 'Observability', desc: 'Traces, errors, metrics, logs, profiling.', href: 'https://docs.reiver.ai/watch/' },
  { tag: 'Agent', title: 'MCP', desc: 'Operate Reiver from your IDE or agent.', href: 'https://docs.reiver.ai/agent/' },
  { tag: 'SDKs', title: 'Clients', desc: 'Python, Rust, Unity, Unreal, OpenTelemetry.', href: 'https://docs.reiver.ai/' },
];
</script>
