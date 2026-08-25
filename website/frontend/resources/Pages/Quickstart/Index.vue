<template>
  <div class="min-h-screen bg-paper text-ink antialiased font-body">
    <MarketingNav />
    <main>
      <section class="max-w-[1080px] mx-auto px-6 pt-14 pb-1">
        <span class="font-mono text-xs tracking-[0.14em] uppercase text-accent font-medium">Quickstart</span>
        <h1 class="font-display font-semibold text-[clamp(2.1rem,4vw,2.95rem)] leading-[1.04] tracking-tight mt-3 max-w-[17ch]">
          Choose the smallest path. Prove it works.
        </h1>
        <p class="text-lg text-muted mt-4 max-w-[60ch]">
          Onboard Flow, Watch, or the complete Reiver loop. Each path has its own evidence-based finish line, so you add only what your application needs.
        </p>
        <div class="flex gap-2 mt-5 flex-wrap">
          <span class="font-mono text-xs text-[#454c58] bg-white border border-line rounded-full px-3.5 py-1.5">Agent-readable</span>
          <span class="font-mono text-xs text-[#454c58] bg-white border border-line rounded-full px-3.5 py-1.5">Evidence-based</span>
        </div>
      </section>

      <div class="max-w-[1080px] mx-auto px-6 pt-9 pb-2">
        <div class="max-w-[760px]">
          <section class="scroll-mt-20">
            <h2 class="font-display font-semibold text-2xl">Choose your track</h2>
            <p class="text-base text-[#3f4651] mt-2">Flow and Watch work independently. Choose Complete Reiver when you want gateway control, full observability and agent verification together.</p>
            <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mt-6">
              <a v-for="track in trackCards" :key="track.title" :href="track.href"
                 class="block bg-white border border-line rounded-xl p-5 transition-all hover:border-[#cfd3db] hover:-translate-y-0.5 hover:shadow-card">
                <span class="font-mono text-[0.68rem] tracking-[0.06em] uppercase text-accent">{{ track.tag }}</span>
                <h3 class="font-display font-semibold text-lg mt-2">{{ track.title }}</h3>
                <p class="text-muted text-sm mt-2">{{ track.desc }}</p>
                <p class="text-[#3f4651] text-sm mt-3"><strong>Done:</strong> {{ track.done }}</p>
              </a>
            </div>
            <div class="bg-white border border-line border-l-[3px] border-l-accent rounded-[10px] p-3.5 mt-4 text-sm text-[#3f4651] leading-relaxed">
              <span class="block font-mono text-[0.66rem] tracking-[0.1em] uppercase text-accent mb-1">This page continues with Complete Reiver</span>
              The steps below deliberately prove Flow, Watch, session identity and MCP. The standalone guides have smaller definitions of done.
            </div>
          </section>

          <section id="complete-reiver" class="py-9 border-t border-line scroll-mt-20 mt-9">
            <h2 class="font-display font-semibold text-2xl">First, the pieces</h2>
            <p class="text-base text-[#3f4651] mt-2">Three credential roles. Keep their boundaries explicit and the integration stays simple.</p>
            <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mt-6 mb-1">
              <div v-for="key in keyCards" :key="key.title" class="bg-white border border-line rounded-xl p-5">
                <h4 class="font-display font-semibold text-base mb-2">{{ key.title }}</h4>
                <span class="inline-flex items-center font-mono text-xs text-[#454c58] bg-paper border border-line rounded-lg px-2.5 py-1">{{ key.location }}</span>
                <p class="text-muted text-sm mt-2">{{ key.desc }}</p>
              </div>
            </div>
            <div class="bg-[#FCF3E9] border border-[#F1DCC4] border-l-[3px] border-l-[#C9802E] rounded-[10px] p-3.5 mt-4 text-sm text-[#5a4a33] leading-relaxed">
              <span class="block font-mono text-[0.66rem] tracking-[0.1em] uppercase text-[#B0651C] mb-1">The 403 trap</span>
              The SDK key and agent token are not interchangeable. Flow and Watch use the <code class="font-mono text-[0.85em] bg-black/5 px-1 py-px rounded">SDK key</code>; MCP uses the <code class="font-mono text-[0.85em] bg-black/5 px-1 py-px rounded">agent token</code>. Never give the provider key to the application or coding agent.
            </div>
          </section>

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
  { title: 'Provider key', location: 'Prompt Hub › Integrations', desc: 'Stored in Reiver only. Do not copy it into your app, repository or coding-agent environment.' },
  { title: 'SDK key', location: 'Settings › General › SDK keys', desc: 'One value authenticates Flow and Watch. Bind it to two explicit application secrets.' },
  { title: 'Agent token', location: 'Agents › Tokens', desc: 'A separate scoped MCP credential. It belongs only in the coding-agent environment.' },
];

const trackCards = [
  {
    tag: 'Gateway',
    title: 'Flow + Prompt Hub',
    desc: 'Route LLM calls, prove the actual provider and model, and add managed prompts only when useful.',
    done: 'A real request and its routing evidence pass.',
    href: 'https://docs.reiver.ai/flow/getting-started',
  },
  {
    tag: 'Observability',
    title: 'Watch',
    desc: 'Send application traces, structured logs and metrics without changing LLM routing.',
    done: 'All three signals are queryable under one service.',
    href: 'https://docs.reiver.ai/watch/',
  },
  {
    tag: 'Combined',
    title: 'Complete Reiver',
    desc: 'Combine Flow, Watch, business sessions and an MCP-connected coding agent.',
    done: 'Every acceptance check on this page passes.',
    href: '#complete-reiver',
  },
];

const steps = [
  {
    num: '1',
    title: 'Prove one provider',
    blocks: [
      { type: 'text', content: 'In <strong>Prompt Hub › Integrations</strong>, add the provider the application already uses and click <strong>Test</strong>. That proves key authentication only. Then prove the exact model with one short request in the <strong>Playground</strong>.' },
      { type: 'note', label: 'Anthropic baseline', content: 'Start with claude-sonnet-5 for the balance of speed and intelligence. Leave sampling at provider default and do not add a legacy thinking budget. Fast mode is limited to supported Opus 5/4.8 aliases, costs more and requires Anthropic access. Batch entries are not interactive Flow choices.' },
      { type: 'warn', label: 'Keep the first run boring', content: 'Do not add Qwen, DeepInfra, auto routing, fallbacks or prompt overrides yet. First prove the existing Anthropic path end to end.' },
    ],
  },
  {
    num: '2',
    title: 'Create explicit secret bindings',
    blocks: [
      { type: 'text', content: 'Create one SDK key, then bind its value twice in the application runtime. The values are currently identical; the names keep Flow and Watch configuration independent.' },
      { type: 'code', lang: 'bash · application runtime', content: 'export REIVER_FLOW_API_KEY="dh_..."\nexport REIVER_WATCH_API_KEY="dh_..."  # same SDK key value' },
      { type: 'text', content: 'Create a separate agent token. Use <code>llm:read</code> and <code>observability:read</code> for evaluation. Add <code>llm:write</code> for autonomous prompt, label and gateway setup, and <code>observability:write</code> when the agent may also create dashboards or alerts.' },
      { type: 'code', lang: 'bash · coding agent only', content: 'export REIVER_AGENT_TOKEN="dh_..."' },
      { type: 'note', label: 'Secret boundary', content: 'SDK keys and agent tokens both currently look like <code>dh_…</code>, but their stored types are not interchangeable. The provider key stays inside Reiver. The agent token never ships with the application. No credential belongs in source control or logs.' },
      { type: 'text', content: 'Have the coding agent write against the two SDK variable names; do not paste their values into its prompt. If it must launch the app, inject them into that disposable test process through the platform secret manager. Otherwise, let the application owner deploy the agent’s code changes into the configured test runtime.' },
    ],
  },
  {
    num: '3',
    title: 'Connect the coding agent',
    blocks: [
      { type: 'text', content: 'Connect to <code>https://reiver.ai/mcp</code> with the agent token, then ask the client to list Reiver resources and read <code>agent://onboarding</code>.' },
      { type: 'code', lang: 'toml · Codex .codex/config.toml', content: '[mcp_servers.reiver]\nurl = "https://reiver.ai/mcp"\nbearer_token_env_var = "REIVER_AGENT_TOKEN"' },
      { type: 'code', lang: 'json · Claude Code .mcp.json', content: '{\n  "mcpServers": {\n    "reiver": {\n      "type": "http",\n      "url": "https://reiver.ai/mcp",\n      "headers": { "Authorization": "Bearer ${REIVER_AGENT_TOKEN}" }\n    }\n  }\n}' },
      { type: 'code', lang: 'json · Cursor .cursor/mcp.json', content: '{\n  "mcpServers": {\n    "reiver": {\n      "type": "http",\n      "url": "https://reiver.ai/mcp",\n      "headers": { "Authorization": "Bearer ${env:REIVER_AGENT_TOKEN}" }\n    }\n  }\n}' },
      { type: 'note', label: 'Verify the client', content: 'After restarting: use <code>codex mcp list</code> or Codex <code>/mcp</code>; use <code>claude mcp list</code>, <code>claude mcp get reiver</code> or Claude Code <code>/mcp</code>; or check <strong>Cursor Settings → Tools & MCP</strong>. Then read <code>agent://onboarding</code>—a visible server name alone is not proof of authenticated resource access.' },
      { type: 'note', label: 'No autonomy-mode setting', content: 'Token scopes are the hard technical boundary. Your assignment tells the agent how proactively it may act inside that boundary. A clear autonomous-onboarding instruction authorises the work once; the agent should not ask again before every in-scope action.' },
      { type: 'warn', label: 'Connected agents act as you', content: 'Actions taken through connected credentials are treated as your actions. Grant write scopes only to agents and tools you trust, and state what remains out of bounds.' },
    ],
  },
  {
    num: '4',
    title: 'Give the agent its business and autonomy contract',
    blocks: [
      { type: 'text', content: 'Paste the assignment below for autonomous setup. The MCP resource contains the stack-specific workflow, business-discovery checkpoint and definition of done. Remove the autonomy paragraph for a read-only evaluation.' },
      { type: 'code', lang: 'paste to your coding agent', content: 'Read agent://onboarding and gateway_settings.agent_soul through MCP before\nediting code. Reuse stored project context that still matches this application.\nThen inspect the app’s README, user-facing behaviour, prompts and data model as\nwell as its framework, LLM client, logging and OTel setup.\n\nThis is a Complete Reiver onboarding. Give me a short “My understanding”\nsummary: what the app does, its users and intended outcome. Include a Session\nand Identity Contract stating the meaningful session unit, start event,\nsuccessful and other terminal events, idle fallback, stable pseudonymous user\nID source, anonymous-user policy and tenant scoping. Ask only about material\nconflicts or gaps; do not ask me to repeat context Reiver already holds.\n\nYou may act autonomously within the llm and observability scopes granted to\nyour Reiver token. Establish and prove a simple baseline first. Then you may\ncreate, test and roll out relevant prompts; configure business-specific labels,\nprofiles and guardrails; and create useful dashboards and alerts. Do not ask\nagain for every in-scope action. Do not delete resources, change provider\ncredentials, increase budgets, weaken safety controls or modify unrelated\nproduction resources without asking.\n\nRoute existing LLM calls through https://reiver.ai/api/gateway/v1 using\nREIVER_FLOW_API_KEY. Export traces, logs and metrics over OTLP HTTP to\nhttps://reiver.ai/api/watch/ingest using REIVER_WATCH_API_KEY. Implement the\nconfirmed Session and Identity Contract and save it in Agent Soul. Correlate the\nstable user and session as agent://onboarding specifies and explicitly end the\nsession.\n\nRun every acceptance check, then configure only capabilities tied to a confirmed\nbusiness outcome. Send synthetic success and failure sessions and verify the\ntraffic, telemetry, labels and controls through MCP. Return a plain-English\nactivation report with evidence, deliberate omissions and rollback paths. Never\nprint or commit keys.' },
      { type: 'note', label: 'Why the wording matters', content: 'The agent investigates before asking questions, understands the business before creating labels, proves the technical baseline before adding complexity, and then acts without repeated approval inside the authority you granted.' },
    ],
  },
  {
    num: '5',
    title: 'Prove Flow before changing the app',
    blocks: [
      { type: 'text', content: 'Run one direct gateway request and inspect the headers. This separates provider/routing problems from application instrumentation problems.' },
      { type: 'code', lang: 'bash', content: 'curl --include https://reiver.ai/api/gateway/v1/chat/completions \\\n  --header "Authorization: Bearer $REIVER_FLOW_API_KEY" \\\n  --header "Content-Type: application/json" \\\n  --header "x-reiver-session-id: onboarding-smoke-1" \\\n  --header "x-reiver-user-id: onboarding-user-1" \\\n  --data \'{"model":"claude-sonnet-5","user":"onboarding-user-1",\n            "messages":[{"role":"user","content":"Reply: reiver-flow-ok"}]}\'' },
      { type: 'note', label: 'Record the evidence', content: 'HTTP must be 200, x-reiver-provider must be the intended provider, x-reiver-model-used must be the actual model, and x-request-id must be present.' },
    ],
  },
  {
    num: '6',
    title: 'Verify the complete loop',
    blocks: [
      { type: 'text', content: 'For this Complete Reiver track, the agent must show evidence for: MCP resource access; provider test; a real application gateway request; actual provider and model; an application trace; a correlated structured log; an application or runtime metric; the confirmed Session and Identity Contract saved in Agent Soul; a <code>202</code> explicit session-end response; a second session with the same test user and a new session ID; and no secrets in source or output.' },
      { type: 'code', lang: 'bash · end the smoke-test session', content: 'curl --request POST \\\n  "https://reiver.ai/api/gateway/v1/sessions/onboarding-smoke-1/end" \\\n  --header "Authorization: Bearer $REIVER_FLOW_API_KEY"' },
      { type: 'warn', label: 'No partial credit', content: 'A 200 gateway response plus one trace is not full observability. Missing logs or metrics means their providers, processors/readers, exporters, or application instrumentation are not configured.' },
      { type: 'text', content: 'After every baseline check is green, the authorised agent should preserve the confirmed project context for future Reiver agent sessions, translate business outcomes into precise session labels, configure only relevant prompts, guardrails, profiles, dashboards and alerts, run synthetic success and failure sessions, and verify the result through MCP.' },
      { type: 'note', label: 'The complete Reiver loop', content: 'Business context gives labels meaning; Flow controls providers, prompts and guardrails; Watch records gateway and application evidence; sessions and labels expose user and business outcomes; MCP lets the agent inspect that evidence and improve the system.' },
    ],
  },
];

const nextCards = [
  { tag: 'Canonical', title: 'Full Quickstart', desc: 'Credential matrix, agent contract and acceptance criteria.', href: 'https://docs.reiver.ai/quickstart' },
  { tag: 'Flow', title: 'Gateway', desc: 'Routing, fallback, caching and API reference.', href: 'https://docs.reiver.ai/flow/getting-started' },
  { tag: 'Watch', title: 'Observability', desc: 'Traces, logs, metrics and troubleshooting.', href: 'https://docs.reiver.ai/watch/' },
  { tag: 'Agent', title: 'MCP', desc: 'Current Codex, Claude Code and Cursor setup.', href: 'https://docs.reiver.ai/agent/mcp-setup' },
];
</script>
