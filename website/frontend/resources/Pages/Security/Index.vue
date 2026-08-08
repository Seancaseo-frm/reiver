<template>
  <div class="min-h-screen bg-paper text-ink antialiased font-body">
    <MarketingNav />
    <main>
      <section class="max-w-[1080px] mx-auto px-6 pt-14 pb-1">
        <span class="font-mono text-xs tracking-[0.14em] uppercase text-accent font-medium">Security &amp; Data Handling</span>
        <h1 class="font-display font-semibold text-[clamp(2.1rem,4vw,2.95rem)] leading-[1.04] tracking-tight mt-3 max-w-[17ch]">
          Security &amp; Data Handling
        </h1>
        <p class="text-lg text-muted mt-4 max-w-[60ch]">
          How Reiver processes data to provide the platform during Private Early Access: routing LLM requests, storing configured provider credentials, ingesting observability data, capturing sessions where enabled, operating Moodeng, and supporting customer-connected agents.
        </p>
        <p class="text-sm text-muted mt-3 max-w-[66ch]">
          Private Early Access. No SLA. Not approved for regulated, safety-critical, emergency, or compliance-dependent workloads unless separately agreed in writing. This page is a plain-language summary and does not replace the Early Access Terms, DPA, or Privacy Policy.
        </p>
      </section>

      <div class="max-w-[1080px] mx-auto px-6">
        <div class="max-w-[780px] mx-auto">
          <section v-for="(section, idx) in sections" :key="idx" class="py-9 border-t border-line scroll-mt-20" :class="idx === 0 ? 'border-t-0 pt-2' : ''">
            <h2 class="font-display font-semibold text-xl">{{ section.title }}</h2>
            <p v-for="(para, pidx) in section.paragraphs" :key="pidx" class="text-[#3f4651] text-base mt-3 leading-relaxed">{{ para }}</p>
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

const sections = [
  {
    title: '1. Flow BYOK credential handling',
    paragraphs: [
      'Flow gateway traffic is BYOK during early access. You connect your own model-provider accounts or credentials, and Reiver stores configured provider credentials to route your requests. Reiver does not control your provider accounts, provider billing, provider regions, provider retention or training settings, rate limits, or provider-side compliance controls, unless Reiver expressly provides managed credits or a Reiver-controlled provider account. Provider data handling is governed by your agreement with that provider.',
    ],
  },
  {
    title: '2. Watch observability data',
    paragraphs: [
      'Watch may process telemetry and diagnostic data you or your systems submit, including traces, spans, metrics, logs, errors, stack traces, profiling data, service names, resource attributes, request metadata, session and user identifiers, model and provider names, token counts, cost estimates, latency, prompts, outputs, tool calls and related payloads, depending on your instrumentation and configuration. You control what telemetry and payloads you send. You are responsible for configuring instrumentation, redaction, retention, access controls and deletion appropriate for your use case.',
    ],
  },
  {
    title: '3. Captured sessions',
    paragraphs: [
      'Captured sessions may include prompts, system prompts, user prompts, model inputs and outputs, request and response bodies, traces, labels, identifiers, errors, tool calls and tool outputs, depending on your configuration. You control capture, access, retention and deletion settings where available. Do not capture personal, sensitive, regulated, confidential or production-secret data unless you have the rights and legal basis to process it.',
    ],
  },
  {
    title: '4. Moodeng and DeepSeek',
    paragraphs: [
      'Moodeng is Reiver\'s native platform assistant. During early access, Moodeng may use a Reiver-controlled DeepSeek provider account to answer questions and assist with platform operations. Moodeng can answer questions by default; write access, tool use and automation are disabled unless you enable them through available settings. Depending on your permissions and configuration, Moodeng may access workspace data, project settings, prompts, captured sessions, observability data, usage data and related Customer Data to provide answers or perform enabled actions. Moodeng is experimental and may produce inaccurate answers or take incomplete or incorrect actions; review important actions before relying on them. Customers who cannot allow DeepSeek or non-EEA AI processing should not enable Moodeng or should request a separate configuration before using it.',
    ],
  },
  {
    title: '5. External MCP and API-connected agents',
    paragraphs: [
      'You can connect external agents, IDEs, MCP clients, MCP servers, automation tools and other software using customer-created or scoped Reiver API keys or agent tokens. Actions taken through those credentials are treated as your actions. Only connect agents and tools you trust. You are responsible for their instructions, actions, outputs, security, data handling, retention and compliance. Grant write scopes carefully: write access may allow connected agents to modify prompts, settings, routing, dashboards, alerts, capture rules, retention settings or other project resources.',
    ],
  },
  {
    title: '6. Retention and deletion',
    paragraphs: [
      'Where retention and deletion settings are available, you control them for observability data and captured sessions. Retention windows vary by plan. During early access some controls may be limited; contact us for deletion requests that cannot yet be self-served.',
    ],
  },
  {
    title: '7. Sub-processors and BYOK destinations',
    paragraphs: [
      'Reiver uses infrastructure and operations vendors, and a Reiver-controlled AI provider for Moodeng, listed on our Sub-processors page. Customer-selected BYOK destinations are not Reiver sub-processors unless accessed through a Reiver-controlled account, Reiver-managed credits, or a native Reiver feature.',
      'Google Workspace may be used for internal business operations, communications, documents, calendar and support. Gemini for Google Workspace may be used only where enabled in our internal workspace settings, and is not a Flow provider unless product traffic is actually routed there through Flow.',
    ],
  },
  {
    title: '8. Cookies and external requests',
    paragraphs: [
      'This site does not run advertising or marketing trackers. Fonts are currently loaded from Google-hosted URLs (fonts.googleapis.com, fonts.gstatic.com), which sends a request to Google when the page loads. See the Cookie Policy for current detail.',
    ],
  },
  {
    title: '9. Early access limitations',
    paragraphs: [
      'Reiver is in Private Early Access. There is no SLA, and production use is at your own risk. Redaction and guardrails are configurable assistive controls and may not catch everything. Routing, failover and cost figures are best-effort and not guaranteed. Regulated, high-risk, safety-critical, emergency and compliance-dependent use requires separate written approval.',
    ],
  },
  {
    title: '10. Security contact',
    paragraphs: [
      'Report a security issue or make a data request at security@reiver.ai.',
    ],
  },
];
</script>
