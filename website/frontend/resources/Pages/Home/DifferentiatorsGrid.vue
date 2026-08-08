<template>
  <section class="py-24" id="how">
    <div class="max-w-wrap mx-auto px-6">
      <div class="max-w-[60ch]">
        <span class="font-mono text-xs tracking-[0.14em] uppercase text-accent font-medium">How it works</span>
        <h2 class="font-display font-semibold text-[clamp(1.9rem,3vw,2.7rem)] leading-[1.04] tracking-tight mt-3">
          Three steps. Your agent handles the rest.
        </h2>
        <p class="text-lg text-muted mt-4 max-w-[52ch]">
          The docs are written for agents, not just humans. Hand them over and the integration runs itself.
        </p>
      </div>

      <div class="grid grid-cols-1 md:grid-cols-3 gap-7 mt-12">
        <div v-for="step in steps" :key="step.num" class="pt-6 border-t-2 border-ink">
          <span class="font-mono text-sm text-accent tracking-wide">{{ step.num }}</span>
          <h3 class="font-display font-semibold text-xl mt-3 mb-2">{{ step.title }}</h3>
          <p class="text-muted text-base">{{ step.desc }}</p>
          <div class="mt-4 font-mono text-xs bg-panel text-[#cfd5de] rounded-[10px] px-3.5 py-3 overflow-x-auto" v-html="step.code"></div>
        </div>
      </div>

      <p class="font-mono text-sm text-ink mt-8 tracking-wide">Zero onboarding friction.</p>
    </div>
  </section>
</template>

<script setup>
const steps = [
  {
    num: '01',
    title: 'Connect your provider',
    desc: 'Point any OpenAI-compatible client at the gateway by changing one base URL, or send OpenTelemetry traces straight to Reiver.',
    code: '<span class="text-hi">baseURL</span>: <span class="text-ok">"https://reiver.ai/api/gateway/v1"</span>',
  },
  {
    num: '02',
    title: 'Give your agent a scoped token and the docs',
    desc: 'A scoped Reiver project token, a scoped MCP token, and our docs. That is everything your coding agent needs to wire Reiver into your codebase.',
    code: '<span class="text-hi">$</span> export REIVER_KEY=<span class="text-ok">"••••"</span><br><span class="text-hi">$</span> claude --mcp reiver',
  },
  {
    num: '03',
    title: 'It does the rest',
    desc: 'Your agent instruments the app, sets up tracing, errors and metrics, wires in prompt management, and you operate all of it through the agent.',
    code: '<span class="text-ok">✓</span> traces, errors, costs<br><span class="text-ok">✓</span> prompts under version control',
  },
];
</script>
