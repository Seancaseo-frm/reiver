<template>
  <section class="py-24" id="product" style="padding-top: 8px">
    <div class="max-w-wrap mx-auto px-6">
      <div class="max-w-[60ch]">
        <span class="font-mono text-xs tracking-[0.14em] uppercase text-accent font-medium">One control plane</span>
        <h2 class="font-display font-semibold text-[clamp(1.9rem,3vw,2.7rem)] leading-[1.04] tracking-tight mt-3">
          Gateway, observability, agents, prompts, and an agent that runs them.
        </h2>
        <p class="text-lg text-muted mt-4 max-w-[52ch]">
          Adopt any part on its own, or run them together for an end-to-end view of your production AI.
        </p>
      </div>

      <div class="grid grid-cols-1 md:grid-cols-2 gap-5 mt-12">
        <div v-for="pill in pillars" :key="pill.tag"
             class="bg-white border border-line rounded-[14px] p-7 transition-all hover:border-[#cfd3db] hover:-translate-y-0.5 hover:shadow-card">
          <span class="font-mono text-xs tracking-[0.08em] uppercase text-accent">{{ pill.tag }}</span>
          <h3 class="font-display font-semibold text-xl mt-3 flex items-baseline gap-2">
            {{ pill.title }}
            <small v-if="pill.subtitle" class="font-sans font-normal text-sm text-faint tracking-normal">{{ pill.subtitle }}</small>
          </h3>
          <p class="text-muted text-base mt-3">{{ pill.desc }}</p>
          <ul class="flex flex-wrap gap-2 mt-4">
            <li v-for="tag in pill.tags" :key="tag"
                class="font-mono text-xs text-[#454c58] bg-paper border border-line rounded-full px-3 py-1">
              {{ tag }}
            </li>
          </ul>
        </div>
      </div>

      <p class="text-sm text-muted mt-6 max-w-[66ch]">
        Moodeng is experimental, Watch processes only the telemetry you send, and captured sessions can include prompts and outputs where enabled.
        <router-link to="/security" class="text-accent hover:underline">Security &amp; Data Handling</router-link>.
      </p>
    </div>
  </section>
</template>

<script setup>
const pillars = [
  {
    tag: 'Flow',
    title: 'LLM gateway',
    subtitle: 'and prompt hub',
    desc: 'One OpenAI-compatible API across providers, with fallback rules where configured, semantic caching and per-request cost tracking. Version prompts and roll them out as config, not redeploys.',
    tags: ['multi-provider routing', 'fallback', 'caching', 'cost tracking', 'canary + A/B'],
  },
  {
    tag: 'Watch',
    title: 'Observability',
    subtitle: 'OpenTelemetry-native',
    desc: 'Traces, errors, logs, metrics and continuous profiling for any stack, including Unity and Unreal. Send data with any OTel SDK or collector. LLM calls link back to the app trace.',
    tags: ['tracing', 'errors', 'logs', 'metrics', 'profiling', 'games'],
  },
  {
    tag: 'Herd',
    title: 'A2A hub',
    subtitle: 'agent-to-agent',
    desc: 'Register, discover and route messages between agents across your org. Built-in access controls, push notifications, and topology views. The backbone for multi-agent systems.',
    tags: ['agent registry', 'discovery', 'A2A messaging', 'access control', 'push notifications'],
  },
  {
    tag: 'Moodeng',
    title: 'The agent',
    subtitle: 'over MCP',
    desc: 'Set up, query and operate everything through your coding agent or IDE. Ask about a rollout, investigate a failed trace, or wire in a new service, in plain language.',
    tags: ['MCP server', 'investigate traces', 'manage prompts', 'in-app agent'],
  },
  {
    tag: 'SDKs',
    title: 'Drop-in clients',
    subtitle: null,
    desc: 'First-party SDKs for Python, Rust, Unity and Unreal, or bring any OpenTelemetry SDK and any OpenAI-compatible client. Instrument once and you are done.',
    tags: ['Python', 'Rust', 'Unity', 'Unreal', 'OpenTelemetry'],
  },
];
</script>
