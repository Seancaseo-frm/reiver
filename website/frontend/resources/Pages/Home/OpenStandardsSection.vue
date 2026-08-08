<template>
  <section class="py-24" id="approach" style="padding-top: 8px">
    <div class="max-w-wrap mx-auto px-6">
      <div class="bg-panel text-panel-fg rounded-3xl p-10 lg:p-14 grid grid-cols-1 lg:grid-cols-[1.1fr_1fr] gap-10 lg:gap-12 items-center">
        <div>
          <span class="font-mono text-xs tracking-[0.14em] uppercase text-hi font-medium">Why agent-first</span>
          <h2 class="font-display font-semibold text-[clamp(1.9rem,3vw,2.6rem)] leading-[1.04] tracking-tight mt-3 text-white">
            Run it through your agent, not another dashboard.
          </h2>
          <p class="text-[#b6bdc9] text-lg mt-4">
            Most observability is dashboard-first. Reiver is agent-first: your own coding agent integrates it, operates it, and can recommend how to use it for your app. OpenTelemetry underneath means it works for any stack and you instrument once. Dashboards are there when you want them. The agent is how you live in it.
          </p>
        </div>

        <ul class="flex flex-col gap-4">
          <li v-for="pt in points" :key="pt.title"
              class="p-5 border border-panel-line rounded-xl bg-panel-2">
            <h4 class="font-display font-semibold text-base text-white mb-1">{{ pt.title }}</h4>
            <p class="text-sm text-panel-muted">{{ pt.desc }}</p>
          </li>
        </ul>
      </div>
    </div>
  </section>
</template>

<script setup>
const points = [
  {
    title: 'Agent-native',
    desc: 'Integrate and operate over MCP, from the IDE you already work in.',
  },
  {
    title: 'OpenTelemetry-native',
    desc: 'Any language, any stack, no lock-in. Reuse the instrumentation you have.',
  },
  {
    title: 'A real control plane',
    desc: 'Gateway, observability and prompts in one place, not three tools stitched together.',
  },
];
</script>
