<template>
  <section class="pt-20 lg:pt-[84px] pb-16">
    <div class="max-w-wrap mx-auto px-6 grid grid-cols-1 lg:grid-cols-[1.02fr_1.18fr] gap-10 lg:gap-14 items-center">
      <div>
        <span class="font-mono text-xs tracking-[0.14em] uppercase text-accent font-medium">Production AI control plane</span>
        <h1 class="font-display font-semibold text-[clamp(2.5rem,4.6vw,3.9rem)] leading-[1.04] tracking-tight mt-4">
          Observability your coding agent sets up for you.
        </h1>
        <p class="text-lg text-muted mt-5 max-w-[34ch]">
          Reiver is a production AI control plane - OpenTelemetry-native observability, LLM gateway, and prompt hub in one. Point your coding agent at the docs, give it a scoped Reiver token, and it instruments your app and runs it all through MCP. No platform team required.
        </p>
        <div class="flex gap-3 mt-7 flex-wrap">
          <a href="https://docs.reiver.ai/" class="inline-flex items-center gap-2 font-medium text-sm px-4 py-2.5 rounded-[10px] bg-accent text-white border border-accent hover:bg-accent-dark hover:-translate-y-px transition-all">
            Read the docs
          </a>
          <a href="#how" class="inline-flex items-center gap-2 font-medium text-sm px-4 py-2.5 rounded-[10px] bg-transparent text-ink border border-line hover:border-ink hover:bg-white transition-all">
            See how it works
          </a>
        </div>
        <p class="font-mono text-xs text-faint mt-5 tracking-wide">
          Works with OpenAI · Anthropic · Gemini · Bedrock · OpenTelemetry · MCP
        </p>
        <p class="text-sm text-muted mt-3">
          Private Early Access. Early Access terms apply.
          <router-link to="/security" class="text-accent hover:underline">More in Security &amp; Data Handling</router-link>.
        </p>
      </div>

      <div class="bg-panel border border-panel-line rounded-2xl overflow-hidden shadow-term" role="img" aria-label="A coding agent reads the Reiver docs, instruments an app, and traces begin streaming in.">
        <div class="flex items-center gap-2 px-4 py-3 border-b border-panel-line bg-panel-2">
          <span class="w-[11px] h-[11px] rounded-full bg-[#333a45]"></span>
          <span class="w-[11px] h-[11px] rounded-full bg-[#333a45]"></span>
          <span class="w-[11px] h-[11px] rounded-full bg-[#333a45]"></span>
          <span class="font-mono text-xs text-panel-muted ml-2">agent session · reiver</span>
        </div>
        <div class="p-5 font-mono text-sm leading-[1.85] text-panel-fg min-h-[300px]">
          <div class="term-line" style="animation-delay: .05s">
            <span class="text-panel-fg">$ <b class="text-hi font-medium">claude</b>, instrument this app with Reiver.</span>
          </div>
          <div class="term-line" style="animation-delay: .35s">
            <span class="text-[#cfd5de]">&nbsp;&nbsp;here are the docs and a scoped Reiver MCP token.</span>
          </div>
          <div class="term-line" style="animation-delay: .9s">
            <span class="text-panel-muted">⠿ reading docs.reiver.ai ............... done</span>
          </div>
          <div class="term-line" style="animation-delay: 1.3s">
            <span class="text-panel-muted">⠿ detected: Next.js app · OpenAI + Anthropic calls</span>
          </div>
          <div class="term-line" style="animation-delay: 1.75s">
            <span class="text-hi">→ routing LLM calls through Flow (one base URL)</span>
          </div>
          <div class="term-line" style="animation-delay: 2.1s">
            <span class="text-hi">→ sending traces to Watch over OpenTelemetry</span>
          </div>
          <div class="term-line" style="animation-delay: 2.45s">
            <span class="text-hi">→ wiring prompt management + cost tracking</span>
          </div>
          <div class="term-line" style="animation-delay: 2.95s">
            <span class="text-ok">✓ instrumented in 3m 12s</span>
          </div>
          <div class="term-line" style="animation-delay: 3.4s">
            <span class="text-panel-muted"><b class="text-ok font-medium">● trace</b> &nbsp;POST /api/chat &nbsp;&nbsp;1.2s &nbsp;<i class="text-warn not-italic">$0.004</i></span>
          </div>
          <div class="term-line" style="animation-delay: 3.7s">
            <span class="text-panel-muted"><b class="text-ok font-medium">● trace</b> &nbsp;tool_call · search &nbsp;420ms</span>
          </div>
          <div class="term-line" style="animation-delay: 4s">
            <span class="text-panel-muted"><b class="text-ok font-medium">● span</b> &nbsp;&nbsp;llm.completion · gpt-4o &nbsp;812 tok<span class="inline-block w-2 h-[1.05em] bg-ok align-[-2px] ml-1 animate-blink"></span></span>
          </div>
        </div>
      </div>
    </div>
  </section>
</template>

<script setup>
</script>

<style scoped>
.term-line {
  opacity: 0;
  animation: lineIn 0.42s ease forwards;
}
@keyframes lineIn {
  from { opacity: 0; transform: translateY(4px); }
  to { opacity: 1; transform: none; }
}
@keyframes blink {
  50% { opacity: 0; }
}
.animate-blink {
  animation: blink 1.1s steps(1) infinite;
}
</style>
