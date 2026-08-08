<template>
  <section class="border-y border-line bg-white">
    <div class="max-w-wrap mx-auto px-6 py-5 flex items-center gap-8 flex-wrap">
      <span class="font-mono text-xs tracking-[0.1em] uppercase text-faint">Built on open standards</span>
      <div class="flex items-center gap-7 flex-wrap">
        <span class="font-display font-medium text-base text-[#363d49] opacity-85">OpenAI</span>
        <span class="font-display font-medium text-base text-[#363d49] opacity-85">Anthropic</span>
        <span class="font-display font-medium text-base text-[#363d49] opacity-85">Gemini</span>
        <span class="font-display font-medium text-base text-[#363d49] opacity-85">Bedrock</span>
        <span class="font-display font-medium text-base text-[#363d49] opacity-85">OpenTelemetry</span>
        <span class="font-display font-medium text-base text-[#363d49] opacity-85">MCP</span>
      </div>
    </div>
    <div class="max-w-wrap mx-auto px-6 pb-4">
      <p class="text-sm text-muted max-w-[66ch]">
        The providers above are customer-selected BYOK destinations (model names are examples); their terms, billing, regions and data handling are governed by your provider account, not by Reiver.
        <router-link to="/security" class="text-accent hover:underline">More in Security &amp; Data Handling</router-link>.
      </p>
    </div>
  </section>
</template>
