<template>
  <div class="min-h-screen bg-paper text-ink antialiased font-body">
    <MarketingNav />
    <main>
      <section class="max-w-[1080px] mx-auto px-6 pt-14 pb-10">
        <span class="font-mono text-xs tracking-[0.14em] uppercase text-accent font-medium">Start here</span>
        <h1 class="font-display font-semibold text-[clamp(2.1rem,4vw,2.95rem)] leading-[1.04] tracking-tight mt-3 max-w-[18ch]">
          Choose the part of Reiver you need first.
        </h1>
        <p class="text-lg text-muted mt-4 max-w-[62ch]">
          Flow and Watch work independently. Start with one track, or connect both when you want to understand an AI request from the customer action through the model response.
        </p>
      </section>

      <section class="max-w-[1080px] mx-auto px-6 pb-10">
        <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
          <article v-for="track in tracks" :key="track.title" class="bg-white border border-line rounded-xl p-6">
            <span class="font-mono text-[0.68rem] tracking-[0.08em] uppercase text-accent">{{ track.tag }}</span>
            <h2 class="font-display font-semibold text-xl mt-2">{{ track.title }}</h2>
            <p class="text-muted text-sm leading-relaxed mt-2">{{ track.description }}</p>
            <p class="text-sm text-[#3f4651] mt-4"><strong>Done when:</strong> {{ track.done }}</p>
            <a :href="track.href" class="inline-flex mt-5 text-sm font-medium text-accent hover:underline">Open this track →</a>
          </article>
        </div>
      </section>

      <section class="max-w-[1080px] mx-auto px-6 py-9 border-t border-line">
        <div class="max-w-[760px]">
          <h2 class="font-display font-semibold text-2xl">Credentials have separate jobs</h2>
          <div class="grid grid-cols-1 sm:grid-cols-3 gap-4 mt-5">
            <div v-for="credential in credentials" :key="credential.title" class="bg-white border border-line rounded-xl p-5">
              <h3 class="font-display font-semibold">{{ credential.title }}</h3>
              <p class="text-muted text-sm mt-2">{{ credential.description }}</p>
            </div>
          </div>
          <p class="text-sm text-[#5a4a33] bg-[#FCF3E9] border border-[#F1DCC4] rounded-xl p-4 mt-4">
            These credentials are not interchangeable. Keep every credential in a secret store or environment variable—never in code, documentation examples, logs, or reports.
          </p>
        </div>
      </section>

      <section class="max-w-[1080px] mx-auto px-6 py-9 border-t border-line">
        <div class="max-w-[760px]">
          <h2 class="font-display font-semibold text-2xl">Agree what a session means</h2>
          <p class="text-[#3f4651] mt-3 leading-relaxed">
            A session is the smallest meaningful business episode you want to evaluate and improve: for example, one support case, booking attempt, or research task. Before adding IDs, agree its start, successful end, failure and abandonment endings, inactivity fallback, user identity, tenant boundary, and privacy boundary.
          </p>
          <p class="text-[#3f4651] mt-3 leading-relaxed">
            The first accepted Flow request carrying a session ID starts recorded activity. End the session explicitly when the episode finishes; Reiver's inactivity timeout remains fallback protection. A later episode gets a new session ID while the same person keeps the same stable pseudonymous user ID.
          </p>
          <a href="https://docs.reiver.ai/flow/session-telemetry.html" class="inline-flex mt-4 text-sm font-medium text-accent hover:underline">Read the Session and Identity Contract →</a>
        </div>
      </section>
    </main>
    <MarketingFooter />
  </div>
</template>

<script setup>
import MarketingNav from '../Home/MarketingNav.vue';
import MarketingFooter from '../Home/MarketingFooter.vue';

const tracks = [
  { tag: 'Track 1', title: 'Flow + Prompt Hub', description: 'Route an application model request through Reiver and, if useful, manage its prompt centrally. Watch, logs, metrics, and MCP are not required.', done: 'a real application request succeeds through Flow and the selected provider and model are visible.', href: 'https://docs.reiver.ai/flow/getting-started.html' },
  { tag: 'Track 2', title: 'Watch', description: 'Send application traces, structured logs, and metrics through three real OpenTelemetry pipelines. No provider key, gateway route, or MCP write access is required.', done: 'one known trace, structured log, and metric can each be found under the intended service.', href: 'https://docs.reiver.ai/watch/' },
  { tag: 'Track 3', title: 'Complete Reiver', description: 'Combine Flow, Watch, session and user correlation, and agent-assisted read-only verification of the evidence Reiver currently exposes.', done: 'two business sessions are correlated across Flow and Watch, with a new session ID and the same pseudonymous user ID.', href: 'https://docs.reiver.ai/quickstart.html' },
];

const credentials = [
  { title: 'Provider key', description: 'Stays inside Reiver. Flow uses it to call the model provider; your application does not.' },
  { title: 'SDK key', description: 'Used by the application. The same value may currently be bound separately as REIVER_FLOW_API_KEY and REIVER_WATCH_API_KEY.' },
  { title: 'Agent token', description: 'A separate REIVER_AGENT_TOKEN lets a coding agent use MCP within its granted scopes.' },
];
</script>
