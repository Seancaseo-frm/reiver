<template>
  <div class="min-h-screen bg-paper text-ink antialiased font-body">
    <MarketingNav />
    <main>

      <!-- Hero -->
      <section class="pt-14 pb-10">
        <div class="max-w-wrap mx-auto px-6">
          <span class="font-mono text-xs tracking-[0.14em] uppercase text-accent font-medium">Compare</span>
          <h1 class="font-display font-semibold text-[clamp(2.1rem,4vw,3.1rem)] leading-[1.04] tracking-tight mt-3">
            Reiver vs Datadog
          </h1>
          <p class="text-lg text-muted mt-4 max-w-[64ch]">
            Datadog LLM Observability is a bolt-on to Datadog's existing platform: it watches your LLM calls from the side. Reiver is a production AI control plane that sits in the request path: gateway, prompt rollout workflow, and a user-triggered investigation agent, priced as one meter instead of two.
          </p>
          <div class="flex gap-3 mt-6 flex-wrap">
            <a href="mailto:hello@reiver.ai?subject=Datadog%20invoice%20teardown"
               class="inline-flex items-center gap-2 font-medium text-sm px-4 py-2.5 rounded-[10px] bg-accent text-white border border-accent hover:bg-accent-dark hover:-translate-y-px transition-all">
              Get a free invoice teardown
            </a>
            <a href="#compare-table"
               class="inline-flex items-center gap-2 font-medium text-sm px-4 py-2.5 rounded-[10px] bg-transparent text-ink border border-line hover:border-ink hover:bg-white transition-all">
              See the comparison
            </a>
          </div>
        </div>
      </section>

      <div class="max-w-wrap mx-auto px-6">

        <!-- Verification banner -->
        <div class="bg-accent-soft border border-accent rounded-[10px] px-4 py-3 font-mono text-xs text-accent-dark">
          All Datadog figures on this page are sourced from Datadog's own published LLM Observability pricing page and were last verified 2026&#8209;07&#8209;05.
        </div>

        <!-- TL;DR verdict -->
        <div class="bg-white border-[1.5px] border-ink rounded-[14px] p-7 mt-7 mb-10 shadow-card">
          <span class="font-mono text-xs tracking-[0.12em] uppercase text-accent font-medium">TL;DR</span>
          <p class="mt-3 text-[1.05rem] leading-relaxed">
            Datadog LLM Observability charges per LLM span above a monthly included quota, on top of Datadog's broader platform pattern of billing ingest and indexing/retention as two separate meters. Reiver is priced as a single control plane: gateway, prompt rollout, and observability together, without a second meter for search and retention.
          </p>
          <p class="mt-2.5 text-[1.05rem] leading-relaxed">
            Depending on workload, Reiver customers who've checked their own Datadog invoice against Reiver have found the gap runs anywhere from 4x to 26x, always verified against the actual bill, never assumed. Send us your invoice and we'll show you the two meters, line by line.
          </p>
        </div>

        <!-- Comparison table -->
        <div id="compare-table">
          <h2 class="font-display font-semibold text-[clamp(1.4rem,2.4vw,1.45rem)] tracking-tight">Datadog vs Reiver, line by line</h2>
          <p class="text-muted text-[0.96rem] mt-1.5 mb-5">
            Every Datadog figure below is sourced from Datadog's published LLM Observability pricing page. Full quotes are in <a href="#sources" class="text-accent hover:underline">Sources</a>.
          </p>
          <div class="overflow-x-auto border border-line rounded-[14px]">
            <table class="w-full min-w-[720px] border-collapse bg-white">
              <thead>
                <tr>
                  <th class="cmp-th"></th>
                  <th class="cmp-th">Datadog</th>
                  <th class="cmp-th !bg-accent-soft">Reiver</th>
                </tr>
              </thead>
              <tbody>
                <tr class="cmp-grp"><td colspan="3">Billing model</td></tr>
                <tr v-for="row in billingRows" :key="row.label">
                  <th class="cmp-td font-medium text-[#3a414d] w-[26%]" scope="row">{{ row.label }}</th>
                  <td class="cmp-td text-[#4b525e]" v-html="row.dd"></td>
                  <td class="cmp-td text-[#4b525e] !bg-accent-soft/30">{{ row.rr }}</td>
                </tr>
                <tr class="cmp-grp"><td colspan="3">Where the product sits</td></tr>
                <tr v-for="row in positionRows" :key="row.label">
                  <th class="cmp-td font-medium text-[#3a414d] w-[26%]" scope="row">{{ row.label }}</th>
                  <td class="cmp-td text-[#4b525e]">{{ row.dd }}</td>
                  <td class="cmp-td text-[#4b525e] !bg-accent-soft/30">{{ row.rr }}</td>
                </tr>
              </tbody>
            </table>
          </div>
          <div class="mt-4 grid gap-2">
            <p class="font-mono text-[0.72rem] leading-snug text-faint">[1]&ndash;[12]: Datadog LLM Observability pricing page, verified 2026-07-05. Exact quotes in Sources below.</p>
            <p class="font-mono text-[0.72rem] leading-snug text-faint">Rows marked "not documented" reflect what's published on Datadog's LLM Observability pricing and docs pages as of the verification date above, not a claim that Datadog lacks the capability anywhere in its broader platform.</p>
          </div>
        </div>

        <!-- Who should pick which -->
        <section class="py-14" id="who">
          <span class="font-mono text-xs tracking-[0.14em] uppercase text-accent font-medium">Who should pick which</span>
          <h2 class="font-display font-semibold text-[clamp(1.7rem,2.8vw,2.3rem)] tracking-tight mt-2">Neither tool is wrong for every team</h2>

          <div class="bg-white border border-line border-l-[3px] border-l-faint rounded-[14px] p-6 mt-5 max-w-[74ch]">
            <h3 class="font-display font-semibold text-lg">Pick Datadog if&hellip;</h3>
            <p class="text-[#3d4450] mt-2.5 leading-relaxed">
              You're already running Datadog across infrastructure, APM, logs, and security, and you want LLM traces in the same dashboards and the same invoice as everything else. Datadog's included 100,000-span monthly tier is genuinely enough for a small pilot, and if you don't need a gateway, a prompt rollout workflow, or an in-path control layer, and you just want traces sitting next to the rest of your observability stack, adding one more Datadog product is operationally simpler than bringing in a new vendor.
            </p>
          </div>

          <div class="bg-white border border-line border-l-[3px] border-l-accent rounded-[14px] p-6 mt-3 max-w-[74ch]">
            <h3 class="font-display font-semibold text-lg">Pick Reiver if&hellip;</h3>
            <p class="text-[#3d4450] mt-2.5 leading-relaxed">
              Your LLM traffic has outgrown "traces bolted onto observability" and you want to manage prompts, ship changes safely, and investigate incidents without switching tools, or without paying twice for the same data. If Datadog's ingest-then-index billing pattern is showing up as separate line items on your bill for the same LLM calls, that's worth checking against your actual invoice.
            </p>
          </div>
        </section>

        <!-- Deep-dive prose -->
        <section class="pb-14 max-w-[74ch]">
          <h3 class="font-display font-semibold text-lg">Why Datadog's LLM Observability bill often has two meters</h3>
          <p class="text-[#3d4450] mt-2.5 mb-3.5 leading-relaxed">
            Datadog's pricing page documents this pattern across its platform, not only in LLM Observability: Log Management bills $0.10 per GB to ingest data, then bills again, separately, to index and retain it, at rates from $0.075 to $0.90 per GB depending on the retention tier and window you choose.<a class="src" href="#src-5">[5]</a><a class="src" href="#src-6">[6]</a><a class="src" href="#src-7">[7]</a> If you archive data instead of indexing it and later need to search it, Datadog charges again to "rehydrate" it: $0.10 per compressed GB scanned, plus your normal indexing rate for whatever the query actually matches.<a class="src" href="#src-8">[8]</a>
          </p>
          <p class="text-[#3d4450] mb-3.5 leading-relaxed">
            APM traces follow the same shape: 150GB of ingested spans are included per host, then $0.10/GB beyond that, but the spans you actually keep and can query are billed again, per million, on a second scale entirely (7-day retention at $1.27/million, 15-day at $1.70/million, 30-day at $2.50/million, billed annually).<a class="src" href="#src-9">[9]</a><a class="src" href="#src-10">[10]</a><a class="src" href="#src-11">[11]</a><a class="src" href="#src-12">[12]</a>
          </p>
          <p class="text-[#3d4450] mb-3.5 leading-relaxed">
            For LLM Observability specifically, Datadog's published pricing includes 100,000 LLM spans per month, with additional spans billed at $3.50 per 10,000 on an annual commitment, rising to $4.20 month-to-month and $5.00 on-demand.<a class="src" href="#src-1">[1]</a><a class="src" href="#src-2">[2]</a><a class="src" href="#src-3">[3]</a><a class="src" href="#src-4">[4]</a> Whether that specific overage is itself split into a separate ingest-versus-index charge isn't broken out on the pricing page. What's documented is that it sits on top of a platform where ingest and index are metered separately everywhere else. That's the bill worth reading twice, and exactly what we'll walk through in a free teardown of your actual invoice.
          </p>

          <h3 class="font-display font-semibold text-lg mt-6">What "in the request path" actually means</h3>
          <p class="text-[#3d4450] mt-2.5 leading-relaxed">
            Competitors observe from the side; Reiver sits in the request path. Watch, Reiver's observability layer, matters less if it's the only thing you have. Flow is the gateway your LLM calls actually run through, Hub is where prompts get versioned and rolled out, and Moodeng is the agent you ask to investigate when something looks wrong. Datadog's LLM Observability product, based on its published pricing and docs, is instrumentation you add to see what already happened, not a layer your requests pass through on the way to the model.
          </p>
        </section>

        <!-- FAQ -->
        <section class="pb-14" id="faq">
          <span class="font-mono text-xs tracking-[0.14em] uppercase text-accent font-medium">FAQ</span>
          <h2 class="font-display font-semibold text-[clamp(1.7rem,2.8vw,2.3rem)] tracking-tight mt-2 mb-5">Common questions</h2>
          <div class="grid gap-2.5">
            <details v-for="(q, i) in faqs" :key="i" class="bg-white border border-line rounded-[10px] px-5">
              <summary class="cursor-pointer py-4 font-display font-semibold text-base flex justify-between items-center gap-3 select-none">
                {{ q.question }}
                <span class="faq-icon font-mono text-accent text-lg shrink-0">+</span>
              </summary>
              <p class="pb-4 text-[#3d4450] text-[0.96rem] leading-relaxed">{{ q.answer }}</p>
            </details>
          </div>
        </section>

        <!-- Sources -->
        <section class="border-t border-line pt-5 pb-14" id="sources">
          <h3 class="font-display font-semibold text-lg">Sources</h3>
          <ol class="mt-3 grid gap-2.5 list-decimal pl-5">
            <li v-for="(s, i) in sources" :key="i" :id="'src-' + (i + 1)" class="text-sm text-muted leading-snug">
              <code class="font-mono text-xs bg-paper px-1 py-0.5 rounded">[{{ i + 1 }}]</code>
              &ldquo;{{ s.quote }}&rdquo; &middot;
              <a :href="s.url" class="text-accent hover:underline">datadoghq.com/pricing</a>, verified {{ s.verified }}.
            </li>
          </ol>
        </section>

      </div>

      <!-- CTA -->
      <section class="text-center py-20">
        <div class="max-w-wrap mx-auto px-6">
          <h2 class="font-display font-semibold text-[clamp(1.9rem,3.2vw,2.6rem)] tracking-tight">Send us your Datadog invoice.</h2>
          <p class="text-muted text-lg mt-4 max-w-[52ch] mx-auto">
            We'll break down exactly where you're paying twice (ingest here, index there) and show you the actual numbers for your workload. No sales call required to get the breakdown.
          </p>
          <div class="flex gap-3 justify-center mt-6 flex-wrap">
            <a href="mailto:hello@reiver.ai?subject=Datadog%20invoice%20teardown"
               class="inline-flex items-center gap-2 font-medium text-sm px-4 py-2.5 rounded-[10px] bg-accent text-white border border-accent hover:bg-accent-dark hover:-translate-y-px transition-all">
              Get your free invoice teardown
            </a>
          </div>
          <p class="text-sm text-muted mt-3.5 max-w-[66ch] mx-auto leading-snug">
            Cost comparisons on this page are illustrative. Only your own Datadog invoice determines your actual savings with Reiver.
          </p>
        </div>
      </section>

    </main>
    <MarketingFooter />
  </div>
</template>

<script setup>
import { onMounted, onUnmounted } from 'vue';
import MarketingNav from '../Home/MarketingNav.vue';
import MarketingFooter from '../Home/MarketingFooter.vue';

let jsonLdScript = null;

onMounted(() => {
  document.title = 'Reiver vs Datadog: Production AI Control Plane vs LLM Observability Add-On';

  const faqSchema = {
    '@context': 'https://schema.org',
    '@type': 'FAQPage',
    mainEntity: faqs.map(q => ({
      '@type': 'Question',
      name: q.question,
      acceptedAnswer: { '@type': 'Answer', text: q.answer },
    })),
  };
  jsonLdScript = document.createElement('script');
  jsonLdScript.type = 'application/ld+json';
  jsonLdScript.textContent = JSON.stringify(faqSchema);
  document.head.appendChild(jsonLdScript);
});

onUnmounted(() => {
  if (jsonLdScript && jsonLdScript.parentNode) {
    jsonLdScript.parentNode.removeChild(jsonLdScript);
  }
});

const billingRows = [
  {
    label: 'LLM spans included',
    dd: '100,000 LLM spans/month included, then billed per span<a class="src" href="#src-1">[1]</a>',
    rr: 'No monthly span cap to hit; spans aren\'t a separately billed unit',
  },
  {
    label: 'LLM span overage rate',
    dd: '$3.50 per 10,000 spans (annual)<a class="src" href="#src-2">[2]</a> · $4.20 (month\u2011to\u2011month)<a class="src" href="#src-3">[3]</a> · $5.00 (on\u2011demand)<a class="src" href="#src-4">[4]</a>',
    rr: 'N/A; platform capacity, not per-span metering',
  },
  {
    label: 'Log ingest',
    dd: '$0.10 per GB ingested for processing<a class="src" href="#src-5">[5]</a>',
    rr: 'Included in platform capacity',
  },
  {
    label: 'Log indexing / retention',
    dd: 'Billed separately from ingest, tiered by retention window: $0.075/GB on-demand for 1\u201315 month retention<a class="src" href="#src-6">[6]</a>, or $0.90/GB on-demand for the extended 3\u201315 month tier<a class="src" href="#src-7">[7]</a>',
    rr: 'One meter; indexing isn\'t a second line item',
  },
  {
    label: 'Searching archived (non-indexed) data',
    dd: 'Rehydration required: $0.10 per compressed GB scanned, plus your normal indexing rate for anything it matches<a class="src" href="#src-8">[8]</a>',
    rr: 'Nothing to rehydrate; your data is already searchable',
  },
  {
    label: 'Trace ingest & retention (platform-wide pattern)',
    dd: '150GB of ingested spans included per APM host, then $0.10/GB<a class="src" href="#src-9">[9]</a>; indexed/retained spans billed again per million by retention window: 7-day $1.27M<a class="src" href="#src-10">[10]</a>, 15-day $1.70M<a class="src" href="#src-11">[11]</a>, 30-day $2.50M<a class="src" href="#src-12">[12]</a> (annual)',
    rr: 'Same principle applies across Watch; no separate ingest/index bill',
  },
];

const positionRows = [
  {
    label: 'Position relative to the LLM request',
    dd: 'Not documented as sitting in the LLM request path on Datadog\'s published LLM Observability pricing or docs pages (checked 2026\u201107\u201105)',
    rr: 'In the request path via the Flow gateway: one base URL in front of your model calls',
  },
  {
    label: 'Prompt versioning & rollout workflow',
    dd: 'Not part of Datadog\'s published LLM Observability feature set (checked 2026\u201107\u201105)',
    rr: 'Hub versions every prompt; manual rollouts, canary releases, and A/B testing built in',
  },
  {
    label: 'Incident investigation',
    dd: 'Not part of Datadog\'s published LLM Observability feature set (checked 2026\u201107\u201105)',
    rr: 'Moodeng: you ask it to investigate; user-triggered and agent-assisted, not autonomous',
  },
];

const faqs = [
  {
    question: 'Is Reiver cheaper than Datadog for LLM observability?',
    answer: 'It depends on your workload; there\'s no single number that\'s honest for every account. Teams who\'ve checked their own Datadog invoice against Reiver have found the gap runs from 4x to 26x, but that range only means something once it\'s verified against your actual bill. Send us yours and we\'ll show you the math.',
  },
  {
    question: 'Does Datadog really bill you twice for the same LLM data?',
    answer: 'Datadog\'s own pricing page shows ingest and indexing/retention billed as two separate meters across its platform: $0.10 per GB to ingest logs, then $0.075 to $0.90 per GB (depending on retention) to index and keep them searchable, plus a separate charge if you ever need to rehydrate archived data to query it. For LLM Observability specifically, Datadog includes 100,000 LLM spans a month and then meters overage at $3.50\u2013$5.00 per 10,000 spans depending on commitment term. Whether that overage itself splits into ingest and index isn\'t stated on the pricing page, but it sits on a platform where that split is the norm.',
  },
  {
    question: 'What\'s the actual difference between Reiver and Datadog LLM Observability?',
    answer: 'Datadog LLM Observability is instrumentation added to an existing observability platform: it watches your LLM calls after they happen. Reiver is a production AI control plane that sits in the request path: a gateway your calls run through, a prompt hub with versioning and rollout workflow, and Moodeng, an agent you can ask to investigate. Competitors observe from the side; Reiver sits in the request path.',
  },
  {
    question: 'Is Moodeng an autonomous agent that runs my LLM ops for me?',
    answer: 'No. Moodeng is user-triggered and agent-assisted: you ask it to investigate a specific incident or question, and it helps you get to an answer. It doesn\'t monitor, auto-detect, or auto-roll-back anything on its own.',
  },
  {
    question: 'Can I use Reiver alongside Datadog?',
    answer: 'Yes. Plenty of teams keep Datadog for infrastructure, RUM, and general APM while routing LLM traffic through Reiver\'s gateway and observability for the control-plane pieces Datadog\'s LLM Observability doesn\'t cover: prompt rollout, in-path routing, and user-triggered investigation.',
  },
  {
    question: 'When does it make more sense to just stick with Datadog?',
    answer: 'If you\'re already all-in on Datadog and want LLM traces in the same dashboards and the same invoice as everything else, and you don\'t need a gateway, prompt rollout workflow, or in-path control, adding Datadog\'s LLM Observability product is the path of least resistance, especially for a small pilot that fits inside their included 100,000-span tier.',
  },
];

const sources = [
  { quote: 'per month100K LLM spans per month included. Additional spans: $3.5 per 10K LLM spans.', url: 'https://www.datadoghq.com/pricing/?product=llm-observability#llm-observability', verified: '2026-07-05' },
  { quote: '$3.5 per 10K LLM spans (billed annually)', url: 'https://www.datadoghq.com/pricing/?product=llm-observability#llm-observability', verified: '2026-07-05' },
  { quote: '$4.20 per 10K LLM spans (billed month-to-month)', url: 'https://www.datadoghq.com/pricing/?product=llm-observability#llm-observability', verified: '2026-07-05' },
  { quote: '$5.00 per 10K LLM spans (billed on-demand)', url: 'https://www.datadoghq.com/pricing/?product=llm-observability#llm-observability', verified: '2026-07-05' },
  { quote: 'Per GB of uncompressed data ingested for processing, or compressed data scanned for rehydrating. Billed annually or $0.10 on-demand', url: 'https://www.datadoghq.com/pricing/?product=llm-observability#llm-observability', verified: '2026-07-05' },
  { quote: 'Minimum retention of 1 month up to 15 months. Billed annually or $0.075 on-demand', url: 'https://www.datadoghq.com/pricing/?product=llm-observability#llm-observability', verified: '2026-07-05' },
  { quote: 'Minimum retention of 3 months, options for 6, 12, and 15 months. Billed annually or $0.90 on-demand', url: 'https://www.datadoghq.com/pricing/?product=llm-observability#llm-observability', verified: '2026-07-05' },
  { quote: 'When you rehydrate logs, Datadog scans the compressed logs in your archive for the time period you requested, and then indexes only log events that match your rehydration query. Datadog charges $0.10 per compressed GB of log data that is scanned. For any log events indexed from a rehydration, the cost is equal to your contracted indexing rates for the retention period selected for the rehydration.', url: 'https://www.datadoghq.com/pricing/?product=llm-observability#llm-observability', verified: '2026-07-05' },
  { quote: 'While we include 150GB per APM host by default for Ingested Spans, you can always ingest additional traces and spans for 15-minute live trace search and analytics at $0.10/GB.', url: 'https://www.datadoghq.com/pricing/?product=llm-observability#llm-observability', verified: '2026-07-05' },
  { quote: '7-day retention at $1.27 per million spans per month (billed annually or $1.91 on-demand)', url: 'https://www.datadoghq.com/pricing/?product=llm-observability#llm-observability', verified: '2026-07-05' },
  { quote: '15-day retention at $1.70 per million spans per month (billed annually or $2.55 on-demand)', url: 'https://www.datadoghq.com/pricing/?product=llm-observability#llm-observability', verified: '2026-07-05' },
  { quote: '30-day retention at $2.50 per million spans per month (billed annually or or $3.75 on-demand)', url: 'https://www.datadoghq.com/pricing/?product=llm-observability#llm-observability', verified: '2026-07-05' },
];
</script>

<style scoped>
.cmp-th {
  padding: 13px 18px;
  text-align: left;
  border-bottom: 1px solid theme('colors.line');
  font-family: theme('fontFamily.display');
  font-weight: 600;
  font-size: 1.02rem;
  color: theme('colors.ink');
  background: white;
}
.cmp-td {
  padding: 13px 18px;
  text-align: left;
  border-bottom: 1px solid theme('colors.line-2');
  font-size: 0.87rem;
  vertical-align: top;
}
.cmp-grp td {
  background: theme('colors.paper');
  font-family: theme('fontFamily.mono');
  font-weight: 500;
  font-size: 0.7rem;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: theme('colors.accent.DEFAULT');
  border-bottom: 1px solid theme('colors.line');
}
:deep(.src) {
  font-family: theme('fontFamily.mono');
  font-size: 0.68rem;
  color: theme('colors.accent.DEFAULT');
  text-decoration: none;
  vertical-align: super;
  margin-left: 2px;
}
:deep(.src:hover) {
  text-decoration: underline;
}
details summary::-webkit-details-marker { display: none; }
details summary { list-style: none; }
details[open] .faq-icon { content: '\2013'; }
details[open] .faq-icon::before { content: '\2013'; }
details:not([open]) .faq-icon::before { content: '+'; }
.faq-icon { font-size: 0; }

@media (max-width: 620px) {
  .cmp-th, .cmp-td { padding: 11px 14px; font-size: 0.82rem; }
}
</style>
