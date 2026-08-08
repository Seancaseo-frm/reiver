<template>
  <div class="min-h-screen bg-paper text-ink antialiased font-body">
    <MarketingNav />
    <main>
      <section class="max-w-[1080px] mx-auto px-6 pt-14 pb-1">
        <span class="font-mono text-xs tracking-[0.14em] uppercase text-accent font-medium">Sub-processors</span>
        <h1 class="font-display font-semibold text-[clamp(2.1rem,4vw,2.95rem)] leading-[1.04] tracking-tight mt-3">
          Sub-processors &amp; BYOK destinations
        </h1>
        <p class="text-lg text-muted mt-4 max-w-[60ch]">
          The vendors Reiver uses to operate the platform, the AI provider Reiver controls for Moodeng, and the customer-selected BYOK destinations available through Flow.
        </p>
        <p class="text-sm text-muted mt-3 max-w-[66ch]">
          Customer-selected BYOK destinations are not Reiver sub-processors unless accessed through a Reiver-controlled account, Reiver-managed credits, or a native Reiver feature. Private Early Access; this list may change.
        </p>
      </section>

      <div class="max-w-[1080px] mx-auto px-6">
        <div class="max-w-[780px] mx-auto">
          <!-- Infrastructure vendors -->
          <section class="py-9 pt-2">
            <h2 class="font-display font-semibold text-xl">1. Reiver infrastructure and operations vendors</h2>
            <p class="text-[#3f4651] text-base mt-3">Vendors Reiver uses to run and operate the platform.</p>
            <div class="overflow-x-auto mt-4">
              <table class="w-full border-collapse text-sm min-w-[640px]">
                <thead>
                  <tr>
                    <th class="text-left p-2.5 font-mono text-[0.68rem] tracking-[0.06em] uppercase text-faint font-medium border-b border-line">Provider</th>
                    <th class="text-left p-2.5 font-mono text-[0.68rem] tracking-[0.06em] uppercase text-faint font-medium border-b border-line">Purpose</th>
                    <th class="text-left p-2.5 font-mono text-[0.68rem] tracking-[0.06em] uppercase text-faint font-medium border-b border-line">Data processed</th>
                    <th class="text-left p-2.5 font-mono text-[0.68rem] tracking-[0.06em] uppercase text-faint font-medium border-b border-line">Location / notes</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="row in infraVendors" :key="row.provider">
                    <th class="text-left p-2.5 font-semibold text-ink border-b border-line whitespace-nowrap">{{ row.provider }}</th>
                    <td class="p-2.5 text-[#3f4651] border-b border-line">{{ row.purpose }}</td>
                    <td class="p-2.5 text-[#3f4651] border-b border-line">{{ row.data }}</td>
                    <td class="p-2.5 text-[#3f4651] border-b border-line">{{ row.location }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </section>

          <!-- Reiver-controlled AI -->
          <section class="py-9 border-t border-line">
            <h2 class="font-display font-semibold text-xl">2. Reiver-controlled AI providers</h2>
            <p class="text-[#3f4651] text-base mt-3">Where Reiver uses its own provider account for a native feature.</p>
            <div class="overflow-x-auto mt-4">
              <table class="w-full border-collapse text-sm min-w-[640px]">
                <thead>
                  <tr>
                    <th class="text-left p-2.5 font-mono text-[0.68rem] tracking-[0.06em] uppercase text-faint font-medium border-b border-line">Provider</th>
                    <th class="text-left p-2.5 font-mono text-[0.68rem] tracking-[0.06em] uppercase text-faint font-medium border-b border-line">Purpose</th>
                    <th class="text-left p-2.5 font-mono text-[0.68rem] tracking-[0.06em] uppercase text-faint font-medium border-b border-line">Data processed</th>
                    <th class="text-left p-2.5 font-mono text-[0.68rem] tracking-[0.06em] uppercase text-faint font-medium border-b border-line">Location / notes</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <th class="text-left p-2.5 font-semibold text-ink border-b border-line">DeepSeek</th>
                    <td class="p-2.5 text-[#3f4651] border-b border-line">Moodeng/native assistant</td>
                    <td class="p-2.5 text-[#3f4651] border-b border-line">Customer Data sent to Moodeng according to permissions/configuration</td>
                    <td class="p-2.5 text-[#3f4651] border-b border-line">12F, Galaxy International Building, Gongshu Dist, Hangzhou 310003, Zhejiang, China</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </section>

          <!-- BYOK destinations -->
          <section class="py-9 border-t border-line">
            <h2 class="font-display font-semibold text-xl">3. Customer-selected BYOK destinations</h2>
            <p class="text-[#3f4651] text-base mt-3">Connected, contracted and paid for by you. Not Reiver sub-processors unless accessed through a Reiver-controlled account, Reiver-managed credits, or a native feature.</p>
            <ul class="flex flex-wrap gap-2 mt-3">
              <li v-for="dest in byokDestinations" :key="dest"
                  class="font-mono text-xs text-[#454c58] bg-white border border-line rounded-full px-3 py-1">
                {{ dest }}
              </li>
            </ul>
          </section>

          <!-- Conditional integrations -->
          <section class="py-9 border-t border-line">
            <h2 class="font-display font-semibold text-xl">4. Conditional integrations</h2>
            <p class="text-[#3f4651] text-base mt-3">Used only where you enable them.</p>
            <ul class="flex flex-wrap gap-2 mt-3">
              <li v-for="int in conditionalIntegrations" :key="int"
                  class="font-mono text-xs text-[#454c58] bg-white border border-line rounded-full px-3 py-1">
                {{ int }}
              </li>
            </ul>
          </section>

          <!-- Self-hosted software -->
          <section class="py-9 border-t border-line">
            <h2 class="font-display font-semibold text-xl">5. Self-hosted software</h2>
            <p class="text-[#3f4651] text-base mt-3">These are self-hosted software components Reiver runs, not third-party sub-processors by themselves.</p>
            <ul class="flex flex-wrap gap-2 mt-3">
              <li v-for="sw in selfHosted" :key="sw"
                  class="font-mono text-xs text-[#454c58] bg-white border border-line rounded-full px-3 py-1">
                {{ sw }}
              </li>
            </ul>
            <p class="text-[#3f4651] text-sm mt-4">
              Google Workspace may be used for internal business operations, communications, documents, calendar and support. Gemini for Google Workspace may be used only where enabled in our internal workspace settings; it is not a Flow provider unless product traffic is actually routed there through Flow.
            </p>
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

const infraVendors = [
  { provider: 'Hetzner Cloud', purpose: 'Hosting / compute', data: 'Platform and customer data', location: 'Finland' },
  { provider: 'Cloudflare R2', purpose: 'Object storage, Parquet warehouse, ClickHouse cold tier', data: 'Customer data where configured', location: 'Eastern Europe (EEUR) location hint' },
  { provider: 'Stripe', purpose: 'Billing and payments', data: 'Billing/payment data', location: 'As described by Stripe terms' },
  { provider: 'Loops.so', purpose: 'Transactional email', data: 'Invites, alerts, onboarding, service email data', location: 'As described by Loops terms' },
  { provider: 'Google Workspace', purpose: 'Internal operations, comms, docs, calendar, support', data: 'Business operations data', location: 'Data-region and Gemini settings depend on internal configuration' },
  { provider: 'GitHub / ghcr.io', purpose: 'Source control and container registry', data: 'Source/container artifacts', location: 'As described by GitHub terms' },
  { provider: "Let's Encrypt", purpose: 'TLS certificates', data: 'Certificate/domain data', location: 'Global certificate authority infrastructure' },
];

const byokDestinations = [
  'OpenAI', 'Anthropic', 'Google Gemini', 'Google Vertex AI', 'AWS Bedrock', 'Azure OpenAI',
  'Theta EdgeCloud', 'Theta Dedicated', 'DeepSeek', 'xAI / Grok', 'Mistral AI', 'Groq',
  'Together AI', 'Fireworks AI', 'Perplexity', 'Cohere', 'OpenRouter', 'Cerebras',
  'DeepInfra', 'Alibaba Cloud / Qwen', 'NVIDIA NIM', 'AI21', 'SambaNova', 'Lambda',
  'Lepton AI', 'Hyperbolic', 'OVHcloud AI', 'Novita AI', 'Hugging Face', 'Cloudflare Workers AI',
];

const conditionalIntegrations = [
  'Google / GitHub / Microsoft OAuth', 'Slack OAuth', 'Calendly', 'Customer SSO IdPs',
  'PagerDuty', 'Discord', 'Teams', 'ServiceNow', 'Webhooks',
  'Customer data connectors and cloud integrations',
];

const selfHosted = [
  'PostgreSQL', 'ClickHouse', 'Redis', 'Redpanda', 'Traefik',
  'cert-manager', 'Argo CD', 'Drone CI', 'Jaeger', 'OpenTelemetry Collector',
];
</script>
