<template>
  <section class="py-24" id="pricing" style="padding-top: 8px">
    <div class="max-w-wrap mx-auto px-6">
      <div class="max-w-[60ch]">
        <span class="font-mono text-xs tracking-[0.14em] uppercase text-accent font-medium">Pricing</span>
        <h2 class="font-display font-semibold text-[clamp(1.9rem,3vw,2.7rem)] leading-[1.04] tracking-tight mt-3">
          Start free. Unlimited users on every paid plan.
        </h2>
        <p class="text-lg text-muted mt-4 max-w-[52ch]">
          Bring your own provider keys and route through Reiver. We charge for platform capacity and controls, not a percentage of your model-provider spend.
        </p>
      </div>

      <!-- Tier cards -->
      <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 mt-12 items-stretch">
        <div v-for="tier in tiers" :key="tier.name"
             class="bg-white border rounded-[14px] p-6 flex flex-col"
             :class="tier.featured ? 'border-accent border-[1.5px] shadow-[0_18px_44px_-26px_rgba(176,83,46,0.45)]' : 'border-line'">
          <div class="font-mono text-[0.66rem] tracking-[0.1em] uppercase text-accent mb-2 h-3.5">
            {{ tier.badge }}
          </div>
          <div class="font-display font-semibold text-lg">{{ tier.name }}</div>
          <div class="font-display font-semibold text-3xl mt-3 mb-0.5 tracking-tight">
            {{ tier.price }}
            <span v-if="tier.per" class="font-sans text-sm font-normal text-faint">{{ tier.per }}</span>
          </div>
          <p class="text-sm text-muted leading-snug mt-0.5 mb-3 min-h-[38px]">{{ tier.desc }}</p>
          <ul class="flex flex-col gap-2.5 my-5 flex-1">
            <li v-for="feat in tier.features" :key="feat" class="text-sm text-[#3c434f] flex gap-2 items-start">
              <span class="flex-none w-[15px] h-[15px] mt-0.5 rounded-full bg-accent-soft flex items-center justify-center">
                <svg width="9" height="9" viewBox="0 0 15 15" fill="none"><path d="M4 7.6l2.2 2.2L11 5" stroke="#B0532E" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/></svg>
              </span>
              {{ feat }}
            </li>
          </ul>
        </div>
      </div>

      <p class="font-mono text-xs text-faint text-center mt-6 tracking-wide max-w-[80ch] mx-auto">
        Prices exclude VAT. Flow gateway is included during Early Access, subject to plan limits. No gateway markup and no percentage fee on model-provider spend. Provider charges remain governed by your provider account. Early-access pricing, limits, and included usage may change.
      </p>
      <p class="text-sm text-muted mt-4 max-w-[66ch]">
        Guardrails and PII redaction are assistive controls and may not catch everything.
        <router-link to="/security" class="text-accent hover:underline">Security &amp; Data Handling</router-link>.
      </p>

      <!-- Comparison table -->
      <div class="mt-14">
        <h3 class="font-display font-semibold text-2xl tracking-tight">Compare every plan</h3>
        <p class="text-muted text-base mt-1 mb-5">Every limit and feature, including observability data, retention and overage.</p>
        <div class="overflow-x-auto border border-line rounded-[14px]">
          <table class="w-full min-w-[780px] border-collapse bg-white text-sm">
            <thead>
              <tr>
                <th class="text-left p-3.5 font-display font-semibold text-base text-ink border-b border-line bg-white"></th>
                <th class="text-left p-3.5 font-display font-semibold text-base text-ink border-b border-line bg-white">
                  Free<span class="block font-mono font-normal text-xs text-muted mt-1">€0</span>
                </th>
                <th class="text-left p-3.5 font-display font-semibold text-base text-ink border-b border-line bg-white">
                  Starter<span class="block font-mono font-normal text-xs text-muted mt-1">€49 / mo</span>
                </th>
                <th class="text-left p-3.5 font-display font-semibold text-base text-ink border-b border-line bg-[#FBF1EB]">
                  Scale
                  <span class="block font-mono font-medium text-[0.6rem] tracking-[0.1em] uppercase text-accent mt-1">Recommended</span>
                  <span class="block font-mono font-normal text-xs text-muted mt-0.5">€299 / mo</span>
                </th>
                <th class="text-left p-3.5 font-display font-semibold text-base text-ink border-b border-line bg-white">
                  Enterprise<span class="block font-mono font-normal text-xs text-muted mt-1">From €3,000 / mo</span>
                </th>
              </tr>
            </thead>
            <tbody>
              <template v-for="group in comparisonData" :key="group.label">
                <tr class="bg-paper">
                  <td colspan="5" class="font-mono font-medium text-[0.7rem] tracking-[0.1em] uppercase text-accent p-3.5 border-b border-line">
                    {{ group.label }}
                  </td>
                </tr>
                <tr v-for="row in group.rows" :key="row.feature">
                  <th class="text-left p-3.5 font-medium text-[#3a414d] w-[34%] border-b border-line-2 whitespace-nowrap">{{ row.feature }}</th>
                  <td class="p-3.5 text-[#4b525e] border-b border-line-2">{{ row.free }}</td>
                  <td class="p-3.5 text-[#4b525e] border-b border-line-2">{{ row.starter }}</td>
                  <td class="p-3.5 text-[#4b525e] border-b border-line-2 bg-[#FBF1EB]">{{ row.scale }}</td>
                  <td class="p-3.5 text-[#4b525e] border-b border-line-2">{{ row.enterprise }}</td>
                </tr>
              </template>
            </tbody>
          </table>
        </div>

        <div class="mt-4 grid gap-2">
          <p class="font-mono text-xs leading-relaxed text-faint">Reiver plan limits apply to gateway requests, observability data, Moodeng credits, retention, MCP access, audit logs, support, and other platform features. Provider invoices, billing dashboards, usage limits, and charges remain governed by your provider account.</p>
          <p class="font-mono text-xs leading-relaxed text-faint">Moodeng credits are Early Access usage units for native assistant requests and tool-assisted actions. Credit consumption may vary by operation and may change as the feature evolves.</p>
          <p class="font-mono text-xs leading-relaxed text-faint">Cost figures are estimates for operational visibility. Provider invoices and billing dashboards remain governed by your provider account.</p>
        </div>
      </div>
    </div>
  </section>
</template>

<script setup>
const tiers = [
  {
    name: 'Free',
    badge: '',
    price: '€0',
    per: null,
    desc: 'Start routing and tracing small projects.',
    featured: false,
    features: [
      '2 users · 1 project',
      '50k gateway requests included',
      '100 Moodeng credits included',
      '25GB observability included · 14-day retention',
      'Canary + A/B testing',
      'MCP, read-only',
      'Community support',
    ],
  },
  {
    name: 'Starter',
    badge: '',
    price: '€49',
    per: '/ mo',
    desc: 'For indie developers and small production apps.',
    featured: false,
    features: [
      'Unlimited users · 3 projects',
      '250k gateway requests included',
      '1,000 Moodeng credits included',
      '200GB observability included · 30-day retention',
      'Basic PII redactions · audit logs',
      'Project-scoped MCP',
      'Email support',
    ],
  },
  {
    name: 'Scale',
    badge: 'Recommended',
    price: '€299',
    per: '/ mo',
    desc: 'For serious production AI teams.',
    featured: true,
    features: [
      'Unlimited users · 10 projects',
      '2M gateway requests included',
      '20,000 Moodeng credits included',
      '1.5TB observability included · 90-day retention',
      'Prompt-injection controls',
      'Full MCP access',
      'Priority support',
    ],
  },
  {
    name: 'Enterprise',
    badge: '',
    price: 'from €3k',
    per: '/ mo',
    desc: 'Custom usage, controls, support, deployment, and retention.',
    featured: false,
    features: [
      'Unlimited users · custom limits',
      'Custom gateway volume',
      'Custom Moodeng rates',
      'Custom observability + retention',
      'Enterprise guardrail policies',
      'Enterprise MCP controls',
      'Cloud or on-prem from €8k / mo',
      'Dedicated support',
    ],
  },
];

const comparisonData = [
  {
    label: 'Plans & limits',
    rows: [
      { feature: 'Users', free: '2', starter: 'Unlimited', scale: 'Unlimited', enterprise: 'Unlimited' },
      { feature: 'Projects', free: '1', starter: '3', scale: '10', enterprise: 'Custom' },
    ],
  },
  {
    label: 'Gateway & routing',
    rows: [
      { feature: 'Gateway requests included', free: '50,000 / mo', starter: '250,000 / mo', scale: '2,000,000 / mo', enterprise: 'Custom' },
      { feature: 'Gateway request caps', free: 'Upgrade required', starter: 'Upgrade or by agreement', scale: 'Upgrade or by agreement', enterprise: 'Custom' },
      { feature: 'Model routing', free: 'Yes', starter: 'Yes', scale: 'Yes', enterprise: 'Yes' },
      { feature: 'Provider fallback', free: 'No', starter: '1 fallback rule', scale: 'Multiple fallback', enterprise: 'Custom' },
    ],
  },
  {
    label: 'Prompts & rollouts',
    rows: [
      { feature: 'Active prompts', free: '10', starter: '50', scale: '500', enterprise: 'Custom' },
      { feature: 'Versions per prompt', free: '5', starter: '25', scale: '100', enterprise: 'Custom' },
      { feature: 'Prompt versioning', free: 'Yes', starter: 'Yes', scale: 'Yes', enterprise: 'Yes' },
      { feature: 'Manual rollouts', free: '1 active', starter: '3 active', scale: '20 active', enterprise: 'Custom' },
      { feature: 'Canary releases', free: 'Yes', starter: 'Yes', scale: 'Yes', enterprise: 'Yes' },
      { feature: 'A/B testing', free: 'Yes', starter: 'Yes', scale: 'Yes', enterprise: 'Yes' },
    ],
  },
  {
    label: 'Moodeng, the agent',
    rows: [
      { feature: 'Moodeng credits included', free: '100', starter: '1,000', scale: '20,000', enterprise: 'Custom' },
      { feature: 'Additional Moodeng usage', free: 'No overages', starter: 'Usage-based', scale: 'Usage-based', enterprise: 'Custom rates' },
      { feature: 'MCP access', free: 'Read only', starter: 'Project scoped', scale: 'Full MCP', enterprise: 'Enterprise controls' },
    ],
  },
  {
    label: 'Observability',
    rows: [
      { feature: 'Observability data included', free: '25 GB / mo', starter: '200 GB / mo', scale: '1,500 GB / mo', enterprise: 'Custom' },
      { feature: 'Observability retention', free: '14 days', starter: '30 days', scale: '90 days', enterprise: 'Custom' },
      { feature: 'Traces & logs overage', free: 'No overages', starter: '€0.20 / GB', scale: '€0.20 / GB', enterprise: 'Negotiated' },
      { feature: 'Metrics overage', free: 'No overages', starter: '€0.10 / M', scale: '€0.10 / M', enterprise: 'Negotiated' },
      { feature: 'Session labels', free: '5', starter: '15', scale: '25', enterprise: 'Custom' },
      { feature: 'Session profiles', free: '10 types', starter: '25 types', scale: '75 types', enterprise: 'Custom' },
      { feature: 'Activity history', free: 'Full', starter: 'Full', scale: 'Full', enterprise: 'Full' },
    ],
  },
  {
    label: 'Security & support',
    rows: [
      { feature: 'Guardrails', free: 'None', starter: 'Basic PII redactions', scale: 'Previous + prompt injection + more', enterprise: 'Enterprise policies' },
      { feature: 'Customer-visible audit logs', free: 'No', starter: 'Yes', scale: 'Yes', enterprise: 'Yes' },
      { feature: 'Support', free: 'Community', starter: 'Email', scale: 'Priority', enterprise: 'Dedicated' },
    ],
  },
];
</script>
