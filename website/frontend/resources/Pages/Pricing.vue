<template>
  <div class="min-h-screen bg-gradient-to-b from-slate-900 via-slate-800 to-slate-900">
    <MarketingNav />

    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
      <div class="text-center mb-10">
        <h1 class="text-4xl font-bold text-white mb-3">Model Catalog & Pricing</h1>
        <p class="text-lg text-slate-400 max-w-2xl mx-auto">
          Transparent pricing, real-time latency, and security stats for every model routed through the Reiver AI Gateway.
          All stats are aggregated from live platform traffic over the last 24 hours.
        </p>
      </div>

      <div class="mb-6">
        <input
          v-model="search"
          type="text"
          placeholder="Search by provider or model name..."
          class="w-full max-w-md mx-auto block bg-slate-800 border border-slate-600 text-slate-200 rounded-lg px-4 py-2.5 placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-brand-500 focus:border-transparent"
        />
      </div>

      <div v-if="loading" class="text-center py-20">
        <div class="inline-block w-8 h-8 border-4 border-slate-600 border-t-brand-500 rounded-full animate-spin"></div>
        <p class="mt-3 text-slate-400">Loading model catalog...</p>
      </div>

      <div v-else-if="error" class="text-center py-20">
        <p class="text-red-400">{{ error }}</p>
      </div>

      <div v-else>
        <div class="overflow-x-auto">
          <table class="w-full text-sm text-left">
            <thead>
              <tr class="text-slate-500 border-b border-slate-700/50 text-xs uppercase tracking-wider">
                <th class="px-4 py-2.5 font-medium">
                  <button @click="toggleSort('provider')" class="inline-flex items-center gap-1 hover:text-white transition-colors">
                    Provider
                    <span v-if="sortKey === 'provider'" class="text-brand-400">{{ sortAsc ? '&#9650;' : '&#9660;' }}</span>
                  </button>
                </th>
                <th class="px-4 py-2.5 font-medium">
                  <button @click="toggleSort('name')" class="inline-flex items-center gap-1 hover:text-white transition-colors">
                    Model
                    <span v-if="sortKey === 'name'" class="text-brand-400">{{ sortAsc ? '&#9650;' : '&#9660;' }}</span>
                  </button>
                </th>
                <th class="px-4 py-2.5 font-medium text-right">
                  <button @click="toggleSort('context_length')" class="inline-flex items-center gap-1 ml-auto hover:text-white transition-colors">
                    Context
                    <span v-if="sortKey === 'context_length'" class="text-brand-400">{{ sortAsc ? '&#9650;' : '&#9660;' }}</span>
                  </button>
                </th>
                <th class="px-4 py-2.5 font-medium text-right">
                  <button @click="toggleSort('input_price')" class="inline-flex items-center gap-1 ml-auto hover:text-white transition-colors">
                    Input / 1M
                    <span v-if="sortKey === 'input_price'" class="text-brand-400">{{ sortAsc ? '&#9650;' : '&#9660;' }}</span>
                  </button>
                </th>
                <th class="px-4 py-2.5 font-medium text-right">
                  <button @click="toggleSort('output_price')" class="inline-flex items-center gap-1 ml-auto hover:text-white transition-colors">
                    Output / 1M
                    <span v-if="sortKey === 'output_price'" class="text-brand-400">{{ sortAsc ? '&#9650;' : '&#9660;' }}</span>
                  </button>
                </th>
                <th class="px-4 py-2.5 font-medium text-right">
                  <button @click="toggleSort('p50')" class="inline-flex items-center gap-1 ml-auto hover:text-white transition-colors">
                    P50
                    <span v-if="sortKey === 'p50'" class="text-brand-400">{{ sortAsc ? '&#9650;' : '&#9660;' }}</span>
                  </button>
                </th>
                <th class="px-4 py-2.5 font-medium text-right">
                  <button @click="toggleSort('p95')" class="inline-flex items-center gap-1 ml-auto hover:text-white transition-colors">
                    P95
                    <span v-if="sortKey === 'p95'" class="text-brand-400">{{ sortAsc ? '&#9650;' : '&#9660;' }}</span>
                  </button>
                </th>
                <th class="px-4 py-2.5 font-medium text-right">
                  <button @click="toggleSort('error_rate')" class="inline-flex items-center gap-1 ml-auto hover:text-white transition-colors">
                    Error
                    <span v-if="sortKey === 'error_rate'" class="text-brand-400">{{ sortAsc ? '&#9650;' : '&#9660;' }}</span>
                  </button>
                </th>
                <th class="px-4 py-2.5 font-medium text-right">
                  <button @click="toggleSort('guardrail')" class="inline-flex items-center gap-1 ml-auto hover:text-white transition-colors">
                    Guardrail
                    <span v-if="sortKey === 'guardrail'" class="text-brand-400">{{ sortAsc ? '&#9650;' : '&#9660;' }}</span>
                  </button>
                </th>
                <th class="px-4 py-2.5 font-medium text-right">
                  <button @click="toggleSort('pii')" class="inline-flex items-center gap-1 ml-auto hover:text-white transition-colors">
                    PII
                    <span v-if="sortKey === 'pii'" class="text-brand-400">{{ sortAsc ? '&#9650;' : '&#9660;' }}</span>
                  </button>
                </th>
                <th class="px-4 py-2.5 font-medium text-right">
                  <button @click="toggleSort('injection')" class="inline-flex items-center gap-1 ml-auto hover:text-white transition-colors">
                    Injection
                    <span v-if="sortKey === 'injection'" class="text-brand-400">{{ sortAsc ? '&#9650;' : '&#9660;' }}</span>
                  </button>
                </th>
                <th class="px-4 py-2.5 font-medium text-right">
                  <button @click="toggleSort('requests')" class="inline-flex items-center gap-1 ml-auto hover:text-white transition-colors">
                    24h Reqs
                    <span v-if="sortKey === 'requests'" class="text-brand-400">{{ sortAsc ? '&#9650;' : '&#9660;' }}</span>
                  </button>
                </th>
              </tr>
            </thead>
            <tbody>
              <tr
                v-for="model in sortedModels"
                :key="model.id"
                class="border-b border-slate-800/50 hover:bg-slate-800/30 transition-colors"
              >
                <td class="px-4 py-2.5 text-slate-400">{{ model.providerName }}</td>
                <td class="px-4 py-2.5">
                  <span class="text-slate-200 font-medium">{{ model.name }}</span>
                  <span class="block text-xs text-slate-500 font-mono">{{ model.id }}</span>
                </td>
                <td class="px-4 py-2.5 text-right text-slate-300 tabular-nums">
                  {{ model.context_length ? formatContext(model.context_length) : 'N/A' }}
                </td>
                <td class="px-4 py-2.5 text-right text-slate-300 tabular-nums">
                  {{ formatPrice(model.pricing?.prompt) }}
                </td>
                <td class="px-4 py-2.5 text-right text-slate-300 tabular-nums">
                  {{ formatPrice(model.pricing?.completion) }}
                </td>
                <td class="px-4 py-2.5 text-right tabular-nums">
                  <span :class="latencyClass(model.latency?.p50_ms)">
                    {{ model.latency ? formatMs(model.latency.p50_ms) : 'N/A' }}
                  </span>
                </td>
                <td class="px-4 py-2.5 text-right tabular-nums">
                  <span :class="latencyClass(model.latency?.p95_ms)">
                    {{ model.latency ? formatMs(model.latency.p95_ms) : 'N/A' }}
                  </span>
                </td>
                <td class="px-4 py-2.5 text-right tabular-nums">
                  <span :class="rateClass(model.error_rate, [0.01, 0.05])">
                    {{ model.error_rate != null ? formatPct(model.error_rate) : 'N/A' }}
                  </span>
                </td>
                <td class="px-4 py-2.5 text-right tabular-nums">
                  <span :class="rateClass(model.security?.guardrail_rate, [0.02, 0.1])">
                    {{ model.security ? formatPct(model.security.guardrail_rate) : 'N/A' }}
                  </span>
                </td>
                <td class="px-4 py-2.5 text-right tabular-nums">
                  <span :class="rateClass(model.security?.pii_rate, [0.01, 0.05])">
                    {{ model.security ? formatPct(model.security.pii_rate) : 'N/A' }}
                  </span>
                </td>
                <td class="px-4 py-2.5 text-right tabular-nums">
                  <span :class="rateClass(model.security?.injection_rate, [0.005, 0.02])">
                    {{ model.security ? formatPct(model.security.injection_rate) : 'N/A' }}
                  </span>
                </td>
                <td class="px-4 py-2.5 text-right text-slate-400 tabular-nums">
                  {{ model.request_count_24h != null ? formatCount(model.request_count_24h) : 'N/A' }}
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <p v-if="sortedModels.length === 0" class="text-center py-12 text-slate-500">
          No models match your search.
        </p>
      </div>

      <div class="mt-16 text-center text-sm text-slate-600">
        <p>Latency, error, and security stats are platform-wide aggregates refreshed every few minutes.</p>
        <p>Pricing reflects per-token costs. Prices shown per 1 million tokens.</p>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue';
import axios from 'axios';
import MarketingNav from '@/Pages/Home/MarketingNav.vue';

const search = ref('');
const loading = ref(true);
const error = ref(null);
const providers = ref([]);
const sortKey = ref('name');
const sortAsc = ref(true);

onMounted(async () => {
  try {
    const { data } = await axios.get('/api/model-catalog');
    providers.value = data.providers || [];
  } catch (e) {
    error.value = 'Failed to load model catalog. Please try again later.';
  } finally {
    loading.value = false;
  }
});

const filteredProviders = computed(() => {
  const q = search.value.toLowerCase().trim();
  if (!q) return providers.value;

  return providers.value
    .map(p => {
      const providerMatch = p.name.toLowerCase().includes(q) || p.id.toLowerCase().includes(q);
      const matchingModels = p.models.filter(
        m => m.name.toLowerCase().includes(q) || m.id.toLowerCase().includes(q)
      );
      if (providerMatch) return p;
      if (matchingModels.length > 0) return { ...p, models: matchingModels };
      return null;
    })
    .filter(Boolean);
});

function getSortValue(item, key) {
  switch (key) {
    case 'provider': return item.providerName;
    case 'name': return item.name;
    case 'context_length': return item.context_length ?? -1;
    case 'input_price': { const v = parseFloat(item.pricing?.prompt); return (isNaN(v) || v < 0) ? Infinity : v; }
    case 'output_price': { const v = parseFloat(item.pricing?.completion); return (isNaN(v) || v < 0) ? Infinity : v; }
    case 'p50': return item.latency?.p50_ms ?? Infinity;
    case 'p95': return item.latency?.p95_ms ?? Infinity;
    case 'error_rate': return item.error_rate ?? Infinity;
    case 'guardrail': return item.security?.guardrail_rate ?? Infinity;
    case 'pii': return item.security?.pii_rate ?? Infinity;
    case 'injection': return item.security?.injection_rate ?? Infinity;
    case 'requests': return item.request_count_24h ?? -1;
    default: return 0;
  }
}

const sortedModels = computed(() => {
  const flat = [];
  for (const p of filteredProviders.value) {
    for (const m of p.models) {
      flat.push({ ...m, providerName: p.name });
    }
  }
  flat.sort((a, b) => {
    const av = getSortValue(a, sortKey.value);
    const bv = getSortValue(b, sortKey.value);
    let cmp = 0;
    if (typeof av === 'string' && typeof bv === 'string') {
      cmp = av.localeCompare(bv);
    } else {
      cmp = (av < bv ? -1 : av > bv ? 1 : 0);
    }
    return sortAsc.value ? cmp : -cmp;
  });
  return flat;
});

function toggleSort(key) {
  if (sortKey.value === key) { sortAsc.value = !sortAsc.value; }
  else { sortKey.value = key; sortAsc.value = true; }
}

function formatPrice(perToken) {
  if (perToken == null) return 'N/A';
  const val = parseFloat(perToken);
  if (isNaN(val) || val < 0) return 'Variable';
  const perMillion = val * 1_000_000;
  if (perMillion < 0.01) return '$' + perMillion.toFixed(4);
  return '$' + perMillion.toFixed(2);
}

function formatContext(len) {
  if (len >= 1_000_000) return (len / 1_000_000).toFixed(1) + 'M';
  if (len >= 1000) return (len / 1000).toFixed(0) + 'K';
  return String(len);
}

function formatMs(ms) {
  if (ms == null) return 'N/A';
  if (ms >= 1000) return (ms / 1000).toFixed(1) + 's';
  return Math.round(ms) + 'ms';
}

function formatPct(rate) {
  if (rate == null) return 'N/A';
  return (rate * 100).toFixed(2) + '%';
}

function formatCount(n) {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M';
  if (n >= 1000) return (n / 1000).toFixed(1) + 'K';
  return String(n);
}

function latencyClass(ms) {
  if (ms == null) return 'text-slate-500';
  if (ms < 1000) return 'text-brand-400';
  if (ms < 3000) return 'text-amber-400';
  return 'text-red-400';
}

function rateClass(rate, thresholds) {
  if (rate == null) return 'text-slate-500';
  const [warn, bad] = thresholds;
  if (rate < warn) return 'text-brand-400';
  if (rate < bad) return 'text-amber-400';
  return 'text-red-400';
}
</script>
