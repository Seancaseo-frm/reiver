<template>
  <div class="space-y-4">
    <div class="flex items-center justify-between">
      <div class="flex items-center gap-3">
        <input
          v-model="catalogSearch"
          type="text"
          placeholder="Search models..."
          class="w-64 border-gray-300 rounded-md shadow-sm text-sm"
        />
        <select v-model="catalogProviderFilter" class="border-gray-300 rounded-md shadow-sm text-sm">
          <option value="">All providers</option>
          <option v-for="p in catalogProviders" :key="p" :value="p">{{ p }}</option>
        </select>
        <select v-model="catalogEnabledFilter" class="border-gray-300 rounded-md shadow-sm text-sm">
          <option value="">All</option>
          <option value="true">Enabled</option>
          <option value="false">Disabled</option>
        </select>
      </div>
      <div class="flex items-center gap-3">
        <span class="text-sm text-gray-500">{{ filteredCatalog.length }} models</span>
        <button
          @click="syncCatalog"
          :disabled="catalogSyncing"
          class="px-4 py-2 rounded-lg bg-blue-600 text-white text-sm font-medium hover:bg-blue-700 disabled:opacity-50"
        >
          {{ catalogSyncing ? 'Syncing...' : 'Sync Now' }}
        </button>
      </div>
    </div>

    <div v-if="catalogError" class="mb-4 rounded-lg bg-red-50 border border-red-200 px-4 py-3 text-sm text-red-700 flex items-center justify-between">
      <span>{{ catalogError }}</span>
      <button @click="catalogError = ''" class="ml-4 text-red-500 hover:text-red-700">&times;</button>
    </div>
    <div v-if="loadingCatalog" class="text-gray-400 text-sm py-8 text-center">Loading model catalog...</div>
    <div v-else class="bg-white border border-gray-200 rounded-lg overflow-hidden">
      <div class="overflow-x-auto">
        <table class="min-w-full divide-y divide-gray-200">
          <thead class="bg-gray-50">
            <tr>
              <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Provider</th>
              <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Model ID</th>
              <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Name</th>
              <th class="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Context</th>
              <th class="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Input $/M</th>
              <th class="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Output $/M</th>
              <th class="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">Enabled</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-gray-200">
            <tr v-for="m in paginatedCatalog" :key="m.id" :class="m.enabled ? '' : 'bg-gray-50 text-gray-400'">
              <td class="px-4 py-2 text-sm">
                <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-gray-100 text-gray-800">{{ m.provider_slug }}</span>
              </td>
              <td class="px-4 py-2 text-sm font-mono">{{ m.model_slug }}</td>
              <td class="px-4 py-2 text-sm text-gray-700">{{ m.name }}</td>
              <td class="px-4 py-2 text-sm text-right font-mono text-gray-500">{{ m.context_length ? m.context_length.toLocaleString() : '-' }}</td>
              <td class="px-4 py-2 text-sm text-right font-mono">{{ formatPrice(m.pricing?.prompt) }}</td>
              <td class="px-4 py-2 text-sm text-right font-mono">{{ formatPrice(m.pricing?.completion) }}</td>
              <td class="px-4 py-2 text-center">
                <button
                  @click="toggleModelEnabled(m)"
                  :disabled="m._toggling"
                  :class="[
                    'relative inline-flex h-5 w-9 flex-shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors duration-200 ease-in-out focus:outline-none',
                    m.enabled ? 'bg-blue-600' : 'bg-gray-200'
                  ]"
                >
                  <span
                    :class="[
                      'pointer-events-none inline-block h-4 w-4 transform rounded-full bg-white shadow ring-0 transition duration-200 ease-in-out',
                      m.enabled ? 'translate-x-4' : 'translate-x-0'
                    ]"
                  />
                </button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
      <div v-if="filteredCatalog.length === 0" class="px-4 py-8 text-center text-gray-500 text-sm">No models found.</div>
      <div v-if="filteredCatalog.length > catalogPageSize" class="px-4 py-3 border-t border-gray-200 flex items-center justify-between text-sm text-gray-500">
        <span>Showing {{ catalogPageStart + 1 }}-{{ Math.min(catalogPageStart + catalogPageSize, filteredCatalog.length) }} of {{ filteredCatalog.length }}</span>
        <div class="flex gap-2">
          <button @click="catalogPage--" :disabled="catalogPage <= 0" class="px-3 py-1 border rounded text-sm disabled:opacity-50">Prev</button>
          <button @click="catalogPage++" :disabled="catalogPageStart + catalogPageSize >= filteredCatalog.length" class="px-3 py-1 border rounded text-sm disabled:opacity-50">Next</button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue';
import axios from 'axios';

const catalogModels = ref([]);
const loadingCatalog = ref(true);
const catalogSearch = ref('');
const catalogProviderFilter = ref('');
const catalogEnabledFilter = ref('');
const catalogSyncing = ref(false);
const catalogError = ref('');
const catalogPage = ref(0);
const catalogPageSize = 50;

const catalogProviders = computed(() => {
  const set = new Set(catalogModels.value.map(m => m.provider_slug));
  return [...set].sort();
});

const filteredCatalog = computed(() => {
  let list = catalogModels.value;
  if (catalogProviderFilter.value) {
    list = list.filter(m => m.provider_slug === catalogProviderFilter.value);
  }
  if (catalogEnabledFilter.value !== '') {
    const wantEnabled = catalogEnabledFilter.value === 'true';
    list = list.filter(m => m.enabled === wantEnabled);
  }
  if (catalogSearch.value) {
    const q = catalogSearch.value.toLowerCase();
    list = list.filter(m => m.name.toLowerCase().includes(q) || m.model_slug.toLowerCase().includes(q));
  }
  return list;
});

const catalogPageStart = computed(() => catalogPage.value * catalogPageSize);
const paginatedCatalog = computed(() => filteredCatalog.value.slice(catalogPageStart.value, catalogPageStart.value + catalogPageSize));

watch([catalogSearch, catalogProviderFilter, catalogEnabledFilter], () => { catalogPage.value = 0; });

async function fetchCatalog() {
  loadingCatalog.value = true;
  catalogError.value = '';
  try {
    const params = {};
    if (catalogProviderFilter.value) params.provider = catalogProviderFilter.value;
    if (catalogSearch.value) params.search = catalogSearch.value;
    if (catalogEnabledFilter.value !== '') params.enabled = catalogEnabledFilter.value === 'true';
    const { data } = await axios.get('/api/admin/model-catalog', { params });
    catalogModels.value = (data || []).map(m => ({ ...m, _toggling: false }));
  } catch (e) {
    catalogError.value = 'Failed to load model catalog. Please try again.';
    console.error('Failed to load model catalog', e);
  } finally {
    loadingCatalog.value = false;
  }
}

async function toggleModelEnabled(m) {
  m._toggling = true;
  catalogError.value = '';
  try {
    const { data } = await axios.patch(`/api/admin/model-catalog/${encodeURIComponent(m.id)}`, {
      enabled: !m.enabled,
    });
    m.enabled = data.enabled;
  } catch (e) {
    catalogError.value = `Failed to toggle "${m.name}". Please try again.`;
    console.error('Failed to toggle model', e);
  } finally {
    m._toggling = false;
  }
}

async function syncCatalog() {
  catalogSyncing.value = true;
  catalogError.value = '';
  try {
    await axios.post('/api/admin/model-catalog/sync');
    await fetchCatalog();
  } catch (e) {
    catalogError.value = 'Failed to trigger catalog sync. Please try again.';
    console.error('Failed to sync catalog', e);
  } finally {
    catalogSyncing.value = false;
  }
}

function formatPrice(val) {
  if (val === undefined || val === null || val === '') return '-';
  const n = parseFloat(val);
  if (isNaN(n) || n === 0) return 'Free';
  return '$' + (n * 1_000_000).toFixed(2);
}

onMounted(() => {
  fetchCatalog();
});
</script>
