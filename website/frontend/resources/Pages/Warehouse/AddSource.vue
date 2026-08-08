<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-4xl mx-auto px-8 py-6">
      <!-- Header -->
      <div class="mb-6">
        <div>
          <h1 class="text-2xl font-semibold text-gray-900">Add Data Source</h1>
          <p class="text-sm text-gray-500 dark:text-gray-400 mt-1">
            Connect a new data source for federated queries
          </p>
        </div>
      </div>

      <!-- Loading -->
      <div v-if="loadingCatalog" class="text-center py-12 text-gray-500">Loading connectors...</div>

      <!-- Step 1: Select Source Type -->
      <BaseCard v-if="!loadingCatalog && step === 1" class="mb-6">
        <template #header>
          <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">1. Select Source Type</h2>
        </template>
        <div v-for="group in groupedConnectors" :key="group.category" class="mb-6 last:mb-0">
          <h3 class="text-sm font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider mb-3">{{ group.label }}</h3>
          <div class="grid grid-cols-2 md:grid-cols-3 gap-4">
            <button
              v-for="conn in group.items"
              :key="conn.source_type"
              @click="selectSourceType(conn)"
              class="p-4 border rounded-lg text-left transition-all"
              :class="selectedConnector?.source_type === conn.source_type
                ? 'border-primary-500 bg-primary-50 dark:bg-primary-900/20'
                : 'border-gray-200 dark:border-gray-700 hover:border-primary-300 dark:hover:border-primary-600'"
            >
              <div class="text-2xl mb-2">{{ conn.icon }}</div>
              <div class="font-medium text-gray-900 dark:text-gray-100">{{ conn.name }}</div>
              <div class="text-sm text-gray-500 dark:text-gray-400">{{ conn.description }}</div>
            </button>
          </div>
        </div>
      </BaseCard>

      <!-- Step 2a: Enable Global Source (blockchain) -->
      <BaseCard v-if="!loadingCatalog && step === 2 && selectedConnector?.is_global" class="mb-6">
        <template #header>
          <div class="flex items-center justify-between">
            <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">2. Enable {{ selectedConnector.name }}</h2>
            <button @click="step = 1" class="text-sm text-primary-600 hover:text-primary-700">
              Change source type
            </button>
          </div>
        </template>

        <div class="space-y-4">
          <p class="text-sm text-gray-500 dark:text-gray-400">
            {{ selectedConnector.name }} is a globally-synced data source. Just pick a name and it will be available for queries immediately.
          </p>

          <div>
            <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Source Name *
            </label>
            <input
              v-model="formData.name"
              type="text"
              required
              :placeholder="selectedConnector.source_type"
              class="w-full px-4 py-2 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500 text-gray-900 dark:text-gray-100"
            />
            <p class="text-xs text-gray-500 mt-1">This name will be used to reference tables in queries (e.g. <code>SELECT * FROM {{ formData.name || selectedConnector.source_type }}.blocks</code>)</p>
          </div>

          <div class="flex justify-end gap-3 pt-4 border-t border-gray-200 dark:border-gray-700">
            <BaseButton variant="secondary" @click="router.push(`/p/${projectId}/warehouse/sources`)">
              Cancel
            </BaseButton>
            <BaseButton variant="primary" @click="enableGlobalSource" :disabled="saving || !formData.name.trim()">
              {{ saving ? 'Enabling...' : `Enable ${selectedConnector.name}` }}
            </BaseButton>
          </div>

          <p v-if="globalError" class="text-sm text-red-600">{{ globalError }}</p>
        </div>
      </BaseCard>

      <!-- Step 2b: Configure Connection (non-global sources) -->
      <BaseCard v-if="!loadingCatalog && step === 2 && selectedConnector && !selectedConnector.is_global" class="mb-6">
        <template #header>
          <div class="flex items-center justify-between">
            <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">2. Configure Connection</h2>
            <button @click="step = 1" class="text-sm text-primary-600 hover:text-primary-700">
              Change source type
            </button>
          </div>
        </template>

        <form @submit.prevent="testConnection" class="space-y-4">
          <!-- Source Name -->
          <div>
            <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Source Name *
            </label>
            <input
              v-model="formData.name"
              type="text"
              required
              placeholder="e.g., production_db, analytics_warehouse"
              class="w-full px-4 py-2 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500 text-gray-900 dark:text-gray-100"
            />
            <p class="text-xs text-gray-500 mt-1">This name will be used to reference tables in queries</p>
          </div>

          <!-- Dynamic config fields from catalog -->
          <DynamicConnectorForm
            :fields="selectedConnector.config_fields"
            v-model="formData.config"
          />

          <!-- Test Connection Button -->
          <div class="flex items-center gap-4 pt-4">
            <BaseButton type="submit" variant="secondary" :disabled="testing">
              <svg v-if="testing" class="animate-spin -ml-1 mr-2 h-4 w-4" fill="none" viewBox="0 0 24 24">
                <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
              </svg>
              {{ testing ? 'Testing...' : 'Test Connection' }}
            </BaseButton>
            <span v-if="testResult" :class="testResult.success ? 'text-green-600' : 'text-red-600'" class="text-sm">
              {{ testResult.success ? `Connected! Found ${testResult.tables?.length || 0} tables` : testResult.error }}
            </span>
          </div>

          <!-- Table selection (ClickHouse only, after successful connection test) -->
          <div v-if="testResult?.success && selectedConnector?.source_type === 'clickhouse' && (testResult.tables?.length || 0) > 0" class="pt-6 mt-6 border-t border-gray-200 dark:border-gray-700">
            <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
              Select tables to sync
            </label>
            <p class="text-xs text-gray-500 dark:text-gray-400 mb-3">
              Leave empty to sync all tables, or select specific tables below.
            </p>
            <div class="flex gap-2 mb-3">
              <button type="button" @click="selectAllTables" class="text-sm text-primary-600 hover:text-primary-700 dark:text-primary-400">
                Select all
              </button>
              <span class="text-gray-400">|</span>
              <button type="button" @click="deselectAllTables" class="text-sm text-primary-600 hover:text-primary-700 dark:text-primary-400">
                Deselect all
              </button>
            </div>
            <div class="max-h-48 overflow-y-auto border border-gray-300 dark:border-gray-600 rounded-lg p-3 bg-white dark:bg-gray-800">
              <label v-for="table in testResult.tables" :key="table" class="flex items-center gap-2 py-1.5 cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-700/50 rounded px-2 -mx-2">
                <input type="checkbox" :checked="isTableSelected(table)" @change="toggleTableSelection(table)" class="rounded border-gray-300 dark:border-gray-600 text-primary-600 focus:ring-primary-500" />
                <span class="text-sm text-gray-900 dark:text-gray-100 font-mono">{{ table }}</span>
              </label>
            </div>
          </div>

          <!-- Sync Scope Config (shown after successful connection test) -->
          <div v-if="testResult?.success" class="pt-6 mt-6 border-t border-gray-200 dark:border-gray-700">
            <h3 class="text-sm font-medium text-gray-700 dark:text-gray-300 mb-3">Sync Scope</h3>
            <div class="space-y-3">
              <label class="flex items-center gap-2 cursor-pointer">
                <input v-model="formData.sync_scope" type="radio" value="full" class="rounded-full border-gray-300 dark:border-gray-600 text-primary-600 focus:ring-primary-500" />
                <span class="text-sm text-gray-900 dark:text-gray-100">Full sync</span>
              </label>
              <label class="flex items-center gap-2 cursor-pointer">
                <input v-model="formData.sync_scope" type="radio" value="time_based" class="rounded-full border-gray-300 dark:border-gray-600 text-primary-600 focus:ring-primary-500" />
                <span class="text-sm text-gray-900 dark:text-gray-100">Time-based</span>
              </label>
              <div v-if="formData.sync_scope === 'time_based'" class="ml-6">
                <label class="block text-sm text-gray-600 dark:text-gray-400 mb-1">Only sync data older than</label>
                <input
                  v-model.number="formData.sync_scope_older_than_days"
                  type="number"
                  min="1"
                  class="w-24 px-3 py-2 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500 text-gray-900 dark:text-gray-100"
                />
                <span class="ml-2 text-sm text-gray-600 dark:text-gray-400">days</span>
              </div>
            </div>
          </div>

          <!-- Storage Tier Policy Config (shown after successful connection test) -->
          <div v-if="testResult?.success" class="pt-6 mt-6 border-t border-gray-200 dark:border-gray-700">
            <h3 class="text-sm font-medium text-gray-700 dark:text-gray-300 mb-3">Storage Tier Policy</h3>
            <div class="space-y-3">
              <label class="flex items-center gap-2 cursor-pointer">
                <input v-model="formData.storage_tier_type" type="radio" value="fixed" class="rounded-full border-gray-300 dark:border-gray-600 text-primary-600 focus:ring-primary-500" />
                <span class="text-sm text-gray-900 dark:text-gray-100">Fixed tier</span>
              </label>
              <div v-if="formData.storage_tier_type === 'fixed'" class="ml-6">
                <label class="block text-sm text-gray-600 dark:text-gray-400 mb-1">Tier</label>
                <select
                  v-model="formData.storage_tier_fixed_tier"
                  class="w-full max-w-xs px-3 py-2 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500 text-gray-900 dark:text-gray-100"
                >
                  <option value="hot">Hot</option>
                  <option value="warm">Warm</option>
                  <option value="cold">Cold</option>
                </select>
              </div>
              <label class="flex items-center gap-2 cursor-pointer">
                <input v-model="formData.storage_tier_type" type="radio" value="lifecycle" class="rounded-full border-gray-300 dark:border-gray-600 text-primary-600 focus:ring-primary-500" />
                <span class="text-sm text-gray-900 dark:text-gray-100">Lifecycle policy (age-based)</span>
              </label>
              <div v-if="formData.storage_tier_type === 'lifecycle'" class="ml-6 space-y-3">
                <div class="text-xs text-gray-500 dark:text-gray-400 mb-2">Data starts at Hot, then transitions after the specified days:</div>
                <div v-for="(t, idx) in formData.storage_tier_transitions" :key="idx" class="flex items-center gap-2">
                  <span class="text-sm text-gray-600 dark:text-gray-400">After</span>
                  <input
                    v-model.number="t.after_days"
                    type="number"
                    min="1"
                    class="w-20 px-3 py-2 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500 text-gray-900 dark:text-gray-100"
                  />
                  <span class="text-sm text-gray-600 dark:text-gray-400">days, move to</span>
                  <select
                    v-model="t.tier"
                    class="px-3 py-2 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500 text-gray-900 dark:text-gray-100"
                  >
                    <option value="warm">Warm</option>
                    <option value="cold">Cold</option>
                  </select>
                  <button type="button" @click="removeStorageTierTransition(idx)" :disabled="formData.storage_tier_transitions.length <= 1" class="p-1 text-red-600 hover:text-red-700 dark:text-red-400 dark:hover:text-red-300 disabled:opacity-50 disabled:cursor-not-allowed" title="Remove">
                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
                    </svg>
                  </button>
                </div>
                <button type="button" @click="addStorageTierTransition" class="text-sm text-primary-600 hover:text-primary-700 dark:text-primary-400 flex items-center gap-1">
                  <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" />
                  </svg>
                  Add transition
                </button>
              </div>
              <label class="flex items-center gap-2 cursor-pointer">
                <input v-model="formData.storage_tier_type" type="radio" value="access_based" class="rounded-full border-gray-300 dark:border-gray-600 text-primary-600 focus:ring-primary-500" />
                <span class="text-sm text-gray-900 dark:text-gray-100">Access-based policy</span>
              </label>
              <div v-if="formData.storage_tier_type === 'access_based'" class="ml-6 space-y-3">
                <div class="text-xs text-gray-500 dark:text-gray-400 mb-2">
                  Automatically promotes frequently queried sources to hotter tiers and demotes infrequently queried sources to colder tiers.
                </div>
                <label class="block text-sm text-gray-600 dark:text-gray-400 mb-1">Sensitivity</label>
                <div class="space-y-2">
                  <label class="flex items-start gap-2 cursor-pointer p-2 rounded-lg border transition-all"
                    :class="formData.access_sensitivity === 'aggressive'
                      ? 'border-primary-500 bg-primary-50 dark:bg-primary-900/20'
                      : 'border-gray-200 dark:border-gray-700 hover:border-primary-300'"
                  >
                    <input v-model="formData.access_sensitivity" type="radio" value="aggressive" class="mt-0.5 rounded-full border-gray-300 dark:border-gray-600 text-primary-600 focus:ring-primary-500" />
                    <div>
                      <div class="text-sm font-medium text-gray-900 dark:text-gray-100">Aggressive</div>
                      <div class="text-xs text-gray-500 dark:text-gray-400">7-day window. Quick to promote and demote.</div>
                    </div>
                  </label>
                  <label class="flex items-start gap-2 cursor-pointer p-2 rounded-lg border transition-all"
                    :class="formData.access_sensitivity === 'moderate'
                      ? 'border-primary-500 bg-primary-50 dark:bg-primary-900/20'
                      : 'border-gray-200 dark:border-gray-700 hover:border-primary-300'"
                  >
                    <input v-model="formData.access_sensitivity" type="radio" value="moderate" class="mt-0.5 rounded-full border-gray-300 dark:border-gray-600 text-primary-600 focus:ring-primary-500" />
                    <div>
                      <div class="text-sm font-medium text-gray-900 dark:text-gray-100">Moderate</div>
                      <div class="text-xs text-gray-500 dark:text-gray-400">14-day window. Balanced approach.</div>
                    </div>
                  </label>
                  <label class="flex items-start gap-2 cursor-pointer p-2 rounded-lg border transition-all"
                    :class="formData.access_sensitivity === 'conservative'
                      ? 'border-primary-500 bg-primary-50 dark:bg-primary-900/20'
                      : 'border-gray-200 dark:border-gray-700 hover:border-primary-300'"
                  >
                    <input v-model="formData.access_sensitivity" type="radio" value="conservative" class="mt-0.5 rounded-full border-gray-300 dark:border-gray-600 text-primary-600 focus:ring-primary-500" />
                    <div>
                      <div class="text-sm font-medium text-gray-900 dark:text-gray-100">Conservative</div>
                      <div class="text-xs text-gray-500 dark:text-gray-400">30-day window. Slow to change tiers.</div>
                    </div>
                  </label>
                </div>
              </div>
            </div>
          </div>

          <!-- Submit (shown after successful connection test) -->
          <div v-if="testResult?.success" class="flex justify-end gap-3 pt-6 mt-6 border-t border-gray-200 dark:border-gray-700">
            <BaseButton variant="secondary" @click="router.push(`/p/${projectId}/warehouse/sources`)">
              Cancel
            </BaseButton>
            <BaseButton variant="primary" @click="createSource" :disabled="saving">
              {{ saving ? 'Creating...' : 'Add Source' }}
            </BaseButton>
          </div>
        </form>
      </BaseCard>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, reactive, computed, onMounted, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import axios from 'axios';
import AppLayout from '@/Layouts/AppLayout.vue';
import BaseCard from '@/components/BaseCard.vue';
import BaseButton from '@/components/BaseButton.vue';
import DynamicConnectorForm from '@/components/DynamicConnectorForm.vue';
import { useAuth } from '@/composables/useAuth';

const route = useRoute();
const router = useRouter();
const { user } = useAuth();

const projectId = computed(() => route.params.id);
const project = computed(() => ({ id: projectId.value }));

const loadingCatalog = ref(true);
const connectorCatalog = ref([]);
const selectedConnector = ref(null);

const categoryLabels = {
  database: 'Databases',
  saas: 'SaaS & APIs',
  blockchain: 'Blockchain',
};

const categoryOrder = ['database', 'saas', 'blockchain'];

const groupedConnectors = computed(() => {
  const byCategory = {};
  for (const conn of connectorCatalog.value) {
    const cat = conn.category || 'other';
    if (!byCategory[cat]) byCategory[cat] = [];
    byCategory[cat].push(conn);
  }
  return categoryOrder
    .filter(cat => byCategory[cat]?.length)
    .map(cat => ({
      category: cat,
      label: categoryLabels[cat] || cat,
      items: byCategory[cat],
    }));
});

async function loadConnectorCatalog() {
  loadingCatalog.value = true;
  try {
    const res = await axios.get(`/api/projects/${projectId.value}/warehouse/connector-types`);
    connectorCatalog.value = res.data;
  } catch (e) {
    console.error('Failed to load connector catalog', e);
  } finally {
    loadingCatalog.value = false;
  }
}

onMounted(loadConnectorCatalog);
watch(projectId, loadConnectorCatalog);

const step = ref(1);
const testing = ref(false);
const saving = ref(false);
const testResult = ref(null);
const globalError = ref(null);

const formData = reactive({
  name: '',
  config: {},
  sync_scope: 'full',
  sync_scope_older_than_days: 30,
  storage_tier_type: 'fixed',
  storage_tier_fixed_tier: 'cold',
  storage_tier_transitions: [{ after_days: 30, tier: 'warm' }],
  access_sensitivity: 'moderate'
});

const selectSourceType = (conn) => {
  selectedConnector.value = conn;
  testResult.value = null;
  globalError.value = null;
  step.value = 2;

  if (conn.is_global) {
    formData.name = conn.source_type;
    return;
  }

  formData.name = '';
  const defaults = {};
  for (const field of conn.config_fields) {
    if (field.default_value !== undefined && field.default_value !== null) {
      defaults[field.key] = field.default_value;
    }
  }
  formData.config = defaults;

  formData.sync_scope = 'full';
  formData.sync_scope_older_than_days = 30;
  formData.storage_tier_type = 'fixed';
  formData.storage_tier_fixed_tier = 'cold';
  formData.storage_tier_transitions = [{ after_days: 30, tier: 'warm' }];
  formData.access_sensitivity = 'moderate';
};

const enableGlobalSource = async () => {
  saving.value = true;
  globalError.value = null;

  try {
    const chain = selectedConnector.value.source_type;
    await axios.post(
      `/api/projects/${projectId.value}/warehouse/blockchain/${chain}`,
      { name: formData.name.trim() }
    );
    router.push(`/p/${projectId.value}/warehouse/sources`);
  } catch (error) {
    globalError.value = error.response?.data?.message || 'Failed to enable source';
  } finally {
    saving.value = false;
  }
};

const buildSubmitConfig = () => {
  const c = { ...formData.config };
  // ClickHouse port mapping: the factory expects http_port or native_port
  if (selectedConnector.value?.source_type === 'clickhouse') {
    const protocol = c.protocol || 'native';
    if (protocol === 'http') {
      c.http_port = c.port;
    } else {
      c.native_port = c.port;
    }
    delete c.port;
  }
  return c;
};

const testConnection = async () => {
  testing.value = true;
  testResult.value = null;

  try {
    const response = await axios.post(`/api/projects/${projectId.value}/warehouse/sources/test`, {
      source_type: selectedConnector.value.source_type,
      config: buildSubmitConfig()
    });
    testResult.value = response.data;
  } catch (error) {
    testResult.value = { success: false, error: error.response?.data?.message || error.message };
  } finally {
    testing.value = false;
  }
};

const toggleTableSelection = (tableName) => {
  const tables = formData.config.tables || [];
  const idx = tables.indexOf(tableName);
  if (idx >= 0) {
    formData.config.tables = tables.filter(t => t !== tableName);
  } else {
    formData.config.tables = [...tables, tableName];
  }
};

const isTableSelected = (tableName) => {
  return (formData.config.tables || []).includes(tableName);
};

const selectAllTables = () => {
  formData.config.tables = [...(testResult.value?.tables || [])];
};

const deselectAllTables = () => {
  formData.config.tables = [];
};

const addStorageTierTransition = () => {
  const last = formData.storage_tier_transitions[formData.storage_tier_transitions.length - 1];
  const nextDays = last ? last.after_days + 30 : 30;
  const nextTier = last?.tier === 'warm' ? 'cold' : 'warm';
  formData.storage_tier_transitions.push({ after_days: nextDays, tier: nextTier });
};

const removeStorageTierTransition = (idx) => {
  formData.storage_tier_transitions.splice(idx, 1);
};

const getStorageTierPolicy = () => {
  if (formData.storage_tier_type === 'fixed') {
    return { type: 'fixed', tier: formData.storage_tier_fixed_tier };
  }
  if (formData.storage_tier_type === 'access_based') {
    return { type: 'access_based', sensitivity: formData.access_sensitivity };
  }
  return {
    type: 'lifecycle',
    transitions: formData.storage_tier_transitions.map(t => ({ after_days: t.after_days, tier: t.tier }))
  };
};

const createSource = async () => {
  saving.value = true;

  try {
    const payload = {
      name: formData.name,
      source_type: selectedConnector.value.source_type,
      tier: 'cold',
      config: buildSubmitConfig(),
      sync_scope: formData.sync_scope,
      sync_scope_older_than_days: formData.sync_scope === 'time_based' ? formData.sync_scope_older_than_days : undefined,
      storage_tier_policy: getStorageTierPolicy()
    };
    await axios.post(`/api/projects/${projectId.value}/warehouse/sources`, payload);

    router.push(`/p/${projectId.value}/warehouse/sources`);
  } catch (error) {
    alert(error.response?.data?.message || 'Failed to create source');
  } finally {
    saving.value = false;
  }
};
</script>
