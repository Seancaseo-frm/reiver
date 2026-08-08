<template>
  <div>
    <div class="flex items-center justify-between mb-6">
      <div></div>
      <button
        @click="showCreateModal = true"
        class="px-4 py-2 rounded-lg bg-blue-600 text-white text-sm font-medium hover:bg-blue-700"
      >
        Create Custom Tier
      </button>
    </div>

    <div v-if="error" class="mb-4 rounded-lg bg-red-50 border border-red-200 px-4 py-3 text-sm text-red-700 flex items-center justify-between">
      <span>{{ error }}</span>
      <button @click="error = ''" class="ml-4 text-red-500 hover:text-red-700">&times;</button>
    </div>

    <div v-if="success" class="mb-4 rounded-lg bg-green-50 border border-green-200 px-4 py-3 text-sm text-green-700 flex items-center justify-between">
      <span>{{ success }}</span>
      <button @click="success = ''" class="ml-4 text-green-500 hover:text-green-700">&times;</button>
    </div>

    <div v-if="initialLoading" class="text-gray-400 text-sm py-8 text-center">Loading tiers...</div>
    <div v-else class="space-y-3">
      <div v-for="tier in tiers" :key="tier.id" class="border border-gray-200 rounded-lg bg-white shadow-sm">
        <button
          @click="toggleExpand(tier.id)"
          class="w-full flex items-center justify-between px-5 py-4 text-left hover:bg-gray-50 transition-colors rounded-lg"
        >
          <div class="flex items-center gap-3">
            <span class="text-sm font-semibold text-gray-900">{{ tier.display_name }}</span>
            <span class="text-xs text-gray-400 font-mono">{{ tier.name }}</span>
            <span v-if="!tier.is_public" class="text-xs bg-gray-200 text-gray-600 px-1.5 py-0.5 rounded font-medium">Private</span>
          </div>
          <div class="flex items-center gap-4">
            <span v-if="tier.stripe_price_id" class="text-xs text-gray-400 font-mono">{{ tier.stripe_price_id }}</span>
            <svg
              class="w-4 h-4 text-gray-400 transition-transform"
              :class="{ 'rotate-180': expandedTierId === tier.id }"
              fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"
            >
              <path stroke-linecap="round" stroke-linejoin="round" d="M19 9l-7 7-7-7" />
            </svg>
          </div>
        </button>

        <div v-if="expandedTierId === tier.id" class="px-5 pb-5 space-y-6 border-t border-gray-100">
          <div class="pt-4 flex items-start gap-6">
            <div class="max-w-sm flex-1">
              <label class="block text-xs font-medium text-gray-500 mb-1">Stripe Price ID</label>
              <input
                type="text"
                :value="tier.stripe_price_id || ''"
                @change="updateTierField(tier.id, 'stripe_price_id', $event.target.value)"
                class="w-full border-gray-300 rounded-md shadow-sm text-sm font-mono"
                placeholder="price_..."
              />
            </div>
            <div class="pt-5">
              <label class="flex items-center gap-2 text-sm text-gray-700 cursor-pointer">
                <input
                  type="checkbox"
                  :checked="tier.is_public"
                  @change="updateTierField(tier.id, 'is_public', $event.target.checked)"
                  class="h-4 w-4 text-blue-600 border-gray-300 rounded"
                />
                Public (visible to users on billing page)
              </label>
            </div>
          </div>

          <template v-for="(sectionSchema, sectionKey) in configSections" :key="sectionKey">
            <div class="bg-gray-50 rounded-lg p-4 border border-gray-200">
              <h4 class="text-xs font-bold text-gray-600 uppercase tracking-wide mb-3 pb-2 border-b border-gray-200">{{ formatLabel(sectionKey) }}</h4>
              <div class="grid grid-cols-2 sm:grid-cols-3 gap-3">
                <template v-for="(fieldSchema, fieldKey) in sectionFields(sectionSchema)" :key="fieldKey">
                  <label v-if="fieldType(fieldSchema) === 'boolean'" class="flex items-center gap-2 text-sm text-gray-700">
                    <input
                      type="checkbox"
                      :checked="getConfigValue(tier, sectionKey, fieldKey)"
                      @change="updateConfig(tier, sectionKey, fieldKey, $event.target.checked)"
                      class="h-4 w-4 text-blue-600 border-gray-300 rounded"
                    />
                    {{ formatLabel(fieldKey) }}
                  </label>

                  <div v-else-if="isPercentField(fieldKey)" >
                    <label class="block text-xs text-gray-600 mb-1">{{ formatLabel(fieldKey) }}</label>
                    <div class="relative">
                      <input
                        type="number"
                        step="0.01"
                        min="0"
                        :value="(getConfigValue(tier, sectionKey, fieldKey) ?? 0) * 100"
                        @change="updateConfig(tier, sectionKey, fieldKey, parseFloat($event.target.value) / 100)"
                        class="w-full border-gray-300 rounded-md shadow-sm text-sm pr-7"
                      />
                      <span class="absolute inset-y-0 right-2 flex items-center text-xs text-gray-400 pointer-events-none">%</span>
                    </div>
                  </div>

                  <div v-else-if="isUsdField(fieldKey)">
                    <label class="block text-xs text-gray-600 mb-1">{{ formatLabel(fieldKey) }}</label>
                    <div class="relative">
                      <span class="absolute inset-y-0 left-2 flex items-center text-xs text-gray-400 pointer-events-none">$</span>
                      <input
                        type="number"
                        step="0.01"
                        min="0"
                        :value="getConfigValue(tier, sectionKey, fieldKey) ?? 0"
                        @change="updateConfig(tier, sectionKey, fieldKey, parseFloat($event.target.value))"
                        class="w-full border-gray-300 rounded-md shadow-sm text-sm pl-5"
                      />
                    </div>
                  </div>

                  <div v-else>
                    <label class="block text-xs text-gray-600 mb-1">{{ formatLabel(fieldKey) }}</label>
                    <input
                      type="number"
                      :value="getConfigValue(tier, sectionKey, fieldKey) ?? 0"
                      @change="updateConfig(tier, sectionKey, fieldKey, parseInt($event.target.value))"
                      class="w-full border-gray-300 rounded-md shadow-sm text-sm"
                      :title="getConfigValue(tier, sectionKey, fieldKey) === -1 ? 'Unlimited (-1)' : ''"
                    />
                  </div>
                </template>
              </div>
            </div>
          </template>

          <p class="text-xs text-gray-400">Use -1 for unlimited integer fields. Percentages are displayed as human-readable (e.g. 3 = 3%). Changes save immediately on edit.</p>
        </div>
      </div>
    </div>

    <div v-if="showCreateModal" class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
      <div class="bg-white rounded-xl shadow-xl p-6 w-full max-w-md space-y-4">
        <h2 class="text-lg font-bold text-gray-900">Create Custom Tier</h2>
        <div>
          <label class="block text-sm font-medium text-gray-700 mb-1">Name (slug)</label>
          <input v-model="createForm.name" class="w-full border-gray-300 rounded-md shadow-sm text-sm" placeholder="acme-enterprise" />
        </div>
        <div>
          <label class="block text-sm font-medium text-gray-700 mb-1">Display Name</label>
          <input v-model="createForm.display_name" class="w-full border-gray-300 rounded-md shadow-sm text-sm" placeholder="Acme Enterprise" />
        </div>
        <div>
          <label class="block text-sm font-medium text-gray-700 mb-1">Stripe Price ID <span class="text-gray-400 font-normal">(optional)</span></label>
          <input v-model="createForm.stripe_price_id" class="w-full border-gray-300 rounded-md shadow-sm text-sm font-mono" placeholder="price_..." />
        </div>
        <div>
          <label class="flex items-center gap-2 text-sm text-gray-700 cursor-pointer">
            <input type="checkbox" v-model="createForm.is_public" class="h-4 w-4 text-blue-600 border-gray-300 rounded" />
            Public (visible to users on billing page)
          </label>
        </div>
        <div class="flex gap-3 pt-2">
          <button
            @click="createTier"
            :disabled="!createForm.name || !createForm.display_name"
            class="px-4 py-2 rounded-lg bg-blue-600 text-white text-sm font-medium hover:bg-blue-700 disabled:opacity-50"
          >
            Create
          </button>
          <button
            @click="showCreateModal = false"
            class="px-4 py-2 rounded-lg bg-gray-100 text-gray-700 text-sm font-medium hover:bg-gray-200"
          >
            Cancel
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted } from 'vue';
import axios from 'axios';

const props = defineProps({
  schema: { type: Object, required: true },
});

const initialLoading = ref(true);
const error = ref('');
const success = ref('');
const tiers = ref([]);
const expandedTierId = ref(null);
const showCreateModal = ref(false);
const createForm = reactive({ name: '', display_name: '', stripe_price_id: '', is_public: true });

const schema = computed(() => props.schema || {});

const configSections = computed(() => {
  const schemaProps = schema.value?.properties;
  if (!schemaProps) return {};
  const result = {};
  for (const [key, val] of Object.entries(schemaProps)) {
    if (val.type === 'object' || val.properties || val.$ref || val.allOf) {
      result[key] = val;
    }
  }
  return result;
});

function resolveRef(refStr) {
  if (!refStr) return null;
  const defName = refStr.split('/').pop();
  return schema.value?.definitions?.[defName] || null;
}

function sectionFields(sectionSchema) {
  if (sectionSchema?.properties) return sectionSchema.properties;

  let ref = sectionSchema?.$ref;
  if (!ref && sectionSchema?.allOf) {
    const entry = sectionSchema.allOf.find(e => e.$ref);
    ref = entry?.$ref;
  }

  const resolved = resolveRef(ref);
  return resolved?.properties || {};
}

function fieldType(fieldSchema) {
  return fieldSchema?.type || 'string';
}

function isPercentField(fieldKey) {
  return fieldKey.endsWith('_percent');
}

function isUsdField(fieldKey) {
  return fieldKey.endsWith('_usd');
}

function getConfigValue(tier, section, field) {
  return tier.config?.[section]?.[field];
}

function toggleExpand(id) {
  expandedTierId.value = expandedTierId.value === id ? null : id;
}

function formatLabel(key) {
  return key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
}

function showSuccess(msg) {
  success.value = msg;
  setTimeout(() => success.value = '', 3000);
}

async function loadTiers() {
  try {
    const { data } = await axios.get('/api/admin/tiers');
    tiers.value = data;
  } catch (e) {
    error.value = e.response?.data?.message || 'Failed to load tiers';
  } finally {
    initialLoading.value = false;
  }
}

async function updateTierField(tierId, field, value) {
  const idx = tiers.value.findIndex(t => t.id === tierId);
  if (idx === -1) return;
  const prev = tiers.value[idx][field];
  tiers.value[idx] = { ...tiers.value[idx], [field]: value };
  try {
    await axios.put(`/api/admin/tiers/${tierId}`, { [field]: value });
    showSuccess('Tier updated');
  } catch (e) {
    tiers.value[idx] = { ...tiers.value[idx], [field]: prev };
    error.value = e.response?.data?.message || 'Failed to update tier';
  }
}

async function updateConfig(tier, section, field, value) {
  const idx = tiers.value.findIndex(t => t.id === tier.id);
  if (idx === -1) return;

  const oldConfig = JSON.parse(JSON.stringify(tier.config || {}));
  const newConfig = JSON.parse(JSON.stringify(tier.config || {}));
  if (!newConfig[section]) newConfig[section] = {};
  newConfig[section][field] = value;

  tiers.value[idx] = { ...tiers.value[idx], config: newConfig };
  try {
    await axios.put(`/api/admin/tiers/${tier.id}`, { config: newConfig });
    showSuccess('Tier updated');
  } catch (e) {
    tiers.value[idx] = { ...tiers.value[idx], config: oldConfig };
    error.value = e.response?.data?.message || 'Failed to update tier';
  }
}

async function createTier() {
  try {
    await axios.post('/api/admin/tiers', {
      name: createForm.name,
      display_name: createForm.display_name,
      stripe_price_id: createForm.stripe_price_id || null,
      is_public: createForm.is_public,
      config: {},
    });
    showCreateModal.value = false;
    createForm.name = '';
    createForm.display_name = '';
    createForm.stripe_price_id = '';
    createForm.is_public = true;
    await loadTiers();
    showSuccess('Tier created');
  } catch (e) {
    error.value = e.response?.data?.message || 'Failed to create tier';
  }
}

onMounted(() => {
  loadTiers();
});
</script>
