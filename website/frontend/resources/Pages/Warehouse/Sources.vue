<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-[1200px] mx-auto px-8 py-6">
      <!-- Header -->
      <div class="mb-6">
        <div>
          <h1 class="text-2xl font-semibold text-gray-900">Data Sources</h1>
          <p class="text-sm text-gray-500 dark:text-gray-400 mt-1">
            Manage your data source connections for federated queries
          </p>
        </div>
        <div class="flex gap-3">
          <BaseButton variant="primary" @click="navigateToAdd">
            <svg class="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" />
            </svg>
            Add Source
          </BaseButton>
        </div>
      </div>

      <!-- Filters -->
      <div class="mb-6 flex gap-4 items-center">
        <div class="relative flex-1 max-w-md">
          <input
            v-model="searchQuery"
            type="text"
            placeholder="Search sources..."
            class="w-full px-4 py-2 pl-10 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent text-gray-900 dark:text-gray-100"
          />
          <svg
            class="absolute left-3 top-1/2 transform -translate-y-1/2 w-5 h-5 text-gray-400"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
          </svg>
        </div>
        <select
          v-model="tierFilter"
          class="px-4 py-2 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500 text-gray-900 dark:text-gray-100"
        >
          <option value="all">All Tiers</option>
          <option value="cold">Cold</option>
          <option value="warm">Warm</option>
          <option value="hot">Hot</option>
        </select>
      </div>

      <!-- Sources List -->
      <BaseCard>
        <template #header>
          <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">
            Sources ({{ filteredSources.length }})
          </h2>
        </template>

        <div v-if="loading" class="text-center py-8 text-gray-500 dark:text-gray-400">
          <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full mx-auto mb-3"></div>
          <p>Loading sources...</p>
        </div>

        <div v-else-if="filteredSources.length === 0" class="text-center py-12 text-gray-500 dark:text-gray-400">
          <svg class="w-12 h-12 mx-auto mb-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4" />
          </svg>
          <p class="text-lg font-medium mb-2">No data sources yet</p>
          <p class="text-sm mb-4">Add a data source to start running federated queries</p>
          <BaseButton variant="primary" @click="navigateToAdd">
            Add Your First Source
          </BaseButton>
        </div>

        <div v-else class="overflow-x-auto">
          <table class="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
            <thead class="bg-gray-50 dark:bg-gray-800">
              <tr>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Name</th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Type</th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Tier</th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Sync Scope</th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Storage Policy</th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Sync Interval</th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Status</th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Created</th>
                <th class="px-6 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Actions</th>
              </tr>
            </thead>
            <tbody class="bg-white dark:bg-gray-900 divide-y divide-gray-200 dark:divide-gray-700">
              <tr v-for="source in filteredSources" :key="source.id" class="hover:bg-gray-50 dark:hover:bg-gray-800">
                <td class="px-6 py-4 whitespace-nowrap">
                  <div class="flex items-center">
                    <div class="w-8 h-8 rounded-full bg-primary-100 dark:bg-primary-900 flex items-center justify-center mr-3">
                      <span class="text-primary-600 dark:text-primary-400 font-medium text-sm">
                        {{ source.name.charAt(0).toUpperCase() }}
                      </span>
                    </div>
                    <div>
                      <div class="text-sm font-medium text-gray-900 dark:text-gray-100">{{ source.name }}</div>
                    </div>
                  </div>
                </td>
                <td class="px-6 py-4 whitespace-nowrap">
                  <span class="text-sm text-gray-900 dark:text-gray-100">{{ formatSourceType(source.source_type) }}</span>
                </td>
                <td class="px-6 py-4 whitespace-nowrap">
                  <span
                    class="px-2 py-1 text-xs font-medium rounded-full"
                    :class="getTierClass(source.tier)"
                  >
                    {{ getTierLabel(source.tier) }}
                  </span>
                </td>
                <td class="px-6 py-4 whitespace-nowrap">
                  <span
                    class="px-2 py-1 text-xs font-medium rounded-full bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300"
                  >
                    {{ formatSyncScope(source) }}
                  </span>
                </td>
                <td class="px-6 py-4 whitespace-nowrap">
                  <span class="text-xs text-gray-600 dark:text-gray-400">
                    {{ formatStorageTierPolicy(source) }}
                  </span>
                </td>
                <td class="px-6 py-4 whitespace-nowrap">
                  <span v-if="source.is_global" class="text-xs text-gray-400">Managed</span>
                  <!-- Sync interval selector for warm/hot tier sources -->
                  <template v-else-if="source.tier !== 'cold'">
                    <select
                      :value="source.sync_interval || ''"
                      @change="updateSyncInterval(source, $event.target.value)"
                      class="text-xs px-2 py-1 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded focus:outline-none focus:ring-1 focus:ring-primary-500"
                    >
                      <option value="">Manual only</option>
                      <option value="5m">Every 5 min</option>
                      <option value="15m">Every 15 min</option>
                      <option value="30m">Every 30 min</option>
                      <option value="1h">Every hour</option>
                      <option value="6h">Every 6 hours</option>
                      <option value="12h">Every 12 hours</option>
                      <option value="24h">Every 24 hours</option>
                    </select>
                  </template>
                  <span v-else class="text-xs text-gray-400">-</span>
                </td>
                <td class="px-6 py-4 whitespace-nowrap">
                  <!-- Show job progress if a job is running -->
                  <template v-if="source.sync_in_progress">
                    <span class="inline-flex items-center px-2 py-1 text-xs font-medium rounded-full bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200 animate-pulse">
                      <svg class="w-3 h-3 mr-1 animate-spin" fill="none" viewBox="0 0 24 24">
                        <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                        <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                      </svg>
                      {{ formatJobType(source.current_job_type) }}
                    </span>
                  </template>
                  <template v-else>
                    <span
                      class="px-2 py-1 text-xs font-medium rounded-full"
                      :class="source.enabled ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200' : 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300'"
                    >
                      {{ source.enabled ? 'Active' : 'Disabled' }}
                    </span>
                  </template>
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500 dark:text-gray-400">
                  {{ formatDate(source.created_at) }}
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                  <button
                    @click="viewSource(source)"
                    class="text-primary-600 hover:text-primary-900 dark:text-primary-400 dark:hover:text-primary-300 mr-3"
                  >
                    View
                  </button>
                  <template v-if="!source.is_global">
                    <!-- Actions for cold tier sources -->
                    <template v-if="source.tier === 'cold'">
                      <button
                        v-if="source.source_type !== 'external_parquet'"
                        @click="upgradeSource(source, 'warm')"
                        class="text-blue-600 hover:text-blue-900 dark:text-blue-400 dark:hover:text-blue-300 mr-3"
                        title="Sync to Parquet on R2 with local indexes"
                      >
                        Upgrade to Warm
                      </button>
                      <button
                        v-else
                        @click="buildIndex(source)"
                        class="text-blue-600 hover:text-blue-900 dark:text-blue-400 dark:hover:text-blue-300 mr-3"
                        title="Build local indexes for faster queries"
                      >
                        Build Index
                      </button>
                      <button
                        @click="upgradeSource(source, 'hot')"
                        class="text-green-600 hover:text-green-900 dark:text-green-400 dark:hover:text-green-300 mr-3"
                        title="Sync to ClickHouse for maximum speed"
                      >
                        Upgrade to Hot
                      </button>
                    </template>
                    <!-- Actions for warm tier sources -->
                    <template v-else-if="source.tier === 'warm'">
                      <button
                        @click="upgradeSource(source, 'hot')"
                        class="text-green-600 hover:text-green-900 dark:text-green-400 dark:hover:text-green-300 mr-3"
                        title="Sync to ClickHouse for maximum speed"
                      >
                        Upgrade to Hot
                      </button>
                      <button
                        @click="downgradeSource(source, 'cold')"
                        class="text-amber-600 hover:text-amber-900 dark:text-amber-400 dark:hover:text-amber-300 mr-3"
                        title="Downgrade to cold tier"
                      >
                        Downgrade to Cold
                      </button>
                    </template>
                    <!-- Actions for hot tier sources -->
                    <template v-else-if="source.tier === 'hot'">
                      <button
                        @click="downgradeSource(source, 'warm')"
                        class="text-amber-600 hover:text-amber-900 dark:text-amber-400 dark:hover:text-amber-300 mr-3"
                        title="Remove from ClickHouse, keep Parquet"
                      >
                        Downgrade to Warm
                      </button>
                      <button
                        @click="downgradeSource(source, 'cold')"
                        class="text-amber-600 hover:text-amber-900 dark:text-amber-400 dark:hover:text-amber-300 mr-3"
                        title="Downgrade to cold tier"
                      >
                        Downgrade to Cold
                      </button>
                    </template>
                  </template>
                  <button
                    @click="deleteSource(source)"
                    class="text-red-600 hover:text-red-900 dark:text-red-400 dark:hover:text-red-300"
                  >
                    Delete
                  </button>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </BaseCard>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import axios from 'axios';
import AppLayout from '@/Layouts/AppLayout.vue';
import BaseCard from '@/components/BaseCard.vue';
import BaseButton from '@/components/BaseButton.vue';
import { useAuth } from '@/composables/useAuth';

const route = useRoute();
const router = useRouter();
const { user } = useAuth();

const projectId = computed(() => route.params.id);
const project = computed(() => ({ id: projectId.value }));

const loading = ref(false);
const searchQuery = ref('');
const tierFilter = ref('all');
const sources = ref([]);

const filteredSources = computed(() => {
  let result = sources.value;
  
  if (searchQuery.value) {
    const query = searchQuery.value.toLowerCase();
    result = result.filter(s => 
      s.name.toLowerCase().includes(query) ||
      s.source_type.toLowerCase().includes(query)
    );
  }
  
  if (tierFilter.value !== 'all') {
    result = result.filter(s => s.tier === tierFilter.value);
  }
  
  return result;
});

const formatSourceType = (type) => {
  const typeMap = {
    'postgresql': 'PostgreSQL',
    'mysql': 'MySQL',
    'mongodb': 'MongoDB',
    'sqlserver': 'SQL Server',
    'clickhouse': 'ClickHouse',
    'sqlite': 'SQLite',
    'bigquery': 'BigQuery',
    'redshift': 'Redshift',
    'snowflake': 'Snowflake',
    'external_parquet': 'External Parquet',
  };
  return typeMap[type?.toLowerCase()] || type;
};

const getTierClass = (tier) => {
  switch (tier) {
    case 'hot':
      return 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200';
    case 'warm':
      return 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200';
    case 'cold':
    default:
      return 'bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200';
  }
};

const getTierLabel = (tier) => {
  switch (tier) {
    case 'hot':
      return 'Hot';
    case 'warm':
      return 'Warm';
    case 'cold':
    default:
      return 'Cold';
  }
};

const formatSyncScope = (source) => {
  if (source.sync_scope === 'time_based' && source.sync_scope_older_than_days != null) {
    return `Time-based (${source.sync_scope_older_than_days}d)`;
  }
  return 'Full';
};

const formatStorageTierPolicy = (source) => {
  if (source.is_global) return 'Managed';
  const policy = source.storage_tier_policy;
  if (!policy) return '-';
  if (policy.type === 'fixed') {
    const tier = policy.tier || 'cold';
    return `Fixed: ${getTierLabel(tier)}`;
  }
  if (policy.type === 'lifecycle' && policy.transitions?.length) {
    const parts = ['Hot'];
    for (const t of policy.transitions) {
      parts.push(`${t.after_days}d \u2192 ${getTierLabel(t.tier)}`);
    }
    return parts.join(' ');
  }
  if (policy.type === 'access_based') {
    const sensitivity = policy.sensitivity || 'moderate';
    const label = sensitivity.charAt(0).toUpperCase() + sensitivity.slice(1);
    return `Access-based: ${label}`;
  }
  return '-';
};

const formatJobType = (jobType) => {
  switch (jobType) {
    case 'upgrade_to_warm':
      return 'Upgrading to Warm...';
    case 'upgrade_to_hot':
      return 'Upgrading to Hot...';
    case 'downgrade_to_warm':
      return 'Downgrading to Warm...';
    case 'downgrade_to_cold':
      return 'Downgrading to Cold...';
    case 'index_build':
      return 'Building Index...';
    case 'sync':
      return 'Syncing...';
    default:
      return 'Processing...';
  }
};

// Update sync interval for a source
const updateSyncInterval = async (source, interval) => {
  try {
    await axios.put(`/api/projects/${projectId.value}/warehouse/sources/${source.id}/sync-interval`, {
      interval: interval || null
    });
    // Update local state
    source.sync_interval = interval || null;
  } catch (error) {
    console.error('Failed to update sync interval:', error);
    alert(`Failed to update sync interval: ${error.response?.data?.error || error.message}`);
  }
};

const formatDate = (dateStr) => {
  if (!dateStr) return '-';
  const date = new Date(dateStr);
  return date.toLocaleDateString('en-US', {
    year: 'numeric',
    month: 'short',
    day: 'numeric'
  });
};

const navigateToAdd = () => {
  router.push(`/p/${projectId.value}/warehouse/sources/add`);
};

const viewSource = (source) => {
  router.push(`/p/${projectId.value}/warehouse/sources/${source.id}`);
};

const deleteSource = async (source) => {
  if (!confirm(`Are you sure you want to delete "${source.name}"? This action cannot be undone.`)) {
    return;
  }
  
  try {
    await axios.delete(`/api/projects/${projectId.value}/warehouse/sources/${source.id}`);
    sources.value = sources.value.filter(s => s.id !== source.id);
  } catch (error) {
    console.error('Failed to delete source:', error);
    alert('Failed to delete source. Please try again.');
  }
};

// Upgrade a source to a higher tier (cold -> warm, cold -> hot, warm -> hot)
const upgradeSource = async (source, tier) => {
  if (!confirm(`Upgrade "${source.name}" to ${tier} tier?\n\n${tier === 'warm' ? 'This will sync data to Parquet on R2/S3 with local indexes for faster queries.' : 'This will sync data to ClickHouse for maximum query performance.'}`)) {
    return;
  }
  
  try {
    await axios.post(`/api/projects/${projectId.value}/warehouse/sources/${source.id}/upgrade`, {
      target_tier: tier
    });
    // Refresh sources list to show job progress
    await loadSources();
  } catch (error) {
    console.error(`Failed to upgrade source to ${tier}:`, error);
    alert(`Failed to upgrade source: ${error.response?.data?.error || error.message}`);
  }
};

// Build index for external Parquet sources
const buildIndex = async (source) => {
  if (!confirm(`Build indexes for "${source.name}"?\n\nThis will create local indexes for faster query filtering.`)) {
    return;
  }
  
  try {
    await axios.post(`/api/projects/${projectId.value}/warehouse/sources/${source.id}/upgrade`, {
      target_tier: 'warm'
    });
    // Refresh sources list to show job progress
    await loadSources();
  } catch (error) {
    console.error('Failed to build index:', error);
    alert(`Failed to build index: ${error.response?.data?.error || error.message}`);
  }
};

// Downgrade from hot to warm, or from warm/hot to cold
const downgradeSource = async (source, tier) => {
  if (!confirm(`Downgrade "${source.name}" to ${tier} tier?\n\n${tier === 'warm' ? 'This will remove data from ClickHouse but keep the Parquet cache.' : 'This will remove all cached data and return to live federated queries.'}`)) {
    return;
  }
  
  try {
    await axios.post(`/api/projects/${projectId.value}/warehouse/sources/${source.id}/downgrade`, {
      target_tier: tier
    });
    // Refresh sources list to show job progress
    await loadSources();
  } catch (error) {
    console.error(`Failed to downgrade source to ${tier}:`, error);
    alert(`Failed to downgrade source: ${error.response?.data?.error || error.message}`);
  }
};

// Polling interval reference
let pollingInterval = null;

// Check if any source has a job in progress
const hasJobsInProgress = computed(() => {
  return sources.value.some(source => source.sync_in_progress);
});

// Load sources from API
const loadSources = async () => {
  try {
    const response = await axios.get(`/api/projects/${projectId.value}/warehouse/sources`);
    sources.value = response.data;
  } catch (error) {
    console.error('Failed to load sources:', error);
  }
};

// Start/stop polling based on job status
const startPolling = () => {
  if (pollingInterval) return;
  pollingInterval = setInterval(async () => {
    await loadSources();
    // Stop polling when no jobs are in progress
    if (!hasJobsInProgress.value) {
      stopPolling();
    }
  }, 3000); // Poll every 3 seconds
};

const stopPolling = () => {
  if (pollingInterval) {
    clearInterval(pollingInterval);
    pollingInterval = null;
  }
};

// Watch for job status changes to start/stop polling
watch(hasJobsInProgress, (hasJobs) => {
  if (hasJobs) {
    startPolling();
  } else {
    stopPolling();
  }
});

async function loadSourcesOnProject() {
  loading.value = true;
  await loadSources();
  loading.value = false;
}

onMounted(loadSourcesOnProject);
watch(projectId, loadSourcesOnProject);

onUnmounted(() => {
  stopPolling();
});
</script>

<style scoped>
.spinner {
  animation: spin 1s linear infinite;
}

@keyframes spin {
  from {
    transform: rotate(0deg);
  }
  to {
    transform: rotate(360deg);
  }
}
</style>
