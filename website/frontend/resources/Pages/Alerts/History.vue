<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-[1200px] mx-auto px-8 py-6">
      <!-- Header -->
      <div class="mb-6">
        <div>
          <h1 class="text-2xl font-semibold text-gray-900">Alert History</h1>
          <p class="text-sm text-gray-500 dark:text-gray-400 mt-1">
            View alert firing and resolution history for your project
          </p>
        </div>
        <div>
          <button
            @click="refreshAlerts"
            :disabled="loading"
            class="px-4 py-2 text-sm font-medium text-gray-700 dark:text-gray-300 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-md hover:bg-gray-50 dark:hover:bg-gray-700 focus:outline-none focus:ring-2 focus:ring-primary-500 disabled:opacity-50 disabled:cursor-not-allowed inline-flex items-center gap-2"
          >
            <svg
              :class="['w-4 h-4', { 'animate-spin': loading }]"
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
            >
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
            </svg>
            Refresh
          </button>
        </div>
      </div>

      <!-- Statistics Cards -->
      <div class="grid grid-cols-1 md:grid-cols-4 gap-6 mb-6">
        <BaseCard>
          <div class="flex items-center">
            <div class="flex-shrink-0 bg-red-100 dark:bg-red-900/30 rounded-md p-3">
              <svg class="w-6 h-6 text-red-600 dark:text-red-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
              </svg>
            </div>
            <div class="ml-4 flex-1">
              <p class="text-sm font-medium text-gray-500 dark:text-gray-400">Firing</p>
              <p class="text-2xl font-semibold text-gray-900 dark:text-gray-100">{{ stats.firing }}</p>
            </div>
          </div>
        </BaseCard>

        <BaseCard>
          <div class="flex items-center">
            <div class="flex-shrink-0 bg-orange-100 dark:bg-orange-900/30 rounded-md p-3">
              <svg class="w-6 h-6 text-orange-600 dark:text-orange-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
            </div>
            <div class="ml-4 flex-1">
              <p class="text-sm font-medium text-gray-500 dark:text-gray-400">Pending</p>
              <p class="text-2xl font-semibold text-gray-900 dark:text-gray-100">{{ stats.pending }}</p>
            </div>
          </div>
        </BaseCard>

        <BaseCard>
          <div class="flex items-center">
            <div class="flex-shrink-0 bg-green-100 dark:bg-green-900/30 rounded-md p-3">
              <svg class="w-6 h-6 text-green-600 dark:text-green-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
            </div>
            <div class="ml-4 flex-1">
              <p class="text-sm font-medium text-gray-500 dark:text-gray-400">Resolved</p>
              <p class="text-2xl font-semibold text-gray-900 dark:text-gray-100">{{ stats.resolved }}</p>
            </div>
          </div>
        </BaseCard>

        <BaseCard>
          <div class="flex items-center">
            <div class="flex-shrink-0 bg-blue-100 dark:bg-blue-900/30 rounded-md p-3">
              <svg class="w-6 h-6 text-blue-600 dark:text-blue-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
              </svg>
            </div>
            <div class="ml-4 flex-1">
              <p class="text-sm font-medium text-gray-500 dark:text-gray-400">Total</p>
              <p class="text-2xl font-semibold text-gray-900 dark:text-gray-100">{{ stats.total }}</p>
            </div>
          </div>
        </BaseCard>
      </div>

      <!-- Filters -->
      <BaseCard class="mb-6">
        <template #header>
          <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">Filters</h2>
        </template>
        <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
          <!-- Rule Filter -->
          <div>
            <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
              Alert Rule
            </label>
            <select
              v-model="filters.rule_id"
              @change="applyFilters"
              class="w-full px-3 py-2 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 text-gray-900 dark:text-gray-100 rounded-md focus:outline-none focus:ring-2 focus:ring-primary-500"
            >
              <option value="">All Rules</option>
              <option v-for="rule in rules" :key="rule.id" :value="rule.id">
                {{ rule.name }}
              </option>
            </select>
          </div>

          <!-- State Filter -->
          <div>
            <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
              State
            </label>
            <select
              v-model="filters.state"
              @change="applyFilters"
              class="w-full px-3 py-2 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 text-gray-900 dark:text-gray-100 rounded-md focus:outline-none focus:ring-2 focus:ring-primary-500"
            >
              <option value="">All States</option>
              <option value="firing">Firing</option>
              <option value="pending">Pending</option>
              <option value="resolved">Resolved</option>
            </select>
          </div>

          <!-- Time Range Filter -->
          <div>
            <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
              Time Range
            </label>
            <select
              v-model="filters.timeRange"
              @change="applyFilters"
              class="w-full px-3 py-2 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 text-gray-900 dark:text-gray-100 rounded-md focus:outline-none focus:ring-2 focus:ring-primary-500"
            >
              <option value="1h">Last Hour</option>
              <option value="24h">Last 24 Hours</option>
              <option value="7d">Last 7 Days</option>
              <option value="30d">Last 30 Days</option>
              <option value="all">All Time</option>
            </select>
          </div>
        </div>
      </BaseCard>

      <!-- Suggested root cause (from dominant OTLP log patterns in the selected time range) -->
      <BaseCard class="mb-6">
        <template #header>
          <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">Suggested root cause</h2>
        </template>
        <div v-if="rootCauseLoading" class="text-sm text-gray-500 dark:text-gray-400">Loading...</div>
        <div v-else-if="rootCause && rootCause.suggestions.length > 0" class="space-y-2">
          <div
            v-for="(s, i) in rootCause.suggestions"
            :key="i"
            class="flex flex-wrap items-baseline gap-2"
          >
            <span class="font-mono text-sm text-gray-900 dark:text-gray-100 truncate max-w-xl" :title="s.pattern">{{ s.pattern }}</span>
            <span class="text-sm text-gray-500 dark:text-gray-400">
              {{ (s.pct * 100).toFixed(1) }}% of {{ rootCause.total_logs.toLocaleString() }} logs
            </span>
          </div>
        </div>
        <div v-else class="text-sm text-gray-500 dark:text-gray-400">
          No dominant log pattern in this period, or no OTLP logs.
        </div>
      </BaseCard>

      <!-- Loading State -->
      <BaseCard v-if="loading && alerts.length === 0">
        <div class="text-center py-12">
          <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full mx-auto mb-3"></div>
          <p class="text-sm text-gray-500 dark:text-gray-400">Loading alert history...</p>
        </div>
      </BaseCard>

      <!-- Empty State -->
      <BaseCard v-else-if="!loading && alerts.length === 0">
        <div class="text-center py-12">
          <svg class="w-16 h-16 mx-auto mb-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
          <h3 class="text-lg font-medium text-gray-900 dark:text-gray-100 mb-2">No alerts found</h3>
          <p class="text-sm text-gray-500 dark:text-gray-400">
            No alerts match your current filters. Try adjusting the filters above.
          </p>
        </div>
      </BaseCard>

      <!-- Alert Timeline -->
      <BaseCard v-else>
        <template #header>
          <div class="flex items-center justify-between">
            <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">
              Alert Timeline ({{ alerts.length }})
            </h2>
          </div>
        </template>

        <div class="space-y-4">
          <div
            v-for="alert in alerts"
            :key="alert.id"
            class="relative pl-8 pb-8 border-l-2 border-gray-200 dark:border-gray-700 last:border-l-0 last:pb-0"
            :class="getTimelineBorderClass(alert.state)"
          >
            <!-- Timeline Dot -->
            <div
              class="absolute -left-2 w-4 h-4 rounded-full border-2 border-white dark:border-gray-900"
              :class="getTimelineDotClass(alert.state)"
            ></div>

            <!-- Alert Content -->
            <div class="flex items-start justify-between">
              <div class="flex-1 min-w-0">
                <div class="flex items-center gap-3 mb-2">
                  <AlertStatusBadge type="state" :value="alert.state" />
                  <span class="text-sm font-medium text-gray-900 dark:text-gray-100">
                    {{ getRuleName(alert.rule_id) }}
                  </span>
                  <span
                    v-if="alert.is_missing"
                    class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-yellow-100 dark:bg-yellow-900/30 text-yellow-800 dark:text-yellow-200"
                  >
                    Missing Data
                  </span>
                </div>

                <div v-if="alert.annotations?.summary || alert.annotations?.description" class="mb-2">
                  <p v-if="alert.annotations?.summary" class="text-sm font-medium text-gray-900 dark:text-gray-100">
                    {{ alert.annotations.summary }}
                  </p>
                  <p v-if="alert.annotations?.description" class="text-sm text-gray-600 dark:text-gray-400 mt-1">
                    {{ alert.annotations.description }}
                  </p>
                </div>

                <div class="flex flex-wrap items-center gap-4 text-xs text-gray-500 dark:text-gray-400 mt-2">
                  <span v-if="alert.value !== null && alert.value !== undefined" class="font-mono">
                    Value: {{ alert.value.toFixed(2) }}
                  </span>
                  <span v-if="alert.fingerprint" class="font-mono text-xs">
                    Fingerprint: {{ alert.fingerprint.substring(0, 8) }}...
                  </span>
                </div>

                <!-- Labels -->
                <div v-if="alert.labels && Object.keys(alert.labels).length > 0" class="flex flex-wrap gap-2 mt-3">
                  <span
                    v-for="(value, key) in alert.labels"
                    :key="key"
                    class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-gray-100 dark:bg-gray-700 text-gray-800 dark:text-gray-200"
                  >
                    {{ key }}: {{ value }}
                  </span>
                </div>
              </div>

              <div class="ml-4 text-right text-xs text-gray-500 dark:text-gray-400 whitespace-nowrap">
                <div v-if="alert.fired_at" class="mb-1">
                  <div class="font-medium">Fired:</div>
                  <div>{{ formatRelativeTime(alert.fired_at) }}</div>
                </div>
                <div v-if="alert.resolved_at" class="mb-1">
                  <div class="font-medium">Resolved:</div>
                  <div>{{ formatRelativeTime(alert.resolved_at) }}</div>
                </div>
                <div v-else-if="alert.active_at">
                  <div class="font-medium">Active:</div>
                  <div>{{ formatRelativeTime(alert.active_at) }}</div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </BaseCard>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import { useAuth } from '../../composables/useAuth';
import { useAlertQuery } from '../../composables/useAlertQuery';
import { useAlertRules } from '../../composables/useAlertRules';
import { formatDistanceToNow } from 'date-fns';
import axios from 'axios';
import AppLayout from '../../Layouts/AppLayout.vue';
import BaseCard from '../../components/BaseCard.vue';
import AlertStatusBadge from '../../components/alerts/AlertStatusBadge.vue';

const route = useRoute();
const router = useRouter();
const { user, fetchUser } = useAuth();

const projectId = computed(() => route.params.id);
const project = ref(null);

// Use composables - pass computed value but access .value inside functions
const {
  alerts,
  loading,
  error,
  loadAlerts,
  refreshAlerts: refreshAlertsQuery,
  getStats,
} = useAlertQuery(projectId.value);

const {
  rules,
  loadRules,
} = useAlertRules(projectId.value);

// Filters
const filters = ref({
  rule_id: '',
  state: '',
  timeRange: '24h',
});

// Suggested root cause (dominant OTLP patterns in time range)
const rootCause = ref(null);
const rootCauseLoading = ref(false);

function timeRangeToMs(range) {
  const now = Date.now();
  switch (range) {
    case '1h': return { start: now - 3600 * 1000, end: now };
    case '24h': return { start: now - 24 * 3600 * 1000, end: now };
    case '7d': return { start: now - 7 * 24 * 3600 * 1000, end: now };
    case '30d': return { start: now - 30 * 24 * 3600 * 1000, end: now };
    case 'all':
    default: return { start: now - 30 * 24 * 3600 * 1000, end: now };
  }
}

async function loadRootCause() {
  rootCauseLoading.value = true;
  rootCause.value = null;
  try {
    const { start, end } = timeRangeToMs(filters.value.timeRange);
    const { data } = await axios.get(`/api/projects/${projectId.value}/root-cause-suggestions`, {
      params: { start, end },
    });
    rootCause.value = data;
  } catch (err) {
    console.warn('Failed to load root cause suggestions:', err);
  } finally {
    rootCauseLoading.value = false;
  }
}

// Computed stats
const stats = computed(() => {
  const currentStats = getStats();
  // Filter stats based on current filters if needed
  if (filters.value.state) {
    return {
      total: currentStats.total,
      firing: filters.value.state === 'firing' ? currentStats.firing : 0,
      pending: filters.value.state === 'pending' ? currentStats.pending : 0,
      resolved: filters.value.state === 'resolved' ? currentStats.resolved : 0,
    };
  }
  return currentStats;
});

// Get rule name by ID
const ruleNames = ref({});
const getRuleName = (ruleId) => {
  return ruleNames.value[ruleId] || 'Unknown Rule';
};

// Format relative time
const formatRelativeTime = (dateString) => {
  try {
    const date = new Date(dateString);
    return formatDistanceToNow(date, { addSuffix: true });
  } catch {
    return dateString;
  }
};

// Timeline styling
const getTimelineBorderClass = (state) => {
  const classMap = {
    firing: 'border-red-500 dark:border-red-400',
    pending: 'border-orange-500 dark:border-orange-400',
    resolved: 'border-green-500 dark:border-green-400',
  };
  return classMap[state] || 'border-gray-300 dark:border-gray-600';
};

const getTimelineDotClass = (state) => {
  const classMap = {
    firing: 'bg-red-500 border-red-500',
    pending: 'bg-orange-500 border-orange-500',
    resolved: 'bg-green-500 border-green-500',
  };
  return classMap[state] || 'bg-gray-400 border-gray-400';
};

// Apply filters
const applyFilters = async () => {
  await Promise.all([
    loadAlerts({
      rule_id: filters.value.rule_id || undefined,
      state: filters.value.state || undefined,
      limit: 100,
    }),
    loadRootCause(),
  ]);
};

// Refresh alerts and root cause
const refreshAlerts = async () => {
  await refreshAlertsQuery({
    rule_id: filters.value.rule_id || undefined,
    state: filters.value.state || undefined,
    limit: 100,
  });
  await loadRootCause();
};

// Load initial data
const loadData = async () => {
  // Load rules first to populate filter and get rule names
  await loadRules();
  
  // Build rule names map
  const nameMap = {};
  rules.value.forEach((rule) => {
    nameMap[rule.id] = rule.name;
  });
  ruleNames.value = nameMap;

  // Load alerts and root cause suggestions
  await Promise.all([
    loadAlerts({ project_id: projectId.value, limit: 100 }),
    loadRootCause(),
  ]);
};

onMounted(async () => {
  try {
    // Fetch user if not already cached
    await fetchUser();
    
    // Fetch project data
    const projectResponse = await axios.get(`/api/projects/${projectId.value}`);
    project.value = projectResponse.data;
    
    // Load alerts and rules data
    loadData();
  } catch (error) {
    console.error('Failed to load project:', error);
    router.push('/projects');
  }
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
