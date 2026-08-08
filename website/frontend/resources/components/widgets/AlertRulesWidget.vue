<template>
  <BaseCard class="h-full flex flex-col">
    <template #header>
      <div class="flex items-center justify-between">
        <div class="flex items-center gap-2">
          <svg class="w-5 h-5 text-primary-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 17h5l-1.405-1.405A2.032 2.032 0 0118 14.158V11a6.002 6.002 0 00-4-5.659V5a2 2 0 10-4 0v.341C7.67 6.165 6 8.388 6 11v3.159c0 .538-.214 1.055-.595 1.436L4 17h5m6 0v1a3 3 0 11-6 0v-1m6 0H9" />
          </svg>
          <h3 class="text-lg font-semibold text-gray-900">Alert Rules</h3>
        </div>
        <div class="flex items-center gap-2">
          <button
            @click="refresh"
            :disabled="loading"
            class="p-1.5 rounded-md text-gray-500 hover:bg-gray-100 disabled:opacity-50 transition-colors"
            title="Refresh"
          >
            <svg
              :class="['w-4 h-4', { 'animate-spin': loading }]"
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
            >
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
            </svg>
          </button>
          <router-link
            v-if="projectId"
            :to="`/p/${projectId}/alerts`"
            class="text-sm text-primary-600 hover:text-primary-700"
          >
            View All
          </router-link>
        </div>
      </div>
    </template>

    <div class="flex-1 overflow-y-auto custom-scrollbar">
      <!-- Loading State -->
      <div v-if="loading && rules.length === 0" class="flex items-center justify-center py-12">
        <div class="spinner w-6 h-6 border-2 border-primary-600 border-t-transparent rounded-full"></div>
      </div>

      <!-- Error State -->
      <div v-else-if="error" class="text-center py-12">
        <p class="text-sm text-red-600">{{ error }}</p>
        <button
          @click="refresh"
          class="mt-2 text-sm text-primary-600 hover:text-primary-700"
        >
          Try Again
        </button>
      </div>

      <!-- Empty State -->
      <div v-else-if="rules.length === 0" class="text-center py-12">
        <svg class="w-12 h-12 mx-auto mb-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 17h5l-1.405-1.405A2.032 2.032 0 0118 14.158V11a6.002 6.002 0 00-4-5.659V5a2 2 0 10-4 0v.341C7.67 6.165 6 8.388 6 11v3.159c0 .538-.214 1.055-.595 1.436L4 17h5m6 0v1a3 3 0 11-6 0v-1m6 0H9" />
        </svg>
        <p class="text-sm text-gray-500 mb-4">No alert rules</p>
        <router-link
          v-if="projectId"
          :to="`/p/${projectId}/alerts/new`"
          class="text-sm text-primary-600 hover:text-primary-700"
        >
          Create Alert Rule
        </router-link>
      </div>

      <!-- Alert Rules List -->
      <div v-else class="divide-y divide-gray-200">
        <div
          v-for="rule in displayRules"
          :key="rule.id"
          class="p-4 hover:bg-gray-50 transition-colors cursor-pointer"
          @click="() => projectId && $router.push(`/p/${projectId}/alerts/${rule.id}/edit`)"
        >
          <div class="flex items-start justify-between">
            <div class="flex-1 min-w-0">
              <div class="flex items-center gap-2 mb-1">
                <h4 class="text-sm font-medium text-gray-900 truncate">
                  {{ rule.name }}
                </h4>
                <AlertStatusBadge type="enabled" :value="rule.enabled" />
                <AlertStatusBadge type="health" :value="rule.health" />
              </div>
              <p v-if="rule.description" class="text-xs text-gray-500 truncate mb-2">
                {{ rule.description }}
              </p>
              <div class="flex items-center gap-4 text-xs text-gray-500">
                <span class="font-mono">{{ getQueryLabel(rule) }}</span>
                <span v-if="rule.threshold_value !== null && rule.threshold_value !== undefined">
                  {{ formatCondition(rule) }}
                </span>
                <span v-if="rule.notification_channels?.length">
                  {{ rule.notification_channels.length }} channel{{ rule.notification_channels.length !== 1 ? 's' : '' }}
                </span>
              </div>
            </div>
            <button
              @click.stop="toggleRule(rule)"
              class="ml-2 p-1.5 rounded-md text-gray-400 hover:text-gray-600 hover:bg-gray-100 transition-colors"
              :title="rule.enabled ? 'Disable' : 'Enable'"
            >
              <svg v-if="rule.enabled" class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M18.364 18.364A9 9 0 005.636 5.636m12.728 12.728A9 9 0 015.636 5.636m12.728 12.728L5.636 5.636" />
              </svg>
              <svg v-else class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
              </svg>
            </button>
          </div>
        </div>
      </div>
    </div>

    <!-- Footer with stats -->
    <div v-if="rules.length > 0 && !loading" class="px-4 py-3 border-t border-gray-200 bg-gray-50">
      <div class="flex items-center justify-between text-xs text-gray-500">
        <span>{{ summaryText }}</span>
        <span>{{ lastRefreshText }}</span>
      </div>
    </div>
  </BaseCard>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted } from 'vue';
import { useRouter } from 'vue-router';
import { useAlertRules } from '../../composables/useAlertRules';
import { formatDistanceToNow } from 'date-fns';
import BaseCard from '../BaseCard.vue';
import AlertStatusBadge from '../alerts/AlertStatusBadge.vue';

const props = defineProps({
  projectId: {
    type: String,
    required: true,
  },
  limit: {
    type: Number,
    default: 5,
  },
  autoRefresh: {
    type: Boolean,
    default: true,
  },
  refreshInterval: {
    type: Number,
    default: 60000, // 1 minute
  },
});

const router = useRouter();

// Use the composable
const {
  rules,
  loading,
  error,
  enabledRules,
  disabledRules,
  rulesByHealth,
  loadRules,
  refreshRules,
  toggleRule: toggleRuleComposable,
  clearError,
} = useAlertRules(props.projectId);

const lastRefresh = ref(new Date());
let refreshTimer = null;

// Computed properties
const displayRules = computed(() => {
  // Show firing/unhealthy rules first, then enabled rules, then disabled
  const sorted = [...rules.value].sort((a, b) => {
    // Prioritize unhealthy rules
    if (a.health === 'err' && b.health !== 'err') return -1;
    if (a.health !== 'err' && b.health === 'err') return 1;
    
    // Then prioritize enabled rules
    if (a.enabled && !b.enabled) return -1;
    if (!a.enabled && b.enabled) return 1;
    
    // Finally sort by name
    return a.name.localeCompare(b.name);
  });
  
  return sorted.slice(0, props.limit);
});

const summaryText = computed(() => {
  const enabled = enabledRules.value.length;
  const total = rules.value.length;
  const unhealthy = rulesByHealth.value.err?.length || 0;
  
  const parts = [`${total} rule${total !== 1 ? 's' : ''}`];
  if (enabled > 0) parts.push(`${enabled} enabled`);
  if (unhealthy > 0) parts.push(`${unhealthy} unhealthy`);
  
  return parts.join(' • ');
});

const lastRefreshText = computed(() => {
  return `Updated ${formatDistanceToNow(lastRefresh.value, { addSuffix: true })}`;
});

const getQueryLabel = (rule) => {
  const qc = rule.query_config;
  if (!qc) return 'N/A';
  return qc.metric_name || 'N/A';
};

const formatCondition = (rule) => {
  if (rule.threshold_value !== null && rule.threshold_value !== undefined) {
    const opMap = {
      above: '>',
      below: '<',
      equal: '==',
      not_equal: '!=',
      above_or_equal: '>=',
      below_or_equal: '<=',
    };
    const op = opMap[rule.compare_op] || rule.compare_op;
    return `${op} ${rule.threshold_value}`;
  }
  return '';
};

// Load rules
const load = async () => {
  try {
    await loadRules({ limit: props.limit * 2 }); // Load a bit more for better sorting
    lastRefresh.value = new Date();
    clearError();
  } catch (err) {
    console.error('Failed to load alert rules in widget:', err);
  }
};

// Refresh handler
const refresh = async () => {
  await refreshRules({ limit: props.limit * 2 });
  lastRefresh.value = new Date();
};

// Toggle rule handler
const toggleRule = async (rule) => {
  try {
    await toggleRuleComposable(rule.id);
  } catch (err) {
    console.error('Failed to toggle rule:', err);
    alert(`Failed to ${rule.enabled ? 'disable' : 'enable'} alert rule: ${err.message || 'Unknown error'}`);
  }
};

// Setup auto-refresh
const setupAutoRefresh = () => {
  if (props.autoRefresh && props.refreshInterval > 0) {
    refreshTimer = setInterval(() => {
      refresh();
    }, props.refreshInterval);
  }
};

// Cleanup auto-refresh
const cleanupAutoRefresh = () => {
  if (refreshTimer) {
    clearInterval(refreshTimer);
    refreshTimer = null;
  }
};

onMounted(() => {
  load();
  setupAutoRefresh();
});

onUnmounted(() => {
  cleanupAutoRefresh();
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
