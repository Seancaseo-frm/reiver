<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-[1200px] mx-auto px-8 py-6">
      <!-- Stats Cards -->
      <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
        <BaseCard class="!p-4">
          <div class="flex items-center justify-between">
            <div>
              <p class="text-sm text-gray-500 dark:text-gray-400">Total Requests</p>
              <p class="text-2xl font-bold text-gray-900 dark:text-gray-100">{{ formatNumber(stats.total_requests) }}</p>
            </div>
            <div class="w-10 h-10 rounded-lg bg-blue-100 dark:bg-blue-900/30 flex items-center justify-center">
              <svg class="w-5 h-5 text-blue-600 dark:text-blue-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z" />
              </svg>
            </div>
          </div>
          <p class="text-xs text-gray-500 dark:text-gray-400 mt-2">Last 24 hours</p>
        </BaseCard>

        <BaseCard class="!p-4">
          <div class="flex items-center justify-between">
            <div>
              <p class="text-sm text-gray-500 dark:text-gray-400">Total Cost</p>
              <p class="text-2xl font-bold text-gray-900 dark:text-gray-100">${{ formatCost(stats.total_cost) }}</p>
            </div>
            <div class="w-10 h-10 rounded-lg bg-green-100 dark:bg-green-900/30 flex items-center justify-center">
              <svg class="w-5 h-5 text-green-600 dark:text-green-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
            </div>
          </div>
          <p class="text-xs text-gray-500 dark:text-gray-400 mt-2">Last 24 hours</p>
        </BaseCard>

        <BaseCard class="!p-4">
          <div class="flex items-center justify-between">
            <div>
              <p class="text-sm text-gray-500 dark:text-gray-400">Tokens Used</p>
              <p class="text-2xl font-bold text-gray-900 dark:text-gray-100">{{ formatNumber(stats.total_tokens) }}</p>
            </div>
            <div class="w-10 h-10 rounded-lg bg-purple-100 dark:bg-purple-900/30 flex items-center justify-center">
              <svg class="w-5 h-5 text-purple-600 dark:text-purple-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" />
              </svg>
            </div>
          </div>
          <p class="text-xs text-gray-500 dark:text-gray-400 mt-2">Last 24 hours</p>
        </BaseCard>

        <BaseCard class="!p-4">
          <div class="flex items-center justify-between">
            <div>
              <p class="text-sm text-gray-500 dark:text-gray-400">Avg Latency</p>
              <p class="text-2xl font-bold text-gray-900 dark:text-gray-100">{{ Math.round(stats.avg_latency_ms) }}ms</p>
            </div>
            <div class="w-10 h-10 rounded-lg bg-yellow-100 dark:bg-yellow-900/30 flex items-center justify-center">
              <svg class="w-5 h-5 text-yellow-600 dark:text-yellow-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
            </div>
          </div>
          <p class="text-xs text-gray-500 dark:text-gray-400 mt-2">Last 24 hours</p>
        </BaseCard>
      </div>

      <!-- Credit Balance & Fees -->
      <div v-if="stats.credit_balance_usd !== null || stats.platform_fee_total_usd !== null" class="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
        <BaseCard v-if="stats.credit_balance_usd !== null" class="!p-4">
          <div class="flex items-center justify-between">
            <div>
              <p class="text-sm text-gray-500 dark:text-gray-400">Credit Balance</p>
              <p class="text-2xl font-bold text-gray-900 dark:text-gray-100">${{ formatCost(stats.credit_balance_usd) }}</p>
            </div>
            <div class="w-10 h-10 rounded-lg bg-brand-100 dark:bg-brand-900/30 flex items-center justify-center">
              <svg class="w-5 h-5 text-brand-600 dark:text-brand-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 10h18M7 15h1m4 0h1m-7 4h12a3 3 0 003-3V8a3 3 0 00-3-3H6a3 3 0 00-3 3v8a3 3 0 003 3z" />
              </svg>
            </div>
          </div>
          <p class="text-xs text-gray-500 dark:text-gray-400 mt-2">Available for platform-key usage</p>
        </BaseCard>
        <BaseCard v-if="stats.platform_fee_total_usd !== null" class="!p-4">
          <div class="flex items-center justify-between">
            <div>
              <p class="text-sm text-gray-500 dark:text-gray-400">Uninvoiced Platform Fees</p>
              <p class="text-2xl font-bold text-gray-900 dark:text-gray-100">${{ formatCost(stats.platform_fee_total_usd) }}</p>
            </div>
            <div class="w-10 h-10 rounded-lg bg-orange-100 dark:bg-orange-900/30 flex items-center justify-center">
              <svg class="w-5 h-5 text-orange-600 dark:text-orange-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 14l6-6m-5.5.5h.01m4.99 5h.01M19 21V5a2 2 0 00-2-2H7a2 2 0 00-2 2v16l3.5-2 3.5 2 3.5-2 3.5 2z" />
              </svg>
            </div>
          </div>
          <p class="text-xs text-gray-500 dark:text-gray-400 mt-2">Accrued BYOK usage fees (3%)</p>
        </BaseCard>
      </div>

      <!-- Recent Sessions -->
      <BaseCard>
        <template #header>
          <div class="flex items-center justify-between">
            <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">Recent Sessions</h2>
            <router-link
              :to="`/p/${projectId}/llm/sessions`"
              class="text-sm text-primary-600 hover:text-primary-700 dark:text-primary-400 font-medium"
            >
              View all
            </router-link>
          </div>
        </template>
        <div v-if="loading" class="text-center py-8">
          <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full mx-auto"></div>
        </div>
        <div v-else-if="recentSessions.length === 0" class="text-center py-8 text-gray-500 dark:text-gray-400">
          <p>No sessions yet. Start using the Prompt Hub to see data here.</p>
        </div>
        <div v-else class="space-y-3">
          <router-link
            v-for="session in recentSessions"
            :key="session.session_id"
            :to="`/p/${projectId}/llm/sessions/${session.session_id}`"
            class="flex items-center justify-between p-3 border border-gray-200 dark:border-gray-700 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors block"
          >
            <div class="flex items-center gap-3">
              <div class="w-8 h-8 rounded-full bg-primary-100 dark:bg-primary-900/30 flex items-center justify-center">
                <svg class="w-4 h-4 text-primary-600 dark:text-primary-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z" />
                </svg>
              </div>
              <div>
                <p class="text-sm font-medium text-gray-900 dark:text-gray-100">{{ session.session_name || 'Unnamed session' }}</p>
                <p class="text-xs text-gray-500 dark:text-gray-400">{{ formatTime(session.first_request_time) }} · {{ session.request_count }} requests</p>
              </div>
            </div>
            <div class="flex items-center gap-4 text-sm">
              <span class="text-gray-600 dark:text-gray-400">{{ formatNumber(session.total_tokens) }} tokens</span>
              <span class="text-gray-600 dark:text-gray-400">${{ formatCost(session.total_cost_usd) }}</span>
              <span v-if="session.error_count > 0" class="text-red-600 dark:text-red-400 text-xs font-medium px-2 py-0.5 rounded-full" style="background:rgb(254 226 226/.3)">
                {{ session.error_count }} errors
              </span>
            </div>
          </router-link>
        </div>
      </BaseCard>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted, watch } from 'vue';
import { useRoute } from 'vue-router';
import axios from 'axios';
import AppLayout from '@/Layouts/AppLayout.vue';
import BaseCard from '@/components/BaseCard.vue';
import { useAuth } from '@/composables/useAuth';
import { usePageContext } from '@/composables/usePageContext';

const route = useRoute();
const { user, fetchUser } = useAuth();

const projectId = computed(() => route.params.id);
const project = computed(() => ({ id: projectId.value }));
const loading = ref(true);

const stats = ref({
  total_requests: 0,
  total_cost: 0,
  total_tokens: 0,
  avg_latency_ms: 0,
  credit_balance_usd: null,
  platform_fee_total_usd: null,
});

const recentSessions = ref([]);

const formatNumber = (num) => {
  if (num >= 1000000) {
    return (num / 1000000).toFixed(1) + 'M';
  }
  if (num >= 1000) {
    return (num / 1000).toFixed(1) + 'K';
  }
  return num.toLocaleString();
};

const formatCost = (cost) => {
  return parseFloat(cost || 0).toFixed(2);
};

const formatTime = (timestamp) => {
  const date = new Date(timestamp);
  const now = new Date();
  const diff = now - date;
  
  if (diff < 60000) return 'Just now';
  if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`;
  if (diff < 86400000) return `${Math.floor(diff / 3600000)}h ago`;
  return date.toLocaleDateString();
};

const fetchData = async () => {
  if (!projectId.value) return;
  loading.value = true;
  try {
    // Fetch LLM metrics
    const [metricsRes, sessionsRes] = await Promise.all([
      axios.get(`/api/llm/metrics/overview`, { params: { project_id: projectId.value } }).catch(() => ({ data: {} })),
      axios.get(`/api/llm/sessions`, { params: { project_id: projectId.value, limit: 5 } }).catch(() => ({ data: { sessions: [] } })),
    ]);
    const m = metricsRes.data;
    const totalTokens = (m?.total_input_tokens ?? 0) + (m?.total_output_tokens ?? 0);
    stats.value = {
      total_requests: m?.total_requests || 0,
      total_cost: m?.total_cost_usd ?? 0,
      total_tokens: totalTokens,
      avg_latency_ms: m?.avg_latency_ms ?? 0,
      credit_balance_usd: m?.credit_balance_usd ?? null,
      platform_fee_total_usd: m?.platform_fee_total_usd ?? null,
    };
    
    recentSessions.value = sessionsRes.data?.sessions || [];
  } catch (error) {
    console.error('Failed to fetch overview data:', error);
  } finally {
    loading.value = false;
  }
};

const { setPageSnapshot, clearPageSnapshot } = usePageContext();

watch([stats, recentSessions], () => {
  if (!stats.value.total_requests && !recentSessions.value.length) return;
  setPageSnapshot({
    page: 'Prompt Hub Overview',
    time_range: '24h',
    stats: {
      total_requests: stats.value.total_requests,
      total_cost_usd: stats.value.total_cost,
      total_tokens: stats.value.total_tokens,
      avg_latency_ms: stats.value.avg_latency_ms,
    },
    recent_sessions: (recentSessions.value || []).slice(0, 5).map(s => ({
      session_name: s.session_name, total_tokens: s.total_tokens,
      total_cost_usd: s.total_cost_usd, request_count: s.request_count,
    })),
  });
}, { deep: true });

onMounted(async () => {
  await fetchUser();
  await fetchData();
});

watch(projectId, async () => {
  await fetchUser();
  await fetchData();
});

onUnmounted(() => clearPageSnapshot());
</script>

<style scoped>
.spinner {
  animation: spin 1s linear infinite;
}

@keyframes spin {
  from { transform: rotate(0deg); }
  to { transform: rotate(360deg); }
}
</style>
