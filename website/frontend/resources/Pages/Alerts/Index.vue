<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-[1200px] mx-auto px-8 py-6">
      <!-- Header -->
      <div class="mb-6">
        <div>
          <h1 class="text-2xl font-semibold text-gray-900">Alert Rules</h1>
          <p class="text-sm text-gray-500 dark:text-gray-400 mt-1">
            Manage alert rules to monitor your metrics and get notified when thresholds are exceeded
          </p>
        </div>
        <div class="flex items-center gap-3">
          <router-link
            :to="`/p/${projectId}/alerts/history`"
            class="px-4 py-2 text-sm font-medium text-gray-700 dark:text-gray-300 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-md hover:bg-gray-50 dark:hover:bg-gray-700 focus:outline-none focus:ring-2 focus:ring-primary-500"
          >
            View History
          </router-link>
          <router-link
            :to="`/p/${projectId}/alerts/new`"
            class="inline-flex items-center gap-2 px-4 py-2 bg-indigo-600 text-white rounded-md hover:bg-indigo-700 font-medium text-sm"
          >
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" />
            </svg>
            New Alert Rule
          </router-link>
        </div>
      </div>

      <!-- Empty State (when no alerts exist) -->
      <AlertsEmptyState
        v-if="!loading && alertRules.length === 0"
        :projectId="projectId"
      />

      <!-- Alert Rules Table -->
      <BaseCard v-else-if="!loading" class="mt-6">
        <template #header>
          <div class="flex items-center justify-between">
            <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">
              Alert Rules ({{ alertRules.length }})
            </h2>
            <div class="flex items-center gap-2">
              <button
                @click="refreshRules"
                :disabled="loading"
                class="p-2 rounded-md text-gray-500 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700 disabled:opacity-50 transition-colors"
                title="Refresh"
              >
                <svg
                  :class="['w-5 h-5', { 'animate-spin': loading }]"
                  fill="none"
                  stroke="currentColor"
                  viewBox="0 0 24 24"
                >
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
                </svg>
              </button>
            </div>
          </div>
        </template>

        <AlertRulesTable
          :rules="alertRules"
          @row-click="(rule) => router.push(`/p/${projectId}/alerts/${rule.id}/edit`)"
          @toggle="toggleRule"
          @edit="(rule) => router.push(`/p/${projectId}/alerts/${rule.id}/edit`)"
          @delete="deleteRule"
        />
      </BaseCard>

      <!-- Loading State -->
      <BaseCard v-else class="mt-6">
        <div class="text-center py-12">
          <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full mx-auto mb-3"></div>
          <p class="text-sm text-gray-500 dark:text-gray-400">Loading alert rules...</p>
        </div>
      </BaseCard>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import { useAuth } from '../../composables/useAuth';
import { usePageContext } from '../../composables/usePageContext';
import axios from 'axios';
import AppLayout from '../../Layouts/AppLayout.vue';
import BaseCard from '../../components/BaseCard.vue';
import AlertRulesTable from '../../components/alerts/AlertRulesTable.vue';
import AlertsEmptyState from '../../components/alerts/AlertsEmptyState.vue';

const route = useRoute();
const router = useRouter();
const { user, fetchUser } = useAuth();

const projectId = computed(() => route.params.id);
const project = ref(null);
const alertRules = ref([]);
const loading = ref(true);

// Load alert rules
const loadRules = async () => {
  loading.value = true;
  try {
    const response = await axios.get('/api/alerting/rules', {
      params: { project_id: projectId.value },
      headers: { 'x-project-id': projectId.value },
    });
    alertRules.value = response.data || [];
  } catch (error) {
    console.error('Failed to load alert rules:', error);
    // Show error message (could use a toast notification here)
    alert('Failed to load alert rules. Please try again.');
  } finally {
    loading.value = false;
  }
};

// Refresh rules
const refreshRules = () => {
  loadRules();
};

// Toggle rule enabled/disabled
const toggleRule = async (rule) => {
  if (!confirm(`Are you sure you want to ${rule.enabled ? 'disable' : 'enable'} this alert rule?`)) {
    return;
  }

  try {
    await axios.put(
      `/api/alerting/rules/${rule.id}`,
      { enabled: !rule.enabled },
      { headers: { 'x-project-id': projectId.value } }
    );
    
    // Update local state
    rule.enabled = !rule.enabled;
  } catch (error) {
    console.error('Failed to toggle rule:', error);
    alert(`Failed to ${rule.enabled ? 'disable' : 'enable'} alert rule. Please try again.`);
  }
};

// Delete rule
const deleteRule = async (rule) => {
  if (!confirm(`Are you sure you want to delete "${rule.name}"? This action cannot be undone.`)) {
    return;
  }

  try {
    await axios.delete(`/api/alerting/rules/${rule.id}`, {
      headers: { 'x-project-id': projectId.value },
    });
    
    // Remove from local state
    alertRules.value = alertRules.value.filter((r) => r.id !== rule.id);
  } catch (error) {
    console.error('Failed to delete rule:', error);
    alert('Failed to delete alert rule. Please try again.');
  }
};

const { setPageSnapshot, clearPageSnapshot } = usePageContext();

watch(alertRules, () => {
  const rules = alertRules.value;
  setPageSnapshot({
    page: 'Alert Rules',
    total_rules: rules.length,
    enabled: rules.filter(r => r.enabled).length,
    disabled: rules.filter(r => !r.enabled).length,
    rules: rules.slice(0, 10).map(r => ({
      name: r.name,
      enabled: r.enabled,
    })),
  });
}, { deep: true });

onMounted(async () => {
  try {
    await fetchUser();
    const projectResponse = await axios.get(`/api/projects/${projectId.value}`);
    project.value = projectResponse.data;
    loadRules();
  } catch (error) {
    console.error('Failed to load project:', error);
    router.push('/projects');
  }
});

onUnmounted(() => clearPageSnapshot());
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
