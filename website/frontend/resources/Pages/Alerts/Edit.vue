<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-[1200px] mx-auto px-8 py-6">
      <!-- Page Header -->
      <div class="mb-6">
        <div>
          <router-link
            :to="`/p/${projectId}/alerts`"
            class="text-primary-600 dark:text-primary-400 hover:text-primary-700 dark:hover:text-primary-300 text-sm font-medium inline-flex items-center gap-2 mb-4"
          >
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 19l-7-7m0 0l7-7m-7 7h18" />
            </svg>
            Back to Alerts
          </router-link>
          <h1 class="text-2xl font-semibold text-gray-900">Edit Alert Rule</h1>
          <p class="text-sm text-gray-500 dark:text-gray-400 mt-1">
            Update your alert rule configuration
          </p>
        </div>
      </div>

      <!-- Loading State -->
      <div v-if="loading">
        <BaseCard>
          <div class="text-center py-12">
            <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full mx-auto mb-3"></div>
            <p class="text-sm text-gray-500 dark:text-gray-400">Loading alert rule...</p>
          </div>
        </BaseCard>
      </div>

      <!-- Error State -->
      <div v-else-if="loadError">
        <BaseCard>
          <div class="text-center py-12">
            <svg class="w-16 h-16 mx-auto mb-4 text-red-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
            <h3 class="text-lg font-medium text-gray-900 dark:text-gray-100 mb-2">Failed to load alert rule</h3>
            <p class="text-sm text-gray-500 dark:text-gray-400 mb-4">{{ loadError }}</p>
            <router-link
              :to="`/p/${projectId}/alerts`"
              class="inline-flex items-center gap-2 px-4 py-2 bg-indigo-600 text-white rounded-md hover:bg-indigo-700 font-medium text-sm"
            >
              Back to Alert Rules
            </router-link>
          </div>
        </BaseCard>
      </div>

      <!-- Edit Form -->
      <BaseCard v-else>
        <form @submit.prevent="handleSubmit" class="space-y-6">
          
          <!-- Alert Name & Description -->
          <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div>
              <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Alert Name <span class="text-red-500">*</span>
              </label>
              <input
                v-model="formData.name"
                type="text"
                placeholder="e.g., High Error Rate"
                class="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:ring-primary-500 focus:border-primary-500"
                :class="{ 'border-red-500': errors.name }"
              />
              <p v-if="errors.name" class="mt-1 text-sm text-red-500">{{ errors.name }}</p>
            </div>
            <div>
              <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Description
              </label>
              <input
                v-model="formData.description"
                type="text"
                placeholder="Optional description"
                class="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:ring-primary-500 focus:border-primary-500"
              />
            </div>
          </div>

          <!-- Alert Type -->
          <div>
            <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
              Alert Type <span class="text-red-500">*</span>
            </label>
            <div class="flex gap-4">
              <label class="flex items-center cursor-pointer">
                <input
                  v-model="formData.alertType"
                  type="radio"
                  value="metric"
                  class="w-4 h-4 text-primary-600 focus:ring-primary-500"
                />
                <span class="ml-2 text-sm text-gray-700 dark:text-gray-300">Metric</span>
              </label>
              <label class="flex items-center cursor-pointer">
                <input
                  v-model="formData.alertType"
                  type="radio"
                  value="log"
                  class="w-4 h-4 text-primary-600 focus:ring-primary-500"
                />
                <span class="ml-2 text-sm text-gray-700 dark:text-gray-300">Log Pattern</span>
              </label>
              <label class="flex items-center cursor-pointer">
                <input
                  v-model="formData.alertType"
                  type="radio"
                  value="promql"
                  class="w-4 h-4 text-primary-600 focus:ring-primary-500"
                />
                <span class="ml-2 text-sm text-gray-700 dark:text-gray-300">PromQL</span>
              </label>
            </div>
          </div>

          <!-- Metric Configuration -->
          <div v-if="formData.alertType === 'metric'" class="space-y-4 p-4 bg-gray-50 dark:bg-gray-800/50 rounded-lg">
            <h3 class="text-sm font-semibold text-gray-900 dark:text-gray-100">Metric Configuration</h3>
            <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div>
                <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Metric Name <span class="text-red-500">*</span>
                </label>
                <input
                  v-model="formData.queryConfig.metric_name"
                  type="text"
                  placeholder="e.g., http_requests_total"
                  class="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:ring-primary-500 focus:border-primary-500"
                  :class="{ 'border-red-500': errors.metric_name }"
                />
                <p v-if="errors.metric_name" class="mt-1 text-sm text-red-500">{{ errors.metric_name }}</p>
              </div>
              <div>
                <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Time Aggregation</label>
                <select
                  v-model="formData.queryConfig.time_aggregation"
                  class="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:ring-primary-500 focus:border-primary-500"
                >
                  <option value="avg">Average</option>
                  <option value="sum">Sum</option>
                  <option value="min">Min</option>
                  <option value="max">Max</option>
                  <option value="count">Count</option>
                </select>
              </div>
              <div>
                <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Space Aggregation</label>
                <select
                  v-model="formData.queryConfig.space_aggregation"
                  class="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:ring-primary-500 focus:border-primary-500"
                >
                  <option value="avg">Average</option>
                  <option value="sum">Sum</option>
                  <option value="min">Min</option>
                  <option value="max">Max</option>
                </select>
              </div>
            </div>
          </div>

          <!-- Log Pattern Configuration -->
          <div v-if="formData.alertType === 'log'" class="space-y-4 p-4 bg-gray-50 dark:bg-gray-800/50 rounded-lg">
            <h3 class="text-sm font-semibold text-gray-900 dark:text-gray-100">Log Pattern Configuration</h3>
            <div>
              <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                Patterns to Match <span class="text-red-500">*</span>
              </label>
              <p class="text-xs text-gray-500 dark:text-gray-400 mb-2">
                Enter patterns to search for in log messages (one per line)
              </p>
              <textarea
                v-model="patternsText"
                rows="3"
                placeholder="error&#10;exception&#10;failed"
                class="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:ring-primary-500 focus:border-primary-500 font-mono text-sm"
                :class="{ 'border-red-500': errors.patterns }"
              ></textarea>
              <p v-if="errors.patterns" class="mt-1 text-sm text-red-500">{{ errors.patterns }}</p>
            </div>
            <div>
              <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Log Source</label>
              <select
                v-model="formData.queryConfig.log_source"
                class="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:ring-primary-500 focus:border-primary-500"
              >
                <option value="all">All Logs</option>
                <option value="otlp">OTLP Logs Only</option>
                <option value="unstructured">Unstructured Logs Only</option>
              </select>
            </div>
          </div>

          <!-- PromQL Configuration (shown when alertType is promql) -->
          <div v-if="formData.alertType === 'promql'" class="space-y-4 p-4 bg-gray-50 dark:bg-gray-800/50 rounded-lg">
            <h3 class="text-sm font-semibold text-gray-900 dark:text-gray-100">PromQL Configuration</h3>
            <div>
              <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                PromQL Expression <span class="text-red-500">*</span>
              </label>
              <p class="text-xs text-gray-500 dark:text-gray-400 mb-2">
                Enter a PromQL expression that returns a numeric value for threshold comparison
              </p>
              <textarea
                v-model="formData.queryConfig.promql"
                rows="3"
                placeholder='e.g., sum(rate(http_requests_total{status=~"5.."}[5m]))'
                class="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:ring-primary-500 focus:border-primary-500 font-mono text-sm"
                :class="{ 'border-red-500': errors.promql }"
              ></textarea>
              <p v-if="errors.promql" class="mt-1 text-sm text-red-500">{{ errors.promql }}</p>
            </div>
          </div>

          <!-- Threshold Configuration -->
          <div class="space-y-4 p-4 bg-gray-50 dark:bg-gray-800/50 rounded-lg">
            <h3 class="text-sm font-semibold text-gray-900 dark:text-gray-100">Alert Condition</h3>
            <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div>
                <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Condition</label>
                <select
                  v-model="formData.threshold_type"
                  class="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:ring-primary-500 focus:border-primary-500"
                >
                  <option value="above">Value is above</option>
                  <option value="below">Value is below</option>
                </select>
              </div>
              <div>
                <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Threshold <span class="text-red-500">*</span>
                </label>
                <input
                  v-model.number="formData.threshold"
                  type="number"
                  step="any"
                  placeholder="100"
                  class="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:ring-primary-500 focus:border-primary-500"
                  :class="{ 'border-red-500': errors.threshold }"
                />
                <p v-if="errors.threshold" class="mt-1 text-sm text-red-500">{{ errors.threshold }}</p>
              </div>
              <div>
                <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Evaluation Window</label>
                <select
                  v-model="formData.eval_window_seconds"
                  class="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:ring-primary-500 focus:border-primary-500"
                >
                  <option :value="60">1 minute</option>
                  <option :value="300">5 minutes</option>
                  <option :value="600">10 minutes</option>
                  <option :value="900">15 minutes</option>
                  <option :value="1800">30 minutes</option>
                  <option :value="3600">1 hour</option>
                </select>
              </div>
            </div>
          </div>

          <!-- Notification Channels -->
          <div class="space-y-4">
            <h3 class="text-sm font-semibold text-gray-900 dark:text-gray-100">Notification Channels</h3>
            <div v-if="loadingChannels" class="text-sm text-gray-500 dark:text-gray-400">
              Loading channels...
            </div>
            <div v-else-if="allNotificationChannels.length === 0" class="text-sm text-gray-500 dark:text-gray-400">
              No notification channels configured.
              <router-link :to="`/p/${projectId}/integrations`" class="text-primary-600 dark:text-primary-400 hover:underline">
                Add one now
              </router-link>
            </div>
            <div v-else class="space-y-2">
              <label
                v-for="channel in allNotificationChannels"
                :key="channel.id"
                class="flex items-center p-3 border border-gray-200 dark:border-gray-700 rounded-lg cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-800/50"
                :class="{ 'border-primary-500 bg-primary-50 dark:bg-primary-900/20': formData.notification_channels.includes(channel.id) }"
              >
                <input
                  type="checkbox"
                  :value="channel.id"
                  v-model="formData.notification_channels"
                  class="w-4 h-4 text-primary-600 focus:ring-primary-500 rounded"
                />
                <div class="ml-3">
                  <span class="text-sm font-medium text-gray-900 dark:text-gray-100">{{ channel.name }}</span>
                  <span class="ml-2 text-xs text-gray-500 dark:text-gray-400 uppercase">{{ channel.type }}</span>
                </div>
              </label>
            </div>
          </div>

          <!-- Enable/Disable -->
          <div class="flex items-center">
            <input
              v-model="formData.enabled"
              type="checkbox"
              class="w-4 h-4 text-primary-600 focus:ring-primary-500 rounded"
            />
            <label class="ml-2 text-sm text-gray-700 dark:text-gray-300">
              Enable this alert rule
            </label>
          </div>

          <!-- Submit Buttons -->
          <div class="flex justify-end gap-3 pt-4 border-t border-gray-200 dark:border-gray-700">
            <router-link
              :to="`/p/${projectId}/alerts`"
              class="px-4 py-2 text-sm font-medium text-gray-700 dark:text-gray-300 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-md hover:bg-gray-50 dark:hover:bg-gray-700 focus:outline-none focus:ring-2 focus:ring-primary-500"
            >
              Cancel
            </router-link>
            <button
              type="submit"
              :disabled="!isFormValid || saving"
              class="px-4 py-2 text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 rounded-md focus:outline-none focus:ring-2 focus:ring-primary-500 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <span v-if="saving">Saving...</span>
              <span v-else>Update Alert Rule</span>
            </button>
          </div>
        </form>
      </BaseCard>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import { useAuth } from '../../composables/useAuth';
import axios from 'axios';
import AppLayout from '../../Layouts/AppLayout.vue';
import BaseCard from '../../components/BaseCard.vue';

const route = useRoute();
const router = useRouter();
const { user, fetchUser } = useAuth();

const projectId = computed(() => route.params.id);
const ruleId = computed(() => route.params.ruleId);
const project = ref(null);
const loading = ref(true);
const loadError = ref(null);
const saving = ref(false);
const loadingChannels = ref(false);

// Form data - simplified HyperDX-style model
const formData = ref({
  name: '',
  description: '',
  alertType: 'metric',  // 'metric', 'log', or 'promql'
  queryConfig: {
    metric_name: '',
    filters: {},
    time_aggregation: 'avg',
    space_aggregation: 'sum',
    group_by: [],
    patterns: [],
    log_source: 'all',
    promql: '',
  },
  threshold: 0,
  threshold_type: 'above',
  eval_window_seconds: 300,
  eval_interval_seconds: 60,
  notification_channels: [],
  enabled: true,
});

// For log patterns - sync with textarea
const patternsText = ref('');
watch(patternsText, (newVal) => {
  formData.value.queryConfig.patterns = newVal
    .split('\n')
    .map(p => p.trim())
    .filter(p => p.length > 0);
});

// Notification channels
const allNotificationChannels = ref([]);

// Validation errors
const errors = computed(() => {
  const e = {};
  if (!formData.value.name.trim()) {
    e.name = 'Alert name is required';
  }
  if (formData.value.alertType === 'metric') {
    if (!formData.value.queryConfig.metric_name?.trim()) {
      e.metric_name = 'Metric name is required';
    }
  } else if (formData.value.alertType === 'log') {
    const patterns = formData.value.queryConfig.patterns || [];
    if (patterns.length === 0 || !patterns.some(p => p.trim())) {
      e.patterns = 'At least one pattern is required';
    }
  } else if (formData.value.alertType === 'promql') {
    if (!formData.value.queryConfig.promql?.trim()) {
      e.promql = 'PromQL expression is required';
    }
  }
  if (formData.value.threshold === null || formData.value.threshold === undefined) {
    e.threshold = 'Threshold is required';
  }
  return e;
});

const isFormValid = computed(() => Object.keys(errors.value).length === 0);

// Load notification channels
const loadAllNotificationChannels = async () => {
  loadingChannels.value = true;
  try {
    const headers = { 'x-project-id': projectId.value };
    const [slackRes, pagerdutyRes, teamsRes, discordRes] = await Promise.allSettled([
      axios.get('/api/slack/integrations', { params: { project_id: projectId.value }, headers }),
      axios.get('/api/pagerduty/integrations', { params: { project_id: projectId.value }, headers }),
      axios.get('/api/teams/integrations', { params: { project_id: projectId.value }, headers }),
      axios.get('/api/discord/integrations', { params: { project_id: projectId.value }, headers }),
    ]);

    const channels = [];
    const integrationTypes = ['slack', 'pagerduty', 'teams', 'discord'];
    [slackRes, pagerdutyRes, teamsRes, discordRes].forEach((result, index) => {
      if (result.status === 'fulfilled' && result.value?.data) {
        const integrationChannels = Array.isArray(result.value.data) ? result.value.data : [];
        channels.push(...integrationChannels
          .filter((c) => c.enabled)
          .map((c) => ({ ...c, type: integrationTypes[index] }))
        );
      }
    });

    allNotificationChannels.value = channels;
  } catch (error) {
    console.warn('Failed to load notification channels:', error);
  } finally {
    loadingChannels.value = false;
  }
};

// Load existing rule
const loadRule = async () => {
  loading.value = true;
  loadError.value = null;
  
  try {
    const response = await axios.get(`/api/alerting/rules/${ruleId.value}`, {
      headers: { 'x-project-id': projectId.value },
    });

    const rule = response.data;
    const qc = rule.query_config || {};
    
    let alertType = 'metric';
    if (qc.query_type === 'promql') {
      alertType = 'promql';
    } else if (qc.query_type === 'log_pattern') {
      alertType = 'log';
    } else if (qc.query_type === 'llm' || qc.query_type === 'metrics') {
      alertType = 'metric';
    } else if (qc.promql && !qc.metric_name) {
      alertType = 'promql';
    } else if (Array.isArray(qc.patterns) && qc.patterns.length > 0) {
      alertType = 'log';
    }

    formData.value = {
      name: rule.name || '',
      description: rule.description || '',
      alertType,
      queryConfig: {
        metric_name: qc.metric_name || '',
        filters: qc.filters || {},
        time_aggregation: qc.time_aggregation || 'avg',
        space_aggregation: qc.space_aggregation || 'sum',
        group_by: qc.group_by || [],
        patterns: qc.patterns || [],
        log_source: qc.log_source || 'all',
        promql: qc.promql || '',
      },
      threshold: rule.threshold || 0,
      threshold_type: rule.threshold_type || 'above',
      eval_window_seconds: rule.eval_window_seconds || 300,
      eval_interval_seconds: rule.eval_interval_seconds || 60,
      notification_channels: rule.notification_channels || [],
      enabled: rule.enabled !== false,
    };

    // Sync patterns textarea
    patternsText.value = (formData.value.queryConfig.patterns || []).join('\n');
  } catch (err) {
    console.error('Failed to load alert rule:', err);
    loadError.value = err.response?.data?.error || err.message || 'Failed to load alert rule';
  } finally {
    loading.value = false;
  }
};

const handleSubmit = async () => {
  if (!isFormValid.value) return;

  saving.value = true;
  try {
    const alertType = formData.value.alertType;
    
    let query_config;
    if (alertType === 'log') {
      query_config = {
        query_type: 'log_pattern',
        patterns: formData.value.queryConfig.patterns.filter(p => p.trim()),
        log_source: formData.value.queryConfig.log_source || 'all',
      };
    } else if (alertType === 'promql') {
      query_config = {
        query_type: 'promql',
        promql: formData.value.queryConfig.promql.trim(),
      };
    } else {
      const mn = formData.value.queryConfig.metric_name || '';
      query_config = {
        query_type: mn.startsWith('llm.') ? 'llm' : 'metrics',
        metric_name: mn,
        filters: formData.value.queryConfig.filters || {},
        group_by: formData.value.queryConfig.group_by || [],
        time_aggregation: formData.value.queryConfig.time_aggregation,
        space_aggregation: formData.value.queryConfig.space_aggregation,
      };
    }

    const payload = {
      name: formData.value.name.trim(),
      description: formData.value.description?.trim() || null,
      query_config,
      threshold: formData.value.threshold,
      threshold_type: formData.value.threshold_type,
      notification_channels: formData.value.notification_channels,
      alert_on_absent: false,
      absent_for_seconds: 300,
      eval_window_seconds: formData.value.eval_window_seconds,
      eval_interval_seconds: formData.value.eval_interval_seconds,
      enabled: formData.value.enabled,
    };

    await axios.put(`/api/alerting/rules/${ruleId.value}`, payload, {
      headers: { 'x-project-id': projectId.value },
    });

    router.push(`/p/${projectId.value}/alerts`);
  } catch (error) {
    console.error('Failed to update alert rule:', error);
    const errorMessage = error.response?.data?.error || 
                        error.response?.data?.message ||
                        error.message || 
                        'Failed to update alert rule. Please try again.';
    alert(errorMessage);
  } finally {
    saving.value = false;
  }
};

onMounted(async () => {
  try {
    await fetchUser();
    const projectResponse = await axios.get(`/api/projects/${projectId.value}`);
    project.value = projectResponse.data;
    await Promise.all([loadAllNotificationChannels(), loadRule()]);
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
  from { transform: rotate(0deg); }
  to { transform: rotate(360deg); }
}
</style>
