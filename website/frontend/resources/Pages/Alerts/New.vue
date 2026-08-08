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
          <h1 class="text-2xl font-semibold text-gray-900">Create Alert Rule</h1>
          <p class="text-sm text-gray-500 dark:text-gray-400 mt-1">
            Create a new alert rule to monitor your metrics or logs
          </p>
        </div>
      </div>

      <!-- Warning: No notification channels configured -->
      <BaseCard v-if="!hasNotificationChannels && !loadingChannels" class="mb-6 border-yellow-500 bg-yellow-50 dark:bg-yellow-900/20">
        <div class="flex items-start">
          <svg class="w-5 h-5 text-yellow-600 dark:text-yellow-400 mt-0.5 mr-3 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
          </svg>
          <div class="flex-1">
            <h3 class="text-sm font-semibold text-yellow-800 dark:text-yellow-200 mb-1">No notification channels configured</h3>
            <p class="text-sm text-yellow-700 dark:text-yellow-300 mb-3">
              Set up at least one notification channel (Slack, PagerDuty, etc.) to receive alerts.
            </p>
            <router-link
              :to="`/p/${projectId}/integrations`"
              class="inline-flex items-center px-3 py-2 text-sm font-medium text-yellow-800 dark:text-yellow-200 bg-yellow-100 dark:bg-yellow-800 hover:bg-yellow-200 dark:hover:bg-yellow-700 rounded-md transition-colors"
            >
              Set up notification channels
              <svg class="w-4 h-4 ml-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
              </svg>
            </router-link>
          </div>
        </div>
      </BaseCard>

      <!-- Simple Alert Form -->
      <BaseCard>
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

          <!-- Metric Configuration (shown when alertType is metric) -->
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

          <!-- Log Pattern Configuration (shown when alertType is log) -->
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
            <button
              type="button"
              @click="cancel"
              class="px-4 py-2 text-sm font-medium text-gray-700 dark:text-gray-300 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-md hover:bg-gray-50 dark:hover:bg-gray-700 focus:outline-none focus:ring-2 focus:ring-primary-500"
            >
              Cancel
            </button>
            <button
              type="submit"
              :disabled="!isFormValid || saving"
              class="px-4 py-2 text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 rounded-md focus:outline-none focus:ring-2 focus:ring-primary-500 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <span v-if="saving">Creating...</span>
              <span v-else>Create Alert Rule</span>
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
const project = ref(null);
const saving = ref(false);

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
  threshold_type: 'above',  // 'above' or 'below'
  eval_window_seconds: 300,
  eval_interval_seconds: 60,
  notification_channels: [],
  enabled: true,
});

// For log patterns - convert textarea to array
const patternsText = ref('');
watch(patternsText, (newVal) => {
  formData.value.queryConfig.patterns = newVal
    .split('\n')
    .map(p => p.trim())
    .filter(p => p.length > 0);
});

// Notification channels
const allNotificationChannels = ref([]);
const loadingChannels = ref(false);
const hasNotificationChannels = computed(() => allNotificationChannels.value.length > 0);

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

const cancel = () => {
  router.push(`/p/${projectId.value}/alerts`);
};

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
    allNotificationChannels.value = [];
  } finally {
    loadingChannels.value = false;
  }
};

const handleSubmit = async () => {
  if (!isFormValid.value) return;

  saving.value = true;
  try {
    const alertType = formData.value.alertType;
    
    // Build query_config based on alert type
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
      project_id: projectId.value,
      name: formData.value.name.trim(),
      description: formData.value.description?.trim() || null,
      rule_type: 'threshold',
      query_config,
      threshold: formData.value.threshold,
      threshold_type: formData.value.threshold_type,
      notification_channels: formData.value.notification_channels,
      alert_on_absent: false,
      absent_for_seconds: 300,
      eval_window_seconds: formData.value.eval_window_seconds,
      eval_interval_seconds: formData.value.eval_interval_seconds,
      labels: {},
      annotations: {},
      enabled: formData.value.enabled,
    };

    await axios.post('/api/alerting/rules', payload, {
      headers: { 'x-project-id': projectId.value },
    });

    router.push(`/p/${projectId.value}/alerts`);
  } catch (error) {
    console.error('Failed to create alert rule:', error);
    const errorMessage = error.response?.data?.error || 
                        error.response?.data?.message ||
                        error.message || 
                        'Failed to create alert rule. Please try again.';
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
    await loadAllNotificationChannels();

    // Pre-fill from query params (e.g. when creating alert from a dashboard widget)
    if (route.query.promql) {
      formData.value.alertType = 'promql';
      formData.value.queryConfig.promql = route.query.promql;
    }
    if (route.query.name) {
      formData.value.name = route.query.name;
    }
    if (route.query.eval_window) {
      const parsed = parseInt(route.query.eval_window, 10);
      if (!isNaN(parsed) && parsed > 0) {
        formData.value.eval_window_seconds = parsed;
      }
    }
  } catch (error) {
    console.error('Failed to load project:', error);
    router.push('/projects');
  }
});
</script>
