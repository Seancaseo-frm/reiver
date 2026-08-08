<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <!-- Loading state -->
      <div v-if="loading" class="flex justify-center py-12">
        <div class="animate-spin rounded-full h-8 w-8 border-b-2 border-primary-600"></div>
      </div>

      <div v-else-if="rollout">
        <!-- Header -->
        <div class="mb-8">
          <nav class="flex mb-4" aria-label="Breadcrumb">
            <ol class="flex items-center space-x-4">
              <li>
                <router-link :to="`/p/${projectId}/llm/rollouts`" class="text-gray-400 hover:text-gray-500 dark:hover:text-gray-300">
                  Rollouts
                </router-link>
              </li>
              <li>
                <div class="flex items-center">
                  <svg class="flex-shrink-0 h-5 w-5 text-gray-300 dark:text-gray-600" fill="currentColor" viewBox="0 0 20 20">
                    <path fill-rule="evenodd" d="M7.293 14.707a1 1 0 010-1.414L10.586 10 7.293 6.707a1 1 0 011.414-1.414l4 4a1 1 0 010 1.414l-4 4a1 1 0 01-1.414 0z" clip-rule="evenodd" />
                  </svg>
                  <span class="ml-4 text-sm font-medium text-gray-900 dark:text-gray-100">{{ rollout.config_name }}</span>
                </div>
              </li>
            </ol>
          </nav>
          <div class="flex justify-between items-start">
            <div>
              <h1 class="text-3xl font-bold text-gray-900 dark:text-gray-100">
                {{ rollout.config_name }}: v{{ rollout.baseline_version || '?' }} → v{{ rollout.target_version }}
              </h1>
              <p class="mt-2 text-gray-600 dark:text-gray-400">
                {{ rollout.name || 'Progressive rollout' }} • {{ rollout.mode }} mode
              </p>
            </div>
            <div class="flex items-center space-x-3">
              <span :class="[
                'inline-flex items-center px-3 py-1 rounded-full text-sm font-medium',
                rollout.status === 'running' ? 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-400' :
                rollout.status === 'completed' ? 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400' :
                rollout.status === 'rolled_back' ? 'bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-400' :
                rollout.status === 'paused' ? 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300' :
                'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300'
              ]">
                <span v-if="rollout.status === 'running'" class="w-2 h-2 mr-2 bg-yellow-400 rounded-full animate-pulse"></span>
                {{ rollout.status }}
              </span>
            </div>
          </div>
        </div>

        <!-- Action buttons -->
        <div v-if="rollout.status === 'running' || rollout.status === 'paused'" class="mb-6 flex space-x-3">
          <button
            v-if="rollout.status === 'running'"
            @click="pauseRollout"
            :disabled="actionLoading"
            class="inline-flex items-center px-4 py-2 border border-gray-300 dark:border-gray-600 shadow-sm text-sm font-medium rounded-md text-gray-700 dark:text-gray-300 bg-white dark:bg-gray-700 hover:bg-gray-50 dark:hover:bg-gray-600 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 disabled:opacity-50"
          >
            <svg class="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 9v6m4-6v6m7-3a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
            Pause
          </button>
          <button
            v-if="rollout.status === 'paused'"
            @click="resumeRollout"
            :disabled="actionLoading"
            class="inline-flex items-center px-4 py-2 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 disabled:opacity-50"
          >
            Resume
          </button>
          <button
            @click="promoteRollout"
            :disabled="actionLoading"
            class="inline-flex items-center px-4 py-2 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-green-600 hover:bg-green-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-green-500 disabled:opacity-50"
          >
            <svg class="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 7l5 5m0 0l-5 5m5-5H6" />
            </svg>
            Promote
          </button>
          <button
            @click="completeRollout"
            :disabled="actionLoading"
            class="inline-flex items-center px-4 py-2 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 disabled:opacity-50"
          >
            <svg class="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
            </svg>
            Complete (100%)
          </button>
          <button
            @click="rollbackRollout"
            :disabled="actionLoading"
            class="inline-flex items-center px-4 py-2 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-red-600 hover:bg-red-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-red-500 disabled:opacity-50"
          >
            <svg class="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 14l2-2m0 0l2-2m-2 2l-2-2m2 2l2 2M3 12l6.414 6.414a2 2 0 001.414.586H19a2 2 0 002-2V7a2 2 0 00-2-2h-8.172a2 2 0 00-1.414.586L3 12z" />
            </svg>
            Rollback
          </button>
        </div>

        <!-- Progress Card -->
        <div class="bg-white dark:bg-gray-800 shadow rounded-lg mb-6">
          <div class="px-6 py-4 border-b border-gray-200 dark:border-gray-700">
            <h2 class="text-lg font-medium text-gray-900 dark:text-gray-100">Rollout Progress</h2>
          </div>
          <div class="px-6 py-6">
            <!-- Progress bar -->
            <div class="mb-6">
              <div class="flex justify-between text-sm text-gray-600 dark:text-gray-400 mb-2">
                <span>Traffic to new version</span>
                <span class="font-medium text-gray-900 dark:text-gray-100">{{ rollout.current_weight }}%</span>
              </div>
              <div class="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-4">
                <div
                  :class="[
                    'h-4 rounded-full transition-all duration-500',
                    rollout.status === 'running' ? 'bg-yellow-400' :
                    rollout.status === 'completed' ? 'bg-green-400' :
                    rollout.status === 'rolled_back' ? 'bg-red-400' : 'bg-gray-400'
                  ]"
                  :style="{ width: `${rollout.current_weight}%` }"
                ></div>
              </div>
            </div>

            <!-- Stages -->
            <div class="flex items-center justify-between">
              <div
                v-for="(stage, index) in rollout.stages"
                :key="stage.id"
                class="flex-1 relative"
              >
                <!-- Connector line -->
                <div v-if="index > 0" :class="[
                  'absolute top-4 right-1/2 w-full h-0.5 -z-10',
                  rollout.stages[index - 1].status === 'passed' ? 'bg-green-400' : 'bg-gray-200 dark:bg-gray-700'
                ]"></div>

                <!-- Stage circle -->
                <div class="flex flex-col items-center">
                  <div :class="[
                    'w-8 h-8 rounded-full flex items-center justify-center text-sm font-medium',
                    stage.status === 'passed' ? 'bg-green-500 text-white' :
                    stage.status === 'active' ? 'bg-yellow-500 text-white ring-4 ring-yellow-200 dark:ring-yellow-900' :
                    stage.status === 'failed' ? 'bg-red-500 text-white' :
                    'bg-gray-200 text-gray-500 dark:bg-gray-700 dark:text-gray-400'
                  ]">
                    <svg v-if="stage.status === 'passed'" class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
                    </svg>
                    <svg v-else-if="stage.status === 'failed'" class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
                    </svg>
                    <span v-else>{{ index + 1 }}</span>
                  </div>
                  <div class="mt-2 text-center">
                    <div class="text-sm font-medium text-gray-900 dark:text-gray-100">{{ stage.weight }}%</div>
                    <div class="text-xs text-gray-500 dark:text-gray-400">{{ stage.min_duration_minutes }}min</div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>

        <!-- Metrics Comparison -->
        <div class="bg-white dark:bg-gray-800 shadow rounded-lg mb-6">
          <div class="px-6 py-4 border-b border-gray-200 dark:border-gray-700 flex justify-between items-center">
            <h2 class="text-lg font-medium text-gray-900 dark:text-gray-100">Metrics Comparison</h2>
            <button @click="fetchMetrics" :disabled="metricsLoading" class="text-sm text-primary-600 hover:text-primary-700 dark:text-primary-400">
              {{ metricsLoading ? 'Refreshing...' : 'Refresh' }}
            </button>
          </div>
          <div class="px-6 py-4">
            <div v-if="metricsLoading" class="flex justify-center py-8">
              <div class="animate-spin rounded-full h-6 w-6 border-b-2 border-primary-600"></div>
            </div>
            <div v-else-if="metrics">
              <!-- Status indicator -->
              <div class="mb-6 p-4 rounded-lg" :class="[
                metrics.comparison.status === 'passing' ? 'bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800' :
                metrics.comparison.status === 'failing' ? 'bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800' :
                'bg-gray-50 dark:bg-gray-700 border border-gray-200 dark:border-gray-600'
              ]">
                <div class="flex items-center">
                  <svg v-if="metrics.comparison.status === 'passing'" class="w-5 h-5 text-green-500 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                  <svg v-else-if="metrics.comparison.status === 'failing'" class="w-5 h-5 text-red-500 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 14l2-2m0 0l2-2m-2 2l-2-2m2 2l2 2m7-2a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                  <svg v-else class="w-5 h-5 text-gray-500 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8.228 9c.549-1.165 2.03-2 3.772-2 2.21 0 4 1.343 4 3 0 1.4-1.278 2.575-3.006 2.907-.542.104-.994.54-.994 1.093m0 3h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                  <span :class="[
                    'font-medium',
                    metrics.comparison.status === 'passing' ? 'text-green-800 dark:text-green-200' :
                    metrics.comparison.status === 'failing' ? 'text-red-800 dark:text-red-200' :
                    'text-gray-800 dark:text-gray-200'
                  ]">
                    {{ metrics.comparison.status === 'passing' ? 'All metrics within thresholds' :
                       metrics.comparison.status === 'failing' ? 'Metrics exceed thresholds' :
                       'Collecting more data...' }}
                  </span>
                </div>
              </div>

              <!-- Metrics table -->
              <table class="min-w-full">
                <thead>
                  <tr>
                    <th class="text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider pb-3">Metric</th>
                    <th class="text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider pb-3">Baseline</th>
                    <th class="text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider pb-3">Target</th>
                    <th class="text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider pb-3">Diff</th>
                  </tr>
                </thead>
                <tbody class="divide-y divide-gray-200 dark:divide-gray-700">
                  <tr>
                    <td class="py-3 text-sm text-gray-900 dark:text-gray-100">Request Count</td>
                    <td class="py-3 text-sm text-right text-gray-600 dark:text-gray-400">{{ metrics.baseline.request_count }}</td>
                    <td class="py-3 text-sm text-right text-gray-600 dark:text-gray-400">{{ metrics.target.request_count }}</td>
                    <td class="py-3 text-sm text-right text-gray-500">-</td>
                  </tr>
                  <tr>
                    <td class="py-3 text-sm text-gray-900 dark:text-gray-100">Error Rate</td>
                    <td class="py-3 text-sm text-right text-gray-600 dark:text-gray-400">{{ (metrics.baseline.error_rate * 100).toFixed(2) }}%</td>
                    <td class="py-3 text-sm text-right text-gray-600 dark:text-gray-400">{{ (metrics.target.error_rate * 100).toFixed(2) }}%</td>
                    <td :class="['py-3 text-sm text-right font-medium', metrics.comparison.error_rate_diff > 0.05 ? 'text-red-600' : 'text-green-600']">
                      {{ metrics.comparison.error_rate_diff > 0 ? '+' : '' }}{{ (metrics.comparison.error_rate_diff * 100).toFixed(2) }}%
                    </td>
                  </tr>
                  <tr>
                    <td class="py-3 text-sm text-gray-900 dark:text-gray-100">Avg Latency</td>
                    <td class="py-3 text-sm text-right text-gray-600 dark:text-gray-400">{{ metrics.baseline.avg_latency_ms.toFixed(0) }}ms</td>
                    <td class="py-3 text-sm text-right text-gray-600 dark:text-gray-400">{{ metrics.target.avg_latency_ms.toFixed(0) }}ms</td>
                    <td :class="['py-3 text-sm text-right font-medium', metrics.comparison.latency_diff_pct > 20 ? 'text-red-600' : 'text-green-600']">
                      {{ metrics.comparison.latency_diff_pct > 0 ? '+' : '' }}{{ metrics.comparison.latency_diff_pct.toFixed(1) }}%
                    </td>
                  </tr>
                  <tr>
                    <td class="py-3 text-sm text-gray-900 dark:text-gray-100">P95 Latency</td>
                    <td class="py-3 text-sm text-right text-gray-600 dark:text-gray-400">{{ metrics.baseline.p95_latency_ms.toFixed(0) }}ms</td>
                    <td class="py-3 text-sm text-right text-gray-600 dark:text-gray-400">{{ metrics.target.p95_latency_ms.toFixed(0) }}ms</td>
                    <td class="py-3 text-sm text-right text-gray-500">-</td>
                  </tr>
                  <tr>
                    <td class="py-3 text-sm text-gray-900 dark:text-gray-100">Avg Cost</td>
                    <td class="py-3 text-sm text-right text-gray-600 dark:text-gray-400">${{ parseFloat(metrics.baseline.avg_cost_usd).toFixed(6) }}</td>
                    <td class="py-3 text-sm text-right text-gray-600 dark:text-gray-400">${{ parseFloat(metrics.target.avg_cost_usd).toFixed(6) }}</td>
                    <td :class="['py-3 text-sm text-right font-medium', metrics.comparison.cost_diff_pct > 20 ? 'text-yellow-600' : 'text-green-600']">
                      {{ metrics.comparison.cost_diff_pct > 0 ? '+' : '' }}{{ metrics.comparison.cost_diff_pct.toFixed(1) }}%
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
            <div v-else class="text-center py-8 text-gray-500 dark:text-gray-400">
              No metrics available yet. Data will appear once requests are processed.
            </div>
          </div>
        </div>

        <!-- Quality Scores -->
        <div v-if="metrics && metrics.quality_scores" class="bg-white dark:bg-gray-800 shadow rounded-lg mb-6">
          <div class="px-6 py-4 border-b border-gray-200 dark:border-gray-700">
            <h2 class="text-lg font-medium text-gray-900 dark:text-gray-100">Quality Scores (LLM-as-Judge)</h2>
          </div>
          <div class="px-6 py-4">
            <table class="min-w-full">
              <thead>
                <tr>
                  <th class="text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider pb-3">Dimension</th>
                  <th class="text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider pb-3">Baseline</th>
                  <th class="text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider pb-3">Target</th>
                  <th class="text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider pb-3">Diff</th>
                </tr>
              </thead>
              <tbody class="divide-y divide-gray-200 dark:divide-gray-700">
                <tr v-for="dim in ['relevance', 'coherence', 'helpfulness', 'average']" :key="dim">
                  <td class="py-3 text-sm text-gray-900 dark:text-gray-100 capitalize">{{ dim }}</td>
                  <td class="py-3 text-sm text-right text-gray-600 dark:text-gray-400">
                    {{ metrics.quality_scores.baseline[dim] != null ? metrics.quality_scores.baseline[dim].toFixed(1) : '-' }}
                  </td>
                  <td class="py-3 text-sm text-right text-gray-600 dark:text-gray-400">
                    {{ metrics.quality_scores.target[dim] != null ? metrics.quality_scores.target[dim].toFixed(1) : '-' }}
                  </td>
                  <td :class="['py-3 text-sm text-right font-medium', qualityDiff(dim) < -5 ? 'text-red-600' : qualityDiff(dim) > 5 ? 'text-green-600' : 'text-gray-500']">
                    {{ qualityDiffLabel(dim) }}
                  </td>
                </tr>
              </tbody>
            </table>
            <p class="mt-3 text-xs text-gray-500 dark:text-gray-400">
              Scores are 0-100. Based on {{ metrics.quality_scores.target.sample_count || 0 }} target / {{ metrics.quality_scores.baseline.sample_count || 0 }} baseline evaluations.
            </p>
          </div>
        </div>

        <!-- Recent Judge Evaluations -->
        <div v-if="metrics && metrics.recent_summaries && metrics.recent_summaries.length > 0" class="bg-white dark:bg-gray-800 shadow rounded-lg mb-6">
          <div class="px-6 py-4 border-b border-gray-200 dark:border-gray-700">
            <h2 class="text-lg font-medium text-gray-900 dark:text-gray-100">Recent Judge Evaluations</h2>
          </div>
          <div class="px-6 py-4 space-y-4">
            <div v-for="summary in metrics.recent_summaries" :key="summary.request_id" class="flex items-start gap-3 p-3 rounded-lg bg-gray-50 dark:bg-gray-700/50">
              <span :class="[
                'inline-flex items-center px-2 py-0.5 rounded text-xs font-medium flex-shrink-0 mt-0.5',
                summary.variant === 'target' ? 'bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400' :
                'bg-gray-100 text-gray-800 dark:bg-gray-600 dark:text-gray-300'
              ]">
                {{ summary.variant }}
              </span>
              <div class="flex-1 min-w-0">
                <p class="text-sm text-gray-900 dark:text-gray-100">{{ summary.summary }}</p>
                <p class="mt-1 text-xs text-gray-500 dark:text-gray-400">
                  Score: {{ summary.score.toFixed(1) }} · {{ formatDate(summary.created_at) }}
                </p>
              </div>
            </div>
          </div>
        </div>

        <!-- Rollout Details -->
        <div class="bg-white dark:bg-gray-800 shadow rounded-lg">
          <div class="px-6 py-4 border-b border-gray-200 dark:border-gray-700">
            <h2 class="text-lg font-medium text-gray-900 dark:text-gray-100">Details</h2>
          </div>
          <div class="px-6 py-4">
            <dl class="grid grid-cols-2 gap-4">
              <div>
                <dt class="text-sm font-medium text-gray-500 dark:text-gray-400">Mode</dt>
                <dd class="mt-1 text-sm text-gray-900 dark:text-gray-100 capitalize">{{ rollout.mode }}</dd>
              </div>
              <div>
                <dt class="text-sm font-medium text-gray-500 dark:text-gray-400">Allocation</dt>
                <dd class="mt-1 text-sm text-gray-900 dark:text-gray-100">{{ formatAllocation(rollout.allocation_type) }}</dd>
              </div>
              <div>
                <dt class="text-sm font-medium text-gray-500 dark:text-gray-400">Created</dt>
                <dd class="mt-1 text-sm text-gray-900 dark:text-gray-100">{{ formatDate(rollout.created_at) }}</dd>
              </div>
              <div>
                <dt class="text-sm font-medium text-gray-500 dark:text-gray-400">Started</dt>
                <dd class="mt-1 text-sm text-gray-900 dark:text-gray-100">{{ rollout.started_at ? formatDate(rollout.started_at) : '-' }}</dd>
              </div>
              <div v-if="rollout.completed_at">
                <dt class="text-sm font-medium text-gray-500 dark:text-gray-400">Completed</dt>
                <dd class="mt-1 text-sm text-gray-900 dark:text-gray-100">{{ formatDate(rollout.completed_at) }}</dd>
              </div>
            </dl>
          </div>
        </div>
      </div>
    </div>
  </AppLayout>
</template>

<script>
import { ref, onMounted, computed, watch, onUnmounted } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import axios from 'axios';
import AppLayout from '../../../Layouts/AppLayout.vue';
import { useAuth } from '../../../composables/useAuth';

export default {
  components: { AppLayout },
  setup() {
    const route = useRoute();
    const router = useRouter();
    const { user } = useAuth();
    const projectId = computed(() => route.params.id);
    const rolloutId = computed(() => route.params.rollout_id);
    const project = computed(() => (projectId.value ? { id: projectId.value } : null));

    const rollout = ref(null);
    const metrics = ref(null);
    const loading = ref(true);
    const metricsLoading = ref(false);
    const actionLoading = ref(false);

    let pollInterval = null;

    const fetchRollout = async () => {
      try {
        const response = await axios.get(`/api/llm/prompts/rollouts/${rolloutId.value}?project_id=${projectId.value}`);
        rollout.value = response.data;
      } catch (error) {
        console.error('Failed to fetch rollout:', error);
      } finally {
        loading.value = false;
      }
    };

    const fetchMetrics = async () => {
      metricsLoading.value = true;
      try {
        const response = await axios.get(`/api/llm/prompts/rollouts/${rolloutId.value}/metrics?project_id=${projectId.value}`);
        metrics.value = response.data;
      } catch (error) {
        console.error('Failed to fetch metrics:', error);
      } finally {
        metricsLoading.value = false;
      }
    };

    const performAction = async (action) => {
      actionLoading.value = true;
      try {
        await axios.post(`/api/llm/prompts/rollouts/${rolloutId.value}/${action}`, {
          project_id: projectId.value,
        });
        await fetchRollout();
        await fetchMetrics();
      } catch (error) {
        console.error(`Failed to ${action} rollout:`, error);
        alert(error.response?.data?.message || `Failed to ${action} rollout`);
      } finally {
        actionLoading.value = false;
      }
    };

    const pauseRollout = () => performAction('pause');
    const resumeRollout = () => performAction('start');
    const promoteRollout = () => performAction('promote');
    const completeRollout = () => performAction('complete');
    const rollbackRollout = () => performAction('rollback');

    const formatAllocation = (type) => {
      const types = {
        random: 'Random',
        user_sticky: 'User Sticky',
        session_sticky: 'Session Sticky',
      };
      return types[type] || type;
    };

    const formatDate = (dateString) => {
      return new Date(dateString).toLocaleString();
    };

    const qualityDiff = (dim) => {
      if (!metrics.value?.quality_scores) return 0;
      const t = metrics.value.quality_scores.target[dim];
      const b = metrics.value.quality_scores.baseline[dim];
      if (t == null || b == null) return 0;
      return t - b;
    };

    const qualityDiffLabel = (dim) => {
      const diff = qualityDiff(dim);
      if (!metrics.value?.quality_scores) return '-';
      const t = metrics.value.quality_scores.target[dim];
      const b = metrics.value.quality_scores.baseline[dim];
      if (t == null || b == null) return '-';
      return (diff > 0 ? '+' : '') + diff.toFixed(1);
    };

    onMounted(async () => {
      await fetchRollout();
      await fetchMetrics();

      // Poll for updates if running
      pollInterval = setInterval(async () => {
        if (rollout.value?.status === 'running') {
          await fetchRollout();
          await fetchMetrics();
        }
      }, 30000);
    });

    onUnmounted(() => {
      if (pollInterval) {
        clearInterval(pollInterval);
      }
    });

    watch([projectId, rolloutId], async () => {
      loading.value = true;
      await fetchRollout();
      await fetchMetrics();
    });

    return {
      user,
      project,
      projectId,
      rollout,
      metrics,
      loading,
      metricsLoading,
      actionLoading,
      fetchMetrics,
      pauseRollout,
      resumeRollout,
      promoteRollout,
      completeRollout,
      rollbackRollout,
      formatAllocation,
      formatDate,
      qualityDiff,
      qualityDiffLabel,
    };
  },
};
</script>
