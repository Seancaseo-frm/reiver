<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <!-- Header -->
      <div class="mb-8">
        <h1 class="text-3xl font-bold text-gray-900 dark:text-gray-100">Rollouts</h1>
        <p class="mt-2 text-gray-600 dark:text-gray-400">Monitor and manage progressive prompt deployments</p>
      </div>

      <!-- Tabs -->
      <div class="border-b border-gray-200 dark:border-gray-700 mb-6">
        <nav class="-mb-px flex space-x-8">
          <button
            @click="statusFilter = 'running'"
            :class="[
              statusFilter === 'running'
                ? 'border-primary-500 text-primary-600 dark:text-primary-400'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300 dark:text-gray-400 dark:hover:text-gray-300',
              'whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm'
            ]"
          >
            Active
            <span v-if="runningCount > 0" class="ml-2 bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-400 py-0.5 px-2 rounded-full text-xs">
              {{ runningCount }}
            </span>
          </button>
          <button
            @click="statusFilter = 'completed'"
            :class="[
              statusFilter === 'completed'
                ? 'border-primary-500 text-primary-600 dark:text-primary-400'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300 dark:text-gray-400 dark:hover:text-gray-300',
              'whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm'
            ]"
          >
            Completed
          </button>
          <button
            @click="statusFilter = 'rolled_back'"
            :class="[
              statusFilter === 'rolled_back'
                ? 'border-primary-500 text-primary-600 dark:text-primary-400'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300 dark:text-gray-400 dark:hover:text-gray-300',
              'whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm'
            ]"
          >
            Rolled Back
          </button>
          <button
            @click="statusFilter = null"
            :class="[
              statusFilter === null
                ? 'border-primary-500 text-primary-600 dark:text-primary-400'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300 dark:text-gray-400 dark:hover:text-gray-300',
              'whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm'
            ]"
          >
            All
          </button>
        </nav>
      </div>

      <!-- Loading state -->
      <div v-if="loading" class="flex justify-center py-12">
        <div class="animate-spin rounded-full h-8 w-8 border-b-2 border-primary-600"></div>
      </div>

      <!-- Empty state -->
      <div v-else-if="rollouts.length === 0" class="text-center py-12 bg-white dark:bg-gray-800 rounded-lg shadow">
        <svg class="mx-auto h-12 w-12 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
        </svg>
        <h3 class="mt-2 text-sm font-medium text-gray-900 dark:text-gray-100">No rollouts</h3>
        <p class="mt-1 text-sm text-gray-500 dark:text-gray-400">Open a prompt, then click Deploy next to a version to start a rollout.</p>
        <div class="mt-6">
          <router-link
            :to="`/p/${projectId}/llm/prompts`"
            class="inline-flex items-center px-4 py-2 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-primary-600 hover:bg-primary-700"
          >
            Go to Prompts
          </router-link>
        </div>
      </div>

      <!-- Rollouts list -->
      <div v-else class="space-y-4">
        <div
          v-for="rollout in rollouts"
          :key="rollout.id"
          class="bg-white dark:bg-gray-800 shadow rounded-lg overflow-hidden hover:shadow-md transition-shadow cursor-pointer"
          @click="$router.push(`/p/${projectId}/llm/rollouts/${rollout.id}`)"
        >
          <div class="px-6 py-4">
            <div class="flex items-center justify-between">
              <div class="flex items-center space-x-4">
                <div :class="[
                  'w-3 h-3 rounded-full',
                  rollout.status === 'running' ? 'bg-yellow-400 animate-pulse' :
                  rollout.status === 'completed' ? 'bg-green-400' :
                  rollout.status === 'rolled_back' ? 'bg-red-400' :
                  rollout.status === 'paused' ? 'bg-gray-400' : 'bg-gray-300'
                ]"></div>
                <div>
                  <h3 class="text-lg font-medium text-gray-900 dark:text-gray-100">
                    {{ rollout.config_name }}
                  </h3>
                  <p class="text-sm text-gray-500 dark:text-gray-400">
                    v{{ rollout.baseline_version || '?' }} → v{{ rollout.target_version }}
                    <span v-if="rollout.name" class="ml-2">({{ rollout.name }})</span>
                  </p>
                </div>
              </div>
              <div class="flex items-center space-x-6">
                <div class="text-right">
                  <div class="text-sm font-medium text-gray-900 dark:text-gray-100">{{ rollout.current_weight }}%</div>
                  <div class="text-xs text-gray-500 dark:text-gray-400">Traffic</div>
                </div>
                <div class="w-32 bg-gray-200 dark:bg-gray-700 rounded-full h-2">
                  <div
                    :class="[
                      'h-2 rounded-full transition-all duration-500',
                      rollout.status === 'running' ? 'bg-yellow-400' :
                      rollout.status === 'completed' ? 'bg-green-400' :
                      rollout.status === 'rolled_back' ? 'bg-red-400' : 'bg-gray-400'
                    ]"
                    :style="{ width: `${rollout.current_weight}%` }"
                  ></div>
                </div>
                <span :class="[
                  'inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium',
                  rollout.status === 'running' ? 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-400' :
                  rollout.status === 'completed' ? 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400' :
                  rollout.status === 'rolled_back' ? 'bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-400' :
                  rollout.status === 'paused' ? 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300' :
                  'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300'
                ]">
                  {{ rollout.status }}
                </span>
              </div>
            </div>

            <!-- Stages indicator -->
            <div class="mt-4 flex items-center space-x-2">
              <div
                v-for="(stage, index) in rollout.stages"
                :key="stage.id"
                class="flex items-center"
              >
                <div :class="[
                  'w-8 h-8 rounded-full flex items-center justify-center text-xs font-medium',
                  stage.status === 'passed' ? 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400' :
                  stage.status === 'active' ? 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-400 ring-2 ring-yellow-400' :
                  stage.status === 'failed' ? 'bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-400' :
                  'bg-gray-100 text-gray-500 dark:bg-gray-700 dark:text-gray-400'
                ]">
                  {{ stage.weight }}%
                </div>
                <div v-if="index < rollout.stages.length - 1" :class="[
                  'w-8 h-0.5',
                  stage.status === 'passed' ? 'bg-green-400' : 'bg-gray-200 dark:bg-gray-700'
                ]"></div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </AppLayout>
</template>

<script>
import { ref, onMounted, computed, watch } from 'vue';
import { useRoute } from 'vue-router';
import axios from 'axios';
import AppLayout from '../../../Layouts/AppLayout.vue';
import { useAuth } from '../../../composables/useAuth';

export default {
  components: { AppLayout },
  setup() {
    const route = useRoute();
    const { user } = useAuth();
    const projectId = computed(() => route.params.id);
    const project = computed(() => (projectId.value ? { id: projectId.value } : null));

    const rollouts = ref([]);
    const loading = ref(true);
    const statusFilter = ref('running');

    const runningCount = computed(() => {
      return rollouts.value.filter(r => r?.status === 'running').length;
    });

    const fetchRollouts = async () => {
      loading.value = true;
      try {
        let url = `/api/llm/prompts/rollouts?project_id=${projectId.value}`;
        if (statusFilter.value) {
          url += `&status=${statusFilter.value}`;
        }
        const response = await axios.get(url);
        rollouts.value = response.data;
      } catch (error) {
        console.error('Failed to fetch rollouts:', error);
      } finally {
        loading.value = false;
      }
    };

    onMounted(fetchRollouts);
    watch([projectId, statusFilter], fetchRollouts);

    return {
      user,
      project,
      projectId,
      rollouts,
      loading,
      statusFilter,
      runningCount,
    };
  },
};
</script>
