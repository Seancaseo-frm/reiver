<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-[1200px] mx-auto px-8 py-6">
      <!-- Header -->
      <div class="mb-6">
        <div>
          <h1 class="text-2xl font-semibold text-gray-900">Maintenance Windows</h1>
          <p class="text-sm text-gray-500 dark:text-gray-400 mt-1">
            Schedule maintenance periods to silence alerts and pause health checks
          </p>
        </div>
        <router-link
          :to="`/p/${projectId}/maintenance/new`"
          class="inline-flex items-center gap-2 px-4 py-2 bg-indigo-600 text-white rounded-md hover:bg-indigo-700 font-medium text-sm"
        >
          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" />
          </svg>
          New Maintenance Window
        </router-link>
      </div>

      <!-- Empty State -->
      <div v-if="!loading && windows.length === 0" class="mt-6">
        <BaseCard>
          <div class="text-center py-12">
            <svg class="mx-auto h-12 w-12 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
            </svg>
            <h3 class="mt-2 text-sm font-semibold text-gray-900 dark:text-gray-100">No maintenance windows</h3>
            <p class="mt-1 text-sm text-gray-500 dark:text-gray-400">
              Schedule a maintenance window to silence alerts during planned downtime.
            </p>
            <div class="mt-6">
              <router-link
                :to="`/p/${projectId}/maintenance/new`"
                class="inline-flex items-center gap-2 px-4 py-2 bg-indigo-600 text-white rounded-md hover:bg-indigo-700 font-medium text-sm"
              >
                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" />
                </svg>
                Create Maintenance Window
              </router-link>
            </div>
          </div>
        </BaseCard>
      </div>

      <!-- Maintenance Windows Table -->
      <BaseCard v-else-if="!loading" class="mt-6">
        <template #header>
          <div class="flex items-center justify-between">
            <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">
              Maintenance Windows ({{ windows.length }})
            </h2>
            <button
              @click="loadWindows"
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
        </template>

        <div class="overflow-x-auto">
          <table class="w-full">
            <thead class="border-b border-gray-200 dark:border-gray-700">
              <tr>
                <th class="text-left py-3 px-4 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Name</th>
                <th class="text-left py-3 px-4 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Schedule</th>
                <th class="text-left py-3 px-4 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Status</th>
                <th class="text-left py-3 px-4 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Next Occurrence</th>
                <th class="text-right py-3 px-4 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Actions</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-gray-200 dark:divide-gray-700">
              <tr
                v-for="window in windows"
                :key="window.id"
                class="hover:bg-gray-50 dark:hover:bg-gray-800/50 cursor-pointer"
                @click="router.push(`/p/${projectId}/maintenance/${window.id}/edit`)"
              >
                <td class="py-4 px-4">
                  <div class="font-medium text-gray-900 dark:text-gray-100">{{ window.name }}</div>
                  <div v-if="window.description" class="text-sm text-gray-500 dark:text-gray-400">{{ window.description }}</div>
                </td>
                <td class="py-4 px-4 text-sm text-gray-600 dark:text-gray-300">
                  <span v-if="window.schedule_type === 'one_time'">
                    One-time
                  </span>
                  <span v-else>
                    {{ formatRecurrence(window) }}
                  </span>
                </td>
                <td class="py-4 px-4">
                  <span
                    :class="[
                      'inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium',
                      window.is_active ? 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400' :
                      !window.enabled ? 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300' :
                      'bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400'
                    ]"
                  >
                    {{ window.is_active ? 'Active' : !window.enabled ? 'Disabled' : 'Scheduled' }}
                  </span>
                </td>
                <td class="py-4 px-4 text-sm text-gray-600 dark:text-gray-300">
                  {{ window.next_occurrence ? formatDateTime(window.next_occurrence) : '-' }}
                </td>
                <td class="py-4 px-4 text-right">
                  <div class="flex items-center justify-end gap-2" @click.stop>
                    <button
                      @click="toggleWindow(window)"
                      :class="[
                        'p-1.5 rounded-md transition-colors',
                        window.enabled
                          ? 'text-green-600 hover:bg-green-100 dark:hover:bg-green-900/30'
                          : 'text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700'
                      ]"
                      :title="window.enabled ? 'Disable' : 'Enable'"
                    >
                      <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path v-if="window.enabled" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
                        <path v-else stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 14l2-2m0 0l2-2m-2 2l-2-2m2 2l2 2m7-2a9 9 0 11-18 0 9 9 0 0118 0z" />
                      </svg>
                    </button>
                    <button
                      @click="deleteWindow(window)"
                      class="p-1.5 rounded-md text-red-600 hover:bg-red-100 dark:hover:bg-red-900/30 transition-colors"
                      title="Delete"
                    >
                      <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
                      </svg>
                    </button>
                  </div>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </BaseCard>

      <!-- Loading State -->
      <BaseCard v-else class="mt-6">
        <div class="text-center py-12">
          <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full mx-auto mb-3"></div>
          <p class="text-sm text-gray-500 dark:text-gray-400">Loading maintenance windows...</p>
        </div>
      </BaseCard>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue';
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
const windows = ref([]);
const loading = ref(true);

const loadWindows = async () => {
  loading.value = true;
  try {
    const response = await axios.get('/api/maintenance-windows', {
      params: { project_id: projectId.value },
    });
    windows.value = response.data || [];
  } catch (error) {
    console.error('Failed to load maintenance windows:', error);
    alert('Failed to load maintenance windows. Please try again.');
  } finally {
    loading.value = false;
  }
};

const toggleWindow = async (window) => {
  if (!confirm(`Are you sure you want to ${window.enabled ? 'disable' : 'enable'} this maintenance window?`)) {
    return;
  }

  try {
    await axios.put(`/api/maintenance-windows/${window.id}`, {
      enabled: !window.enabled,
    });
    window.enabled = !window.enabled;
    // Reload to get updated is_active status
    loadWindows();
  } catch (error) {
    console.error('Failed to toggle window:', error);
    alert(`Failed to ${window.enabled ? 'disable' : 'enable'} maintenance window. Please try again.`);
  }
};

const deleteWindow = async (window) => {
  if (!confirm(`Are you sure you want to delete "${window.name}"? This action cannot be undone.`)) {
    return;
  }

  try {
    await axios.delete(`/api/maintenance-windows/${window.id}`);
    windows.value = windows.value.filter((w) => w.id !== window.id);
  } catch (error) {
    console.error('Failed to delete window:', error);
    alert('Failed to delete maintenance window. Please try again.');
  }
};

const formatRecurrence = (window) => {
  const type = window.recurrence_type;
  if (type === 'daily') return 'Daily';
  if (type === 'weekly') {
    const days = window.recurrence_days || [];
    const dayNames = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
    const dayStr = days.map(d => dayNames[d]).join(', ');
    return `Weekly (${dayStr})`;
  }
  if (type === 'monthly') {
    const days = window.recurrence_days || [];
    return `Monthly (day ${days.join(', ')})`;
  }
  return type;
};

const formatDateTime = (isoString) => {
  const date = new Date(isoString);
  return date.toLocaleString();
};

onMounted(async () => {
  try {
    await fetchUser();
    const projectResponse = await axios.get(`/api/projects/${projectId.value}`);
    project.value = projectResponse.data;
    loadWindows();
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
