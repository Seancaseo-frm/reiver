<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-3xl mx-auto px-8 py-6">
      <!-- Page Header -->
      <div class="mb-6">
        <div>
          <router-link
            :to="`/p/${projectId}/maintenance`"
            class="text-primary-600 dark:text-primary-400 hover:text-primary-700 dark:hover:text-primary-300 text-sm font-medium inline-flex items-center gap-2 mb-4"
          >
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 19l-7-7m0 0l7-7m-7 7h18" />
            </svg>
            Back to Maintenance
          </router-link>
          <h1 class="text-2xl font-semibold text-gray-900">Edit Maintenance Window</h1>
          <p class="text-sm text-gray-500 dark:text-gray-400 mt-1">
            Update the maintenance window settings
          </p>
        </div>
      </div>

      <!-- Loading State -->
      <BaseCard v-if="loading" class="mt-6">
        <div class="text-center py-12">
          <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full mx-auto mb-3"></div>
          <p class="text-sm text-gray-500 dark:text-gray-400">Loading maintenance window...</p>
        </div>
      </BaseCard>

      <!-- Form -->
      <BaseCard v-else class="mt-6">
        <form @submit.prevent="handleSubmit" novalidate>
          <!-- Basic Info -->
          <div class="space-y-6">
            <div>
              <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                Name <span class="text-red-500">*</span>
              </label>
              <input
                v-model="formData.name"
                type="text"
                class="w-full px-3 py-2 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-primary-500 focus:border-transparent"
                placeholder="e.g., Database Maintenance, API Deployment"
              />
            </div>

            <div>
              <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                Description
              </label>
              <textarea
                v-model="formData.description"
                rows="2"
                class="w-full px-3 py-2 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-primary-500 focus:border-transparent"
                placeholder="Optional description of this maintenance window"
              />
            </div>

            <div class="flex items-center">
              <input
                v-model="formData.enabled"
                type="checkbox"
                class="mr-2 text-primary-600 focus:ring-primary-500 rounded"
              />
              <label class="text-sm text-gray-700 dark:text-gray-300">Enabled</label>
            </div>
          </div>

          <!-- Schedule Type -->
          <div class="mt-8">
            <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-3">
              Schedule Type <span class="text-red-500">*</span>
            </label>
            <div class="flex gap-4">
              <label class="flex items-center">
                <input
                  v-model="formData.schedule_type"
                  type="radio"
                  value="one_time"
                  class="mr-2 text-primary-600 focus:ring-primary-500"
                />
                <span class="text-sm text-gray-700 dark:text-gray-300">One-time</span>
              </label>
              <label class="flex items-center">
                <input
                  v-model="formData.schedule_type"
                  type="radio"
                  value="recurring"
                  class="mr-2 text-primary-600 focus:ring-primary-500"
                />
                <span class="text-sm text-gray-700 dark:text-gray-300">Recurring</span>
              </label>
            </div>
          </div>

          <!-- One-time Schedule -->
          <div v-if="formData.schedule_type === 'one_time'" class="mt-6 space-y-4 p-4 bg-gray-50 dark:bg-gray-800/50 rounded-lg">
            <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                  Start Time <span class="text-red-500">*</span>
                </label>
                <input
                  v-model="formData.start_time"
                  type="datetime-local"
                  class="w-full px-3 py-2 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-primary-500 focus:border-transparent"
                />
              </div>
              <div>
                <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                  End Time <span class="text-red-500">*</span>
                </label>
                <input
                  v-model="formData.end_time"
                  type="datetime-local"
                  class="w-full px-3 py-2 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-primary-500 focus:border-transparent"
                />
              </div>
            </div>
          </div>

          <!-- Recurring Schedule -->
          <div v-else class="mt-6 space-y-4 p-4 bg-gray-50 dark:bg-gray-800/50 rounded-lg">
            <div>
              <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                Repeats <span class="text-red-500">*</span>
              </label>
              <select
                v-model="formData.recurrence_type"
                class="w-full px-3 py-2 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-primary-500 focus:border-transparent"
              >
                <option value="daily">Daily</option>
                <option value="weekly">Weekly</option>
                <option value="monthly">Monthly</option>
              </select>
            </div>

            <!-- Weekly Days -->
            <div v-if="formData.recurrence_type === 'weekly'">
              <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                Days of Week <span class="text-red-500">*</span>
              </label>
              <div class="flex flex-wrap gap-2">
                <label
                  v-for="(day, index) in weekDays"
                  :key="index"
                  class="flex items-center"
                >
                  <input
                    type="checkbox"
                    :value="index"
                    v-model="formData.recurrence_days"
                    class="mr-1.5 text-primary-600 focus:ring-primary-500 rounded"
                  />
                  <span class="text-sm text-gray-700 dark:text-gray-300">{{ day }}</span>
                </label>
              </div>
            </div>

            <!-- Monthly Days -->
            <div v-if="formData.recurrence_type === 'monthly'">
              <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                Day of Month <span class="text-red-500">*</span>
              </label>
              <select
                v-model="selectedMonthDay"
                @change="formData.recurrence_days = [selectedMonthDay]"
                class="w-full px-3 py-2 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-primary-500 focus:border-transparent"
              >
                <option v-for="d in 31" :key="d" :value="d">{{ d }}</option>
              </select>
            </div>

            <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                  Start Time <span class="text-red-500">*</span>
                </label>
                <input
                  v-model="formData.recurrence_start_time"
                  type="time"
                  class="w-full px-3 py-2 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-primary-500 focus:border-transparent"
                />
              </div>
              <div>
                <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                  Duration (minutes) <span class="text-red-500">*</span>
                </label>
                <input
                  v-model.number="formData.recurrence_duration_minutes"
                  type="number"
                  min="1"
                  max="1440"
                  class="w-full px-3 py-2 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-primary-500 focus:border-transparent"
                  placeholder="60"
                />
              </div>
            </div>

            <div>
              <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                Timezone
              </label>
              <select
                v-model="formData.recurrence_timezone"
                class="w-full px-3 py-2 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-primary-500 focus:border-transparent"
              >
                <option v-for="tz in timezones" :key="tz" :value="tz">{{ tz }}</option>
              </select>
            </div>

            <div>
              <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                End Date (optional)
              </label>
              <input
                v-model="formData.recurrence_end_date"
                type="date"
                class="w-full px-3 py-2 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-primary-500 focus:border-transparent"
              />
            </div>
          </div>

          <!-- Actions -->
          <div class="flex justify-end items-center gap-3 mt-8 pt-6 border-t border-gray-200 dark:border-gray-700">
            <router-link
              :to="`/p/${projectId}/maintenance`"
              class="px-4 py-2 text-sm font-medium text-gray-700 dark:text-gray-300 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-md hover:bg-gray-50 dark:hover:bg-gray-700 focus:outline-none focus:ring-2 focus:ring-primary-500"
            >
              Cancel
            </router-link>
            <button
              type="submit"
              :disabled="saving"
              class="px-4 py-2 text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 rounded-md focus:outline-none focus:ring-2 focus:ring-primary-500 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <span v-if="saving">Saving...</span>
              <span v-else>Save Changes</span>
            </button>
          </div>
        </form>
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
const windowId = computed(() => route.params.windowId);
const project = ref(null);
const loading = ref(true);
const saving = ref(false);
const selectedMonthDay = ref(1);

const weekDays = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];

const timezones = [
  'UTC',
  'America/New_York',
  'America/Chicago',
  'America/Denver',
  'America/Los_Angeles',
  'Europe/London',
  'Europe/Paris',
  'Europe/Berlin',
  'Asia/Tokyo',
  'Asia/Shanghai',
  'Asia/Kolkata',
  'Australia/Sydney',
];

const formData = ref({
  name: '',
  description: '',
  schedule_type: 'one_time',
  start_time: '',
  end_time: '',
  recurrence_type: 'daily',
  recurrence_days: [],
  recurrence_start_time: '22:00',
  recurrence_duration_minutes: 60,
  recurrence_timezone: 'UTC',
  recurrence_end_date: '',
  enabled: true,
});

const loadWindow = async () => {
  loading.value = true;
  try {
    const response = await axios.get(`/api/maintenance-windows/${windowId.value}`);
    const w = response.data;

    formData.value.name = w.name;
    formData.value.description = w.description || '';
    formData.value.schedule_type = w.schedule_type;
    formData.value.enabled = w.enabled;

    if (w.schedule_type === 'one_time') {
      // Convert ISO to datetime-local format
      formData.value.start_time = w.start_time ? toDatetimeLocal(w.start_time) : '';
      formData.value.end_time = w.end_time ? toDatetimeLocal(w.end_time) : '';
    } else {
      formData.value.recurrence_type = w.recurrence_type || 'daily';
      formData.value.recurrence_days = w.recurrence_days || [];
      formData.value.recurrence_start_time = w.recurrence_start_time || '22:00';
      formData.value.recurrence_duration_minutes = w.recurrence_duration_minutes || 60;
      formData.value.recurrence_timezone = w.recurrence_timezone || 'UTC';
      formData.value.recurrence_end_date = w.recurrence_end_date || '';

      if (w.recurrence_type === 'monthly' && w.recurrence_days?.length > 0) {
        selectedMonthDay.value = w.recurrence_days[0];
      }
    }
  } catch (error) {
    console.error('Failed to load maintenance window:', error);
    alert('Failed to load maintenance window. Please try again.');
    router.push(`/p/${projectId.value}/maintenance`);
  } finally {
    loading.value = false;
  }
};

const toDatetimeLocal = (isoString) => {
  const date = new Date(isoString);
  const offset = date.getTimezoneOffset();
  const local = new Date(date.getTime() - offset * 60000);
  return local.toISOString().slice(0, 16);
};

const handleSubmit = async () => {
  if (!formData.value.name.trim()) {
    alert('Please enter a name for the maintenance window.');
    return;
  }

  if (formData.value.schedule_type === 'one_time') {
    if (!formData.value.start_time || !formData.value.end_time) {
      alert('Please enter both start and end times.');
      return;
    }
  } else {
    if (!formData.value.recurrence_start_time || !formData.value.recurrence_duration_minutes) {
      alert('Please enter start time and duration.');
      return;
    }
    if (formData.value.recurrence_type === 'weekly' && formData.value.recurrence_days.length === 0) {
      alert('Please select at least one day of the week.');
      return;
    }
    if (formData.value.recurrence_type === 'monthly' && formData.value.recurrence_days.length === 0) {
      alert('Please select a day of the month.');
      return;
    }
  }

  saving.value = true;

  try {
    const payload = {
      name: formData.value.name,
      description: formData.value.description || null,
      schedule_type: formData.value.schedule_type,
      enabled: formData.value.enabled,
    };

    if (formData.value.schedule_type === 'one_time') {
      payload.start_time = new Date(formData.value.start_time).toISOString();
      payload.end_time = new Date(formData.value.end_time).toISOString();
    } else {
      payload.recurrence_type = formData.value.recurrence_type;
      payload.recurrence_start_time = formData.value.recurrence_start_time;
      payload.recurrence_duration_minutes = formData.value.recurrence_duration_minutes;
      payload.recurrence_timezone = formData.value.recurrence_timezone;
      if (formData.value.recurrence_end_date) {
        payload.recurrence_end_date = formData.value.recurrence_end_date;
      }
      if (formData.value.recurrence_type !== 'daily') {
        payload.recurrence_days = formData.value.recurrence_days.map(d => parseInt(d));
      }
    }

    await axios.put(`/api/maintenance-windows/${windowId.value}`, payload);
    router.push(`/p/${projectId.value}/maintenance`);
  } catch (error) {
    console.error('Failed to update maintenance window:', error);
    const message = error.response?.data?.error || 'Failed to update maintenance window. Please try again.';
    alert(message);
  } finally {
    saving.value = false;
  }
};

onMounted(async () => {
  try {
    await fetchUser();
    const projectResponse = await axios.get(`/api/projects/${projectId.value}`);
    project.value = projectResponse.data;
    loadWindow();
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
