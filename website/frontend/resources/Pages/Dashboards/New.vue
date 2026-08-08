<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-3xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
      <div class="mb-6">
        <router-link
          :to="`/p/${projectId}/dashboards`"
          class="text-primary-600 dark:text-primary-400 hover:text-primary-700 dark:hover:text-primary-300 text-sm font-medium inline-flex items-center gap-2"
        >
          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 19l-7-7m0 0l7-7m-7 7h18" />
          </svg>
          Back to Dashboards
        </router-link>
      </div>

      <BaseCard class="p-6">
        <h1 class="text-2xl font-bold text-gray-100 mb-6">Create New Dashboard</h1>

        <form @submit.prevent="createDashboard">
          <div class="space-y-4">
            <!-- Dashboard Name -->
            <div>
              <label class="block text-sm font-medium text-gray-300 mb-2">
                Dashboard Name <span class="text-danger-400">*</span>
              </label>
              <input
                v-model="dashboardForm.name"
                type="text"
                required
                class="w-full px-3 py-2 bg-gray-700 border border-gray-600 text-gray-100 rounded-md focus:outline-none focus:ring-2 focus:ring-primary-500"
                placeholder="Error Overview Dashboard"
              />
            </div>

            <!-- Description -->
            <div>
              <label class="block text-sm font-medium text-gray-300 mb-2">
                Description
              </label>
              <textarea
                v-model="dashboardForm.description"
                rows="3"
                class="w-full px-3 py-2 bg-gray-700 border border-gray-600 text-gray-100 rounded-md focus:outline-none focus:ring-2 focus:ring-primary-500"
                placeholder="Overview of errors and metrics..."
              />
            </div>

            <!-- Refresh Interval -->
            <div>
              <label class="block text-sm font-medium text-gray-300 mb-2">
                Refresh Interval (seconds)
              </label>
              <input
                v-model.number="dashboardForm.refresh_interval"
                type="number"
                min="10"
                max="3600"
                class="w-full px-3 py-2 bg-gray-700 border border-gray-600 text-gray-100 rounded-md focus:outline-none focus:ring-2 focus:ring-primary-500"
                placeholder="30"
              />
              <p class="mt-1 text-xs text-gray-400">Leave empty for no auto-refresh</p>
            </div>

            <!-- Default Time Range -->
            <div>
              <label class="block text-sm font-medium text-gray-300 mb-2">
                Default Time Range
              </label>
              <select
                v-model="dashboardForm.time_range"
                class="w-full px-3 py-2 bg-gray-700 border border-gray-600 text-gray-100 rounded-md focus:outline-none focus:ring-2 focus:ring-primary-500"
              >
                <option value="1h">Last 1 hour</option>
                <option value="24h">Last 24 hours</option>
                <option value="7d">Last 7 days</option>
                <option value="30d">Last 30 days</option>
              </select>
            </div>
          </div>

          <!-- Actions -->
          <div class="flex items-center justify-end gap-3 mt-6 pt-6 border-t border-gray-700">
            <router-link
              :to="`/p/${projectId}/dashboards`"
              class="px-4 py-2 border border-gray-600 rounded-md shadow-sm text-sm font-medium text-gray-300 bg-gray-700 hover:bg-gray-600"
            >
              Cancel
            </router-link>
            <button
              type="submit"
              :disabled="creating"
              class="px-4 py-2 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {{ creating ? 'Creating...' : 'Create Dashboard' }}
            </button>
          </div>
        </form>
      </BaseCard>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import AppLayout from '@/Layouts/AppLayout.vue'
import BaseCard from '@/components/BaseCard.vue'
import { useAuth } from '@/composables/useAuth'
import axios from 'axios'

const route = useRoute()
const router = useRouter()
const { user, fetchUser } = useAuth()
const projectId = ref(route.params.id)
const project = ref(null)
const creating = ref(false)

const dashboardForm = ref({
  name: '',
  description: '',
  refresh_interval: 30,
  time_range: '1h',
})

const createDashboard = async () => {
  creating.value = true
  try {
    const payload = {
      name: dashboardForm.value.name,
      description: dashboardForm.value.description || null,
      refresh_interval: dashboardForm.value.refresh_interval || null,
      time_range: dashboardForm.value.time_range,
    }

    const response = await axios.post(`/api/dashboards/${projectId.value}/dashboards`, payload)
    
    // Navigate to edit page to add widgets
    router.push(`/p/${projectId.value}/dashboards/${response.data.id}/edit`)
  } catch (err) {
    console.error('Failed to create dashboard:', err)
    alert(err.response?.data?.message || 'Failed to create dashboard')
  } finally {
    creating.value = false
  }
}

const loadProject = async () => {
  try {
    const response = await axios.get(`/api/projects/${projectId.value}`)
    project.value = response.data
  } catch (err) {
    console.error('Failed to load project:', err)
  }
}

onMounted(async () => {
  await fetchUser()
  await loadProject()
})
</script>


