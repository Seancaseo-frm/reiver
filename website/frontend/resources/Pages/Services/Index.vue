<template>
  <AppLayout :user="user" :current-project="currentProject">
    <div class="services-page max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <!-- Header -->
      <div class="mb-6">
        <div class="flex items-center justify-between">
          <div>
            <h1 class="text-2xl font-bold text-gray-900">Services</h1>
            <p class="text-sm text-gray-500 mt-1">
              Monitor your services health and dependencies
            </p>
          </div>
          <div class="flex items-center gap-3">
            <!-- Time Range -->
            <select v-model="timeRange" @change="refreshData" class="time-select">
              <option value="15m">Last 15 minutes</option>
              <option value="1h">Last 1 hour</option>
              <option value="6h">Last 6 hours</option>
              <option value="24h">Last 24 hours</option>
              <option value="7d">Last 7 days</option>
            </select>
            
            <!-- View Toggle -->
            <div class="flex items-center bg-gray-100 rounded-lg p-1">
              <button
                @click="viewMode = 'map'"
                :class="['view-btn', viewMode === 'map' ? 'active' : '']"
              >
                <svg class="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 20l-5.447-2.724A1 1 0 013 16.382V5.618a1 1 0 011.447-.894L9 7m0 13l6-3m-6 3V7m6 10l4.553 2.276A1 1 0 0021 18.382V7.618a1 1 0 00-.553-.894L15 4m0 13V4m0 0L9 7" />
                </svg>
                Map
              </button>
              <button
                @click="viewMode = 'list'"
                :class="['view-btn', viewMode === 'list' ? 'active' : '']"
              >
                <svg class="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 10h16M4 14h16M4 18h16" />
                </svg>
                List
              </button>
            </div>
          </div>
        </div>
      </div>

      <!-- Loading State -->
      <div v-if="loading" class="flex items-center justify-center py-12">
        <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full"></div>
        <span class="ml-3 text-gray-600">Loading services...</span>
      </div>

      <!-- Service Map View -->
      <div v-else-if="viewMode === 'map'" class="space-y-6">
        <BaseCard>
          <template #header>
            <div class="flex items-center justify-between">
              <h2 class="text-lg font-semibold text-gray-900">Service Topology</h2>
              <div class="flex items-center gap-2">
                <span class="text-sm text-gray-500">
                  {{ services.length }} services
                </span>
              </div>
            </div>
          </template>
          <ServiceMap
            :services="services"
            :dependencies="dependencies"
            @select-service="handleServiceSelect"
          />
        </BaseCard>

        <!-- Legend -->
        <div class="flex items-center justify-center gap-6 text-sm text-gray-600">
          <div class="flex items-center gap-2">
            <span class="w-3 h-3 rounded-full bg-green-500"></span>
            <span>Healthy</span>
          </div>
          <div class="flex items-center gap-2">
            <span class="w-3 h-3 rounded-full bg-yellow-500"></span>
            <span>Degraded</span>
          </div>
          <div class="flex items-center gap-2">
            <span class="w-3 h-3 rounded-full bg-red-500"></span>
            <span>Unhealthy</span>
          </div>
        </div>
      </div>

      <!-- Services List View -->
      <div v-else class="space-y-4">
        <!-- Summary Stats -->
        <div class="grid grid-cols-1 md:grid-cols-4 gap-4">
          <BaseCard>
            <div class="text-sm font-medium text-gray-500">Total Services</div>
            <div class="mt-1 text-2xl font-bold text-gray-900">
              {{ services.length }}
            </div>
          </BaseCard>
          <BaseCard>
            <div class="text-sm font-medium text-gray-500">Healthy</div>
            <div class="mt-1 text-2xl font-bold text-green-600">
              {{ healthyCount }}
            </div>
          </BaseCard>
          <BaseCard>
            <div class="text-sm font-medium text-gray-500">Degraded</div>
            <div class="mt-1 text-2xl font-bold text-yellow-600">
              {{ degradedCount }}
            </div>
          </BaseCard>
          <BaseCard>
            <div class="text-sm font-medium text-gray-500">Unhealthy</div>
            <div class="mt-1 text-2xl font-bold text-red-600">
              {{ unhealthyCount }}
            </div>
          </BaseCard>
        </div>

        <!-- Services Table -->
        <BaseCard>
          <template #header>
            <div class="flex items-center justify-between">
              <h2 class="text-lg font-semibold text-gray-900">All Services</h2>
              <input
                v-model="searchQuery"
                type="text"
                placeholder="Search services..."
                class="px-3 py-1.5 text-sm bg-gray-50 border border-gray-200 rounded-md focus:ring-2 focus:ring-primary-500"
              />
            </div>
          </template>
          <div class="overflow-x-auto">
            <table class="min-w-full divide-y divide-gray-200">
              <thead class="bg-gray-50">
                <tr>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                    Service
                  </th>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                    Status
                  </th>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                    Requests/s
                  </th>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                    Error Rate
                  </th>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                    P50 Latency
                  </th>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                    P99 Latency
                  </th>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                    Dependencies
                  </th>
                </tr>
              </thead>
              <tbody class="bg-white divide-y divide-gray-200">
                <tr
                  v-for="service in filteredServices"
                  :key="service.name"
                  class="hover:bg-gray-50 cursor-pointer"
                  @click="goToService(service)"
                >
                  <td class="px-4 py-4">
                    <div class="flex items-center gap-3">
                      <div
                        :class="['w-2 h-2 rounded-full', getHealthClass(service.health)]"
                      ></div>
                      <div>
                        <div class="text-sm font-medium text-gray-900">
                          {{ service.name }}
                        </div>
                        <div class="text-xs text-gray-500">
                          {{ service.environment || 'default' }}
                        </div>
                      </div>
                    </div>
                  </td>
                  <td class="px-4 py-4">
                    <span :class="['px-2 py-1 text-xs font-medium rounded-full', getHealthBadgeClass(service.health)]">
                      {{ service.health }}
                    </span>
                  </td>
                  <td class="px-4 py-4">
                    <span class="text-sm text-gray-900">
                      {{ formatNumber(service.requestRate) }}
                    </span>
                  </td>
                  <td class="px-4 py-4">
                    <span :class="['text-sm font-medium', getErrorRateClass(service.errorRate)]">
                      {{ formatPercent(service.errorRate) }}
                    </span>
                  </td>
                  <td class="px-4 py-4">
                    <span class="text-sm text-gray-900">
                      {{ formatDuration(service.p50Latency) }}
                    </span>
                  </td>
                  <td class="px-4 py-4">
                    <span class="text-sm text-gray-900">
                      {{ formatDuration(service.p99Latency) }}
                    </span>
                  </td>
                  <td class="px-4 py-4">
                    <span class="text-sm text-gray-600">
                      {{ service.dependencyCount || 0 }}
                    </span>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </BaseCard>
      </div>

      <!-- Empty State -->
      <div v-if="!loading && services.length === 0" class="text-center py-12">
        <svg class="mx-auto h-12 w-12 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10" />
        </svg>
        <h3 class="mt-2 text-sm font-medium text-gray-900">No services found</h3>
        <p class="mt-1 text-sm text-gray-500">
          Start sending traces to see your services appear here.
        </p>
      </div>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useAuth } from '@/composables/useAuth'
import { usePageContext } from '@/composables/usePageContext'
import AppLayout from '@/Layouts/AppLayout.vue'
import BaseCard from '@/components/BaseCard.vue'
import ServiceMap from '@/components/ServiceMap.vue'
import axios from 'axios'

const route = useRoute()
const router = useRouter()
const { user } = useAuth()

const projectId = computed(() => route.params.id)
const currentProject = ref(null)
const services = ref([])
const dependencies = ref([])
const loading = ref(false)
const timeRange = ref('1h')
const viewMode = ref('map')
const searchQuery = ref('')

// Computed
const filteredServices = computed(() => {
  if (!searchQuery.value) return services.value
  const query = searchQuery.value.toLowerCase()
  return services.value.filter(s => 
    s.name.toLowerCase().includes(query) ||
    s.environment?.toLowerCase().includes(query)
  )
})

const healthyCount = computed(() => 
  services.value.filter(s => s.health === 'healthy').length
)

const degradedCount = computed(() => 
  services.value.filter(s => s.health === 'degraded').length
)

const unhealthyCount = computed(() => 
  services.value.filter(s => s.health === 'unhealthy').length
)

// API calls
const fetchProject = async () => {
  try {
    const response = await axios.get(`/api/projects/${projectId.value}`)
    currentProject.value = response.data
  } catch (error) {
    console.error('Failed to fetch project:', error)
  }
}

const fetchServices = async () => {
  loading.value = true
  try {
    const response = await axios.get(`/api/projects/${projectId.value}/services`, {
      params: { time_range: timeRange.value }
    })
    services.value = response.data.services || []
    dependencies.value = response.data.dependencies || []
  } catch (error) {
    console.error('Failed to fetch services:', error)
    services.value = []
    dependencies.value = []
  } finally {
    loading.value = false
  }
}

const refreshData = () => {
  fetchServices()
}

// Event handlers
const handleServiceSelect = (service) => {
  goToService(service)
}

const goToService = (service) => {
  router.push(`/p/${projectId.value}/services/${encodeURIComponent(service.name)}`)
}

// Formatting
const formatNumber = (num) => {
  if (num === undefined || num === null) return '0'
  if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`
  if (num >= 1000) return `${(num / 1000).toFixed(1)}K`
  return num.toFixed(1)
}

const formatPercent = (num) => {
  if (num === undefined || num === null) return '0%'
  return `${(num * 100).toFixed(2)}%`
}

const formatDuration = (ms) => {
  if (ms === undefined || ms === null) return '0ms'
  if (ms < 1) return '<1ms'
  if (ms < 1000) return `${Math.round(ms)}ms`
  return `${(ms / 1000).toFixed(2)}s`
}

// Styling
const getHealthClass = (health) => {
  const classes = {
    healthy: 'bg-green-500',
    degraded: 'bg-yellow-500',
    unhealthy: 'bg-red-500',
  }
  return classes[health] || 'bg-gray-400'
}

const getHealthBadgeClass = (health) => {
  const classes = {
    healthy: 'bg-green-100 text-green-800',
    degraded: 'bg-yellow-100 text-yellow-800',
    unhealthy: 'bg-red-100 text-red-800',
  }
  return classes[health] || 'bg-gray-100 text-gray-800'
}

const getErrorRateClass = (rate) => {
  if (!rate || rate < 0.01) return 'text-green-600'
  if (rate < 0.05) return 'text-yellow-600'
  return 'text-red-600'
}

const { setPageSnapshot, clearPageSnapshot } = usePageContext()

watch([services, timeRange], () => {
  if (!services.value?.length) return
  setPageSnapshot({
    page: 'Services',
    time_range: timeRange.value,
    counts: {
      total: services.value.length,
      healthy: healthyCount.value,
      degraded: degradedCount.value,
      unhealthy: unhealthyCount.value,
    },
    top_services: services.value.slice(0, 10).map(s => ({
      name: s.name,
      health: s.health,
      request_rate: s.requestRate,
      error_rate: s.errorRate,
      p99_latency: s.p99Latency,
    })),
  })
}, { deep: true })

onMounted(async () => {
  await fetchProject()
  await fetchServices()
})

onUnmounted(() => clearPageSnapshot())
</script>

<style scoped>
.spinner {
  animation: spin 0.6s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

.time-select {
  @apply px-3 py-2 text-sm bg-white border border-gray-300 text-gray-900 rounded-md focus:ring-2 focus:ring-primary-500;
}

.view-btn {
  @apply flex items-center px-3 py-1.5 text-sm font-medium text-gray-600 rounded-md transition-colors;
}

.view-btn:hover {
  @apply text-gray-900;
}

.view-btn.active {
  @apply bg-white text-gray-900 shadow-sm;
}
</style>
