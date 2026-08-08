<template>
  <AppLayout :user="user" :current-project="currentProject">
    <div class="api-monitoring-page max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <!-- Header -->
      <div class="mb-6">
        <div class="flex items-center justify-between">
          <div>
            <h1 class="text-2xl font-bold text-gray-900">API Monitoring</h1>
            <p class="text-sm text-gray-500 mt-1">
              Monitor your HTTP endpoints performance and reliability
            </p>
          </div>
          <div class="flex items-center gap-3">
            <select v-model="timeRange" @change="refreshData" class="time-select">
              <option value="15m">Last 15 minutes</option>
              <option value="1h">Last 1 hour</option>
              <option value="6h">Last 6 hours</option>
              <option value="24h">Last 24 hours</option>
              <option value="7d">Last 7 days</option>
            </select>
          </div>
        </div>
      </div>

      <!-- Loading State -->
      <div v-if="loading" class="flex items-center justify-center py-12">
        <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full"></div>
        <span class="ml-3 text-gray-600">Loading API data...</span>
      </div>

      <div v-else class="space-y-6">
        <!-- Summary Stats -->
        <div class="grid grid-cols-1 md:grid-cols-5 gap-4">
          <BaseCard>
            <div class="text-sm font-medium text-gray-500">Total Requests</div>
            <div class="mt-1 text-2xl font-bold text-gray-900">
              {{ formatNumber(summary.totalRequests) }}
            </div>
          </BaseCard>
          <BaseCard>
            <div class="text-sm font-medium text-gray-500">Avg Latency</div>
            <div class="mt-1 text-2xl font-bold text-gray-900">
              {{ formatDuration(summary.avgLatency) }}
            </div>
          </BaseCard>
          <BaseCard>
            <div class="text-sm font-medium text-gray-500">Error Rate</div>
            <div :class="['mt-1 text-2xl font-bold', getErrorRateClass(summary.errorRate)]">
              {{ formatPercent(summary.errorRate) }}
            </div>
          </BaseCard>
          <BaseCard>
            <div class="text-sm font-medium text-gray-500">P99 Latency</div>
            <div class="mt-1 text-2xl font-bold text-gray-900">
              {{ formatDuration(summary.p99Latency) }}
            </div>
          </BaseCard>
          <BaseCard>
            <div class="text-sm font-medium text-gray-500">Endpoints</div>
            <div class="mt-1 text-2xl font-bold text-gray-900">
              {{ endpoints.length }}
            </div>
          </BaseCard>
        </div>

        <!-- Status Code Distribution -->
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <BaseCard>
            <template #header>
              <h2 class="text-lg font-semibold text-gray-900">Status Code Distribution</h2>
            </template>
            <StatusCodeChart :data="statusCodeData" />
          </BaseCard>

          <BaseCard>
            <template #header>
              <h2 class="text-lg font-semibold text-gray-900">Request Volume</h2>
            </template>
            <div class="h-64">
              <RequestVolumeChart :data="requestVolumeData" />
            </div>
          </BaseCard>
        </div>

        <!-- Top Errors -->
        <BaseCard v-if="topErrors.length > 0">
          <template #header>
            <div class="flex items-center justify-between">
              <h2 class="text-lg font-semibold text-gray-900">Top Errors</h2>
              <span class="text-sm text-gray-500">{{ topErrors.length }} error types</span>
            </div>
          </template>
          <div class="overflow-x-auto">
            <table class="min-w-full divide-y divide-gray-200">
              <thead class="bg-gray-50">
                <tr>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                    Endpoint
                  </th>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                    Status
                  </th>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                    Error Message
                  </th>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                    Count
                  </th>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                    Last Seen
                  </th>
                </tr>
              </thead>
              <tbody class="bg-white divide-y divide-gray-200">
                <tr
                  v-for="error in topErrors"
                  :key="error.id"
                  class="hover:bg-gray-50"
                >
                  <td class="px-4 py-3">
                    <div class="flex items-center gap-2">
                      <span :class="['method-badge', `method-${error.method.toLowerCase()}`]">
                        {{ error.method }}
                      </span>
                      <span class="text-sm font-mono text-gray-900">{{ error.path }}</span>
                    </div>
                  </td>
                  <td class="px-4 py-3">
                    <span :class="['status-badge', getStatusBadgeClass(error.statusCode)]">
                      {{ error.statusCode }}
                    </span>
                  </td>
                  <td class="px-4 py-3">
                    <span class="text-sm text-gray-600 truncate max-w-xs block">
                      {{ error.message || 'No message' }}
                    </span>
                  </td>
                  <td class="px-4 py-3">
                    <span class="text-sm font-medium text-gray-900">
                      {{ formatNumber(error.count) }}
                    </span>
                  </td>
                  <td class="px-4 py-3">
                    <span class="text-sm text-gray-500">
                      {{ formatRelativeTime(error.lastSeen) }}
                    </span>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </BaseCard>

        <!-- Endpoints Table -->
        <BaseCard>
          <template #header>
            <div class="flex items-center justify-between">
              <h2 class="text-lg font-semibold text-gray-900">All Endpoints</h2>
              <div class="flex items-center gap-3">
                <input
                  v-model="searchQuery"
                  type="text"
                  placeholder="Search endpoints..."
                  class="search-input"
                />
                <select v-model="methodFilter" class="filter-select">
                  <option value="">All Methods</option>
                  <option value="GET">GET</option>
                  <option value="POST">POST</option>
                  <option value="PUT">PUT</option>
                  <option value="DELETE">DELETE</option>
                  <option value="PATCH">PATCH</option>
                </select>
              </div>
            </div>
          </template>
          <div class="overflow-x-auto">
            <table class="min-w-full divide-y divide-gray-200">
              <thead class="bg-gray-50">
                <tr>
                  <th
                    @click="toggleSort('path')"
                    class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase cursor-pointer hover:bg-gray-100"
                  >
                    Endpoint
                  </th>
                  <th
                    @click="toggleSort('requestCount')"
                    class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase cursor-pointer hover:bg-gray-100"
                  >
                    Requests
                  </th>
                  <th
                    @click="toggleSort('errorRate')"
                    class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase cursor-pointer hover:bg-gray-100"
                  >
                    Error Rate
                  </th>
                  <th
                    @click="toggleSort('avgLatency')"
                    class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase cursor-pointer hover:bg-gray-100"
                  >
                    Avg Latency
                  </th>
                  <th
                    @click="toggleSort('p99Latency')"
                    class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase cursor-pointer hover:bg-gray-100"
                  >
                    P99 Latency
                  </th>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                    Trend
                  </th>
                </tr>
              </thead>
              <tbody class="bg-white divide-y divide-gray-200">
                <tr
                  v-for="endpoint in sortedEndpoints"
                  :key="`${endpoint.method}-${endpoint.path}`"
                  class="hover:bg-gray-50 cursor-pointer"
                  @click="goToEndpoint(endpoint)"
                >
                  <td class="px-4 py-4">
                    <div class="flex items-center gap-2">
                      <span :class="['method-badge', `method-${endpoint.method.toLowerCase()}`]">
                        {{ endpoint.method }}
                      </span>
                      <span class="text-sm font-mono text-gray-900">{{ endpoint.path }}</span>
                    </div>
                  </td>
                  <td class="px-4 py-4">
                    <span class="text-sm text-gray-900">
                      {{ formatNumber(endpoint.requestCount) }}
                    </span>
                  </td>
                  <td class="px-4 py-4">
                    <span :class="['text-sm font-medium', getErrorRateClass(endpoint.errorRate)]">
                      {{ formatPercent(endpoint.errorRate) }}
                    </span>
                  </td>
                  <td class="px-4 py-4">
                    <span class="text-sm text-gray-900">
                      {{ formatDuration(endpoint.avgLatency) }}
                    </span>
                  </td>
                  <td class="px-4 py-4">
                    <span class="text-sm text-gray-900">
                      {{ formatDuration(endpoint.p99Latency) }}
                    </span>
                  </td>
                  <td class="px-4 py-4">
                    <div class="w-24 h-6">
                      <MiniChart :data="endpoint.trend || []" :color="getTrendColor(endpoint)" />
                    </div>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </BaseCard>
      </div>

      <!-- Empty State -->
      <div v-if="!loading && endpoints.length === 0" class="text-center py-12">
        <svg class="mx-auto h-12 w-12 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 9l3 3-3 3m5 0h3M5 20h14a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
        </svg>
        <h3 class="mt-2 text-sm font-medium text-gray-900">No API data found</h3>
        <p class="mt-1 text-sm text-gray-500">
          Start sending HTTP traces to see your endpoints here.
        </p>
      </div>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useAuth } from '@/composables/useAuth'
import { formatDistanceToNow } from 'date-fns'
import AppLayout from '@/Layouts/AppLayout.vue'
import BaseCard from '@/components/BaseCard.vue'
import StatusCodeChart from '@/components/api/StatusCodeChart.vue'
import RequestVolumeChart from '@/components/api/RequestVolumeChart.vue'
import MiniChart from '@/components/charts/MiniChart.vue'
import axios from 'axios'

const route = useRoute()
const router = useRouter()
const { user } = useAuth()

const projectId = computed(() => route.params.id)
const currentProject = ref(null)
const endpoints = ref([])
const topErrors = ref([])
const statusCodeData = ref([])
const requestVolumeData = ref([])
const summary = ref({
  totalRequests: 0,
  avgLatency: 0,
  errorRate: 0,
  p99Latency: 0,
})
const loading = ref(false)
const timeRange = ref('1h')
const searchQuery = ref('')
const methodFilter = ref('')
const sortBy = ref('requestCount')
const sortDirection = ref('desc')

// Computed
const filteredEndpoints = computed(() => {
  let result = endpoints.value
  
  if (searchQuery.value) {
    const query = searchQuery.value.toLowerCase()
    result = result.filter(e => e.path.toLowerCase().includes(query))
  }
  
  if (methodFilter.value) {
    result = result.filter(e => e.method === methodFilter.value)
  }
  
  return result
})

const sortedEndpoints = computed(() => {
  const sorted = [...filteredEndpoints.value]
  
  sorted.sort((a, b) => {
    const aVal = a[sortBy.value] || 0
    const bVal = b[sortBy.value] || 0
    
    if (sortDirection.value === 'asc') {
      return aVal > bVal ? 1 : -1
    }
    return aVal < bVal ? 1 : -1
  })
  
  return sorted
})

// API calls
const fetchProject = async () => {
  try {
    const response = await axios.get(`/api/projects/${projectId.value}`)
    currentProject.value = response.data
  } catch (error) {
    console.error('Failed to fetch project:', error)
  }
}

const fetchData = async () => {
  loading.value = true
  try {
    const [endpointsRes, errorsRes, summaryRes] = await Promise.all([
      axios.get(`/api/projects/${projectId.value}/api-endpoints`, {
        params: { time_range: timeRange.value }
      }),
      axios.get(`/api/projects/${projectId.value}/api-endpoints/errors`, {
        params: { time_range: timeRange.value, limit: 10 }
      }),
      axios.get(`/api/projects/${projectId.value}/api-endpoints/summary`, {
        params: { time_range: timeRange.value }
      }),
    ])
    
    endpoints.value = endpointsRes.data.endpoints || []
    topErrors.value = errorsRes.data.errors || []
    summary.value = summaryRes.data.summary || summary.value
    statusCodeData.value = summaryRes.data.statusCodes || []
    requestVolumeData.value = summaryRes.data.requestVolume || []
  } catch (error) {
    console.error('Failed to fetch API data:', error)
  } finally {
    loading.value = false
  }
}

const refreshData = () => {
  fetchData()
}

const toggleSort = (field) => {
  if (sortBy.value === field) {
    sortDirection.value = sortDirection.value === 'asc' ? 'desc' : 'asc'
  } else {
    sortBy.value = field
    sortDirection.value = 'desc'
  }
}

const goToEndpoint = (endpoint) => {
  router.push({
    path: `/p/${projectId.value}/traces`,
    query: { http_method: endpoint.method, http_route: endpoint.path }
  })
}

// Formatting
const formatNumber = (num) => {
  if (num === undefined || num === null) return '0'
  if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`
  if (num >= 1000) return `${(num / 1000).toFixed(1)}K`
  return num.toString()
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

const formatRelativeTime = (dateString) => {
  if (!dateString) return 'Unknown'
  try {
    return formatDistanceToNow(new Date(dateString), { addSuffix: true })
  } catch {
    return dateString
  }
}

// Styling
const getErrorRateClass = (rate) => {
  if (!rate || rate < 0.01) return 'text-green-600'
  if (rate < 0.05) return 'text-yellow-600'
  return 'text-red-600'
}

const getStatusBadgeClass = (code) => {
  if (code >= 500) return 'status-5xx'
  if (code >= 400) return 'status-4xx'
  if (code >= 300) return 'status-3xx'
  return 'status-2xx'
}

const getTrendColor = (endpoint) => {
  if (endpoint.errorRate > 0.05) return '#EF4444'
  if (endpoint.errorRate > 0.01) return '#F59E0B'
  return '#3B82F6'
}

onMounted(async () => {
  await fetchProject()
  await fetchData()
})
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

.time-select,
.filter-select {
  @apply px-3 py-2 text-sm bg-white border border-gray-300 text-gray-900 rounded-md focus:ring-2 focus:ring-primary-500;
}

.search-input {
  @apply px-3 py-2 text-sm bg-white border border-gray-300 text-gray-900 rounded-md focus:ring-2 focus:ring-primary-500 w-64;
}

.method-badge {
  @apply px-2 py-0.5 text-xs font-bold rounded;
}

.method-get {
  @apply bg-green-100 text-green-800;
}

.method-post {
  @apply bg-blue-100 text-blue-800;
}

.method-put {
  @apply bg-yellow-100 text-yellow-800;
}

.method-delete {
  @apply bg-red-100 text-red-800;
}

.method-patch {
  @apply bg-purple-100 text-purple-800;
}

.status-badge {
  @apply px-2 py-0.5 text-xs font-medium rounded;
}

.status-2xx {
  @apply bg-green-100 text-green-800;
}

.status-3xx {
  @apply bg-blue-100 text-blue-800;
}

.status-4xx {
  @apply bg-yellow-100 text-yellow-800;
}

.status-5xx {
  @apply bg-red-100 text-red-800;
}
</style>
