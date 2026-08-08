<template>
  <AppLayout :user="user" :current-project="currentProject">
    <div class="service-detail-page max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <!-- Header -->
      <div class="mb-6">
        <router-link
          :to="`/p/${projectId}/services`"
          class="text-primary-600 hover:text-primary-700 text-sm font-medium mb-2 inline-block"
        >
          ← Back to Services
        </router-link>
        <div class="flex items-center justify-between">
          <div class="flex items-center gap-4">
            <div
              :class="['w-4 h-4 rounded-full', getHealthClass(service?.health)]"
            ></div>
            <div>
              <h1 class="text-2xl font-bold text-gray-900">{{ serviceName }}</h1>
              <p class="text-sm text-gray-500 mt-1">
                {{ service?.environment || 'default' }} environment
              </p>
            </div>
          </div>
          <div class="flex items-center gap-3">
            <select v-model="timeRange" @change="refreshData" class="time-select">
              <option value="15m">Last 15 minutes</option>
              <option value="1h">Last 1 hour</option>
              <option value="6h">Last 6 hours</option>
              <option value="24h">Last 24 hours</option>
              <option value="7d">Last 7 days</option>
            </select>
            <span :class="['px-3 py-1 text-sm font-medium rounded-full', getHealthBadgeClass(service?.health)]">
              {{ service?.health || 'unknown' }}
            </span>
          </div>
        </div>
      </div>

      <!-- Loading -->
      <div v-if="loading" class="flex items-center justify-center py-12">
        <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full"></div>
        <span class="ml-3 text-gray-600">Loading service details...</span>
      </div>

      <div v-else class="space-y-6">
        <!-- RED Metrics Overview -->
        <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
          <!-- Rate -->
          <BaseCard>
            <div class="flex items-center justify-between mb-2">
              <span class="text-sm font-medium text-gray-500">Request Rate</span>
              <span :class="['text-xs px-2 py-0.5 rounded', getTrendClass(service?.rateTrend)]">
                {{ formatTrend(service?.rateTrend) }}
              </span>
            </div>
            <div class="text-3xl font-bold text-gray-900">
              {{ formatNumber(service?.requestRate) }}<span class="text-lg text-gray-500">/s</span>
            </div>
            <div class="mt-4 h-16">
              <MiniChart :data="rateChartData" color="#3B82F6" />
            </div>
          </BaseCard>

          <!-- Errors -->
          <BaseCard>
            <div class="flex items-center justify-between mb-2">
              <span class="text-sm font-medium text-gray-500">Error Rate</span>
              <span :class="['text-xs px-2 py-0.5 rounded', getTrendClass(service?.errorTrend, true)]">
                {{ formatTrend(service?.errorTrend) }}
              </span>
            </div>
            <div :class="['text-3xl font-bold', getErrorRateTextClass(service?.errorRate)]">
              {{ formatPercent(service?.errorRate) }}
            </div>
            <div class="mt-4 h-16">
              <MiniChart :data="errorChartData" color="#EF4444" />
            </div>
          </BaseCard>

          <!-- Duration -->
          <BaseCard>
            <div class="flex items-center justify-between mb-2">
              <span class="text-sm font-medium text-gray-500">P99 Latency</span>
              <span :class="['text-xs px-2 py-0.5 rounded', getTrendClass(service?.latencyTrend, true)]">
                {{ formatTrend(service?.latencyTrend) }}
              </span>
            </div>
            <div class="text-3xl font-bold text-gray-900">
              {{ formatDuration(service?.p99Latency) }}
            </div>
            <div class="mt-4 h-16">
              <MiniChart :data="latencyChartData" color="#10B981" />
            </div>
          </BaseCard>
        </div>

        <!-- Detailed Metrics -->
        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
          <!-- Latency Percentiles -->
          <BaseCard>
            <template #header>
              <h3 class="text-lg font-semibold text-gray-900">Latency Distribution</h3>
            </template>
            <div class="space-y-4">
              <div class="flex items-center justify-between">
                <span class="text-sm text-gray-600">P50</span>
                <div class="flex-1 mx-4">
                  <div class="h-2 bg-gray-100 rounded-full overflow-hidden">
                    <div
                      class="h-full bg-green-500 rounded-full"
                      :style="{ width: `${getLatencyWidth(service?.p50Latency)}%` }"
                    ></div>
                  </div>
                </div>
                <span class="text-sm font-medium text-gray-900 w-20 text-right">
                  {{ formatDuration(service?.p50Latency) }}
                </span>
              </div>
              <div class="flex items-center justify-between">
                <span class="text-sm text-gray-600">P90</span>
                <div class="flex-1 mx-4">
                  <div class="h-2 bg-gray-100 rounded-full overflow-hidden">
                    <div
                      class="h-full bg-yellow-500 rounded-full"
                      :style="{ width: `${getLatencyWidth(service?.p90Latency)}%` }"
                    ></div>
                  </div>
                </div>
                <span class="text-sm font-medium text-gray-900 w-20 text-right">
                  {{ formatDuration(service?.p90Latency) }}
                </span>
              </div>
              <div class="flex items-center justify-between">
                <span class="text-sm text-gray-600">P99</span>
                <div class="flex-1 mx-4">
                  <div class="h-2 bg-gray-100 rounded-full overflow-hidden">
                    <div
                      class="h-full bg-red-500 rounded-full"
                      :style="{ width: `${getLatencyWidth(service?.p99Latency)}%` }"
                    ></div>
                  </div>
                </div>
                <span class="text-sm font-medium text-gray-900 w-20 text-right">
                  {{ formatDuration(service?.p99Latency) }}
                </span>
              </div>
            </div>
          </BaseCard>

          <!-- Apdex Score -->
          <BaseCard>
            <template #header>
              <div class="flex items-center justify-between">
                <h3 class="text-lg font-semibold text-gray-900">Apdex Score</h3>
                <span class="text-xs text-gray-500">T = 500ms</span>
              </div>
            </template>
            <div class="flex items-center justify-center py-4">
              <div class="relative w-32 h-32">
                <svg class="w-full h-full transform -rotate-90">
                  <circle
                    cx="64"
                    cy="64"
                    r="56"
                    fill="none"
                    stroke="currentColor"
                    stroke-width="12"
                    class="text-gray-900"
                  />
                  <circle
                    cx="64"
                    cy="64"
                    r="56"
                    fill="none"
                    :stroke="getApdexColor(service?.apdex)"
                    stroke-width="12"
                    :stroke-dasharray="`${(service?.apdex || 0) * 352} 352`"
                    stroke-linecap="round"
                  />
                </svg>
                <div class="absolute inset-0 flex items-center justify-center">
                  <div class="text-center">
                    <div class="text-3xl font-bold text-gray-900">
                      {{ (service?.apdex || 0).toFixed(2) }}
                    </div>
                    <div :class="['text-xs font-medium', getApdexClass(service?.apdex)]">
                      {{ getApdexLabel(service?.apdex) }}
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </BaseCard>
        </div>

        <!-- Dependencies -->
        <BaseCard>
          <template #header>
            <h3 class="text-lg font-semibold text-gray-900">Dependencies</h3>
          </template>
          <div v-if="dependencies.length === 0" class="py-8 text-center text-gray-500">
            No dependencies detected
          </div>
          <div v-else class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            <div
              v-for="dep in dependencies"
              :key="dep.name"
              class="p-4 border border-gray-200 rounded-lg hover:bg-gray-50 cursor-pointer"
              @click="goToService(dep.name)"
            >
              <div class="flex items-center gap-3">
                <div :class="['w-2 h-2 rounded-full', getHealthClass(dep.health)]"></div>
                <span class="font-medium text-gray-900">{{ dep.name }}</span>
              </div>
              <div class="mt-2 grid grid-cols-2 gap-2 text-xs">
                <div>
                  <span class="text-gray-500">Rate:</span>
                  <span class="ml-1 text-gray-900">{{ formatNumber(dep.requestRate) }}/s</span>
                </div>
                <div>
                  <span class="text-gray-500">Error:</span>
                  <span :class="['ml-1', getErrorRateTextClass(dep.errorRate)]">{{ formatPercent(dep.errorRate) }}</span>
                </div>
              </div>
            </div>
          </div>
        </BaseCard>

        <!-- Top Operations -->
        <BaseCard>
          <template #header>
            <h3 class="text-lg font-semibold text-gray-900">Top Operations</h3>
          </template>
          <div class="overflow-x-auto">
            <table class="min-w-full divide-y divide-gray-200">
              <thead class="bg-gray-50">
                <tr>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                    Operation
                  </th>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                    Requests
                  </th>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                    Error Rate
                  </th>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                    P50
                  </th>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                    P99
                  </th>
                </tr>
              </thead>
              <tbody class="bg-white divide-y divide-gray-200">
                <tr
                  v-for="op in operations"
                  :key="op.name"
                  class="hover:bg-gray-50"
                >
                  <td class="px-4 py-3">
                    <span class="text-sm font-medium text-gray-900">{{ op.name }}</span>
                  </td>
                  <td class="px-4 py-3">
                    <span class="text-sm text-gray-900">{{ formatNumber(op.requestCount) }}</span>
                  </td>
                  <td class="px-4 py-3">
                    <span :class="['text-sm font-medium', getErrorRateTextClass(op.errorRate)]">
                      {{ formatPercent(op.errorRate) }}
                    </span>
                  </td>
                  <td class="px-4 py-3">
                    <span class="text-sm text-gray-900">{{ formatDuration(op.p50Latency) }}</span>
                  </td>
                  <td class="px-4 py-3">
                    <span class="text-sm text-gray-900">{{ formatDuration(op.p99Latency) }}</span>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </BaseCard>

        <!-- Recent Errors -->
        <BaseCard v-if="recentErrors.length > 0">
          <template #header>
            <div class="flex items-center justify-between">
              <h3 class="text-lg font-semibold text-gray-900">Recent Errors</h3>
              <router-link
                :to="`/p/${projectId}/errors?service=${serviceName}`"
                class="text-sm text-primary-600 hover:text-primary-700"
              >
                View All →
              </router-link>
            </div>
          </template>
          <div class="space-y-3">
            <div
              v-for="error in recentErrors"
              :key="error.id"
              class="p-3 border border-red-200 rounded-lg bg-red-50"
            >
              <div class="text-sm font-medium text-red-800">{{ error.message }}</div>
              <div class="text-xs text-red-600 mt-1">{{ error.count }} occurrences</div>
            </div>
          </div>
        </BaseCard>
      </div>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useAuth } from '@/composables/useAuth'
import AppLayout from '@/Layouts/AppLayout.vue'
import BaseCard from '@/components/BaseCard.vue'
import MiniChart from '@/components/charts/MiniChart.vue'
import axios from 'axios'

const route = useRoute()
const router = useRouter()
const { user } = useAuth()

const projectId = computed(() => route.params.id)
const serviceName = computed(() => decodeURIComponent(route.params.service_name))
const currentProject = ref(null)
const service = ref(null)
const dependencies = ref([])
const operations = ref([])
const recentErrors = ref([])
const loading = ref(false)
const timeRange = ref('1h')

// Chart data
const rateChartData = ref([])
const errorChartData = ref([])
const latencyChartData = ref([])

// API calls
const fetchProject = async () => {
  try {
    const response = await axios.get(`/api/projects/${projectId.value}`)
    currentProject.value = response.data
  } catch (error) {
    console.error('Failed to fetch project:', error)
  }
}

const fetchService = async () => {
  loading.value = true
  try {
    const response = await axios.get(`/api/projects/${projectId.value}/services/${encodeURIComponent(serviceName.value)}`, {
      params: { time_range: timeRange.value }
    })
    service.value = response.data.service
    dependencies.value = response.data.dependencies || []
    operations.value = response.data.operations || []
    recentErrors.value = response.data.recentErrors || []
    rateChartData.value = response.data.rateTimeseries || []
    errorChartData.value = response.data.errorTimeseries || []
    latencyChartData.value = response.data.latencyTimeseries || []
  } catch (error) {
    console.error('Failed to fetch service:', error)
  } finally {
    loading.value = false
  }
}

const refreshData = () => {
  fetchService()
}

const goToService = (name) => {
  router.push(`/p/${projectId.value}/services/${encodeURIComponent(name)}`)
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

const formatTrend = (trend) => {
  if (!trend) return '—'
  const sign = trend > 0 ? '+' : ''
  return `${sign}${(trend * 100).toFixed(1)}%`
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

const getTrendClass = (trend, inverse = false) => {
  if (!trend) return 'bg-gray-100 text-gray-600'
  const isGood = inverse ? trend < 0 : trend > 0
  return isGood
    ? 'bg-green-100 text-green-700'
    : 'bg-red-100 text-red-700'
}

const getErrorRateTextClass = (rate) => {
  if (!rate || rate < 0.01) return 'text-green-600'
  if (rate < 0.05) return 'text-yellow-600'
  return 'text-red-600'
}

const getLatencyWidth = (latency) => {
  const max = service.value?.p99Latency || 1000
  return Math.min((latency / max) * 100, 100)
}

const getApdexColor = (apdex) => {
  if (!apdex) return '#9CA3AF'
  if (apdex >= 0.94) return '#10B981'
  if (apdex >= 0.85) return '#3B82F6'
  if (apdex >= 0.7) return '#F59E0B'
  return '#EF4444'
}

const getApdexClass = (apdex) => {
  if (!apdex) return 'text-gray-500'
  if (apdex >= 0.94) return 'text-green-600'
  if (apdex >= 0.85) return 'text-blue-600'
  if (apdex >= 0.7) return 'text-yellow-600'
  return 'text-red-600'
}

const getApdexLabel = (apdex) => {
  if (!apdex) return 'Unknown'
  if (apdex >= 0.94) return 'Excellent'
  if (apdex >= 0.85) return 'Good'
  if (apdex >= 0.7) return 'Fair'
  if (apdex >= 0.5) return 'Poor'
  return 'Unacceptable'
}

onMounted(async () => {
  await fetchProject()
  await fetchService()
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

.time-select {
  @apply px-3 py-2 text-sm bg-white border border-gray-300 text-gray-900 rounded-md focus:ring-2 focus:ring-primary-500;
}
</style>
