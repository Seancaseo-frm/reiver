<template>
  <AppLayout :user="user" :current-project="currentProject">
    <div class="metrics-page max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <!-- Header -->
      <div class="mb-6">
        <div class="flex items-center justify-between">
          <div>
            <h1 class="text-2xl font-bold text-gray-900">Metrics Explorer</h1>
            <p class="text-sm text-gray-500 mt-1">
              Explore and analyze your application metrics
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

      <!-- Search and Filters -->
      <div class="mb-6">
        <div class="flex items-center gap-4">
          <div class="flex-1 relative">
            <svg class="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
            </svg>
            <input
              v-model="searchQuery"
              @input="handleSearch"
              type="text"
              placeholder="Search metrics by name..."
              class="search-input"
            />
          </div>
          <select v-model="metricType" @change="refreshData" class="filter-select">
            <option value="">All Types</option>
            <option value="counter">Counter</option>
            <option value="gauge">Gauge</option>
            <option value="histogram">Histogram</option>
            <option value="summary">Summary</option>
          </select>
          <div class="flex items-center bg-gray-100 rounded-lg p-1">
            <button
              @click="viewMode = 'list'"
              :class="['view-btn', viewMode === 'list' ? 'active' : '']"
            >
              <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 10h16M4 14h16M4 18h16" />
              </svg>
            </button>
            <button
              @click="viewMode = 'treemap'"
              :class="['view-btn', viewMode === 'treemap' ? 'active' : '']"
            >
              <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 5a1 1 0 011-1h14a1 1 0 011 1v2a1 1 0 01-1 1H5a1 1 0 01-1-1V5zM4 13a1 1 0 011-1h6a1 1 0 011 1v6a1 1 0 01-1 1H5a1 1 0 01-1-1v-6zM16 13a1 1 0 011-1h2a1 1 0 011 1v6a1 1 0 01-1 1h-2a1 1 0 01-1-1v-6z" />
              </svg>
            </button>
          </div>
        </div>
      </div>

      <!-- Loading State -->
      <div v-if="loading" class="flex items-center justify-center py-12">
        <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full"></div>
        <span class="ml-3 text-gray-600">Loading metrics...</span>
      </div>

      <!-- Summary Stats -->
      <div v-else class="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
        <BaseCard>
          <div class="text-sm font-medium text-gray-500">Total Metrics</div>
          <div class="mt-1 text-2xl font-bold text-gray-900">
            {{ metrics.length }}
          </div>
        </BaseCard>
        <BaseCard>
          <div class="text-sm font-medium text-gray-500">Total Series</div>
          <div class="mt-1 text-2xl font-bold text-gray-900">
            {{ formatNumber(totalSeries) }}
          </div>
        </BaseCard>
        <BaseCard>
          <div class="text-sm font-medium text-gray-500">Data Points/min</div>
          <div class="mt-1 text-2xl font-bold text-gray-900">
            {{ formatNumber(dataPointsPerMin) }}
          </div>
        </BaseCard>
        <BaseCard>
          <div class="text-sm font-medium text-gray-500">Top Cardinality</div>
          <div class="mt-1 text-2xl font-bold text-gray-900">
            {{ formatNumber(topCardinality) }}
          </div>
        </BaseCard>
      </div>

      <!-- Treemap View -->
      <div v-if="viewMode === 'treemap' && !loading" class="mb-6">
        <BaseCard>
          <template #header>
            <div class="flex items-center justify-between">
              <h2 class="text-lg font-semibold text-gray-900">Metric Cardinality</h2>
              <span class="text-sm text-gray-500">Sized by series count</span>
            </div>
          </template>
          <MetricsTreemap
            :metrics="filteredMetrics"
            @select-metric="handleMetricSelect"
          />
        </BaseCard>
      </div>

      <!-- List View -->
      <div v-if="viewMode === 'list' && !loading">
        <BaseCard>
          <template #header>
            <div class="flex items-center justify-between">
              <h2 class="text-lg font-semibold text-gray-900">All Metrics</h2>
              <span class="text-sm text-gray-500">
                {{ filteredMetrics.length }} metrics
              </span>
            </div>
          </template>
          <div class="overflow-x-auto">
            <table class="min-w-full divide-y divide-gray-200">
              <thead class="bg-gray-50">
                <tr>
                  <th 
                    @click="toggleSort('name')"
                    class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase cursor-pointer hover:bg-gray-100"
                  >
                    <div class="flex items-center gap-1">
                      Metric Name
                      <SortIcon :active="sortBy === 'name'" :direction="sortDirection" />
                    </div>
                  </th>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                    Type
                  </th>
                  <th 
                    @click="toggleSort('series_count')"
                    class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase cursor-pointer hover:bg-gray-100"
                  >
                    <div class="flex items-center gap-1">
                      Series
                      <SortIcon :active="sortBy === 'series_count'" :direction="sortDirection" />
                    </div>
                  </th>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                    Labels
                  </th>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                    Unit
                  </th>
                  <th 
                    @click="toggleSort('last_seen')"
                    class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase cursor-pointer hover:bg-gray-100"
                  >
                    <div class="flex items-center gap-1">
                      Last Seen
                      <SortIcon :active="sortBy === 'last_seen'" :direction="sortDirection" />
                    </div>
                  </th>
                </tr>
              </thead>
              <tbody class="bg-white divide-y divide-gray-200">
                <tr
                  v-for="metric in sortedMetrics"
                  :key="metric.name"
                  class="hover:bg-gray-50 cursor-pointer"
                  @click="handleMetricSelect(metric)"
                >
                  <td class="px-4 py-4">
                    <div class="flex items-center gap-2">
                      <span :class="['w-2 h-2 rounded-full', getTypeColor(metric.metric_type)]"></span>
                      <span class="text-sm font-medium text-gray-900 font-mono">
                        {{ metric.name }}
                      </span>
                    </div>
                    <div v-if="metric.description" class="text-xs text-gray-500 mt-1 truncate max-w-md">
                      {{ metric.description }}
                    </div>
                  </td>
                  <td class="px-4 py-4">
                    <span :class="['px-2 py-1 text-xs font-medium rounded', getTypeBadgeClass(metric.metric_type)]">
                      {{ metric.metric_type }}
                    </span>
                  </td>
                  <td class="px-4 py-4">
                    <span class="text-sm text-gray-900">
                      {{ formatNumber(metric.series_count) }}
                    </span>
                  </td>
                  <td class="px-4 py-4">
                    <div class="flex flex-wrap gap-1 max-w-xs">
                      <span
                        v-for="label in metric.label_keys?.slice(0, 3)"
                        :key="label"
                        class="px-1.5 py-0.5 text-xs bg-gray-100 text-gray-700 rounded"
                      >
                        {{ label }}
                      </span>
                      <span
                        v-if="metric.label_keys?.length > 3"
                        class="text-xs text-gray-500"
                      >
                        +{{ metric.label_keys.length - 3 }} more
                      </span>
                    </div>
                  </td>
                  <td class="px-4 py-4">
                    <span class="text-sm text-gray-600">
                      {{ metric.unit || '—' }}
                    </span>
                  </td>
                  <td class="px-4 py-4">
                    <span class="text-sm text-gray-600">
                      {{ formatRelativeTime(metric.last_seen) }}
                    </span>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </BaseCard>
      </div>

      <!-- Empty State -->
      <div v-if="!loading && metrics.length === 0" class="text-center py-12">
        <svg class="mx-auto h-12 w-12 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
        </svg>
        <h3 class="mt-2 text-sm font-medium text-gray-900">No metrics found</h3>
        <p class="mt-1 text-sm text-gray-500">
          Start sending metrics to see them here.
        </p>
      </div>

      <!-- Metric Detail Drawer -->
      <MetricDetailDrawer
        :is-open="showMetricDrawer"
        :metric="selectedMetric"
        :project-id="projectId"
        :time-range="timeRange"
        @close="showMetricDrawer = false"
      />
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
import MetricsTreemap from '@/components/MetricsTreemap.vue'
import MetricDetailDrawer from '@/components/MetricDetailDrawer.vue'
import SortIcon from '@/components/SortIcon.vue'
import axios from 'axios'

const route = useRoute()
const router = useRouter()
const { user } = useAuth()

const projectId = computed(() => route.params.id)
const currentProject = ref(null)
const metrics = ref([])
const loading = ref(false)
const timeRange = ref('1h')
const searchQuery = ref('')
const metricType = ref('')
const viewMode = ref('list')
const sortBy = ref('series_count')
const sortDirection = ref('desc')
const selectedMetric = ref(null)
const showMetricDrawer = ref(false)

// Computed
const filteredMetrics = computed(() => {
  let result = metrics.value
  
  if (searchQuery.value) {
    const query = searchQuery.value.toLowerCase()
    result = result.filter(m => 
      m.name.toLowerCase().includes(query) ||
      m.description?.toLowerCase().includes(query)
    )
  }
  
  if (metricType.value) {
    result = result.filter(m => m.metric_type === metricType.value)
  }
  
  return result
})

const sortedMetrics = computed(() => {
  const sorted = [...filteredMetrics.value]
  
  sorted.sort((a, b) => {
    let aVal = a[sortBy.value]
    let bVal = b[sortBy.value]
    
    if (sortBy.value === 'last_seen') {
      aVal = new Date(aVal).getTime()
      bVal = new Date(bVal).getTime()
    }
    
    if (sortDirection.value === 'asc') {
      return aVal > bVal ? 1 : -1
    }
    return aVal < bVal ? 1 : -1
  })
  
  return sorted
})

const totalSeries = computed(() => {
  return metrics.value.reduce((sum, m) => sum + (m.series_count || 0), 0)
})

const dataPointsPerMin = computed(() => {
  return metrics.value.reduce((sum, m) => sum + (m.data_points_per_min || 0), 0)
})

const topCardinality = computed(() => {
  if (metrics.value.length === 0) return 0
  return Math.max(...metrics.value.map(m => m.series_count || 0))
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

const fetchMetrics = async () => {
  loading.value = true
  try {
    const response = await axios.get(`/api/projects/${projectId.value}/metrics/names`, {
      params: { time_range: timeRange.value, limit: 500 }
    })
    metrics.value = response.data.metrics || []
  } catch (error) {
    console.error('Failed to fetch metrics:', error)
    metrics.value = []
  } finally {
    loading.value = false
  }
}

const refreshData = () => {
  fetchMetrics()
}

const handleSearch = () => {
  // Debounced search is handled by computed
}

const toggleSort = (field) => {
  if (sortBy.value === field) {
    sortDirection.value = sortDirection.value === 'asc' ? 'desc' : 'asc'
  } else {
    sortBy.value = field
    sortDirection.value = 'desc'
  }
}

const handleMetricSelect = (metric) => {
  selectedMetric.value = metric
  showMetricDrawer.value = true
}

// Formatting
const formatNumber = (num) => {
  if (num === undefined || num === null) return '0'
  if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`
  if (num >= 1000) return `${(num / 1000).toFixed(1)}K`
  return num.toString()
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
const getTypeColor = (type) => {
  const colors = {
    counter: 'bg-blue-500',
    gauge: 'bg-green-500',
    histogram: 'bg-purple-500',
    summary: 'bg-yellow-500',
  }
  return colors[type] || 'bg-gray-400'
}

const getTypeBadgeClass = (type) => {
  const classes = {
    counter: 'bg-blue-100 text-blue-800',
    gauge: 'bg-green-100 text-green-800',
    histogram: 'bg-purple-100 text-purple-800',
    summary: 'bg-yellow-100 text-yellow-800',
  }
  return classes[type] || 'bg-gray-100 text-gray-800'
}

onMounted(async () => {
  await fetchProject()
  await fetchMetrics()
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

.search-input {
  @apply w-full pl-10 pr-4 py-2 bg-white border border-gray-300 text-gray-900 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-primary-500;
}

.time-select,
.filter-select {
  @apply px-3 py-2 text-sm bg-white border border-gray-300 text-gray-900 rounded-md focus:ring-2 focus:ring-primary-500;
}

.view-btn {
  @apply p-2 text-gray-600 rounded-md transition-colors;
}

.view-btn:hover {
  @apply text-gray-900;
}

.view-btn.active {
  @apply bg-white text-gray-900 shadow-sm;
}
</style>
