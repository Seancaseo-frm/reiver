<template>
  <AppLayout :user="user" :current-project="currentProject">
    <div class="logs-explorer-page" :class="{ 'filter-visible': showFilters }">
        <!-- Filter Sidebar (Collapsible) -->
        <aside
          v-if="showFilters"
          class="filter-panel border-r border-gray-200 bg-white overflow-y-auto custom-scrollbar"
        >
          <QuickFilters
            v-model="filters"
            :show-severity-filters="false"
            :show-exception-status-filters="false"
            :show-search-filter="false"
            :show-context-filters="true"
            :filter-values="filterValues"
            :attribute-keys="attributeKeys"
            :attribute-values-map="attributeValuesMap"
            @filter-change="handleFilterChange"
            @load-attribute-values="fetchAttributeValues"
            @close="showFilters = false"
          />
        </aside>

        <!-- Main Content -->
        <section class="data-section flex-1 flex flex-col overflow-hidden">
          <!-- Toolbar -->
          <div class="toolbar border-b border-gray-200 bg-white px-4 py-3 flex items-center justify-between">
            <div class="flex items-center gap-2">
              <button
                @click="showFilters = !showFilters"
                class="p-2 rounded-md text-gray-500 hover:bg-gray-100 transition-colors"
                :title="showFilters ? 'Hide Filters' : 'Show Filters'"
              >
                <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z" />
                </svg>
              </button>

              <!-- View Selector -->
              <div class="flex items-center gap-1 ml-4">
                <button
                  v-for="view in views"
                  :key="view.id"
                  @click="activeView = view.id"
                  :class="[
                    'px-3 py-1.5 rounded text-sm font-medium transition-colors',
                    activeView === view.id
                      ? 'bg-primary-600 text-white'
                      : 'text-gray-600 hover:text-gray-900 hover:bg-gray-100'
                  ]"
                >
                  {{ view.label }}
                </button>
              </div>
            </div>

            <!-- Right Actions -->
            <div class="flex items-center gap-2">
              <button
                v-if="activeView === 'list'"
                @click="toggleLiveLogs"
                :class="[
                  'px-3 py-1.5 text-sm font-medium rounded transition-colors',
                  showLiveLogs
                    ? 'bg-red-600 text-white hover:bg-red-700'
                    : 'bg-green-600 text-white hover:bg-green-700'
                ]"
              >
                {{ showLiveLogs ? 'Stop Live' : 'Go Live' }}
              </button>
              <button
                @click="refreshData"
                :disabled="loading"
                class="p-2 rounded-md text-gray-500 hover:bg-gray-100 disabled:opacity-50 transition-colors"
                title="Refresh"
              >
                <svg :class="['w-5 h-5', { 'animate-spin': loading }]" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
                </svg>
              </button>
            </div>
          </div>

          <!-- Query Section -->
          <div class="query-section border-b border-gray-200 bg-white p-4">
            <div class="space-y-4">
              <div class="flex items-center gap-4">
                <div class="flex-1">
                  <label class="block text-sm font-medium text-gray-700 mb-1">
                    Search Query
                  </label>
                  <input
                    v-model="query.search"
                    @input="debouncedQueryChange"
                    type="text"
                    placeholder="Search logs..."
                    class="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm bg-white text-gray-900 text-sm placeholder-gray-500 focus:ring-2 focus:ring-primary-500 focus:border-primary-500"
                  />
                </div>
                <div class="w-48">
                  <label class="block text-sm font-medium text-gray-700 mb-1">
                    Service
                  </label>
                  <select
                    v-model="query.service"
                    @change="handleQueryChange"
                    class="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm bg-white text-gray-900 text-sm focus:ring-2 focus:ring-primary-500 focus:border-primary-500"
                  >
                    <option value="">All services</option>
                    <option v-for="service in availableServices" :key="service" :value="service">
                      {{ service }}
                    </option>
                  </select>
                </div>
              </div>
            </div>
          </div>

          <!-- Live Logs Indicator -->
          <div v-if="showLiveLogs" class="bg-green-50 border-b border-green-200 px-4 py-2">
            <div class="flex items-center gap-2">
              <div class="w-2 h-2 bg-green-500 rounded-full animate-pulse"></div>
              <span class="text-sm text-green-700 font-medium">Live logs active</span>
              <span class="text-sm text-green-600">• New logs will appear automatically</span>
            </div>
          </div>

          <!-- Trace Correlation Indicator -->
          <div v-if="filters.trace_id" class="bg-blue-50 border-b border-blue-200 px-4 py-2">
            <div class="flex items-center justify-between">
              <div class="flex items-center gap-2">
                <svg class="w-4 h-4 text-blue-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1" />
                </svg>
                <span class="text-sm text-blue-700 font-medium">Trace-correlated logs</span>
                <span class="text-sm text-blue-600">• Showing logs linked to trace ID: <code class="bg-blue-100 px-1 rounded text-xs font-mono">{{ filters.trace_id.length > 20 ? filters.trace_id.slice(0, 20) + '...' : filters.trace_id }}</code></span>
              </div>
              <button
                @click="clearTraceFilter"
                class="text-blue-600 hover:text-blue-800 text-sm font-medium"
              >
                Clear filter
              </button>
            </div>
          </div>

          <!-- Prompt Hub Filter Indicator -->
          <div v-if="hasLlmFilters" class="bg-purple-50 border-b border-purple-200 px-4 py-2">
            <div class="flex items-center justify-between">
              <div class="flex items-center gap-2">
                <svg class="w-4 h-4 text-purple-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
                </svg>
                <span class="text-sm text-purple-700 font-medium">Prompt Hub logs</span>
                <span v-if="filters.llm_model" class="text-sm text-purple-600">
                  • Model: <code class="bg-purple-100 px-1 rounded text-xs font-mono">{{ filters.llm_model }}</code>
                </span>
                <span v-if="filters.llm_session_id" class="text-sm text-purple-600">
                  • Session: <code class="bg-purple-100 px-1 rounded text-xs font-mono">{{ filters.llm_session_id.slice(0, 8) }}...</code>
                </span>
              </div>
              <button
                @click="clearLlmFilters"
                class="text-purple-600 hover:text-purple-800 text-sm font-medium"
              >
                Clear LLM filters
              </button>
            </div>
          </div>

          <!-- Content Area -->
          <div class="flex-1 overflow-y-auto custom-scrollbar">
            <div class="p-4">
              <!-- Loading State -->
              <div v-if="loading && !initialLoadComplete" class="text-center py-8">
                <div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-primary-500"></div>
                <p class="mt-2 text-gray-500">Loading logs...</p>
              </div>

              <!-- Empty State -->
              <div v-else-if="filteredItems.length === 0 && !loading">
                <div class="text-center py-12">
                  <svg class="mx-auto h-12 w-12 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                  </svg>
                  <h3 class="mt-2 text-sm font-medium text-gray-900">No logs found</h3>
                  <p class="mt-1 text-sm text-gray-500">Try adjusting your filters or time range</p>
                </div>
              </div>

              <!-- Logs List -->
              <LogsList
                v-else
                :items="filteredItems"
                :view="activeView"
                :loading="loading"
                :live="showLiveLogs"
                :expanded-items="expanded"
                @row-click="handleRowClick"
                @expand="handleExpand"
                @action="handleAction"
                @add-filter="handleAddFilter"
              />
            </div>
          </div>
        </section>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useAuth } from '@/composables/useAuth'
import AppLayout from '@/Layouts/AppLayout.vue'
import QuickFilters from '@/components/QuickFilters.vue'
import LogsList from '@/components/LogsList.vue'
import axios from 'axios'

const route = useRoute()
const router = useRouter()
const { user } = useAuth()

const projectId = computed(() => route.params.id)
const currentProject = ref(null)

// UI State
const activeView = ref('list')
const showFilters = ref((() => {
  const stored = localStorage.getItem('showLogsFilters')
  return stored ? stored === 'true' : true
})())
const showLiveLogs = ref(false)
const loading = ref(false)
const initialLoadComplete = ref(false)
const expanded = ref({})

// Data
const items = ref([])
const availableServices = ref([])

// Available filter values from backend
const filterValues = ref({
  environments: [],
  versions: [],
  regions: [],
  host_names: [],
  pod_names: [],
  service_names: [],
})

// Attribute filter discovery
const attributeKeys = ref([])
const attributeValuesMap = ref({})

// Filters and Query
const filters = ref({
  timeRange: '24h',
  severity: [],
  status: [],
  service: '',
  serviceNames: [],
  search: '',
  startTime: null,
  endTime: null,
  customStartTime: null,
  customEndTime: null,
  trace_id: null, // For trace-correlated log filtering
  // Context filters (arrays for multi-select)
  environments: [],
  versions: [],
  regions: [],
  hostNames: [],
  podNames: [],
  attributeFilters: [],
  // Prompt Hub filters
  llm_source: null, // 'llm_gateway' to filter to Prompt Hub logs
  llm_model: null, // Filter by AI model
  llm_session_id: null, // Filter by session
  llm_prompt_id: null, // Filter by prompt
})

const query = ref({
  search: '',
  service: '',
  trace_id: null, // For trace-correlated log filtering
})

// Views
const views = [
  { id: 'list', label: 'List View' },
  { id: 'table', label: 'Table View' },
]

// Debounce timer
let debounceTimer = null
let liveLogsInterval = null

// Computed
const filteredItems = computed(() => {
  let filtered = items.value

  // Apply query filters
  if (query.value.search) {
    const searchLower = query.value.search.toLowerCase()
    filtered = filtered.filter(item =>
      item.message?.toLowerCase().includes(searchLower) ||
      item.level?.toLowerCase().includes(searchLower) ||
      item.service?.toLowerCase().includes(searchLower)
    )
  }

  if (query.value.service) {
    filtered = filtered.filter(item => item.service === query.value.service)
  }

  // Apply severity filter (client-side fallback if backend doesn't filter)
  if (filters.value.severity && filters.value.severity.length > 0) {
    filtered = filtered.filter(item => {
      const itemLevel = (item.level || item.severity || '').toLowerCase()
      return filters.value.severity.some(s => s.toLowerCase() === itemLevel)
    })
  }

  return filtered
})

// Methods
const fetchProject = async () => {
  try {
    const response = await axios.get(`/api/projects/${projectId.value}`)
    currentProject.value = response.data
  } catch (error) {
    console.error('Failed to fetch project:', error)
  }
}

const fetchFilterValues = async () => {
  if (!projectId.value) return

  try {
    const response = await axios.get(`/api/projects/${projectId.value}/events/filter-values`)
    filterValues.value = response.data
  } catch (error) {
    console.error('Failed to fetch filter values:', error)
  }
}

const fetchAttributeKeys = async () => {
  if (!projectId.value) return
  try {
    const response = await axios.get(`/api/projects/${projectId.value}/events/attribute-keys`)
    attributeKeys.value = response.data || []
  } catch (error) {
    console.error('Failed to fetch attribute keys:', error)
  }
}

const fetchAttributeValues = async (key) => {
  if (!projectId.value || !key) return
  try {
    const response = await axios.get(`/api/projects/${projectId.value}/events/attribute-values`, {
      params: { key }
    })
    attributeValuesMap.value = { ...attributeValuesMap.value, [key]: response.data || [] }
  } catch (error) {
    console.error(`Failed to fetch attribute values for ${key}:`, error)
  }
}

const fetchLogs = async () => {
  if (!projectId.value) return

  loading.value = true
  try {
    const params = {
      event_type: 'logs',
      service: query.value.service || filters.value.service,
      search: query.value.search || filters.value.search,
    }

    // Add severity filter if selected
    if (filters.value.severity && filters.value.severity.length > 0) {
      params.severity = filters.value.severity.join(',')
    }

    // Add trace_id filter for trace-correlated logs (Datadog-style correlation)
    const traceId = query.value.trace_id || filters.value.trace_id
    if (traceId) {
      params.trace_id = traceId
    }

    // Add service filter (support both single and multi-select)
    if (filters.value.serviceNames.length > 0) {
      params.service_names = filters.value.serviceNames.join(',')
    } else if (filters.value.service) {
      params.service = filters.value.service
    }

    // Add context filters (arrays)
    if (filters.value.environments.length > 0) {
      params.environments = filters.value.environments.join(',')
    }
    if (filters.value.versions.length > 0) {
      params.versions = filters.value.versions.join(',')
    }
    if (filters.value.regions.length > 0) {
      params.regions = filters.value.regions.join(',')
    }
    if (filters.value.hostNames.length > 0) {
      params.host_names = filters.value.hostNames.join(',')
    }
    if (filters.value.podNames.length > 0) {
      params.pod_names = filters.value.podNames.join(',')
    }

    // Use exact start/end times if available (from custom range or route query)
    if (filters.value.startTime && filters.value.endTime) {
      params.start_time = filters.value.startTime
      params.end_time = filters.value.endTime
    } else if (filters.value.timeRange && filters.value.timeRange !== 'custom') {
      params.time_range = filters.value.timeRange
    }

    // Dynamic attribute filters
    if (filters.value.attributeFilters) {
      for (const af of filters.value.attributeFilters) {
        if (af.key && af.values && af.values.length > 0) {
          params[`attr.${af.key}`] = af.values.join(',')
        }
      }
    }

    // Remove empty params
    Object.keys(params).forEach(key => {
      if (!params[key]) delete params[key]
    })

    const response = await axios.get(`/api/projects/${projectId.value}/events`, { params })
    items.value = response.data || []
    initialLoadComplete.value = true

    // Extract unique services
    const services = [...new Set(items.value.map(item => item.service || item.serviceName).filter(Boolean))]
    availableServices.value = services
  } catch (error) {
    console.error('Failed to fetch logs:', error)
    items.value = []

    // If endpoint doesn't exist yet, show empty state
    if (error.response?.status === 404) {
      console.warn('Events endpoint not implemented yet, showing empty state')
      initialLoadComplete.value = true
    }
  } finally {
    loading.value = false
  }
}

const refreshData = async () => {
  await Promise.all([fetchLogs(), fetchFilterValues()])
}

const handleFilterChange = () => {
  fetchLogs()
}

const handleTimeRangeChange = () => {
  // Clear custom times when switching away from custom range
  if (filters.value.timeRange !== 'custom') {
    filters.value.startTime = null
    filters.value.endTime = null
    filters.value.customStartTime = null
    filters.value.customEndTime = null
  }
  fetchLogs()
}

const handleCustomTimeChange = () => {
  if (filters.value.customStartTime && filters.value.customEndTime) {
    // Convert datetime-local format to ISO string
    const startDate = new Date(filters.value.customStartTime)
    const endDate = new Date(filters.value.customEndTime)
    
    filters.value.startTime = startDate.toISOString()
    filters.value.endTime = endDate.toISOString()
    
    fetchLogs()
  }
}

const handleQueryChange = () => {
  fetchLogs()
}

const debouncedQueryChange = () => {
  if (debounceTimer) {
    clearTimeout(debounceTimer)
  }

  debounceTimer = setTimeout(() => {
    handleQueryChange()
  }, 300)
}

const debouncedServiceChange = () => {
  // Sync service filter with query
  query.value.service = filters.value.service
  handleFilterChange()
}

const resetFilters = () => {
  filters.value = {
    timeRange: '24h',
    severity: [],
    status: [],
    service: '',
    serviceNames: [],
    search: '',
    startTime: null,
    endTime: null,
    customStartTime: null,
    customEndTime: null,
    trace_id: null,
    // Context filters
    environments: [],
    versions: [],
    regions: [],
    hostNames: [],
    podNames: [],
  }
  query.value = {
    search: '',
    service: '',
    trace_id: null,
  }
  handleFilterChange()
}

const toggleLiveLogs = () => {
  showLiveLogs.value = !showLiveLogs.value

  if (showLiveLogs.value) {
    // Start live logs polling
    liveLogsInterval = setInterval(() => {
      if (!loading.value) {
        fetchLogs()
      }
    }, 5000) // Poll every 5 seconds
  } else {
    // Stop live logs
    if (liveLogsInterval) {
      clearInterval(liveLogsInterval)
      liveLogsInterval = null
    }
  }
}

const clearTraceFilter = () => {
  // Clear trace_id filter and update URL
  filters.value.trace_id = null
  query.value.trace_id = null
  
  // Remove trace_id from URL query params
  const newQuery = { ...route.query }
  delete newQuery.trace_id
  router.replace({ query: newQuery })
  
  // Refresh logs without trace filter
  fetchLogs()
}

// Check if any LLM-specific filters are active
const hasLlmFilters = computed(() => {
  return filters.value.llm_source || filters.value.llm_model || 
         filters.value.llm_session_id || filters.value.llm_prompt_id
})

const clearLlmFilters = () => {
  // Clear all LLM-related filters
  filters.value.llm_source = null
  filters.value.llm_model = null
  filters.value.llm_session_id = null
  filters.value.llm_prompt_id = null
  
  // Remove LLM params from URL query
  const newQuery = { ...route.query }
  delete newQuery.source
  delete newQuery.llm_model
  delete newQuery.llm_session_id
  delete newQuery.llm_prompt_id
  router.replace({ query: newQuery })
  
  // Refresh logs
  fetchLogs()
}

const handleRowClick = (item) => {
  const query = item.timestamp ? { timestamp: item.timestamp } : {}
  router.push({ path: `/p/${projectId.value}/logs/${item.id}`, query })
}

const handleExpand = (itemId) => {
  expanded.value[itemId] = !expanded.value[itemId]
}

const handleAction = async ({ action, item }) => {
  if (action === 'copy') {
    // Copy log message to clipboard
    navigator.clipboard?.writeText(item.message || item.body)
  }
}

const handleAddFilter = ({ field, value }) => {
  if (field === 'service') {
    // If we have available service names, add to array, otherwise use single value
    if (filterValues.value.service_names.length > 0) {
      if (!filters.value.serviceNames.includes(value)) {
        filters.value.serviceNames.push(value)
      }
    } else {
      query.value.service = value
    }
  } else if (field === 'environment') {
    if (!filters.value.environments.includes(value)) {
      filters.value.environments.push(value)
    }
  } else if (field === 'version') {
    if (!filters.value.versions.includes(value)) {
      filters.value.versions.push(value)
    }
  } else if (field === 'region') {
    if (!filters.value.regions.includes(value)) {
      filters.value.regions.push(value)
    }
  } else if (field === 'host_name') {
    if (!filters.value.hostNames.includes(value)) {
      filters.value.hostNames.push(value)
    }
  } else if (field === 'pod_name') {
    if (!filters.value.podNames.includes(value)) {
      filters.value.podNames.push(value)
    }
  } else if (field === 'severity') {
    if (!filters.value.severity.includes(value)) {
      filters.value.severity.push(value)
    }
  }
  // Ensure filter panel is visible
  showFilters.value = true
  // Trigger re-fetch
  fetchLogs()
}

// Watch for filter visibility changes
watch(showFilters, (value) => {
  localStorage.setItem('showLogsFilters', String(value))
})

// Watch for route query changes (when navigating from error page or trace page)
watch(() => route.query, (newQuery) => {
  if (newQuery.service || newQuery.search || newQuery.startTime || newQuery.endTime || newQuery.trace_id) {
    initializeFromRoute()
    fetchLogs()
  }
}, { immediate: false })

// Cleanup on unmount
onUnmounted(() => {
  if (liveLogsInterval) {
    clearInterval(liveLogsInterval)
  }
  if (debounceTimer) {
    clearTimeout(debounceTimer)
  }
})

// Initialize filters and query from route query parameters
const initializeFromRoute = () => {
  const routeQuery = route.query
  
  // Set service from route query (set in both query and filters)
  if (routeQuery.service) {
    query.value.service = routeQuery.service
    filters.value.service = routeQuery.service
  }
  
  // Set search from route query (set in both query and filters)
  if (routeQuery.search) {
    query.value.search = routeQuery.search
    filters.value.search = routeQuery.search
  }
  
  // Set trace_id for trace-correlated log filtering (Datadog-style correlation)
  if (routeQuery.trace_id) {
    query.value.trace_id = routeQuery.trace_id
    filters.value.trace_id = routeQuery.trace_id
  } else {
    query.value.trace_id = null
    filters.value.trace_id = null
  }
  
  // Set time range if startTime/endTime are provided
  if (routeQuery.startTime && routeQuery.endTime) {
    try {
      const startTime = new Date(routeQuery.startTime)
      const endTime = new Date(routeQuery.endTime)
      
      // Store the exact start/end times for API call
      filters.value.startTime = routeQuery.startTime
      filters.value.endTime = routeQuery.endTime
      
      // Set timeRange to 'custom' to indicate we're using custom times
      filters.value.timeRange = 'custom'
      
      // Format dates for display
      filters.value.customStartTime = formatDateTimeLocal(startTime)
      filters.value.customEndTime = formatDateTimeLocal(endTime)
    } catch (e) {
      console.warn('Failed to parse time range from route query:', e)
    }
  }
}

// Format date for datetime-local input
const formatDateTimeLocal = (date) => {
  const d = new Date(date)
  const year = d.getFullYear()
  const month = String(d.getMonth() + 1).padStart(2, '0')
  const day = String(d.getDate()).padStart(2, '0')
  const hours = String(d.getHours()).padStart(2, '0')
  const minutes = String(d.getMinutes()).padStart(2, '0')
  return `${year}-${month}-${day}T${hours}:${minutes}`
}

// Lifecycle
onMounted(async () => {
  await fetchProject()
  initializeFromRoute()
  // Fetch filter values and logs in parallel
  await Promise.all([fetchFilterValues(), fetchAttributeKeys(), fetchLogs()])
  // Re-apply route query params after services are loaded
  if (route.query.service) {
    query.value.service = route.query.service
  }
})
</script>

<style scoped>
.logs-explorer-page {
  @apply h-full flex;
}

.filter-panel {
  @apply flex-shrink-0 w-64 transition-all duration-300;
}

.data-section {
  @apply flex-1;
}

.toolbar {
  @apply flex items-center justify-between;
}

.query-section {
  @apply bg-gray-50;
}

.custom-scrollbar {
  scrollbar-width: thin;
  scrollbar-color: rgba(155, 155, 155, 0.5) transparent;
}

.custom-scrollbar::-webkit-scrollbar {
  width: 6px;
  height: 6px;
}

.custom-scrollbar::-webkit-scrollbar-track {
  background: transparent;
}

.custom-scrollbar::-webkit-scrollbar-thumb {
  background-color: rgba(155, 155, 155, 0.5);
  border-radius: 3px;
}

.custom-scrollbar::-webkit-scrollbar-thumb:hover {
  background-color: rgba(155, 155, 155, 0.7);
}
</style>