<template>
  <AppLayout :user="user" :current-project="currentProject">
    <div class="traces-explorer-page" :class="{ 'filter-visible': showFilters }">
        <!-- Filter Sidebar (Collapsible) -->
        <aside
          v-if="showFilters"
          class="filter-panel border-r border-gray-200 bg-white overflow-y-auto custom-scrollbar"
        >
          <QuickFilters
            v-model="filters"
            :show-severity-filters="false"
            :show-duration-filters="true"
            :show-context-filters="true"
            :show-exception-status-filters="false"
            :show-trace-outcome-filters="true"
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

          <!-- Deep-link filter from API Monitoring (http.method + http.route) -->
          <div
            v-if="httpEndpointFilterActive"
            class="border-b border-primary-200 bg-primary-50 px-4 py-2 flex items-center justify-between gap-3 flex-wrap"
          >
            <p class="text-sm text-primary-900">
              <span class="font-medium">Filtered by endpoint:</span>
              <code class="ml-2 text-xs bg-white/80 px-1.5 py-0.5 rounded border border-primary-200">{{ httpEndpointFilterLabel }}</code>
            </p>
            <button
              type="button"
              class="text-sm font-medium text-primary-700 hover:text-primary-900 underline shrink-0"
              @click="clearHttpEndpointFilter"
            >
              Clear filter
            </button>
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
                    placeholder="Search traces..."
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

          <!-- Content Area -->
          <div class="flex-1 overflow-y-auto custom-scrollbar">
            <div class="p-4">
              <!-- Loading State -->
              <div v-if="loading && !initialLoadComplete" class="text-center py-8">
                <div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-primary-500"></div>
                <p class="mt-2 text-gray-500">Loading traces...</p>
              </div>

              <!-- Empty State -->
              <div v-else-if="filteredItems.length === 0 && !loading">
                <div class="text-center py-12">
                  <svg class="mx-auto h-12 w-12 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
                  </svg>
                  <h3 class="mt-2 text-sm font-medium text-gray-900">No traces found</h3>
                  <p class="mt-1 text-sm text-gray-500">Try adjusting your filters or time range</p>
                </div>
              </div>

              <!-- Trace Table -->
              <TracesTable
                v-else
                :items="filteredItems"
                :view="activeView"
                :loading="loading"
                :total="filteredItems.length"
                :current-page="1"
                :page-size="50"
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
import { usePageContext } from '@/composables/usePageContext'
import AppLayout from '@/Layouts/AppLayout.vue'
import QuickFilters from '@/components/QuickFilters.vue'
import TracesTable from '@/components/TracesTable.vue'
import axios from 'axios'

const route = useRoute()
const router = useRouter()
const { user } = useAuth()

const projectId = computed(() => route.params.id)
const currentProject = ref(null)

// UI State
const activeView = ref('list')
const showFilters = ref((() => {
  const stored = localStorage.getItem('showTracesFilters')
  return stored ? stored === 'true' : true
})())
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
  durationOperator: '',
  durationMin: null,
  durationMax: null,
  // Context filters (arrays for multi-select)
  environments: [],
  versions: [],
  regions: [],
  hostNames: [],
  podNames: [],
  traceOutcome: [],
  attributeFilters: [],
})

const query = ref({
  search: '',
  service: '',
})

/** Normalize vue-router query (string | string[] | undefined) to a single string */
function queryParamStr (v) {
  if (v == null) return ''
  if (Array.isArray(v)) return v[0] ? String(v[0]) : ''
  return String(v)
}

const httpEndpointFilterActive = computed(() => {
  const m = queryParamStr(route.query.http_method)
  const p = queryParamStr(route.query.http_route)
  return Boolean(m && p)
})

const httpEndpointFilterLabel = computed(() => {
  const m = queryParamStr(route.query.http_method)
  const p = queryParamStr(route.query.http_route)
  if (m && p) return `${m} ${p}`
  if (m) return m
  if (p) return p
  return ''
})

function clearHttpEndpointFilter () {
  const q = { ...route.query }
  delete q.http_method
  delete q.http_route
  router.replace({ path: route.path, query: q })
}

/** Maps sidebar filters to `trace_status` query param / API (error, ok, or omit). */
function getTraceStatusParam (f) {
  const wantsError = (f.severity && f.severity.includes('error')) ||
    (f.traceOutcome && f.traceOutcome.includes('error'))
  const wantsOk = f.traceOutcome && f.traceOutcome.includes('ok')
  if (wantsError && wantsOk) return undefined
  if (wantsError && !wantsOk) return 'error'
  if (!wantsError && wantsOk) return 'ok'
  return undefined
}

function hydrateTraceFiltersFromQuery (tsRaw) {
  const ts = String(tsRaw || '').trim()
  if (!ts) {
    filters.value.traceOutcome = []
    return
  }
  const parts = ts.split(',').map(s => s.trim()).filter(Boolean)
  const hasError = parts.includes('error')
  const hasOk = parts.includes('ok')
  if (hasError && hasOk) {
    filters.value.traceOutcome = ['error', 'ok']
  } else if (hasError) {
    filters.value.traceOutcome = ['error']
    if (!filters.value.severity.includes('error')) {
      filters.value.severity = [...filters.value.severity, 'error']
    }
  } else if (hasOk) {
    filters.value.traceOutcome = ['ok']
  } else {
    filters.value.traceOutcome = []
  }
}

const internalTraceStatusUrlUpdate = ref(false)

function syncTraceStatusToUrl () {
  const value = getTraceStatusParam(filters.value)
  const q = { ...route.query }
  const current = queryParamStr(q.trace_status)
  const next = value === undefined ? '' : value
  if (current === next) return
  if (next === '') {
    delete q.trace_status
  } else {
    q.trace_status = next
  }
  internalTraceStatusUrlUpdate.value = true
  router.replace({ path: route.path, query: q }).finally(() => {
    internalTraceStatusUrlUpdate.value = false
  })
}

// Views
const views = [
  { id: 'list', label: 'List View' },
  { id: 'table', label: 'Table View' },
  { id: 'timeline', label: 'Timeline' },
]

// Debounce timer
let debounceTimer = null

// Computed
const filteredItems = computed(() => {
  let filtered = items.value

  // Apply query filters
  if (query.value.search) {
    const searchLower = query.value.search.toLowerCase()
    filtered = filtered.filter(item =>
      item.traceId?.toLowerCase().includes(searchLower) ||
      item.serviceName?.toLowerCase().includes(searchLower) ||
      item.operationName?.toLowerCase().includes(searchLower)
    )
  }

  if (query.value.service) {
    filtered = filtered.filter(item => item.serviceName === query.value.service)
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
    const response = await axios.get(`/api/projects/${projectId.value}/traces/filter-values`)
    filterValues.value = response.data
  } catch (error) {
    console.error('Failed to fetch filter values:', error)
  }
}

const fetchAttributeKeys = async () => {
  if (!projectId.value) return
  try {
    const response = await axios.get(`/api/projects/${projectId.value}/traces/attribute-keys`)
    attributeKeys.value = response.data || []
  } catch (error) {
    console.error('Failed to fetch attribute keys:', error)
  }
}

const fetchAttributeValues = async (key) => {
  if (!projectId.value || !key) return
  try {
    const response = await axios.get(`/api/projects/${projectId.value}/traces/attribute-values`, {
      params: { key }
    })
    attributeValuesMap.value = { ...attributeValuesMap.value, [key]: response.data || [] }
  } catch (error) {
    console.error(`Failed to fetch attribute values for ${key}:`, error)
  }
}

const getTimeRange = () => {
  const now = new Date()
  const map = {
    '1h': 60 * 60 * 1000,
    '6h': 6 * 60 * 60 * 1000,
    '24h': 24 * 60 * 60 * 1000,
    '3d': 3 * 24 * 60 * 60 * 1000,
    '7d': 7 * 24 * 60 * 60 * 1000,
    '30d': 30 * 24 * 60 * 60 * 1000,
  }
  const ms = map[filters.value.timeRange] || map['24h']
  return {
    start_time: new Date(now.getTime() - ms).toISOString(),
    end_time: now.toISOString(),
  }
}

const fetchTraces = async () => {
  if (!projectId.value) return

  loading.value = true
  try {
    const { start_time, end_time } = getTimeRange()
    const params = {
      start_time,
      end_time,
      sort_by: 'start_time',
      sort_order: 'desc',
    }

    // Add service filter (support both single and multi-select)
    if (filters.value.serviceNames.length > 0) {
      params.service = filters.value.serviceNames[0] // Use first selected service
    } else if (filters.value.service) {
      params.service = filters.value.service
    } else if (query.value.service) {
      params.service = query.value.service
    }

    // Add context filters
    if (filters.value.environments.length > 0) {
      params.environment = filters.value.environments[0]
    }
    if (filters.value.versions.length > 0) {
      params.version = filters.value.versions[0]
    }

    // Server-side text search (span name substring match)
    if (query.value.search) {
      params.search = query.value.search
    }

    // HTTP endpoint filter (deep link from API Monitoring)
    const httpMethod = queryParamStr(route.query.http_method)
    const httpRoute = queryParamStr(route.query.http_route)
    if (httpMethod) params.http_method = httpMethod
    if (httpRoute) params.http_route = httpRoute

    const traceStatus = getTraceStatusParam(filters.value)
    if (traceStatus) params.trace_status = traceStatus

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

    // Use the dedicated traces endpoint
    const response = await axios.get(`/api/projects/${projectId.value}/traces`, { params })
    
    // Transform response data to match expected format
    items.value = (response.data || []).map(trace => ({
      id: trace.trace_id,
      trace_id: trace.trace_id,
      traceId: trace.trace_id,
      serviceName: trace.service_name || 'unknown',
      service_name: trace.service_name || 'unknown',
      operationName: trace.root_span_name || `${trace.span_count || 0} spans`,
      name: trace.root_span_name || `${trace.span_count || 0} spans`,
      duration: trace.duration_ns,
      duration_ns: trace.duration_ns,
      status: trace.status,
      timestamp: trace.start_time,
      start_time: trace.start_time,
      end_time: trace.end_time,
      span_count: trace.span_count,
      service_count: trace.service_count,
    }))
    initialLoadComplete.value = true

    // Extract unique services
    const services = [...new Set(items.value.map(item => item.serviceName).filter(Boolean))]
    availableServices.value = services
  } catch (error) {
    console.error('Failed to fetch traces:', error)
    items.value = []

    // If endpoint doesn't exist yet, show empty state
    if (error.response?.status === 404) {
      console.warn('Traces endpoint not implemented yet, showing empty state')
      initialLoadComplete.value = true
    }
  } finally {
    loading.value = false
  }
}

const refreshData = async () => {
  await Promise.all([fetchTraces(), fetchFilterValues()])
}

const handleFilterChange = () => {
  fetchTraces()
  syncTraceStatusToUrl()
}

const handleQueryChange = () => {
  fetchTraces()
}

const debouncedQueryChange = () => {
  if (debounceTimer) {
    clearTimeout(debounceTimer)
  }

  debounceTimer = setTimeout(() => {
    handleQueryChange()
  }, 300)
}

const handleRowClick = (item) => {
  // Navigate to trace detail
  router.push(`/p/${projectId.value}/traces/${item.trace_id || item.id}`)
}

const handleExpand = (itemId) => {
  expanded.value[itemId] = !expanded.value[itemId]
}

const handleAction = async ({ action, item }) => {
  if (action === 'copy') {
    // Copy trace ID to clipboard
    navigator.clipboard?.writeText(item.traceId)
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
  }
  // Ensure filter panel is visible
  showFilters.value = true
  // Trigger re-fetch
  fetchTraces()
}

// Watch for filter visibility changes
watch(showFilters, (value) => {
  localStorage.setItem('showTracesFilters', String(value))
})

// Re-fetch when HTTP endpoint deep-link query params change (e.g. API Monitoring → Traces)
watch(
  () => [queryParamStr(route.query.http_method), queryParamStr(route.query.http_route)],
  () => {
    fetchTraces()
  }
)

// External edits to trace_status (share link, browser back/forward): hydrate + refetch
watch(
  () => queryParamStr(route.query.trace_status),
  (newTs, oldTs) => {
    if (internalTraceStatusUrlUpdate.value) return
    if (newTs === oldTs) return
    hydrateTraceFiltersFromQuery(newTs)
    fetchTraces()
  }
)

// Page snapshot for AI agent
const { setPageSnapshot, clearPageSnapshot } = usePageContext()

watch([items, filters], () => {
  if (!items.value?.length) return
  const serviceCounts = {}
  items.value.forEach(t => {
    const svc = t.serviceName || 'unknown'
    serviceCounts[svc] = (serviceCounts[svc] || 0) + 1
  })
  setPageSnapshot({
    page: 'Traces',
    time_range: filters.value.timeRange,
    filters_active: {
      services: filters.value.serviceNames,
      outcome: filters.value.traceOutcome,
      search: filters.value.search || undefined,
    },
    trace_count: items.value.length,
    service_breakdown: serviceCounts,
  })
}, { deep: true })

// Lifecycle
onMounted(async () => {
  await fetchProject()
  hydrateTraceFiltersFromQuery(queryParamStr(route.query.trace_status))
  await Promise.all([fetchFilterValues(), fetchAttributeKeys(), fetchTraces()])
})

onUnmounted(() => clearPageSnapshot())
</script>

<style scoped>
.traces-explorer-page {
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