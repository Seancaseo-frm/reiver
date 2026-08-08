<template>
  <AppLayout :user="user" :current-project="currentProject">
    <div class="profiles-page">
      <!-- Quick Filters Sidebar -->
      <aside
        v-if="showFiltersPanel"
        class="w-64 flex-shrink-0 border-r border-gray-200 bg-white overflow-y-auto custom-scrollbar"
      >
        <QuickFilters
          v-model="quickFilterModel"
          :show-severity-filters="false"
          :show-search-filter="false"
          :show-duration-filters="false"
          :show-context-filters="true"
          :show-exception-status-filters="false"
          :show-trace-outcome-filters="false"
          :filter-values="filterValues"
          :attribute-keys="attributeKeys"
          :attribute-values-map="attributeValuesMap"
          @filter-change="handleQuickFilterChange"
          @load-attribute-values="loadAttributeValues"
        />
      </aside>

      <section class="data-section flex-1 flex flex-col overflow-hidden">
        <!-- Toolbar -->
        <div class="toolbar border-b border-gray-200 bg-white px-4 py-3 flex items-center justify-between">
          <div class="flex items-center gap-2">
            <button
              @click="showFiltersPanel = !showFiltersPanel"
              :class="['p-2 rounded-md transition-colors', showFiltersPanel ? 'bg-primary-50 text-primary-600' : 'text-gray-500 hover:bg-gray-100']"
              title="Toggle filters"
            >
              <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z" />
              </svg>
            </button>
            <h1 class="text-lg font-semibold text-gray-900">Profiles</h1>
            <!-- Tab Switcher -->
            <div class="ml-4 flex items-center bg-gray-100 rounded-lg p-0.5">
              <button
                @click="activeTab = 'list'"
                :class="['px-3 py-1 text-sm font-medium rounded-md transition-colors', activeTab === 'list' ? 'bg-white text-gray-900 shadow-sm' : 'text-gray-500 hover:text-gray-700']"
              >
                List
              </button>
              <button
                @click="activeTab = 'top-functions'"
                :class="['px-3 py-1 text-sm font-medium rounded-md transition-colors', activeTab === 'top-functions' ? 'bg-white text-gray-900 shadow-sm' : 'text-gray-500 hover:text-gray-700']"
              >
                Top Functions
              </button>
            </div>
          </div>

          <!-- Right Actions -->
          <div class="flex items-center gap-2">
            <button
              v-if="filters.service"
              @click="openAggregateView"
              :disabled="aggregateLoading"
              class="inline-flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium rounded-md border border-gray-300 text-gray-700 hover:bg-gray-50 disabled:opacity-50 transition-colors"
            >
              <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 12h16M4 18h7" />
              </svg>
              Aggregate
            </button>
            <router-link
              v-if="filters.service"
              :to="`/p/${projectId}/profiles/compare?service=${encodeURIComponent(filters.service)}`"
              class="inline-flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium rounded-md border border-gray-300 text-gray-700 hover:bg-gray-50 transition-colors"
            >
              <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
              </svg>
              Compare Versions
            </router-link>
            <button
              @click="fetchProfiles"
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

        <!-- Filters -->
        <div class="border-b border-gray-200 bg-white p-4">
          <div class="flex items-center gap-4 flex-wrap">
            <div class="w-48">
              <label class="block text-sm font-medium text-gray-700 mb-1">Service</label>
              <select
                v-model="filters.service"
                @change="handleFilterChange"
                class="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm bg-white text-gray-900 text-sm focus:ring-2 focus:ring-primary-500 focus:border-primary-500"
              >
                <option value="">All services</option>
                <option v-for="s in availableServices" :key="s" :value="s">{{ s }}</option>
              </select>
            </div>

            <div class="w-36">
              <label class="block text-sm font-medium text-gray-700 mb-1">Profile Type</label>
              <select
                v-model="filters.profileType"
                @change="handleFilterChange"
                class="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm bg-white text-gray-900 text-sm focus:ring-2 focus:ring-primary-500 focus:border-primary-500"
              >
                <option value="">All types</option>
                <option v-for="pt in availableProfileTypes" :key="pt" :value="pt">{{ pt }}</option>
              </select>
            </div>

            <div class="w-48">
              <label class="block text-sm font-medium text-gray-700 mb-1">Time Range</label>
              <select
                v-model="filters.timeRange"
                @change="handleFilterChange"
                class="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm bg-white text-gray-900 text-sm focus:ring-2 focus:ring-primary-500 focus:border-primary-500"
              >
                <option value="1h">Last 1 hour</option>
                <option value="6h">Last 6 hours</option>
                <option value="24h">Last 24 hours</option>
                <option value="3d">Last 3 days</option>
                <option value="7d">Last 7 days</option>
              </select>
            </div>

            <div class="flex-1 min-w-[200px]">
              <label class="block text-sm font-medium text-gray-700 mb-1">Search by Trace ID</label>
              <input
                v-model="filters.traceId"
                @input="debouncedFilterChange"
                type="text"
                placeholder="Filter by trace ID..."
                class="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm bg-white text-gray-900 text-sm placeholder-gray-500 focus:ring-2 focus:ring-primary-500 focus:border-primary-500"
              />
            </div>
          </div>
        </div>

        <!-- Content Area -->
        <div class="flex-1 overflow-y-auto custom-scrollbar">
          <!-- List Tab -->
          <div v-if="activeTab === 'list'" class="p-4">
            <div v-if="loading && profiles.length === 0" class="text-center py-8">
              <div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-primary-500"></div>
              <p class="mt-2 text-gray-500">Loading profiles...</p>
            </div>

            <div v-else-if="profiles.length === 0 && !loading" class="text-center py-12">
              <svg class="mx-auto h-12 w-12 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17.657 18.657A8 8 0 016.343 7.343S7 9 9 10c0-2 .5-5 2.986-7C14 5 16.09 5.777 17.656 7.343A7.975 7.975 0 0120 13a7.975 7.975 0 01-2.343 5.657z" />
              </svg>
              <h3 class="mt-2 text-sm font-medium text-gray-900">No profiles found</h3>
              <p class="mt-1 text-sm text-gray-500">
                Profiles will appear here once continuous profiling is enabled for your services.
              </p>
            </div>

            <div v-else>
              <table class="min-w-full divide-y divide-gray-200">
                <thead class="bg-gray-50">
                  <tr>
                    <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Service</th>
                    <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Profile Type</th>
                    <th class="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">Samples</th>
                    <th class="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">Duration</th>
                    <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Attributes</th>
                    <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Trace</th>
                    <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Timestamp</th>
                  </tr>
                </thead>
                <tbody class="bg-white divide-y divide-gray-200">
                  <tr
                    v-for="profile in profiles"
                    :key="profile.profile_id"
                    @click="viewProfile(profile)"
                    class="hover:bg-gray-50 cursor-pointer transition-colors"
                  >
                    <td class="px-4 py-3 whitespace-nowrap">
                      <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
                        {{ profile.service_name || 'unknown' }}
                      </span>
                    </td>
                    <td class="px-4 py-3 whitespace-nowrap text-sm text-gray-700">
                      {{ profile.period_type || 'cpu' }}
                    </td>
                    <td class="px-4 py-3 whitespace-nowrap text-sm text-gray-700 text-right font-mono">
                      {{ formatNumber(profile.sample_count) }}
                    </td>
                    <td class="px-4 py-3 whitespace-nowrap text-sm text-gray-700 text-right font-mono">
                      {{ formatDuration(profile.duration_nano) }}
                    </td>
                    <td class="px-4 py-3 text-sm">
                      <div class="flex flex-wrap gap-1 max-w-xs">
                        <span
                          v-for="(val, key) in truncatedAttributes(profile.attributes)"
                          :key="key"
                          class="inline-flex items-center px-1.5 py-0.5 rounded text-xs bg-gray-100 text-gray-600 font-mono truncate max-w-[180px]"
                          :title="`${key}: ${val}`"
                        >
                          {{ key }}={{ val }}
                        </span>
                        <span v-if="!profile.attributes || Object.keys(profile.attributes).length === 0" class="text-gray-400">--</span>
                      </div>
                    </td>
                    <td class="px-4 py-3 whitespace-nowrap text-sm">
                      <router-link
                        v-if="profile.trace_id"
                        :to="`/p/${projectId}/traces/${profile.trace_id}`"
                        @click.stop
                        class="text-primary-600 hover:underline font-mono text-xs"
                      >
                        {{ profile.trace_id.substring(0, 12) }}...
                      </router-link>
                      <span v-else class="text-gray-400">--</span>
                    </td>
                    <td class="px-4 py-3 whitespace-nowrap text-sm text-gray-500">
                      {{ formatTimestamp(profile.timestamp) }}
                    </td>
                  </tr>
                </tbody>
              </table>

              <div v-if="total > filters.limit" class="mt-4 flex items-center justify-between px-4">
                <p class="text-sm text-gray-500">
                  Showing {{ filters.offset + 1 }}-{{ Math.min(filters.offset + filters.limit, total) }} of {{ total }} profiles
                </p>
                <div class="flex items-center gap-2">
                  <button
                    @click="prevPage"
                    :disabled="filters.offset === 0"
                    class="px-3 py-1.5 text-sm font-medium rounded-md border border-gray-300 text-gray-700 hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                  >
                    Previous
                  </button>
                  <button
                    @click="nextPage"
                    :disabled="filters.offset + filters.limit >= total"
                    class="px-3 py-1.5 text-sm font-medium rounded-md border border-gray-300 text-gray-700 hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                  >
                    Next
                  </button>
                </div>
              </div>
            </div>
          </div>

          <!-- Top Functions Tab -->
          <div v-if="activeTab === 'top-functions'" class="p-4">
            <div v-if="topFunctionsLoading" class="text-center py-8">
              <div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-primary-500"></div>
              <p class="mt-2 text-gray-500">Loading top functions...</p>
            </div>
            <div v-else-if="topFunctions.length === 0" class="text-center py-12 text-gray-500">
              <p class="text-sm">No function data available for this service and time range.</p>
            </div>
            <div v-else>
              <div class="flex items-center justify-between mb-3">
                <label class="inline-flex items-center gap-2 text-sm text-gray-600 cursor-pointer select-none">
                  <input type="checkbox" v-model="hideRuntimeFunctions" class="rounded border-gray-300 text-primary-600 focus:ring-primary-500" />
                  Hide runtime / stdlib
                </label>
                <span v-if="hideRuntimeFunctions && topFunctions.length !== filteredTopFunctions.length" class="text-xs text-gray-400">
                  Showing {{ filteredTopFunctions.length }} of {{ topFunctions.length }} functions
                </span>
              </div>
              <table class="min-w-full divide-y divide-gray-200 mb-6">
                <thead class="bg-gray-50">
                  <tr>
                    <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">#</th>
                    <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Function</th>
                    <th class="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">Total Samples</th>
                    <th class="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">% of Total</th>
                  </tr>
                </thead>
                <tbody class="bg-white divide-y divide-gray-200">
                  <tr
                    v-for="(fn, i) in filteredTopFunctions"
                    :key="fn.function_name"
                    :class="['transition-colors', selectedFunction === fn.function_name ? 'bg-primary-50' : 'hover:bg-gray-50']"
                    @click="selectFunction(fn.function_name)"
                    class="cursor-pointer"
                  >
                    <td class="px-4 py-3 text-sm text-gray-400 font-mono w-10">{{ i + 1 }}</td>
                    <td class="px-4 py-3 text-sm font-mono text-gray-900 break-all">{{ fn.function_name }}</td>
                    <td class="px-4 py-3 text-sm text-gray-700 text-right font-mono">{{ formatNumber(fn.total_samples) }}</td>
                    <td class="px-4 py-3 text-sm text-gray-700 text-right font-mono">
                      {{ totalSamples > 0 ? ((fn.total_samples / totalSamples) * 100).toFixed(1) + '%' : '--' }}
                    </td>
                  </tr>
                </tbody>
              </table>

              <!-- Time-series chart for selected function -->
              <div v-if="selectedFunction && timeseriesData[selectedFunction]" class="border border-gray-200 rounded-lg p-4">
                <h3 class="text-sm font-medium text-gray-700 mb-3">
                  Samples over time: <span class="font-mono text-gray-900">{{ selectedFunction }}</span>
                </h3>
                <div class="h-48 flex items-end gap-px">
                  <div
                    v-for="(point, i) in timeseriesData[selectedFunction]"
                    :key="i"
                    class="flex-1 bg-primary-400 hover:bg-primary-500 transition-colors rounded-t"
                    :style="{ height: getBarHeight(point.samples) }"
                    :title="`${formatTimestamp(point.timestamp)}: ${formatNumber(point.samples)} samples`"
                  ></div>
                </div>
                <div class="flex justify-between mt-1 text-xs text-gray-400">
                  <span v-if="timeseriesData[selectedFunction].length">{{ formatTimestamp(timeseriesData[selectedFunction][0].timestamp) }}</span>
                  <span v-if="timeseriesData[selectedFunction].length > 1">{{ formatTimestamp(timeseriesData[selectedFunction][timeseriesData[selectedFunction].length - 1].timestamp) }}</span>
                </div>
              </div>
            </div>
          </div>
        </div>

        <!-- Aggregate Modal -->
        <div v-if="showAggregateModal" class="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4" @click.self="showAggregateModal = false">
          <div class="bg-white rounded-xl shadow-2xl w-full max-w-6xl max-h-[90vh] flex flex-col">
            <div class="flex items-center justify-between px-6 py-4 border-b border-gray-200">
              <h2 class="text-lg font-semibold text-gray-900">Aggregated Profile — {{ filters.service }}</h2>
              <button @click="showAggregateModal = false" class="p-1 text-gray-400 hover:text-gray-600">
                <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>
            <div class="flex-1 overflow-auto">
              <div v-if="aggregateLoading" class="text-center py-12">
                <div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-primary-500"></div>
                <p class="mt-2 text-gray-500">Building aggregated flamegraph...</p>
              </div>
              <ProfileFlamegraph v-else-if="aggregateFlameGraph" :flameGraph="aggregateFlameGraph" />
              <div v-else class="text-center py-12 text-gray-500">
                <p>Failed to build aggregated flamegraph.</p>
              </div>
            </div>
          </div>
        </div>
      </section>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, reactive, computed, onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useAuth } from '@/composables/useAuth'
import AppLayout from '@/Layouts/AppLayout.vue'
import ProfileFlamegraph from '@/components/ProfileFlamegraph.vue'
import QuickFilters from '@/components/QuickFilters.vue'
import axios from 'axios'

const route = useRoute()
const router = useRouter()
const { user } = useAuth()

const projectId = computed(() => route.params.id)
const currentProject = ref(null)

const activeTab = ref('list')
const loading = ref(false)
const profiles = ref([])
const total = ref(0)
const availableServices = ref([])
const availableProfileTypes = ref([])

// Aggregate state
const showAggregateModal = ref(false)
const aggregateLoading = ref(false)
const aggregateFlameGraph = ref(null)

// Top functions state
const topFunctionsLoading = ref(false)
const topFunctions = ref([])
const timeseriesData = ref({})
const selectedFunction = ref(null)
const hideRuntimeFunctions = ref(true)

const RUNTIME_PATTERNS = [
  /^tokio::/,
  /^std::/,
  /^core::/,
  /^alloc::/,
  /^mio::/,
  /^hyper::/,
  /^h2::/,
  /^__rust_begin_short_backtrace$/,
  /^__libc_start_main$/,
  /^start_thread$/,
  /^clone[3]?$/,
  /^pthread_/,
  /^___pthread_/,
  /^_start$/,
  /^runtime\./,
  /^net\/http\./,
  /^syscall\./,
  /^internal\/poll\./,
  /^runtime\.goexit/,
  /^java\.lang\.Thread\.run$/,
  /^java\.util\.concurrent\./,
]
const isRuntimeFunction = (name) => {
  const normalized = name.replace(/^<+/, '')
  return RUNTIME_PATTERNS.some(p => p.test(normalized))
}

const filteredTopFunctions = computed(() => {
  if (!hideRuntimeFunctions.value) return topFunctions.value
  return topFunctions.value.filter(f => !isRuntimeFunction(f.function_name))
})
const totalSamples = computed(() => filteredTopFunctions.value.reduce((sum, f) => sum + f.total_samples, 0))

// Quick filters sidebar
const showFiltersPanel = ref(false)
const attributeKeys = ref([])
const attributeValuesMap = reactive({})
const filterValues = ref({ environments: [], versions: [], regions: [], host_names: [], pod_names: [], service_names: [] })
const quickFilterModel = ref({
  timeRange: '24h',
  severity: [],
  status: [],
  service: '',
  serviceNames: [],
  search: '',
  attributeFilters: [],
  environments: [],
  versions: [],
  regions: [],
  hostNames: [],
  podNames: [],
})

const filters = ref({
  service: route.query.service || '',
  profileType: '',
  timeRange: '24h',
  traceId: '',
  limit: 50,
  offset: 0,
})

let debounceTimer = null
const debouncedFilterChange = () => {
  clearTimeout(debounceTimer)
  debounceTimer = setTimeout(() => {
    filters.value.offset = 0
    fetchProfiles()
  }, 400)
}

const handleFilterChange = () => {
  filters.value.offset = 0
  fetchProfiles()
  if (activeTab.value === 'top-functions' && filters.value.service) {
    fetchTopFunctions()
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
  }
  const ms = map[filters.value.timeRange] || map['24h']
  return {
    start_time: new Date(now.getTime() - ms).toISOString(),
    end_time: now.toISOString(),
  }
}

const fetchProject = async () => {
  try {
    const response = await axios.get(`/api/projects/${projectId.value}`)
    currentProject.value = response.data
  } catch (error) {
    console.error('Failed to fetch project:', error)
  }
}

const fetchProfiles = async () => {
  loading.value = true
  try {
    const { start_time, end_time } = getTimeRange()
    const params = { start_time, end_time, limit: filters.value.limit, offset: filters.value.offset }
    if (filters.value.service) params.service_name = filters.value.service
    if (filters.value.profileType) params.profile_type = filters.value.profileType
    if (filters.value.traceId) params.trace_id = filters.value.traceId

    if (quickFilterModel.value.attributeFilters) {
      for (const af of quickFilterModel.value.attributeFilters) {
        if (af.key && af.values?.length > 0) {
          params[`attr.${af.key}`] = af.values.join(',')
        }
      }
    }

    const response = await axios.get(`/api/profiles/projects/${projectId.value}/profiles`, { params })
    profiles.value = response.data?.profiles || []
    total.value = response.data?.total || 0

    const types = new Set(profiles.value.map(p => p.period_type).filter(Boolean))
    if (types.size > 0) availableProfileTypes.value = [...types].sort()
  } catch (error) {
    console.error('Failed to fetch profiles:', error)
    profiles.value = []
    total.value = 0
  } finally {
    loading.value = false
  }
}

const fetchProfileServices = async () => {
  try {
    const { start_time, end_time } = getTimeRange()
    const response = await axios.get(`/api/profiles/projects/${projectId.value}/profiles`, {
      params: { start_time, end_time, limit: 200, offset: 0 },
    })
    const all = response.data?.profiles || []
    const svcSet = new Set(all.map(p => p.service_name).filter(Boolean))
    availableServices.value = [...svcSet].sort()
  } catch (error) {
    console.error('Failed to fetch profile services:', error)
  }
}

const fetchTopFunctions = async () => {
  topFunctionsLoading.value = true
  selectedFunction.value = null
  try {
    const { start_time, end_time } = getTimeRange()
    const params = {
      start_time,
      end_time,
      limit: 20,
      timeseries: 'true',
    }
    if (filters.value.service) params.service_name = filters.value.service
    if (filters.value.profileType) params.profile_type = filters.value.profileType

    if (quickFilterModel.value.attributeFilters) {
      for (const af of quickFilterModel.value.attributeFilters) {
        if (af.key && af.values?.length > 0) {
          params[`attr.${af.key}`] = af.values.join(',')
        }
      }
    }

    const response = await axios.get(
      `/api/profiles/projects/${projectId.value}/profiles/top-functions`,
      { params }
    )
    topFunctions.value = response.data?.functions || []
    timeseriesData.value = response.data?.timeseries || {}
  } catch (error) {
    console.error('Failed to fetch top functions:', error)
    topFunctions.value = []
    timeseriesData.value = {}
  } finally {
    topFunctionsLoading.value = false
  }
}

const fetchAttributeKeys = async () => {
  try {
    const { start_time, end_time } = getTimeRange()
    const response = await axios.get(`/api/profiles/projects/${projectId.value}/profiles/attribute-keys`, {
      params: { start_time, end_time },
    })
    attributeKeys.value = response.data || []
  } catch (error) {
    console.error('Failed to fetch attribute keys:', error)
  }
}

const loadAttributeValues = async (key) => {
  try {
    const { start_time, end_time } = getTimeRange()
    const response = await axios.get(`/api/profiles/projects/${projectId.value}/profiles/attribute-values`, {
      params: { key, start_time, end_time },
    })
    attributeValuesMap[key] = response.data || []
  } catch (error) {
    console.error(`Failed to fetch attribute values for ${key}:`, error)
  }
}

const handleQuickFilterChange = () => {
  filters.value.offset = 0
  fetchProfiles()
  if (activeTab.value === 'top-functions' && filters.value.service) {
    fetchTopFunctions()
  }
}

const truncatedAttributes = (attrs) => {
  if (!attrs || typeof attrs !== 'object') return {}
  const entries = Object.entries(attrs)
  const result = {}
  for (const [k, v] of entries.slice(0, 4)) {
    result[k] = v
  }
  return result
}

const openAggregateView = async () => {
  aggregateLoading.value = true
  showAggregateModal.value = true
  aggregateFlameGraph.value = null
  try {
    const { start_time, end_time } = getTimeRange()
    const params = { start_time, end_time }
    if (filters.value.profileType) params.profile_type = filters.value.profileType
    const response = await axios.get(
      `/api/profiles/projects/${projectId.value}/services/${encodeURIComponent(filters.value.service)}/profiles/aggregate`,
      { params }
    )
    aggregateFlameGraph.value = response.data?.flame_graph || null
  } catch (error) {
    console.error('Failed to fetch aggregate:', error)
  } finally {
    aggregateLoading.value = false
  }
}

const selectFunction = (name) => {
  selectedFunction.value = selectedFunction.value === name ? null : name
}

const getBarHeight = (samples) => {
  if (!selectedFunction.value || !timeseriesData.value[selectedFunction.value]) return '0%'
  const maxSamples = Math.max(...timeseriesData.value[selectedFunction.value].map(p => p.samples))
  if (maxSamples === 0) return '0%'
  return `${Math.max(2, (samples / maxSamples) * 100)}%`
}

const viewProfile = (profile) => {
  router.push(`/p/${projectId.value}/profiles/${profile.profile_id}`)
}

const prevPage = () => {
  filters.value.offset = Math.max(0, filters.value.offset - filters.value.limit)
  fetchProfiles()
}

const nextPage = () => {
  filters.value.offset += filters.value.limit
  fetchProfiles()
}

const formatNumber = (n) => {
  if (n == null) return '--'
  return n.toLocaleString()
}

const formatDuration = (nanos) => {
  if (nanos == null) return '--'
  const ms = nanos / 1_000_000
  if (ms < 1000) return `${ms.toFixed(1)}ms`
  const sec = ms / 1000
  if (sec < 60) return `${sec.toFixed(1)}s`
  const min = sec / 60
  return `${min.toFixed(1)}m`
}

const formatTimestamp = (ts) => {
  if (!ts) return '--'
  const date = new Date(ts)
  return date.toLocaleString()
}

watch(activeTab, (tab) => {
  if (tab === 'top-functions') {
    fetchTopFunctions()
  }
})

onMounted(async () => {
  await fetchProject()
  await Promise.all([fetchProfiles(), fetchProfileServices(), fetchAttributeKeys()])
})
</script>

<style scoped>
.profiles-page {
  display: flex;
  height: calc(100vh - 64px);
  overflow: hidden;
}

.data-section {
  min-width: 0;
}

.custom-scrollbar::-webkit-scrollbar {
  width: 6px;
}

.custom-scrollbar::-webkit-scrollbar-track {
  background: transparent;
}

.custom-scrollbar::-webkit-scrollbar-thumb {
  background-color: rgba(156, 163, 175, 0.5);
  border-radius: 3px;
}
</style>
