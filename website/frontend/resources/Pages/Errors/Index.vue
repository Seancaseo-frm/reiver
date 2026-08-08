<template>
  <AppLayout :user="user" :current-project="currentProject">
    <!-- Warning: No notification channels configured -->
    <BaseCard v-if="!hasNotificationChannels && !loadingChannels && initialLoadComplete" class="mx-4 mt-4 mb-0 border-yellow-500 bg-yellow-50">
      <div class="flex items-start">
        <svg class="w-5 h-5 text-yellow-600 mt-0.5 mr-3 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
        </svg>
        <div class="flex-1">
          <h3 class="text-sm font-semibold text-yellow-800 mb-1">No notification channels configured</h3>
          <p class="text-sm text-yellow-700 mb-3">
            Add a notification integration like Slack, Discord, or PagerDuty to receive alerts when exceptions occur.
          </p>
          <router-link
            :to="`/p/${projectId}/integrations`"
            class="inline-flex items-center px-3 py-2 text-sm font-medium text-yellow-800 bg-yellow-100 hover:bg-yellow-200 rounded-md transition-colors"
          >
            Set up notification channels
            <svg class="w-4 h-4 ml-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
            </svg>
          </router-link>
        </div>
      </div>
    </BaseCard>

    <div class="all-errors-page" :class="{ 'filter-visible': showFilters }">
        <!-- Filter Sidebar (Collapsible) -->
        <aside
          v-if="showFilters"
          class="filter-panel border-r border-gray-200 bg-white overflow-y-auto custom-scrollbar"
        >
          <QuickFilters
            v-model="filters"
            :show-severity-filters="false"
            :show-context-filters="true"
            :filter-values="filterValues"
            @filter-change="handleFilterChange"
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

              <!-- Tab Selector -->
              <div class="flex items-center gap-1 ml-4">
                <button
                  v-for="tab in tabs"
                  :key="tab.id"
                  @click="activeTab = tab.id"
                  :class="[
                    'px-3 py-1.5 rounded text-sm font-medium transition-colors',
                    activeTab === tab.id
                      ? 'bg-primary-600 text-white'
                      : 'text-gray-600 hover:text-gray-900 hover:bg-gray-100'
                  ]"
                >
                  {{ tab.label }}
                  <span
                    v-if="tab.count > 0"
                    class="ml-1.5 text-xs px-1.5 py-0.5 rounded-full"
                    :class="activeTab === tab.id ? 'bg-primary-700' : 'bg-gray-200'"
                  >
                    {{ tab.count }}
                  </span>
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

          <!-- Content Area -->
          <div class="flex-1 overflow-y-auto custom-scrollbar">
            <div class="p-4">
              <!-- Loading State -->
              <div v-if="loading && !initialLoadComplete" class="text-center py-8">
                <div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-primary-500"></div>
                <p class="mt-2 text-gray-500">Loading errors...</p>
              </div>

              <!-- Empty State -->
              <div v-else-if="filteredItems.length === 0 && !loading">
                <div class="text-center py-12">
                  <svg class="mx-auto h-12 w-12 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L3.732 16.5c-.77.833.192 2.5 1.732 2.5z" />
                  </svg>
                  <h3 class="mt-2 text-sm font-medium text-gray-900">No errors found</h3>
                  <p class="mt-1 text-sm text-gray-500">Try adjusting your filters or time range</p>
                </div>
              </div>

              <!-- Data Table -->
              <AllErrorsTable
                v-else
                :items="filteredItems"
                :loading="loading"
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
import AllErrorsTable from '@/components/AllErrorsTable.vue'
import BaseCard from '@/components/BaseCard.vue'
import axios from 'axios'

const route = useRoute()
const router = useRouter()
const { user } = useAuth()

const projectId = computed(() => route.params.id)
const currentProject = ref(null)

// UI State
const activeTab = ref('all')
const showFilters = ref((() => {
  const stored = localStorage.getItem('showErrorsFilters')
  return stored ? stored === 'true' : true
})())
const loading = ref(false)
const initialLoadComplete = ref(false)
const expanded = ref({})

// Notification channels
const notificationChannels = ref([])
const loadingChannels = ref(false)
const hasNotificationChannels = computed(() => notificationChannels.value.length > 0)

// Available filter values from backend
const filterValues = ref({
  environments: [],
  versions: [],
  regions: [],
  host_names: [],
  pod_names: [],
  service_names: [],
})

// Data
const items = ref([])
const filters = ref({
  timeRange: '24h',
  severity: [],
  status: [],
  service: '',
  serviceNames: [],
  search: '',
  // Context filters (arrays for multi-select)
  environments: [],
  versions: [],
  regions: [],
  hostNames: [],
  podNames: [],
})

// Tab definitions
const tabs = computed(() => [
  { id: 'all', label: 'All Errors', count: items.value.length },
  { id: 'unresolved', label: 'Unresolved', count: items.value.filter(i => i.status !== 'resolved').length },
  { id: 'resolved', label: 'Resolved', count: items.value.filter(i => i.status === 'resolved').length },
])

// Computed
const filteredItems = computed(() => {
  let filtered = items.value

  // Filter by tab
  if (activeTab.value === 'unresolved') {
    filtered = filtered.filter(item => item.status !== 'resolved')
  } else if (activeTab.value === 'resolved') {
    filtered = filtered.filter(item => item.status === 'resolved')
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

const fetchNotificationChannels = async () => {
  if (!projectId.value) return
  
  loadingChannels.value = true
  try {
    const headers = { 'x-project-id': projectId.value }
    const [slackRes, pagerdutyRes, teamsRes, discordRes] = await Promise.allSettled([
      axios.get('/api/slack/integrations', { params: { project_id: projectId.value }, headers }),
      axios.get('/api/pagerduty/integrations', { params: { project_id: projectId.value }, headers }),
      axios.get('/api/teams/integrations', { params: { project_id: projectId.value }, headers }),
      axios.get('/api/discord/integrations', { params: { project_id: projectId.value }, headers }),
    ])

    const channels = []
    const integrationTypes = ['slack', 'pagerduty', 'teams', 'discord']
    ;[slackRes, pagerdutyRes, teamsRes, discordRes].forEach((result, index) => {
      if (result.status === 'fulfilled' && result.value?.data) {
        const integrationChannels = Array.isArray(result.value.data) ? result.value.data : []
        channels.push(...integrationChannels
          .filter((c) => c.enabled)
          .map((c) => ({ ...c, type: integrationTypes[index] }))
        )
      }
    })

    notificationChannels.value = channels
  } catch (error) {
    console.warn('Failed to load notification channels:', error)
    notificationChannels.value = []
  } finally {
    loadingChannels.value = false
  }
}

const fetchFilterValues = async () => {
  if (!projectId.value) return

  try {
    const response = await axios.get(`/api/projects/${projectId.value}/exceptions/filter-values`)
    filterValues.value = response.data
  } catch (error) {
    console.error('Failed to fetch filter values:', error)
    // Keep default empty arrays on error
  }
}

const fetchErrors = async () => {
  if (!projectId.value) return

  loading.value = true
  try {
    const params = {
      event_type: 'errors',
      time_range: filters.value.timeRange,
      severity: filters.value.severity.join(','),
      status: filters.value.status.join(','),
      search: filters.value.search,
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

    // Remove empty params
    Object.keys(params).forEach(key => {
      if (!params[key]) delete params[key]
    })

    const response = await axios.get(`/api/projects/${projectId.value}/exceptions`, { params })
    items.value = response.data || []
    initialLoadComplete.value = true
  } catch (error) {
    console.error('Failed to fetch errors:', error)
    items.value = []

    // If endpoint doesn't exist yet, show empty state
    if (error.response?.status === 404) {
      console.warn('Exceptions endpoint not found, showing empty state')
      initialLoadComplete.value = true
    }
  } finally {
    loading.value = false
  }
}

const refreshData = async () => {
  await Promise.all([fetchErrors(), fetchFilterValues()])
}

const handleFilterChange = () => {
  fetchErrors()
}

const handleRowClick = (item) => {
  // Navigate to error detail page using fingerprint (reliable grouping key)
  router.push(`/p/${projectId.value}/exceptions/${item.fingerprint}`)
}

const handleExpand = (itemId) => {
  expanded.value[itemId] = !expanded.value[itemId]
}

const handleAction = async ({ action, item }) => {
  if (action === 'resolve') {
    try {
      await axios.patch(`/api/projects/${projectId.value}/exceptions/${item.fingerprint}`, {
        status: 'resolved'
      })
      // Refresh to get updated data
      await fetchErrors()
    } catch (error) {
      console.error('Failed to resolve error:', error)
    }
  } else if (action === 'copy') {
    // Already handled in AllErrorsTable
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
      filters.value.service = value
    }
  } else if (field === 'severity') {
    if (!filters.value.severity.includes(value)) {
      filters.value.severity.push(value)
    }
  } else if (field === 'environment') {
    if (!filters.value.environments.includes(value)) {
      filters.value.environments.push(value)
    }
  } else if (field === 'version') {
    if (!filters.value.versions.includes(value)) {
      filters.value.versions.push(value)
    }
  }
  // Ensure filter panel is visible
  showFilters.value = true
  // Trigger re-fetch
  fetchErrors()
}

// Watch for tab changes
watch(activeTab, () => {
  // Don't refetch, just filter client-side for better UX
})

// Watch for filter visibility changes
watch(showFilters, (value) => {
  localStorage.setItem('showErrorsFilters', String(value))
})

// Page snapshot for AI agent
const { setPageSnapshot, clearPageSnapshot } = usePageContext()

watch([items, filters], () => {
  if (!items.value?.length) return
  const topErrors = items.value.slice(0, 5).map(e => ({
    type: e.exception_type || e.fingerprint,
    count: e.count,
    service: e.service_name,
  }))
  setPageSnapshot({
    page: 'Exceptions',
    time_range: filters.value.timeRange,
    filters_active: {
      severity: filters.value.severity,
      services: filters.value.serviceNames,
      status: filters.value.status,
      search: filters.value.search || undefined,
    },
    counts: {
      total: tabs.value[0]?.count || 0,
      unresolved: tabs.value[1]?.count || 0,
      resolved: tabs.value[2]?.count || 0,
    },
    top_errors: topErrors,
  })
}, { deep: true })

// Lifecycle
onMounted(async () => {
  await fetchProject()
  await Promise.all([fetchFilterValues(), fetchErrors(), fetchNotificationChannels()])
})

onUnmounted(() => clearPageSnapshot())
</script>

<style scoped>
.all-errors-page {
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
