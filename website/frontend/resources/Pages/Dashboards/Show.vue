<template>
  <AppLayout :user="user" :current-project="project">
    <div class="dashboard-viewer">
      <!-- Dashboard Header -->
      <div class="dashboard-header">
        <div class="dashboard-header-left">
          <router-link :to="`/p/${projectId}/dashboards`" class="back-link">
            ← Back to Dashboards
          </router-link>
          <div>
            <h1 class="dashboard-title">{{ dashboard?.name }}</h1>
            <p v-if="dashboard?.description" class="dashboard-description">{{ dashboard.description }}</p>
          </div>
        </div>
        <div class="dashboard-header-right">
          <div class="dashboard-controls">
            <!-- Service Selector (if dashboard has service variable) -->
            <select 
              v-if="hasServiceVariable" 
              v-model="selectedService" 
              class="service-select"
            >
              <option value="">All Services</option>
              <option v-for="s in services" :key="s.service_name" :value="s.service_name">
                {{ s.service_name }}
              </option>
            </select>
            
            <select v-model="timeRange" class="time-range-select">
              <option value="15m">Last 15 minutes</option>
              <option value="30m">Last 30 minutes</option>
              <option value="1h">Last 1 hour</option>
              <option value="3h">Last 3 hours</option>
              <option value="6h">Last 6 hours</option>
              <option value="12h">Last 12 hours</option>
              <option value="24h">Last 24 hours</option>
              <option value="7d">Last 7 days</option>
              <option value="30d">Last 30 days</option>
            </select>
            <button
              @click="refreshDashboard"
              class="refresh-btn"
              :disabled="refreshing"
            >
              <svg class="w-4 h-4" :class="{ 'animate-spin': refreshing }" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
              </svg>
            </button>
            <button
              @click="analyzeDashboard"
              class="analyze-btn"
              :disabled="isStreaming"
              title="Analyze dashboard with MooDeng"
            >
              <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
              </svg>
              <span class="analyze-label">Analyze</span>
            </button>
            <button
              @click="toggleLock"
              class="lock-btn"
              :title="dashboard?.locked ? 'Unlock dashboard' : 'Lock dashboard'"
            >
              <svg v-if="dashboard?.locked" class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z" />
              </svg>
              <svg v-else class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 11V7a4 4 0 118 0m-4 8v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2z" />
              </svg>
            </button>
            <router-link
              v-if="!dashboard?.locked"
              :to="`/p/${projectId}/dashboards/${dashboardId}/edit`"
              class="edit-btn"
            >
              Edit
            </router-link>
          </div>
          <div class="refresh-status" v-if="lastRefreshed">
            Refreshed {{ formatRefreshTime(lastRefreshed) }}
          </div>
        </div>
      </div>

      <!-- Variable Bar -->
      <div v-if="dashboardVariables.length > 0" class="variable-bar">
        <div class="variable-bar-inner">
          <div
            v-for="v in dashboardVariables"
            :key="v.name"
            class="variable-item"
          >
            <label class="variable-label">{{ v.label || v.name }}</label>
            <!-- Query / Custom dropdown -->
            <select
              v-if="v.type === 'query' || v.type === 'custom'"
              v-model="variableValues[v.name]"
              class="variable-select"
              :multiple="v.multi"
            >
              <option v-if="v.includeAll" :value="v.allValue || '.*'">All</option>
              <option
                v-for="opt in filterGrafanaTokens(variableOptions[v.name] || v.options || [])"
                :key="opt"
                :value="opt"
              >
                {{ opt }}
              </option>
            </select>
            <!-- Interval dropdown -->
            <select
              v-else-if="v.type === 'interval'"
              v-model="variableValues[v.name]"
              class="variable-select"
            >
              <option v-for="opt in filterGrafanaTokens(v.options || intervalPresets)" :key="opt" :value="opt">
                {{ opt }}
              </option>
            </select>
            <!-- Textbox / Constant -->
            <input
              v-else
              v-model="variableValues[v.name]"
              type="text"
              class="variable-input"
              :placeholder="v.name"
            />
          </div>
        </div>
      </div>

      <!-- Tab Navigation -->
      <div v-if="tabs.length > 0" class="tab-navigation">
        <div class="tab-list">
          <button
            v-for="tab in tabs"
            :key="tab.id"
            @click="activeTabId = tab.id"
            class="tab-item"
            :class="{ 'tab-active': activeTabId === tab.id }"
          >
            <component v-if="getTabIcon(tab.icon)" :is="getTabIcon(tab.icon)" class="w-4 h-4 mr-2" />
            {{ tab.name }}
          </button>
        </div>
      </div>

      <!-- Loading -->
      <div v-if="initialLoading" class="flex items-center justify-center py-12">
        <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full"></div>
        <span class="ml-3 text-gray-400">Loading dashboard...</span>
      </div>

      <!-- Empty State -->
      <div v-else-if="filteredWidgets.length === 0" class="empty-dashboard">
        <svg class="mx-auto h-12 w-12 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
        </svg>
        <h3 class="mt-2 text-sm font-medium text-gray-900">No widgets</h3>
        <p class="mt-1 text-sm text-gray-400">{{ tabs.length > 0 ? 'This tab has no widgets yet.' : 'This dashboard has no widgets yet.' }}</p>
        <div class="mt-6">
          <router-link
            :to="`/p/${projectId}/dashboards/${dashboardId}/edit`"
            class="inline-flex items-center px-4 py-2 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-primary-600 hover:bg-primary-700"
          >
            Add Widgets
          </router-link>
        </div>
      </div>

      <!-- Widget Grid (12-column grid) -->
      <div v-else class="widget-grid">
        <div
          v-for="widget in sortedFilteredWidgets"
          :key="widget.id"
          :style="widgetGridStyle(widget)"
          class="widget-container"
        >
          <div class="widget-wrapper" @mouseenter="hoveredWidget = widget.id" @mouseleave="hoveredWidget = null">
            <div v-if="widget.title" class="widget-title">
              <span>{{ resolveTitle(widget.title) }}</span>
              <div v-if="hoveredWidget === widget.id" class="widget-actions">
                <a
                  v-for="(link, li) in (widget.widget_config?.contextLinks || [])"
                  :key="li"
                  :href="link.url"
                  target="_blank"
                  rel="noopener"
                  class="widget-action-btn context-link"
                  :title="link.label"
                >
                  <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1" />
                  </svg>
                </a>
                <button
                  v-if="canDrillDown(widget)"
                  @click="drillDown(widget)"
                  class="widget-action-btn"
                  title="Explore in trace/log viewer"
                >
                  <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
                  </svg>
                </button>
                <button
                  v-if="canCreateAlert(widget)"
                  @click="createAlertFromWidget(widget)"
                  class="widget-action-btn"
                  title="Create alert from this query"
                >
                  <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 17h5l-1.405-1.405A2.032 2.032 0 0118 14.158V11a6.002 6.002 0 00-4-5.659V5a2 2 0 10-4 0v.341C7.67 6.165 6 8.388 6 11v3.159c0 .538-.214 1.055-.595 1.436L4 17h5m6 0v1a3 3 0 11-6 0v-1m6 0H9" />
                  </svg>
                </button>
                <button
                  @click="explainWidget(widget)"
                  class="widget-action-btn explain-btn"
                  :disabled="isStreaming"
                  title="Explain this widget with MooDeng"
                >
                  <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
                  </svg>
                </button>
              </div>
            </div>
            <div class="widget-content">
              <component
                :is="getWidgetComponent(widget.widget_type)"
                :config="widget.widget_config"
                :project-id="projectId"
                :time-range="timeRange"
                :variables="widgetVariables"
              />
            </div>
          </div>
        </div>
      </div>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted, watch, markRaw } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { formatDistanceToNow } from 'date-fns'
import AppLayout from '@/Layouts/AppLayout.vue'
import DashboardWidget from '@/components/DashboardWidget.vue'
import TimeSeriesWidget from '@/components/widgets/TimeSeriesWidget.vue'
import StatWidget from '@/components/widgets/StatWidget.vue'
import TableWidget from '@/components/widgets/TableWidget.vue'
import HistogramWidget from '@/components/widgets/HistogramWidget.vue'
import HorizontalBarWidget from '@/components/widgets/HorizontalBarWidget.vue'
import PieWidget from '@/components/widgets/PieWidget.vue'
import GeomapWidget from '@/components/widgets/GeomapWidget.vue'
import TextWidget from '@/components/widgets/TextWidget.vue'
import HeatmapWidget from '@/components/widgets/HeatmapWidget.vue'
import TopListWidget from '@/components/widgets/TopListWidget.vue'
import { useAuth } from '@/composables/useAuth'
import { useWidgetQuery } from '@/composables/useWidgetQuery'
import { usePageContext } from '@/composables/usePageContext'
import { useAgent } from '@/composables/useAgent'
import axios from 'axios'

const route = useRoute()
const router = useRouter()
const { user, fetchUser } = useAuth()
const { fetchServices } = useWidgetQuery()

const projectId = ref(route.params.id)
const dashboardId = ref(route.params.dashboard_id)
const project = ref(null)
const dashboard = ref(null)
const tabs = ref([])
const widgets = ref([])
const services = ref([])
const initialLoading = ref(true)
const refreshing = ref(false)
const timeRange = ref('1h')
const lastRefreshed = ref(null)
const activeTabId = ref(null)
const selectedService = ref('')
const variableValues = ref({})
const variableOptions = ref({})
const intervalPresets = ['10s', '30s', '1m', '5m', '15m', '1h']

const filterGrafanaTokens = (opts) =>
  opts.filter(o => typeof o !== 'string' || (!o.startsWith('$__') && !o.startsWith('$_') && !o.startsWith('${__')))
let refreshInterval = null
let initialTimeRangeApplied = false
const hoveredWidget = ref(null)

const canDrillDown = (widget) => {
  const type = widget.widget_type
  return ['timeseries', 'time_series', 'line', 'bar', 'horizontal_bar', 'histogram'].includes(type)
}

const canCreateAlert = (widget) => {
  const query = widget.widget_config?.query
  return query && (query.promql || query.queries)
}

const drillDown = (widget) => {
  const query = widget.widget_config?.query
  if (!query) return
  const promql = query.promql || (query.queries && query.queries[0]?.promql) || ''
  if (promql) {
    const params = new URLSearchParams({ promql, time_range: timeRange.value })
    router.push(`/p/${projectId.value}/traces?${params.toString()}`)
  }
}

const toggleLock = async () => {
  const newLocked = !dashboard.value.locked
  try {
    await axios.put(`/api/dashboards/${projectId.value}/dashboards/${dashboardId.value}`, {
      locked: newLocked,
    })
    dashboard.value.locked = newLocked
  } catch (err) {
    console.error('Failed to toggle lock:', err)
    alert(err.response?.data?.error || 'Failed to toggle lock')
  }
}

const timeRangeToSeconds = (tr) => {
  const match = tr.match(/^(\d+)(m|h|d)$/)
  if (!match) return 300
  const [, n, unit] = match
  const multiplier = { m: 60, h: 3600, d: 86400 }
  return parseInt(n) * (multiplier[unit] || 60)
}

const createAlertFromWidget = (widget) => {
  const query = widget.widget_config?.query
  if (!query) return
  const promql = query.promql || (query.queries && query.queries[0]?.promql) || ''
  const params = new URLSearchParams({
    name: resolveTitle(widget.title) || 'New Alert',
    promql,
    eval_window: String(timeRangeToSeconds(timeRange.value)),
  })
  router.push(`/p/${projectId.value}/alerts/new?${params.toString()}`)
}

// Agent integration
const { isOpen: agentOpen, sendMessage, startNewConversation, isStreaming } = useAgent()

const analyzeDashboard = async () => {
  if (isStreaming.value) return
  startNewConversation()
  agentOpen.value = true
  await sendMessage(
    'Analyze the health of this dashboard. For each widget, check whether the data looks ' +
    'normal and flag any anomalies, missing data, or patterns worth noting. Search the ' +
    'knowledge base for any known issues related to the metrics shown here.'
  )
}

const explainWidget = async (widget) => {
  if (isStreaming.value) return
  startNewConversation()
  agentOpen.value = true
  const queries = widget.widget_config?.query
  let queryInfo = ''
  if (queries?.promql) {
    queryInfo = ` The query is: ${queries.promql}.`
  } else if (queries?.queries?.length) {
    queryInfo = ` The queries are: ${queries.queries.map(q => q.promql).filter(Boolean).join(', ')}.`
  }
  await sendMessage(
    `Explain the "${widget.title || 'Untitled'}" widget (type: ${widget.widget_type}). ` +
    `What does this metric show, does the current data look normal, and is there ` +
    `anything I should know about it? Check the knowledge base for known patterns.${queryInfo}`
  )
}

// Map widget types to components
const widgetComponentMap = {
  'timeseries': markRaw(TimeSeriesWidget),
  'time_series': markRaw(TimeSeriesWidget),
  'line': markRaw(TimeSeriesWidget),
  'stat': markRaw(StatWidget),
  'metric': markRaw(StatWidget),
  'number': markRaw(StatWidget),
  'table': markRaw(TableWidget),
  'histogram': markRaw(HistogramWidget),
  'bar': markRaw(HorizontalBarWidget),
  'horizontal_bar': markRaw(HorizontalBarWidget),
  'pie': markRaw(PieWidget),
  'geomap': markRaw(GeomapWidget),
  'text': markRaw(TextWidget),
  'heatmap': markRaw(HeatmapWidget),
  'top_list': markRaw(TopListWidget),
  'toplist': markRaw(TopListWidget),
}

const getWidgetComponent = (type) => {
  return widgetComponentMap[type] || DashboardWidget
}

// Icon mapping for tabs
const getTabIcon = (iconName) => {
  // Could import icons from a library like heroicons
  // For now, return null to skip icons
  return null
}

// Check if dashboard has service variable defined
const hasServiceVariable = computed(() => {
  const vars = dashboard.value?.layout_config?.variables
  if (!vars || !Array.isArray(vars)) return false
  return vars.some(v => v.type === 'service_select')
})

// Dashboard variables from layout_config (Grafana-imported)
const dashboardVariables = computed(() => {
  const vars = dashboard.value?.layout_config?.variables
  if (!vars || !Array.isArray(vars)) return []
  return vars.filter(v => v.type !== 'service_select' && v.type !== 'datasource')
})

// Build variables object for widgets
const widgetVariables = computed(() => {
  const vars = { ...variableValues.value }
  if (selectedService.value) {
    vars.service = selectedService.value
  }
  return vars
})

// Filter widgets by active tab
const filteredWidgets = computed(() => {
  if (tabs.value.length === 0) {
    // No tabs, show all widgets
    return widgets.value
  }
  if (!activeTabId.value) {
    // Show widgets with no tab
    return widgets.value.filter(w => !w.tab_id)
  }
  return widgets.value.filter(w => w.tab_id === activeTabId.value)
})

const sortedFilteredWidgets = computed(() => {
  return [...filteredWidgets.value].sort((a, b) => {
    if (a.position_y !== b.position_y) {
      return a.position_y - b.position_y
    }
    return a.position_x - b.position_x
  })
})

const resolveTitle = (title) => {
  if (!title) return title
  return title.replace(/\$\{(\w+)\}|\$(\w+)/g, (match, braced, bare) => {
    const name = braced || bare
    const val = variableValues.value[name]
    if (val !== undefined && val !== '') return val
    return match
  })
}

const TABLE_TYPES = new Set(['table', 'toplist', 'top_list'])
const CHART_TYPES = new Set(['timeseries', 'time_series', 'line', 'bar', 'histogram', 'heatmap'])
const MIN_TABLE_ROWS = 4
const MIN_TABLE_COLS = 6
const MIN_CHART_ROWS = 3

const widgetGridStyle = (widget) => {
  const type = widget.widget_type
  let rows = widget.height || 2
  let cols = widget.width || 6

  if (TABLE_TYPES.has(type)) {
    if (rows < MIN_TABLE_ROWS) rows = MIN_TABLE_ROWS
    if (cols < MIN_TABLE_COLS) cols = MIN_TABLE_COLS
  }
  if (CHART_TYPES.has(type) && rows < MIN_CHART_ROWS) {
    rows = MIN_CHART_ROWS
  }

  return {
    gridColumn: `${(widget.position_x || 0) + 1} / span ${cols}`,
    gridRow: `span ${rows}`,
  }
}

const formatRefreshTime = (time) => {
  try {
    return formatDistanceToNow(new Date(time), { addSuffix: true })
  } catch {
    return 'just now'
  }
}

const loadDashboard = async () => {
  try {
    const [dashboardRes, widgetsRes, tabsRes] = await Promise.all([
      axios.get(`/api/dashboards/${projectId.value}/dashboards/${dashboardId.value}`),
      axios.get(`/api/dashboards/${projectId.value}/dashboards/${dashboardId.value}/widgets`),
      axios.get(`/api/dashboards/${projectId.value}/dashboards/${dashboardId.value}/tabs`).catch(() => ({ data: [] }))
    ])
    
    dashboard.value = dashboardRes.data
    widgets.value = widgetsRes.data.map(w => ({
      ...w,
      updatedAt: w.updated_at,
    }))
    tabs.value = tabsRes.data || []
    
    if (tabs.value.length > 0 && !activeTabId.value) {
      activeTabId.value = tabs.value[0].id
    }
    
    if (!initialTimeRangeApplied && dashboard.value.time_range) {
      timeRange.value = dashboard.value.time_range
      initialTimeRangeApplied = true
    }
    
    lastRefreshed.value = new Date()
  } catch (err) {
    console.error('Failed to load dashboard:', err)
    if (err.response?.status === 404) {
      router.push(`/p/${projectId.value}/dashboards`)
    }
  } finally {
    initialLoading.value = false
  }
}

const loadServices = async () => {
  try {
    services.value = await fetchServices(projectId.value)
  } catch (err) {
    console.error('Failed to load services:', err)
  }
}

const initializeVariables = () => {
  const vars = dashboard.value?.layout_config?.variables
  if (!vars || !Array.isArray(vars)) return

  const defaults = {}
  for (const v of vars) {
    if (v.type === 'service_select' || v.type === 'datasource') continue
    defaults[v.name] = v.default || ''
  }
  variableValues.value = defaults
}

const fetchVariableOptions = async () => {
  const vars = dashboardVariables.value
  if (!vars.length) return

  for (const v of vars) {
    if (v.type !== 'query' || !v.query) continue

    try {
      const response = await axios.post(`/api/${projectId.value}/variable-values`, {
        query: v.query,
        time_range: {
          from: `now-${timeRange.value}`,
          to: 'now',
        },
      })
      variableOptions.value = {
        ...variableOptions.value,
        [v.name]: response.data.values || [],
      }
    } catch (err) {
      console.error(`Failed to fetch options for variable ${v.name}:`, err)
    }
  }
}

const refreshDashboard = async () => {
  refreshing.value = true
  await loadDashboard()
  refreshing.value = false
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
  await Promise.all([
    loadProject(),
    loadDashboard(),
    loadServices()
  ])

  initializeVariables()
  await fetchVariableOptions()
  
  // Auto-refresh if refresh_interval is set
  if (dashboard.value?.refresh_interval) {
    const intervalMs = dashboard.value.refresh_interval * 1000
    refreshInterval = setInterval(() => {
      refreshDashboard()
    }, intervalMs)
  }
})

const { setPageSnapshot, clearPageSnapshot } = usePageContext()

watch([dashboard, widgets, timeRange, selectedService, variableValues], () => {
  if (!dashboard.value) return
  setPageSnapshot({
    page: 'Dashboard',
    dashboard_name: dashboard.value.name,
    description: dashboard.value.description || undefined,
    time_range: timeRange.value,
    selected_service: selectedService.value || undefined,
    tabs: tabs.value.map(t => t.name),
    widget_count: widgets.value.length,
    widgets: widgets.value.slice(0, 10).map(w => ({
      title: w.title,
      type: w.widget_type,
    })),
  })
}, { deep: true })

onUnmounted(() => {
  clearPageSnapshot()
  if (refreshInterval) {
    clearInterval(refreshInterval)
  }
})
</script>

<style scoped>
.dashboard-viewer {
  @apply min-h-screen bg-white;
}

.dashboard-header {
  @apply flex items-center justify-between px-6 py-4 border-b border-gray-200 bg-gray-50;
}

.dashboard-header-left {
  @apply flex items-center gap-4;
}

.back-link {
  @apply text-sm text-primary-400 hover:text-primary-300;
}

.dashboard-title {
  @apply text-xl font-semibold text-gray-900;
}

.dashboard-description {
  @apply text-sm text-gray-400 mt-1;
}

.dashboard-header-right {
  @apply flex flex-col items-end gap-2;
}

.dashboard-controls {
  @apply flex items-center gap-2;
}

.service-select,
.time-range-select {
  @apply px-3 py-1.5 text-sm bg-gray-100 border border-gray-300 text-gray-900 rounded-md focus:outline-none focus:ring-2 focus:ring-primary-500;
}

.service-select {
  @apply min-w-[150px];
}

.refresh-btn,
.edit-btn {
  @apply px-3 py-1.5 text-sm font-medium rounded-md transition-colors flex items-center gap-1;
}

.lock-btn {
  @apply text-gray-600 hover:text-gray-900 hover:bg-gray-100 border border-gray-300;
}

.refresh-btn {
  @apply text-gray-600 hover:text-gray-900 hover:bg-gray-100 border border-gray-300;
}

.refresh-btn:disabled {
  @apply opacity-50 cursor-not-allowed;
}

.analyze-btn {
  @apply px-3 py-1.5 text-sm font-medium rounded-md transition-colors flex items-center gap-1;
  @apply text-purple-600 hover:text-purple-800 hover:bg-purple-50 border border-purple-300;
}

.analyze-btn:disabled {
  @apply opacity-50 cursor-not-allowed;
}

.analyze-label {
  @apply hidden sm:inline;
}

.edit-btn {
  @apply text-white bg-primary-600 hover:bg-primary-700;
}

.refresh-status {
  @apply text-xs text-gray-500;
}

/* Variable Bar */
.variable-bar {
  @apply px-6 py-3 border-b border-gray-200 bg-gray-50/80;
}

.variable-bar-inner {
  @apply flex items-center gap-4 flex-wrap;
}

.variable-item {
  @apply flex items-center gap-2;
}

.variable-label {
  @apply text-xs font-medium text-gray-500 uppercase tracking-wider;
}

.variable-select {
  @apply px-3 py-1.5 text-sm bg-white border border-gray-300 text-gray-900 rounded-md focus:outline-none focus:ring-2 focus:ring-primary-500 min-w-[120px];
}

.variable-input {
  @apply px-3 py-1.5 text-sm bg-white border border-gray-300 text-gray-900 rounded-md focus:outline-none focus:ring-2 focus:ring-primary-500 w-32;
}

/* Tab Navigation */
.tab-navigation {
  @apply px-6 py-0 border-b border-gray-200 bg-gray-50;
}

.tab-list {
  @apply flex gap-1;
}

.tab-item {
  @apply px-4 py-3 text-sm font-medium text-gray-400 hover:text-gray-700 border-b-2 border-transparent transition-colors flex items-center;
}

.tab-item.tab-active {
  @apply text-primary-400 border-primary-400;
}

.empty-dashboard {
  @apply text-center py-24;
}

/* Widget Grid - 12 column grid */
.widget-grid {
  @apply grid grid-cols-12 gap-4 p-6;
  grid-auto-rows: 80px;
  grid-auto-flow: row dense;
}

.widget-container {
  @apply min-h-0;
}

.widget-wrapper {
  @apply h-full bg-gray-50 rounded-lg border border-gray-200 flex flex-col overflow-hidden;
}

.widget-title {
  @apply px-4 py-2 text-sm font-medium text-gray-700 border-b border-gray-200 flex-shrink-0 flex items-center justify-between;
}

.widget-actions {
  @apply flex items-center gap-1;
}

.widget-action-btn {
  @apply p-1 rounded text-gray-400 hover:text-gray-700 hover:bg-gray-100 transition-colors;
}

.context-link {
  @apply text-primary-400 hover:text-primary-600;
}

.explain-btn {
  @apply text-purple-400 hover:text-purple-600 hover:bg-purple-50;
}

.widget-content {
  @apply flex-1 min-h-0 p-3;
}

.spinner {
  animation: spin 0.6s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}
</style>

