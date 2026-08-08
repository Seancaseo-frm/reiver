<template>
  <div
    v-if="traceIds.length > 0"
    class="correlated-panel"
    :class="{ collapsed: isCollapsed }"
    :style="{ height: isCollapsed ? '36px' : panelHeight + 'px' }"
  >
    <!-- Drag handle -->
    <div
      v-if="!isCollapsed"
      class="drag-handle"
      @mousedown="startResize"
    >
      <div class="drag-indicator" />
    </div>

    <!-- Header bar -->
    <div class="panel-header">
      <div class="flex items-center gap-2">
        <button @click="isCollapsed = !isCollapsed" class="collapse-btn" :title="isCollapsed ? 'Expand' : 'Collapse'">
          <svg
            class="w-4 h-4 transition-transform"
            :class="{ 'rotate-180': !isCollapsed }"
            fill="none" stroke="currentColor" viewBox="0 0 24 24"
          >
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 15l7-7 7 7" />
          </svg>
        </button>
        <span class="text-xs font-semibold text-gray-600 dark:text-gray-400 uppercase tracking-wider">Correlated Signals</span>
      </div>

      <!-- Tabs -->
      <div v-if="!isCollapsed" class="flex items-center gap-1">
        <button
          v-for="tab in visibleTabs"
          :key="tab.id"
          @click="activeTab = tab.id"
          :class="[
            'tab-btn',
            activeTab === tab.id ? 'tab-btn-active' : 'tab-btn-inactive',
          ]"
        >
          {{ tab.label }}
          <span v-if="tab.count !== null" class="tab-count">{{ tab.count }}</span>
        </button>
      </div>
    </div>

    <!-- Tab content -->
    <div v-if="!isCollapsed" class="panel-body">
      <!-- Logs tab -->
      <div v-if="activeTab === 'logs'" class="tab-content">
        <div v-if="logsLoading" class="tab-loading">Loading logs...</div>
        <div v-else-if="logs.length === 0" class="tab-empty">No correlated logs found.</div>
        <div v-else class="log-rows">
          <div
            v-for="log in logs"
            :key="log.id"
            class="log-row"
            @click="$router.push({ path: `/p/${projectId}/logs/${log.id}`, query: log.timestamp ? { timestamp: log.timestamp } : {} })"
          >
            <span :class="levelBadge(log.severity_text || log.level)">{{ (log.severity_text || log.level || 'info').toUpperCase() }}</span>
            <span class="log-time">{{ fmtTime(log.timestamp) }}</span>
            <span class="log-service">{{ log.service_name }}</span>
            <span class="log-msg">{{ log.message || log.body }}</span>
          </div>
        </div>
      </div>

      <!-- Traces tab -->
      <div v-if="activeTab === 'traces'" class="tab-content">
        <div v-if="tracesLoading" class="tab-loading">Loading trace...</div>
        <div v-else-if="!traceSummary" class="tab-empty">No trace data available.</div>
        <div v-else class="trace-summary">
          <div class="grid grid-cols-4 gap-3 mb-3">
            <div class="summary-stat">
              <div class="summary-label">Duration</div>
              <div class="summary-value">{{ fmtDuration(traceSummary.duration_ns) }}</div>
            </div>
            <div class="summary-stat">
              <div class="summary-label">Spans</div>
              <div class="summary-value">{{ traceSummary.span_count }}</div>
            </div>
            <div class="summary-stat">
              <div class="summary-label">Services</div>
              <div class="summary-value">{{ traceSummary.service_count }}</div>
            </div>
            <div class="summary-stat">
              <div class="summary-label">Status</div>
              <div class="summary-value" :class="traceSummary.status === 'error' ? 'text-red-600' : 'text-green-600'">
                {{ (traceSummary.status || 'ok').toUpperCase() }}
              </div>
            </div>
          </div>
          <router-link
            :to="`/p/${projectId}/traces/${traceIds[0]}`"
            class="text-sm text-primary-600 hover:text-primary-700 font-medium"
          >
            Open full trace →
          </router-link>
        </div>
      </div>

      <!-- Exceptions tab -->
      <div v-if="activeTab === 'exceptions'" class="tab-content">
        <div v-if="exceptionsLoading" class="tab-loading">Loading exceptions...</div>
        <div v-else-if="exceptions.length === 0" class="tab-empty">No correlated exceptions found.</div>
        <div v-else class="exception-rows">
          <div
            v-for="ex in exceptions"
            :key="ex.id || ex.fingerprint"
            class="exception-row"
            @click="$router.push(`/p/${projectId}/exceptions/${ex.fingerprint || ex.id}`)"
          >
            <span class="exception-type">{{ ex.exception_type || 'Exception' }}</span>
            <span class="exception-msg">{{ ex.message || ex.exception_value }}</span>
            <span class="exception-time">{{ fmtTime(ex.timestamp || ex.last_seen) }}</span>
          </div>
        </div>
      </div>

      <!-- Metrics tab -->
      <div v-if="activeTab === 'metrics'" class="tab-content">
        <div class="metrics-content">
          <div v-if="serviceMetricsLoading" class="tab-loading">Loading metrics...</div>
          <div v-else-if="metricNames.length > 0" class="space-y-3">
            <div class="metric-names-list">
              <div
                v-for="m in metricNames"
                :key="m"
                class="metric-name-row"
                @click="$router.push(`/p/${projectId}/dashboards`)"
              >
                <span class="metric-name-label">{{ m }}</span>
              </div>
            </div>
            <router-link
              :to="`/p/${projectId}/dashboards`"
              class="text-sm text-primary-600 hover:text-primary-700 font-medium inline-block"
            >
              View in dashboards →
            </router-link>
          </div>
          <div v-else class="tab-empty">
            <span v-if="serviceName">No OTel metrics found for service <strong>{{ serviceName }}</strong>.</span>
            <span v-else>No service context available for metric lookup.</span>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, watch, onMounted, onUnmounted } from 'vue'
import { format } from 'date-fns'
import axios from 'axios'

const props = defineProps({
  traceIds: { type: Array, default: () => [] },
  spanId: { type: String, default: '' },
  serviceName: { type: String, default: '' },
  projectId: { type: String, required: true },
  timestamp: { type: String, default: '' },
  hideTabs: { type: Array, default: () => [] },
})

const STORAGE_KEY = 'correlatedPanelHeight'
const MIN_HEIGHT = 120
const MAX_HEIGHT_VH = 0.6

const isCollapsed = ref(false)
const panelHeight = ref(parseInt(localStorage.getItem(STORAGE_KEY)) || 260)
const activeTab = ref('')

// Data
const logs = ref([])
const logsLoading = ref(false)
const traceSummary = ref(null)
const tracesLoading = ref(false)
const exceptions = ref([])
const exceptionsLoading = ref(false)

const visibleTabs = computed(() => {
  const tabs = []
  if (!props.hideTabs.includes('logs'))       tabs.push({ id: 'logs', label: 'Logs', count: logs.value.length || null })
  if (!props.hideTabs.includes('traces'))     tabs.push({ id: 'traces', label: 'Traces', count: null })
  if (!props.hideTabs.includes('exceptions')) tabs.push({ id: 'exceptions', label: 'Exceptions', count: exceptions.value.length || null })
  if (!props.hideTabs.includes('metrics'))    tabs.push({ id: 'metrics', label: 'Metrics', count: null })
  return tabs
})

const metricNames = ref([])
const serviceMetricsLoading = ref(false)

// Set default active tab
watch(visibleTabs, (tabs) => {
  if (tabs.length > 0 && !tabs.find(t => t.id === activeTab.value)) {
    activeTab.value = tabs[0].id
  }
}, { immediate: true })

// Fetch data when traceIds change
watch(() => props.traceIds, async (ids) => {
  if (ids.length > 0) {
    await Promise.all([fetchLogs(), fetchTrace(), fetchExceptions(), fetchServiceMetrics()])
  }
}, { immediate: true })

async function fetchLogs() {
  if (props.traceIds.length === 0) return
  logsLoading.value = true
  try {
    const response = await axios.get(`/api/projects/${props.projectId}/events`, {
      params: { event_type: 'logs', trace_id: props.traceIds[0], time_range: '30d' }
    })
    const data = Array.isArray(response.data) ? response.data : (response.data?.logs || [])
    logs.value = data.filter(e => e.type === 'log' || !e.type)
  } catch (e) {
    console.error('Correlated logs fetch failed:', e)
    logs.value = []
  } finally {
    logsLoading.value = false
  }
}

async function fetchTrace() {
  if (props.traceIds.length === 0) return
  tracesLoading.value = true
  try {
    const response = await axios.get(`/api/projects/${props.projectId}/traces/${props.traceIds[0]}`)
    traceSummary.value = response.data?.trace || response.data || null
  } catch (e) {
    console.error('Correlated trace fetch failed:', e)
    traceSummary.value = null
  } finally {
    tracesLoading.value = false
  }
}

async function fetchExceptions() {
  if (props.traceIds.length === 0) return
  exceptionsLoading.value = true
  try {
    const response = await axios.get(`/api/projects/${props.projectId}/events`, {
      params: { event_type: 'errors', trace_id: props.traceIds[0], time_range: '30d' }
    })
    const data = Array.isArray(response.data) ? response.data : []
    exceptions.value = data.filter(e => e.type === 'error')
  } catch (e) {
    console.error('Correlated exceptions fetch failed:', e)
    exceptions.value = []
  } finally {
    exceptionsLoading.value = false
  }
}

async function fetchServiceMetrics() {
  if (!props.serviceName) return
  serviceMetricsLoading.value = true
  try {
    const response = await axios.get(`/api/projects/${props.projectId}/metrics/names`, {
      params: { limit: 50 }
    })
    const all = response.data?.metrics || response.data || []
    metricNames.value = Array.isArray(all)
      ? all.map(m => m.name || m).filter(Boolean)
      : []
  } catch (e) {
    console.error('Service metrics fetch failed:', e)
    metricNames.value = []
  } finally {
    serviceMetricsLoading.value = false
  }
}

// Resize logic
let resizing = false
function startResize(e) {
  resizing = true
  const startY = e.clientY
  const startHeight = panelHeight.value
  const maxH = window.innerHeight * MAX_HEIGHT_VH

  function onMove(ev) {
    if (!resizing) return
    const delta = startY - ev.clientY
    panelHeight.value = Math.max(MIN_HEIGHT, Math.min(startHeight + delta, maxH))
  }
  function onUp() {
    resizing = false
    localStorage.setItem(STORAGE_KEY, String(panelHeight.value))
    document.removeEventListener('mousemove', onMove)
    document.removeEventListener('mouseup', onUp)
  }
  document.addEventListener('mousemove', onMove)
  document.addEventListener('mouseup', onUp)
}

// Formatting helpers
function fmtTime(ts) {
  if (!ts) return ''
  try { return format(new Date(ts), 'HH:mm:ss.SSS') } catch { return ts }
}

function fmtDuration(ns) {
  if (!ns) return '—'
  const ms = ns / 1_000_000
  if (ms < 1000) return `${ms.toFixed(0)}ms`
  if (ms < 60000) return `${(ms / 1000).toFixed(2)}s`
  return `${(ms / 60000).toFixed(2)}m`
}

function levelBadge(level) {
  const l = (level || 'info').toLowerCase()
  const base = 'inline-block px-1.5 py-0.5 text-[10px] font-semibold rounded mr-1.5'
  if (l === 'error' || l === 'fatal') return `${base} bg-red-100 text-red-800`
  if (l === 'warning' || l === 'warn') return `${base} bg-yellow-100 text-yellow-800`
  if (l === 'info') return `${base} bg-blue-100 text-blue-800`
  return `${base} bg-gray-100 text-gray-700`
}
</script>

<style scoped>
.correlated-panel {
  @apply fixed bottom-0 left-60 max-lg:left-0 right-0 z-40
    bg-white dark:bg-gray-800 border-t border-gray-200 dark:border-gray-700
    flex flex-col shadow-[0_-4px_12px_rgba(0,0,0,0.08)];
  transition: height 0.15s ease;
}
.correlated-panel.collapsed {
  @apply overflow-hidden;
}

.drag-handle {
  @apply absolute -top-1 left-0 right-0 h-2 cursor-row-resize flex items-center justify-center;
}
.drag-indicator {
  @apply w-10 h-1 rounded-full bg-gray-300 dark:bg-gray-600;
}

.panel-header {
  @apply flex items-center justify-between px-4 py-1.5 border-b border-gray-100 dark:border-gray-700 flex-shrink-0;
}

.collapse-btn {
  @apply p-1 rounded hover:bg-gray-100 dark:hover:bg-gray-700 text-gray-500;
}

.tab-btn {
  @apply px-2.5 py-1 text-xs font-medium rounded transition-colors;
}
.tab-btn-active {
  @apply bg-primary-100 text-primary-700 dark:bg-primary-900/30 dark:text-primary-300;
}
.tab-btn-inactive {
  @apply text-gray-500 hover:text-gray-700 hover:bg-gray-100 dark:hover:bg-gray-700;
}
.tab-count {
  @apply ml-1 text-[10px] text-gray-400;
}

.panel-body {
  @apply flex-1 overflow-y-auto;
}

.tab-content {
  @apply p-3;
}
.tab-loading {
  @apply text-center py-6 text-sm text-gray-500;
}
.tab-empty {
  @apply text-center py-6 text-sm text-gray-400;
}

/* Logs */
.log-rows {
  @apply space-y-0.5;
}
.log-row {
  @apply flex items-center gap-2 px-2 py-1.5 text-xs font-mono rounded cursor-pointer
    hover:bg-gray-50 dark:hover:bg-gray-700/50 transition-colors;
}
.log-time {
  @apply text-gray-400 flex-shrink-0;
}
.log-service {
  @apply text-gray-500 truncate max-w-[120px] flex-shrink-0;
}
.log-msg {
  @apply text-gray-800 dark:text-gray-200 truncate;
}

/* Traces */
.trace-summary {
  @apply p-2;
}
.summary-stat {
  @apply bg-gray-50 dark:bg-gray-900 rounded-lg p-2;
}
.summary-label {
  @apply text-[10px] text-gray-500 uppercase tracking-wider mb-0.5;
}
.summary-value {
  @apply text-sm font-semibold text-gray-900 dark:text-gray-100;
}

/* Exceptions */
.exception-rows {
  @apply space-y-0.5;
}
.exception-row {
  @apply flex items-center gap-2 px-2 py-1.5 text-xs rounded cursor-pointer
    hover:bg-gray-50 dark:hover:bg-gray-700/50 transition-colors;
}
.exception-type {
  @apply font-semibold text-red-700 dark:text-red-400 flex-shrink-0;
}
.exception-msg {
  @apply text-gray-700 dark:text-gray-300 truncate;
}
.exception-time {
  @apply text-gray-400 ml-auto flex-shrink-0;
}

/* Metrics */
.metrics-content {
  @apply p-2;
}
.metric-names-list {
  @apply space-y-0.5;
}
.metric-name-row {
  @apply flex items-center px-2 py-1.5 text-xs font-mono rounded cursor-pointer
    hover:bg-gray-50 dark:hover:bg-gray-700/50 transition-colors;
}
.metric-name-label {
  @apply text-gray-800 dark:text-gray-200;
}
</style>
