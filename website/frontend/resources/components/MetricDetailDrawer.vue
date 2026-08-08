<template>
  <Transition name="drawer">
    <div v-if="isOpen && metric" class="metric-detail-drawer">
      <!-- Header -->
      <div class="drawer-header">
        <div class="flex-1 min-w-0">
          <h3 class="drawer-title">{{ metric.name }}</h3>
          <div class="flex items-center gap-2 mt-1">
            <span :class="['type-badge', getTypeBadgeClass(metric.metric_type)]">
              {{ metric.metric_type }}
            </span>
            <span v-if="metric.unit" class="unit-badge">{{ metric.unit }}</span>
          </div>
        </div>
        <button @click="$emit('close')" class="close-btn">
          <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      </div>

      <!-- Content -->
      <div class="drawer-content">
        <!-- Description -->
        <div v-if="metric.description" class="section">
          <h4 class="section-title">Description</h4>
          <p class="text-sm text-gray-600 dark:text-gray-400">{{ metric.description }}</p>
        </div>

        <!-- Stats -->
        <div class="section">
          <h4 class="section-title">Statistics</h4>
          <div class="stats-grid">
            <div class="stat-item">
              <span class="stat-label">Series Count</span>
              <span class="stat-value">{{ formatNumber(metric.series_count) }}</span>
            </div>
            <div class="stat-item">
              <span class="stat-label">Data Points/min</span>
              <span class="stat-value">{{ formatNumber(metric.data_points_per_min) }}</span>
            </div>
            <div class="stat-item">
              <span class="stat-label">Labels</span>
              <span class="stat-value">{{ metric.label_keys?.length || 0 }}</span>
            </div>
            <div class="stat-item">
              <span class="stat-label">Last Seen</span>
              <span class="stat-value">{{ formatRelativeTime(metric.last_seen) }}</span>
            </div>
          </div>
        </div>

        <!-- Chart -->
        <div class="section">
          <h4 class="section-title">Time Series</h4>
          <div class="chart-container">
            <div v-if="loadingChart" class="chart-loading">
              <div class="spinner"></div>
              <span>Loading chart...</span>
            </div>
            <div v-else-if="chartData.length > 0" class="chart-wrapper">
              <svg :width="chartWidth" :height="chartHeight" class="metric-chart">
                <!-- Y-axis grid lines -->
                <g class="grid-lines">
                  <line
                    v-for="tick in yTicks"
                    :key="tick.value"
                    x1="40"
                    :y1="tick.y"
                    :x2="chartWidth"
                    :y2="tick.y"
                    stroke="currentColor"
                    class="text-gray-200 dark:text-gray-700"
                  />
                  <text
                    v-for="tick in yTicks"
                    :key="'label-' + tick.value"
                    x="35"
                    :y="tick.y + 4"
                    class="axis-label"
                  >
                    {{ formatChartValue(tick.value) }}
                  </text>
                </g>

                <!-- Chart area -->
                <path
                  :d="areaPath"
                  fill="url(#metric-gradient)"
                />
                <path
                  :d="linePath"
                  stroke="#3B82F6"
                  stroke-width="2"
                  fill="none"
                  stroke-linecap="round"
                  stroke-linejoin="round"
                />

                <!-- Exemplar markers -->
                <g v-if="exemplarPoints.length > 0" class="exemplar-markers">
                  <g
                    v-for="(ex, i) in exemplarPoints"
                    :key="'ex-' + i"
                    class="exemplar-marker"
                    :transform="`translate(${ex.x}, ${ex.y})`"
                    @mouseenter="hoveredExemplar = ex"
                    @mouseleave="hoveredExemplar = null"
                    @click="navigateToTrace(ex)"
                  >
                    <rect
                      x="-5" y="-5" width="10" height="10"
                      :transform="`rotate(45)`"
                      fill="#F59E0B"
                      fill-opacity="0.85"
                      stroke="#D97706"
                      stroke-width="1"
                      class="cursor-pointer"
                    />
                  </g>
                </g>

                <!-- Exemplar tooltip -->
                <foreignObject
                  v-if="hoveredExemplar"
                  :x="Math.min(hoveredExemplar.x + 10, chartWidth - 180)"
                  :y="Math.max(hoveredExemplar.y - 70, 0)"
                  width="170"
                  height="70"
                >
                  <div class="exemplar-tooltip">
                    <div class="exemplar-tooltip-trace">trace: {{ hoveredExemplar.trace_id.slice(0, 16) }}…</div>
                    <div class="exemplar-tooltip-value">value: {{ formatChartValue(hoveredExemplar.value) }}</div>
                    <div class="exemplar-tooltip-hint">Click to view trace</div>
                  </div>
                </foreignObject>

                <!-- Gradient definition -->
                <defs>
                  <linearGradient id="metric-gradient" x1="0%" y1="0%" x2="0%" y2="100%">
                    <stop offset="0%" stop-color="#3B82F6" stop-opacity="0.3" />
                    <stop offset="100%" stop-color="#3B82F6" stop-opacity="0" />
                  </linearGradient>
                </defs>
              </svg>
            </div>
            <div v-else class="no-data">
              No data available for this time range
            </div>
          </div>
        </div>

        <!-- Labels -->
        <div v-if="metric.label_keys?.length > 0" class="section">
          <h4 class="section-title">Labels</h4>
          <div class="labels-grid">
            <div
              v-for="label in metric.label_keys"
              :key="label"
              class="label-item"
            >
              <span class="label-key">{{ label }}</span>
              <span class="label-count" v-if="labelValues[label]">
                {{ labelValues[label].length }} values
              </span>
            </div>
          </div>
        </div>

        <!-- Sample Label Values -->
        <div v-if="Object.keys(labelValues).length > 0" class="section">
          <h4 class="section-title">Sample Values</h4>
          <div class="sample-values">
            <div
              v-for="(values, key) in labelValues"
              :key="key"
              class="value-group"
            >
              <span class="value-key">{{ key }}:</span>
              <div class="value-list">
                <span
                  v-for="value in values.slice(0, 5)"
                  :key="value"
                  class="value-tag"
                >
                  {{ value }}
                </span>
                <span v-if="values.length > 5" class="value-more">
                  +{{ values.length - 5 }} more
                </span>
              </div>
            </div>
          </div>
        </div>

        <!-- Related Metrics -->
        <div v-if="relatedMetrics.length > 0" class="section">
          <h4 class="section-title">Related Metrics</h4>
          <div class="related-list">
            <button
              v-for="related in relatedMetrics"
              :key="related.name"
              class="related-item"
              @click="$emit('close'); $emit('select-metric', related)"
            >
              <span class="related-name">{{ related.name }}</span>
              <span class="related-type">{{ related.metric_type }}</span>
            </button>
          </div>
        </div>
      </div>

      <!-- Footer -->
      <div class="drawer-footer">
        <button @click="queryMetric" class="action-btn primary">
          <svg class="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
          </svg>
          Query in Explorer
        </button>
        <button @click="addToDashboard" class="action-btn">
          <svg class="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" />
          </svg>
          Add to Dashboard
        </button>
      </div>
    </div>
  </Transition>
</template>

<script setup>
import { ref, computed, watch, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { formatDistanceToNow } from 'date-fns'
import axios from 'axios'

const props = defineProps({
  isOpen: {
    type: Boolean,
    default: false,
  },
  metric: {
    type: Object,
    default: null,
  },
  projectId: {
    type: String,
    required: true,
  },
  timeRange: {
    type: String,
    default: '1h',
  },
})

const emit = defineEmits(['close', 'select-metric'])
const router = useRouter()

const chartData = ref([])
const exemplarData = ref([])
const hoveredExemplar = ref(null)
const labelValues = ref({})
const relatedMetrics = ref([])
const loadingChart = ref(false)

const chartWidth = 360
const chartHeight = 200
const chartPadding = { top: 10, right: 10, bottom: 30, left: 50 }

// Fetch metric details when opened
watch([() => props.isOpen, () => props.metric], async ([isOpen, metric]) => {
  if (isOpen && metric) {
    await Promise.all([
      fetchChartData(),
      fetchLabelValues(),
      fetchRelatedMetrics(),
    ])
  }
}, { immediate: true })

const fetchChartData = async () => {
  if (!props.metric) return
  
  loadingChart.value = true
  try {
    const response = await axios.get(`/api/projects/${props.projectId}/metrics/${encodeURIComponent(props.metric.name)}/timeseries`, {
      params: { time_range: props.timeRange, include_exemplars: true }
    })
    chartData.value = response.data.data || []
    exemplarData.value = response.data.exemplars || []
  } catch (error) {
    console.error('Failed to fetch chart data:', error)
    chartData.value = []
    exemplarData.value = []
  } finally {
    loadingChart.value = false
  }
}

const fetchLabelValues = async () => {
  if (!props.metric) return
  
  try {
    const response = await axios.get(`/api/projects/${props.projectId}/metrics/${encodeURIComponent(props.metric.name)}/labels`, {
      params: { limit: 100 }
    })
    labelValues.value = response.data.label_values || {}
  } catch (error) {
    console.error('Failed to fetch label values:', error)
    labelValues.value = {}
  }
}

const fetchRelatedMetrics = async () => {
  if (!props.metric) return
  
  try {
    const response = await axios.get(`/api/projects/${props.projectId}/metrics/names`, {
      params: { prefix: props.metric.name.split('_').slice(0, 2).join('_'), limit: 5 }
    })
    relatedMetrics.value = (response.data.metrics || []).filter(m => m.name !== props.metric.name)
  } catch (error) {
    console.error('Failed to fetch related metrics:', error)
    relatedMetrics.value = []
  }
}

// Chart calculations
const yTicks = computed(() => {
  if (chartData.value.length === 0) return []
  
  const values = chartData.value.map(d => d.value || 0)
  const min = Math.min(...values)
  const max = Math.max(...values)
  const range = max - min || 1
  
  const ticks = []
  const tickCount = 4
  for (let i = 0; i <= tickCount; i++) {
    const value = min + (range * i / tickCount)
    const y = chartPadding.top + (1 - i / tickCount) * (chartHeight - chartPadding.top - chartPadding.bottom)
    ticks.push({ value, y })
  }
  
  return ticks
})

const linePath = computed(() => {
  if (chartData.value.length < 2) return ''
  
  const values = chartData.value.map(d => d.value || 0)
  const min = Math.min(...values)
  const max = Math.max(...values)
  const range = max - min || 1
  
  const points = chartData.value.map((d, i) => {
    const x = chartPadding.left + (i / (chartData.value.length - 1)) * (chartWidth - chartPadding.left - chartPadding.right)
    const y = chartPadding.top + (1 - (d.value - min) / range) * (chartHeight - chartPadding.top - chartPadding.bottom)
    return { x, y }
  })
  
  return points.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x} ${p.y}`).join(' ')
})

const areaPath = computed(() => {
  if (chartData.value.length < 2) return ''
  
  const values = chartData.value.map(d => d.value || 0)
  const min = Math.min(...values)
  const max = Math.max(...values)
  const range = max - min || 1
  
  const points = chartData.value.map((d, i) => {
    const x = chartPadding.left + (i / (chartData.value.length - 1)) * (chartWidth - chartPadding.left - chartPadding.right)
    const y = chartPadding.top + (1 - (d.value - min) / range) * (chartHeight - chartPadding.top - chartPadding.bottom)
    return { x, y }
  })
  
  const bottomY = chartHeight - chartPadding.bottom
  const linePart = points.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x} ${p.y}`).join(' ')
  
  return `${linePart} L ${points[points.length - 1].x} ${bottomY} L ${points[0].x} ${bottomY} Z`
})

const exemplarPoints = computed(() => {
  if (chartData.value.length < 2 || exemplarData.value.length === 0) return []
  
  const values = chartData.value.map(d => d.value || 0)
  const min = Math.min(...values)
  const max = Math.max(...values)
  const range = max - min || 1
  
  const tsMin = chartData.value[0].timestamp_ms
  const tsMax = chartData.value[chartData.value.length - 1].timestamp_ms
  const tsRange = tsMax - tsMin || 1
  
  return exemplarData.value
    .filter(ex => ex.timestamp_ms >= tsMin && ex.timestamp_ms <= tsMax)
    .map(ex => ({
      x: chartPadding.left + ((ex.timestamp_ms - tsMin) / tsRange) * (chartWidth - chartPadding.left - chartPadding.right),
      y: chartPadding.top + (1 - (ex.value - min) / range) * (chartHeight - chartPadding.top - chartPadding.bottom),
      trace_id: ex.trace_id,
      span_id: ex.span_id,
      value: ex.value,
      filtered_attributes: ex.filtered_attributes || {},
    }))
})

const navigateToTrace = (ex) => {
  if (ex.trace_id) {
    router.push(`/p/${props.projectId}/traces/${ex.trace_id}`)
    emit('close')
  }
}

// Formatting
const formatNumber = (num) => {
  if (num === undefined || num === null) return '0'
  if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`
  if (num >= 1000) return `${(num / 1000).toFixed(1)}K`
  return num.toString()
}

const formatChartValue = (num) => {
  if (num >= 1000000) return `${(num / 1000000).toFixed(0)}M`
  if (num >= 1000) return `${(num / 1000).toFixed(0)}K`
  return num.toFixed(1)
}

const formatRelativeTime = (dateString) => {
  if (!dateString) return 'Unknown'
  try {
    return formatDistanceToNow(new Date(dateString), { addSuffix: true })
  } catch {
    return dateString
  }
}

const getTypeBadgeClass = (type) => {
  const classes = {
    counter: 'bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300',
    gauge: 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300',
    histogram: 'bg-purple-100 text-purple-800 dark:bg-purple-900/30 dark:text-purple-300',
    summary: 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-300',
  }
  return classes[type] || 'bg-gray-100 text-gray-800 dark:bg-gray-900/30 dark:text-gray-300'
}

// Actions
const queryMetric = () => {
  router.push({
    path: `/p/${props.projectId}/dashboards/new`,
    query: { metric: props.metric?.name }
  })
  emit('close')
}

const addToDashboard = () => {
  // This would open a dashboard selector modal
  console.log('Add to dashboard:', props.metric?.name)
}
</script>

<style scoped>
.metric-detail-drawer {
  @apply fixed right-0 top-0 h-full w-[400px] max-w-full bg-white dark:bg-gray-800 border-l border-gray-200 dark:border-gray-700 shadow-xl z-50 flex flex-col;
}

.drawer-header {
  @apply flex items-start justify-between px-4 py-4 border-b border-gray-200 dark:border-gray-700;
}

.drawer-title {
  @apply text-lg font-semibold text-gray-900 dark:text-gray-100 font-mono truncate;
}

.type-badge {
  @apply px-2 py-0.5 text-xs font-medium rounded;
}

.unit-badge {
  @apply px-2 py-0.5 text-xs bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400 rounded;
}

.close-btn {
  @apply p-2 text-gray-400 hover:text-gray-600 dark:hover:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700 rounded-md transition-colors;
}

.drawer-content {
  @apply flex-1 overflow-y-auto p-4 space-y-6;
}

.section {
  @apply space-y-3;
}

.section-title {
  @apply text-sm font-semibold text-gray-900 dark:text-gray-100;
}

.stats-grid {
  @apply grid grid-cols-2 gap-3;
}

.stat-item {
  @apply flex flex-col;
}

.stat-label {
  @apply text-xs text-gray-500 dark:text-gray-400;
}

.stat-value {
  @apply text-sm font-semibold text-gray-900 dark:text-gray-100;
}

.chart-container {
  @apply bg-gray-50 dark:bg-gray-900 rounded-lg p-3;
}

.chart-loading {
  @apply flex items-center justify-center py-8 text-gray-500 dark:text-gray-400 text-sm;
}

.spinner {
  @apply w-5 h-5 border-2 border-primary-600 border-t-transparent rounded-full mr-2;
  animation: spin 0.6s linear infinite;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

.chart-wrapper {
  @apply overflow-x-auto;
}

.metric-chart {
  @apply block;
}

.axis-label {
  @apply text-[10px] fill-gray-500 dark:fill-gray-400;
  text-anchor: end;
}

.no-data {
  @apply py-8 text-center text-gray-500 dark:text-gray-400 text-sm;
}

.exemplar-marker {
  transition: transform 0.15s ease;
}
.exemplar-marker:hover {
  transform: scale(1.4);
}

.exemplar-tooltip {
  @apply bg-gray-900 text-white text-xs rounded px-2 py-1.5 shadow-lg;
  pointer-events: none;
}
.exemplar-tooltip-trace {
  @apply font-mono text-amber-300 truncate;
}
.exemplar-tooltip-value {
  @apply text-gray-300;
}
.exemplar-tooltip-hint {
  @apply text-gray-500 text-[10px] mt-0.5;
}

.labels-grid {
  @apply grid grid-cols-2 gap-2;
}

.label-item {
  @apply flex items-center justify-between px-2 py-1.5 bg-gray-50 dark:bg-gray-900 rounded;
}

.label-key {
  @apply text-xs font-mono text-gray-900 dark:text-gray-100;
}

.label-count {
  @apply text-xs text-gray-500 dark:text-gray-400;
}

.sample-values {
  @apply space-y-2;
}

.value-group {
  @apply flex items-start gap-2;
}

.value-key {
  @apply text-xs font-mono text-gray-600 dark:text-gray-400 flex-shrink-0;
}

.value-list {
  @apply flex flex-wrap gap-1;
}

.value-tag {
  @apply px-1.5 py-0.5 text-xs bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded;
}

.value-more {
  @apply text-xs text-gray-500 dark:text-gray-400;
}

.related-list {
  @apply space-y-1;
}

.related-item {
  @apply w-full flex items-center justify-between px-3 py-2 text-left hover:bg-gray-50 dark:hover:bg-gray-700 rounded-md transition-colors;
}

.related-name {
  @apply text-sm text-gray-900 dark:text-gray-100 font-mono truncate;
}

.related-type {
  @apply text-xs text-gray-500 dark:text-gray-400;
}

.drawer-footer {
  @apply flex items-center gap-2 px-4 py-3 border-t border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-900;
}

.action-btn {
  @apply flex items-center px-3 py-2 text-sm font-medium text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-100 hover:bg-gray-100 dark:hover:bg-gray-800 rounded-md transition-colors;
}

.action-btn.primary {
  @apply text-white bg-primary-600 hover:bg-primary-700;
}

/* Drawer transition */
.drawer-enter-active,
.drawer-leave-active {
  transition: transform 0.3s ease;
}

.drawer-enter-from,
.drawer-leave-to {
  transform: translateX(100%);
}
</style>
