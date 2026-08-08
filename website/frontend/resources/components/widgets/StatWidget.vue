<template>
  <div class="stat-widget">
    <div v-if="loading" class="flex items-center justify-center h-full">
      <div class="spinner w-6 h-6 border-2 border-primary-600 border-t-transparent rounded-full"></div>
    </div>
    <div v-else-if="error" class="text-center text-danger-400 text-sm">
      {{ error }}
    </div>
    <div v-else-if="!hasData" class="flex items-center justify-center h-full text-gray-400 text-sm">
      No data
    </div>
    <!-- Gauge mode -->
    <GaugeDisplay
      v-else-if="isGauge"
      :value="value"
      :min="gaugeMin"
      :max="gaugeMax"
      :unit="widgetUnit"
      :thresholds="widgetThresholds"
    />
    <!-- Bar gauge mode -->
    <div v-else-if="isBarGauge" class="bargauge-content">
      <div class="bargauge-label">{{ config.label || '' }}</div>
      <div class="bargauge-track">
        <div
          class="bargauge-fill"
          :style="{
            width: barPercent + '%',
            backgroundColor: thresholdColor || '#6366F1',
          }"
        ></div>
        <span class="bargauge-value">{{ formattedValue }}</span>
      </div>
    </div>
    <!-- Standard stat display -->
    <div v-else class="stat-content" :style="statBackground">
      <div class="stat-value" :style="{ color: thresholdColor }">{{ formattedValue }}</div>
      <div v-if="config.label" class="stat-label">{{ config.label }}</div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import { useWidgetQuery, parseTimeRange } from '@/composables/useWidgetQuery'
import { formatGrafanaUnit } from '@/utils/widgetTransforms'
import GaugeDisplay from './GaugeDisplay.vue'
import axios from 'axios'

const props = defineProps({
  config: {
    type: Object,
    required: true,
  },
  projectId: {
    type: String,
    required: true,
  },
  timeRange: {
    type: String,
    default: '1h',
  },
  variables: {
    type: Object,
    default: () => ({}),
  },
})

const { executeQuery } = useWidgetQuery()
const value = ref(0)
const hasData = ref(false)
const loading = ref(true)
const error = ref(null)

const widgetUnit = computed(() => props.config.unit || props.config.query?.unit)

const formattedValue = computed(() => {
  const unit = widgetUnit.value
  // Use Grafana unit formatter when a unit is specified
  if (unit) {
    return formatGrafanaUnit(value.value, unit)
  }
  if (props.config.format === 'currency') {
    return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(value.value)
  }
  if (props.config.format === 'percentage') {
    return `${value.value.toFixed(2)}%`
  }
  if (props.config.format === 'number') {
    return new Intl.NumberFormat('en-US').format(value.value)
  }
  return formatGrafanaUnit(value.value, 'short')
})

const isGauge = computed(() => props.config.query?.stat_type === 'gauge')
const isBarGauge = computed(() => props.config.query?.stat_type === 'bargauge')

const gaugeMin = computed(() => props.config.query?.min ?? 0)
const gaugeMax = computed(() => props.config.query?.max ?? 100)

const barPercent = computed(() => {
  const min = gaugeMin.value
  const max = gaugeMax.value
  const range = max - min
  if (range <= 0) return 0
  return Math.max(0, Math.min(100, ((value.value - min) / range) * 100))
})

const widgetThresholds = computed(() => props.config.query?.thresholds || [])

const thresholdColor = computed(() => {
  const thresholds = widgetThresholds.value
  if (!thresholds || thresholds.length === 0) return undefined
  const sorted = [...thresholds].sort((a, b) => a.value - b.value)
  let color = sorted[0]?.color || undefined
  for (const t of sorted) {
    if (value.value >= t.value) color = resolveColor(t.color)
  }
  return color
})

const statBackground = computed(() => {
  const color = thresholdColor.value
  if (!color) return {}
  // Subtle background tint matching the threshold color
  return { backgroundColor: color + '15' }
})

function resolveColor(c) {
  const map = {
    'green': '#22c55e', 'red': '#ef4444', 'orange': '#f97316',
    'yellow': '#eab308', 'blue': '#3b82f6', 'purple': '#a855f7',
    'super-light-green': '#73BF69', 'light-green': '#56c05a',
    'semi-dark-green': '#37872D', 'dark-green': '#1a7c11',
  }
  return map[c] || c
}

const fetchStat = async () => {
  loading.value = true
  error.value = null

  try {
    if (props.config.query) {
      const range = parseTimeRange(props.timeRange)
      const result = await executeQuery(
        props.projectId,
        props.config.query,
        range,
        props.variables
      )
      
      if (result.data && result.data.length > 0) {
        const skipCols = new Set(['project_id', 'unix_milli', 'fingerprint'])

        const extractValue = (row) => {
          if ('value' in row && typeof row.value === 'number') return row.value
          for (const col of result.columns) {
            if (skipCols.has(col) || col.startsWith('lbl_')) continue
            const val = row[col]
            if (typeof val === 'number') return val
            if (typeof val === 'string' && !isNaN(parseFloat(val))) return parseFloat(val)
          }
          return null
        }

        for (let i = result.data.length - 1; i >= 0; i--) {
          const v = extractValue(result.data[i])
          if (v !== null && !isNaN(v) && v !== 0) {
            value.value = v
            hasData.value = true
            return
          }
        }
        const lastVal = extractValue(result.data[result.data.length - 1])
        if (lastVal !== null && !isNaN(lastVal)) {
          value.value = lastVal
          hasData.value = true
          return
        }
      }
      value.value = 0
      hasData.value = false
      return
    }
    
    if (props.config.query_type === 'api') {
      const response = await axios.get(props.config.api_endpoint)
      value.value = response.data[props.config.field || 'count'] || 0
      hasData.value = true
    } else {
      const response = await axios.get(`/api/projects/${props.projectId}/stats`)
      const stats = response.data
      
      if (props.config.stat_type === 'total_exceptions') {
        value.value = stats.total_exceptions || 0
      } else if (props.config.stat_type === 'unresolved_exceptions') {
        value.value = stats.unresolved_exceptions || 0
      } else if (props.config.stat_type === 'resolved_exceptions') {
        value.value = stats.resolved_exceptions || 0
      } else {
        value.value = stats.total_exceptions || 0
      }
      hasData.value = true
    }
  } catch (err) {
    console.error('Failed to fetch stat:', err)
    error.value = err.response?.data?.message || err.message || 'Failed to load stat'
  } finally {
    loading.value = false
  }
}

onMounted(() => {
  fetchStat()
})

watch([() => props.timeRange, () => props.variables], () => {
  fetchStat()
}, { deep: true })
</script>

<style scoped>
.stat-widget {
  @apply h-full flex items-center justify-center;
}

.stat-content {
  @apply text-center;
}

.stat-value {
  @apply text-3xl font-bold;
  color: inherit;
}

.stat-label {
  @apply text-sm text-gray-500 dark:text-gray-400 mt-2;
}

.bargauge-content {
  @apply w-full px-4 flex flex-col justify-center h-full;
}

.bargauge-label {
  @apply text-xs text-gray-500 mb-1 truncate;
}

.bargauge-track {
  @apply relative w-full h-8 bg-gray-200 rounded overflow-hidden flex items-center;
}

.bargauge-fill {
  @apply absolute inset-y-0 left-0 rounded transition-all;
}

.bargauge-value {
  @apply relative z-10 pl-2 text-sm font-semibold text-gray-800;
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


