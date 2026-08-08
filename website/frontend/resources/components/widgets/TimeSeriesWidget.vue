<template>
  <div class="time-series-widget h-full flex flex-col">
    <!-- Loading State -->
    <div v-if="loading" class="flex-1 flex items-center justify-center">
      <div class="spinner w-8 h-8 border-2 border-primary-500 border-t-transparent rounded-full animate-spin"></div>
    </div>
    
    <!-- Error State -->
    <div v-else-if="error" class="flex-1 flex items-center justify-center">
      <div class="text-center p-4">
        <div class="text-danger-400 mb-2">
          <svg class="w-8 h-8 mx-auto" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
          </svg>
        </div>
        <p class="text-sm text-gray-400">{{ error }}</p>
      </div>
    </div>
    
    <!-- Empty State -->
    <div v-else-if="!chartData || (Array.isArray(chartData) && chartData.length === 0) || (chartData.timestamps && chartData.timestamps.length === 0)" class="flex-1 flex items-center justify-center">
      <div class="text-center text-gray-400">
        <svg class="w-10 h-10 mx-auto mb-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
        </svg>
        <p class="text-sm">No data available</p>
      </div>
    </div>
    
    <!-- Chart -->
    <div v-else class="flex-1 min-h-0">
      <UPlotChart
        :data="chartData"
        :unit="widgetUnit"
        :show-legend="showLegend"
        :y-scale="widgetYScale"
        :stacking="widgetStacking"
        class="h-full"
      />
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import { useWidgetQuery, parseTimeRange } from '@/composables/useWidgetQuery'
import UPlotChart from '@/components/charts/UPlotChart.vue'

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

const { loading, error, executeQuery } = useWidgetQuery()
const chartData = ref([])

const widgetUnit = computed(() => props.config.unit || props.config.query?.unit)
const widgetYScale = computed(() => props.config.query?.yScale || null)
const widgetStacking = computed(() => props.config.query?.stacking || null)

const showLegend = computed(() => {
  if (props.config.query?.hideLegend === true) return false
  if (props.config.displayMode === 'single') return false
  return true
})

// Transform query results for UPlotChart.
// Returns { timestamps: number[], datasets: [{label, data}] } with raw epoch seconds.
const transformData = (result) => {
  if (!result || !result.data || result.data.length === 0) {
    return []
  }

  const columns = result.columns
  const timeCols = new Set(['time_bucket', 'time', 'timestamp', 'unix_milli'])
  const skipCols = new Set(['project_id', 'fingerprint'])
  const timeCol = columns.find(c => timeCols.has(c))
  const dataColumns = columns.filter(c => !timeCols.has(c) && !skipCols.has(c) && !c.startsWith('lbl_'))
  const isNanos = props.config.unit === 'ns'
  const valueCol = dataColumns.includes('value') ? 'value' : null

  const parseVal = (raw) => {
    let y = (raw === null || raw === undefined) ? null : parseFloat(raw)
    if (y !== null && isNaN(y)) y = null
    if (isNanos && y !== null && y > 0) y = Math.round(y / 1e6 * 100) / 100
    return y
  }

  // Multi-series: group by lbl__series if present AND populated
  const seriesCol = columns.find(c => c === 'lbl__series')
  const hasSeriesValues = seriesCol && result.data.some(row => row[seriesCol] && row[seriesCol].length > 0)
  if (hasSeriesValues) {
    const seriesMap = new Map()
    const allTimes = new Set()
    for (const row of result.data) {
      const series = row[seriesCol] || 'value'
      const ts = toEpochSec(row[timeCol], timeCol)
      allTimes.add(ts)
      if (!seriesMap.has(series)) seriesMap.set(series, new Map())
      const raw = valueCol ? row[valueCol] : findNumericValue(row, dataColumns)
      seriesMap.get(series).set(ts, parseVal(raw))
    }
    const timestamps = [...allTimes].sort((a, b) => a - b)
    const datasets = [...seriesMap.entries()].map(([name, points]) => ({
      label: name,
      data: timestamps.map(t => points.get(t) ?? null),
    }))
    return { timestamps, datasets }
  }

  // Also support grouping by any lbl_* column for PromQL grouped results
  const lblCols = columns.filter(c => c.startsWith('lbl_') && c !== 'lbl__series')
  if (lblCols.length > 0 && timeCol) {
    const seriesMap = new Map()
    const allTimes = new Set()
    for (const row of result.data) {
      const seriesKey = lblCols.map(c => row[c] || '').join(' / ')
      const ts = toEpochSec(row[timeCol], timeCol)
      allTimes.add(ts)
      if (!seriesMap.has(seriesKey)) seriesMap.set(seriesKey, new Map())
      const raw = valueCol ? row[valueCol] : findNumericValue(row, dataColumns)
      seriesMap.get(seriesKey).set(ts, parseVal(raw))
    }
    if (seriesMap.size > 1) {
      const timestamps = [...allTimes].sort((a, b) => a - b)
      const datasets = [...seriesMap.entries()].map(([name, points]) => ({
        label: name,
        data: timestamps.map(t => points.get(t) ?? null),
      }))
      return { timestamps, datasets }
    }
  }

  // Single series -- sort by timestamp (the backend should return sorted data,
  // but aggregation plans may not guarantee order).
  const pairs = result.data.map(row => ({
    ts: toEpochSec(row[timeCol], timeCol),
    val: parseVal(valueCol ? row[valueCol] : findNumericValue(row, dataColumns)),
  }))
  pairs.sort((a, b) => a.ts - b.ts)
  return { timestamps: pairs.map(p => p.ts), datasets: [{ label: 'Value', data: pairs.map(p => p.val) }] }
}

const findNumericValue = (row, columns) => {
  for (const col of columns) {
    const val = row[col]
    if (val === null || val === undefined) continue
    if (typeof val === 'number') return val
    if (typeof val === 'string' && !isNaN(parseFloat(val))) return parseFloat(val)
  }
  return null
}

const toEpochSec = (value, columnName) => {
  if (!value && value !== 0) return 0
  if (typeof value === 'number') {
    if (columnName === 'unix_milli') return Math.floor(value / 1000)
    if (value > 1e12) return Math.floor(value / 1000)
    if (value > 1e9) return Math.floor(value)
    return value
  }
  const d = new Date(value)
  return isNaN(d.getTime()) ? 0 : Math.floor(d.getTime() / 1000)
}

const fetchData = async () => {
  if (!props.config.query) {
    return
  }
  
  try {
    const range = parseTimeRange(props.timeRange)
    const result = await executeQuery(
      props.projectId,
      props.config.query,
      range,
      props.variables
    )
    
    chartData.value = transformData(result)
  } catch (err) {
    console.error('Widget query failed:', err)
  }
}

onMounted(fetchData)

watch([() => props.timeRange, () => props.variables], fetchData, { deep: true })
</script>
