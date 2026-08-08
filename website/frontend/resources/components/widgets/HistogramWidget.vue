<template>
  <div class="histogram-widget h-full flex flex-col">
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
    <div v-else-if="!chartData || !chartData.labels || chartData.labels.length === 0" class="flex-1 flex items-center justify-center">
      <div class="text-center text-gray-400">
        <svg class="w-10 h-10 mx-auto mb-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
        </svg>
        <p class="text-sm">No data available</p>
      </div>
    </div>
    
    <!-- Chart -->
    <div v-else class="flex-1 min-h-0">
      <canvas ref="chartCanvas" class="h-full w-full"></canvas>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onBeforeUnmount, watch, nextTick } from 'vue'
import { useWidgetQuery, parseTimeRange } from '@/composables/useWidgetQuery'
import {
  Chart,
  CategoryScale,
  LinearScale,
  BarController,
  BarElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js'

Chart.register(CategoryScale, LinearScale, BarController, BarElement, Title, Tooltip, Legend)

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
const chartData = ref(null)
const chartCanvas = ref(null)
let chartInstance = null

const renderChart = () => {
  if (!chartCanvas.value || !chartData.value) return
  if (chartInstance) {
    chartInstance.destroy()
    chartInstance = null
  }
  chartInstance = new Chart(chartCanvas.value, {
    type: 'bar',
    data: chartData.value,
    options: chartOptions.value,
  })
}

onBeforeUnmount(() => {
  if (chartInstance) {
    chartInstance.destroy()
    chartInstance = null
  }
})

// Chart.js options for histogram
const chartOptions = computed(() => ({
  responsive: true,
  maintainAspectRatio: false,
  plugins: {
    legend: {
      display: false,
    },
    tooltip: {
      backgroundColor: '#1F2937',
      borderColor: '#374151',
      borderWidth: 1,
      callbacks: {
        label: function(context) {
          const value = context.parsed.y
          return `Count: ${value.toLocaleString()}`
        },
        title: function(context) {
          // Label is already formatted with consistent unit
          return context[0].label
        },
      },
    },
  },
  scales: {
    x: {
      grid: {
        display: false,
      },
      ticks: {
        color: '#9CA3AF',
        maxRotation: 45,
        autoSkip: true,
        maxTicksLimit: 10,
      },
    },
    y: {
      grid: {
        color: '#374151',
        drawBorder: false,
      },
      ticks: {
        color: '#9CA3AF',
      },
      beginAtZero: true,
    },
  },
}))

// Determine the best display unit for a set of values so all labels are consistent.
// Uses the median value to pick the unit, so outliers don't force an inappropriate scale.
const pickConsistentUnit = (values) => {
  const unit = props.config.unit || 'ns'
  const sorted = values.filter(v => !isNaN(v) && v > 0).sort((a, b) => a - b)
  // Use median to pick the unit -- resilient to outliers
  const representative = sorted.length > 0 ? sorted[Math.floor(sorted.length / 2)] : 0

  if (unit === 'ns') {
    if (representative >= 1e9) return { divisor: 1e9, suffix: 's' }
    if (representative >= 1e6) return { divisor: 1e6, suffix: 'ms' }
    if (representative >= 1e3) return { divisor: 1e3, suffix: 'µs' }
    return { divisor: 1, suffix: 'ns' }
  }

  if (unit === 'bytes') {
    if (representative >= 1e9) return { divisor: 1e9, suffix: 'GB' }
    if (representative >= 1e6) return { divisor: 1e6, suffix: 'MB' }
    if (representative >= 1e3) return { divisor: 1e3, suffix: 'KB' }
    return { divisor: 1, suffix: 'B' }
  }

  return { divisor: 1, suffix: '' }
}

// Format a single value using a pre-determined unit
const formatWithUnit = (value, { divisor, suffix }) => {
  const converted = value / divisor
  // Use up to 2 decimal places, but drop trailing zeros
  const formatted = converted < 10 ? converted.toFixed(2) : converted < 100 ? converted.toFixed(1) : converted.toFixed(0)
  return `${parseFloat(formatted)}${suffix}`
}

// Format bucket label (used in tooltips where we want per-value smart formatting)
const formatBucketLabel = (label) => {
  const value = parseFloat(label)
  if (isNaN(value)) return label
  const unitInfo = pickConsistentUnit([value])
  return formatWithUnit(value, unitInfo)
}

// Transform query results to Chart.js format
const transformData = (result) => {
  if (!result || !result.data || result.data.length === 0) {
    return null
  }

  // Find histogram data in result
  // ClickHouse histogram() returns array of (lower, upper, count) tuples
  const histogramCol = result.columns.find(c => 
    c.includes('histogram') || result.data[0][c] instanceof Array
  )
  
  if (histogramCol) {
    // Parse histogram tuples from ClickHouse
    const histData = result.data[0][histogramCol]
    if (Array.isArray(histData)) {
      const rawLowers = []
      const counts = []
      
      for (const bucket of histData) {
        if (Array.isArray(bucket) && bucket.length >= 3) {
          rawLowers.push(parseFloat(bucket[0]))
          counts.push(bucket[2])
        }
      }
      
      // Pick one consistent unit for ALL bucket labels
      const unitInfo = pickConsistentUnit(rawLowers)
      const labels = rawLowers.map(v => formatWithUnit(v, unitInfo))
      
      return {
        labels,
        datasets: [{
          label: 'Count',
          data: counts,
          backgroundColor: 'rgba(99, 102, 241, 0.7)',
          borderColor: 'rgba(99, 102, 241, 1)',
          borderWidth: 1,
          borderRadius: 2,
        }],
      }
    }
  }
  
  // Fallback: treat as pre-bucketed data
  const rawLabels = []
  const values = []
  
  for (const row of result.data) {
    const label = Object.values(row).find(v => typeof v === 'string') || ''
    const value = Object.values(row).find(v => typeof v === 'number') || 0
    
    rawLabels.push(label)
    values.push(value)
  }
  
  // If labels look numeric and we have a unit, format consistently
  const numericLabels = rawLabels.map(l => parseFloat(l))
  const allNumeric = numericLabels.every(v => !isNaN(v))
  let labels
  if (allNumeric && props.config.unit) {
    const unitInfo = pickConsistentUnit(numericLabels)
    labels = numericLabels.map(v => formatWithUnit(v, unitInfo))
  } else {
    labels = rawLabels
  }
  
  return {
    labels,
    datasets: [{
      label: 'Count',
      data: values,
      backgroundColor: 'rgba(99, 102, 241, 0.7)',
      borderColor: 'rgba(99, 102, 241, 1)',
      borderWidth: 1,
      borderRadius: 2,
    }],
  }
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
    // Use nextTick to ensure canvas is in the DOM before rendering
    await nextTick()
    renderChart()
  } catch (err) {
    console.error('Widget query failed:', err)
  }
}

onMounted(fetchData)

watch([() => props.timeRange, () => props.variables], fetchData, { deep: true })
</script>
