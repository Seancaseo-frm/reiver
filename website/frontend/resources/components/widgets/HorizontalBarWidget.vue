<template>
  <div class="horizontal-bar-widget h-full flex flex-col">
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
    <div v-else-if="!chartData || chartData.length === 0" class="flex-1 flex items-center justify-center">
      <div class="text-center text-gray-400">
        <svg class="w-10 h-10 mx-auto mb-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
        </svg>
        <p class="text-sm">No data available</p>
      </div>
    </div>
    
    <!-- Chart -->
    <div v-else class="flex-1 min-h-0">
      <BarChart
        :data="chartData"
        :options="chartOptions"
        class="h-full"
      />
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import { useWidgetQuery, parseTimeRange } from '@/composables/useWidgetQuery'
import { transformBarData, formatGrafanaUnit } from '@/utils/widgetTransforms'
import BarChart from '@/components/charts/BarChart.vue'

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

const widgetUnit = computed(() => props.config.unit || props.config.query?.unit)

const chartOptions = computed(() => {
  const unit = widgetUnit.value
  const isHorizontal = props.config.orientation === 'horizontal'
  const valueAxis = isHorizontal ? 'x' : 'y'
  const labelAxis = isHorizontal ? 'y' : 'x'

  const tooltipCallbacks = unit
    ? { label: (ctx) => formatGrafanaUnit(ctx.parsed[valueAxis] ?? ctx.parsed.y, unit) }
    : undefined

  const valueTicks = unit
    ? { color: '#9CA3AF', callback: (v) => formatGrafanaUnit(v, unit) }
    : { color: '#9CA3AF' }

  return {
    indexAxis: isHorizontal ? 'y' : 'x',
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { display: false },
      tooltip: {
        backgroundColor: '#1F2937',
        borderColor: '#374151',
        borderWidth: 1,
        callbacks: tooltipCallbacks,
      },
    },
    scales: {
      [valueAxis]: {
        grid: { color: '#374151', drawBorder: false },
        ticks: valueTicks,
      },
      [labelAxis]: {
        grid: { display: false },
        ticks: {
          color: '#9CA3AF',
          maxRotation: 0,
          callback: function(value, index) {
            const label = this.getLabelForValue(index)
            if (label && label.length > 30) return label.substring(0, 30) + '...'
            return label
          },
        },
      },
    },
  }
})

// Get color gradient based on index
const getBarColor = (index, total) => {
  // Create a gradient from primary to secondary colors
  const colors = [
    'rgba(99, 102, 241, 0.8)',   // Indigo
    'rgba(139, 92, 246, 0.8)',   // Purple
    'rgba(168, 85, 247, 0.8)',   // Violet
    'rgba(192, 132, 252, 0.8)',  // Purple light
    'rgba(167, 139, 250, 0.8)',  // Violet light
  ]
  return colors[index % colors.length]
}

// Transform query results to flat [{x, y}] format expected by BarChart.
// Delegates to the shared pure function in widgetTransforms.js.
const transformData = (result) => transformBarData(result, props.config)

const formatColumnName = (col) => {
  return col
    .replace(/_/g, ' ')
    .replace(/([A-Z])/g, ' $1')
    .replace(/^\w/, c => c.toUpperCase())
    .trim()
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
