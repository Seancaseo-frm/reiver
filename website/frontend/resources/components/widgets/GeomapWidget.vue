<template>
  <div class="geomap-widget h-full flex flex-col">
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
    <div v-else-if="rows.length === 0" class="flex-1 flex items-center justify-center">
      <div class="text-center text-gray-400">
        <svg class="w-10 h-10 mx-auto mb-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3.055 11H5a2 2 0 012 2v1a2 2 0 002 2 2 2 0 012 2v2.945M8 3.935V5.5A2.5 2.5 0 0010.5 8h.5a2 2 0 012 2 2 2 0 104 0 2 2 0 012-2h1.064M15 20.488V18a2 2 0 012-2h3.064M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
        </svg>
        <p class="text-sm">No data available</p>
      </div>
    </div>

    <!-- Ranked List -->
    <div v-else class="flex-1 min-h-0 overflow-y-auto">
      <div class="space-y-1 p-1">
        <div
          v-for="(row, idx) in rows"
          :key="idx"
          class="geomap-row flex items-center gap-3 px-3 py-2 rounded"
        >
          <span class="rank text-xs text-gray-500 w-5 text-right">{{ idx + 1 }}</span>
          <span class="label text-sm text-gray-200 flex-shrink-0 w-16">{{ row.label }}</span>
          <div class="flex-1 h-4 bg-gray-700 rounded-full overflow-hidden">
            <div
              class="h-full bg-primary-500 rounded-full transition-all"
              :style="{ width: row.pct + '%' }"
            ></div>
          </div>
          <span class="value text-sm text-gray-300 tabular-nums w-20 text-right">{{ row.formattedValue }}</span>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, watch } from 'vue'
import { useWidgetQuery, parseTimeRange } from '@/composables/useWidgetQuery'

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
const rows = ref([])

const formatValue = (val) => {
  if (typeof val !== 'number') return String(val)
  if (val >= 1e6) return `${(val / 1e6).toFixed(1)}M`
  if (val >= 1e3) return `${(val / 1e3).toFixed(1)}K`
  if (Number.isInteger(val)) return val.toLocaleString()
  return val.toFixed(1)
}

const transformData = (result) => {
  if (!result?.data?.length || !result.columns?.length) return []

  const metricField = props.config.metricField
  const labelCol = result.columns.find(c =>
    typeof result.data[0][c] === 'string' && isNaN(Number(result.data[0][c]))
  )
  const valueCol = metricField && result.columns.includes(metricField)
    ? metricField
    : result.columns.find(c => typeof result.data[0][c] === 'number' && c !== labelCol)

  if (!labelCol || !valueCol) return []

  const maxVal = Math.max(...result.data.map(r => parseFloat(r[valueCol]) || 0), 1)

  return result.data.map(r => ({
    label: String(r[labelCol] || 'Unknown'),
    value: parseFloat(r[valueCol]) || 0,
    formattedValue: formatValue(parseFloat(r[valueCol]) || 0),
    pct: ((parseFloat(r[valueCol]) || 0) / maxVal) * 100,
  }))
}

const fetchData = async () => {
  if (!props.config.query) return

  try {
    const range = parseTimeRange(props.timeRange)
    const result = await executeQuery(
      props.projectId,
      props.config.query,
      range,
      props.variables
    )
    rows.value = transformData(result)
  } catch (err) {
    console.error('Widget query failed:', err)
  }
}

onMounted(fetchData)

watch([() => props.timeRange, () => props.variables], fetchData, { deep: true })
</script>

<style scoped>
.geomap-row:hover {
  @apply bg-gray-800/50;
}
</style>
