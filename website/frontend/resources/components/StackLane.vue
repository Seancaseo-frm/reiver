<template>
  <div class="stack-lane rounded-lg border border-gray-200 bg-white shadow-sm overflow-hidden">
    <!-- Header -->
    <div class="flex items-center justify-between px-4 py-3 bg-gray-50 border-b border-gray-200">
      <div class="flex items-center gap-2">
        <span
          class="inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium"
          :class="tierBadgeClass"
        >
          {{ tier }}
        </span>
        <h3 class="text-sm font-semibold text-gray-900">{{ technology }}</h3>
      </div>
    </div>

    <!-- Golden Signal Charts -->
    <div class="grid gap-4 p-4" :class="gridCols">
      <div
        v-for="(signal, idx) in goldenSignals"
        :key="idx"
        class="signal-chart"
      >
        <p class="text-xs font-medium text-gray-500 mb-1">
          {{ signal.label }}
          <span class="text-gray-400">({{ signal.unit }})</span>
        </p>
        <div class="h-32">
          <div v-if="signalStates[idx]?.loading" class="h-full flex items-center justify-center">
            <div class="spinner w-5 h-5 border-2 border-primary-500 border-t-transparent rounded-full animate-spin"></div>
          </div>
          <div v-else-if="signalStates[idx]?.error" class="h-full flex items-center justify-center">
            <p class="text-xs text-red-400">{{ signalStates[idx].error }}</p>
          </div>
          <div v-else-if="!signalStates[idx]?.data" class="h-full flex items-center justify-center">
            <p class="text-xs text-gray-400">No data</p>
          </div>
          <UPlotChart
            v-else
            ref="charts"
            :data="signalStates[idx].data"
            :unit="signal.unit"
            :show-legend="false"
            class="h-full"
          />
        </div>
      </div>
    </div>

    <!-- Brush hint -->
    <div class="px-4 pb-2">
      <p class="text-xs text-gray-400 italic">
        Drag on any chart to select a time range for cross-stack correlation
      </p>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, watch, onMounted } from 'vue'
import axios from 'axios'
import { parseTimeRange } from '@/composables/useWidgetQuery'
import UPlotChart from '@/components/charts/UPlotChart.vue'

const props = defineProps({
  technology: { type: String, required: true },
  tier: { type: String, required: true },
  goldenSignals: { type: Array, required: true },
  projectId: { type: String, required: true },
  timeRange: { type: String, required: true },
  cursorTime: { type: Number, default: null },
})

const emit = defineEmits(['update:cursor-time', 'select-range'])

const charts = ref([])
const signalStates = ref([])

const gridCols = computed(() => {
  const count = props.goldenSignals.length
  if (count >= 3) return 'grid-cols-1 sm:grid-cols-3'
  if (count === 2) return 'grid-cols-1 sm:grid-cols-2'
  return 'grid-cols-1'
})

const tierBadgeClass = computed(() => {
  const map = {
    application: 'bg-blue-100 text-blue-700',
    queue: 'bg-purple-100 text-purple-700',
    database: 'bg-green-100 text-green-700',
    cache: 'bg-amber-100 text-amber-700',
    infrastructure: 'bg-gray-100 text-gray-700',
    runtime: 'bg-teal-100 text-teal-700',
  }
  return map[props.tier] || 'bg-gray-100 text-gray-700'
})

async function fetchSignal(index) {
  const signal = props.goldenSignals[index]
  if (!signal) return

  signalStates.value[index] = { loading: true, error: null, data: null }

  try {
    const tr = parseTimeRange(props.timeRange)
    const response = await axios.post(`/api/${props.projectId}/widget-query`, {
      query: { type: 'promql', promql: signal.promql },
      time_range: tr,
      variables: {},
    })

    const result = response.data
    if (!result || !result.data || result.data.length === 0) {
      signalStates.value[index] = { loading: false, error: null, data: null }
      return
    }

    const chartData = transformToChart(result)
    signalStates.value[index] = { loading: false, error: null, data: chartData }
  } catch (err) {
    signalStates.value[index] = {
      loading: false,
      error: err.response?.data?.error || err.message || 'Query failed',
      data: null,
    }
  }
}

function transformToChart(result) {
  if (!result?.data?.length) return null

  const columns = result.columns
  const timeCols = new Set(['time_bucket', 'time', 'timestamp', 'unix_milli'])
  const timeCol = columns.find(c => timeCols.has(c))
  if (!timeCol) return null

  const dataColumns = columns.filter(
    c => !timeCols.has(c) && !c.startsWith('lbl_') && c !== 'project_id' && c !== 'fingerprint'
  )
  const valueCol = dataColumns.includes('value') ? 'value' : dataColumns[0]
  if (!valueCol) return null

  const pairs = result.data.map(row => {
    const raw = row[timeCol]
    let ts
    if (timeCol === 'unix_milli') {
      ts = typeof raw === 'number' ? raw / 1000 : parseInt(raw) / 1000
    } else if (typeof raw === 'number') {
      ts = raw > 1e12 ? raw / 1000 : raw
    } else {
      const d = new Date(raw)
      ts = isNaN(d.getTime()) ? 0 : d.getTime() / 1000
    }

    let val = row[valueCol]
    val = val === null || val === undefined ? null : parseFloat(val)
    if (isNaN(val)) val = null
    return { ts, val }
  })

  pairs.sort((a, b) => a.ts - b.ts)

  return {
    timestamps: pairs.map(p => p.ts),
    datasets: [{ label: 'Value', data: pairs.map(p => p.val) }],
  }
}

async function fetchAllSignals() {
  signalStates.value = props.goldenSignals.map(() => ({ loading: true, error: null, data: null }))
  await Promise.all(props.goldenSignals.map((_, i) => fetchSignal(i)))
}

watch(() => props.timeRange, () => {
  fetchAllSignals()
})

onMounted(() => {
  fetchAllSignals()
})
</script>

<style scoped>
.stack-lane:hover {
  box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
}
</style>
