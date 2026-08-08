<template>
  <div class="heatmap-widget h-full flex flex-col">
    <div v-if="loading" class="flex-1 flex items-center justify-center">
      <div class="spinner w-8 h-8 border-2 border-primary-500 border-t-transparent rounded-full animate-spin"></div>
    </div>
    <div v-else-if="error" class="flex-1 flex items-center justify-center">
      <div class="text-center p-4">
        <p class="text-sm text-danger-400">{{ error }}</p>
      </div>
    </div>
    <div v-else-if="!heatmapData || (Array.isArray(heatmapData) && heatmapData.length === 0) || (heatmapData.datasets && heatmapData.datasets.length === 0)" class="flex-1 flex items-center justify-center">
      <p class="text-sm text-gray-400">No data available</p>
    </div>
    <div v-else class="flex-1 min-h-0">
      <canvas ref="chartCanvas"></canvas>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, watch } from 'vue'
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
import { useWidgetQuery, parseTimeRange } from '@/composables/useWidgetQuery'
import { applyChartTheme } from '../../utils/chartTheme.js'

Chart.register(CategoryScale, LinearScale, BarController, BarElement, Title, Tooltip, Legend)

const props = defineProps({
  config: { type: Object, required: true },
  projectId: { type: String, required: true },
  timeRange: { type: String, default: '1h' },
  variables: { type: Object, default: () => ({}) },
})

const { loading, error, executeQuery } = useWidgetQuery()
const heatmapData = ref([])
const chartCanvas = ref(null)
let chartInstance = null

const heatmapColors = [
  'rgba(99, 102, 241, 0.1)',
  'rgba(99, 102, 241, 0.3)',
  'rgba(99, 102, 241, 0.5)',
  'rgba(99, 102, 241, 0.7)',
  'rgba(99, 102, 241, 0.9)',
]

const getColor = (value, max) => {
  if (max === 0) return heatmapColors[0]
  const idx = Math.min(Math.floor((value / max) * (heatmapColors.length - 1)), heatmapColors.length - 1)
  return heatmapColors[idx]
}

const renderChart = () => {
  const hd = heatmapData.value
  if (!chartCanvas.value || !hd || (Array.isArray(hd) && hd.length === 0)) return
  if (chartInstance) { chartInstance.destroy(); chartInstance = null }

  const data = heatmapData.value

  if (data.datasets) {
    chartInstance = new Chart(chartCanvas.value, {
      type: 'bar',
      data: {
        labels: data.labels,
        datasets: data.datasets,
      },
      options: applyChartTheme({
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: { mode: 'index', intersect: false },
        },
        scales: {
          x: { stacked: true, grid: { display: false }, ticks: { color: '#9CA3AF', maxRotation: 0, autoSkip: true } },
          y: { stacked: true, grid: { color: '#374151' }, ticks: { color: '#9CA3AF' }, beginAtZero: true },
        },
      }),
    })
    return
  }

  const labels = data.map(d => d.x)
  const values = data.map(d => d.y)
  const max = Math.max(...values.filter(v => v != null && !isNaN(v)), 1)

  chartInstance = new Chart(chartCanvas.value, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        data: values,
        backgroundColor: values.map(v => getColor(v || 0, max)),
        borderWidth: 0,
        borderRadius: 2,
      }],
    },
    options: applyChartTheme({
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false }, tooltip: { mode: 'index', intersect: false } },
      scales: {
        x: { grid: { display: false }, ticks: { color: '#9CA3AF', maxRotation: 0, autoSkip: true } },
        y: { grid: { color: '#374151' }, ticks: { color: '#9CA3AF' }, beginAtZero: true },
      },
    }),
  })
}

const parseBucketBound = (le) => {
  if (le === '+Inf' || le === 'Inf') return Infinity
  return parseFloat(le) || 0
}

const fetchData = async () => {
  if (!props.config.query) return
  try {
    const range = parseTimeRange(props.timeRange)
    const result = await executeQuery(props.projectId, props.config.query, range, props.variables)
    if (result?.data && result.data.length > 0) {
      const timeCols = ['time_bucket', 'time', 'timestamp', 'unix_milli']
      const skipCols = new Set(['project_id', 'fingerprint', 'value', ...timeCols])
      const timeCol = result.columns.find(c => timeCols.includes(c))
      const lblCols = result.columns.filter(c => !skipCols.has(c) && (c.startsWith('lbl_') || c === 'le'))

      if (lblCols.length > 0 && timeCol) {
        const dimCol = lblCols.find(c => c === 'lbl_le' || c === 'le') || lblCols[0]
        const valCol = result.columns.find(c => c === 'value')
          || result.columns.find(c => !skipCols.has(c) && !lblCols.includes(c))

        const byTime = new Map()
        const bucketSet = new Set()
        for (const row of result.data) {
          const t = timeCol ? new Date(row[timeCol]).toLocaleTimeString() : ''
          const bucket = String(row[dimCol] ?? '')
          const value = parseFloat(row[valCol]) || 0
          bucketSet.add(bucket)
          if (!byTime.has(t)) byTime.set(t, new Map())
          byTime.get(t).set(bucket, (byTime.get(t).get(bucket) || 0) + value)
        }

        const timeLabels = Array.from(byTime.keys())
        const sortedBuckets = Array.from(bucketSet).sort((a, b) => parseBucketBound(a) - parseBucketBound(b))
        const datasets = sortedBuckets.map((bucket, idx) => {
          const intensity = Math.min(0.2 + (idx / Math.max(sortedBuckets.length - 1, 1)) * 0.8, 1)
          return {
            label: bucket === '+Inf' ? '+Inf' : bucket,
            data: timeLabels.map(t => byTime.get(t)?.get(bucket) || 0),
            backgroundColor: `rgba(99, 102, 241, ${intensity.toFixed(2)})`,
            borderWidth: 0,
            borderRadius: 1,
          }
        })

        heatmapData.value = { labels: timeLabels, datasets }
      } else {
        const valCol = result.columns.find(c => c === 'value')
          || result.columns.find(c => !skipCols.has(c))
        heatmapData.value = result.data.map(row => ({
          x: timeCol ? new Date(row[timeCol]).toLocaleTimeString() : '',
          y: parseFloat(row[valCol]) || 0,
        }))
      }
    }
    renderChart()
  } catch (err) {
    console.error('Heatmap query failed:', err)
  }
}

onMounted(fetchData)
watch([() => props.timeRange, () => props.variables], fetchData, { deep: true })
watch(heatmapData, renderChart, { flush: 'post' })
</script>
