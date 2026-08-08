<template>
  <div class="line-chart">
    <canvas ref="chartCanvas"></canvas>
  </div>
</template>

<script setup>
import { ref, onMounted, watch } from 'vue'
import {
  Chart,
  CategoryScale,
  LinearScale,
  LineController,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler,
} from 'chart.js'
import { chartTheme, applyChartTheme } from '../../utils/chartTheme.js'

Chart.register(
  CategoryScale,
  LinearScale,
  LineController,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler
)

const props = defineProps({
  data: {
    type: Array,
    required: true,
  },
  config: {
    type: Object,
    default: () => ({}),
  },
  options: {
    type: Object,
    default: null,
  },
})

const chartCanvas = ref(null)
let chartInstance = null

const seriesColors = [
  { border: 'rgba(99, 102, 241, 1)', bg: 'rgba(99, 102, 241, 0.1)' },
  { border: 'rgba(16, 185, 129, 1)', bg: 'rgba(16, 185, 129, 0.1)' },
  { border: 'rgba(245, 158, 11, 1)', bg: 'rgba(245, 158, 11, 0.1)' },
  { border: 'rgba(239, 68, 68, 1)',  bg: 'rgba(239, 68, 68, 0.1)' },
  { border: 'rgba(139, 92, 246, 1)', bg: 'rgba(139, 92, 246, 0.1)' },
  { border: 'rgba(59, 130, 246, 1)', bg: 'rgba(59, 130, 246, 0.1)' },
  { border: 'rgba(236, 72, 153, 1)', bg: 'rgba(236, 72, 153, 0.1)' },
  { border: 'rgba(20, 184, 166, 1)', bg: 'rgba(20, 184, 166, 0.1)' },
]

const createChart = () => {
  if (!chartCanvas.value) return

  // Accept either a flat [{x, y}] array or { labels, datasets } object
  const isMultiSeries = props.data && !Array.isArray(props.data) && props.data.datasets
  if (!isMultiSeries && (!props.data || !Array.isArray(props.data) || props.data.length === 0)) {
    console.warn('LineChart: No data provided or empty data array')
    return
  }

  if (chartInstance) {
    chartInstance.destroy()
    chartInstance = null
  }

  let labels, datasets

  // Adaptive rendering: hide point markers when data is dense
  const computePointRadius = (count) => count > 80 ? 0 : count > 30 ? 1 : 2

  if (isMultiSeries) {
    labels = props.data.labels
    const pointCount = props.data.labels.length
    const pr = computePointRadius(pointCount)
    datasets = props.data.datasets.map((ds, i) => {
      const c = seriesColors[i % seriesColors.length]
      return {
        label: ds.label || `Series ${i + 1}`,
        data: ds.data,
        borderColor: c.border,
        backgroundColor: c.bg,
        borderWidth: 1.5,
        fill: false,
        tension: 0.3,
        pointRadius: pr,
        pointHoverRadius: 4,
        spanGaps: false,
      }
    })
  } else {
    labels = props.data.map((item, index) => {
      if (!item) return `Point ${index}`
      if (item.x instanceof Date) {
        try { return item.x.toLocaleString() } catch { return item.x.toISOString() }
      }
      if (typeof item.x === 'string') {
        try {
          const date = new Date(item.x)
          if (!isNaN(date.getTime())) return date.toLocaleString()
        } catch { /* not a date */ }
      }
      return String(item.x || item.label || `Point ${index}`)
    })

    const values = props.data.map(item => {
      if (!item) return null
      const val = item.y ?? item.value ?? null
      if (val === null || val === undefined) return null
      if (typeof val === 'number') return isNaN(val) ? null : val
      const parsed = parseFloat(val)
      return isNaN(parsed) ? null : parsed
    })

    const defaultColor = props.config.color || chartTheme.colors.primary
    const defaultFillColor = props.config.fillColor || chartTheme.colors.primaryLight
    const pr = props.config.pointRadius ?? computePointRadius(values.length)

    datasets = [{
      label: props.config.label || 'Value',
      data: values,
      borderColor: defaultColor,
      backgroundColor: defaultFillColor,
      borderWidth: 1.5,
      fill: props.config.fill !== false,
      tension: 0.3,
      pointRadius: pr,
      pointHoverRadius: pr > 0 ? 5 : 3,
      pointBackgroundColor: defaultColor,
      pointBorderColor: '#ffffff',
      pointBorderWidth: pr > 0 ? 1 : 0,
      spanGaps: false,
    }]
  }

  try {
    const defaultOptions = applyChartTheme({
      plugins: {
        legend: {
          display: isMultiSeries || !!props.config.label,
          position: 'bottom',
          labels: { color: '#9CA3AF', usePointStyle: true, padding: 12 },
        },
        tooltip: {
          mode: 'index',
          intersect: false,
          callbacks: {
            title: (ctx) => ctx[0].label || '',
            label: (ctx) => `${ctx.dataset.label || 'Value'}: ${ctx.parsed.y}`,
          },
        },
      },
      scales: {
        x: {
          display: true,
          title: { display: !!props.config.xLabel, text: props.config.xLabel || '' },
        },
        y: {
          display: true,
          title: { display: !!props.config.yLabel, text: props.config.yLabel || '' },
          beginAtZero: props.config.beginAtZero !== false,
        },
      },
    })

    chartInstance = new Chart(chartCanvas.value, {
      type: 'line',
      data: { labels, datasets },
      options: props.options || defaultOptions,
    })
  } catch (error) {
    console.error('LineChart: Error creating chart:', error)
    console.error('Data:', props.data)
  }
}

onMounted(() => {
  createChart()
})

watch(() => props.data, () => {
  createChart()
}, { deep: true })
</script>

<style scoped>
.line-chart {
  width: 100%;
  height: 100%;
  position: relative;
}
</style>

