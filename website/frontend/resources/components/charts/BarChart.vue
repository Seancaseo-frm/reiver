<template>
  <div class="bar-chart">
    <canvas ref="chartCanvas"></canvas>
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
import { chartTheme, applyChartTheme } from '../../utils/chartTheme.js'

Chart.register(
  CategoryScale,
  LinearScale,
  BarController,
  BarElement,
  Title,
  Tooltip,
  Legend
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
    default: () => ({}),
  },
})

const chartCanvas = ref(null)
let chartInstance = null

// Simple deep merge for Chart.js options
const deepMerge = (target, source) => {
  const result = { ...target }
  for (const key of Object.keys(source)) {
    if (source[key] && typeof source[key] === 'object' && !Array.isArray(source[key]) &&
        target[key] && typeof target[key] === 'object' && !Array.isArray(target[key])) {
      result[key] = deepMerge(target[key], source[key])
    } else {
      result[key] = source[key]
    }
  }
  return result
}

const createChart = () => {
  if (!chartCanvas.value) return
  if (!Array.isArray(props.data) || props.data.length === 0) return

  if (chartInstance) {
    chartInstance.destroy()
  }

  const labels = props.data.map(item => item.x || item.label || '')
  const values = props.data.map(item => item.y || item.value || 0)

  const defaultColor = props.config.color || chartTheme.hexToRgba(chartTheme.colors.primary, 0.8)
  const defaultBorderColor = props.config.borderColor || chartTheme.colors.primary

  // Build default options, then deep-merge with any custom options passed as prop
  const defaultOptions = applyChartTheme({
    plugins: {
      legend: {
        display: !!props.config.label,
      },
    },
    scales: {
      x: {
        title: {
          display: !!props.config.xLabel,
          text: props.config.xLabel || '',
        },
      },
      y: {
        beginAtZero: true,
        title: {
          display: !!props.config.yLabel,
          text: props.config.yLabel || '',
        },
      },
    },
  })
  
  // Merge custom options (supports indexAxis, custom scales, tooltips, etc.)
  const mergedOptions = deepMerge(defaultOptions, props.options || {})

  chartInstance = new Chart(chartCanvas.value, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: props.config.label || 'Value',
        data: values,
        backgroundColor: defaultColor,
        borderColor: defaultBorderColor,
        borderWidth: 1,
      }],
    },
    options: mergedOptions,
  })
}

onMounted(() => {
  createChart()
})

watch(() => props.data, () => {
  createChart()
}, { deep: true })
</script>

<style scoped>
.bar-chart {
  width: 100%;
  height: 100%;
  position: relative;
}
</style>

