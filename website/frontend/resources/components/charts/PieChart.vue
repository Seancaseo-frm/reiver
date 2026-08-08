<template>
  <div class="pie-chart">
    <canvas ref="chartCanvas"></canvas>
  </div>
</template>

<script setup>
import { ref, onMounted, watch } from 'vue'
import {
  Chart,
  ArcElement,
  PieController,
  Tooltip,
  Legend,
} from 'chart.js'
import { chartTheme, applyChartTheme } from '../../utils/chartTheme.js'
import { formatGrafanaUnit } from '@/utils/widgetTransforms'

Chart.register(
  ArcElement,
  PieController,
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
})

const chartCanvas = ref(null)
let chartInstance = null

const getPieColors = () => {
  return chartTheme.colors.lineColors.map(color => chartTheme.hexToRgba(color, 0.8))
}

const createChart = () => {
  if (!chartCanvas.value) return
  if (!Array.isArray(props.data) || props.data.length === 0) return

  if (chartInstance) {
    chartInstance.destroy()
  }

  const labels = props.data.map(item => item.x || item.label || '')
  const values = props.data.map(item => item.y || item.value || 0)

  chartInstance = new Chart(chartCanvas.value, {
    type: 'pie',
    data: {
      labels,
      datasets: [{
        data: values,
        backgroundColor: props.config.colors || getPieColors(),
        borderWidth: 2,
        borderColor: chartTheme.colors.bgSecondary,
      }],
    },
    options: applyChartTheme({
      plugins: {
        legend: {
          position: 'right',
        },
        tooltip: {
          callbacks: {
            label: (context) => {
              const total = context.dataset.data.reduce((a, b) => a + b, 0)
              const percentage = ((context.parsed / total) * 100).toFixed(1)
              const unit = props.config.unit || props.config.query?.unit
              const formatted = unit
                ? formatGrafanaUnit(context.parsed, unit)
                : context.parsed
              return `${context.label}: ${formatted} (${percentage}%)`
            },
          },
        },
      },
    }),
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
.pie-chart {
  width: 100%;
  height: 100%;
  position: relative;
}
</style>

