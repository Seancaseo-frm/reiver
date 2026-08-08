<template>
  <div class="scatter-chart">
    <canvas ref="chartCanvas"></canvas>
  </div>
</template>

<script setup>
import { ref, onMounted, watch } from 'vue'
import {
  Chart,
  LinearScale,
  ScatterController,
  PointElement,
  LineElement,
  Tooltip,
  Legend,
} from 'chart.js'
import { chartTheme, applyChartTheme } from '../../utils/chartTheme.js'

Chart.register(
  LinearScale,
  ScatterController,
  PointElement,
  LineElement,
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

const createChart = () => {
  if (!chartCanvas.value) return
  if (!Array.isArray(props.data) || props.data.length === 0) return

  if (chartInstance) {
    chartInstance.destroy()
  }

  const scatterData = props.data.map(item => ({
    x: typeof item.x === 'number' ? item.x : (item.x instanceof Date ? item.x.getTime() : 0),
    y: item.y || item.value || 0,
  }))

  const defaultColor = props.config.color || chartTheme.hexToRgba(chartTheme.colors.primary, 0.6)
  const defaultBorderColor = props.config.borderColor || chartTheme.colors.primary

  chartInstance = new Chart(chartCanvas.value, {
    type: 'scatter',
    data: {
      datasets: [{
        label: props.config.label || 'Data Points',
        data: scatterData,
        backgroundColor: defaultColor,
        borderColor: defaultBorderColor,
        pointRadius: props.config.pointRadius ?? 5,
      }],
    },
    options: applyChartTheme({
      plugins: {
        legend: {
          display: !!props.config.label,
        },
      },
      scales: {
        x: {
          type: 'linear',
          position: 'bottom',
          title: {
            display: !!props.config.xLabel,
            text: props.config.xLabel || '',
          },
        },
        y: {
          title: {
            display: !!props.config.yLabel,
            text: props.config.yLabel || '',
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
.scatter-chart {
  width: 100%;
  height: 100%;
  position: relative;
}
</style>

