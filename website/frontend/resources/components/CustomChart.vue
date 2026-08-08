<template>
  <div class="custom-chart">
    <LineChart v-if="chartType === 'line'" :data="transformedData" :config="config" />
    <BarChart v-else-if="chartType === 'bar'" :data="transformedData" :config="config" />
    <PieChart v-else-if="chartType === 'pie'" :data="transformedData" :config="config" />
    <ScatterChart v-else-if="chartType === 'scatter'" :data="transformedData" :config="config" />
    <HeatmapChart v-else-if="chartType === 'heatmap'" :data="transformedData" :config="config" />
    <HistogramChart v-else-if="chartType === 'histogram'" :data="transformedData" :config="config" />
    <div v-else class="flex items-center justify-center h-full text-gray-500">
      Unknown chart type: {{ chartType }}
    </div>
  </div>
</template>

<script setup>
import { computed } from 'vue'
import LineChart from './charts/LineChart.vue'
import BarChart from './charts/BarChart.vue'
import PieChart from './charts/PieChart.vue'
import ScatterChart from './charts/ScatterChart.vue'
import HeatmapChart from './charts/HeatmapChart.vue'
import HistogramChart from './charts/HistogramChart.vue'

const props = defineProps({
  data: {
    type: Array,
    required: true,
  },
  chartType: {
    type: String,
    required: true,
  },
  config: {
    type: Object,
    default: () => ({}),
  },
})

const transformedData = computed(() => {
  return props.data
})
</script>


