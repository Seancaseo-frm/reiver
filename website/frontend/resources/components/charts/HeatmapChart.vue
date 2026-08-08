<template>
  <div class="heatmap-chart">
    <div class="text-sm text-gray-500 dark:text-gray-400 mb-2 text-center">
      Heatmap visualization (coming soon - use bar chart for now)
    </div>
    <BarChart :data="data" :config="config" />
  </div>
</template>

<script setup>
import BarChart from './BarChart.vue'

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
</script>


