<template>
  <div class="histogram-chart">
    <BarChart :data="binnedData" :config="config" />
  </div>
</template>

<script setup>
import { computed } from 'vue'
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

const binnedData = computed(() => {
  // Validate input data
  if (!props.data || !Array.isArray(props.data) || props.data.length === 0) {
    return []
  }

  // Extract numeric values, filtering out null/undefined and non-numeric values
  const values = props.data
    .filter(item => item != null) // Filter out null/undefined items
    .map(item => {
      // Try to get numeric value from various possible fields
      if (typeof item.y === 'number') return item.y
      if (typeof item.value === 'number') return item.value
      // For histogram, we might have x as the value and y as count
      if (typeof item.x === 'number') return item.x
      // Try parsing if it's a string number
      if (typeof item.x === 'string') {
        const parsed = parseFloat(item.x)
        if (!isNaN(parsed)) return parsed
      }
      return null
    })
    .filter(v => v != null && typeof v === 'number' && !isNaN(v)) // Filter out null, undefined, and NaN

  if (values.length === 0) {
    console.warn('HistogramChart: No valid numeric values found in data', props.data)
    return []
  }

  const min = Math.min(...values)
  const max = Math.max(...values)
  
  // Handle case where min === max (all values are the same)
  if (min === max) {
    return [{
      x: `${min}`,
      y: values.length,
      label: `${min}`,
    }]
  }

  const bins = props.config.bins || 10
  const binSize = (max - min) / bins

  const binsData = Array(bins).fill(0).map((_, i) => ({
    x: `${(min + i * binSize).toFixed(1)}-${(min + (i + 1) * binSize).toFixed(1)}`,
    y: 0,
    label: `${(min + i * binSize).toFixed(1)}-${(min + (i + 1) * binSize).toFixed(1)}`,
  }))

  values.forEach(value => {
    const binIndex = Math.min(Math.floor((value - min) / binSize), bins - 1)
    if (binsData[binIndex]) {
      binsData[binIndex].y++
    }
  })

  return binsData
})
</script>

