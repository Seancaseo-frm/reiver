<template>
  <div class="status-code-chart">
    <div class="chart-container">
      <!-- Bar Chart -->
      <div class="bars">
        <div
          v-for="item in chartData"
          :key="item.code"
          class="bar-group"
        >
          <div class="bar-wrapper">
            <div
              :class="['bar', getBarClass(item.code)]"
              :style="{ height: `${getBarHeight(item.count)}%` }"
            >
              <span v-if="getBarHeight(item.count) > 15" class="bar-value">
                {{ formatNumber(item.count) }}
              </span>
            </div>
          </div>
          <div class="bar-label">{{ item.code }}</div>
        </div>
      </div>
    </div>

    <!-- Legend -->
    <div class="chart-legend">
      <div class="legend-item">
        <span class="legend-dot bg-green-500"></span>
        <span class="legend-label">2xx Success</span>
        <span class="legend-value">{{ formatNumber(totals['2xx']) }}</span>
      </div>
      <div class="legend-item">
        <span class="legend-dot bg-blue-500"></span>
        <span class="legend-label">3xx Redirect</span>
        <span class="legend-value">{{ formatNumber(totals['3xx']) }}</span>
      </div>
      <div class="legend-item">
        <span class="legend-dot bg-yellow-500"></span>
        <span class="legend-label">4xx Client Error</span>
        <span class="legend-value">{{ formatNumber(totals['4xx']) }}</span>
      </div>
      <div class="legend-item">
        <span class="legend-dot bg-red-500"></span>
        <span class="legend-label">5xx Server Error</span>
        <span class="legend-value">{{ formatNumber(totals['5xx']) }}</span>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed } from 'vue'

const props = defineProps({
  data: {
    type: Array,
    default: () => [],
  },
})

// Process and sort data
const chartData = computed(() => {
  if (!props.data || props.data.length === 0) {
    // Return sample data for display
    return [
      { code: '200', count: 0 },
      { code: '201', count: 0 },
      { code: '400', count: 0 },
      { code: '404', count: 0 },
      { code: '500', count: 0 },
    ]
  }
  
  return [...props.data]
    .sort((a, b) => parseInt(a.code) - parseInt(b.code))
    .slice(0, 10)
})

// Calculate max value for scaling
const maxValue = computed(() => {
  if (chartData.value.length === 0) return 1
  return Math.max(...chartData.value.map(d => d.count), 1)
})

// Calculate totals by category
const totals = computed(() => {
  const result = { '2xx': 0, '3xx': 0, '4xx': 0, '5xx': 0 }
  
  chartData.value.forEach(item => {
    const code = parseInt(item.code)
    if (code >= 500) result['5xx'] += item.count
    else if (code >= 400) result['4xx'] += item.count
    else if (code >= 300) result['3xx'] += item.count
    else if (code >= 200) result['2xx'] += item.count
  })
  
  return result
})

const getBarHeight = (count) => {
  return (count / maxValue.value) * 100
}

const getBarClass = (code) => {
  const codeNum = parseInt(code)
  if (codeNum >= 500) return 'bar-5xx'
  if (codeNum >= 400) return 'bar-4xx'
  if (codeNum >= 300) return 'bar-3xx'
  return 'bar-2xx'
}

const formatNumber = (num) => {
  if (num === undefined || num === null) return '0'
  if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`
  if (num >= 1000) return `${(num / 1000).toFixed(1)}K`
  return num.toString()
}
</script>

<style scoped>
.status-code-chart {
  @apply space-y-4;
}

.chart-container {
  @apply h-48;
}

.bars {
  @apply flex items-end justify-around h-full gap-2 px-4;
}

.bar-group {
  @apply flex flex-col items-center gap-1 flex-1;
}

.bar-wrapper {
  @apply h-40 w-full flex items-end justify-center;
}

.bar {
  @apply w-full max-w-[40px] rounded-t transition-all flex items-end justify-center;
  min-height: 4px;
}

.bar-2xx {
  @apply bg-green-500;
}

.bar-3xx {
  @apply bg-blue-500;
}

.bar-4xx {
  @apply bg-yellow-500;
}

.bar-5xx {
  @apply bg-red-500;
}

.bar-value {
  @apply text-[10px] font-medium text-white pb-1;
}

.bar-label {
  @apply text-xs text-gray-600 dark:text-gray-400 font-mono;
}

.chart-legend {
  @apply flex flex-wrap justify-center gap-4 pt-2 border-t border-gray-200 dark:border-gray-700;
}

.legend-item {
  @apply flex items-center gap-1.5;
}

.legend-dot {
  @apply w-2 h-2 rounded-full;
}

.legend-label {
  @apply text-xs text-gray-600 dark:text-gray-400;
}

.legend-value {
  @apply text-xs font-medium text-gray-900 dark:text-gray-100;
}
</style>
