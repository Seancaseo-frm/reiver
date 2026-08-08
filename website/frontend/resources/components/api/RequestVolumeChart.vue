<template>
  <div class="request-volume-chart" ref="containerRef">
    <svg :width="width" :height="height" class="chart-svg">
      <!-- Grid lines -->
      <g class="grid">
        <line
          v-for="tick in yTicks"
          :key="tick.value"
          :x1="padding.left"
          :y1="tick.y"
          :x2="width - padding.right"
          :y2="tick.y"
          stroke="currentColor"
          class="text-gray-700"
        />
      </g>

      <!-- Y-axis labels -->
      <g class="y-axis">
        <text
          v-for="tick in yTicks"
          :key="'label-' + tick.value"
          :x="padding.left - 8"
          :y="tick.y + 4"
          class="axis-label"
        >
          {{ formatNumber(tick.value) }}
        </text>
      </g>

      <!-- Area fill -->
      <path
        v-if="areaPath"
        :d="areaPath"
        fill="url(#volume-gradient)"
      />

      <!-- Line -->
      <path
        v-if="linePath"
        :d="linePath"
        stroke="#3B82F6"
        stroke-width="2"
        fill="none"
        stroke-linecap="round"
        stroke-linejoin="round"
      />

      <!-- Data points -->
      <circle
        v-for="(point, index) in chartPoints"
        :key="index"
        :cx="point.x"
        :cy="point.y"
        r="3"
        fill="#3B82F6"
        class="opacity-0 hover:opacity-100 transition-opacity cursor-pointer"
        @mouseenter="hoveredPoint = point"
        @mouseleave="hoveredPoint = null"
      />

      <!-- X-axis labels -->
      <g class="x-axis">
        <text
          v-for="(label, index) in xLabels"
          :key="index"
          :x="label.x"
          :y="height - 5"
          class="axis-label"
        >
          {{ label.text }}
        </text>
      </g>

      <!-- Gradient definition -->
      <defs>
        <linearGradient id="volume-gradient" x1="0%" y1="0%" x2="0%" y2="100%">
          <stop offset="0%" stop-color="#3B82F6" stop-opacity="0.3" />
          <stop offset="100%" stop-color="#3B82F6" stop-opacity="0" />
        </linearGradient>
      </defs>
    </svg>

    <!-- Tooltip -->
    <div
      v-if="hoveredPoint"
      class="chart-tooltip"
      :style="tooltipStyle"
    >
      <div class="tooltip-time">{{ hoveredPoint.time }}</div>
      <div class="tooltip-value">{{ formatNumber(hoveredPoint.value) }} requests</div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { format } from 'date-fns'

const props = defineProps({
  data: {
    type: Array,
    default: () => [],
  },
})

const containerRef = ref(null)
const width = ref(400)
const height = ref(200)
const hoveredPoint = ref(null)

const padding = { top: 10, right: 10, bottom: 30, left: 50 }

// Normalize data
const normalizedData = computed(() => {
  if (!props.data || props.data.length === 0) {
    // Generate empty data points
    return Array(12).fill(0).map((_, i) => ({
      timestamp: new Date(Date.now() - (11 - i) * 5 * 60 * 1000),
      value: 0,
    }))
  }
  
  return props.data.map(d => ({
    timestamp: new Date(d.timestamp),
    value: d.requests,
  }))
})

// Calculate chart dimensions
const chartWidth = computed(() => width.value - padding.left - padding.right)
const chartHeight = computed(() => height.value - padding.top - padding.bottom)

// Calculate Y-axis ticks
const yTicks = computed(() => {
  const values = normalizedData.value.map(d => d.value)
  const max = Math.max(...values, 1)
  const tickCount = 4
  const ticks = []
  
  for (let i = 0; i <= tickCount; i++) {
    const value = (max * i) / tickCount
    const y = padding.top + chartHeight.value * (1 - i / tickCount)
    ticks.push({ value: Math.round(value), y })
  }
  
  return ticks
})

// Calculate chart points
const chartPoints = computed(() => {
  const values = normalizedData.value.map(d => d.value)
  const max = Math.max(...values, 1)
  
  return normalizedData.value.map((d, i) => ({
    x: padding.left + (i / (normalizedData.value.length - 1 || 1)) * chartWidth.value,
    y: padding.top + (1 - d.value / max) * chartHeight.value,
    value: d.value,
    time: format(d.timestamp, 'HH:mm'),
  }))
})

// Generate line path
const linePath = computed(() => {
  if (chartPoints.value.length < 2) return ''
  
  return chartPoints.value
    .map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x} ${p.y}`)
    .join(' ')
})

// Generate area path
const areaPath = computed(() => {
  if (chartPoints.value.length < 2) return ''
  
  const bottomY = height.value - padding.bottom
  const linePart = chartPoints.value
    .map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x} ${p.y}`)
    .join(' ')
  
  const lastX = chartPoints.value[chartPoints.value.length - 1].x
  const firstX = chartPoints.value[0].x
  
  return `${linePart} L ${lastX} ${bottomY} L ${firstX} ${bottomY} Z`
})

// X-axis labels
const xLabels = computed(() => {
  const labelCount = Math.min(6, normalizedData.value.length)
  const step = Math.floor(normalizedData.value.length / labelCount)
  const labels = []
  
  for (let i = 0; i < normalizedData.value.length; i += step) {
    const d = normalizedData.value[i]
    labels.push({
      x: padding.left + (i / (normalizedData.value.length - 1 || 1)) * chartWidth.value,
      text: format(d.timestamp, 'HH:mm'),
    })
  }
  
  return labels
})

// Tooltip position
const tooltipStyle = computed(() => {
  if (!hoveredPoint.value) return {}
  
  return {
    left: `${hoveredPoint.value.x}px`,
    top: `${hoveredPoint.value.y - 10}px`,
    transform: 'translate(-50%, -100%)',
  }
})

// Format number
const formatNumber = (num) => {
  if (num === undefined || num === null) return '0'
  if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`
  if (num >= 1000) return `${(num / 1000).toFixed(1)}K`
  return num.toString()
}

// Handle resize
const handleResize = () => {
  if (containerRef.value) {
    width.value = containerRef.value.clientWidth
  }
}

onMounted(() => {
  handleResize()
  window.addEventListener('resize', handleResize)
})

onUnmounted(() => {
  window.removeEventListener('resize', handleResize)
})
</script>

<style scoped>
.request-volume-chart {
  @apply relative w-full h-full;
}

.chart-svg {
  @apply block;
}

.axis-label {
  @apply text-[10px] fill-gray-500;
}

.axis-label {
  text-anchor: end;
}

.x-axis .axis-label {
  text-anchor: middle;
}

.chart-tooltip {
  @apply absolute z-50 bg-white border border-gray-200 rounded-lg shadow-lg px-2 py-1 pointer-events-none;
}

.tooltip-time {
  @apply text-xs text-gray-500;
}

.tooltip-value {
  @apply text-sm font-medium text-gray-900;
}
</style>
