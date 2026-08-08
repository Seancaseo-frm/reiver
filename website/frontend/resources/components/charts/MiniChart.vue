<template>
  <div class="mini-chart" ref="containerRef">
    <svg :width="width" :height="height" class="chart-svg">
      <!-- Gradient definition -->
      <defs>
        <linearGradient :id="gradientId" x1="0%" y1="0%" x2="0%" y2="100%">
          <stop offset="0%" :stop-color="color" stop-opacity="0.3" />
          <stop offset="100%" :stop-color="color" stop-opacity="0" />
        </linearGradient>
      </defs>
      
      <!-- Area fill -->
      <path
        v-if="areaPath"
        :d="areaPath"
        :fill="`url(#${gradientId})`"
      />
      
      <!-- Line -->
      <path
        v-if="linePath"
        :d="linePath"
        :stroke="color"
        stroke-width="2"
        fill="none"
        stroke-linecap="round"
        stroke-linejoin="round"
      />
      
      <!-- Dots for data points (optional) -->
      <circle
        v-if="showDots && lastPoint"
        :cx="lastPoint.x"
        :cy="lastPoint.y"
        r="3"
        :fill="color"
      />
    </svg>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted, watch } from 'vue'

const props = defineProps({
  data: {
    type: Array,
    default: () => [],
  },
  color: {
    type: String,
    default: '#3B82F6',
  },
  showDots: {
    type: Boolean,
    default: true,
  },
  smooth: {
    type: Boolean,
    default: true,
  },
})

const containerRef = ref(null)
const width = ref(200)
const height = ref(60)

// Unique gradient ID
const gradientId = computed(() => `gradient-${Math.random().toString(36).substr(2, 9)}`)

// Normalize data to array of values
const normalizedData = computed(() => {
  if (!props.data || props.data.length === 0) return []
  
  return props.data.map(d => {
    if (typeof d === 'number') return d
    if (typeof d === 'object' && d !== null) {
      return d.value ?? d.y ?? d.count ?? 0
    }
    return 0
  })
})

// Calculate chart points
const points = computed(() => {
  const data = normalizedData.value
  if (data.length === 0) return []
  
  const min = Math.min(...data)
  const max = Math.max(...data)
  const range = max - min || 1
  
  const padding = 4
  const chartWidth = width.value - padding * 2
  const chartHeight = height.value - padding * 2
  
  return data.map((value, index) => ({
    x: padding + (index / (data.length - 1 || 1)) * chartWidth,
    y: padding + (1 - (value - min) / range) * chartHeight,
    value,
  }))
})

// Last point for the dot
const lastPoint = computed(() => {
  if (points.value.length === 0) return null
  return points.value[points.value.length - 1]
})

// Generate line path
const linePath = computed(() => {
  if (points.value.length < 2) return ''
  
  if (props.smooth) {
    return generateSmoothPath(points.value)
  }
  
  return points.value
    .map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x} ${p.y}`)
    .join(' ')
})

// Generate area path
const areaPath = computed(() => {
  if (points.value.length < 2) return ''
  
  const padding = 4
  const bottomY = height.value - padding
  
  let path = ''
  if (props.smooth) {
    path = generateSmoothPath(points.value)
  } else {
    path = points.value
      .map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x} ${p.y}`)
      .join(' ')
  }
  
  // Close the area
  const firstX = points.value[0].x
  const lastX = points.value[points.value.length - 1].x
  
  return `${path} L ${lastX} ${bottomY} L ${firstX} ${bottomY} Z`
})

// Generate smooth bezier curve path
const generateSmoothPath = (pts) => {
  if (pts.length < 2) return ''
  
  let path = `M ${pts[0].x} ${pts[0].y}`
  
  for (let i = 1; i < pts.length; i++) {
    const prev = pts[i - 1]
    const curr = pts[i]
    const next = pts[i + 1]
    
    // Calculate control points
    const cp1x = prev.x + (curr.x - prev.x) * 0.5
    const cp1y = prev.y
    const cp2x = prev.x + (curr.x - prev.x) * 0.5
    const cp2y = curr.y
    
    path += ` C ${cp1x} ${cp1y}, ${cp2x} ${cp2y}, ${curr.x} ${curr.y}`
  }
  
  return path
}

// Handle resize
const handleResize = () => {
  if (containerRef.value) {
    width.value = containerRef.value.clientWidth
    height.value = containerRef.value.clientHeight || 60
  }
}

onMounted(() => {
  handleResize()
  window.addEventListener('resize', handleResize)
})

onUnmounted(() => {
  window.removeEventListener('resize', handleResize)
})

watch(() => props.data, () => {
  handleResize()
}, { deep: true })
</script>

<style scoped>
.mini-chart {
  @apply w-full h-full;
}

.chart-svg {
  @apply block;
}
</style>
