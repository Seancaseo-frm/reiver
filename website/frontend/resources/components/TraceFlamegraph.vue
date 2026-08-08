<template>
  <div class="trace-flamegraph">
    <!-- Controls -->
    <div class="flamegraph-controls">
      <div class="flex items-center gap-4">
        <button
          @click="resetZoom"
          :disabled="!isZoomed"
          class="control-btn"
        >
          <svg class="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0zM13 10H7" />
          </svg>
          Reset Zoom
        </button>
        
        <div class="flex items-center gap-2 text-sm text-gray-500 dark:text-gray-400">
          <span>Color by:</span>
          <select v-model="colorBy" class="color-select">
            <option value="service">Service</option>
            <option value="status">Status</option>
            <option value="duration">Duration</option>
          </select>
        </div>
      </div>
      
      <div class="flex items-center gap-2 text-sm text-gray-500 dark:text-gray-400">
        <span>{{ visibleSpans.length }} spans</span>
        <span v-if="selectedSpan">| Selected: {{ selectedSpan.span_name }}</span>
      </div>
    </div>

    <!-- Flamegraph Canvas -->
    <div class="flamegraph-container" ref="containerRef">
      <svg
        :width="containerWidth"
        :height="svgHeight"
        class="flamegraph-svg"
        @mouseleave="hoveredSpan = null"
      >
        <!-- Time axis -->
        <g class="time-axis">
          <line
            x1="0"
            :y1="svgHeight - 20"
            :x2="containerWidth"
            :y2="svgHeight - 20"
            stroke="currentColor"
            class="text-gray-300 dark:text-gray-600"
          />
          <text
            v-for="tick in timeTicks"
            :key="tick.value"
            :x="tick.x"
            :y="svgHeight - 5"
            class="time-tick"
          >
            {{ tick.label }}
          </text>
        </g>

        <!-- Span bars -->
        <g class="span-bars">
          <g
            v-for="span in visibleSpans"
            :key="span.span_id"
            class="span-group"
            @click="selectSpan(span)"
            @mouseenter="hoveredSpan = span"
            @mouseleave="hoveredSpan = null"
          >
            <rect
              :x="getSpanX(span)"
              :y="getSpanY(span)"
              :width="Math.max(getSpanWidth(span), 2)"
              :height="barHeight - 2"
              :fill="getSpanColor(span)"
              :class="[
                'span-bar',
                { 'selected': selectedSpan?.span_id === span.span_id },
                { 'hovered': hoveredSpan?.span_id === span.span_id },
              ]"
              rx="2"
            />
            <text
              v-if="getSpanWidth(span) > 60"
              :x="getSpanX(span) + 4"
              :y="getSpanY(span) + barHeight / 2 + 4"
              class="span-label"
            >
              {{ truncateText(span.span_name, getSpanWidth(span) - 8) }}
            </text>
          </g>
        </g>
      </svg>

      <!-- Tooltip -->
      <div
        v-if="hoveredSpan"
        class="flamegraph-tooltip"
        :style="tooltipStyle"
      >
        <div class="tooltip-header">
          <span class="tooltip-name">{{ hoveredSpan.span_name }}</span>
          <span :class="['tooltip-status', getStatusClass(hoveredSpan.status_code)]">
            {{ hoveredSpan.status_code || 'OK' }}
          </span>
        </div>
        <div class="tooltip-content">
          <div class="tooltip-row">
            <span class="tooltip-label">Service:</span>
            <span class="tooltip-value">{{ hoveredSpan.service_name }}</span>
          </div>
          <div class="tooltip-row">
            <span class="tooltip-label">Duration:</span>
            <span class="tooltip-value">{{ formatDuration(hoveredSpan.duration_ns / 1_000_000) }}</span>
          </div>
          <div class="tooltip-row">
            <span class="tooltip-label">Self Time:</span>
            <span class="tooltip-value">{{ formatDuration(getSelfTime(hoveredSpan)) }}</span>
          </div>
          <div v-if="hoveredSpan.span_kind" class="tooltip-row">
            <span class="tooltip-label">Kind:</span>
            <span class="tooltip-value">{{ hoveredSpan.span_kind }}</span>
          </div>
        </div>
        <div class="tooltip-hint">Click to view details</div>
      </div>
    </div>

    <!-- Legend -->
    <div class="flamegraph-legend">
      <div v-if="colorBy === 'service'" class="legend-items">
        <div
          v-for="(color, service) in serviceColors"
          :key="service"
          class="legend-item"
        >
          <span class="legend-color" :style="{ backgroundColor: color }"></span>
          <span class="legend-label">{{ service }}</span>
        </div>
      </div>
      <div v-else-if="colorBy === 'status'" class="legend-items">
        <div class="legend-item">
          <span class="legend-color bg-green-500"></span>
          <span class="legend-label">OK</span>
        </div>
        <div class="legend-item">
          <span class="legend-color bg-red-500"></span>
          <span class="legend-label">Error</span>
        </div>
        <div class="legend-item">
          <span class="legend-color bg-gray-400"></span>
          <span class="legend-label">Unset</span>
        </div>
      </div>
      <div v-else class="legend-items">
        <div class="legend-gradient">
          <span class="gradient-bar"></span>
          <div class="gradient-labels">
            <span>Fast</span>
            <span>Slow</span>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted, watch } from 'vue'

const props = defineProps({
  trace: {
    type: Object,
    required: true,
  },
  selectedSpanId: {
    type: String,
    default: null,
  },
})

const emit = defineEmits(['select-span'])

const containerRef = ref(null)
const containerWidth = ref(800)
const hoveredSpan = ref(null)
const selectedSpan = ref(null)
const colorBy = ref('service')
const zoomLevel = ref({ start: 0, end: 1 })

const barHeight = 24
const padding = { top: 10, right: 10, bottom: 30, left: 10 }

// Service color palette
const colorPalette = [
  '#8B5CF6', '#3B82F6', '#10B981', '#F59E0B', '#EF4444',
  '#EC4899', '#6366F1', '#14B8A6', '#F97316', '#84CC16',
]

// Calculate service colors
const serviceColors = computed(() => {
  const services = [...new Set(props.trace.spans.map(s => s.service_name))]
  const colors = {}
  services.forEach((service, index) => {
    colors[service] = colorPalette[index % colorPalette.length]
  })
  return colors
})

// Calculate span depths
const spanDepths = computed(() => {
  const depths = {}
  const spans = props.trace.spans
  
  // Build parent-child relationships
  const childrenMap = {}
  spans.forEach(span => {
    if (span.parent_span_id) {
      if (!childrenMap[span.parent_span_id]) {
        childrenMap[span.parent_span_id] = []
      }
      childrenMap[span.parent_span_id].push(span.span_id)
    }
  })
  
  // Calculate depths using BFS
  const rootSpans = spans.filter(s => !s.parent_span_id)
  const queue = rootSpans.map(s => ({ span: s, depth: 0 }))
  
  while (queue.length > 0) {
    const { span, depth } = queue.shift()
    depths[span.span_id] = depth
    
    const children = childrenMap[span.span_id] || []
    children.forEach(childId => {
      const childSpan = spans.find(s => s.span_id === childId)
      if (childSpan) {
        queue.push({ span: childSpan, depth: depth + 1 })
      }
    })
  }
  
  return depths
})

// Max depth for height calculation
const maxDepth = computed(() => {
  return Math.max(...Object.values(spanDepths.value), 0)
})

// SVG height
const svgHeight = computed(() => {
  return (maxDepth.value + 1) * barHeight + padding.top + padding.bottom
})

// Total trace duration
const totalDuration = computed(() => {
  return (props.trace.trace.duration_ns || 1) / 1_000_000
})

// Trace start time
const traceStart = computed(() => {
  return new Date(props.trace.trace.start_time).getTime()
})

// Visible spans based on zoom
const visibleSpans = computed(() => {
  return props.trace.spans.filter(span => {
    const spanStart = getSpanStartMs(span)
    const spanEnd = spanStart + (span.duration_ns || 0) / 1_000_000
    const zoomStart = zoomLevel.value.start * totalDuration.value
    const zoomEnd = zoomLevel.value.end * totalDuration.value
    return spanEnd >= zoomStart && spanStart <= zoomEnd
  })
})

// Is zoomed
const isZoomed = computed(() => {
  return zoomLevel.value.start !== 0 || zoomLevel.value.end !== 1
})

// Time ticks for axis
const timeTicks = computed(() => {
  const ticks = []
  const zoomStart = zoomLevel.value.start * totalDuration.value
  const zoomEnd = zoomLevel.value.end * totalDuration.value
  const duration = zoomEnd - zoomStart
  const tickCount = 5
  
  for (let i = 0; i <= tickCount; i++) {
    const value = zoomStart + (duration * i / tickCount)
    const x = padding.left + (containerWidth.value - padding.left - padding.right) * (i / tickCount)
    ticks.push({
      value,
      x,
      label: formatDuration(value),
    })
  }
  
  return ticks
})

// Tooltip position
const tooltipStyle = computed(() => {
  if (!hoveredSpan.value) return {}
  
  const span = hoveredSpan.value
  const x = getSpanX(span) + getSpanWidth(span) / 2
  const y = getSpanY(span)
  
  return {
    left: `${Math.min(x, containerWidth.value - 200)}px`,
    top: `${y - 10}px`,
    transform: 'translateY(-100%)',
  }
})

// Get span start time in ms relative to trace start
const getSpanStartMs = (span) => {
  const spanStart = new Date(span.timestamp).getTime()
  return Math.max(0, spanStart - traceStart.value)
}

// Get span X position
const getSpanX = (span) => {
  const spanStart = getSpanStartMs(span)
  const zoomStart = zoomLevel.value.start * totalDuration.value
  const zoomEnd = zoomLevel.value.end * totalDuration.value
  const zoomDuration = zoomEnd - zoomStart
  
  const relativeStart = (spanStart - zoomStart) / zoomDuration
  return padding.left + relativeStart * (containerWidth.value - padding.left - padding.right)
}

// Get span Y position
const getSpanY = (span) => {
  const depth = spanDepths.value[span.span_id] || 0
  return padding.top + depth * barHeight
}

// Get span width
const getSpanWidth = (span) => {
  const durationMs = (span.duration_ns || 0) / 1_000_000
  const zoomStart = zoomLevel.value.start * totalDuration.value
  const zoomEnd = zoomLevel.value.end * totalDuration.value
  const zoomDuration = zoomEnd - zoomStart
  
  const relativeWidth = durationMs / zoomDuration
  return relativeWidth * (containerWidth.value - padding.left - padding.right)
}

// Get span color based on colorBy mode
const getSpanColor = (span) => {
  if (colorBy.value === 'service') {
    return serviceColors.value[span.service_name] || '#6B7280'
  }
  
  if (colorBy.value === 'status') {
    if (span.status_code === 'STATUS_CODE_ERROR') return '#EF4444'
    if (span.status_code === 'STATUS_CODE_OK') return '#10B981'
    return '#9CA3AF'
  }
  
  // Duration-based coloring
  const durationMs = (span.duration_ns || 0) / 1_000_000
  const ratio = Math.min(durationMs / totalDuration.value, 1)
  
  // Interpolate from green to red
  const r = Math.round(16 + ratio * (239 - 16))
  const g = Math.round(185 - ratio * (185 - 68))
  const b = Math.round(129 - ratio * (129 - 68))
  
  return `rgb(${r}, ${g}, ${b})`
}

// Get self time (duration minus children duration)
const getSelfTime = (span) => {
  const spanDuration = (span.duration_ns || 0) / 1_000_000
  const children = props.trace.spans.filter(s => s.parent_span_id === span.span_id)
  const childrenDuration = children.reduce((sum, child) => {
    return sum + (child.duration_ns || 0) / 1_000_000
  }, 0)
  return Math.max(0, spanDuration - childrenDuration)
}

// Get status class
const getStatusClass = (statusCode) => {
  if (statusCode === 'STATUS_CODE_ERROR') return 'text-red-500'
  if (statusCode === 'STATUS_CODE_OK') return 'text-green-500'
  return 'text-gray-500'
}

// Format duration
const formatDuration = (ms) => {
  if (ms < 1) return '<1ms'
  if (ms < 1000) return `${Math.round(ms)}ms`
  if (ms < 60000) return `${(ms / 1000).toFixed(2)}s`
  return `${(ms / 60000).toFixed(2)}m`
}

// Truncate text to fit width
const truncateText = (text, maxWidth) => {
  const charWidth = 7 // Approximate character width
  const maxChars = Math.floor(maxWidth / charWidth)
  if (text.length <= maxChars) return text
  return text.slice(0, maxChars - 3) + '...'
}

// Select a span
const selectSpan = (span) => {
  selectedSpan.value = span
  emit('select-span', span)
}

// Reset zoom
const resetZoom = () => {
  zoomLevel.value = { start: 0, end: 1 }
}

// Handle resize
const handleResize = () => {
  if (containerRef.value) {
    containerWidth.value = containerRef.value.clientWidth
  }
}

// Watch for selected span changes from props
watch(() => props.selectedSpanId, (newId) => {
  if (newId) {
    selectedSpan.value = props.trace.spans.find(s => s.span_id === newId)
  } else {
    selectedSpan.value = null
  }
})

onMounted(() => {
  handleResize()
  window.addEventListener('resize', handleResize)
})

onUnmounted(() => {
  window.removeEventListener('resize', handleResize)
})
</script>

<style scoped>
.trace-flamegraph {
  @apply w-full;
}

.flamegraph-controls {
  @apply flex items-center justify-between px-4 py-2 bg-gray-50 dark:bg-gray-900 border-b border-gray-200 dark:border-gray-700;
}

.control-btn {
  @apply flex items-center px-3 py-1.5 text-sm text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-100 hover:bg-gray-100 dark:hover:bg-gray-800 rounded-md disabled:opacity-50 disabled:cursor-not-allowed transition-colors;
}

.color-select {
  @apply px-2 py-1 text-sm bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 text-gray-900 dark:text-gray-100 rounded focus:ring-2 focus:ring-primary-500;
}

.flamegraph-container {
  @apply relative overflow-x-auto;
}

.flamegraph-svg {
  @apply block;
}

.span-bar {
  @apply cursor-pointer transition-opacity;
  opacity: 0.9;
}

.span-bar:hover,
.span-bar.hovered {
  opacity: 1;
  stroke: white;
  stroke-width: 2;
}

.span-bar.selected {
  stroke: #3B82F6;
  stroke-width: 2;
}

.span-label {
  @apply text-xs fill-white pointer-events-none;
  font-family: ui-monospace, monospace;
}

.time-tick {
  @apply text-xs fill-gray-500 dark:fill-gray-400;
  text-anchor: middle;
}

.flamegraph-tooltip {
  @apply absolute z-50 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg shadow-lg p-3 pointer-events-none;
  min-width: 200px;
}

.tooltip-header {
  @apply flex items-center justify-between gap-2 mb-2 pb-2 border-b border-gray-200 dark:border-gray-700;
}

.tooltip-name {
  @apply text-sm font-medium text-gray-900 dark:text-gray-100 truncate;
}

.tooltip-status {
  @apply text-xs font-medium px-1.5 py-0.5 rounded;
}

.tooltip-content {
  @apply space-y-1;
}

.tooltip-row {
  @apply flex items-center justify-between text-xs;
}

.tooltip-label {
  @apply text-gray-500 dark:text-gray-400;
}

.tooltip-value {
  @apply text-gray-900 dark:text-gray-100 font-medium;
}

.tooltip-hint {
  @apply mt-2 pt-2 border-t border-gray-200 dark:border-gray-700 text-xs text-gray-400 dark:text-gray-500 text-center;
}

.flamegraph-legend {
  @apply px-4 py-2 bg-gray-50 dark:bg-gray-900 border-t border-gray-200 dark:border-gray-700;
}

.legend-items {
  @apply flex flex-wrap items-center gap-4;
}

.legend-item {
  @apply flex items-center gap-1.5;
}

.legend-color {
  @apply w-3 h-3 rounded;
}

.legend-label {
  @apply text-xs text-gray-600 dark:text-gray-400;
}

.legend-gradient {
  @apply flex items-center gap-2;
}

.gradient-bar {
  @apply w-32 h-3 rounded;
  background: linear-gradient(to right, #10B981, #F59E0B, #EF4444);
}

.gradient-labels {
  @apply flex justify-between text-xs text-gray-500 dark:text-gray-400 w-32;
}
</style>
