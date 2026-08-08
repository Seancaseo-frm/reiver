<template>
  <div class="metrics-treemap" ref="containerRef">
    <svg :width="width" :height="height" class="treemap-svg">
      <g
        v-for="(node, index) in treemapNodes"
        :key="node.name"
        class="treemap-node"
        @click="$emit('select-metric', node.data)"
        @mouseenter="hoveredNode = node"
        @mouseleave="hoveredNode = null"
      >
        <rect
          :x="node.x0"
          :y="node.y0"
          :width="node.x1 - node.x0"
          :height="node.y1 - node.y0"
          :fill="getNodeColor(node, index)"
          :stroke="hoveredNode === node ? '#3B82F6' : 'white'"
          :stroke-width="hoveredNode === node ? 2 : 1"
          rx="4"
          class="node-rect"
        />
        <text
          v-if="(node.x1 - node.x0) > 60 && (node.y1 - node.y0) > 30"
          :x="node.x0 + 8"
          :y="node.y0 + 20"
          class="node-label"
        >
          {{ truncateText(node.data.name, node.x1 - node.x0 - 16) }}
        </text>
        <text
          v-if="(node.x1 - node.x0) > 60 && (node.y1 - node.y0) > 50"
          :x="node.x0 + 8"
          :y="node.y0 + 38"
          class="node-value"
        >
          {{ formatNumber(node.data.series_count) }} series
        </text>
      </g>
    </svg>

    <!-- Tooltip -->
    <div
      v-if="hoveredNode"
      class="treemap-tooltip"
      :style="tooltipStyle"
    >
      <div class="tooltip-name">{{ hoveredNode.data.name }}</div>
      <div class="tooltip-stats">
        <div class="stat">
          <span class="stat-label">Series:</span>
          <span class="stat-value">{{ formatNumber(hoveredNode.data.series_count) }}</span>
        </div>
        <div class="stat">
          <span class="stat-label">Type:</span>
          <span class="stat-value">{{ hoveredNode.data.metric_type }}</span>
        </div>
        <div v-if="hoveredNode.data.label_keys?.length" class="stat">
          <span class="stat-label">Labels:</span>
          <span class="stat-value">{{ hoveredNode.data.label_keys.length }}</span>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted, watch } from 'vue'

const props = defineProps({
  metrics: {
    type: Array,
    default: () => [],
  },
})

const emit = defineEmits(['select-metric'])

const containerRef = ref(null)
const width = ref(800)
const height = ref(400)
const hoveredNode = ref(null)

// Color palette for metrics
const colorPalette = [
  '#8B5CF6', '#3B82F6', '#10B981', '#F59E0B', '#EF4444',
  '#EC4899', '#6366F1', '#14B8A6', '#F97316', '#84CC16',
  '#A855F7', '#0EA5E9', '#22C55E', '#EAB308', '#F43F5E',
]

// Simple treemap layout algorithm
const treemapNodes = computed(() => {
  if (props.metrics.length === 0) return []

  // Sort by series count descending
  const sorted = [...props.metrics]
    .sort((a, b) => (b.series_count || 0) - (a.series_count || 0))
    .slice(0, 50) // Limit to 50 for performance

  const totalValue = sorted.reduce((sum, m) => sum + (m.series_count || 1), 0)
  
  // Simple row-based layout
  const nodes = []
  let currentY = 0
  let rowHeight = 0
  let rowX = 0
  let rowItems = []
  let rowValue = 0
  const targetRowWidth = width.value
  
  sorted.forEach((metric, index) => {
    const value = metric.series_count || 1
    const ratio = value / totalValue
    const area = ratio * width.value * height.value
    
    // Calculate dimensions
    const itemWidth = Math.sqrt(area * (width.value / height.value))
    const itemHeight = area / itemWidth
    
    // Check if we need a new row
    if (rowX + itemWidth > targetRowWidth && rowItems.length > 0) {
      // Finalize previous row
      let x = 0
      rowItems.forEach(item => {
        item.x0 = x
        item.x1 = x + item.width
        item.y0 = currentY
        item.y1 = currentY + rowHeight
        x += item.width
        nodes.push(item)
      })
      
      currentY += rowHeight
      rowX = 0
      rowItems = []
      rowValue = 0
    }
    
    // Add to current row
    rowItems.push({
      data: metric,
      name: metric.name,
      width: itemWidth,
      height: itemHeight,
    })
    rowHeight = Math.max(rowHeight, itemHeight)
    rowX += itemWidth
    rowValue += value
  })
  
  // Finalize last row
  if (rowItems.length > 0) {
    let x = 0
    rowItems.forEach(item => {
      item.x0 = x
      item.x1 = Math.min(x + item.width, width.value)
      item.y0 = currentY
      item.y1 = Math.min(currentY + rowHeight, height.value)
      x += item.width
      nodes.push(item)
    })
  }
  
  return nodes
})

// Tooltip position
const tooltipStyle = computed(() => {
  if (!hoveredNode.value) return {}
  
  const x = (hoveredNode.value.x0 + hoveredNode.value.x1) / 2
  const y = hoveredNode.value.y0
  
  return {
    left: `${Math.min(x, width.value - 180)}px`,
    top: `${y - 10}px`,
    transform: 'translateY(-100%)',
  }
})

const getNodeColor = (node, index) => {
  // Color by metric type
  const typeColors = {
    counter: '#3B82F6',
    gauge: '#10B981',
    histogram: '#8B5CF6',
    summary: '#F59E0B',
  }
  
  const baseColor = typeColors[node.data.metric_type] || colorPalette[index % colorPalette.length]
  
  // Vary opacity based on series count
  const maxSeries = Math.max(...props.metrics.map(m => m.series_count || 0))
  const ratio = (node.data.series_count || 0) / maxSeries
  const opacity = 0.4 + ratio * 0.6
  
  return baseColor + Math.round(opacity * 255).toString(16).padStart(2, '0')
}

const truncateText = (text, maxWidth) => {
  const charWidth = 7
  const maxChars = Math.floor(maxWidth / charWidth)
  if (text.length <= maxChars) return text
  return text.slice(0, maxChars - 3) + '...'
}

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
    height.value = Math.max(containerRef.value.clientHeight, 400)
  }
}

onMounted(() => {
  handleResize()
  window.addEventListener('resize', handleResize)
})

onUnmounted(() => {
  window.removeEventListener('resize', handleResize)
})

watch(() => props.metrics, handleResize, { deep: true })
</script>

<style scoped>
.metrics-treemap {
  @apply relative w-full min-h-[400px];
}

.treemap-svg {
  @apply block;
}

.treemap-node {
  @apply cursor-pointer;
}

.node-rect {
  @apply transition-all;
}

.node-label {
  @apply text-xs font-medium fill-white pointer-events-none;
  text-shadow: 0 1px 2px rgba(0, 0, 0, 0.3);
}

.node-value {
  @apply text-[10px] fill-white opacity-80 pointer-events-none;
  text-shadow: 0 1px 2px rgba(0, 0, 0, 0.3);
}

.treemap-tooltip {
  @apply absolute z-50 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg shadow-xl p-3 pointer-events-none min-w-[160px];
}

.tooltip-name {
  @apply text-sm font-medium text-gray-900 dark:text-gray-100 truncate mb-2;
}

.tooltip-stats {
  @apply space-y-1;
}

.stat {
  @apply flex items-center justify-between text-xs;
}

.stat-label {
  @apply text-gray-500 dark:text-gray-400;
}

.stat-value {
  @apply text-gray-900 dark:text-gray-100 font-medium;
}
</style>
