<template>
  <div class="service-map" ref="containerRef">
    <svg
      :width="containerWidth"
      :height="containerHeight"
      class="map-svg"
    >
      <!-- Background grid -->
      <defs>
        <pattern id="grid" width="40" height="40" patternUnits="userSpaceOnUse">
          <path d="M 40 0 L 0 0 0 40" fill="none" stroke="currentColor" stroke-width="0.5" class="text-gray-200"/>
        </pattern>
      </defs>
      <rect width="100%" height="100%" fill="url(#grid)" />

      <!-- Edges (dependencies) -->
      <g class="edges">
        <g v-for="edge in computedEdges" :key="`${edge.source}-${edge.target}`">
          <path
            :d="edge.path"
            :stroke="getEdgeColor(edge)"
            stroke-width="2"
            fill="none"
            class="edge-path"
            :class="{ highlighted: hoveredService && (hoveredService === edge.source || hoveredService === edge.target) }"
          />
          <!-- Arrow head -->
          <polygon
            :points="edge.arrowPoints"
            :fill="getEdgeColor(edge)"
          />
          <!-- Edge label (request count) -->
          <text
            v-if="edge.requestCount"
            :x="edge.labelX"
            :y="edge.labelY"
            class="edge-label"
          >
            {{ formatNumber(edge.requestCount) }}/s
          </text>
        </g>
      </g>

      <!-- Nodes (services) -->
      <g class="nodes">
        <g
          v-for="node in computedNodes"
          :key="node.name"
          class="node-group"
          :transform="`translate(${node.x}, ${node.y})`"
          @mouseenter="hoveredService = node.name"
          @mouseleave="hoveredService = null"
          @click="$emit('select-service', node)"
        >
          <!-- Node circle -->
          <circle
            :r="nodeRadius"
            :fill="getNodeColor(node)"
            :stroke="hoveredService === node.name ? '#3B82F6' : 'white'"
            :stroke-width="hoveredService === node.name ? 3 : 2"
            class="node-circle"
          />
          
          <!-- Service icon -->
          <g :transform="`translate(-12, -12)`">
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2">
              <rect x="2" y="2" width="20" height="8" rx="2" ry="2"></rect>
              <rect x="2" y="14" width="20" height="8" rx="2" ry="2"></rect>
              <line x1="6" y1="6" x2="6.01" y2="6"></line>
              <line x1="6" y1="18" x2="6.01" y2="18"></line>
            </svg>
          </g>

          <!-- Service name -->
          <text
            :y="nodeRadius + 20"
            class="node-label"
          >
            {{ node.name }}
          </text>

          <!-- Metrics badge -->
          <g :transform="`translate(${nodeRadius - 10}, ${-nodeRadius + 10})`">
            <rect
              v-if="node.errorRate > 0.01"
              x="-12"
              y="-10"
              width="24"
              height="20"
              rx="4"
              fill="#EF4444"
            />
            <text
              v-if="node.errorRate > 0.01"
              class="error-badge"
            >
              {{ formatPercent(node.errorRate) }}
            </text>
          </g>
        </g>
      </g>
    </svg>

    <!-- Service Details Popover -->
    <div
      v-if="hoveredService && hoveredNode"
      class="service-popover"
      :style="popoverStyle"
    >
      <div class="popover-header">
        <div
          :class="['status-dot', getHealthClass(hoveredNode.health)]"
        ></div>
        <span class="popover-title">{{ hoveredNode.name }}</span>
      </div>
      <div class="popover-content">
        <div class="metric-row">
          <span class="metric-label">Request Rate</span>
          <span class="metric-value">{{ formatNumber(hoveredNode.requestRate) }}/s</span>
        </div>
        <div class="metric-row">
          <span class="metric-label">Error Rate</span>
          <span :class="['metric-value', getErrorRateClass(hoveredNode.errorRate)]">
            {{ formatPercent(hoveredNode.errorRate) }}
          </span>
        </div>
        <div class="metric-row">
          <span class="metric-label">P50 Latency</span>
          <span class="metric-value">{{ formatDuration(hoveredNode.p50Latency) }}</span>
        </div>
        <div class="metric-row">
          <span class="metric-label">P99 Latency</span>
          <span class="metric-value">{{ formatDuration(hoveredNode.p99Latency) }}</span>
        </div>
      </div>
      <div class="popover-footer">
        Click to view details
      </div>
    </div>

    <!-- Zoom Controls -->
    <div class="zoom-controls">
      <button @click="zoomIn" class="zoom-btn">
        <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" />
        </svg>
      </button>
      <button @click="zoomOut" class="zoom-btn">
        <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M20 12H4" />
        </svg>
      </button>
      <button @click="resetView" class="zoom-btn">
        <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 8V4m0 0h4M4 4l5 5m11-1V4m0 0h-4m4 0l-5 5M4 16v4m0 0h4m-4 0l5-5m11 5l-5-5m5 5v-4m0 4h-4" />
        </svg>
      </button>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted, watch } from 'vue'

const props = defineProps({
  services: {
    type: Array,
    default: () => [],
  },
  dependencies: {
    type: Array,
    default: () => [],
  },
})

const emit = defineEmits(['select-service'])

const containerRef = ref(null)
const containerWidth = ref(800)
const containerHeight = ref(500)
const hoveredService = ref(null)
const zoom = ref(1)

const nodeRadius = 35
const padding = 80

// Compute node positions using force-directed layout simulation
const computedNodes = computed(() => {
  if (props.services.length === 0) return []
  
  const nodes = props.services.map((service, index) => {
    // Simple circular layout for now
    const angle = (2 * Math.PI * index) / props.services.length
    const radius = Math.min(containerWidth.value, containerHeight.value) / 3
    
    return {
      ...service,
      x: containerWidth.value / 2 + radius * Math.cos(angle) * zoom.value,
      y: containerHeight.value / 2 + radius * Math.sin(angle) * zoom.value,
    }
  })
  
  return nodes
})

// Compute edge paths
const computedEdges = computed(() => {
  if (props.dependencies.length === 0 || computedNodes.value.length === 0) return []
  
  return props.dependencies.map(dep => {
    const sourceNode = computedNodes.value.find(n => n.name === dep.source)
    const targetNode = computedNodes.value.find(n => n.name === dep.target)
    
    if (!sourceNode || !targetNode) return null
    
    // Calculate path
    const dx = targetNode.x - sourceNode.x
    const dy = targetNode.y - sourceNode.y
    const distance = Math.sqrt(dx * dx + dy * dy)
    
    // Offset for node radius
    const offsetX = (dx / distance) * nodeRadius
    const offsetY = (dy / distance) * nodeRadius
    
    const startX = sourceNode.x + offsetX
    const startY = sourceNode.y + offsetY
    const endX = targetNode.x - offsetX
    const endY = targetNode.y - offsetY
    
    // Control point for curve
    const midX = (startX + endX) / 2
    const midY = (startY + endY) / 2
    const curvature = 0.2
    const controlX = midX - (dy * curvature)
    const controlY = midY + (dx * curvature)
    
    // Arrow head points
    const arrowSize = 8
    const arrowAngle = Math.atan2(endY - controlY, endX - controlX)
    const arrowPoints = `
      ${endX},${endY}
      ${endX - arrowSize * Math.cos(arrowAngle - Math.PI / 6)},${endY - arrowSize * Math.sin(arrowAngle - Math.PI / 6)}
      ${endX - arrowSize * Math.cos(arrowAngle + Math.PI / 6)},${endY - arrowSize * Math.sin(arrowAngle + Math.PI / 6)}
    `
    
    return {
      source: dep.source,
      target: dep.target,
      path: `M ${startX} ${startY} Q ${controlX} ${controlY} ${endX} ${endY}`,
      arrowPoints,
      labelX: midX,
      labelY: midY - 10,
      requestCount: dep.requestCount,
      errorRate: dep.errorRate,
    }
  }).filter(Boolean)
})

// Hovered node details
const hoveredNode = computed(() => {
  if (!hoveredService.value) return null
  return computedNodes.value.find(n => n.name === hoveredService.value)
})

// Popover position
const popoverStyle = computed(() => {
  if (!hoveredNode.value) return {}
  return {
    left: `${hoveredNode.value.x + nodeRadius + 20}px`,
    top: `${hoveredNode.value.y - 50}px`,
  }
})

// Zoom controls
const zoomIn = () => {
  zoom.value = Math.min(zoom.value * 1.2, 2)
}

const zoomOut = () => {
  zoom.value = Math.max(zoom.value / 1.2, 0.5)
}

const resetView = () => {
  zoom.value = 1
}

// Styling helpers
const getNodeColor = (node) => {
  const colors = {
    healthy: '#10B981',
    degraded: '#F59E0B',
    unhealthy: '#EF4444',
  }
  return colors[node.health] || '#6B7280'
}

const getEdgeColor = (edge) => {
  if (edge.errorRate > 0.05) return '#EF4444'
  if (edge.errorRate > 0.01) return '#F59E0B'
  return '#9CA3AF'
}

const getHealthClass = (health) => {
  const classes = {
    healthy: 'bg-green-500',
    degraded: 'bg-yellow-500',
    unhealthy: 'bg-red-500',
  }
  return classes[health] || 'bg-gray-400'
}

const getErrorRateClass = (rate) => {
  if (!rate || rate < 0.01) return 'text-green-600'
  if (rate < 0.05) return 'text-yellow-600'
  return 'text-red-600'
}

// Formatting
const formatNumber = (num) => {
  if (num === undefined || num === null) return '0'
  if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`
  if (num >= 1000) return `${(num / 1000).toFixed(1)}K`
  return num.toFixed(1)
}

const formatPercent = (num) => {
  if (num === undefined || num === null) return '0%'
  return `${(num * 100).toFixed(1)}%`
}

const formatDuration = (ms) => {
  if (ms === undefined || ms === null) return '0ms'
  if (ms < 1) return '<1ms'
  if (ms < 1000) return `${Math.round(ms)}ms`
  return `${(ms / 1000).toFixed(2)}s`
}

// Resize handler
const handleResize = () => {
  if (containerRef.value) {
    containerWidth.value = containerRef.value.clientWidth
    containerHeight.value = Math.max(containerRef.value.clientHeight, 400)
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
.service-map {
  @apply relative w-full min-h-[400px] bg-white rounded-lg overflow-hidden;
}

.map-svg {
  @apply block;
}

.edge-path {
  @apply transition-opacity;
  opacity: 0.6;
}

.edge-path.highlighted {
  opacity: 1;
  stroke-width: 3;
}

.edge-label {
  @apply text-xs fill-gray-500;
  font-family: ui-monospace, monospace;
}

.node-group {
  @apply cursor-pointer;
}

.node-circle {
  @apply transition-all;
  filter: drop-shadow(0 2px 4px rgba(0, 0, 0, 0.1));
}

.node-group:hover .node-circle {
  filter: drop-shadow(0 4px 8px rgba(0, 0, 0, 0.2));
}

.node-label {
  @apply text-sm font-medium fill-gray-900;
  text-anchor: middle;
}

.error-badge {
  @apply text-[10px] font-bold fill-white;
  text-anchor: middle;
  dominant-baseline: middle;
}

.service-popover {
  @apply absolute z-50 bg-white border border-gray-200 rounded-lg shadow-xl p-3 min-w-[180px];
}

.popover-header {
  @apply flex items-center gap-2 pb-2 border-b border-gray-200;
}

.status-dot {
  @apply w-2 h-2 rounded-full;
}

.popover-title {
  @apply text-sm font-semibold text-gray-900;
}

.popover-content {
  @apply py-2 space-y-1.5;
}

.metric-row {
  @apply flex items-center justify-between;
}

.metric-label {
  @apply text-xs text-gray-500;
}

.metric-value {
  @apply text-xs font-medium text-gray-900;
}

.popover-footer {
  @apply pt-2 border-t border-gray-200 text-xs text-gray-400 text-center;
}

.zoom-controls {
  @apply absolute bottom-4 right-4 flex flex-col gap-1 bg-white border border-gray-200 rounded-lg shadow-lg p-1;
}

.zoom-btn {
  @apply p-2 text-gray-600 hover:text-gray-900 hover:bg-gray-100 rounded transition-colors;
}
</style>
