<template>
  <div class="pipeline-graph" ref="containerRef">
    <svg
      :width="containerWidth"
      :height="containerHeight"
      class="graph-svg"
    >
      <defs>
        <pattern id="pipeline-grid" width="40" height="40" patternUnits="userSpaceOnUse">
          <path d="M 40 0 L 0 0 0 40" fill="none" stroke="currentColor" stroke-width="0.5" class="text-gray-200"/>
        </pattern>
      </defs>
      <rect width="100%" height="100%" fill="url(#pipeline-grid)" />

      <g :transform="`scale(${zoom})`">
        <!-- Edges -->
        <g class="edges">
          <g v-for="edge in computedEdges" :key="`${edge.source}-${edge.target}`">
            <path
              :d="edge.path"
              :stroke="getEdgeColor(edge)"
              stroke-width="2"
              fill="none"
              class="edge-path"
              :class="{ highlighted: isEdgeHighlighted(edge) }"
            />
            <polygon
              :points="edge.arrowPoints"
              :fill="getEdgeColor(edge)"
              :class="{ highlighted: isEdgeHighlighted(edge) }"
            />
            <text
              v-if="edge.label"
              :x="edge.labelX"
              :y="edge.labelY"
              class="edge-label"
            >
              {{ edge.label }}
            </text>
          </g>
        </g>

        <!-- Nodes -->
        <g class="nodes">
          <g
            v-for="node in computedNodes"
            :key="node.id"
            class="node-group"
            :transform="`translate(${node.x}, ${node.y})`"
            @mouseenter="hoveredNode = node"
            @mouseleave="hoveredNode = null"
          >
            <rect
              :x="-nodeWidth / 2"
              :y="-nodeHeight / 2"
              :width="nodeWidth"
              :height="nodeHeight"
              :rx="nodeRx(node)"
              :fill="getNodeFill(node)"
              :stroke="hoveredNode && hoveredNode.id === node.id ? '#3B82F6' : getNodeStroke(node)"
              :stroke-width="hoveredNode && hoveredNode.id === node.id ? 2.5 : 1.5"
              class="node-rect"
            />

            <!-- Icon area -->
            <g :transform="`translate(${-nodeWidth / 2 + 14}, -10)`">
              <!-- Source: database icon -->
              <g v-if="node.type === 'source'">
                <ellipse cx="10" cy="2" rx="8" ry="3" fill="none" :stroke="getIconColor(node)" stroke-width="1.5"/>
                <path d="M 2 2 L 2 16 Q 2 19 10 19 Q 18 19 18 16 L 18 2" fill="none" :stroke="getIconColor(node)" stroke-width="1.5"/>
                <ellipse cx="10" cy="16" rx="8" ry="3" fill="none" :stroke="getIconColor(node)" stroke-width="1.5"/>
              </g>
              <!-- Warehouse: cylinder icon -->
              <g v-else-if="node.type === 'warehouse'">
                <ellipse cx="10" cy="3" rx="9" ry="3.5" fill="none" :stroke="getIconColor(node)" stroke-width="1.5"/>
                <path d="M 1 3 L 1 17 Q 1 20.5 10 20.5 Q 19 20.5 19 17 L 19 3" fill="none" :stroke="getIconColor(node)" stroke-width="1.5"/>
                <ellipse cx="10" cy="17" rx="9" ry="3.5" fill="none" :stroke="getIconColor(node)" stroke-width="1.5"/>
                <ellipse cx="10" cy="10" rx="9" ry="3.5" fill="none" :stroke="getIconColor(node)" stroke-width="1" opacity="0.5"/>
              </g>
              <!-- UDF: function icon -->
              <g v-else-if="node.type === 'udf'">
                <text :fill="getIconColor(node)" font-size="16" font-weight="700" font-family="monospace" y="16">f(x)</text>
              </g>
              <!-- Sink: target icon -->
              <g v-else-if="node.type === 'sink'">
                <circle cx="10" cy="10" r="9" fill="none" :stroke="getIconColor(node)" stroke-width="1.5"/>
                <circle cx="10" cy="10" r="5" fill="none" :stroke="getIconColor(node)" stroke-width="1.5"/>
                <circle cx="10" cy="10" r="1.5" :fill="getIconColor(node)"/>
              </g>
            </g>

            <!-- Label -->
            <text
              :x="4"
              y="4"
              class="node-label"
            >
              {{ node.label }}
            </text>

            <!-- Subtitle -->
            <text
              v-if="getSubtitle(node)"
              :x="4"
              y="18"
              class="node-subtitle"
            >
              {{ getSubtitle(node) }}
            </text>

            <!-- Status indicator -->
            <circle
              v-if="node.status"
              :cx="nodeWidth / 2 - 12"
              :cy="-nodeHeight / 2 + 12"
              r="4"
              :fill="getStatusColor(node.status)"
            />
          </g>
        </g>
      </g>
    </svg>

    <!-- Hover Popover -->
    <div
      v-if="hoveredNode"
      class="node-popover"
      :style="popoverStyle"
    >
      <div class="popover-header">
        <span class="popover-type-badge" :style="{ backgroundColor: getNodeStroke(hoveredNode) }">
          {{ hoveredNode.type }}
        </span>
        <span class="popover-title">{{ hoveredNode.label }}</span>
      </div>
      <div class="popover-content">
        <div v-if="hoveredNode.source_type" class="popover-row">
          <span class="popover-label">Connector</span>
          <span class="popover-value">{{ hoveredNode.source_type }}</span>
        </div>
        <div v-if="hoveredNode.tier" class="popover-row">
          <span class="popover-label">Tier</span>
          <span class="popover-value">{{ hoveredNode.tier }}</span>
        </div>
        <div v-if="hoveredNode.status" class="popover-row">
          <span class="popover-label">Status</span>
          <span class="popover-value">{{ hoveredNode.status }}</span>
        </div>
        <div v-if="hoveredNode.schedule" class="popover-row">
          <span class="popover-label">Schedule</span>
          <span class="popover-value font-mono text-[11px]">{{ hoveredNode.schedule }}</span>
        </div>
        <div v-if="hoveredNode.last_sync_at" class="popover-row">
          <span class="popover-label">Last sync</span>
          <span class="popover-value">{{ formatTime(hoveredNode.last_sync_at) }}</span>
        </div>
      </div>
    </div>

    <!-- Zoom Controls -->
    <div class="zoom-controls">
      <button @click="zoomIn" class="zoom-btn" title="Zoom in">
        <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" />
        </svg>
      </button>
      <button @click="zoomOut" class="zoom-btn" title="Zoom out">
        <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M20 12H4" />
        </svg>
      </button>
      <button @click="resetView" class="zoom-btn" title="Reset">
        <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 8V4m0 0h4M4 4l5 5m11-1V4m0 0h-4m4 0l-5 5M4 16v4m0 0h4m-4 0l5-5m11 5l-5-5m5 5v-4m0 4h-4" />
        </svg>
      </button>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, watch, onMounted, onUnmounted } from 'vue'

const props = defineProps({
  nodes: { type: Array, default: () => [] },
  edges: { type: Array, default: () => [] },
})

const containerRef = ref(null)
const containerWidth = ref(900)
const containerHeight = ref(500)
const hoveredNode = ref(null)
const zoom = ref(1)

const nodeWidth = 160
const nodeHeight = 52
const columnGap = 220
const rowGap = 80
const paddingX = 100
const paddingY = 60

const nodeRx = (node) => node.type === 'warehouse' ? 14 : 8

// Assign each node to a column (x-rank) based on type
function assignColumns(nodes, edges) {
  const cols = {}
  for (const n of nodes) {
    if (n.type === 'source') cols[n.id] = 0
    else if (n.type === 'warehouse') cols[n.id] = 1
    else if (n.type === 'udf') cols[n.id] = 1
    else if (n.type === 'sink') cols[n.id] = 2
  }

  // If a source connects to both warehouse and a UDF, the UDF should still be col 1
  // but if a source only connects to a UDF (not to warehouse), keep source at 0
  // Adjust: if there's no warehouse node but there are UDFs, UDFs go to col 1
  return cols
}

// Group connected components for vertical stacking
function findConnectedComponents(nodes, edges) {
  const adj = {}
  for (const n of nodes) adj[n.id] = new Set()
  for (const e of edges) {
    if (adj[e.source]) adj[e.source].add(e.target)
    if (adj[e.target]) adj[e.target].add(e.source)
  }

  const visited = new Set()
  const components = []

  for (const n of nodes) {
    if (visited.has(n.id)) continue
    const component = []
    const stack = [n.id]
    while (stack.length) {
      const id = stack.pop()
      if (visited.has(id)) continue
      visited.add(id)
      component.push(id)
      for (const neighbor of (adj[id] || [])) {
        if (!visited.has(neighbor)) stack.push(neighbor)
      }
    }
    components.push(component)
  }

  return components
}

const computedNodes = computed(() => {
  if (props.nodes.length === 0) return []

  const cols = assignColumns(props.nodes, props.edges)
  const components = findConnectedComponents(props.nodes, props.edges)

  const nodeMap = {}
  for (const n of props.nodes) nodeMap[n.id] = { ...n }

  let globalY = paddingY
  const positions = {}

  for (const comp of components) {
    // Group nodes in this component by column
    const byCol = {}
    for (const id of comp) {
      const col = cols[id] ?? 1
      if (!byCol[col]) byCol[col] = []
      byCol[col].push(id)
    }

    const colKeys = Object.keys(byCol).map(Number).sort()
    let maxRowsInComp = 0
    for (const col of colKeys) {
      maxRowsInComp = Math.max(maxRowsInComp, byCol[col].length)
    }

    // Position nodes within this component
    for (const col of colKeys) {
      const nodeIds = byCol[col]
      const compHeight = nodeIds.length * (nodeHeight + rowGap) - rowGap
      const startY = globalY + (maxRowsInComp * (nodeHeight + rowGap) - rowGap - compHeight) / 2

      for (let i = 0; i < nodeIds.length; i++) {
        positions[nodeIds[i]] = {
          x: paddingX + col * (nodeWidth + columnGap) + nodeWidth / 2,
          y: startY + i * (nodeHeight + rowGap) + nodeHeight / 2,
        }
      }
    }

    globalY += maxRowsInComp * (nodeHeight + rowGap) + 40
  }

  return props.nodes.map(n => ({
    ...n,
    x: positions[n.id]?.x ?? 0,
    y: positions[n.id]?.y ?? 0,
  }))
})

watch(computedNodes, (nodes) => {
  if (nodes.length === 0) return
  const maxY = Math.max(...nodes.map(n => n.y)) + nodeHeight / 2 + paddingY
  if (maxY > containerHeight.value) {
    containerHeight.value = maxY
  }
}, { immediate: true })

const computedEdges = computed(() => {
  if (props.edges.length === 0 || computedNodes.value.length === 0) return []

  const nodeById = {}
  for (const n of computedNodes.value) nodeById[n.id] = n

  return props.edges.map(edge => {
    const src = nodeById[edge.source]
    const tgt = nodeById[edge.target]
    if (!src || !tgt) return null

    const startX = src.x + nodeWidth / 2
    const startY = src.y
    const endX = tgt.x - nodeWidth / 2
    const endY = tgt.y

    const dx = endX - startX
    const ctrlOffset = Math.abs(dx) * 0.35

    const c1x = startX + ctrlOffset
    const c1y = startY
    const c2x = endX - ctrlOffset
    const c2y = endY

    const arrowSize = 7
    const arrowAngle = Math.atan2(endY - c2y, endX - c2x)
    const arrowPoints = `
      ${endX},${endY}
      ${endX - arrowSize * Math.cos(arrowAngle - Math.PI / 6)},${endY - arrowSize * Math.sin(arrowAngle - Math.PI / 6)}
      ${endX - arrowSize * Math.cos(arrowAngle + Math.PI / 6)},${endY - arrowSize * Math.sin(arrowAngle + Math.PI / 6)}
    `

    const labelX = (startX + endX) / 2
    const labelY = (startY + endY) / 2 - 8

    return {
      source: edge.source,
      target: edge.target,
      label: edge.label,
      path: `M ${startX} ${startY} C ${c1x} ${c1y}, ${c2x} ${c2y}, ${endX} ${endY}`,
      arrowPoints,
      labelX,
      labelY,
    }
  }).filter(Boolean)
})

const isEdgeHighlighted = (edge) => {
  if (!hoveredNode.value) return false
  return edge.source === hoveredNode.value.id || edge.target === hoveredNode.value.id
}

const popoverStyle = computed(() => {
  if (!hoveredNode.value) return {}
  return {
    left: `${(hoveredNode.value.x + nodeWidth / 2 + 16) * zoom.value}px`,
    top: `${(hoveredNode.value.y - 40) * zoom.value}px`,
  }
})

// Colors
const nodeColors = {
  source: { fill: '#EFF6FF', stroke: '#3B82F6', icon: '#2563EB' },       // blue
  warehouse: { fill: '#F0FDFA', stroke: '#14B8A6', icon: '#0D9488' },    // teal
  udf: { fill: '#FFF7ED', stroke: '#F97316', icon: '#EA580C' },          // orange
  sink: { fill: '#FDF2F8', stroke: '#EC4899', icon: '#DB2777' },         // pink
}
const nodeColorsDark = {
  source: { fill: '#1E3A5F', stroke: '#60A5FA', icon: '#93C5FD' },
  warehouse: { fill: '#134E4A', stroke: '#2DD4BF', icon: '#5EEAD4' },
  udf: { fill: '#431407', stroke: '#FB923C', icon: '#FDBA74' },
  sink: { fill: '#500724', stroke: '#F472B6', icon: '#F9A8D4' },
}

const isDark = ref(false)
const updateDarkMode = () => {
  isDark.value = document.documentElement.classList.contains('dark')
}

const palette = computed(() => isDark.value ? nodeColorsDark : nodeColors)

const getNodeFill = (node) => (palette.value[node.type] || palette.value.source).fill
const getNodeStroke = (node) => (palette.value[node.type] || palette.value.source).stroke
const getIconColor = (node) => (palette.value[node.type] || palette.value.source).icon

const getEdgeColor = (edge) => {
  if (!hoveredNode.value) return isDark.value ? '#4B5563' : '#9CA3AF'
  if (isEdgeHighlighted(edge)) return '#3B82F6'
  return isDark.value ? '#374151' : '#D1D5DB'
}

const getStatusColor = (status) => {
  const map = { active: '#10B981', syncing: '#3B82F6', disabled: '#6B7280', external: '#F59E0B' }
  return map[status] || '#6B7280'
}

const getSubtitle = (node) => {
  if (node.type === 'source' && node.source_type) return node.source_type
  if (node.type === 'udf' && node.schedule) return node.schedule
  if (node.type === 'warehouse') return 'ClickHouse'
  return null
}

const formatTime = (iso) => {
  if (!iso) return ''
  const d = new Date(iso)
  const now = new Date()
  const diff = Math.floor((now - d) / 1000)
  if (diff < 60) return `${diff}s ago`
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`
  return `${Math.floor(diff / 86400)}d ago`
}

// Zoom
const zoomIn = () => { zoom.value = Math.min(zoom.value * 1.2, 2) }
const zoomOut = () => { zoom.value = Math.max(zoom.value / 1.2, 0.4) }
const resetView = () => { zoom.value = 1 }

// Resize
const handleResize = () => {
  if (containerRef.value) {
    containerWidth.value = containerRef.value.clientWidth
    const minH = Math.max(containerRef.value.clientHeight, 400)
    if (containerHeight.value < minH) containerHeight.value = minH
  }
}

let darkModeObserver = null

onMounted(() => {
  handleResize()
  updateDarkMode()
  window.addEventListener('resize', handleResize)
  darkModeObserver = new MutationObserver(updateDarkMode)
  darkModeObserver.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] })
})

onUnmounted(() => {
  window.removeEventListener('resize', handleResize)
  if (darkModeObserver) {
    darkModeObserver.disconnect()
    darkModeObserver = null
  }
})
</script>

<style scoped>
.pipeline-graph {
  @apply relative w-full min-h-[400px] bg-white rounded-lg overflow-hidden border border-gray-200;
}

.graph-svg {
  @apply block;
}

.edge-path {
  @apply transition-opacity;
  opacity: 0.5;
}

.edge-path.highlighted {
  opacity: 1;
  stroke-width: 3;
}

.edge-label {
  @apply text-[10px] fill-gray-400;
  font-family: ui-monospace, monospace;
  text-anchor: middle;
}

.node-group {
  @apply cursor-pointer;
}

.node-rect {
  @apply transition-all;
  filter: drop-shadow(0 1px 3px rgba(0, 0, 0, 0.08));
}

.node-group:hover .node-rect {
  filter: drop-shadow(0 4px 8px rgba(0, 0, 0, 0.15));
}

.node-label {
  @apply text-[13px] font-semibold fill-gray-800;
}

.node-subtitle {
  @apply text-[10px] fill-gray-500;
  font-family: ui-monospace, monospace;
}

.node-popover {
  @apply absolute z-50 bg-white border border-gray-200 rounded-lg shadow-xl p-3 min-w-[200px] pointer-events-none;
}

.popover-header {
  @apply flex items-center gap-2 pb-2 border-b border-gray-200;
}

.popover-type-badge {
  @apply text-[10px] font-bold uppercase text-white px-1.5 py-0.5 rounded;
}

.popover-title {
  @apply text-sm font-semibold text-gray-900;
}

.popover-content {
  @apply py-2 space-y-1.5;
}

.popover-row {
  @apply flex items-center justify-between;
}

.popover-label {
  @apply text-xs text-gray-500;
}

.popover-value {
  @apply text-xs font-medium text-gray-900;
}

.zoom-controls {
  @apply absolute bottom-4 right-4 flex flex-col gap-1 bg-white border border-gray-200 rounded-lg shadow-lg p-1;
}

.zoom-btn {
  @apply p-2 text-gray-600 hover:text-gray-900 hover:bg-gray-100 rounded transition-colors;
}
</style>
