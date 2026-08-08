<template>
  <div class="profile-diff-flamegraph" ref="containerRef">
    <!-- Controls -->
    <div class="flamegraph-controls border-b border-gray-200 bg-white px-4 py-2 flex items-center justify-between">
      <div class="flex items-center gap-3">
        <button
          @click="resetZoom"
          :disabled="!zoomStack.length"
          class="inline-flex items-center px-3 py-1.5 text-sm font-medium rounded-md border border-gray-300 text-gray-700 hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
        >
          Reset Zoom
        </button>
        <div class="relative">
          <input
            v-model="searchQuery"
            type="text"
            placeholder="Search functions..."
            class="pl-3 pr-3 py-1.5 w-64 text-sm border border-gray-300 rounded-md bg-white text-gray-900 placeholder-gray-400 focus:ring-2 focus:ring-primary-500 focus:border-primary-500"
          />
        </div>
        <span v-if="searchQuery && searchMatchCount > 0" class="text-xs text-gray-500">
          {{ searchMatchCount }} match{{ searchMatchCount !== 1 ? 'es' : '' }}
        </span>
      </div>
      <!-- Legend -->
      <div class="flex items-center gap-4 text-xs text-gray-500">
        <span class="flex items-center gap-1"><span class="w-3 h-3 rounded bg-red-400 inline-block"></span> Slower</span>
        <span class="flex items-center gap-1"><span class="w-3 h-3 rounded bg-green-400 inline-block"></span> Faster</span>
        <span class="flex items-center gap-1"><span class="w-3 h-3 rounded bg-purple-400 inline-block"></span> Removed</span>
        <span class="flex items-center gap-1"><span class="w-3 h-3 rounded bg-blue-400 inline-block"></span> New</span>
      </div>
    </div>

    <!-- SVG -->
    <div class="flamegraph-canvas overflow-x-auto" ref="canvasRef">
      <svg
        v-if="layoutNodes.length"
        :width="svgWidth"
        :height="svgHeight"
        class="block"
        @mouseleave="hoveredNode = null"
      >
        <g v-for="node in layoutNodes" :key="node.id">
          <rect
            :x="node.x"
            :y="node.y"
            :width="Math.max(node.width, 1)"
            :height="barHeight - 1"
            :fill="getDiffFill(node)"
            :stroke="hoveredNode === node ? '#fff' : 'rgba(0,0,0,0.1)'"
            :stroke-width="hoveredNode === node ? 2 : 0.5"
            class="cursor-pointer"
            :opacity="getNodeOpacity(node)"
            @click="zoomInto(node)"
            @mouseenter="hoveredNode = node"
            @mouseleave="hoveredNode = null"
          />
          <text
            v-if="node.width > 40"
            :x="node.x + 4"
            :y="node.y + barHeight / 2 + 4"
            class="text-[11px] fill-gray-900 pointer-events-none select-none"
          >
            {{ truncateLabel(node.name, node.width) }}
          </text>
        </g>
      </svg>
      <div v-else class="text-center py-12 text-gray-500">No diff data</div>
    </div>

    <!-- Tooltip -->
    <div
      v-if="hoveredNode"
      class="flamegraph-tooltip"
      :style="tooltipStyle"
    >
      <p class="font-medium text-sm text-gray-900 break-all">{{ hoveredNode.name }}</p>
      <div class="mt-1 space-y-0.5 text-xs text-gray-600">
        <p>Baseline: {{ formatSamples(hoveredNode.value_a) }}</p>
        <p>Target: {{ formatSamples(hoveredNode.value_b) }}</p>
        <p :class="hoveredNode.diff > 0 ? 'text-red-600' : hoveredNode.diff < 0 ? 'text-green-600' : 'text-gray-500'">
          Diff: {{ hoveredNode.diff > 0 ? '+' : '' }}{{ formatSamples(hoveredNode.diff) }}
          <template v-if="hoveredNode.value_a > 0">
            ({{ hoveredNode.diff > 0 ? '+' : '' }}{{ ((hoveredNode.diff / hoveredNode.value_a) * 100).toFixed(1) }}%)
          </template>
        </p>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted } from 'vue'

const props = defineProps({
  diffFlameGraph: { type: Object, default: null },
})

const containerRef = ref(null)
const canvasRef = ref(null)
const hoveredNode = ref(null)
const searchQuery = ref('')
const zoomStack = ref([])
const barHeight = 20
const svgWidth = ref(1200)

let resizeObserver = null
onMounted(() => {
  if (canvasRef.value) {
    svgWidth.value = canvasRef.value.clientWidth || 1200
    resizeObserver = new ResizeObserver((entries) => {
      for (const entry of entries) svgWidth.value = entry.contentRect.width || 1200
    })
    resizeObserver.observe(canvasRef.value)
  }
})
onUnmounted(() => { if (resizeObserver) resizeObserver.disconnect() })

const currentRoot = computed(() => {
  if (zoomStack.value.length > 0) return zoomStack.value[zoomStack.value.length - 1]
  return props.diffFlameGraph?.root || null
})

const maxValue = computed(() => {
  const r = currentRoot.value
  return r ? Math.max(r.value_a || 0, r.value_b || 0, 1) : 1
})

const layoutNodes = computed(() => {
  const root = currentRoot.value
  if (!root) return []
  const nodes = []
  const width = svgWidth.value
  let idCounter = 0

  const traverse = (node, x, y, parentWidth) => {
    const nodeMax = Math.max(node.value_a || 0, node.value_b || 0)
    const w = parentWidth * (nodeMax / maxValue.value)
    if (w < 0.5) return

    nodes.push({
      id: idCounter++,
      name: node.name,
      value_a: node.value_a || 0,
      value_b: node.value_b || 0,
      diff: node.diff || 0,
      x, y,
      width: w,
      children: node.children,
      _raw: node,
    })

    let childX = x
    if (node.children) {
      const sorted = [...node.children].sort((a, b) => Math.max(b.value_a || 0, b.value_b || 0) - Math.max(a.value_a || 0, a.value_b || 0))
      for (const child of sorted) {
        const childMax = Math.max(child.value_a || 0, child.value_b || 0)
        const childW = w * (childMax / (nodeMax || 1))
        if (childW >= 0.5) {
          traverse(child, childX, y + barHeight, w)
          childX += childW
        }
      }
    }
  }

  traverse(root, 0, 0, width)
  return nodes
})

const maxDepth = computed(() => {
  let max = 0
  for (const n of layoutNodes.value) {
    const depth = Math.round(n.y / barHeight)
    if (depth > max) max = depth
  }
  return max + 1
})

const svgHeight = computed(() => maxDepth.value * barHeight + 4)

const searchMatchCount = computed(() => {
  if (!searchQuery.value) return 0
  const q = searchQuery.value.toLowerCase()
  return layoutNodes.value.filter(n => n.name.toLowerCase().includes(q)).length
})

const getDiffFill = (node) => {
  if (searchQuery.value && node.name.toLowerCase().includes(searchQuery.value.toLowerCase())) return '#a855f7'
  if (node.name === 'root') return '#6b7280'

  if (node.value_a === 0 && node.value_b > 0) return '#60a5fa' // blue = new
  if (node.value_b === 0 && node.value_a > 0) return '#a78bfa' // purple = removed

  const diff = node.diff || 0
  const maxVal = Math.max(node.value_a, node.value_b, 1)
  const intensity = Math.min(Math.abs(diff) / maxVal, 1)

  if (diff > 0) {
    const l = 70 - intensity * 30
    return `hsl(0, 70%, ${l}%)`
  } else if (diff < 0) {
    const l = 70 - intensity * 30
    return `hsl(120, 60%, ${l}%)`
  }
  return '#d1d5db'
}

const getNodeOpacity = (node) => {
  if (searchQuery.value && !node.name.toLowerCase().includes(searchQuery.value.toLowerCase())) return 0.35
  return 1
}

const zoomInto = (node) => {
  if (node._raw?.children?.length > 0) zoomStack.value.push(node._raw)
}

const resetZoom = () => { zoomStack.value = [] }

const truncateLabel = (name, width) => {
  const charWidth = 7
  const maxChars = Math.floor((width - 8) / charWidth)
  if (maxChars <= 0) return ''
  if (name.length <= maxChars) return name
  return name.substring(0, maxChars - 1) + '\u2026'
}

const tooltipStyle = computed(() => {
  if (!hoveredNode.value || !canvasRef.value) return { display: 'none' }
  const node = hoveredNode.value
  const rect = canvasRef.value.getBoundingClientRect()
  let left = node.x + node.width / 2
  let top = node.y + barHeight + 8
  if (left + 200 > rect.width) left = rect.width - 220
  if (left < 0) left = 10
  return { position: 'absolute', left: `${left}px`, top: `${top}px`, zIndex: 50 }
})

const formatSamples = (n) => {
  if (n == null) return '--'
  return n.toLocaleString()
}
</script>

<style scoped>
.profile-diff-flamegraph {
  position: relative;
  background: white;
}

.flamegraph-canvas {
  position: relative;
  min-height: 200px;
}

.flamegraph-tooltip {
  position: absolute;
  background: white;
  border: 1px solid #e5e7eb;
  border-radius: 6px;
  padding: 8px 12px;
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
  pointer-events: none;
  max-width: 400px;
  z-index: 50;
}
</style>
