<template>
  <div class="profile-flamegraph" ref="containerRef">
    <!-- Controls -->
    <div class="flamegraph-controls border-b border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 px-4 py-2 flex items-center justify-between">
      <div class="flex items-center gap-3">
        <button
          @click="resetZoom"
          :disabled="!zoomStack.length"
          class="inline-flex items-center px-3 py-1.5 text-sm font-medium rounded-md border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
        >
          <svg class="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0zM13 10H7" />
          </svg>
          Reset Zoom
        </button>

        <div class="relative">
          <svg class="absolute left-2.5 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
          </svg>
          <input
            v-model="searchQuery"
            type="text"
            placeholder="Search functions..."
            class="pl-8 pr-3 py-1.5 w-64 text-sm border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-500 focus:ring-2 focus:ring-primary-500 focus:border-primary-500"
          />
        </div>

        <span v-if="searchQuery && searchMatchCount > 0" class="text-xs text-gray-500 dark:text-gray-400">
          {{ searchMatchCount }} match{{ searchMatchCount !== 1 ? 'es' : '' }}
        </span>
      </div>

      <div class="flex items-center gap-3 text-sm text-gray-500 dark:text-gray-400">
        <label class="inline-flex items-center gap-1.5 cursor-pointer select-none">
          <input
            type="checkbox"
            v-model="collapseRuntime"
            class="h-3.5 w-3.5 text-primary-600 border-gray-300 rounded focus:ring-primary-500"
          />
          <span class="text-xs text-gray-600 dark:text-gray-400">Collapse runtime</span>
        </label>
        <span v-if="flameGraph?.metadata">
          {{ flameGraph.metadata.profile_type || 'cpu' }} &middot; {{ formatSamples(flameGraph.metadata.sample_count) }} samples
        </span>
      </div>
    </div>

    <!-- Flamegraph SVG -->
    <div class="flamegraph-canvas overflow-x-auto" ref="canvasRef">
      <svg
        v-if="layoutNodes.length"
        :width="svgWidth"
        :height="svgHeight"
        class="block"
        @mouseleave="scheduleHideTooltip"
      >
        <g v-for="node in layoutNodes" :key="node.id">
          <rect
            :x="node.x"
            :y="node.y"
            :width="Math.max(node.width, 1)"
            :height="barHeight - 1"
            :fill="getNodeFill(node)"
            :stroke="hoveredNode === node ? '#fff' : 'rgba(0,0,0,0.1)'"
            :stroke-width="hoveredNode === node ? 2 : 0.5"
            class="cursor-pointer transition-opacity"
            :opacity="getNodeOpacity(node)"
            @click="zoomInto(node)"
            @mouseenter="showTooltip(node)"
            @mouseleave="scheduleHideTooltip"
          />
          <text
            v-if="node.width > 40"
            :x="node.x + 4"
            :y="node.y + barHeight / 2 + 4"
            class="text-[11px] fill-gray-900 dark:fill-gray-100 pointer-events-none select-none"
            :clip-path="`inset(0 ${Math.max(0, svgWidth - node.x - node.width + 4)}px 0 0)`"
          >
            {{ truncateLabel(node.name, node.width) }}
          </text>
        </g>
      </svg>

      <div v-else class="text-center py-12 text-gray-500 dark:text-gray-400">
        No flamegraph data available
      </div>
    </div>

    <!-- Tooltip -->
    <div
      v-if="hoveredNode"
      class="flamegraph-tooltip"
      :style="tooltipStyle"
      @mouseenter="cancelHideTooltip"
      @mouseleave="scheduleHideTooltip"
    >
      <p class="font-medium text-sm text-gray-900 dark:text-gray-100 break-all">{{ hoveredNode.name }}</p>
      <div class="mt-1 space-y-0.5 text-xs text-gray-600 dark:text-gray-400">
        <p>Total: {{ formatSamples(hoveredNode.value) }} ({{ formatPercent(hoveredNode.value, rootValue) }})</p>
        <p>Self: {{ formatSamples(hoveredNode.self) }} ({{ formatPercent(hoveredNode.self, rootValue) }})</p>
        <p v-if="hoveredNode.filename" class="text-gray-500 dark:text-gray-500">
          {{ hoveredNode.filename }}{{ hoveredNode.line_number ? ':' + hoveredNode.line_number : '' }}
        </p>
      </div>
      <button
        v-if="hoveredNode.filename"
        @click.stop="emitViewSource(hoveredNode)"
        class="mt-2 inline-flex items-center gap-1 px-2 py-1 text-xs font-medium rounded bg-primary-50 dark:bg-primary-900/30 text-primary-700 dark:text-primary-300 hover:bg-primary-100 dark:hover:bg-primary-900/50 transition-colors pointer-events-auto"
      >
        <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 20l4-16m4 4l4 4-4 4M6 16l-4-4 4-4" />
        </svg>
        View Source
      </button>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, watch, onMounted, onUnmounted, nextTick } from 'vue'

const props = defineProps({
  flameGraph: {
    type: Object,
    default: null,
  },
})

const emit = defineEmits(['view-source'])

const containerRef = ref(null)
const canvasRef = ref(null)
const hoveredNode = ref(null)
const searchQuery = ref('')
const zoomStack = ref([])
const collapseRuntime = ref(true)

const RUNTIME_PATTERNS = [
  /^tokio::/,
  /^std::/,
  /^core::/,
  /^alloc::/,
  /^mio::/,
  /^hyper::/,
  /^h2::/,
  /^__rust_begin_short_backtrace$/,
  /^__libc_start_main$/,
  /^start_thread$/,
  /^clone[3]?$/,
  /^pthread_/,
  /^___pthread_/,
  /^_start$/,
  /^runtime\./,
  /^net\/http\./,
  /^syscall\./,
  /^internal\/poll\./,
  /^runtime\.goexit/,
  /^java\.lang\.Thread\.run$/,
  /^java\.util\.concurrent\./,
]

const isRuntimeFrame = (name) => {
  const normalized = name.replace(/^<+/, '')
  return RUNTIME_PATTERNS.some(p => p.test(normalized))
}

const collapseTree = (node) => {
  if (!node) return node
  if (!collapseRuntime.value) return node

  const children = (node.children || []).map(c => collapseTree(c)).filter(Boolean)

  if (isRuntimeFrame(node.name) && node.name !== 'root' && node.name !== '(root)') {
    if (children.length === 0) {
      return null
    }
    if (children.length === 1) {
      return { ...children[0], value: node.value }
    }
    // Multi-child runtime frame: promote all children up
    return { ...node, children, _promoted: true }
  }

  // Flatten promoted children into this node's child list
  const flatChildren = []
  for (const child of children) {
    if (child._promoted) {
      flatChildren.push(...(child.children || []))
    } else {
      flatChildren.push(child)
    }
  }

  return { ...node, children: flatChildren }
}

// Tooltip hide debounce -- keeps the tooltip visible while the mouse
// travels from the SVG rect to the tooltip "View Source" button.
let hideTooltipTimer = null

const showTooltip = (node) => {
  cancelHideTooltip()
  hoveredNode.value = node
}

const scheduleHideTooltip = () => {
  cancelHideTooltip()
  hideTooltipTimer = setTimeout(() => {
    hoveredNode.value = null
  }, 150)
}

const cancelHideTooltip = () => {
  if (hideTooltipTimer) {
    clearTimeout(hideTooltipTimer)
    hideTooltipTimer = null
  }
}

const barHeight = 20
const svgWidth = ref(1200)

// Observe container width
let resizeObserver = null
onMounted(() => {
  if (canvasRef.value) {
    svgWidth.value = canvasRef.value.clientWidth || 1200
    resizeObserver = new ResizeObserver((entries) => {
      for (const entry of entries) {
        svgWidth.value = entry.contentRect.width || 1200
      }
    })
    resizeObserver.observe(canvasRef.value)
  }
})

onUnmounted(() => {
  if (resizeObserver) resizeObserver.disconnect()
  cancelHideTooltip()
})

const processedRoot = computed(() => {
  const raw = props.flameGraph?.root || null
  if (!raw) return null
  return collapseTree(raw)
})

// Current root for zoom
const currentRoot = computed(() => {
  if (zoomStack.value.length > 0) {
    return zoomStack.value[zoomStack.value.length - 1]
  }
  return processedRoot.value
})

const rootValue = computed(() => {
  return currentRoot.value?.value || 1
})

// Flatten the tree into layout nodes
const layoutNodes = computed(() => {
  const root = currentRoot.value
  if (!root) return []

  const nodes = []
  const width = svgWidth.value
  let idCounter = 0

  const traverse = (node, x, y, parentWidth) => {
    const w = parentWidth * (node.value / (currentRoot.value.value || 1))
    if (w < 0.5) return // skip tiny nodes

    const selfValue = node.value - (node.children || []).reduce((sum, c) => sum + (c.value || 0), 0)

    nodes.push({
      id: idCounter++,
      name: node.name,
      value: node.value,
      self: Math.max(0, selfValue),
      x,
      y,
      width: w,
      children: node.children,
      _raw: node,
      filename: node.filename || null,
      function_name: node.function_name || null,
      line_number: node.line_number || null,
    })

    let childX = x
    if (node.children) {
      // Sort children by value descending for stable layout
      const sorted = [...node.children].sort((a, b) => (b.value || 0) - (a.value || 0))
      for (const child of sorted) {
        const childW = w * ((child.value || 0) / (node.value || 1))
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
  if (layoutNodes.value.length === 0) return 0
  let max = 0
  for (const n of layoutNodes.value) {
    const depth = Math.round(n.y / barHeight)
    if (depth > max) max = depth
  }
  return max + 1
})

const svgHeight = computed(() => {
  return maxDepth.value * barHeight + 4
})

// Search matching
const searchMatchCount = computed(() => {
  if (!searchQuery.value) return 0
  const q = searchQuery.value.toLowerCase()
  return layoutNodes.value.filter(n => n.name.toLowerCase().includes(q)).length
})

const isSearchMatch = (node) => {
  if (!searchQuery.value) return false
  return node.name.toLowerCase().includes(searchQuery.value.toLowerCase())
}

// Color generation (warm flame palette)
const getNodeFill = (node) => {
  if (isSearchMatch(node)) return '#a855f7' // purple for search matches
  if (node.name === 'root' || node.name === '(root)') return '#6b7280'

  if (!collapseRuntime.value && isRuntimeFrame(node.name)) {
    return '#9ca3af' // gray for runtime frames when visible
  }

  // Hash-based warm color
  let hash = 0
  for (let i = 0; i < node.name.length; i++) {
    hash = ((hash << 5) - hash + node.name.charCodeAt(i)) | 0
  }
  const h = 10 + (Math.abs(hash) % 40)        // hue 10-50 (red to orange-yellow)
  const s = 70 + (Math.abs(hash >> 8) % 25)    // saturation 70-95
  const l = 50 + (Math.abs(hash >> 16) % 15)   // lightness 50-65
  return `hsl(${h}, ${s}%, ${l}%)`
}

const getNodeOpacity = (node) => {
  if (searchQuery.value && !isSearchMatch(node)) return 0.35
  if (hoveredNode.value && hoveredNode.value !== node) return 0.85
  return 1
}

// Zoom
const zoomInto = (node) => {
  if (node._raw && node._raw.children && node._raw.children.length > 0) {
    zoomStack.value.push(node._raw)
  }
}

const resetZoom = () => {
  zoomStack.value = []
}

const emitViewSource = (node) => {
  if (node.filename) {
    emit('view-source', {
      filename: node.filename,
      functionName: node.function_name || node.name,
      lineNumber: node.line_number || null,
    })
  }
}

watch(collapseRuntime, () => {
  zoomStack.value = []
})

// Label truncation
const truncateLabel = (name, width) => {
  const charWidth = 7
  const maxChars = Math.floor((width - 8) / charWidth)
  if (maxChars <= 0) return ''
  if (name.length <= maxChars) return name
  return name.substring(0, maxChars - 1) + '\u2026'
}

// Tooltip position
const tooltipStyle = computed(() => {
  if (!hoveredNode.value || !canvasRef.value) return { display: 'none' }
  const node = hoveredNode.value
  const rect = canvasRef.value.getBoundingClientRect()
  let left = node.x + node.width / 2
  let top = node.y + barHeight + 8

  // Clamp to viewport
  if (left + 200 > rect.width) left = rect.width - 220
  if (left < 0) left = 10

  return {
    position: 'absolute',
    left: `${left}px`,
    top: `${top}px`,
    zIndex: 50,
  }
})

// Formatting helpers
const formatSamples = (n) => {
  if (n == null) return '--'
  return n.toLocaleString()
}

const formatPercent = (value, total) => {
  if (!total || !value) return '0%'
  return ((value / total) * 100).toFixed(1) + '%'
}
</script>

<style scoped>
.profile-flamegraph {
  position: relative;
  background: var(--bg, white);
}

:root.dark .profile-flamegraph,
.dark .profile-flamegraph {
  --bg: #111827;
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
  pointer-events: auto;
  max-width: 400px;
  z-index: 50;
}

.dark .flamegraph-tooltip {
  background: #1f2937;
  border-color: #374151;
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.4);
}
</style>
