<template>
  <div class="trace-waterfall">
    <div class="overflow-x-auto">
      <div class="min-w-max">
        <!-- Timeline Header -->
        <div class="flex border-b border-gray-200 dark:border-gray-700 pb-2 mb-4">
          <div class="w-64 flex-shrink-0 font-semibold text-sm text-gray-700 dark:text-gray-300">
            Service / Operation
          </div>
          <div class="flex-1 relative">
            <div class="absolute inset-0 flex items-center">
              <div class="w-full border-t border-gray-300 dark:border-gray-600"></div>
            </div>
            <div class="relative flex justify-between text-xs text-gray-500 dark:text-gray-400 px-2">
              <span>0ms</span>
              <span>{{ formatDuration(totalDuration) }}</span>
            </div>
          </div>
        </div>

        <!-- Span Rows -->
        <div class="space-y-2">
          <div
            v-for="(span, index) in sortedSpans"
            :key="span.span_id"
            :class="['flex items-center cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-800/50 rounded-md py-1 -mx-2 px-2 transition-colors', { 'bg-primary-50 dark:bg-primary-900/20': props.selectedSpanId === span.span_id }]"
            :style="{ paddingLeft: `${getDepth(span) * 20}px` }"
            @click="emit('select-span', span)"
          >
            <div class="w-64 flex-shrink-0 pr-4">
              <div class="flex items-center gap-2">
                <span
                  :class="[
                    'w-2 h-2 rounded-full',
                    getStatusColor(span.status_code),
                  ]"
                ></span>
                <div class="min-w-0 flex-1">
                  <div class="flex items-center gap-1">
                    <div class="text-sm font-medium text-gray-900 dark:text-gray-100 truncate">
                      {{ span.span_name }}
                    </div>
                    <span
                      v-if="hasException(span.span_id)"
                      class="shrink-0 w-4 h-4 flex items-center justify-center bg-red-100 dark:bg-red-900/30 rounded-full"
                      :title="getExceptionCount(span.span_id) + ' exception(s)'"
                    >
                      <svg class="w-3 h-3 text-red-600 dark:text-red-400" fill="currentColor" viewBox="0 0 20 20">
                        <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7 4a1 1 0 11-2 0 1 1 0 012 0zm-1-9a1 1 0 00-1 1v4a1 1 0 102 0V6a1 1 0 00-1-1z" clip-rule="evenodd" />
                      </svg>
                    </span>
                  </div>
                  <div class="text-xs text-gray-500 dark:text-gray-400 truncate">
                    {{ span.service_name }}
                  </div>
                </div>
              </div>
            </div>
            <div class="flex-1 relative h-8">
              <div
                :class="[
                  'absolute h-6 rounded flex items-center justify-center text-xs font-medium cursor-pointer hover:opacity-80 transition-opacity',
                  getSpanBarClass(span.status_code),
                ]"
                :style="{
                  left: `${getSpanLeft(span)}%`,
                  width: `${getSpanWidth(span)}%`,
                  minWidth: '4px',
                }"
                :title="`${span.span_name} - ${formatDuration(getSpanDurationMs(span))}`"
              >
                <span v-if="getSpanWidth(span) > 5" class="text-white dark:text-gray-100 px-1 truncate">
                  {{ formatDuration(getSpanDurationMs(span)) }}
                </span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed } from 'vue'

const props = defineProps({
  trace: {
    type: Object,
    required: true,
  },
  exceptions: {
    type: Array,
    default: () => [],
  },
  selectedSpanId: {
    type: String,
    default: null,
  },
})

const emit = defineEmits(['select-span'])

const hasException = (spanId) => {
  return props.exceptions.some(e => e.span_id === spanId)
}

const getExceptionCount = (spanId) => {
  return props.exceptions.filter(e => e.span_id === spanId).length
}

const totalDuration = computed(() => {
  // Convert nanoseconds to milliseconds for calculations
  return (props.trace.trace.duration_ns || 0) / 1_000_000 || 1
})

const getSpanDurationMs = (span) => {
  // Convert nanoseconds to milliseconds
  return (span.duration_ns || 0) / 1_000_000
}

const sortedSpans = computed(() => {
  // Sort spans by timestamp, maintaining hierarchy
  return [...props.trace.spans].sort((a, b) => {
    const aTime = new Date(a.timestamp).getTime()
    const bTime = new Date(b.timestamp).getTime()
    return aTime - bTime
  })
})

const getDepth = (span) => {
  // Calculate depth based on parent_span_id
  if (!span.parent_span_id) return 0
  
  let depth = 0
  let current = span
  const visited = new Set()
  
  while (current.parent_span_id && !visited.has(current.span_id)) {
    visited.add(current.span_id)
    const parent = props.trace.spans.find(s => s.span_id === current.parent_span_id)
    if (parent) {
      depth++
      current = parent
    } else {
      break
    }
  }
  
  return Math.min(depth, 5) // Max depth of 5 for visual clarity
}

const getSpanLeft = (span) => {
  const traceStart = new Date(props.trace.trace.start_time).getTime()
  const spanStart = new Date(span.timestamp).getTime()
  const offset = spanStart - traceStart
  const offsetMs = Math.max(0, offset)
  return totalDuration.value > 0 ? (offsetMs / totalDuration.value) * 100 : 0
}

const getSpanWidth = (span) => {
  if (totalDuration.value <= 0) return 1
  const durationMs = getSpanDurationMs(span)
  const width = (durationMs / totalDuration.value) * 100
  return Math.max(0.5, width) // Minimum 0.5% width for visibility
}

const formatDuration = (ms) => {
  if (ms < 1) return '<1ms'
  if (ms < 1000) return `${Math.round(ms)}ms`
  if (ms < 60000) return `${(ms / 1000).toFixed(2)}s`
  return `${(ms / 60000).toFixed(2)}m`
}

const getStatusColor = (statusCode) => {
  if (statusCode === 'STATUS_CODE_ERROR') return 'bg-error-500'
  if (statusCode === 'STATUS_CODE_OK') return 'bg-success-500'
  return 'bg-gray-500'
}

const getSpanBarClass = (statusCode) => {
  if (statusCode === 'STATUS_CODE_ERROR') return 'bg-error-500'
  if (statusCode === 'STATUS_CODE_OK') return 'bg-success-500'
  return 'bg-primary-500'
}
</script>

<style scoped>
.trace-waterfall {
  @apply w-full;
}
</style>

