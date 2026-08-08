<template>
  <div class="spans-table-wrapper">
    <div class="overflow-x-auto">
      <table class="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
        <thead class="bg-gray-50 dark:bg-gray-900">
          <tr>
            <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 dark:text-gray-300 uppercase tracking-wider">
              Operation
            </th>
            <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 dark:text-gray-300 uppercase tracking-wider">
              Service
            </th>
            <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 dark:text-gray-300 uppercase tracking-wider">
              Duration
            </th>
            <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 dark:text-gray-300 uppercase tracking-wider">
              Status
            </th>
            <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 dark:text-gray-300 uppercase tracking-wider">
              Start Time
            </th>
          </tr>
        </thead>
        <tbody class="bg-white dark:bg-gray-800 divide-y divide-gray-200 dark:divide-gray-700">
          <tr
            v-for="span in spans"
            :key="span.span_id"
            :class="['hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors cursor-pointer', { 'bg-primary-50 dark:bg-primary-900/20': props.selectedSpanId === span.span_id }]"
            @click="emit('select-span', span)"
          >
            <td class="px-4 py-3">
              <div class="flex items-center gap-2">
                <div class="flex-1">
                  <div class="text-sm font-medium text-gray-900 dark:text-gray-100">
                    {{ span.span_name }}
                  </div>
                  <div v-if="span.parent_span_id" class="text-xs text-gray-500 dark:text-gray-400 mt-1">
                    Parent: {{ span.parent_span_id.slice(0, 8) }}...
                  </div>
                </div>
                <span
                  v-if="hasException(span.span_id)"
                  class="shrink-0 px-2 py-0.5 text-xs font-medium bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-300 rounded-full"
                  :title="getExceptionCount(span.span_id) + ' exception(s) in this span'"
                >
                  {{ getExceptionCount(span.span_id) }} error{{ getExceptionCount(span.span_id) > 1 ? 's' : '' }}
                </span>
              </div>
            </td>
            <td class="px-4 py-3">
              <span class="text-sm text-gray-900 dark:text-gray-100">
                {{ span.service_name }}
              </span>
            </td>
            <td class="px-4 py-3">
              <span class="text-sm text-gray-900 dark:text-gray-100 font-medium">
                {{ formatDuration((span.duration_ns || 0) / 1_000_000) }}
              </span>
            </td>
            <td class="px-4 py-3">
              <span
                :class="[
                  'px-2 py-1 text-xs font-medium rounded-full',
                  getStatusBadgeClass(span.status_code),
                ]"
              >
                {{ formatStatus(span.status_code) }}
              </span>
            </td>
            <td class="px-4 py-3">
              <span class="text-sm text-gray-600 dark:text-gray-400">
                {{ formatRelativeTime(span.timestamp) }}
              </span>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<script setup>
import { computed } from 'vue'
import { formatDistanceToNow } from 'date-fns'

const props = defineProps({
  spans: {
    type: Array,
    default: () => [],
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

const formatDuration = (ms) => {
  if (ms < 1) return '<1ms'
  if (ms < 1000) return `${Math.round(ms)}ms`
  if (ms < 60000) return `${(ms / 1000).toFixed(2)}s`
  return `${(ms / 60000).toFixed(2)}m`
}

const formatRelativeTime = (dateString) => {
  if (!dateString) return 'Unknown'
  try {
    return formatDistanceToNow(new Date(dateString), { addSuffix: true })
  } catch {
    return dateString
  }
}

const formatStatus = (statusCode) => {
  if (statusCode === 'STATUS_CODE_ERROR') return 'error'
  if (statusCode === 'STATUS_CODE_OK') return 'ok'
  return 'unset'
}

const getStatusBadgeClass = (statusCode) => {
  if (statusCode === 'STATUS_CODE_ERROR') {
    return 'bg-error-100 text-error-800 dark:bg-error-900/30 dark:text-error-300'
  }
  if (statusCode === 'STATUS_CODE_OK') {
    return 'bg-success-100 text-success-800 dark:bg-success-900/30 dark:text-success-300'
  }
  return 'bg-gray-100 text-gray-800 dark:bg-gray-900/30 dark:text-gray-300'
}
</script>


