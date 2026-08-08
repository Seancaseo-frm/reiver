<template>
  <div class="traces-table-wrapper">
    <!-- Loading State -->
    <div v-if="loading" class="flex items-center justify-center py-12">
      <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full"></div>
      <span class="ml-3 text-gray-600 dark:text-gray-400">Loading traces...</span>
    </div>

    <!-- Empty State -->
    <div v-else-if="items.length === 0" class="text-center py-12">
      <svg class="mx-auto h-12 w-12 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
      </svg>
      <h3 class="mt-2 text-sm font-medium text-gray-900 dark:text-gray-100">No traces found</h3>
      <p class="mt-1 text-sm text-gray-500 dark:text-gray-400">Try adjusting your filters or time range</p>
    </div>

    <!-- List View -->
    <div v-else-if="view === 'list'" class="space-y-2">
      <div
        v-for="item in sortedItems"
        :key="item.traceId || item.id"
        @click="$emit('row-click', item)"
        class="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4 cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors"
      >
        <div class="flex items-start justify-between">
          <div class="flex-1 min-w-0">
            <div class="flex items-center gap-3 mb-2">
              <div class="text-sm font-medium text-gray-900 dark:text-gray-100 truncate">
                {{ item.name || item.operationName || item.operation_name || 'Unknown operation' }}
              </div>
              <span
                :class="[
                  'px-2 py-1 text-xs font-medium rounded-full',
                  getStatusBadgeClass(item.status),
                ]"
              >
                {{ item.status || 'ok' }}
              </span>
            </div>
            <div class="flex items-center gap-4 text-sm text-gray-600 dark:text-gray-400">
              <span>
                <span class="font-medium">Service:</span> {{ item.service_name || item.serviceName || '-' }}
              </span>
              <span>
                <span class="font-medium">Duration:</span> {{ formatDuration((item.duration_ns || item.duration || item.duration_ms || 0) / 1_000_000) }}
              </span>
              <span>
                <span class="font-medium">Time:</span> {{ formatDate(item.timestamp || item.startTime) }}
              </span>
              <span class="font-mono text-xs text-gray-400 dark:text-gray-500 truncate max-w-[18ch]" :title="item.trace_id || item.id">
                {{ item.trace_id || item.id }}
              </span>
            </div>
          </div>
          <div class="flex items-center gap-2 ml-4">
            <button
              @click.stop="$emit('action', { action: 'copy', item })"
              class="text-xs text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300"
              title="Copy trace ID"
            >
              Copy
            </button>
            <button
              @click.stop="$emit('row-click', item)"
              class="text-xs text-primary-600 dark:text-primary-400 hover:text-primary-700 dark:hover:text-primary-300 font-medium"
            >
              View →
            </button>
          </div>
        </div>
      </div>
    </div>

    <!-- Timeline View -->
    <div v-else-if="view === 'timeline'" class="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 overflow-hidden">
      <div class="p-4 space-y-4">
        <div
          v-for="item in sortedItems"
          :key="item.traceId || item.id"
          @click="$emit('row-click', item)"
          class="relative pl-8 pb-4 border-l-2 border-gray-200 dark:border-gray-700 cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors rounded-r-lg"
        >
          <div class="absolute left-0 top-0 w-4 h-4 bg-primary-600 rounded-full -translate-x-[9px] border-2 border-white dark:border-gray-800"></div>
          <div class="flex items-start justify-between">
            <div class="flex-1 min-w-0">
              <div class="text-xs text-gray-500 dark:text-gray-400 mb-1">
                {{ formatDate(item.timestamp || item.startTime) }}
              </div>
              <div class="text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">
                {{ item.name || item.operationName || item.operation_name || 'Unknown operation' }}
              </div>
              <div class="flex items-center gap-3 text-sm text-gray-600 dark:text-gray-400">
                <span>{{ item.service_name || item.serviceName || '-' }}</span>
                <span>•</span>
                <span>{{ formatDuration((item.duration_ns || item.duration || item.duration_ms || 0) / 1_000_000) }}</span>
                <span>•</span>
                <span class="font-mono text-xs text-gray-400 dark:text-gray-500 truncate max-w-[18ch]">{{ item.trace_id || item.id }}</span>
              </div>
            </div>
            <div class="flex items-center gap-2 ml-4">
              <span
                :class="[
                  'px-2 py-1 text-xs font-medium rounded-full',
                  getStatusBadgeClass(item.status),
                ]"
              >
                {{ item.status || 'ok' }}
              </span>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- Table View (default) -->
    <div v-else class="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 overflow-hidden">
      <table class="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
        <thead class="bg-gray-50 dark:bg-gray-900">
          <tr>
            <th
              v-for="column in columns"
              :key="column.key"
              @click="column.sortable && handleSort(column.key)"
              :class="[
                'px-4 py-3 text-left text-xs font-semibold text-gray-700 dark:text-gray-300 uppercase tracking-wider',
                column.sortable && 'cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-800 select-none',
                column.align && `text-${column.align}`,
              ]"
            >
              <div class="flex items-center gap-2">
                <span>{{ column.label }}</span>
                <span v-if="column.sortable && sortField === column.key" class="text-primary-600 dark:text-primary-400">
                  {{ sortDirection === 'asc' ? '↑' : '↓' }}
                </span>
              </div>
            </th>
          </tr>
        </thead>
        <tbody class="bg-white dark:bg-gray-800 divide-y divide-gray-200 dark:divide-gray-700">
          <tr
            v-for="item in sortedItems"
            :key="item.traceId || item.id"
            @click="$emit('row-click', item)"
            class="cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors"
          >
            <!-- Operation -->
            <td class="px-4 py-3">
              <div class="flex items-start gap-3">
                <div class="flex-1 min-w-0">
                  <div class="text-sm font-medium text-gray-900 dark:text-gray-100 truncate max-w-xs">
                    {{ item.name || item.operationName || item.operation_name || 'Unknown operation' }}
                  </div>
                  <div class="text-xs text-gray-500 dark:text-gray-400 font-mono mt-1 truncate max-w-[20ch]" :title="item.trace_id || item.id">
                    {{ item.trace_id || item.id }}
                  </div>
                </div>
              </div>
            </td>

            <!-- Service -->
            <td class="px-4 py-3">
              <span class="text-sm text-gray-900 dark:text-gray-100">
                {{ item.service_name || item.serviceName || '-' }}
              </span>
              <button
                @click.stop="$emit('add-filter', { field: 'service', value: item.service_name || item.serviceName })"
                class="ml-2 text-xs text-primary-600 dark:text-primary-400 hover:text-primary-700 dark:hover:text-primary-300 opacity-0 log-entry-hover:opacity-100 transition-opacity"
                title="Filter by this service"
              >
                +
              </button>
            </td>

            <!-- Duration -->
            <td class="px-4 py-3">
              <span class="text-sm text-gray-900 dark:text-gray-100 font-mono">
                {{ formatDuration((item.duration_ns || item.duration || item.duration_ms || 0) / 1_000_000) }}
              </span>
            </td>

            <!-- Start Time -->
            <td class="px-4 py-3">
              <span class="text-sm text-gray-600 dark:text-gray-400">
                {{ formatDate(item.timestamp || item.startTime) }}
              </span>
            </td>

            <!-- Status -->
            <td class="px-4 py-3">
              <span
                :class="[
                  'px-2 py-1 text-xs font-medium rounded-full',
                  getStatusBadgeClass(item.status),
                ]"
              >
                {{ item.status || 'ok' }}
              </span>
            </td>

            <!-- Actions -->
            <td class="px-4 py-3 text-right">
              <div class="flex items-center justify-end gap-2">
                <button
                  @click.stop="$emit('action', { action: 'copy', item })"
                  class="text-xs text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300"
                  title="Copy trace ID"
                >
                  Copy
                </button>
                <button
                  @click.stop="$emit('row-click', item)"
                  class="text-xs text-primary-600 dark:text-primary-400 hover:text-primary-700 dark:hover:text-primary-300 font-medium"
                >
                  View →
                </button>
              </div>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <!-- Pagination (for all views) -->
    <div v-if="total > pageSize" class="mt-4 bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 px-4 py-3 flex items-center justify-between">
      <div class="text-sm text-gray-700 dark:text-gray-300">
        Showing {{ Math.min((currentPage - 1) * pageSize + 1, total) }}-{{ Math.min(currentPage * pageSize, total) }} of {{ total }} results
      </div>
      <div class="flex items-center gap-2">
        <button
          @click="handlePageChange(currentPage - 1)"
          :disabled="currentPage <= 1"
          class="px-3 py-1 text-sm border border-gray-300 dark:border-gray-600 rounded-md text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          Previous
        </button>
        <span class="text-sm text-gray-700 dark:text-gray-300">
          Page {{ currentPage }} of {{ Math.ceil(total / pageSize) }}
        </span>
        <button
          @click="handlePageChange(currentPage + 1)"
          :disabled="currentPage >= Math.ceil(total / pageSize)"
          class="px-3 py-1 text-sm border border-gray-300 dark:border-gray-600 rounded-md text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          Next
        </button>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed, ref } from 'vue'
import { formatDistanceToNow } from 'date-fns'

const props = defineProps({
  items: {
    type: Array,
    default: () => [],
  },
  view: {
    type: String,
    default: 'list',
  },
  loading: {
    type: Boolean,
    default: false,
  },
  sortField: {
    type: String,
    default: 'startTime',
  },
  sortDirection: {
    type: String,
    default: 'desc',
    validator: (value) => ['asc', 'desc'].includes(value),
  },
  currentPage: {
    type: Number,
    default: 1,
  },
  pageSize: {
    type: Number,
    default: 25,
  },
  total: {
    type: Number,
    default: 0,
  },
})

const emit = defineEmits(['sort', 'row-click', 'expand', 'action', 'add-filter', 'page-change'])

const columns = [
  { key: 'name', label: 'Operation', sortable: true, align: 'left' },
  { key: 'service_name', label: 'Service', sortable: true, align: 'left' },
  { key: 'duration', label: 'Duration', sortable: true, align: 'left' },
  { key: 'timestamp', label: 'Start Time', sortable: true, align: 'left' },
  { key: 'status', label: 'Status', sortable: true, align: 'left' },
  { key: 'actions', label: 'Actions', sortable: false, align: 'right' },
]

const sortedItems = computed(() => {
  if (!props.sortField) return props.items

  const sorted = [...props.items].sort((a, b) => {
    let aVal = a[props.sortField]
    let bVal = b[props.sortField]

    // Handle different types
    if (props.sortField === 'timestamp') {
      aVal = new Date(aVal || 0).getTime()
      bVal = new Date(bVal || 0).getTime()
    } else if (props.sortField === 'duration') {
      aVal = parseInt(aVal || a['duration_ns'] || a['duration_ms']) || 0
      bVal = parseInt(bVal || b['duration_ns'] || b['duration_ms']) || 0
    } else if (typeof aVal === 'string') {
      aVal = (aVal || '').toLowerCase()
      bVal = (bVal || '').toLowerCase()
    } else {
      aVal = aVal || 0
      bVal = bVal || 0
    }

    if (aVal < bVal) return props.sortDirection === 'asc' ? -1 : 1
    if (aVal > bVal) return props.sortDirection === 'asc' ? 1 : -1
    return 0
  })

  return sorted
})

const handleSort = (field) => {
  emit('sort', field)
}

const handlePageChange = (page) => {
  emit('page-change', page)
}

const formatDate = (dateString) => {
  if (!dateString) return 'Never'
  try {
    return formatDistanceToNow(new Date(dateString), { addSuffix: true })
  } catch {
    return dateString
  }
}

const formatDuration = (durationMs) => {
  if (!durationMs || durationMs === 0) return '0ms'

  const duration = parseInt(durationMs)

  if (duration < 1000) {
    return `${duration}ms`
  } else if (duration < 60000) {
    return `${(duration / 1000).toFixed(2)}s`
  } else if (duration < 3600000) {
    return `${(duration / 60000).toFixed(2)}m`
  } else {
    return `${(duration / 3600000).toFixed(2)}h`
  }
}

const getStatusBadgeClass = (status) => {
  const statusMap = {
    error: 'bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-300',
    ok: 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300',
    unknown: 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300',
  }
  return statusMap[status] || statusMap.unknown
}
</script>

<style scoped>
.traces-table-wrapper {
  @apply relative;
}

.spinner {
  animation: spin 0.6s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

.group:hover .group-hover\:opacity-100 {
  opacity: 1;
}
</style>