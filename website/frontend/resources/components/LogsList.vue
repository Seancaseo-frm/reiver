<template>
  <div class="logs-list-wrapper">
    <!-- Loading State -->
    <div v-if="loading" class="flex items-center justify-center py-12">
      <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full"></div>
      <span class="ml-3 text-gray-600 dark:text-gray-400">Loading logs...</span>
    </div>

    <!-- Empty State -->
    <div v-else-if="items.length === 0" class="text-center py-12">
      <svg class="mx-auto h-12 w-12 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
      </svg>
      <h3 class="mt-2 text-sm font-medium text-gray-900 dark:text-gray-100">No logs found</h3>
      <p class="mt-1 text-sm text-gray-500 dark:text-gray-400">Try adjusting your filters or time range</p>
    </div>

    <!-- Logs List -->
    <div v-else class="space-y-2">
      <div
        v-for="item in items"
        :key="item.id || item.timestamp"
        class="log-entry bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg p-4 hover:shadow-md transition-shadow cursor-pointer"
        @click="$emit('row-click', item)"
      >
        <!-- Log Header -->
        <div class="flex items-start justify-between gap-4 mb-2">
          <div class="flex items-center gap-3 flex-1 min-w-0">
            <!-- Timestamp -->
            <div class="text-xs text-gray-500 dark:text-gray-400 font-mono whitespace-nowrap">
              {{ formatTimestamp(item.timestamp) }}
            </div>

            <!-- Level Badge -->
            <span
              :class="[
                'px-2 py-0.5 text-xs font-medium rounded',
                getLevelBadgeClass(item.level || item.severity),
              ]"
            >
              {{ (item.level || item.severity || 'info').toUpperCase() }}
            </span>

            <!-- Service -->
            <span class="text-xs text-gray-600 dark:text-gray-400 truncate max-w-xs">
              {{ item.service_name || item.service || item.serviceName || 'unknown' }}
            </span>
          </div>

    <!-- Actions -->
    <div class="flex items-center gap-2 opacity-0 log-entry-hover:opacity-100 transition-opacity">
      <button
        @click.stop="$emit('action', { action: 'copy', item })"
        class="text-xs text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300 p-1"
        title="Copy log message"
      >
        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
        </svg>
      </button>
    </div>
        </div>

        <!-- Log Message -->
        <div class="text-sm text-gray-900 dark:text-gray-100 leading-relaxed">
          <pre v-if="isExpanded(item.id)" class="whitespace-pre-wrap font-sans">{{ item.message || item.body || 'No message' }}</pre>
          <div v-else class="truncate">{{ item.message || item.body || 'No message' }}</div>
        </div>

        <!-- Expanded Details -->
        <div v-if="isExpanded(item.id)" class="mt-3 pt-3 border-t border-gray-200 dark:border-gray-700">
          <div class="grid grid-cols-2 gap-4 text-xs">
            <div v-if="item.trace_id" class="flex justify-between">
              <span class="text-gray-500 dark:text-gray-400">Trace ID:</span>
              <span class="font-mono text-gray-900 dark:text-gray-100">{{ item.trace_id }}</span>
            </div>
            <div v-if="item.source" class="flex justify-between">
              <span class="text-gray-500 dark:text-gray-400">Source:</span>
              <span class="text-gray-900 dark:text-gray-100">{{ item.source }}</span>
            </div>
            <div v-if="item.count" class="flex justify-between">
              <span class="text-gray-500 dark:text-gray-400">Count:</span>
              <span class="text-gray-900 dark:text-gray-100">{{ item.count }}</span>
            </div>
          </div>
        </div>

        <!-- Expand/Collapse Button -->
        <div class="mt-2 flex justify-center">
          <button
            @click.stop="$emit('expand', item.id)"
            class="text-xs text-primary-600 dark:text-primary-400 hover:text-primary-700 dark:hover:text-primary-300 flex items-center gap-1"
          >
            <span>{{ isExpanded(item.id) ? 'Collapse' : 'Expand' }}</span>
            <svg :class="['w-3 h-3 transition-transform', { 'rotate-180': isExpanded(item.id) }]" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
            </svg>
          </button>
        </div>
      </div>
    </div>

    <!-- Load More Button -->
    <div v-if="items.length > 0 && !loading" class="text-center mt-6">
      <button
        @click="$emit('load-more')"
        class="px-4 py-2 text-sm font-medium text-primary-600 dark:text-primary-400 bg-primary-50 dark:bg-primary-900/20 border border-primary-200 dark:border-primary-800 rounded-md hover:bg-primary-100 dark:hover:bg-primary-900/30 focus:ring-2 focus:ring-primary-500 focus:outline-none transition-colors"
      >
        Load More Logs
      </button>
    </div>
  </div>
</template>

<script setup>
import { computed } from 'vue'
import { format } from 'date-fns'

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
  live: {
    type: Boolean,
    default: false,
  },
  expandedItems: {
    type: Object,
    default: () => ({}),
  },
})

const emit = defineEmits(['row-click', 'expand', 'action', 'add-filter', 'load-more'])

const isExpanded = (itemId) => {
  return props.expandedItems[itemId] || false
}

const formatTimestamp = (timestamp) => {
  if (!timestamp) return '--:--:--'

  try {
    const date = new Date(timestamp)
    return format(date, 'HH:mm:ss.SSS')
  } catch {
    return timestamp
  }
}

const getLevelBadgeClass = (level) => {
  if (!level) return 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300'

  const levelMap = {
    error: 'bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-300',
    warn: 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-300',
    warning: 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-300',
    info: 'bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300',
    debug: 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300',
    trace: 'bg-purple-100 text-purple-800 dark:bg-purple-900/30 dark:text-purple-300',
  }

  return levelMap[level.toLowerCase()] || levelMap.info
}
</script>

<style scoped>
.logs-list-wrapper {
  @apply relative;
}

.log-entry:hover .log-entry-hover {
  opacity: 1;
}

.spinner {
  animation: spin 0.6s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

pre {
  @apply font-mono text-xs;
}
</style>