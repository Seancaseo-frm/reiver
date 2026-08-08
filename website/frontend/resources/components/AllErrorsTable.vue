<template>
  <div class="all-errors-table-wrapper">
    <!-- Loading State -->
    <div v-if="loading" class="flex items-center justify-center py-12">
      <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full"></div>
      <span class="ml-3 text-gray-600 dark:text-gray-400">Loading errors...</span>
    </div>

    <!-- Empty State -->
    <div v-else-if="filteredItems.length === 0" class="text-center py-12">
      <svg class="mx-auto h-12 w-12 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
      </svg>
      <h3 class="mt-2 text-sm font-medium text-gray-900 dark:text-gray-100">No errors found</h3>
      <p class="mt-1 text-sm text-gray-500 dark:text-gray-400">
        {{ hasActiveFilters ? 'Try adjusting your filters' : 'Once errors are reported, they will appear here.' }}
      </p>
      <button
        v-if="hasActiveFilters"
        @click="clearAllFilters"
        class="mt-4 text-sm text-primary-600 dark:text-primary-400 hover:underline"
      >
        Clear all filters
      </button>
    </div>

    <!-- Error Table -->
    <div v-else class="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 overflow-hidden">
      <div>
        <table class="w-full divide-y divide-gray-200 dark:divide-gray-700">
          <thead class="bg-gray-50 dark:bg-gray-900">
            <tr>
              <th
                v-for="column in columns"
                :key="column.key"
                :class="[
                  'px-4 py-3 text-left text-xs font-semibold text-gray-700 dark:text-gray-300 uppercase tracking-wider',
                  column.align && `text-${column.align}`,
                  column.width
                ]"
              >
                <div class="flex flex-col gap-2">
                  <!-- Header with sort -->
                  <div
                    :class="[
                      'flex items-center gap-2',
                      column.sortable && 'cursor-pointer hover:text-gray-900 dark:hover:text-white select-none'
                    ]"
                    @click="column.sortable && handleSort(column.key)"
                  >
                    <span>{{ column.label }}</span>
                    <span v-if="column.sortable && sortField === column.key" class="text-primary-600 dark:text-primary-400">
                      {{ sortDirection === 'asc' ? '↑' : '↓' }}
                    </span>
                    <!-- Filter indicator -->
                    <span
                      v-if="column.filterable && columnFilters[column.key]"
                      class="w-2 h-2 bg-primary-500 rounded-full"
                    ></span>
                  </div>

                  <!-- Column Filter -->
                  <div v-if="column.filterable" class="relative" @click.stop>
                    <div class="flex items-center">
                      <input
                        v-model="columnFilters[column.key]"
                        type="text"
                        :placeholder="`Filter ${column.label.toLowerCase()}...`"
                        class="column-filter-input"
                        @input="handleFilterChange(column.key)"
                      />
                      <button
                        v-if="columnFilters[column.key]"
                        @click="clearColumnFilter(column.key)"
                        class="absolute right-2 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300"
                      >
                        <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
                        </svg>
                      </button>
                    </div>
                  </div>
                </div>
              </th>
            </tr>
          </thead>
          <tbody class="bg-white dark:bg-gray-800 divide-y divide-gray-200 dark:divide-gray-700">
            <tr
              v-for="item in paginatedItems"
              :key="item.id || item.exceptionId"
              @click="handleRowClick(item)"
              class="cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-700/50 transition-colors group"
            >
              <!-- Exception Type -->
              <td class="px-3 py-2">
                <div class="flex items-center gap-2">
                  <span class="px-1.5 py-0.5 text-xs font-medium rounded bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-300 whitespace-nowrap">
                    {{ item.exceptionType || item.exception_type || 'ERROR' }}
                  </span>
                  <span class="text-xs text-gray-500 dark:text-gray-400 font-mono truncate">
                    {{ truncateId(item.groupID || item.group_id || item.id) }}
                  </span>
                </div>
              </td>

              <!-- Message -->
              <td class="px-3 py-2">
                <div class="text-sm text-gray-900 dark:text-gray-100 truncate">
                  <Tooltip :title="item.exceptionMessage || item.exception_message || item.message || 'No message'">
                    {{ item.exceptionMessage || item.exception_message || item.message || 'No message' }}
                  </Tooltip>
                </div>
              </td>

              <!-- Count -->
              <td class="px-2 py-2 text-center">
                <span class="text-sm text-gray-900 dark:text-gray-100 font-medium tabular-nums">
                  {{ formatNumber(item.exceptionCount || item.exception_count || item.count || 0) }}
                </span>
              </td>

              <!-- Last Seen -->
              <td class="px-2 py-2">
                <span class="text-xs text-gray-600 dark:text-gray-400 whitespace-nowrap">
                  {{ formatDate(item.lastSeen || item.last_seen) }}
                </span>
              </td>

              <!-- Service -->
              <td class="px-2 py-2">
                <span class="text-xs text-gray-900 dark:text-gray-100 truncate block">
                  {{ item.serviceName || item.service_name || '-' }}
                </span>
              </td>

              <!-- Actions -->
              <td class="px-2 py-2 text-right">
                <div class="flex items-center justify-end gap-1">
                  <span
                    v-if="item.status"
                    :class="[
                      'px-1.5 py-0.5 text-xs font-medium rounded',
                      item.status === 'resolved' ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400' :
                      item.status === 'ignored' ? 'bg-gray-100 text-gray-600 dark:bg-gray-700 dark:text-gray-400' :
                      'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400'
                    ]"
                  >
                    {{ item.status === 'unresolved' ? 'open' : item.status }}
                  </span>
                  <button
                    v-if="item.status !== 'resolved'"
                    @click.stop="$emit('action', { action: 'resolve', item })"
                    class="text-xs text-green-600 dark:text-green-400 hover:text-green-700 dark:hover:text-green-300 font-medium opacity-0 group-hover:opacity-100 transition-opacity"
                    title="Mark as resolved"
                  >
                    ✓
                  </button>
                </div>
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <!-- Pagination -->
      <div class="px-4 py-3 bg-gray-50 dark:bg-gray-900 border-t border-gray-200 dark:border-gray-700 flex items-center justify-between">
        <div class="flex items-center gap-4">
          <div class="text-sm text-gray-700 dark:text-gray-300">
            Showing {{ paginationStart }}-{{ paginationEnd }} of {{ filteredItems.length }} results
          </div>
          <!-- Active filters summary -->
          <div v-if="hasActiveFilters" class="flex items-center gap-2">
            <span class="text-xs text-gray-500">Filters:</span>
            <span
              v-for="(value, key) in activeFilters"
              :key="key"
              class="inline-flex items-center gap-1 px-2 py-0.5 text-xs bg-primary-100 dark:bg-primary-900/30 text-primary-700 dark:text-primary-300 rounded"
            >
              {{ key }}: {{ value }}
              <button @click="clearColumnFilter(key)" class="hover:text-primary-900 dark:hover:text-primary-100">
                <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </span>
          </div>
        </div>

        <div class="flex items-center gap-2">
          <!-- Page Size Selector -->
          <select
            v-model="localPageSize"
            @change="handlePageSizeChange"
            class="text-sm border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-800 text-gray-700 dark:text-gray-300 px-2 py-1"
          >
            <option :value="10">10 / page</option>
            <option :value="25">25 / page</option>
            <option :value="50">50 / page</option>
            <option :value="100">100 / page</option>
          </select>

          <button
            @click="handlePageChange(localCurrentPage - 1)"
            :disabled="localCurrentPage <= 1"
            class="px-3 py-1 text-sm border border-gray-300 dark:border-gray-600 rounded-md text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            Previous
          </button>
          <span class="text-sm text-gray-700 dark:text-gray-300">
            Page {{ localCurrentPage }} of {{ totalPages }}
          </span>
          <button
            @click="handlePageChange(localCurrentPage + 1)"
            :disabled="localCurrentPage >= totalPages"
            class="px-3 py-1 text-sm border border-gray-300 dark:border-gray-600 rounded-md text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            Next
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed, ref, reactive, watch } from 'vue'
import { formatDistanceToNow } from 'date-fns'
import Tooltip from '@/components/BaseTooltip.vue'

const props = defineProps({
  items: {
    type: Array,
    default: () => [],
  },
  loading: {
    type: Boolean,
    default: false,
  },
  sortField: {
    type: String,
    default: 'last_seen',
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

const emit = defineEmits(['sort', 'row-click', 'expand', 'action', 'add-filter', 'page-change', 'page-size-change', 'filter-change'])

// Local state for pagination
const localCurrentPage = ref(props.currentPage)
const localPageSize = ref(props.pageSize)
const localSortField = ref(props.sortField)
const localSortDirection = ref(props.sortDirection)

// Column filters
const columnFilters = reactive({
  exception_type: '',
  message: '',
  service_name: ''
})

// Debounce timer for filter changes
let filterDebounceTimer = null

// Column definitions with filtering support - sized to fit without horizontal scroll
const columns = [
  { key: 'exception_type', label: 'Exception', sortable: true, filterable: true, align: 'left', width: 'w-[35%]' },
  { key: 'message', label: 'Message', sortable: false, filterable: true, align: 'left', width: 'w-[25%]' },
  { key: 'count', label: 'Count', sortable: true, filterable: false, align: 'center', width: 'w-16' },
  { key: 'last_seen', label: 'Last Seen', sortable: true, filterable: false, align: 'left', width: 'w-24' },
  { key: 'service_name', label: 'Service', sortable: true, filterable: true, align: 'left', width: 'w-28' },
  { key: 'actions', label: '', sortable: false, filterable: false, align: 'right', width: 'w-24' },
]

// Computed: Check if any filters are active
const hasActiveFilters = computed(() => {
  return Object.values(columnFilters).some(v => v && v.trim() !== '')
})

// Computed: Get active filters as object
const activeFilters = computed(() => {
  const filters = {}
  for (const [key, value] of Object.entries(columnFilters)) {
    if (value && value.trim() !== '') {
      const column = columns.find(c => c.key === key)
      filters[column?.label || key] = value
    }
  }
  return filters
})

// Computed: Filter items based on column filters
const filteredItems = computed(() => {
  let result = [...props.items]

  // Apply column filters
  if (columnFilters.exception_type) {
    const filter = columnFilters.exception_type.toLowerCase()
    result = result.filter(item => {
      const value = (item.exceptionType || item.exception_type || '').toLowerCase()
      return value.includes(filter)
    })
  }

  if (columnFilters.message) {
    const filter = columnFilters.message.toLowerCase()
    result = result.filter(item => {
      const value = (item.exceptionMessage || item.exception_message || item.message || '').toLowerCase()
      return value.includes(filter)
    })
  }

  if (columnFilters.service_name) {
    const filter = columnFilters.service_name.toLowerCase()
    result = result.filter(item => {
      const value = (item.serviceName || item.service_name || '').toLowerCase()
      return value.includes(filter)
    })
  }

  return result
})

// Computed: Sort filtered items
const sortedItems = computed(() => {
  if (!localSortField.value) return filteredItems.value

  const sorted = [...filteredItems.value].sort((a, b) => {
    let aVal = a[localSortField.value] || a[toCamelCase(localSortField.value)]
    let bVal = b[localSortField.value] || b[toCamelCase(localSortField.value)]

    // Handle different types
    if (localSortField.value === 'last_seen' || localSortField.value === 'first_seen') {
      aVal = new Date(aVal || 0).getTime()
      bVal = new Date(bVal || 0).getTime()
    } else if (localSortField.value === 'count') {
      aVal = parseInt(aVal) || 0
      bVal = parseInt(bVal) || 0
    } else if (typeof aVal === 'string') {
      aVal = (aVal || '').toLowerCase()
      bVal = (bVal || '').toLowerCase()
    } else {
      aVal = aVal || 0
      bVal = bVal || 0
    }

    if (aVal < bVal) return localSortDirection.value === 'asc' ? -1 : 1
    if (aVal > bVal) return localSortDirection.value === 'asc' ? 1 : -1
    return 0
  })

  return sorted
})

// Computed: Paginate sorted items
const paginatedItems = computed(() => {
  const start = (localCurrentPage.value - 1) * localPageSize.value
  const end = start + localPageSize.value
  return sortedItems.value.slice(start, end)
})

// Computed: Total pages
const totalPages = computed(() => {
  return Math.max(1, Math.ceil(filteredItems.value.length / localPageSize.value))
})

// Computed: Pagination info
const paginationStart = computed(() => {
  if (filteredItems.value.length === 0) return 0
  return (localCurrentPage.value - 1) * localPageSize.value + 1
})

const paginationEnd = computed(() => {
  return Math.min(localCurrentPage.value * localPageSize.value, filteredItems.value.length)
})

// Watch for prop changes
watch(() => props.currentPage, (val) => { localCurrentPage.value = val })
watch(() => props.pageSize, (val) => { localPageSize.value = val })
watch(() => props.sortField, (val) => { localSortField.value = val })
watch(() => props.sortDirection, (val) => { localSortDirection.value = val })

// Reset page when filters change
watch(columnFilters, () => {
  localCurrentPage.value = 1
})

// Methods
const handleSort = (field) => {
  if (localSortField.value === field) {
    localSortDirection.value = localSortDirection.value === 'asc' ? 'desc' : 'asc'
  } else {
    localSortField.value = field
    localSortDirection.value = 'desc'
  }
  emit('sort', { field: localSortField.value, direction: localSortDirection.value })
}

const handlePageChange = (page) => {
  if (page >= 1 && page <= totalPages.value) {
    localCurrentPage.value = page
    emit('page-change', page)
  }
}

const handlePageSizeChange = () => {
  localCurrentPage.value = 1
  emit('page-size-change', localPageSize.value)
}

const handleFilterChange = (key) => {
  // Debounce filter changes
  if (filterDebounceTimer) {
    clearTimeout(filterDebounceTimer)
  }
  filterDebounceTimer = setTimeout(() => {
    emit('filter-change', { ...columnFilters })
  }, 300)
}

const clearColumnFilter = (key) => {
  columnFilters[key] = ''
  emit('filter-change', { ...columnFilters })
}

const clearAllFilters = () => {
  Object.keys(columnFilters).forEach(key => {
    columnFilters[key] = ''
  })
  emit('filter-change', { ...columnFilters })
}

const handleAddServiceFilter = (service) => {
  if (service) {
    columnFilters.service_name = service
    emit('add-filter', { field: 'service', value: service })
  }
}

const handleRowClick = (item) => {
  emit('row-click', item)
}

const handleCopy = async (item) => {
  try {
    const text = JSON.stringify(item, null, 2)
    await navigator.clipboard.writeText(text)
    emit('action', { action: 'copy', item })
  } catch (err) {
    console.error('Failed to copy:', err)
  }
}

// Utility functions
const formatDate = (dateString) => {
  if (!dateString) return 'Never'
  try {
    return formatDistanceToNow(new Date(dateString), { addSuffix: true })
  } catch {
    return dateString
  }
}

const formatNumber = (num) => {
  if (num >= 1000000) {
    return (num / 1000000).toFixed(1) + 'M'
  }
  if (num >= 1000) {
    return (num / 1000).toFixed(1) + 'K'
  }
  return num.toString()
}

const truncateId = (id) => {
  if (!id) return ''
  if (id.length <= 8) return id
  return id.substring(0, 8) + '...'
}

const toCamelCase = (str) => {
  return str.replace(/_([a-z])/g, (g) => g[1].toUpperCase())
}
</script>

<style scoped>
.all-errors-table-wrapper {
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

/* Column filter input */
.column-filter-input {
  @apply w-full px-2 py-1 pr-6 text-xs font-normal normal-case;
  @apply bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded;
  @apply text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-500;
  @apply focus:outline-none focus:ring-1 focus:ring-primary-500 focus:border-primary-500;
}

/* Table styles */
table {
  @apply table-fixed w-full;
}

/* Ensure table header cells don't wrap */
thead th {
  @apply whitespace-nowrap;
}
</style>
