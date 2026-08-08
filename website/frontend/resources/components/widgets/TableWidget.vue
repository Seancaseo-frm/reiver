<template>
  <div class="table-widget h-full flex flex-col">
    <!-- Loading State -->
    <div v-if="loading" class="flex-1 flex items-center justify-center">
      <div class="spinner w-8 h-8 border-2 border-primary-500 border-t-transparent rounded-full animate-spin"></div>
    </div>
    
    <!-- Error State -->
    <div v-else-if="error" class="flex-1 flex items-center justify-center">
      <div class="text-center p-4">
        <div class="text-danger-400 mb-2">
          <svg class="w-8 h-8 mx-auto" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
          </svg>
        </div>
        <p class="text-sm text-gray-400">{{ error }}</p>
      </div>
    </div>
    
    <!-- Empty State -->
    <div v-else-if="!tableData || tableData.length === 0" class="flex-1 flex items-center justify-center">
      <div class="text-center text-gray-400">
        <svg class="w-10 h-10 mx-auto mb-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 10h18M3 14h18m-9-4v8m-7 0h14a2 2 0 002-2V8a2 2 0 00-2-2H5a2 2 0 00-2 2v8a2 2 0 002 2z" />
        </svg>
        <p class="text-sm">No data available</p>
      </div>
    </div>
    
    <!-- Table -->
    <div v-else class="flex-1 overflow-auto">
      <table class="min-w-full divide-y divide-gray-200">
        <thead class="bg-gray-50 sticky top-0">
          <tr>
            <th 
              v-for="col in displayColumns" 
              :key="col"
              class="px-4 py-3 text-left text-xs font-medium text-gray-400 uppercase tracking-wider cursor-pointer hover:text-gray-700"
              @click="sortBy(col)"
            >
              <div class="flex items-center gap-1">
                {{ formatColumnName(col) }}
                <span v-if="sortColumn === col" class="text-primary-400">
                  {{ sortDirection === 'asc' ? '↑' : '↓' }}
                </span>
              </div>
            </th>
          </tr>
        </thead>
        <tbody class="bg-white divide-y divide-gray-200">
          <tr 
            v-for="(row, index) in sortedData" 
            :key="index"
            class="hover:bg-gray-50 transition-colors"
          >
            <td 
              v-for="col in displayColumns" 
              :key="col"
              class="px-4 py-3 text-sm whitespace-nowrap"
              :style="cellStyle(row[col], col)"
            >
              {{ displayCellValue(row[col], col) }}
            </td>
          </tr>
        </tbody>
      </table>
    </div>
    
    <!-- Footer -->
    <div v-if="tableData && tableData.length > 0" class="border-t border-gray-200 px-4 py-2 text-xs text-gray-500">
      Showing {{ sortedData.length }} rows
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import { useWidgetQuery, parseTimeRange } from '@/composables/useWidgetQuery'
import { formatCellValue, formatGrafanaUnit, pivotMultiSeries } from '@/utils/widgetTransforms'

const props = defineProps({
  config: {
    type: Object,
    required: true,
  },
  projectId: {
    type: String,
    required: true,
  },
  timeRange: {
    type: String,
    default: '1h',
  },
  variables: {
    type: Object,
    default: () => ({}),
  },
})

const { loading, error, executeQuery } = useWidgetQuery()
const tableData = ref([])
const columns = ref([])
const sortColumn = ref(null)
const sortDirection = ref('desc')

const widgetUnit = computed(() => props.config.unit || props.config.query?.unit)
const widgetThresholds = computed(() => props.config.query?.thresholds || props.config.thresholds || [])

// Filter out internal columns
const displayColumns = computed(() => {
  if (props.config.columns && Array.isArray(props.config.columns)) {
    return props.config.columns
  }
  const hideCols = new Set(['project_id', 'fingerprint', 'unix_milli'])
  return columns.value.filter(c => !hideCols.has(c))
})

// Sort the data
const sortedData = computed(() => {
  if (!sortColumn.value || !tableData.value) return tableData.value
  
  return [...tableData.value].sort((a, b) => {
    const aVal = a[sortColumn.value]
    const bVal = b[sortColumn.value]
    
    // Handle numeric sorting
    if (typeof aVal === 'number' && typeof bVal === 'number') {
      return sortDirection.value === 'asc' ? aVal - bVal : bVal - aVal
    }
    
    // String sorting
    const aStr = String(aVal || '')
    const bStr = String(bVal || '')
    
    if (sortDirection.value === 'asc') {
      return aStr.localeCompare(bStr)
    }
    return bStr.localeCompare(aStr)
  })
})

const sortBy = (col) => {
  if (sortColumn.value === col) {
    sortDirection.value = sortDirection.value === 'asc' ? 'desc' : 'asc'
  } else {
    sortColumn.value = col
    sortDirection.value = 'desc'
  }
}

const formatColumnName = (col) => {
  if (col.startsWith('lbl_')) {
    return col.slice(4).replace(/_/g, ' ').replace(/^\w/, c => c.toUpperCase())
  }
  return col
    .replace(/_/g, ' ')
    .replace(/([A-Z])/g, ' $1')
    .replace(/^\w/, c => c.toUpperCase())
    .trim()
}

const displayCellValue = (value, col) => {
  if (value === null || value === undefined) return '-'
  const unit = widgetUnit.value
  if (unit && typeof value === 'number') {
    return formatGrafanaUnit(value, unit)
  }
  return formatCellValue(value, col)
}

const getThresholdColor = (value) => {
  const thresholds = widgetThresholds.value
  if (!thresholds.length || typeof value !== 'number') return null
  const sorted = [...thresholds].sort((a, b) => (b.value ?? 0) - (a.value ?? 0))
  for (const t of sorted) {
    if (value >= (t.value ?? 0)) return t.color
  }
  return sorted[sorted.length - 1]?.color || null
}

const cellStyle = (value, col) => {
  if (col !== 'value' || typeof value !== 'number') return { color: '#4B5563' }
  const color = getThresholdColor(value)
  if (!color) return { color: '#4B5563' }
  return { color, fontWeight: '600' }
}

/**
 * Deduplicate PromQL time series data for table display.
 * Groups by label columns (lbl_*) and keeps only the latest row per group.
 */
const deduplicateTimeSeries = (rows, cols) => {
  const lblCols = cols.filter(c => c.startsWith('lbl_'))
  if (lblCols.length === 0 || !cols.includes('unix_milli')) return rows

  const groups = new Map()
  for (const row of rows) {
    const key = lblCols.map(c => row[c] ?? '').join('\0')
    const existing = groups.get(key)
    if (!existing || (row.unix_milli || 0) > (existing.unix_milli || 0)) {
      groups.set(key, row)
    }
  }
  return [...groups.values()]
}

const fetchData = async () => {
  if (!props.config.query) {
    return
  }
  
  try {
    const range = parseTimeRange(props.timeRange)
    const result = await executeQuery(
      props.projectId,
      props.config.query,
      range,
      props.variables
    )
    
    let processed = { columns: result.columns || [], data: result.data || [] }
    processed = pivotMultiSeries(processed, props.config)
    processed.data = deduplicateTimeSeries(processed.data, processed.columns)
    columns.value = processed.columns
    tableData.value = processed.data
  } catch (err) {
    console.error('Widget query failed:', err)
  }
}

onMounted(fetchData)

watch([() => props.timeRange, () => props.variables], fetchData, { deep: true })
</script>
