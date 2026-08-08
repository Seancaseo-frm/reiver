<template>
  <div class="query-builder">
    <!-- Data Source Selector -->
    <div class="query-header">
      <div class="flex items-center gap-4">
        <div class="flex items-center gap-2">
          <span class="query-label">A</span>
          <select
            v-model="localQuery.dataSource"
            @change="handleDataSourceChange"
            class="data-source-select"
          >
            <option value="logs">Logs</option>
            <option value="traces">Traces</option>
            <option value="metrics">Metrics</option>
          </select>
        </div>
        
        <!-- View Mode Toggle -->
        <div class="flex items-center gap-1 bg-gray-100 rounded-md p-0.5">
          <button
            @click="viewMode = 'builder'"
            :class="['view-mode-btn', viewMode === 'builder' ? 'active' : '']"
          >
            Builder
          </button>
          <button
            @click="viewMode = 'clickhouse'"
            :class="['view-mode-btn', viewMode === 'clickhouse' ? 'active' : '']"
          >
            ClickHouse
          </button>
        </div>
      </div>
      
      <div class="flex items-center gap-2">
        <button
          v-if="showRunButton"
          @click="runQuery"
          :disabled="loading"
          class="run-query-btn"
        >
          <svg v-if="loading" class="animate-spin w-4 h-4 mr-1" fill="none" viewBox="0 0 24 24">
            <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
            <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
          </svg>
          <span>{{ loading ? 'Running...' : 'Run Query' }}</span>
        </button>
      </div>
    </div>

    <!-- Builder Mode -->
    <div v-if="viewMode === 'builder'" class="query-body">
      <!-- Aggregation Section -->
      <div class="query-section">
        <div class="section-label">Aggregate</div>
        <div class="flex flex-wrap items-center gap-2">
          <select
            v-model="localQuery.aggregation"
            @change="emitChange"
            class="aggregation-select"
          >
            <option value="">None</option>
            <option value="count">Count</option>
            <option value="count_distinct">Count Distinct</option>
            <option value="sum">Sum</option>
            <option value="avg">Avg</option>
            <option value="min">Min</option>
            <option value="max">Max</option>
            <option value="p50">P50</option>
            <option value="p90">P90</option>
            <option value="p95">P95</option>
            <option value="p99">P99</option>
            <option value="rate">Rate</option>
          </select>
          
          <!-- Aggregate Attribute (for count_distinct, sum, avg, etc.) -->
          <template v-if="localQuery.aggregation && localQuery.aggregation !== 'count'">
            <span class="text-gray-500">(</span>
            <AttributeSelector
              v-model="localQuery.aggregateAttribute"
              :data-source="localQuery.dataSource"
              :attributes="availableAttributes"
              placeholder="Select attribute..."
              @change="emitChange"
            />
            <span class="text-gray-500">)</span>
          </template>
        </div>
      </div>

      <!-- Filter Section -->
      <div class="query-section">
        <div class="section-label">Where</div>
        <div class="filters-container">
          <FilterRow
            v-for="(filter, index) in localQuery.filters"
            :key="index"
            :filter="filter"
            :index="index"
            :data-source="localQuery.dataSource"
            :attributes="availableAttributes"
            :is-first="index === 0"
            @update="updateFilter(index, $event)"
            @remove="removeFilter(index)"
          />
          
          <button
            @click="addFilter"
            class="add-filter-btn"
          >
            <svg class="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" />
            </svg>
            Add Filter
          </button>
        </div>
      </div>

      <!-- Group By Section -->
      <div class="query-section">
        <div class="section-label">Group By</div>
        <div class="flex flex-wrap items-center gap-2">
          <div
            v-for="(groupBy, index) in localQuery.groupBy"
            :key="index"
            class="group-by-tag"
          >
            <AttributeSelector
              v-model="localQuery.groupBy[index]"
              :data-source="localQuery.dataSource"
              :attributes="availableAttributes"
              placeholder="Select..."
              compact
              @change="emitChange"
            />
            <button
              @click="removeGroupBy(index)"
              class="group-by-remove"
            >
              <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>
          
          <button
            @click="addGroupBy"
            class="add-group-by-btn"
          >
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" />
            </svg>
          </button>
        </div>
      </div>

      <!-- Order By Section -->
      <div class="query-section">
        <div class="section-label">Order By</div>
        <div class="flex items-center gap-2">
          <select
            v-model="localQuery.orderBy.attribute"
            @change="emitChange"
            class="order-by-select"
          >
            <option value="">Default</option>
            <option value="timestamp">Timestamp</option>
            <option value="_value">Value</option>
            <option v-for="attr in availableAttributes" :key="attr.name" :value="attr.name">
              {{ attr.name }}
            </option>
          </select>
          
          <select
            v-if="localQuery.orderBy.attribute"
            v-model="localQuery.orderBy.direction"
            @change="emitChange"
            class="order-direction-select"
          >
            <option value="desc">Descending</option>
            <option value="asc">Ascending</option>
          </select>
        </div>
      </div>

      <!-- Limit Section -->
      <div class="query-section">
        <div class="section-label">Limit</div>
        <input
          v-model.number="localQuery.limit"
          @input="emitChange"
          type="number"
          placeholder="100"
          min="1"
          max="10000"
          class="limit-input"
        />
      </div>
    </div>

    <!-- ClickHouse Mode (Raw Query) -->
    <div v-else class="query-body">
      <div class="clickhouse-editor">
        <textarea
          v-model="localQuery.rawQuery"
          @input="emitChange"
          placeholder="SELECT * FROM logs WHERE project_id = '...' AND timestamp >= now() - INTERVAL 1 HOUR"
          class="raw-query-input"
          rows="4"
        ></textarea>
        <p class="text-xs text-gray-500 mt-2">
          Write raw ClickHouse SQL. Use <code class="code-hint">{{projectId}}</code> for project ID placeholder.
        </p>
      </div>
    </div>

    <!-- Generated Query Preview -->
    <div v-if="showPreview && viewMode === 'builder'" class="query-preview">
      <div class="preview-header">
        <span class="text-xs font-medium text-gray-500">Generated Query</span>
        <button @click="copyQuery" class="copy-btn">
          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
          </svg>
        </button>
      </div>
      <pre class="preview-code">{{ generatedQuery }}</pre>
    </div>
  </div>
</template>

<script setup>
import { ref, reactive, computed, watch, onMounted } from 'vue'
import AttributeSelector from './AttributeSelector.vue'
import FilterRow from './FilterRow.vue'

const props = defineProps({
  modelValue: {
    type: Object,
    default: () => ({
      dataSource: 'logs',
      aggregation: '',
      aggregateAttribute: '',
      filters: [],
      groupBy: [],
      orderBy: { attribute: '', direction: 'desc' },
      limit: 100,
      rawQuery: '',
    }),
  },
  projectId: {
    type: String,
    required: true,
  },
  showRunButton: {
    type: Boolean,
    default: true,
  },
  showPreview: {
    type: Boolean,
    default: true,
  },
  loading: {
    type: Boolean,
    default: false,
  },
})

const emit = defineEmits(['update:modelValue', 'run-query'])

const viewMode = ref('builder')

const localQuery = reactive({
  dataSource: 'logs',
  aggregation: '',
  aggregateAttribute: '',
  filters: [],
  groupBy: [],
  orderBy: { attribute: '', direction: 'desc' },
  limit: 100,
  rawQuery: '',
  ...props.modelValue,
})

// Available attributes based on data source
const availableAttributes = computed(() => {
  switch (localQuery.dataSource) {
    case 'logs':
      return [
        { name: 'timestamp', type: 'datetime' },
        { name: 'body', type: 'string' },
        { name: 'severity_text', type: 'string' },
        { name: 'severity_number', type: 'number' },
        { name: 'service_name', type: 'string' },
        { name: 'trace_id', type: 'string' },
        { name: 'span_id', type: 'string' },
        { name: 'resource.service.name', type: 'string' },
        { name: 'resource.host.name', type: 'string' },
        { name: 'resource.deployment.environment', type: 'string' },
      ]
    case 'traces':
      return [
        { name: 'timestamp', type: 'datetime' },
        { name: 'trace_id', type: 'string' },
        { name: 'span_id', type: 'string' },
        { name: 'parent_span_id', type: 'string' },
        { name: 'service_name', type: 'string' },
        { name: 'operation_name', type: 'string' },
        { name: 'span_kind', type: 'string' },
        { name: 'status_code', type: 'string' },
        { name: 'duration_ms', type: 'number' },
        { name: 'http.method', type: 'string' },
        { name: 'http.url', type: 'string' },
        { name: 'http.status_code', type: 'number' },
        { name: 'db.system', type: 'string' },
        { name: 'db.statement', type: 'string' },
      ]
    case 'metrics':
      return [
        { name: 'timestamp', type: 'datetime' },
        { name: 'metric_name', type: 'string' },
        { name: 'value', type: 'number' },
        { name: 'service_name', type: 'string' },
        { name: 'host', type: 'string' },
        { name: 'environment', type: 'string' },
      ]
    default:
      return []
  }
})

// Generate query preview
const generatedQuery = computed(() => {
  const { dataSource, aggregation, aggregateAttribute, filters, groupBy, orderBy, limit } = localQuery
  
  const table = dataSource === 'logs' ? 'reiver.logs' 
    : dataSource === 'traces' ? 'reiver.spans' 
    : 'reiver.metrics'
  
  let select = '*'
  if (aggregation) {
    if (aggregation === 'count') {
      select = 'count() as value'
    } else if (aggregation === 'count_distinct' && aggregateAttribute) {
      select = `count(DISTINCT ${aggregateAttribute}) as value`
    } else if (aggregateAttribute) {
      const aggFn = aggregation.startsWith('p') 
        ? `quantile(0.${aggregation.slice(1)})` 
        : aggregation
      select = `${aggFn}(${aggregateAttribute}) as value`
    }
    
    if (groupBy.length > 0) {
      select = `${groupBy.filter(g => g).join(', ')}, ${select}`
    }
  }
  
  let query = `SELECT ${select}\nFROM ${table}\nWHERE project_id = '${props.projectId}'`
  
  // Add filters
  filters.forEach(filter => {
    if (filter.attribute && filter.operator && filter.value !== undefined && filter.value !== '') {
      const op = getOperatorSql(filter.operator)
      const val = formatValue(filter.value, filter.operator)
      query += `\n  AND ${filter.attribute} ${op} ${val}`
    }
  })
  
  // Add group by
  if (aggregation && groupBy.length > 0) {
    const validGroupBy = groupBy.filter(g => g)
    if (validGroupBy.length > 0) {
      query += `\nGROUP BY ${validGroupBy.join(', ')}`
    }
  }
  
  // Add order by
  if (orderBy.attribute) {
    query += `\nORDER BY ${orderBy.attribute} ${orderBy.direction.toUpperCase()}`
  }
  
  // Add limit
  if (limit) {
    query += `\nLIMIT ${limit}`
  }
  
  return query
})

// Helper functions
const getOperatorSql = (op) => {
  const operators = {
    '=': '=',
    '!=': '!=',
    '>': '>',
    '>=': '>=',
    '<': '<',
    '<=': '<=',
    'LIKE': 'LIKE',
    'NOT LIKE': 'NOT LIKE',
    'IN': 'IN',
    'NOT IN': 'NOT IN',
    'EXISTS': 'IS NOT NULL',
    'NOT EXISTS': 'IS NULL',
    'CONTAINS': 'LIKE',
    'NOT CONTAINS': 'NOT LIKE',
  }
  return operators[op] || '='
}

const formatValue = (value, operator) => {
  if (operator === 'IN' || operator === 'NOT IN') {
    const values = value.split(',').map(v => `'${v.trim()}'`).join(', ')
    return `(${values})`
  }
  if (operator === 'CONTAINS' || operator === 'NOT CONTAINS') {
    return `'%${value}%'`
  }
  if (operator === 'EXISTS' || operator === 'NOT EXISTS') {
    return ''
  }
  if (typeof value === 'number') {
    return value
  }
  return `'${value}'`
}

// Event handlers
const handleDataSourceChange = () => {
  // Reset filters when data source changes
  localQuery.filters = []
  localQuery.groupBy = []
  localQuery.aggregateAttribute = ''
  emitChange()
}

const addFilter = () => {
  localQuery.filters.push({
    attribute: '',
    operator: '=',
    value: '',
    conjunction: localQuery.filters.length > 0 ? 'AND' : '',
  })
  emitChange()
}

const updateFilter = (index, filter) => {
  localQuery.filters[index] = filter
  emitChange()
}

const removeFilter = (index) => {
  localQuery.filters.splice(index, 1)
  // Update first filter's conjunction
  if (localQuery.filters.length > 0) {
    localQuery.filters[0].conjunction = ''
  }
  emitChange()
}

const addGroupBy = () => {
  localQuery.groupBy.push('')
}

const removeGroupBy = (index) => {
  localQuery.groupBy.splice(index, 1)
  emitChange()
}

const emitChange = () => {
  emit('update:modelValue', { ...localQuery })
}

const runQuery = () => {
  emit('run-query', { ...localQuery, generatedQuery: generatedQuery.value })
}

const copyQuery = async () => {
  try {
    await navigator.clipboard.writeText(generatedQuery.value)
  } catch (err) {
    console.error('Failed to copy query:', err)
  }
}

// Watch for external changes
watch(() => props.modelValue, (newValue) => {
  Object.assign(localQuery, newValue)
}, { deep: true })
</script>

<style scoped>
.query-builder {
  @apply border border-gray-200 rounded-lg bg-white overflow-hidden;
}

.query-header {
  @apply flex items-center justify-between px-4 py-3 bg-gray-50 border-b border-gray-200;
}

.query-label {
  @apply w-6 h-6 flex items-center justify-center text-xs font-bold text-white bg-purple-500 rounded;
}

.data-source-select {
  @apply px-3 py-1.5 text-sm font-medium bg-white border border-gray-300 text-gray-900 rounded-md focus:ring-2 focus:ring-primary-500 focus:border-primary-500;
}

.view-mode-btn {
  @apply px-3 py-1 text-xs font-medium text-gray-600 rounded transition-colors;
}

.view-mode-btn.active {
  @apply bg-white text-gray-900 shadow-sm;
}

.run-query-btn {
  @apply flex items-center px-4 py-1.5 text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 rounded-md disabled:opacity-50 disabled:cursor-not-allowed transition-colors;
}

.query-body {
  @apply p-4 space-y-4;
}

.query-section {
  @apply flex items-start gap-4;
}

.section-label {
  @apply w-20 flex-shrink-0 text-sm font-medium text-gray-500 pt-2;
}

.aggregation-select {
  @apply px-3 py-1.5 text-sm bg-white border border-gray-300 text-gray-900 rounded-md focus:ring-2 focus:ring-primary-500;
}

.filters-container {
  @apply flex-1 space-y-2;
}

.add-filter-btn {
  @apply flex items-center px-3 py-1.5 text-sm text-primary-600 hover:bg-primary-50 rounded-md transition-colors;
}

.group-by-tag {
  @apply flex items-center gap-1 px-2 py-1 bg-gray-100 rounded-md;
}

.group-by-remove {
  @apply p-0.5 text-gray-400 hover:text-gray-600 rounded;
}

.add-group-by-btn {
  @apply p-1.5 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-md transition-colors;
}

.order-by-select,
.order-direction-select {
  @apply px-3 py-1.5 text-sm bg-white border border-gray-300 text-gray-900 rounded-md focus:ring-2 focus:ring-primary-500;
}

.limit-input {
  @apply w-24 px-3 py-1.5 text-sm bg-white border border-gray-300 text-gray-900 rounded-md focus:ring-2 focus:ring-primary-500;
}

.clickhouse-editor {
  @apply space-y-2;
}

.raw-query-input {
  @apply w-full px-3 py-2 text-sm font-mono bg-gray-50 text-gray-800 border border-gray-200 rounded-md focus:ring-2 focus:ring-primary-500 focus:border-primary-500 resize-none;
}

.code-hint {
  @apply px-1 py-0.5 bg-gray-50 text-gray-600 rounded text-xs;
}

.query-preview {
  @apply border-t border-gray-200 bg-gray-50 p-4;
}

.preview-header {
  @apply flex items-center justify-between mb-2;
}

.copy-btn {
  @apply p-1 text-gray-400 hover:text-gray-600 rounded;
}

.preview-code {
  @apply text-xs font-mono text-gray-700 bg-white p-3 rounded border border-gray-200 overflow-x-auto whitespace-pre-wrap;
}
</style>
