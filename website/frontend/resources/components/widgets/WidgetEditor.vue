<template>
  <div class="widget-editor">
    <!-- Tabs -->
    <div class="editor-tabs">
      <button
        v-for="tab in tabs"
        :key="tab.id"
        @click="activeTab = tab.id"
        :class="['tab-btn', { active: activeTab === tab.id }]"
      >
        {{ tab.label }}
      </button>
    </div>

    <div class="editor-content">
      <!-- Query Tab -->
      <div v-if="activeTab === 'query'" class="tab-panel">
        <div class="form-section">
          <label class="form-label">Data Source</label>
          <select v-model="localConfig.dataSource" @change="handleDataSourceChange" class="form-select">
            <option value="logs">Logs</option>
            <option value="traces">Traces</option>
            <option value="metrics">Metrics</option>
          </select>
        </div>

        <!-- Metric Selector (for metrics data source) -->
        <div v-if="localConfig.dataSource === 'metrics'" class="form-section">
          <label class="form-label">Metric Name</label>
          <input
            v-model="localConfig.metricName"
            type="text"
            class="form-input"
            placeholder="e.g., http.server.request_duration_ms"
          />
        </div>

        <!-- Aggregation -->
        <div class="form-section">
          <label class="form-label">Aggregation</label>
          <div class="flex gap-2">
            <select v-model="localConfig.aggregation" class="form-select flex-1">
              <option value="count">Count</option>
              <option value="sum">Sum</option>
              <option value="avg">Average</option>
              <option value="min">Min</option>
              <option value="max">Max</option>
              <option value="p50">P50</option>
              <option value="p90">P90</option>
              <option value="p95">P95</option>
              <option value="p99">P99</option>
              <option value="rate">Rate</option>
            </select>
            <select v-if="localConfig.aggregation !== 'count'" v-model="localConfig.aggregateField" class="form-select flex-1">
              <option value="">Select field...</option>
              <option v-for="field in availableFields" :key="field" :value="field">{{ field }}</option>
            </select>
          </div>
        </div>

        <!-- Filters -->
        <div class="form-section">
          <label class="form-label">Filters</label>
          <div class="filter-list">
            <div
              v-for="(filter, index) in localConfig.filters"
              :key="index"
              class="filter-row"
            >
              <select v-model="filter.field" class="form-select flex-1">
                <option value="">Select field...</option>
                <option v-for="field in availableFields" :key="field" :value="field">{{ field }}</option>
              </select>
              <select v-model="filter.operator" class="form-select w-24">
                <option value="=">=</option>
                <option value="!=">!=</option>
                <option value=">">></option>
                <option value="<">&lt;</option>
                <option value="LIKE">LIKE</option>
                <option value="IN">IN</option>
              </select>
              <input v-model="filter.value" type="text" class="form-input flex-1" placeholder="Value" />
              <button @click="removeFilter(index)" class="remove-btn">
                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>
            <button @click="addFilter" class="add-btn">
              <svg class="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" />
              </svg>
              Add Filter
            </button>
          </div>
        </div>

        <!-- Group By -->
        <div class="form-section">
          <label class="form-label">Group By</label>
          <div class="group-by-list">
            <div
              v-for="(groupBy, index) in localConfig.groupBy"
              :key="index"
              class="group-by-tag"
            >
              <span>{{ groupBy }}</span>
              <button @click="removeGroupBy(index)" class="tag-remove">
                <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>
            <select v-model="newGroupBy" @change="addGroupBy" class="form-select w-40">
              <option value="">Add group by...</option>
              <option v-for="field in availableFields" :key="field" :value="field">{{ field }}</option>
            </select>
          </div>
        </div>
      </div>

      <!-- Display Tab -->
      <div v-if="activeTab === 'display'" class="tab-panel">
        <div class="form-section">
          <label class="form-label">Chart Type</label>
          <div class="chart-type-grid">
            <button
              v-for="type in chartTypes"
              :key="type.id"
              @click="localConfig.chartType = type.id"
              :class="['chart-type-btn', { active: localConfig.chartType === type.id }]"
            >
              <component :is="type.icon" class="w-6 h-6" />
              <span>{{ type.label }}</span>
            </button>
          </div>
        </div>

        <div class="form-section">
          <label class="form-label">Title</label>
          <input v-model="localConfig.title" type="text" class="form-input" placeholder="Widget title" />
        </div>

        <div class="form-section">
          <label class="form-label">Description</label>
          <textarea v-model="localConfig.description" class="form-textarea" rows="2" placeholder="Optional description"></textarea>
        </div>

        <!-- Y-Axis Settings -->
        <div class="form-section">
          <label class="form-label">Y-Axis</label>
          <div class="grid grid-cols-2 gap-3">
            <div>
              <label class="form-sub-label">Unit</label>
              <select v-model="localConfig.yAxisUnit" class="form-select">
                <option value="">None</option>
                <option value="ms">Milliseconds (ms)</option>
                <option value="s">Seconds (s)</option>
                <option value="bytes">Bytes</option>
                <option value="percent">Percent (%)</option>
                <option value="ops">Ops/sec</option>
                <option value="req">Req/sec</option>
              </select>
            </div>
            <div>
              <label class="form-sub-label">Scale</label>
              <select v-model="localConfig.yAxisScale" class="form-select">
                <option value="linear">Linear</option>
                <option value="log">Logarithmic</option>
              </select>
            </div>
          </div>
        </div>

        <!-- Legend Settings -->
        <div class="form-section">
          <label class="form-label">Legend</label>
          <div class="flex items-center gap-4">
            <label class="checkbox-label">
              <input v-model="localConfig.showLegend" type="checkbox" class="form-checkbox" />
              Show legend
            </label>
            <select v-if="localConfig.showLegend" v-model="localConfig.legendPosition" class="form-select w-32">
              <option value="bottom">Bottom</option>
              <option value="right">Right</option>
              <option value="top">Top</option>
            </select>
          </div>
        </div>
      </div>

      <!-- Thresholds Tab -->
      <div v-if="activeTab === 'thresholds'" class="tab-panel">
        <div class="form-section">
          <label class="form-label">Thresholds</label>
          <p class="form-hint">Define visual thresholds for your chart. Lines will be drawn at these values.</p>
          
          <div class="threshold-list">
            <div
              v-for="(threshold, index) in localConfig.thresholds"
              :key="index"
              class="threshold-row"
            >
              <input
                v-model="threshold.label"
                type="text"
                class="form-input flex-1"
                placeholder="Label"
              />
              <input
                v-model.number="threshold.value"
                type="number"
                class="form-input w-24"
                placeholder="Value"
              />
              <input
                v-model="threshold.color"
                type="color"
                class="color-input"
              />
              <select v-model="threshold.style" class="form-select w-24">
                <option value="solid">Solid</option>
                <option value="dashed">Dashed</option>
                <option value="dotted">Dotted</option>
              </select>
              <button @click="removeThreshold(index)" class="remove-btn">
                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>
            <button @click="addThreshold" class="add-btn">
              <svg class="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" />
              </svg>
              Add Threshold
            </button>
          </div>
        </div>

        <!-- Color Ranges -->
        <div class="form-section">
          <label class="form-label">Color Ranges</label>
          <p class="form-hint">Color the chart area based on value ranges.</p>
          
          <div class="color-range-list">
            <div
              v-for="(range, index) in localConfig.colorRanges"
              :key="index"
              class="color-range-row"
            >
              <input
                v-model.number="range.from"
                type="number"
                class="form-input w-24"
                placeholder="From"
              />
              <span class="text-gray-500">to</span>
              <input
                v-model.number="range.to"
                type="number"
                class="form-input w-24"
                placeholder="To"
              />
              <input
                v-model="range.color"
                type="color"
                class="color-input"
              />
              <button @click="removeColorRange(index)" class="remove-btn">
                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>
            <button @click="addColorRange" class="add-btn">
              <svg class="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" />
              </svg>
              Add Color Range
            </button>
          </div>
        </div>
      </div>

      <!-- Links Tab -->
      <div v-if="activeTab === 'links'" class="tab-panel">
        <div class="form-section">
          <label class="form-label">Context Links</label>
          <p class="form-hint">Add links that appear when clicking on data points.</p>
          
          <div class="link-list">
            <div
              v-for="(link, index) in localConfig.contextLinks"
              :key="index"
              class="link-row"
            >
              <input
                v-model="link.label"
                type="text"
                class="form-input flex-1"
                placeholder="Link label"
              />
              <input
                v-model="link.url"
                type="text"
                class="form-input flex-2"
                placeholder="URL (use ${field} for variables)"
              />
              <button @click="removeContextLink(index)" class="remove-btn">
                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>
            <button @click="addContextLink" class="add-btn">
              <svg class="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" />
              </svg>
              Add Context Link
            </button>
          </div>
        </div>

        <div class="form-section">
          <label class="form-label">Drill-down Link</label>
          <input
            v-model="localConfig.drilldownUrl"
            type="text"
            class="form-input"
            placeholder="URL to navigate when clicking the widget"
          />
          <p class="form-hint mt-1">Use variables like ${service}, ${time_start}, ${time_end}</p>
        </div>
      </div>
    </div>

    <!-- Footer -->
    <div class="editor-footer">
      <button @click="$emit('cancel')" class="cancel-btn">Cancel</button>
      <button @click="handleSave" class="save-btn">Apply</button>
    </div>
  </div>
</template>

<script setup>
import { ref, reactive, computed, watch, h } from 'vue'

const props = defineProps({
  widget: {
    type: Object,
    required: true,
  },
})

const emit = defineEmits(['save', 'cancel'])

const activeTab = ref('query')
const newGroupBy = ref('')

const tabs = [
  { id: 'query', label: 'Query' },
  { id: 'display', label: 'Display' },
  { id: 'thresholds', label: 'Thresholds' },
  { id: 'links', label: 'Links' },
]

// Chart type icons
const LineIcon = { render: () => h('svg', { fill: 'none', stroke: 'currentColor', viewBox: '0 0 24 24' }, [
  h('path', { 'stroke-linecap': 'round', 'stroke-linejoin': 'round', 'stroke-width': '2', d: 'M7 12l3-3 3 3 4-4' })
])}
const BarIcon = { render: () => h('svg', { fill: 'none', stroke: 'currentColor', viewBox: '0 0 24 24' }, [
  h('path', { 'stroke-linecap': 'round', 'stroke-linejoin': 'round', 'stroke-width': '2', d: 'M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10' })
])}
const AreaIcon = { render: () => h('svg', { fill: 'none', stroke: 'currentColor', viewBox: '0 0 24 24' }, [
  h('path', { 'stroke-linecap': 'round', 'stroke-linejoin': 'round', 'stroke-width': '2', d: 'M3 19l5-5 4 4 8-8' })
])}
const PieIcon = { render: () => h('svg', { fill: 'none', stroke: 'currentColor', viewBox: '0 0 24 24' }, [
  h('path', { 'stroke-linecap': 'round', 'stroke-linejoin': 'round', 'stroke-width': '2', d: 'M11 3.055A9.001 9.001 0 1020.945 13H11V3.055z' })
])}
const StatIcon = { render: () => h('svg', { fill: 'none', stroke: 'currentColor', viewBox: '0 0 24 24' }, [
  h('path', { 'stroke-linecap': 'round', 'stroke-linejoin': 'round', 'stroke-width': '2', d: 'M9 7h6m0 10v-3m-3 3h.01M9 17h.01M9 14h.01M12 14h.01M15 11h.01M12 11h.01M9 11h.01M7 21h10a2 2 0 002-2V5a2 2 0 00-2-2H7a2 2 0 00-2 2v14a2 2 0 002 2z' })
])}
const TableIcon = { render: () => h('svg', { fill: 'none', stroke: 'currentColor', viewBox: '0 0 24 24' }, [
  h('path', { 'stroke-linecap': 'round', 'stroke-linejoin': 'round', 'stroke-width': '2', d: 'M3 10h18M3 14h18m-9-4v8m-7 0h14a2 2 0 002-2V8a2 2 0 00-2-2H5a2 2 0 00-2 2v8a2 2 0 002 2z' })
])}

const chartTypes = [
  { id: 'line', label: 'Line', icon: LineIcon },
  { id: 'bar', label: 'Bar', icon: BarIcon },
  { id: 'area', label: 'Area', icon: AreaIcon },
  { id: 'pie', label: 'Pie', icon: PieIcon },
  { id: 'stat', label: 'Stat', icon: StatIcon },
  { id: 'table', label: 'Table', icon: TableIcon },
]

// Local config state
const localConfig = reactive({
  dataSource: 'traces',
  metricName: '',
  aggregation: 'count',
  aggregateField: '',
  filters: [],
  groupBy: [],
  chartType: 'line',
  title: '',
  description: '',
  yAxisUnit: '',
  yAxisScale: 'linear',
  showLegend: true,
  legendPosition: 'bottom',
  thresholds: [],
  colorRanges: [],
  contextLinks: [],
  drilldownUrl: '',
  ...props.widget.widget_config,
})

// Available fields based on data source
const availableFields = computed(() => {
  switch (localConfig.dataSource) {
    case 'logs':
      return ['service_name', 'severity_text', 'body', 'trace_id', 'span_id']
    case 'traces':
      return ['service_name', 'operation_name', 'duration_ms', 'status_code', 'http.method', 'http.url']
    case 'metrics':
      return ['value', 'service_name', 'host', 'environment']
    default:
      return []
  }
})

// Data source change handler
const handleDataSourceChange = () => {
  localConfig.filters = []
  localConfig.groupBy = []
  localConfig.aggregateField = ''
}

// Filter management
const addFilter = () => {
  localConfig.filters.push({ field: '', operator: '=', value: '' })
}

const removeFilter = (index) => {
  localConfig.filters.splice(index, 1)
}

// Group by management
const addGroupBy = () => {
  if (newGroupBy.value && !localConfig.groupBy.includes(newGroupBy.value)) {
    localConfig.groupBy.push(newGroupBy.value)
    newGroupBy.value = ''
  }
}

const removeGroupBy = (index) => {
  localConfig.groupBy.splice(index, 1)
}

// Threshold management
const addThreshold = () => {
  localConfig.thresholds.push({
    label: '',
    value: 0,
    color: '#EF4444',
    style: 'dashed',
  })
}

const removeThreshold = (index) => {
  localConfig.thresholds.splice(index, 1)
}

// Color range management
const addColorRange = () => {
  localConfig.colorRanges.push({
    from: 0,
    to: 100,
    color: '#10B981',
  })
}

const removeColorRange = (index) => {
  localConfig.colorRanges.splice(index, 1)
}

// Context link management
const addContextLink = () => {
  localConfig.contextLinks.push({
    label: '',
    url: '',
  })
}

const removeContextLink = (index) => {
  localConfig.contextLinks.splice(index, 1)
}

// Save handler
const handleSave = () => {
  emit('save', {
    ...props.widget,
    title: localConfig.title,
    widget_config: { ...localConfig },
  })
}

// Watch for widget changes
watch(() => props.widget, (newWidget) => {
  Object.assign(localConfig, {
    dataSource: 'traces',
    metricName: '',
    aggregation: 'count',
    aggregateField: '',
    filters: [],
    groupBy: [],
    chartType: 'line',
    title: '',
    description: '',
    yAxisUnit: '',
    yAxisScale: 'linear',
    showLegend: true,
    legendPosition: 'bottom',
    thresholds: [],
    colorRanges: [],
    contextLinks: [],
    drilldownUrl: '',
    ...newWidget.widget_config,
  })
}, { deep: true })
</script>

<style scoped>
.widget-editor {
  @apply flex flex-col h-full;
}

.editor-tabs {
  @apply flex border-b border-gray-200 bg-gray-50;
}

.tab-btn {
  @apply px-4 py-3 text-sm font-medium text-gray-600 hover:text-gray-900 border-b-2 border-transparent transition-colors;
}

.tab-btn.active {
  @apply text-primary-600 border-primary-600;
}

.editor-content {
  @apply flex-1 overflow-y-auto;
}

.tab-panel {
  @apply p-4 space-y-4;
}

.form-section {
  @apply space-y-2;
}

.form-label {
  @apply block text-sm font-medium text-gray-900;
}

.form-sub-label {
  @apply block text-xs text-gray-500 mb-1;
}

.form-hint {
  @apply text-xs text-gray-500;
}

.form-input {
  @apply w-full px-3 py-2 text-sm bg-white border border-gray-300 text-gray-900 rounded-md focus:ring-2 focus:ring-primary-500;
}

.form-select {
  @apply px-3 py-2 text-sm bg-white border border-gray-300 text-gray-900 rounded-md focus:ring-2 focus:ring-primary-500;
}

.form-textarea {
  @apply w-full px-3 py-2 text-sm bg-white border border-gray-300 text-gray-900 rounded-md focus:ring-2 focus:ring-primary-500 resize-none;
}

.form-checkbox {
  @apply h-4 w-4 text-primary-600 bg-gray-100 border-gray-300 rounded focus:ring-primary-500;
}

.checkbox-label {
  @apply flex items-center gap-2 text-sm text-gray-700 cursor-pointer;
}

.filter-list {
  @apply space-y-2;
}

.filter-row {
  @apply flex items-center gap-2;
}

.group-by-list {
  @apply flex flex-wrap items-center gap-2;
}

.group-by-tag {
  @apply flex items-center gap-1 px-2 py-1 bg-gray-100 text-gray-900 text-sm rounded;
}

.tag-remove {
  @apply text-gray-400 hover:text-gray-600;
}

.chart-type-grid {
  @apply grid grid-cols-3 gap-2;
}

.chart-type-btn {
  @apply flex flex-col items-center gap-1 p-3 border border-gray-200 rounded-lg text-gray-600 hover:border-primary-500 hover:text-primary-600 transition-colors;
}

.chart-type-btn.active {
  @apply border-primary-500 text-primary-600 bg-primary-50;
}

.threshold-list,
.color-range-list,
.link-list {
  @apply space-y-2 mt-2;
}

.threshold-row,
.color-range-row,
.link-row {
  @apply flex items-center gap-2;
}

.color-input {
  @apply w-10 h-8 p-0.5 border border-gray-300 rounded cursor-pointer;
}

.add-btn {
  @apply flex items-center px-3 py-2 text-sm text-primary-600 hover:bg-primary-50 rounded-md transition-colors;
}

.remove-btn {
  @apply p-1.5 text-gray-400 hover:text-red-500 hover:bg-red-50 rounded transition-colors;
}

.editor-footer {
  @apply flex items-center justify-end gap-2 px-4 py-3 border-t border-gray-200 bg-gray-50;
}

.cancel-btn {
  @apply px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-100 rounded-md transition-colors;
}

.save-btn {
  @apply px-4 py-2 text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 rounded-md transition-colors;
}
</style>
