<template>
  <div class="multi-query-builder">
    <!-- Query Tabs -->
    <div class="query-tabs">
      <div class="tabs-container">
        <button
          v-for="(query, index) in localQueries"
          :key="index"
          @click="activeQueryIndex = index"
          :class="['query-tab', { active: activeQueryIndex === index }]"
        >
          <span class="query-letter">{{ getQueryLetter(index) }}</span>
          <span class="query-name">{{ query.name || `Query ${getQueryLetter(index)}` }}</span>
          <button
            v-if="localQueries.length > 1"
            @click.stop="removeQuery(index)"
            class="tab-close"
          >
            <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </button>
        
        <button
          v-if="localQueries.length < maxQueries"
          @click="addQuery"
          class="add-query-btn"
          title="Add Query"
        >
          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" />
          </svg>
        </button>
      </div>
      
      <div class="flex items-center gap-2">
        <button
          @click="runAllQueries"
          :disabled="loading"
          class="run-all-btn"
        >
          <svg v-if="loading" class="animate-spin w-4 h-4 mr-1" fill="none" viewBox="0 0 24 24">
            <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
            <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
          </svg>
          <span>{{ loading ? 'Running...' : 'Run All' }}</span>
        </button>
      </div>
    </div>
    
    <!-- Active Query Builder -->
    <div class="active-query">
      <QueryBuilder
        v-model="localQueries[activeQueryIndex]"
        :project-id="projectId"
        :show-run-button="false"
        :show-preview="showPreview"
        @run-query="handleSingleQuery"
      />
    </div>
    
    <!-- Formula Section -->
    <div v-if="showFormula && localQueries.length > 1" class="formula-section">
      <div class="formula-header">
        <h4 class="text-sm font-medium text-gray-900">Formula</h4>
        <button
          @click="showFormulaHelp = !showFormulaHelp"
          class="help-btn"
        >
          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8.228 9c.549-1.165 2.03-2 3.772-2 2.21 0 4 1.343 4 3 0 1.4-1.278 2.575-3.006 2.907-.542.104-.994.54-.994 1.093m0 3h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
        </button>
      </div>
      
      <div v-if="showFormulaHelp" class="formula-help">
        <p class="text-xs text-gray-600 mb-2">
          Combine queries using formulas. Reference queries by their letter (A, B, C, etc.).
        </p>
        <div class="text-xs text-gray-500 space-y-1">
          <div><code>A + B</code> - Sum of query A and B</div>
          <div><code>A - B</code> - Difference</div>
          <div><code>A * B</code> - Product</div>
          <div><code>A / B</code> - Division (with null check)</div>
          <div><code>(A / B) * 100</code> - Percentage</div>
        </div>
      </div>
      
      <div class="formula-input-container">
        <input
          v-model="localFormula"
          @input="emitChange"
          type="text"
          placeholder="e.g., (A / B) * 100"
          class="formula-input"
        />
        <div class="formula-preview">
          <span
            v-for="(letter, index) in usedQueryLetters"
            :key="letter"
            class="formula-query-tag"
            :style="{ backgroundColor: getQueryColor(index) }"
          >
            {{ letter }}
          </span>
        </div>
      </div>
    </div>
    
    <!-- Query Summary -->
    <div v-if="localQueries.length > 1" class="query-summary">
      <div
        v-for="(query, index) in localQueries"
        :key="index"
        class="summary-item"
        @click="activeQueryIndex = index"
      >
        <span
          class="summary-letter"
          :style="{ backgroundColor: getQueryColor(index) }"
        >
          {{ getQueryLetter(index) }}
        </span>
        <span class="summary-text">
          {{ getQuerySummary(query) }}
        </span>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, reactive, computed, watch } from 'vue'
import QueryBuilder from './QueryBuilder.vue'

const props = defineProps({
  modelValue: {
    type: Object,
    default: () => ({
      queries: [createEmptyQuery()],
      formula: '',
    }),
  },
  projectId: {
    type: String,
    required: true,
  },
  showPreview: {
    type: Boolean,
    default: false,
  },
  showFormula: {
    type: Boolean,
    default: true,
  },
  maxQueries: {
    type: Number,
    default: 5,
  },
  loading: {
    type: Boolean,
    default: false,
  },
})

const emit = defineEmits(['update:modelValue', 'run-query', 'run-all'])

const activeQueryIndex = ref(0)
const showFormulaHelp = ref(false)

// Local state
const localQueries = reactive([...props.modelValue.queries] || [createEmptyQuery()])
const localFormula = ref(props.modelValue.formula || '')

// Query colors for visual distinction
const queryColors = [
  '#8B5CF6', // Purple (A)
  '#3B82F6', // Blue (B)
  '#10B981', // Green (C)
  '#F59E0B', // Amber (D)
  '#EF4444', // Red (E)
]

function createEmptyQuery() {
  return {
    dataSource: 'logs',
    aggregation: '',
    aggregateAttribute: '',
    filters: [],
    groupBy: [],
    orderBy: { attribute: '', direction: 'desc' },
    limit: 100,
    rawQuery: '',
    name: '',
  }
}

// Get query letter (A, B, C, etc.)
const getQueryLetter = (index) => {
  return String.fromCharCode(65 + index) // A = 65 in ASCII
}

// Get query color
const getQueryColor = (index) => {
  return queryColors[index % queryColors.length]
}

// Used query letters in formula
const usedQueryLetters = computed(() => {
  const letters = []
  const formula = localFormula.value.toUpperCase()
  
  for (let i = 0; i < localQueries.length; i++) {
    const letter = getQueryLetter(i)
    if (formula.includes(letter)) {
      letters.push(letter)
    }
  }
  
  return letters
})

// Generate query summary
const getQuerySummary = (query) => {
  if (!query) return 'Empty query'
  
  let summary = query.dataSource
  
  if (query.aggregation) {
    summary = `${query.aggregation}(${query.aggregateAttribute || '*'})`
  }
  
  if (query.filters && query.filters.length > 0) {
    const filterCount = query.filters.filter(f => f.attribute).length
    if (filterCount > 0) {
      summary += ` with ${filterCount} filter${filterCount > 1 ? 's' : ''}`
    }
  }
  
  if (query.groupBy && query.groupBy.length > 0) {
    const groupCount = query.groupBy.filter(Boolean).length
    if (groupCount > 0) {
      summary += ` by ${query.groupBy.filter(Boolean).join(', ')}`
    }
  }
  
  return summary
}

// Add a new query
const addQuery = () => {
  if (localQueries.length < props.maxQueries) {
    localQueries.push(createEmptyQuery())
    activeQueryIndex.value = localQueries.length - 1
    emitChange()
  }
}

// Remove a query
const removeQuery = (index) => {
  if (localQueries.length > 1) {
    localQueries.splice(index, 1)
    
    // Adjust active index if necessary
    if (activeQueryIndex.value >= localQueries.length) {
      activeQueryIndex.value = localQueries.length - 1
    }
    
    emitChange()
  }
}

// Handle single query run
const handleSingleQuery = (query) => {
  emit('run-query', { query, index: activeQueryIndex.value })
}

// Run all queries
const runAllQueries = () => {
  emit('run-all', {
    queries: [...localQueries],
    formula: localFormula.value,
  })
}

// Emit changes
const emitChange = () => {
  emit('update:modelValue', {
    queries: [...localQueries],
    formula: localFormula.value,
  })
}

// Watch for local query changes
watch(localQueries, () => {
  emitChange()
}, { deep: true })

// Watch for external changes
watch(() => props.modelValue, (newValue) => {
  if (newValue.queries) {
    localQueries.splice(0, localQueries.length, ...newValue.queries)
  }
  if (newValue.formula !== undefined) {
    localFormula.value = newValue.formula
  }
}, { deep: true })
</script>

<style scoped>
.multi-query-builder {
  @apply border border-gray-200 rounded-lg bg-white overflow-hidden;
}

.query-tabs {
  @apply flex items-center justify-between px-4 py-2 bg-gray-50 border-b border-gray-200;
}

.tabs-container {
  @apply flex items-center gap-1;
}

.query-tab {
  @apply flex items-center gap-2 px-3 py-1.5 text-sm text-gray-600 hover:text-gray-900 hover:bg-gray-100 rounded-t-md transition-colors;
}

.query-tab.active {
  @apply bg-white text-gray-900 border-x border-t border-gray-200 -mb-px;
}

.query-letter {
  @apply w-5 h-5 flex items-center justify-center text-xs font-bold text-white bg-purple-500 rounded;
}

.query-name {
  @apply hidden sm:inline;
}

.tab-close {
  @apply p-0.5 text-gray-400 hover:text-red-500 rounded;
}

.add-query-btn {
  @apply p-1.5 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-md transition-colors;
}

.run-all-btn {
  @apply flex items-center px-4 py-1.5 text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 rounded-md disabled:opacity-50 disabled:cursor-not-allowed transition-colors;
}

.active-query {
  @apply p-0;
}

.active-query :deep(.query-builder) {
  @apply border-0 rounded-none;
}

.formula-section {
  @apply px-4 py-3 border-t border-gray-200 bg-gray-50;
}

.formula-header {
  @apply flex items-center justify-between mb-2;
}

.help-btn {
  @apply p-1 text-gray-400 hover:text-gray-600 rounded;
}

.formula-help {
  @apply mb-3 p-3 bg-white rounded-md border border-gray-200;
}

.formula-help code {
  @apply px-1 py-0.5 bg-gray-100 rounded text-xs font-mono;
}

.formula-input-container {
  @apply flex items-center gap-2;
}

.formula-input {
  @apply flex-1 px-3 py-2 text-sm font-mono bg-white border border-gray-300 text-gray-900 rounded-md focus:ring-2 focus:ring-primary-500;
}

.formula-preview {
  @apply flex items-center gap-1;
}

.formula-query-tag {
  @apply w-5 h-5 flex items-center justify-center text-xs font-bold text-white rounded;
}

.query-summary {
  @apply px-4 py-2 border-t border-gray-200 bg-gray-50 space-y-1;
}

.summary-item {
  @apply flex items-center gap-2 px-2 py-1 rounded hover:bg-gray-100 cursor-pointer;
}

.summary-letter {
  @apply w-4 h-4 flex items-center justify-center text-[10px] font-bold text-white rounded;
}

.summary-text {
  @apply text-xs text-gray-600 truncate;
}
</style>
