<template>
  <div class="filter-row">
    <!-- Conjunction (AND/OR) -->
    <div v-if="!isFirst" class="conjunction-select-wrapper">
      <select
        v-model="localFilter.conjunction"
        @change="emitUpdate"
        class="conjunction-select"
      >
        <option value="AND">AND</option>
        <option value="OR">OR</option>
      </select>
    </div>
    <div v-else class="conjunction-placeholder"></div>
    
    <!-- Attribute -->
    <AttributeSelector
      v-model="localFilter.attribute"
      :data-source="dataSource"
      :attributes="attributes"
      placeholder="Select attribute..."
      @change="handleAttributeChange"
    />
    
    <!-- Operator -->
    <select
      v-model="localFilter.operator"
      @change="handleOperatorChange"
      class="operator-select"
    >
      <optgroup label="Comparison">
        <option value="=">=</option>
        <option value="!=">!=</option>
        <option value=">">></option>
        <option value=">=">>=</option>
        <option value="<">&lt;</option>
        <option value="<=">&lt;=</option>
      </optgroup>
      <optgroup label="String">
        <option value="LIKE">LIKE</option>
        <option value="NOT LIKE">NOT LIKE</option>
        <option value="CONTAINS">Contains</option>
        <option value="NOT CONTAINS">Not Contains</option>
      </optgroup>
      <optgroup label="Set">
        <option value="IN">IN</option>
        <option value="NOT IN">NOT IN</option>
      </optgroup>
      <optgroup label="Existence">
        <option value="EXISTS">Exists</option>
        <option value="NOT EXISTS">Not Exists</option>
      </optgroup>
    </select>
    
    <!-- Value Input -->
    <div class="value-input-wrapper">
      <template v-if="localFilter.operator === 'EXISTS' || localFilter.operator === 'NOT EXISTS'">
        <!-- No value needed for EXISTS operators -->
      </template>
      <template v-else-if="localFilter.operator === 'IN' || localFilter.operator === 'NOT IN'">
        <div class="multi-value-container">
          <div class="value-tags">
            <span
              v-for="(val, idx) in valueArray"
              :key="idx"
              class="value-tag"
            >
              {{ val }}
              <button @click="removeValue(idx)" class="tag-remove">
                <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </span>
          </div>
          <input
            v-model="newValue"
            @keydown.enter.prevent="addValue"
            @keydown.backspace="handleBackspace"
            type="text"
            placeholder="Add value, press Enter"
            class="multi-value-input"
          />
        </div>
      </template>
      <template v-else-if="selectedAttributeType === 'number'">
        <input
          v-model.number="localFilter.value"
          @input="emitUpdate"
          type="number"
          placeholder="Enter number..."
          class="value-input"
        />
      </template>
      <template v-else-if="selectedAttributeType === 'datetime'">
        <input
          v-model="localFilter.value"
          @input="emitUpdate"
          type="datetime-local"
          class="value-input"
        />
      </template>
      <template v-else>
        <input
          v-model="localFilter.value"
          @input="emitUpdate"
          type="text"
          :placeholder="getPlaceholder()"
          class="value-input"
        />
      </template>
    </div>
    
    <!-- Remove Button -->
    <button
      @click="$emit('remove')"
      class="remove-btn"
      title="Remove filter"
    >
      <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
      </svg>
    </button>
  </div>
</template>

<script setup>
import { ref, reactive, computed, watch } from 'vue'
import AttributeSelector from './AttributeSelector.vue'

const props = defineProps({
  filter: {
    type: Object,
    required: true,
  },
  index: {
    type: Number,
    required: true,
  },
  dataSource: {
    type: String,
    default: 'logs',
  },
  attributes: {
    type: Array,
    default: () => [],
  },
  isFirst: {
    type: Boolean,
    default: false,
  },
})

const emit = defineEmits(['update', 'remove'])

const localFilter = reactive({
  attribute: '',
  operator: '=',
  value: '',
  conjunction: 'AND',
  ...props.filter,
})

const newValue = ref('')

// Get selected attribute type
const selectedAttributeType = computed(() => {
  const attr = props.attributes.find(a => a.name === localFilter.attribute)
  return attr?.type || 'string'
})

// Parse value array for IN/NOT IN operators
const valueArray = computed(() => {
  if (typeof localFilter.value === 'string' && localFilter.value) {
    return localFilter.value.split(',').map(v => v.trim()).filter(Boolean)
  }
  return []
})

// Get placeholder based on operator
const getPlaceholder = () => {
  if (localFilter.operator === 'LIKE' || localFilter.operator === 'NOT LIKE') {
    return 'Use % as wildcard...'
  }
  if (localFilter.operator === 'CONTAINS' || localFilter.operator === 'NOT CONTAINS') {
    return 'Enter text to search...'
  }
  return 'Enter value...'
}

// Handle attribute change
const handleAttributeChange = (attr) => {
  // Reset value when attribute changes
  localFilter.value = ''
  emitUpdate()
}

// Handle operator change
const handleOperatorChange = () => {
  // Reset value for certain operator changes
  if (localFilter.operator === 'EXISTS' || localFilter.operator === 'NOT EXISTS') {
    localFilter.value = ''
  }
  emitUpdate()
}

// Add value for IN/NOT IN
const addValue = () => {
  if (newValue.value.trim()) {
    const current = valueArray.value
    current.push(newValue.value.trim())
    localFilter.value = current.join(', ')
    newValue.value = ''
    emitUpdate()
  }
}

// Remove value from IN/NOT IN
const removeValue = (idx) => {
  const current = valueArray.value
  current.splice(idx, 1)
  localFilter.value = current.join(', ')
  emitUpdate()
}

// Handle backspace in multi-value input
const handleBackspace = () => {
  if (!newValue.value && valueArray.value.length > 0) {
    removeValue(valueArray.value.length - 1)
  }
}

// Emit update
const emitUpdate = () => {
  emit('update', { ...localFilter })
}

// Watch for external changes
watch(() => props.filter, (newFilter) => {
  Object.assign(localFilter, newFilter)
}, { deep: true })
</script>

<style scoped>
.filter-row {
  @apply flex items-center gap-2 py-2 px-3 bg-gray-50 rounded-md;
}

.conjunction-select-wrapper {
  @apply flex-shrink-0;
}

.conjunction-select {
  @apply w-16 px-2 py-1.5 text-xs font-medium bg-gray-100 border border-gray-300 text-gray-700 rounded focus:ring-2 focus:ring-primary-500;
}

.conjunction-placeholder {
  @apply w-16 flex-shrink-0;
}

.operator-select {
  @apply w-32 px-2 py-1.5 text-sm bg-white border border-gray-300 text-gray-900 rounded-md focus:ring-2 focus:ring-primary-500;
}

.value-input-wrapper {
  @apply flex-1 min-w-0;
}

.value-input {
  @apply w-full px-3 py-1.5 text-sm bg-white border border-gray-300 text-gray-900 rounded-md focus:ring-2 focus:ring-primary-500;
}

.multi-value-container {
  @apply flex flex-wrap items-center gap-1 px-2 py-1 bg-white border border-gray-300 rounded-md focus-within:ring-2 focus-within:ring-primary-500;
}

.value-tags {
  @apply flex flex-wrap gap-1;
}

.value-tag {
  @apply inline-flex items-center gap-1 px-2 py-0.5 text-xs bg-primary-100 text-primary-800 rounded;
}

.tag-remove {
  @apply text-primary-600 hover:text-primary-800;
}

.multi-value-input {
  @apply flex-1 min-w-[100px] px-1 py-0.5 text-sm bg-transparent border-none focus:outline-none text-gray-900;
}

.remove-btn {
  @apply flex-shrink-0 p-1.5 text-gray-400 hover:text-red-500 hover:bg-red-50 rounded transition-colors;
}
</style>
