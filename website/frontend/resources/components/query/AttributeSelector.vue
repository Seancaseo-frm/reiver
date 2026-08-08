<template>
  <div class="attribute-selector" :class="{ compact }">
    <div class="relative">
      <input
        v-model="searchQuery"
        @focus="showDropdown = true"
        @blur="handleBlur"
        @input="handleSearch"
        @keydown.enter="selectFirstMatch"
        @keydown.escape="showDropdown = false"
        @keydown.down.prevent="highlightNext"
        @keydown.up.prevent="highlightPrev"
        type="text"
        :placeholder="placeholder"
        class="attribute-input"
        :class="{ compact }"
      />
      <svg
        v-if="!searchQuery"
        class="dropdown-icon"
        fill="none"
        stroke="currentColor"
        viewBox="0 0 24 24"
      >
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
      </svg>
      <button
        v-else
        @click.stop="clearSelection"
        class="clear-btn"
      >
        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
        </svg>
      </button>
    </div>
    
    <!-- Dropdown -->
    <div
      v-if="showDropdown && filteredAttributes.length > 0"
      class="attribute-dropdown"
    >
      <div class="dropdown-scroll">
        <!-- Recent/Common Attributes -->
        <div v-if="!searchQuery && commonAttributes.length > 0" class="dropdown-section">
          <div class="section-title">Common</div>
          <div
            v-for="(attr, index) in commonAttributes"
            :key="'common-' + attr.name"
            @mousedown.prevent="selectAttribute(attr)"
            :class="['dropdown-item', { highlighted: highlightedIndex === index }]"
          >
            <span class="attr-name">{{ attr.name }}</span>
            <span class="attr-type">{{ attr.type }}</span>
          </div>
        </div>
        
        <!-- All/Filtered Attributes -->
        <div class="dropdown-section">
          <div v-if="!searchQuery" class="section-title">All Attributes</div>
          <div
            v-for="(attr, index) in displayedAttributes"
            :key="attr.name"
            @mousedown.prevent="selectAttribute(attr)"
            :class="['dropdown-item', { highlighted: highlightedIndex === (commonAttributes.length + index) }]"
          >
            <span class="attr-name">{{ attr.name }}</span>
            <span class="attr-type">{{ attr.type }}</span>
          </div>
        </div>
        
        <!-- No results -->
        <div v-if="filteredAttributes.length === 0" class="no-results">
          No matching attributes
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, watch, onMounted } from 'vue'

const props = defineProps({
  modelValue: {
    type: String,
    default: '',
  },
  dataSource: {
    type: String,
    default: 'logs',
  },
  attributes: {
    type: Array,
    default: () => [],
  },
  placeholder: {
    type: String,
    default: 'Select attribute...',
  },
  compact: {
    type: Boolean,
    default: false,
  },
})

const emit = defineEmits(['update:modelValue', 'change'])

const searchQuery = ref(props.modelValue || '')
const showDropdown = ref(false)
const highlightedIndex = ref(-1)

// Common attributes that appear at the top
const commonAttributes = computed(() => {
  const common = {
    logs: ['service_name', 'severity_text', 'body'],
    traces: ['service_name', 'operation_name', 'duration_ms', 'status_code'],
    metrics: ['metric_name', 'value', 'service_name'],
  }
  
  const commonNames = common[props.dataSource] || []
  return props.attributes.filter(attr => commonNames.includes(attr.name))
})

// Filter attributes based on search
const filteredAttributes = computed(() => {
  if (!searchQuery.value) {
    return props.attributes
  }
  
  const query = searchQuery.value.toLowerCase()
  return props.attributes.filter(attr => 
    attr.name.toLowerCase().includes(query)
  )
})

// Attributes to display (excluding common ones when not searching)
const displayedAttributes = computed(() => {
  if (searchQuery.value) {
    return filteredAttributes.value
  }
  
  const commonNames = commonAttributes.value.map(a => a.name)
  return props.attributes.filter(attr => !commonNames.includes(attr.name))
})

// Select an attribute
const selectAttribute = (attr) => {
  searchQuery.value = attr.name
  showDropdown.value = false
  emit('update:modelValue', attr.name)
  emit('change', attr)
}

// Clear selection
const clearSelection = () => {
  searchQuery.value = ''
  emit('update:modelValue', '')
  emit('change', null)
}

// Handle blur with delay for click events
const handleBlur = () => {
  setTimeout(() => {
    showDropdown.value = false
  }, 150)
}

// Handle search input
const handleSearch = () => {
  showDropdown.value = true
  highlightedIndex.value = -1
}

// Select first match on Enter
const selectFirstMatch = () => {
  if (filteredAttributes.value.length > 0) {
    const index = highlightedIndex.value >= 0 ? highlightedIndex.value : 0
    const allAttrs = [...commonAttributes.value, ...displayedAttributes.value]
    if (allAttrs[index]) {
      selectAttribute(allAttrs[index])
    }
  }
}

// Keyboard navigation
const highlightNext = () => {
  const allAttrs = [...commonAttributes.value, ...displayedAttributes.value]
  if (highlightedIndex.value < allAttrs.length - 1) {
    highlightedIndex.value++
  }
}

const highlightPrev = () => {
  if (highlightedIndex.value > 0) {
    highlightedIndex.value--
  }
}

// Watch for external model changes
watch(() => props.modelValue, (newValue) => {
  if (newValue !== searchQuery.value) {
    searchQuery.value = newValue || ''
  }
})
</script>

<style scoped>
.attribute-selector {
  @apply relative;
}

.attribute-selector.compact {
  @apply inline-block;
}

.attribute-input {
  @apply w-full px-3 py-1.5 pr-8 text-sm bg-white border border-gray-300 text-gray-900 rounded-md focus:ring-2 focus:ring-primary-500 focus:border-primary-500;
  min-width: 180px;
}

.attribute-input.compact {
  @apply px-2 py-1 text-xs;
  min-width: 120px;
}

.dropdown-icon {
  @apply absolute right-2 top-1/2 transform -translate-y-1/2 w-4 h-4 text-gray-400 pointer-events-none;
}

.clear-btn {
  @apply absolute right-2 top-1/2 transform -translate-y-1/2 text-gray-400 hover:text-gray-600;
}

.attribute-dropdown {
  @apply absolute z-50 mt-1 w-full min-w-[240px] bg-white border border-gray-200 rounded-lg shadow-lg;
}

.dropdown-scroll {
  @apply max-h-64 overflow-y-auto;
}

.dropdown-section {
  @apply py-1;
}

.section-title {
  @apply px-3 py-1 text-xs font-semibold text-gray-500 uppercase tracking-wider;
}

.dropdown-item {
  @apply flex items-center justify-between px-3 py-2 cursor-pointer hover:bg-gray-100;
}

.dropdown-item.highlighted {
  @apply bg-gray-100;
}

.attr-name {
  @apply text-sm text-gray-900 font-mono;
}

.attr-type {
  @apply text-xs text-gray-500 px-1.5 py-0.5 bg-gray-100 rounded;
}

.no-results {
  @apply px-3 py-4 text-sm text-gray-500 text-center;
}
</style>
