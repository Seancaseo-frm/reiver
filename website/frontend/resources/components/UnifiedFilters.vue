<template>
  <div class="filters-panel">
    <!-- Header -->
    <div class="filters-header">
      <h3 class="text-sm font-semibold text-gray-900">Filters</h3>
      <button
        @click="$emit('close')"
        class="text-gray-400 hover:text-gray-600"
      >
        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
        </svg>
      </button>
    </div>

    <!-- Time Range -->
    <FilterSection title="Time Range">
      <select
        v-model="localFilters.timeRange"
        @change="applyFilters"
        class="w-full px-2 py-1 bg-white text-gray-900 border border-gray-300 rounded text-sm focus:ring-primary-500 focus:border-primary-500"
      >
        <option value="15m">Last 15 minutes</option>
        <option value="1h">Last 1 hour</option>
        <option value="24h">Last 24 hours</option>
        <option value="7d">Last 7 days</option>
        <option value="30d">Last 30 days</option>
      </select>
    </FilterSection>

    <!-- Search -->
    <FilterSection title="Search">
      <input
        v-model="localFilters.search"
        type="text"
        placeholder="Search events..."
        @input="applyFilters"
        class="w-full px-3 py-2 text-sm border border-gray-300 rounded-md bg-white text-gray-900 placeholder-gray-400 focus:ring-primary-500 focus:border-primary-500"
      />
    </FilterSection>

    <!-- Severity (for errors/logs) -->
    <FilterSection title="Severity">
      <CheckboxGroup v-model="localFilters.severity" @update:modelValue="applyFilters">
        <Checkbox value="error" label="Error" />
        <Checkbox value="warning" label="Warning" />
        <Checkbox value="info" label="Info" />
      </CheckboxGroup>
    </FilterSection>

    <!-- Status (for errors) -->
    <FilterSection title="Status">
      <CheckboxGroup v-model="localFilters.status" @update:modelValue="applyFilters">
        <Checkbox value="unresolved" label="Unresolved" />
        <Checkbox value="resolved" label="Resolved" />
        <Checkbox value="ignored" label="Ignored" />
      </CheckboxGroup>
    </FilterSection>

    <!-- Service -->
    <FilterSection title="Service">
      <input
        v-model="localFilters.service"
        type="text"
        placeholder="Filter by service..."
        @input="applyFilters"
        class="w-full px-2 py-1 bg-white text-gray-900 border border-gray-300 rounded text-sm placeholder-gray-400 focus:ring-primary-500 focus:border-primary-500"
      />
    </FilterSection>

    <!-- Duration/Latency Filter (for traces) -->
    <FilterSection title="Duration (traces)">
      <div class="space-y-2">
        <select
          v-model="localFilters.durationOperator"
          @change="applyFilters"
          class="w-full px-2 py-1 bg-white text-gray-900 border border-gray-300 rounded text-sm focus:ring-primary-500 focus:border-primary-500"
        >
          <option value="">Any duration</option>
          <option value="gt">Greater than</option>
          <option value="lt">Less than</option>
          <option value="between">Between</option>
        </select>

        <div v-if="localFilters.durationOperator" class="flex items-center gap-2">
          <input
            v-model.number="localFilters.durationMin"
            type="number"
            placeholder="ms"
            @input="applyFilters"
            class="w-20 px-2 py-1 bg-white text-gray-900 border border-gray-300 rounded text-sm"
          />
          <span v-if="localFilters.durationOperator === 'between'" class="text-gray-500 text-sm">to</span>
          <input
            v-if="localFilters.durationOperator === 'between'"
            v-model.number="localFilters.durationMax"
            type="number"
            placeholder="ms"
            @input="applyFilters"
            class="w-20 px-2 py-1 bg-white text-gray-900 border border-gray-300 rounded text-sm"
          />
          <span class="text-xs text-gray-500">ms</span>
        </div>

        <!-- Quick presets -->
        <div class="flex flex-wrap gap-1">
          <button
            v-for="preset in durationPresets"
            :key="preset.label"
            @click="setDurationPreset(preset)"
            class="px-2 py-1 text-xs bg-gray-100 text-gray-700 rounded hover:bg-gray-200 transition-colors"
          >
            {{ preset.label }}
          </button>
        </div>
      </div>
    </FilterSection>

    <!-- Active Filters Display -->
    <div v-if="hasActiveFilters" class="mt-4 p-3 bg-gray-50 rounded">
      <div class="flex items-center justify-between mb-2">
        <span class="text-xs font-semibold text-gray-700">Active Filters</span>
        <button
          @click="clearFilters"
          class="text-xs text-primary-600 hover:text-primary-700"
        >
          Clear All
        </button>
      </div>
      <div class="flex flex-wrap gap-1">
        <span
          v-if="localFilters.search"
          class="inline-flex items-center gap-1 px-2 py-1 bg-primary-100 text-primary-800 text-xs rounded"
        >
          Search: {{ localFilters.search }}
          <button @click="localFilters.search = ''; applyFilters()" class="hover:opacity-75">✕</button>
        </span>
        <span
          v-for="sev in localFilters.severity"
          :key="sev"
          class="inline-flex items-center gap-1 px-2 py-1 bg-primary-100 text-primary-800 text-xs rounded"
        >
          {{ sev }}
          <button @click="removeSeverity(sev)" class="hover:opacity-75">✕</button>
        </span>
        <span
          v-for="stat in localFilters.status"
          :key="stat"
          class="inline-flex items-center gap-1 px-2 py-1 bg-primary-100 text-primary-800 text-xs rounded"
        >
          {{ stat }}
          <button @click="removeStatus(stat)" class="hover:opacity-75">✕</button>
        </span>
        <span
          v-if="localFilters.service"
          class="inline-flex items-center gap-1 px-2 py-1 bg-primary-100 text-primary-800 text-xs rounded"
        >
          Service: {{ localFilters.service }}
          <button @click="localFilters.service = ''; applyFilters()" class="hover:opacity-75">✕</button>
        </span>
        <span
          v-if="localFilters.durationOperator"
          class="inline-flex items-center gap-1 px-2 py-1 bg-primary-100 text-primary-800 text-xs rounded"
        >
          Duration: {{ formatDurationFilter() }}
          <button @click="clearDurationFilter" class="hover:opacity-75">✕</button>
        </span>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, watch } from 'vue'
import FilterSection from '@/components/FilterSection.vue'
import CheckboxGroup from '@/components/CheckboxGroup.vue'
import Checkbox from '@/components/Checkbox.vue'

const props = defineProps({
  modelValue: {
    type: Object,
    default: () => ({
      timeRange: '24h',
      severity: [],
      status: [],
      service: '',
      search: '',
      durationOperator: '',
      durationMin: null,
      durationMax: null,
    })
  },
})

const emit = defineEmits(['update:modelValue', 'filter-change', 'close'])

// Duration presets
const durationPresets = [
  { label: '> 100ms', operator: 'gt', min: 100, max: null },
  { label: '> 500ms', operator: 'gt', min: 500, max: null },
  { label: '> 1s', operator: 'gt', min: 1000, max: null },
  { label: '> 5s', operator: 'gt', min: 5000, max: null },
]

const setDurationPreset = (preset) => {
  localFilters.value.durationOperator = preset.operator
  localFilters.value.durationMin = preset.min
  localFilters.value.durationMax = preset.max
  applyFilters()
}

const clearDurationFilter = () => {
  localFilters.value.durationOperator = ''
  localFilters.value.durationMin = null
  localFilters.value.durationMax = null
  applyFilters()
}

const formatDurationFilter = () => {
  const op = localFilters.value.durationOperator
  const min = localFilters.value.durationMin
  const max = localFilters.value.durationMax

  if (op === 'gt') return `> ${min}ms`
  if (op === 'lt') return `< ${min}ms`
  if (op === 'between') return `${min}-${max}ms`
  return ''
}

const localFilters = ref({ ...props.modelValue })

const hasActiveFilters = computed(() => {
  return (
    localFilters.value.severity.length > 0 ||
    localFilters.value.status.length > 0 ||
    localFilters.value.service.trim().length > 0 ||
    localFilters.value.search.trim().length > 0 ||
    localFilters.value.durationOperator !== ''
  )
})

const removeSeverity = (sev) => {
  localFilters.value.severity = localFilters.value.severity.filter(s => s !== sev)
  applyFilters()
}

const removeStatus = (stat) => {
  localFilters.value.status = localFilters.value.status.filter(s => s !== stat)
  applyFilters()
}

const applyFilters = () => {
  emit('update:modelValue', localFilters.value)
  emit('filter-change')
}

const clearFilters = () => {
  localFilters.value = {
    timeRange: '24h',
    severity: [],
    status: [],
    service: '',
    search: '',
    durationOperator: '',
    durationMin: null,
    durationMax: null,
  }
  applyFilters()
}

// Sync with parent
watch(() => props.modelValue, (newValue) => {
  localFilters.value = { ...newValue }
}, { deep: true })
</script>

<style scoped>
.filters-panel {
  @apply p-4 flex flex-col gap-4 h-full overflow-y-auto;
}

.filters-header {
  @apply flex items-center justify-between pb-3 border-b border-gray-200;
}
</style>
