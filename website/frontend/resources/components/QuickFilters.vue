<template>
  <div class="quick-filters-panel">
    <div class="quick-filters-header">
      <h3 class="text-sm font-semibold text-gray-900 mb-4">
        Quick Filters
      </h3>
    </div>

    <div class="quick-filters-content space-y-4">
      <!-- Time Range Filter -->
      <div class="filter-group">
        <label class="filter-label">Time Range</label>
        <select
          v-model="localFilters.timeRange"
          @change="handleFilterChange"
          class="filter-select"
        >
          <option value="15m">Last 15 minutes</option>
          <option value="1h">Last 1 hour</option>
          <option value="24h">Last 24 hours</option>
          <option value="7d">Last 7 days</option>
          <option value="30d">Last 30 days</option>
        </select>
      </div>

      <!-- Severity Filter -->
      <div v-if="showSeverityFilters" class="filter-group">
        <label class="filter-label">Severity</label>
        <div class="checkbox-group">
          <label v-for="severity in severityOptions" :key="severity" class="checkbox-item">
            <input
              type="checkbox"
              :value="severity"
              v-model="localFilters.severity"
              @change="handleFilterChange"
              class="checkbox-input"
            />
            <span class="checkbox-label">{{ severity }}</span>
          </label>
        </div>
      </div>

      <!-- Trace outcome (OpenTelemetry trace-level ok vs error spans) -->
      <div v-if="showTraceOutcomeFilters" class="filter-group">
        <label class="filter-label">Trace outcome</label>
        <p class="text-xs text-gray-500 mb-2">
          Filter by whether any span in the trace recorded an error status.
        </p>
        <div class="checkbox-group">
          <label class="checkbox-item">
            <input
              type="checkbox"
              value="error"
              v-model="localFilters.traceOutcome"
              @change="handleFilterChange"
              class="checkbox-input"
            />
            <span class="checkbox-label">Error</span>
          </label>
          <label class="checkbox-item">
            <input
              type="checkbox"
              value="ok"
              v-model="localFilters.traceOutcome"
              @change="handleFilterChange"
              class="checkbox-input"
            />
            <span class="checkbox-label">Success</span>
          </label>
        </div>
      </div>

      <!-- Status Filter (exception workflow — hidden on Traces) -->
      <div v-if="showExceptionStatusFilters" class="filter-group">
        <label class="filter-label">Status</label>
        <div class="checkbox-group">
          <label v-for="status in statusOptions" :key="status" class="checkbox-item">
            <input
              type="checkbox"
              :value="status"
              v-model="localFilters.status"
              @change="handleFilterChange"
              class="checkbox-input"
            />
            <span class="checkbox-label">{{ status }}</span>
          </label>
        </div>
      </div>

      <!-- Service Filter (with checkboxes if values available) -->
      <div class="filter-group">
        <label class="filter-label">Service</label>
        <div v-if="availableServiceNames.length > 0" class="checkbox-group-scrollable">
          <label v-for="service in availableServiceNames" :key="service" class="checkbox-item">
            <input
              type="checkbox"
              :value="service"
              v-model="localFilters.serviceNames"
              @change="handleFilterChange"
              class="checkbox-input"
            />
            <span class="checkbox-label truncate" :title="service">{{ service }}</span>
          </label>
        </div>
        <input
          v-else
          v-model="localFilters.service"
          @input="debouncedFilterChange"
          type="text"
          placeholder="Filter by service name"
          class="filter-input"
        />
      </div>

      <!-- Search Filter -->
      <div v-if="showSearchFilter" class="filter-group">
        <label class="filter-label">Search</label>
        <input
          v-model="localFilters.search"
          @input="debouncedFilterChange"
          type="text"
          placeholder="Search..."
          class="filter-input"
        />
      </div>

      <!-- Environment Filter -->
      <div v-if="showContextFilters" class="filter-group">
        <label class="filter-label">Environment</label>
        <div v-if="availableEnvironments.length > 0" class="checkbox-group-scrollable">
          <label v-for="env in availableEnvironments" :key="env" class="checkbox-item">
            <input
              type="checkbox"
              :value="env"
              v-model="localFilters.environments"
              @change="handleFilterChange"
              class="checkbox-input"
            />
            <span class="checkbox-label truncate" :title="env">{{ env }}</span>
          </label>
        </div>
        <p v-else class="text-xs text-gray-500 italic">No values available</p>
      </div>

      <!-- Version Filter -->
      <div v-if="showContextFilters" class="filter-group">
        <label class="filter-label">Version</label>
        <div v-if="availableVersions.length > 0" class="checkbox-group-scrollable">
          <label v-for="version in availableVersions" :key="version" class="checkbox-item">
            <input
              type="checkbox"
              :value="version"
              v-model="localFilters.versions"
              @change="handleFilterChange"
              class="checkbox-input"
            />
            <span class="checkbox-label truncate" :title="version">{{ version }}</span>
          </label>
        </div>
        <p v-else class="text-xs text-gray-500 italic">No values available</p>
      </div>

      <!-- Region Filter -->
      <div v-if="showContextFilters" class="filter-group">
        <label class="filter-label">Region</label>
        <div v-if="availableRegions.length > 0" class="checkbox-group-scrollable">
          <label v-for="region in availableRegions" :key="region" class="checkbox-item">
            <input
              type="checkbox"
              :value="region"
              v-model="localFilters.regions"
              @change="handleFilterChange"
              class="checkbox-input"
            />
            <span class="checkbox-label truncate" :title="region">{{ region }}</span>
          </label>
        </div>
        <p v-else class="text-xs text-gray-500 italic">No values available</p>
      </div>

      <!-- Host Name Filter -->
      <div v-if="showContextFilters" class="filter-group">
        <label class="filter-label">Host Name</label>
        <input
          v-if="availableHostNames.length > 5"
          v-model="hostNameSearch"
          type="text"
          placeholder="Search hosts..."
          class="filter-input mb-1"
        />
        <div v-if="availableHostNames.length > 0" class="checkbox-group-scrollable">
          <label v-for="hostName in filteredHostNames" :key="hostName" class="checkbox-item">
            <input
              type="checkbox"
              :value="hostName"
              v-model="localFilters.hostNames"
              @change="handleFilterChange"
              class="checkbox-input"
            />
            <span class="checkbox-label truncate" :title="hostName">{{ hostName }}</span>
          </label>
        </div>
        <p v-else class="text-xs text-gray-500 italic">No values available</p>
      </div>

      <!-- Pod Name Filter (K8s) -->
      <div v-if="showContextFilters" class="filter-group">
        <label class="filter-label">Pod Name</label>
        <input
          v-if="availablePodNames.length > 5"
          v-model="podNameSearch"
          type="text"
          placeholder="Search pods..."
          class="filter-input mb-1"
        />
        <div v-if="availablePodNames.length > 0" class="checkbox-group-scrollable">
          <label v-for="podName in filteredPodNames" :key="podName" class="checkbox-item">
            <input
              type="checkbox"
              :value="podName"
              v-model="localFilters.podNames"
              @change="handleFilterChange"
              class="checkbox-input"
            />
            <span class="checkbox-label truncate" :title="podName">{{ podName }}</span>
          </label>
        </div>
        <p v-else class="text-xs text-gray-500 italic">No values available</p>
      </div>

      <!-- Dynamic Attribute Filters -->
      <template v-if="showContextFilters && attributeKeys.length > 0">
        <div
          v-for="(attrFilter, idx) in localFilters.attributeFilters"
          :key="'attr-' + attrFilter.key"
          class="filter-group"
        >
          <div class="flex items-center justify-between">
            <label class="filter-label truncate" :title="attrFilter.key">{{ attrFilter.key }}</label>
            <button
              @click="removeAttributeFilter(idx)"
              class="text-gray-400 hover:text-red-500 transition-colors p-0.5"
              title="Remove filter"
            >
              <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/>
              </svg>
            </button>
          </div>
          <input
            v-if="(attributeValuesMap[attrFilter.key] || []).length > 5"
            v-model="attrValueSearchMap[attrFilter.key]"
            type="text"
            placeholder="Search values..."
            class="filter-input mb-1"
          />
          <div v-if="(attributeValuesMap[attrFilter.key] || []).length > 0" class="checkbox-group-scrollable">
            <label
              v-for="val in filteredAttrValues(attrFilter.key)"
              :key="val"
              class="checkbox-item"
            >
              <input
                type="checkbox"
                :value="val"
                v-model="attrFilter.values"
                @change="handleFilterChange"
                class="checkbox-input"
              />
              <span class="checkbox-label truncate" :title="val">{{ val }}</span>
            </label>
          </div>
          <p v-else class="text-xs text-gray-500 italic">Loading values...</p>
        </div>

        <div v-if="availableAttrKeysToAdd.length > 0" class="filter-group">
          <div v-if="showingKeyPicker" class="space-y-1">
            <input
              v-model="attrKeySearch"
              type="text"
              placeholder="Search attribute keys..."
              class="filter-input"
              ref="attrKeyInput"
            />
            <div class="checkbox-group-scrollable max-h-40">
              <button
                v-for="key in filteredAttrKeysToAdd"
                :key="key"
                @click="addAttributeFilter(key)"
                class="block w-full text-left px-2 py-1 text-sm text-gray-700 hover:bg-primary-50 hover:text-primary-700 rounded truncate"
                :title="key"
              >
                {{ key }}
              </button>
            </div>
            <button
              @click="showingKeyPicker = false; attrKeySearch = ''"
              class="text-xs text-gray-500 hover:text-gray-700"
            >
              Cancel
            </button>
          </div>
          <button
            v-else
            @click="showingKeyPicker = true"
            class="w-full px-3 py-1.5 text-xs font-medium text-primary-600 bg-primary-50 border border-primary-200 rounded-md hover:bg-primary-100 transition-colors"
          >
            + Add Attribute Filter
          </button>
        </div>
      </template>

      <!-- Duration Filters (for traces) -->
      <div v-if="showDurationFilters" class="filter-group">
        <label class="filter-label">Duration</label>
        <select
          v-model="localFilters.durationOperator"
          @change="handleFilterChange"
          class="filter-select mb-2"
        >
          <option value="">Any duration</option>
          <option value="gt">Greater than</option>
          <option value="lt">Less than</option>
          <option value="between">Between</option>
        </select>

        <div v-if="localFilters.durationOperator === 'between'" class="flex gap-2">
          <input
            v-model="localFilters.durationMin"
            @input="debouncedFilterChange"
            type="number"
            placeholder="Min (ms)"
            class="filter-input flex-1"
          />
          <input
            v-model="localFilters.durationMax"
            @input="debouncedFilterChange"
            type="number"
            placeholder="Max (ms)"
            class="filter-input flex-1"
          />
        </div>
        <input
          v-else-if="localFilters.durationOperator"
          v-model="localFilters.durationMin"
          @input="debouncedFilterChange"
          type="number"
          :placeholder="localFilters.durationOperator === 'gt' ? 'Min duration (ms)' : 'Max duration (ms)'"
          class="filter-input"
        />
      </div>

      <!-- Actions -->
      <div class="filter-actions">
        <button
          @click="resetFilters"
          class="reset-button"
        >
          Reset Filters
        </button>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, reactive, watch, onMounted, computed } from 'vue'

// Props
const props = defineProps({
  modelValue: {
    type: Object,
    default: () => ({
      timeRange: '24h',
      severity: [],
      status: [],
      service: '',
      serviceNames: [],
      search: '',
      durationOperator: '',
      durationMin: null,
      durationMax: null,
      traceOutcome: [],
      attributeFilters: [],
      // Context filters (now arrays for multi-select)
      environments: [],
      versions: [],
      regions: [],
      hostNames: [],
      podNames: [],
      // Legacy single-value filters (for backwards compatibility)
      environment: '',
      version: '',
      region: '',
      hostName: '',
      podName: '',
    }),
  },
  showSeverityFilters: {
    type: Boolean,
    default: true,
  },
  showSearchFilter: {
    type: Boolean,
    default: true,
  },
  showDurationFilters: {
    type: Boolean,
    default: false,
  },
  showContextFilters: {
    type: Boolean,
    default: false,
  },
  /** Exception workflow status (resolved / unresolved / ignored). Not used on Traces or Logs pages. */
  showExceptionStatusFilters: {
    type: Boolean,
    default: true,
  },
  /** Per-trace ok vs error (from span status), for the Traces explorer only. */
  showTraceOutcomeFilters: {
    type: Boolean,
    default: false,
  },
  // Available filter values from the backend
  filterValues: {
    type: Object,
    default: () => ({
      environments: [],
      versions: [],
      regions: [],
      host_names: [],
      pod_names: [],
      service_names: [],
    }),
  },
  attributeKeys: {
    type: Array,
    default: () => [],
  },
  attributeValuesMap: {
    type: Object,
    default: () => ({}),
  },
})

// Emits
const emit = defineEmits(['update:modelValue', 'filter-change', 'close', 'load-attribute-values'])

// Local state
const localFilters = reactive({
  timeRange: '24h',
  severity: [],
  status: [],
  service: '',
  serviceNames: [],
  search: '',
  durationOperator: '',
  durationMin: null,
  durationMax: null,
  environments: [],
  versions: [],
  regions: [],
  hostNames: [],
  podNames: [],
  traceOutcome: [],
  attributeFilters: [],
  environment: '',
  version: '',
  region: '',
  hostName: '',
  podName: '',
  ...props.modelValue
})

// Options
const severityOptions = ['error', 'warning', 'info', 'debug']
const statusOptions = ['resolved', 'unresolved', 'ignored']

// Computed available values from props
const availableEnvironments = computed(() => props.filterValues?.environments || [])
const availableVersions = computed(() => props.filterValues?.versions || [])
const availableRegions = computed(() => props.filterValues?.regions || [])
const availableHostNames = computed(() => props.filterValues?.host_names || [])
const availablePodNames = computed(() => props.filterValues?.pod_names || [])
const availableServiceNames = computed(() => props.filterValues?.service_names || [])

const podNameSearch = ref('')
const hostNameSearch = ref('')
const filteredPodNames = computed(() => {
  if (!podNameSearch.value) return availablePodNames.value
  const q = podNameSearch.value.toLowerCase()
  return availablePodNames.value.filter(n => n.toLowerCase().includes(q))
})
const filteredHostNames = computed(() => {
  if (!hostNameSearch.value) return availableHostNames.value
  const q = hostNameSearch.value.toLowerCase()
  return availableHostNames.value.filter(n => n.toLowerCase().includes(q))
})

// Attribute filter state
const showingKeyPicker = ref(false)
const attrKeySearch = ref('')
const attrValueSearchMap = reactive({})

const activeAttrKeys = computed(() =>
  (localFilters.attributeFilters || []).map(f => f.key)
)
const availableAttrKeysToAdd = computed(() =>
  props.attributeKeys.filter(k => !activeAttrKeys.value.includes(k))
)
const filteredAttrKeysToAdd = computed(() => {
  const list = availableAttrKeysToAdd.value
  if (!attrKeySearch.value) return list.slice(0, 50)
  const q = attrKeySearch.value.toLowerCase()
  return list.filter(k => k.toLowerCase().includes(q)).slice(0, 50)
})
const filteredAttrValues = (key) => {
  const vals = props.attributeValuesMap[key] || []
  const q = (attrValueSearchMap[key] || '').toLowerCase()
  if (!q) return vals
  return vals.filter(v => v.toLowerCase().includes(q))
}

const addAttributeFilter = (key) => {
  if (!localFilters.attributeFilters) localFilters.attributeFilters = []
  localFilters.attributeFilters.push({ key, values: [] })
  showingKeyPicker.value = false
  attrKeySearch.value = ''
  emit('load-attribute-values', key)
}

const removeAttributeFilter = (idx) => {
  const removed = localFilters.attributeFilters.splice(idx, 1)
  if (removed.length) delete attrValueSearchMap[removed[0].key]
  handleFilterChange()
}

// Debounce timer
let debounceTimer = null

// Watch for external changes
watch(() => props.modelValue, (newValue) => {
  Object.assign(localFilters, newValue)
}, { deep: true })

// Methods
const handleFilterChange = () => {
  emit('update:modelValue', { ...localFilters })
  emit('filter-change')
}

const debouncedFilterChange = () => {
  if (debounceTimer) {
    clearTimeout(debounceTimer)
  }

  debounceTimer = setTimeout(() => {
    handleFilterChange()
  }, 300)
}

const resetFilters = () => {
  Object.assign(localFilters, {
    timeRange: '24h',
    severity: [],
    status: [],
    service: '',
    serviceNames: [],
    search: '',
    durationOperator: '',
    durationMin: null,
    durationMax: null,
    traceOutcome: [],
    attributeFilters: [],
    // Context filters
    environments: [],
    versions: [],
    regions: [],
    hostNames: [],
    podNames: [],
    environment: '',
    version: '',
    region: '',
    hostName: '',
    podName: '',
  })

  handleFilterChange()
}

// Initialize
onMounted(() => {
  // Ensure local filters match props
  Object.assign(localFilters, props.modelValue)
})
</script>

<style scoped>
.quick-filters-panel {
  @apply p-4 h-full;
}

.quick-filters-header {
  @apply border-b border-gray-200 pb-3;
}

.filter-group {
  @apply space-y-2;
}

.filter-label {
  @apply block text-sm font-medium text-gray-700;
}

.filter-select {
  @apply w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm bg-white text-gray-900 text-sm focus:ring-2 focus:ring-primary-500 focus:border-primary-500;
}

.filter-input {
  @apply w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm bg-white text-gray-900 text-sm placeholder-gray-500 focus:ring-2 focus:ring-primary-500 focus:border-primary-500;
}

.checkbox-group {
  @apply space-y-2;
}

.checkbox-group-scrollable {
  @apply space-y-1 max-h-32 overflow-y-auto pr-1;
  scrollbar-width: thin;
  scrollbar-color: rgba(155, 155, 155, 0.5) transparent;
}

.checkbox-group-scrollable::-webkit-scrollbar {
  width: 4px;
}

.checkbox-group-scrollable::-webkit-scrollbar-track {
  background: transparent;
}

.checkbox-group-scrollable::-webkit-scrollbar-thumb {
  background-color: rgba(155, 155, 155, 0.5);
  border-radius: 2px;
}

.checkbox-item {
  @apply flex items-center space-x-2 cursor-pointer;
}

.checkbox-input {
  @apply h-4 w-4 text-primary-600 bg-gray-100 border-gray-300 rounded focus:ring-primary-500 focus:ring-2 flex-shrink-0;
}

.checkbox-label {
  @apply text-sm text-gray-700;
}

.filter-actions {
  @apply pt-4 border-t border-gray-200;
}

.reset-button {
  @apply w-full px-3 py-2 text-sm font-medium text-gray-700 bg-gray-100 border border-gray-300 rounded-md hover:bg-gray-200 focus:ring-2 focus:ring-primary-500 focus:outline-none transition-colors;
}
</style>
