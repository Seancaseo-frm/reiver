<template>
  <div class="trace-list-widget">
    <div v-if="loading" class="flex items-center justify-center h-full">
      <div class="spinner w-6 h-6 border-2 border-primary-600 border-t-transparent rounded-full"></div>
    </div>
    <div v-else-if="error" class="text-center text-danger-400 text-sm p-4">
      {{ error }}
    </div>
    <div v-else-if="traces.length === 0" class="text-center text-gray-400 text-sm py-8">
      No traces
    </div>
    <div v-else class="trace-list">
      <div
        v-for="trace in traces"
        :key="trace.trace_id"
        class="trace-item p-3 border-b border-gray-700 hover:bg-gray-700/50 cursor-pointer transition-colors"
        @click="$router.push(`/p/${projectId}/traces/${trace.trace_id}`)"
      >
        <div class="flex items-start justify-between">
          <div class="flex-1 min-w-0">
            <div class="text-sm font-mono text-gray-300 truncate">{{ trace.trace_id }}</div>
            <div class="flex items-center gap-2 mt-1">
              <span
                :class="[
                  trace.status === 'error' ? 'bg-danger-900/30 text-danger-300' : 'bg-success-900/30 text-success-300',
                  'px-2 py-0.5 text-xs font-semibold rounded'
                ]"
              >
                {{ trace.status }}
              </span>
              <span class="text-xs text-gray-400">{{ ((trace.duration_ns || 0) / 1_000_000).toFixed(0) }}ms</span>
              <span class="text-xs text-gray-400">{{ trace.span_count }} spans</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, watch } from 'vue'
import { useRouter } from 'vue-router'
import axios from 'axios'

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
})

const router = useRouter()
const traces = ref([])
const loading = ref(true)
const error = ref(null)

const fetchTraces = async () => {
  loading.value = true
  error.value = null

  try {
    const limit = props.config.limit || 10
    const response = await axios.get(`/api/projects/${props.projectId}/traces`, {
      params: { limit },
    })
    traces.value = response.data || []
  } catch (err) {
    console.error('Failed to fetch traces:', err)
    error.value = err.response?.data?.message || err.message || 'Failed to load traces'
  } finally {
    loading.value = false
  }
}

onMounted(() => {
  fetchTraces()
})

watch(() => props.timeRange, () => {
  fetchTraces()
})
</script>

<style scoped>
.trace-list-widget {
  @apply h-full flex flex-col;
}

.trace-list {
  @apply flex-1 overflow-y-auto;
}

.trace-item {
  @apply transition-colors;
}

.spinner {
  animation: spin 0.6s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}
</style>


