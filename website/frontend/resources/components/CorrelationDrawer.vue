<template>
  <div class="correlation-drawer fixed inset-0 z-50 flex justify-end" @click.self="emit('close')">
    <!-- Backdrop -->
    <div class="absolute inset-0 bg-black/30" @click="emit('close')"></div>

    <!-- Panel -->
    <div class="relative w-full max-w-lg bg-white shadow-xl flex flex-col overflow-hidden">
      <!-- Header -->
      <div class="flex items-center justify-between px-5 py-4 border-b border-gray-200 bg-gray-50">
        <div>
          <h2 class="text-base font-semibold text-gray-900">Cross-Stack Correlation</h2>
          <p class="text-xs text-gray-500 mt-0.5">
            {{ formatTime(startMs) }} — {{ formatTime(endMs) }}
            <span class="text-gray-400">({{ durationLabel }})</span>
          </p>
        </div>
        <button @click="emit('close')" class="text-gray-400 hover:text-gray-600 transition-colors">
          <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      </div>

      <!-- Tabs -->
      <div class="flex border-b border-gray-200">
        <button
          @click="activeTab = 'traces'"
          class="flex-1 px-4 py-2.5 text-sm font-medium transition-colors"
          :class="activeTab === 'traces' ? 'text-primary-600 border-b-2 border-primary-600' : 'text-gray-500 hover:text-gray-700'"
        >
          Traces
          <span v-if="contextData?.traces?.length" class="ml-1 text-xs bg-gray-100 text-gray-600 px-1.5 py-0.5 rounded-full">
            {{ contextData.traces.length }}
          </span>
        </button>
        <button
          @click="activeTab = 'logs'"
          class="flex-1 px-4 py-2.5 text-sm font-medium transition-colors"
          :class="activeTab === 'logs' ? 'text-primary-600 border-b-2 border-primary-600' : 'text-gray-500 hover:text-gray-700'"
        >
          Logs
          <span v-if="contextData?.logs?.length" class="ml-1 text-xs bg-gray-100 text-gray-600 px-1.5 py-0.5 rounded-full">
            {{ contextData.logs.length }}
          </span>
        </button>
      </div>

      <!-- Content -->
      <div class="flex-1 overflow-y-auto">
        <!-- Loading -->
        <div v-if="loading" class="flex items-center justify-center py-12">
          <div class="spinner w-6 h-6 border-2 border-primary-500 border-t-transparent rounded-full animate-spin"></div>
        </div>

        <!-- Error -->
        <div v-else-if="error" class="p-5 text-center">
          <p class="text-sm text-red-500">{{ error }}</p>
        </div>

        <!-- Traces Tab -->
        <div v-else-if="activeTab === 'traces'" class="divide-y divide-gray-100">
          <div v-if="!contextData?.traces?.length" class="p-5 text-center text-gray-400 text-sm">
            No traces found in this time window.
          </div>
          <div v-else>
            <div v-for="(group, svc) in tracesByService" :key="svc" class="border-b border-gray-100 last:border-0">
              <button
                @click="toggleSection(svc)"
                class="w-full flex items-center justify-between px-5 py-3 hover:bg-gray-50 transition-colors"
              >
                <span class="text-sm font-medium text-gray-700">{{ svc }}</span>
                <span class="text-xs text-gray-400">{{ group.length }} traces</span>
              </button>
              <div v-show="expandedSections.has(svc)" class="bg-gray-50 px-5 pb-3 space-y-2">
                <a
                  v-for="trace in group"
                  :key="trace.trace_id"
                  :href="`/p/${projectId}/traces/${trace.trace_id}`"
                  class="block rounded-md border border-gray-200 bg-white p-3 hover:border-primary-300 hover:shadow-sm transition-all"
                >
                  <div class="flex items-center justify-between mb-1">
                    <span class="text-sm font-medium text-gray-900 truncate">{{ trace.operation }}</span>
                    <span
                      class="text-xs font-mono px-1.5 py-0.5 rounded"
                      :class="trace.status === 'ERROR' ? 'bg-red-100 text-red-700' : 'bg-yellow-100 text-yellow-700'"
                    >
                      {{ trace.duration_ms.toFixed(0) }}ms
                    </span>
                  </div>
                  <div class="flex items-center gap-2 text-xs text-gray-400">
                    <span>{{ trace.timestamp }}</span>
                    <span v-if="trace.status === 'ERROR'" class="text-red-500 font-medium">ERROR</span>
                    <span v-else class="text-yellow-600">slow</span>
                  </div>
                </a>
              </div>
            </div>
          </div>
        </div>

        <!-- Logs Tab -->
        <div v-else-if="activeTab === 'logs'" class="divide-y divide-gray-100">
          <div v-if="!contextData?.logs?.length" class="p-5 text-center text-gray-400 text-sm">
            No error/warn logs found in this time window.
          </div>
          <div v-else>
            <div v-for="(group, svc) in logsByService" :key="svc" class="border-b border-gray-100 last:border-0">
              <button
                @click="toggleSection('log_' + svc)"
                class="w-full flex items-center justify-between px-5 py-3 hover:bg-gray-50 transition-colors"
              >
                <span class="text-sm font-medium text-gray-700">{{ svc }}</span>
                <span class="text-xs text-gray-400">{{ group.length }} entries</span>
              </button>
              <div v-show="expandedSections.has('log_' + svc)" class="bg-gray-50 px-5 pb-3 space-y-2">
                <div
                  v-for="(log, idx) in group"
                  :key="idx"
                  class="rounded-md border border-gray-200 bg-white p-3"
                >
                  <div class="flex items-center gap-2 mb-1">
                    <span
                      class="text-xs font-medium px-1.5 py-0.5 rounded"
                      :class="log.level.toLowerCase().includes('error') ? 'bg-red-100 text-red-700' : 'bg-yellow-100 text-yellow-700'"
                    >
                      {{ log.level }}
                    </span>
                    <span class="text-xs text-gray-400">{{ log.timestamp }}</span>
                  </div>
                  <p class="text-sm text-gray-700 font-mono break-all">{{ log.message }}</p>
                  <a
                    v-if="log.trace_id"
                    :href="`/p/${projectId}/traces/${log.trace_id}`"
                    class="inline-block mt-1 text-xs text-primary-600 hover:text-primary-700"
                  >
                    View trace &rarr;
                  </a>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, reactive } from 'vue'
import axios from 'axios'

const props = defineProps({
  projectId: { type: String, required: true },
  startMs: { type: Number, required: true },
  endMs: { type: Number, required: true },
})

const emit = defineEmits(['close'])

const loading = ref(true)
const error = ref(null)
const contextData = ref(null)
const activeTab = ref('traces')
const expandedSections = reactive(new Set())

const durationLabel = computed(() => {
  const diffMs = props.endMs - props.startMs
  if (diffMs < 60000) return `${Math.round(diffMs / 1000)}s`
  if (diffMs < 3600000) return `${Math.round(diffMs / 60000)}m`
  return `${(diffMs / 3600000).toFixed(1)}h`
})

const tracesByService = computed(() => {
  if (!contextData.value?.traces) return {}
  const grouped = {}
  for (const trace of contextData.value.traces) {
    const svc = trace.service || 'unknown'
    if (!grouped[svc]) grouped[svc] = []
    grouped[svc].push(trace)
  }
  return grouped
})

const logsByService = computed(() => {
  if (!contextData.value?.logs) return {}
  const grouped = {}
  for (const log of contextData.value.logs) {
    const svc = log.service || 'unknown'
    if (!grouped[svc]) grouped[svc] = []
    grouped[svc].push(log)
  }
  return grouped
})

function toggleSection(key) {
  if (expandedSections.has(key)) {
    expandedSections.delete(key)
  } else {
    expandedSections.add(key)
  }
}

function formatTime(ms) {
  return new Date(ms).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
}

async function fetchContext() {
  loading.value = true
  error.value = null
  try {
    const response = await axios.post(`/api/system-overview/${props.projectId}/context`, {
      start_ms: props.startMs,
      end_ms: props.endMs,
    })
    contextData.value = response.data

    // Auto-expand first sections
    const firstTrace = Object.keys(tracesByService.value)[0]
    const firstLog = Object.keys(logsByService.value)[0]
    if (firstTrace) expandedSections.add(firstTrace)
    if (firstLog) expandedSections.add('log_' + firstLog)
  } catch (err) {
    error.value = err.response?.data?.error || err.message || 'Failed to load context'
  } finally {
    loading.value = false
  }
}

onMounted(() => {
  fetchContext()
})
</script>
