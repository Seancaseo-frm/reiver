<template>
  <AppLayout :user="user" :current-project="currentProject">
    <div class="log-detail-page max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6 pb-72">
      <!-- Back Navigation -->
      <div class="mb-6">
        <router-link
          :to="`/p/${projectId}/logs`"
          class="text-primary-600 hover:text-primary-700 text-sm font-medium inline-flex items-center gap-2"
        >
          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 19l-7-7m0 0l7-7m-7 7h18" />
          </svg>
          Back to Logs
        </router-link>
      </div>

      <!-- Loading State -->
      <div v-if="loading" class="bg-white rounded-lg shadow p-12 text-center">
        <div class="flex items-center justify-center">
          <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full"></div>
          <span class="ml-3 text-gray-600">Loading log details...</span>
        </div>
      </div>

      <!-- Error State -->
      <div v-else-if="error" class="bg-white rounded-lg shadow p-12 text-center">
        <p class="text-danger-600">{{ error }}</p>
        <router-link
          :to="`/p/${projectId}/logs`"
          class="mt-4 inline-block text-primary-600 hover:text-primary-700 text-sm font-medium"
        >
          ← Back to Logs
        </router-link>
      </div>

      <!-- Log Content -->
      <div v-else-if="logDetail" class="space-y-6">
        <!-- Log Header -->
        <BaseCard class="p-6">
          <div class="flex items-start justify-between mb-4">
            <div class="flex-1">
              <div class="flex items-center space-x-3 mb-3">
                <span :class="getLevelBadgeClass(logDetail.level)">
                  {{ (logDetail.level || 'info').toUpperCase() }}
                </span>
                <span class="text-sm text-gray-500">
                  {{ formatDate(logDetail.timestamp) }}
                </span>
              </div>
              <h1 class="text-xl font-semibold text-gray-900 font-mono break-all">
                {{ logDetail.message || logDetail.body }}
              </h1>
            </div>
            <div class="flex items-center gap-2">
              <BaseButton size="sm" variant="secondary" @click="copyLogLink">
                {{ copied ? 'Copied!' : 'Copy Link' }}
              </BaseButton>
            </div>
          </div>

          <div class="grid grid-cols-1 md:grid-cols-3 gap-4 mt-4 pt-4 border-t border-gray-200">
            <div>
              <div class="text-sm font-medium text-gray-500">Service</div>
              <div class="mt-1 text-sm font-mono text-gray-900">
                {{ logDetail.service_name || 'unknown' }}
              </div>
            </div>
            <div v-if="logDetail.trace_id">
              <div class="text-sm font-medium text-gray-500">Trace ID</div>
              <router-link
                :to="`/p/${projectId}/traces/${logDetail.trace_id}`"
                class="mt-1 text-sm font-mono text-primary-600 hover:underline inline-flex items-center gap-1"
              >
                {{ logDetail.trace_id }}
                <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
                </svg>
              </router-link>
            </div>
            <div>
              <div class="text-sm font-medium text-gray-500">Source</div>
              <div class="mt-1 text-sm text-gray-900">
                {{ logDetail.source || 'direct' }}
              </div>
            </div>
          </div>
        </BaseCard>

        <!-- Tabs: Overview | JSON | Context -->
        <BaseCard>
          <div class="border-b border-gray-200">
            <nav class="flex -mb-px">
              <button
                v-for="tab in tabs"
                :key="tab.id"
                @click="activeTab = tab.id"
                :class="[
                  'px-6 py-3 text-sm font-medium border-b-2 transition-colors',
                  activeTab === tab.id
                    ? 'border-primary-500 text-primary-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700'
                ]"
              >
                {{ tab.label }}
              </button>
            </nav>
          </div>

          <!-- Overview Tab: Attributes Table -->
          <div v-if="activeTab === 'overview'" class="p-6">
            <table class="w-full text-sm">
              <tbody class="divide-y divide-gray-200">
                <tr v-for="(value, key) in logAttributes" :key="key">
                  <td class="py-2 pr-4 font-medium text-gray-600 w-1/3">{{ key }}</td>
                  <td class="py-2 font-mono text-gray-900 break-all">{{ formatValue(value) }}</td>
                </tr>
              </tbody>
            </table>
            <div v-if="Object.keys(logAttributes).length === 0" class="text-center py-8 text-gray-500">
              No additional attributes available.
            </div>
          </div>

          <!-- JSON Tab: Raw Log -->
          <div v-else-if="activeTab === 'json'" class="p-6">
            <div class="flex justify-end mb-2">
              <BaseButton size="sm" variant="ghost" @click="copyJson">
                {{ jsonCopied ? 'Copied!' : 'Copy JSON' }}
              </BaseButton>
            </div>
            <pre class="bg-gray-100 text-gray-800 border border-gray-200 p-4 rounded-lg text-xs font-mono overflow-x-auto max-h-96 overflow-y-auto">{{ JSON.stringify(logDetail, null, 2) }}</pre>
          </div>

          <!-- Context Tab: Surrounding Logs -->
          <div v-else-if="activeTab === 'context'" class="p-6">
            <div v-if="contextLoading" class="text-center py-8">
              <div class="spinner w-6 h-6 border-3 border-primary-600 border-t-transparent rounded-full mx-auto"></div>
              <p class="mt-2 text-sm text-gray-500">Loading surrounding logs...</p>
            </div>
            <div v-else-if="contextLogs.length === 0" class="text-center py-8 text-gray-500">
              No surrounding logs found within ±2 minutes.
            </div>
            <div v-else class="space-y-2 max-h-96 overflow-y-auto">
              <div
                v-for="log in contextLogs"
                :key="log.id"
                :class="[
                  'p-3 rounded border text-sm font-mono cursor-pointer transition-colors',
                  log.id === logId
                    ? 'border-primary-500 bg-primary-50'
                    : 'border-gray-200 hover:bg-gray-50'
                ]"
                @click="log.id !== logId && navigateToLog(log.id)"
              >
                <div class="flex items-center gap-2 mb-1">
                  <span :class="getLevelBadgeClass(log.level)" class="text-xs px-1.5 py-0.5">{{ log.level }}</span>
                  <span class="text-xs text-gray-500">{{ formatTime(log.timestamp) }}</span>
                  <span class="text-xs text-gray-400">{{ log.service_name }}</span>
                  <span v-if="log.id === logId" class="text-xs text-primary-500 font-semibold ml-auto">Current</span>
                </div>
                <div class="text-gray-900 break-all line-clamp-2">{{ log.message }}</div>
              </div>
            </div>
          </div>
        </BaseCard>
      </div>
    </div>

    <!-- Correlated Signals Panel -->
    <CorrelatedSignalsPanel
      v-if="logDetail?.trace_id"
      :trace-ids="[logDetail.trace_id]"
      :span-id="logDetail.span_id || ''"
      :service-name="logDetail.service_name || ''"
      :project-id="projectId"
      :timestamp="logDetail.timestamp || ''"
      :hide-tabs="['logs']"
    />
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { format } from 'date-fns'
import axios from 'axios'
import AppLayout from '@/Layouts/AppLayout.vue'
import BaseCard from '@/components/BaseCard.vue'
import BaseButton from '@/components/BaseButton.vue'
import CorrelatedSignalsPanel from '@/components/CorrelatedSignalsPanel.vue'
import { useAuth } from '@/composables/useAuth'

const route = useRoute()
const router = useRouter()
const { user, fetchUser } = useAuth()

const projectId = computed(() => route.params.id)
const logId = computed(() => route.params.log_id)
const currentProject = ref(null)
const logDetail = ref(null)
const loading = ref(true)
const error = ref(null)
const copied = ref(false)
const jsonCopied = ref(false)

// Tabs
const activeTab = ref('overview')
const tabs = [
  { id: 'overview', label: 'Overview' },
  { id: 'json', label: 'JSON' },
  { id: 'context', label: 'Context' },
]

// Context logs
const contextLogs = ref([])
const contextLoading = ref(false)

// Computed attributes for overview tab
const logAttributes = computed(() => {
  if (!logDetail.value) return {}

  const attrs = {}
  const excludeKeys = ['id', 'project_id']

  for (const [key, value] of Object.entries(logDetail.value)) {
    if (!excludeKeys.includes(key) && value !== null && value !== undefined && value !== '') {
      attrs[key] = value
    }
  }

  return attrs
})

// Methods
const fetchProject = async () => {
  try {
    const response = await axios.get(`/api/projects/${projectId.value}`)
    currentProject.value = response.data
  } catch (err) {
    console.error('Failed to fetch project:', err)
  }
}

const fetchLogDetail = async () => {
  loading.value = true
  error.value = null
  try {
    const params = {}
    const ts = route.query.timestamp
    if (ts) params.timestamp = ts
    const response = await axios.get(`/api/projects/${projectId.value}/logs/${logId.value}`, { params })
    logDetail.value = response.data
  } catch (err) {
    console.error('Failed to fetch log detail:', err)
    error.value = err.response?.data?.message || 'Failed to load log details'
  } finally {
    loading.value = false
  }
}

const fetchContextLogs = async () => {
  if (!logDetail.value) return
  contextLoading.value = true
  try {
    const params = {
      log_id: logId.value,
      time_range: '2m',
    }
    if (logDetail.value.trace_id) {
      params.trace_id = logDetail.value.trace_id
    }
    if (logDetail.value.timestamp) {
      params.timestamp = logDetail.value.timestamp
    }
    const response = await axios.get(`/api/projects/${projectId.value}/logs/context`, { params })
    contextLogs.value = response.data || []
  } catch (err) {
    console.error('Failed to fetch context logs:', err)
    contextLogs.value = []
  } finally {
    contextLoading.value = false
  }
}

const copyLogLink = async () => {
  const url = window.location.href
  await navigator.clipboard.writeText(url)
  copied.value = true
  setTimeout(() => { copied.value = false }, 2000)
}

const copyJson = async () => {
  await navigator.clipboard.writeText(JSON.stringify(logDetail.value, null, 2))
  jsonCopied.value = true
  setTimeout(() => { jsonCopied.value = false }, 2000)
}

const navigateToLog = (id) => {
  router.push(`/p/${projectId.value}/logs/${id}`)
}

const getLevelBadgeClass = (level) => {
  const baseClasses = 'inline-flex px-2 py-1 text-xs font-medium rounded'
  const levelLower = (level || 'info').toLowerCase()

  if (levelLower === 'error' || levelLower === 'fatal') {
    return `${baseClasses} bg-red-100 text-red-800`
  }
  if (levelLower === 'warning' || levelLower === 'warn') {
    return `${baseClasses} bg-yellow-100 text-yellow-800`
  }
  if (levelLower === 'info') {
    return `${baseClasses} bg-blue-100 text-blue-800`
  }
  if (levelLower === 'debug') {
    return `${baseClasses} bg-gray-100 text-gray-800`
  }
  return `${baseClasses} bg-gray-100 text-gray-800`
}

const formatDate = (dateString) => {
  if (!dateString) return 'Unknown'
  try {
    return format(new Date(dateString), 'MMM d, yyyy HH:mm:ss.SSS')
  } catch {
    return dateString
  }
}

const formatTime = (dateString) => {
  if (!dateString) return ''
  try {
    return format(new Date(dateString), 'HH:mm:ss.SSS')
  } catch {
    return dateString
  }
}

const formatValue = (value) => {
  if (typeof value === 'object') {
    return JSON.stringify(value)
  }
  return String(value)
}

// Watch for tab changes to load context
watch(activeTab, (newTab) => {
  if (newTab === 'context' && contextLogs.value.length === 0 && !contextLoading.value) {
    fetchContextLogs()
  }
})

// Watch for route changes (when navigating between logs)
watch(() => route.params.log_id, async (newLogId) => {
  if (newLogId) {
    await fetchLogDetail()
    // Reset context when navigating to a new log
    contextLogs.value = []
    if (activeTab.value === 'context') {
      await fetchContextLogs()
    }
  }
})

onMounted(async () => {
  await fetchUser()
  await Promise.all([fetchProject(), fetchLogDetail()])
})
</script>

<style scoped>
.spinner {
  animation: spin 0.6s linear infinite;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

.line-clamp-2 {
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  overflow: hidden;
}
</style>
