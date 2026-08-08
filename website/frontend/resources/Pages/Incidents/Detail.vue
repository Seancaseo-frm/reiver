<template>
  <AppLayout :user="user" :current-project="currentProject">
    <div class="incident-detail-page max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
      <div class="mb-6">
        <router-link
          :to="`/p/${projectId}/incidents`"
          class="text-primary-600 hover:text-primary-700 text-sm font-medium inline-flex items-center gap-2"
        >
          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 19l-7-7m0 0l7-7m-7 7h18" /></svg>
          Back to Incidents
        </router-link>
      </div>

      <!-- Loading State -->
      <div v-if="loading" class="bg-white rounded-lg shadow p-12 text-center">
        <div class="flex items-center justify-center">
          <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full"></div>
          <span class="ml-3 text-gray-600">Loading incident details...</span>
        </div>
      </div>

      <!-- Error State -->
      <div v-else-if="error" class="bg-white rounded-lg shadow p-12 text-center">
        <p class="text-danger-600">Error: {{ error }}</p>
        <router-link
          :to="`/p/${projectId}/incidents`"
          class="mt-4 inline-block text-primary-600 hover:text-primary-700 text-sm font-medium"
        >
          ← Back to Incidents
        </router-link>
      </div>

      <!-- Incident Content -->
      <div v-else-if="incident" class="space-y-6">
        <!-- Incident Summary -->
        <BaseCard class="p-6">
          <div class="flex items-start justify-between mb-4">
            <div class="flex-1">
              <div class="flex items-center space-x-3 mb-3">
                <span
                  :class="[
                    incident.type === 'exception' ? 'bg-danger-100 text-danger-800' : 'bg-warning-100 text-warning-800',
                    'inline-flex px-3 py-1 text-sm font-semibold rounded-full'
                  ]"
                >
                  {{ incident.type.toUpperCase() }}
                </span>
                <span
                  :class="[
                    incident.status === 'resolved' ? 'bg-success-100 text-success-800'
                    : incident.status === 'ignored' ? 'bg-gray-100 text-gray-800'
                    : 'bg-warning-100 text-warning-800',
                    'inline-flex px-3 py-1 text-sm font-semibold rounded-full'
                  ]"
                >
                  {{ (incident.status || 'unresolved').toUpperCase() }}
                </span>
              </div>
              <h1 class="text-2xl font-bold text-gray-900 mb-2">{{ incident.title }}</h1>
              <p v-if="incident.description" class="text-lg text-gray-600">
                {{ incident.description }}
              </p>
            </div>
          </div>

          <div class="grid grid-cols-1 md:grid-cols-4 gap-6 mt-6 pt-6 border-t border-gray-200">
            <div v-if="incident.occurrences !== undefined">
              <div class="text-sm font-medium text-gray-500">Occurrences</div>
              <div class="mt-1 text-2xl font-bold text-gray-900">{{ incident.occurrences || 0 }}</div>
            </div>
            <div v-if="incident.firstSeen">
              <div class="text-sm font-medium text-gray-500">First Seen</div>
              <div class="mt-1 text-sm text-gray-900">{{ formatDate(incident.firstSeen) }}</div>
            </div>
            <div v-if="incident.lastSeen">
              <div class="text-sm font-medium text-gray-500">Last Seen</div>
              <div class="mt-1 text-sm text-gray-900">{{ formatDate(incident.lastSeen) }}</div>
            </div>
            <div v-if="incident.serviceName">
              <div class="text-sm font-medium text-gray-500">Service</div>
              <div class="mt-1 text-sm text-gray-900">{{ incident.serviceName }}</div>
            </div>
          </div>
        </BaseCard>

        <!-- Root Cause Suggestions -->
        <BaseCard v-if="rootCause && rootCause.suggestions && rootCause.suggestions.length">
          <template #header><h2 class="text-lg font-semibold text-gray-900">Dominant Log Patterns</h2></template>
          <div class="space-y-3">
            <div v-for="s in rootCause.suggestions" :key="s.pattern" class="flex items-center justify-between py-2 px-3 bg-gray-50 rounded-lg">
              <div class="flex-1 min-w-0">
                <div class="font-mono text-sm text-gray-900 truncate" :title="s.pattern">{{ s.pattern }}</div>
                <div class="text-xs text-gray-500 mt-1">
                  {{ s.count.toLocaleString() }} occurrences
                </div>
              </div>
              <div class="text-right ml-4">
                <div class="text-sm font-medium text-gray-900">
                  {{ (s.pct * 100).toFixed(1) }}%
                </div>
                <div class="text-xs text-gray-500">
                  of {{ rootCause.total_logs.toLocaleString() }} logs
                </div>
              </div>
            </div>
          </div>
          <div v-if="rootCause.total_logs === 0" class="text-sm text-gray-500 py-4 text-center">
            No log data available for this time period.
          </div>
        </BaseCard>

        <!-- Timeline -->
        <BaseCard v-if="ctx?.timeline && ctx.timeline.length">
          <template #header><h2 class="text-lg font-semibold text-gray-900">Timeline</h2></template>
          <IncidentTimeline :events="ctx.timeline" :project-id="projectId" />
        </BaseCard>

        <!-- Logs Around (for exceptions) -->
        <BaseCard v-if="ctx?.logs_around && ((ctx.logs_around.structured && ctx.logs_around.structured.length) || (ctx.logs_around.unstructured && ctx.logs_around.unstructured.length))">
          <template #header><h2 class="text-lg font-semibold text-gray-900">Logs Around Incident (±2 min)</h2></template>
          <div v-if="ctx.logs_around.structured && ctx.logs_around.structured.length" class="mb-4">
            <div class="text-sm font-medium text-gray-600 mb-2">Structured</div>
            <div class="overflow-x-auto max-h-48 overflow-y-auto">
              <table class="min-w-full text-sm">
                <thead><tr class="border-b"><th class="text-left py-1 pr-2">Template</th><th class="text-left py-1 pr-2">Level</th><th class="text-left py-1">Count</th></tr></thead>
                <tbody>
                  <tr v-for="(l, i) in ctx.logs_around.structured.slice(0, 20)" :key="i" class="border-b border-gray-100">
                    <td class="py-1 pr-2 font-mono truncate max-w-md" :title="l.template">{{ l.template }}</td>
                    <td class="py-1 pr-2">{{ l.level }}</td>
                    <td class="py-1">{{ l.count }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
          <div v-if="ctx.logs_around.unstructured && ctx.logs_around.unstructured.length">
            <div class="text-sm font-medium text-gray-600 mb-2">Unstructured</div>
            <div class="space-y-1 max-h-40 overflow-y-auto text-sm">
              <div v-for="(l, i) in ctx.logs_around.unstructured.slice(0, 15)" :key="i">[{{ formatDate(l.timestamp) }}] {{ l.service_name }}: {{ l.message }}</div>
            </div>
          </div>
        </BaseCard>

        <!-- Logs in Window -->
        <BaseCard v-if="(ctx?.structured_logs && ctx.structured_logs.length) || (ctx?.unstructured_logs && ctx.unstructured_logs.length)">
          <template #header><h2 class="text-lg font-semibold text-gray-900">Logs in Incident Window</h2></template>
          <div v-if="ctx.structured_logs && ctx.structured_logs.length" class="mb-4">
            <div class="text-sm font-medium text-gray-600 mb-2">Structured</div>
            <div class="overflow-x-auto max-h-48 overflow-y-auto">
              <table class="min-w-full text-sm">
                <thead><tr class="border-b"><th class="text-left py-1 pr-2">Template</th><th class="text-left py-1 pr-2">Level</th><th class="text-left py-1">Count</th></tr></thead>
                <tbody>
                  <tr v-for="(l, i) in ctx.structured_logs.slice(0, 20)" :key="i" class="border-b border-gray-100">
                    <td class="py-1 pr-2 font-mono truncate max-w-md" :title="l.template">{{ l.template }}</td>
                    <td class="py-1 pr-2">{{ l.level }}</td>
                    <td class="py-1">{{ l.count }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
          <div v-if="ctx.unstructured_logs && ctx.unstructured_logs.length">
            <div class="text-sm font-medium text-gray-600 mb-2">Unstructured</div>
            <div class="space-y-1 max-h-40 overflow-y-auto text-sm">
              <div v-for="(l, i) in ctx.unstructured_logs.slice(0, 15)" :key="i">[{{ formatDate(l.timestamp) }}] {{ l.service_name }}: {{ l.message }}</div>
            </div>
          </div>
        </BaseCard>

        <!-- Traces -->
        <BaseCard v-if="ctx?.traces && ctx.traces.length">
          <template #header><h2 class="text-lg font-semibold text-gray-900">Related Traces</h2></template>
          <div class="space-y-2">
            <router-link
              v-for="t in ctx.traces"
              :key="t.trace_id"
              :to="`/p/${projectId}/traces/${t.trace_id}`"
              class="block py-2 px-2 -mx-2 rounded hover:bg-gray-50 text-sm"
            >
              <span class="font-mono">{{ t.trace_id }}</span>
              <span class="text-gray-500 ml-2">{{ t.span_count }} spans · {{ ((t.duration_ns || 0) / 1_000_000).toFixed(0) }}ms · {{ t.status }}</span>
            </router-link>
          </div>
        </BaseCard>

        <!-- Alerts -->
        <BaseCard v-if="ctx?.alerts && ctx.alerts.length">
          <template #header><h2 class="text-lg font-semibold text-gray-900">Fired Alerts</h2></template>
          <ul class="space-y-2">
            <li v-for="a in ctx.alerts" :key="a.id" class="text-sm py-1">
              <span class="font-medium">{{ a.rule_name }}</span>
              <span class="text-gray-500"> · {{ a.state }} · {{ formatDate(a.fired_at) }}</span>
            </li>
          </ul>
        </BaseCard>
      </div>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useAuth } from '@/composables/useAuth'
import AppLayout from '@/Layouts/AppLayout.vue'
import BaseCard from '@/components/BaseCard.vue'
import IncidentTimeline from '@/components/IncidentTimeline.vue'
import axios from 'axios'

const route = useRoute()
const router = useRouter()
const { user } = useAuth()

const projectId = computed(() => route.params.id)
const incidentType = computed(() => route.params.type) // 'exceptions' or 'errors'
const incidentId = computed(() => route.params.incident_id)

const currentProject = ref(null)
const incident = ref(null)
const ctx = ref(null)
const rootCause = ref(null)
const loading = ref(true)
const error = ref(null)

const fetchData = async () => {
  if (!projectId.value || !incidentType.value || !incidentId.value) {
    error.value = 'Invalid route parameters'
    loading.value = false
    return
  }

  try {
    loading.value = true

    let incidentData = null
    let timeWindow = null

    if (incidentType.value === 'exceptions') {
      // Fetch exception group
      const groupResponse = await axios.get(`/api/projects/${projectId.value}/exceptions/${incidentId.value}`)
      const group = groupResponse.data.group

      incidentData = {
        type: 'exception',
        title: group.message || 'Exception',
        description: group.exception_type ? `${group.exception_type}${group.exception_value ? ': ' + group.exception_value : ''}` : null,
        status: group.status,
        occurrences: group.count,
        firstSeen: group.first_seen,
        lastSeen: group.last_seen,
        serviceName: group.service_name,
        fingerprint: group.fingerprint
      }

      // Time window for context and root cause
      const pad = 15 * 60 * 1000
      timeWindow = {
        start_ms: new Date(group.first_seen).getTime() - pad,
        end_ms: new Date(group.last_seen).getTime() + pad,
        around_ms: new Date(group.last_seen).getTime(),
        service_name: group.service_name
      }
    } else if (incidentType.value === 'errors') {
      // For OTLP errors, we need to get the summary from the list or decode from incidentId
      // For now, assume incidentId is the otlp:xxx slug and we need to get details
      // This is a placeholder - we'd need to implement proper OTLP error detail fetching
      incidentData = {
        type: 'error',
        title: 'OTLP Error Pattern',
        description: 'Error-level logs from OTLP sources',
        status: 'active',
        firstSeen: new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(),
        lastSeen: new Date().toISOString()
      }

      timeWindow = {
        start_ms: Date.now() - 24 * 60 * 60 * 1000,
        end_ms: Date.now()
      }
    }

    incident.value = incidentData

    if (timeWindow) {
      // Fetch context and root cause
      const contextPromise = axios.get(`/api/projects/${projectId.value}/incidents/context`, { params: timeWindow })
      const rootCausePromise = axios.get(`/api/projects/${projectId.value}/root-cause-suggestions`, {
        params: { start_ms: timeWindow.start_ms, end_ms: timeWindow.end_ms }
      })

      const [contextResponse, rootCauseResponse] = await Promise.all([contextPromise, rootCausePromise])

      ctx.value = contextResponse.data
      rootCause.value = rootCauseResponse.data
    }

  } catch (e) {
    console.error('Failed to load incident:', e)
    error.value = e.response?.data?.message || e.message || 'Failed to load incident'
  } finally {
    loading.value = false
  }
}

const formatDate = (dateString) => {
  if (!dateString) return '—'
  try {
    return new Date(dateString).toLocaleString(undefined, { dateStyle: 'short', timeStyle: 'short' })
  } catch {
    return dateString
  }
}

onMounted(async () => {
  await fetchData()
})
</script>

<style scoped>
.spinner {
  animation: spin 0.6s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}
</style>
