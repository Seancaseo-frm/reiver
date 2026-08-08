<template>
  <AppLayout :user="user" :current-project="currentProject">
    <div class="incident-error-detail max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
      <div class="mb-6">
        <router-link
          :to="`/p/${projectId}/incidents`"
          class="text-primary-600 hover:text-primary-700 text-sm font-medium inline-flex items-center gap-2"
        >
          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 19l-7-7m0 0l7-7m-7 7h18" /></svg>
          Back to Incidents
        </router-link>
      </div>

      <div v-if="!startMs || !endMs" class="bg-white rounded-lg shadow p-8 text-center text-gray-500">
        Missing start_ms or end_ms. <router-link :to="`/p/${projectId}/incidents`" class="text-primary-600">Go back</router-link>.
      </div>

      <template v-else>
        <!-- Summary -->
        <BaseCard class="mb-6">
          <template #header><h2 class="text-lg font-semibold text-gray-900">Error (OTLP)</h2></template>
          <div class="space-y-2">
            <div><span class="text-gray-500">Template:</span> <span class="font-mono text-sm break-all">{{ template || '—' }}</span></div>
            <div><span class="text-gray-500">Level:</span> {{ level || '—' }} · <span class="text-gray-500">Service:</span> {{ service_name || '—' }} · <span class="text-gray-500">Source:</span> {{ source || '—' }}</div>
            <div><span class="text-gray-500">Count:</span> {{ count || 0 }}</div>
          </div>
        </BaseCard>

        <!-- Context: Timeline, Logs, Traces, Alerts -->
        <div v-if="ctxLoading" class="text-center py-8 text-gray-500">Loading context...</div>
        <template v-else-if="ctx">
          <BaseCard v-if="ctx.timeline && ctx.timeline.length" class="mb-6">
            <template #header><h2 class="text-lg font-semibold text-gray-900">Timeline</h2></template>
            <IncidentTimeline :events="ctx.timeline" :project-id="projectId" />
          </BaseCard>
          <!-- Structured logs -->
          <BaseCard v-if="ctx.structured_logs && ctx.structured_logs.length" class="mb-6">
            <template #header><h2 class="text-lg font-semibold text-gray-900">Logs (structured)</h2></template>
            <div class="overflow-x-auto max-h-64 overflow-y-auto">
              <table class="min-w-full text-sm">
                <thead><tr class="border-b"><th class="text-left py-2 pr-4">Template</th><th class="text-left py-2 pr-4">Level</th><th class="text-left py-2 pr-4">Service</th><th class="text-left py-2">Count</th></tr></thead>
                <tbody>
                  <tr v-for="(l, i) in ctx.structured_logs" :key="i" class="border-b border-gray-100">
                    <td class="py-2 pr-4 font-mono truncate max-w-md" :title="l.template">{{ l.template }}</td>
                    <td class="py-2 pr-4">{{ l.level }}</td>
                    <td class="py-2 pr-4">{{ l.service_name }}</td>
                    <td class="py-2">{{ l.count }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </BaseCard>

          <!-- Unstructured logs -->
          <BaseCard v-if="ctx.unstructured_logs && ctx.unstructured_logs.length" class="mb-6">
            <template #header><h2 class="text-lg font-semibold text-gray-900">Logs (unstructured)</h2></template>
            <div class="space-y-2 max-h-64 overflow-y-auto">
              <div v-for="(l, i) in ctx.unstructured_logs" :key="i" class="text-sm py-1 border-b border-gray-100 last:border-0">
                <span class="text-gray-500">{{ formatDate(l.timestamp) }}</span> [{{ l.level }}] {{ l.service_name }}: {{ l.message }}
              </div>
            </div>
          </BaseCard>

          <!-- Traces -->
          <BaseCard v-if="ctx.traces && ctx.traces.length" class="mb-6">
            <template #header><h2 class="text-lg font-semibold text-gray-900">Traces</h2></template>
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
          <BaseCard v-if="ctx.alerts && ctx.alerts.length">
            <template #header><h2 class="text-lg font-semibold text-gray-900">Fired alerts</h2></template>
            <ul class="space-y-2">
              <li v-for="a in ctx.alerts" :key="a.id" class="text-sm py-1">
                <span class="font-medium">{{ a.rule_name }}</span>
                <span class="text-gray-500"> · {{ a.state }} · {{ formatDate(a.fired_at) }}</span>
              </li>
            </ul>
          </BaseCard>

          <!-- Root Cause Suggestions -->
          <BaseCard v-if="rootCauseLoading">
            <div class="py-4 text-center text-gray-500">Loading root cause suggestions...</div>
          </BaseCard>
          <BaseCard v-else-if="rootCause && rootCause.suggestions && rootCause.suggestions.length" class="mb-6">
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

          <div v-if="!hasAnyContext" class="text-center py-8 text-gray-500">No logs, traces, or alerts in this time range.</div>
        </template>
      </template>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import { useRoute } from 'vue-router'
import { useAuth } from '@/composables/useAuth'
import AppLayout from '@/Layouts/AppLayout.vue'
import BaseCard from '@/components/BaseCard.vue'
import IncidentTimeline from '@/components/IncidentTimeline.vue'
import axios from 'axios'

const route = useRoute()
const { user } = useAuth()

const projectId = computed(() => route.params.id)
const currentProject = ref(null)

const startMs = computed(() => route.query.start_ms ? parseInt(route.query.start_ms, 10) : null)
const endMs = computed(() => route.query.end_ms ? parseInt(route.query.end_ms, 10) : null)
const template = computed(() => route.query.template || '')
const level = computed(() => route.query.level || '')
const service_name = computed(() => route.query.service_name || '')
const source = computed(() => route.query.source || '')
const count = computed(() => route.query.count || '0')

const ctx = ref(null)
const ctxLoading = ref(false)
const rootCause = ref(null)
const rootCauseLoading = ref(false)

const hasAnyContext = computed(() => {
  if (!ctx.value) return false
  const c = ctx.value
  return (c.timeline && c.timeline.length) || (c.structured_logs && c.structured_logs.length) || (c.unstructured_logs && c.unstructured_logs.length) || (c.traces && c.traces.length) || (c.alerts && c.alerts.length)
})

function formatDate(v) {
  if (!v) return '—'
  return new Date(v).toLocaleString(undefined, { dateStyle: 'short', timeStyle: 'short' })
}

async function loadContext() {
  if (!startMs.value || !endMs.value) return
  ctxLoading.value = true
  rootCauseLoading.value = true
  try {
    const contextPromise = axios.get(`/api/projects/${projectId.value}/incidents/context`, {
      params: { start_ms: startMs.value, end_ms: endMs.value }
    })
    const rootCausePromise = axios.get(`/api/projects/${projectId.value}/root-cause-suggestions`, {
      params: { start_ms: startMs.value, end_ms: endMs.value }
    })

    const [contextResponse, rootCauseResponse] = await Promise.all([contextPromise, rootCausePromise])

    ctx.value = contextResponse.data
    rootCause.value = rootCauseResponse.data
  } catch (e) {
    console.warn('Incident context/root cause:', e)
    ctx.value = null
    rootCause.value = null
  } finally {
    ctxLoading.value = false
    rootCauseLoading.value = false
  }
}

onMounted(() => {
  currentProject.value = route.meta?.currentProject ?? null
  loadContext()
})
watch([startMs, endMs], loadContext)
</script>
