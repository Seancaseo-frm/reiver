<template>
  <AppLayout :user="user" :current-project="currentProject">
    <div class="trace-detail-page max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8 pb-72">
      <!-- Header -->
      <div class="mb-6">
        <router-link
          :to="`/p/${projectId}/traces`"
          class="text-primary-600 hover:text-primary-700 text-sm font-medium mb-2 inline-block"
        >
          ← Back to Traces
        </router-link>
        <div class="flex items-center justify-between">
          <div>
            <h1 class="text-2xl font-bold text-gray-900">Trace Details</h1>
            <p class="text-sm text-gray-500 mt-1 font-mono">
              {{ traceId }}
            </p>
          </div>
          <div class="flex items-center gap-3">
            <!-- View Mode Toggle -->
            <div class="flex items-center bg-gray-100 rounded-lg p-1">
              <button
                @click="viewMode = 'waterfall'"
                :class="['view-toggle-btn', viewMode === 'waterfall' ? 'active' : '']"
              >
                <svg class="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 12h16M4 18h16" />
                </svg>
                Waterfall
              </button>
              <button
                @click="viewMode = 'flamegraph'"
                :class="['view-toggle-btn', viewMode === 'flamegraph' ? 'active' : '']"
              >
                <svg class="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17.657 18.657A8 8 0 016.343 7.343S7 9 9 10c0-2 .5-5 2.986-7C14 5 16.09 5.777 17.656 7.343A7.975 7.975 0 0120 13a7.975 7.975 0 01-2.343 5.657z" />
                </svg>
                Flamegraph
              </button>
            </div>

            <span
              :class="[
                'px-3 py-1 text-sm font-medium rounded-full',
                getStatusBadgeClass(traceDetail?.trace?.status),
              ]"
            >
              {{ traceDetail?.trace?.status?.toUpperCase() || 'OK' }}
            </span>
            <span v-if="traceDetail?.exceptions?.length" class="px-2 py-1 text-xs font-medium bg-red-100 text-red-800 rounded-full">
              {{ traceDetail.exceptions.length }} exception{{ traceDetail.exceptions.length > 1 ? 's' : '' }}
            </span>
          </div>
        </div>
      </div>

      <!-- Trace Metadata -->
      <div v-if="traceDetail" class="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
        <BaseCard>
          <div class="text-sm font-medium text-gray-500">Duration</div>
          <div class="mt-1 text-2xl font-bold text-gray-900">
            {{ formatDuration((traceDetail.trace.duration_ns || 0) / 1_000_000) }}
          </div>
        </BaseCard>
        <BaseCard>
          <div class="text-sm font-medium text-gray-500">Spans</div>
          <div class="mt-1 text-2xl font-bold text-gray-900">
            {{ traceDetail.trace.span_count }}
          </div>
        </BaseCard>
        <BaseCard>
          <div class="text-sm font-medium text-gray-500">Services</div>
          <div class="mt-1 text-2xl font-bold text-gray-900">
            {{ traceDetail.trace.service_count }}
          </div>
        </BaseCard>
        <BaseCard>
          <div class="text-sm font-medium text-gray-500">Start Time</div>
          <div class="mt-1 text-lg font-semibold text-gray-900">
            {{ formatDate(traceDetail.trace.start_time) }}
          </div>
        </BaseCard>
      </div>

      <!-- Waterfall View -->
      <div v-if="loading" class="flex items-center justify-center py-12">
        <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full"></div>
        <span class="ml-3 text-gray-600">Loading trace...</span>
      </div>

      <div v-else-if="traceDetail" class="space-y-4">
        <!-- Waterfall View -->
        <BaseCard v-if="viewMode === 'waterfall'">
          <template #header>
            <div class="flex items-center justify-between">
              <h2 class="text-lg font-semibold text-gray-900">Span Timeline</h2>
              <span class="text-sm text-gray-500">Click a span to view details</span>
            </div>
          </template>
          <TraceWaterfall 
            :trace="traceDetail" 
            :exceptions="traceDetail.exceptions || []"
            :selected-span-id="selectedSpan?.span_id"
            @select-span="handleSpanSelect"
          />
        </BaseCard>

        <!-- Flamegraph View -->
        <BaseCard v-if="viewMode === 'flamegraph'">
          <template #header>
            <div class="flex items-center justify-between">
              <h2 class="text-lg font-semibold text-gray-900">Flamegraph</h2>
              <span class="text-sm text-gray-500">Click a span to view details</span>
            </div>
          </template>
          <TraceFlamegraph
            :trace="traceDetail"
            :selected-span-id="selectedSpan?.span_id"
            @select-span="handleSpanSelect"
          />
        </BaseCard>

        <!-- Spans Table -->
        <BaseCard>
          <template #header>
            <div class="flex items-center justify-between">
              <h2 class="text-lg font-semibold text-gray-900">All Spans</h2>
              <div class="flex items-center gap-2">
                <input
                  v-model="spanSearch"
                  type="text"
                  placeholder="Search spans..."
                  class="px-3 py-1.5 text-sm bg-gray-50 border border-gray-200 rounded-md focus:ring-2 focus:ring-primary-500"
                />
              </div>
            </div>
          </template>
          <SpansTable 
            :spans="filteredSpans" 
            :exceptions="traceDetail.exceptions || []"
            :selected-span-id="selectedSpan?.span_id"
            @select-span="handleSpanSelect"
          />
        </BaseCard>

        <!-- Exceptions -->
        <BaseCard v-if="traceDetail?.exceptions?.length">
          <template #header>
            <h2 class="text-lg font-semibold text-gray-900">Exceptions</h2>
          </template>
          <div class="space-y-3">
            <div
              v-for="exception in traceDetail.exceptions"
              :key="exception.id"
              class="block p-3 border border-gray-200 rounded-lg bg-gray-50"
            >
              <div class="flex items-start justify-between">
                <div class="flex-1">
                  <div class="font-medium text-gray-900 truncate" :title="exception.message">
                    {{ exception.message || 'Exception' }}
                  </div>
                  <div class="text-sm text-gray-500 mt-1">
                    <span class="capitalize">{{ exception.level }}</span>
                    <span v-if="exception.exception_type"> · {{ exception.exception_type }}</span>
                    <span v-if="exception.service_name"> · {{ exception.service_name }}</span>
                    <span v-if="exception.span_id" class="inline-flex items-center gap-1 ml-2 px-2 py-0.5 bg-blue-100 text-blue-800 rounded text-xs font-mono">
                      <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z" />
                      </svg>
                      Span: {{ exception.span_id.slice(0, 8) }}
                    </span>
                  </div>
                  <div class="text-xs text-gray-400 mt-1">
                    {{ formatDate(exception.timestamp) }}
                  </div>
                </div>
                <div class="text-right ml-4">
                  <div class="text-xs text-gray-500">
                    Instance #{{ exception.id.slice(0, 8) }}
                  </div>
                </div>
              </div>
            </div>
            <div class="text-sm text-gray-600 mt-3">
              These exceptions occurred within this trace. Check the exception tracking page for detailed group information.
            </div>
          </div>
        </BaseCard>

        <!-- LLM Operations -->
        <BaseCard v-if="llmSpans.length > 0">
          <template #header>
            <div class="flex items-center gap-2">
              <svg class="w-5 h-5 text-purple-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
              </svg>
              <h2 class="text-lg font-semibold text-gray-900">LLM Operations</h2>
            </div>
          </template>
          <div class="space-y-4">
            <div
              v-for="span in llmSpans"
              :key="span.span_id"
              class="border border-gray-200 rounded-lg overflow-hidden"
            >
              <!-- LLM Span Header -->
              <div class="bg-gray-50 p-4 border-b border-gray-200">
                <div class="flex items-center justify-between">
                  <div>
                    <span class="text-sm font-medium text-gray-900">{{ span.name }}</span>
                    <span class="ml-2 px-2 py-0.5 text-xs font-medium rounded bg-purple-100 text-purple-800">
                      {{ getLlmModel(span) }}
                    </span>
                  </div>
                  <div class="flex items-center gap-4 text-sm text-gray-500">
                    <span v-if="getLlmTokens(span)">{{ getLlmTokens(span) }} tokens</span>
                    <span v-if="getLlmCost(span)">${{ getLlmCost(span) }}</span>
                    <span>{{ formatDuration(span.duration_ns / 1_000_000) }}</span>
                  </div>
                </div>
              </div>
              
              <!-- LLM Details -->
              <div class="p-4 space-y-4">
                <!-- Prompt Preview -->
                <div v-if="getLlmPrompt(span)">
                  <p class="text-xs font-medium text-gray-500 uppercase mb-2">Prompt</p>
                  <div class="bg-gray-50 rounded-lg p-3 max-h-32 overflow-y-auto">
                    <pre class="text-sm text-gray-700 whitespace-pre-wrap">{{ getLlmPrompt(span) }}</pre>
                  </div>
                </div>
                
                <!-- Thinking Content (if available) -->
                <div v-if="getLlmThinking(span)">
                  <p class="text-xs font-medium text-gray-500 uppercase mb-2 flex items-center gap-1">
                    <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
                    </svg>
                    AI Thinking
                  </p>
                  <div class="bg-yellow-50 border border-yellow-200 rounded-lg p-3 max-h-32 overflow-y-auto">
                    <pre class="text-sm text-yellow-900 whitespace-pre-wrap">{{ getLlmThinking(span) }}</pre>
                  </div>
                </div>
                
                <!-- Response Preview -->
                <div v-if="getLlmResponse(span)">
                  <p class="text-xs font-medium text-gray-500 uppercase mb-2">Response</p>
                  <div class="bg-gray-50 rounded-lg p-3 max-h-32 overflow-y-auto">
                    <pre class="text-sm text-gray-700 whitespace-pre-wrap">{{ getLlmResponse(span) }}</pre>
                  </div>
                </div>
                
                <!-- Links -->
                <div class="flex gap-3 pt-2">
                  <router-link
                    v-if="getLlmSessionId(span)"
                    :to="`/p/${projectId}/llm/sessions/${getLlmSessionId(span)}`"
                    class="text-sm text-primary-600 hover:text-primary-700 font-medium"
                  >
                    View Full Session →
                  </router-link>
                  <router-link
                    v-if="getLlmPromptId(span)"
                    :to="`/p/${projectId}/llm/prompts/${getLlmPromptId(span)}`"
                    class="text-sm text-primary-600 hover:text-primary-700 font-medium"
                  >
                    View Prompt Template →
                  </router-link>
                </div>
              </div>
            </div>
          </div>
        </BaseCard>

        <!-- Profile Panel -->
        <BaseCard v-if="traceProfile !== undefined">
          <template #header>
            <div class="flex items-center gap-2">
              <svg class="w-5 h-5 text-orange-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17.657 18.657A8 8 0 016.343 7.343S7 9 9 10c0-2 .5-5 2.986-7C14 5 16.09 5.777 17.656 7.343A7.975 7.975 0 0120 13a7.975 7.975 0 01-2.343 5.657z" />
              </svg>
              <h2 class="text-lg font-semibold text-gray-900">Profile</h2>
            </div>
          </template>
          <div v-if="traceProfileLoading" class="text-center py-8">
            <div class="inline-block animate-spin rounded-full h-6 w-6 border-b-2 border-primary-500"></div>
            <p class="mt-2 text-sm text-gray-500">Loading profile...</p>
          </div>
          <div v-else-if="traceProfile && traceProfile.profile">
            <div class="flex items-center justify-between mb-3 px-1">
              <div class="text-sm text-gray-600">
                <span class="font-medium">{{ traceProfile.profile.service_name }}</span>
                &middot; {{ traceProfile.profile.sample_count?.toLocaleString() }} samples
              </div>
              <router-link
                :to="`/p/${projectId}/profiles/${traceProfile.profile.profile_id}`"
                class="text-sm text-primary-600 hover:text-primary-700 font-medium"
              >
                View Full Profile →
              </router-link>
            </div>
            <div class="border border-gray-200 rounded-lg overflow-hidden">
              <ProfileFlamegraph v-if="traceProfile.flame_graph" :flameGraph="traceProfile.flame_graph" />
              <div v-else class="text-center py-6 text-gray-500 text-sm">
                Flamegraph not available for this profile.
              </div>
            </div>
          </div>
          <div v-else class="text-center py-6 text-gray-400 text-sm">
            No profile linked to this trace.
          </div>
        </BaseCard>
      </div>

      <div v-else-if="!loading" class="text-center py-12">
        <svg class="mx-auto h-12 w-12 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
        </svg>
        <h3 class="mt-2 text-sm font-medium text-gray-900">Trace not found</h3>
        <p class="mt-1 text-sm text-gray-500">The trace you're looking for doesn't exist.</p>
      </div>
    </div>

    <!-- Span Details Drawer -->
    <SpanDetailsDrawer
      :is-open="showSpanDrawer"
      :span="selectedSpan"
      :all-spans="traceDetail?.spans || []"
      :exceptions="traceDetail?.exceptions || []"
      :logs="relatedLogs"
      @close="closeSpanDrawer"
      @select-span="handleSpanSelect"
    />

    <!-- Correlated Signals Panel -->
    <CorrelatedSignalsPanel
      v-if="traceDetail"
      :trace-ids="[traceId]"
      :service-name="traceDetail?.trace?.root_service_name || ''"
      :project-id="projectId"
      :timestamp="traceDetail?.trace?.start_time || ''"
      :hide-tabs="['traces']"
    />
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useAuth } from '@/composables/useAuth'
import { formatDistanceToNow } from 'date-fns'
import AppLayout from '@/Layouts/AppLayout.vue'
import BaseCard from '@/components/BaseCard.vue'
import TraceWaterfall from '@/components/TraceWaterfall.vue'
import TraceFlamegraph from '@/components/TraceFlamegraph.vue'
import SpansTable from '@/components/SpansTable.vue'
import SpanDetailsDrawer from '@/components/SpanDetailsDrawer.vue'
import CorrelatedSignalsPanel from '@/components/CorrelatedSignalsPanel.vue'
import ProfileFlamegraph from '@/components/ProfileFlamegraph.vue'
import axios from 'axios'

const route = useRoute()
const router = useRouter()
const { user } = useAuth()

const projectId = computed(() => route.params.id)
const traceId = computed(() => route.params.trace_id)
const currentProject = ref(null)
const traceDetail = ref(null)
const loading = ref(false)

// View state
const viewMode = ref('waterfall')
const selectedSpan = ref(null)
const showSpanDrawer = ref(false)
const spanSearch = ref('')
const relatedLogs = ref([])
const traceProfile = ref(undefined)
const traceProfileLoading = ref(false)

// Filtered spans based on search
const filteredSpans = computed(() => {
  if (!traceDetail.value?.spans) return []
  if (!spanSearch.value) return traceDetail.value.spans
  
  const query = spanSearch.value.toLowerCase()
  return traceDetail.value.spans.filter(span => 
    span.span_name?.toLowerCase().includes(query) ||
    span.service_name?.toLowerCase().includes(query) ||
    span.span_id?.toLowerCase().includes(query)
  )
})

// Handle span selection
const handleSpanSelect = (span) => {
  selectedSpan.value = span
  showSpanDrawer.value = true
  fetchSpanLogs(span.span_id)
}

// Close span drawer
const closeSpanDrawer = () => {
  showSpanDrawer.value = false
}

// Fetch logs related to a span
const fetchSpanLogs = async (spanId) => {
  try {
    const response = await axios.get(`/api/projects/${projectId.value}/logs`, {
      params: { span_id: spanId, limit: 100 }
    })
    relatedLogs.value = response.data.logs || []
  } catch (error) {
    console.error('Failed to fetch span logs:', error)
    relatedLogs.value = []
  }
}

// Detect LLM spans - spans with LLM-related attributes
const llmSpans = computed(() => {
  if (!traceDetail.value?.spans) return []
  return traceDetail.value.spans.filter(span => {
    const name = span.name?.toLowerCase() || ''
    const attrs = span.attributes || {}
    // Check for LLM-related span names or attributes
    return (
      name.includes('chat') ||
      name.includes('completion') ||
      name.includes('embedding') ||
      name.includes('llm') ||
      attrs['gen_ai.request.model'] ||
      attrs['llm.model'] ||
      attrs['gen_ai.system']
    )
  })
})

// Helper functions for extracting LLM attributes from spans
const getLlmModel = (span) => {
  const attrs = span.attributes || {}
  return attrs['gen_ai.request.model'] || attrs['llm.model'] || attrs['model'] || 'Unknown'
}

const getLlmTokens = (span) => {
  const attrs = span.attributes || {}
  const input = parseInt(attrs['gen_ai.usage.prompt_tokens'] || attrs['llm.input_tokens'] || 0)
  const output = parseInt(attrs['gen_ai.usage.completion_tokens'] || attrs['llm.output_tokens'] || 0)
  if (input || output) {
    return `${input} in / ${output} out`
  }
  return null
}

const getLlmCost = (span) => {
  const attrs = span.attributes || {}
  const cost = parseFloat(attrs['gen_ai.usage.cost'] || attrs['llm.cost'] || 0)
  return cost > 0 ? cost.toFixed(4) : null
}

const getLlmPrompt = (span) => {
  const attrs = span.attributes || {}
  const prompt = attrs['gen_ai.prompt'] || attrs['llm.prompt'] || attrs['input']
  if (prompt && prompt.length > 500) {
    return prompt.slice(0, 500) + '...'
  }
  return prompt || null
}

const getLlmResponse = (span) => {
  const attrs = span.attributes || {}
  const response = attrs['gen_ai.completion'] || attrs['llm.response'] || attrs['output']
  if (response && response.length > 500) {
    return response.slice(0, 500) + '...'
  }
  return response || null
}

const getLlmThinking = (span) => {
  const attrs = span.attributes || {}
  const thinking = attrs['gen_ai.thinking'] || attrs['llm.thinking'] || attrs['thinking_content']
  if (thinking && thinking.length > 500) {
    return thinking.slice(0, 500) + '...'
  }
  return thinking || null
}

const getLlmSessionId = (span) => {
  const attrs = span.attributes || {}
  return attrs['gen_ai.session_id'] || attrs['llm.session_id'] || null
}

const getLlmPromptId = (span) => {
  const attrs = span.attributes || {}
  return attrs['gen_ai.prompt_id'] || attrs['llm.prompt_id'] || null
}

const fetchProject = async () => {
  try {
    const response = await axios.get(`/api/projects/${projectId.value}`)
    currentProject.value = response.data
  } catch (error) {
    console.error('Failed to fetch project:', error)
  }
}

const fetchTrace = async () => {
  if (!projectId.value || !traceId.value) return

  loading.value = true
  try {
    const response = await axios.get(`/api/projects/${projectId.value}/traces/${traceId.value}`)
    traceDetail.value = response.data
  } catch (error) {
    console.error('Failed to fetch trace:', error)
    traceDetail.value = null
  } finally {
    loading.value = false
  }
}

const formatDate = (dateString) => {
  if (!dateString) return 'Never'
  try {
    return formatDistanceToNow(new Date(dateString), { addSuffix: true })
  } catch {
    return dateString
  }
}

const formatDuration = (ms) => {
  if (ms < 1000) return `${ms}ms`
  if (ms < 60000) return `${(ms / 1000).toFixed(2)}s`
  return `${(ms / 60000).toFixed(2)}m`
}

const getStatusBadgeClass = (status) => {
  const statusMap = {
    error: 'bg-error-100 text-error-800',
    ok: 'bg-success-100 text-success-800',
    timeout: 'bg-warning-100 text-warning-800',
  }
  return statusMap[status?.toLowerCase()] || statusMap.ok
}

const fetchTraceProfile = async () => {
  traceProfileLoading.value = true
  try {
    const response = await axios.get(
      `/api/profiles/projects/${projectId.value}/traces/${traceId.value}/profile`
    )
    traceProfile.value = response.data
  } catch (e) {
    traceProfile.value = { profile: null, flame_graph: null }
  } finally {
    traceProfileLoading.value = false
  }
}

onMounted(async () => {
  await fetchProject()
  await fetchTrace()
  fetchTraceProfile()
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

.view-toggle-btn {
  @apply flex items-center px-3 py-1.5 text-sm font-medium text-gray-600 rounded-md transition-colors;
}

.view-toggle-btn:hover {
  @apply text-gray-900;
}

.view-toggle-btn.active {
  @apply bg-white text-gray-900 shadow-sm;
}
</style>


