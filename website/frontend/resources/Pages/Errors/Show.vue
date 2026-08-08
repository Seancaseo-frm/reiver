<template>
  <AppLayout :user="user" :current-project="currentProject">
    <div class="error-details-page pb-72">
      <!-- Loading State -->
      <div v-if="loading" class="text-center py-12">
        <div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-primary-500"></div>
        <p class="mt-2 text-gray-500">Loading error details...</p>
      </div>

      <!-- Error State -->
      <div v-else-if="error" class="text-center py-12">
        <svg class="mx-auto h-12 w-12 text-red-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
        </svg>
        <h3 class="mt-2 text-sm font-medium text-gray-900">Error loading details</h3>
        <p class="mt-1 text-sm text-gray-500">{{ error }}</p>
      </div>

      <!-- Error Details -->
      <div v-else-if="errorDetail">
        <!-- Error Header -->
        <div class="mb-6">
          <h1 class="text-2xl font-bold text-gray-900 mb-2">
            {{ errorDetail.exception_type || errorDetail.exceptionType || 'Unknown Error' }}
          </h1>
          <p class="text-lg text-gray-600">
            {{ errorDetail.exception_message || errorDetail.exceptionMessage || errorDetail.message || 'No message' }}
          </p>
          <hr class="mt-4 border-gray-200" />
        </div>

        <!-- Event Information -->
        <div class="event-container">
          <div>
            <p class="text-sm text-gray-600">Event {{ errorDetail.fingerprint || errorDetail.error_id || errorDetail.errorId || errorDetail.id }}</p>
            <p class="text-sm text-gray-900">{{ formatTimestamp(errorDetail.timestamp) }}</p>
            <p v-if="errorDetail.count" class="text-xs text-gray-500 mt-1">
              {{ errorDetail.count }} occurrences
            </p>
          </div>

          <!-- Navigation Buttons -->
          <div class="flex items-center space-x-3">
            <button
              @click="navigateToError(prevErrorId, prevTimestamp)"
              :disabled="!prevErrorId || loading"
              class="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Older
            </button>
            <button
              @click="navigateToError(nextErrorId, nextTimestamp)"
              :disabled="!nextErrorId || loading"
              class="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Newer
            </button>
          </div>
        </div>

        <!-- Action Buttons -->
        <div class="mb-6 flex items-center gap-3 flex-wrap">
          <!-- View Trace -->
          <button
            @click="navigateToTrace"
            :disabled="!hasTraceData"
            class="action-btn action-btn-trace"
          >
            <svg class="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 20l-5.447-2.724A1 1 0 013 16.382V5.618a1 1 0 011.447-.894L9 7m0 13l6-3m-6 3V7m6 10l4.553 2.276A1 1 0 0021 18.382V7.618a1 1 0 00-.553-.894L15 4m0 13V4m0 0L9 7" />
            </svg>
            View Trace
          </button>

          <!-- Correlated Logs (via trace_id) -->
          <button
            @click="viewRelatedLogs"
            :disabled="!hasRelatedLogs && !checkingLogs"
            class="action-btn action-btn-logs"
            :title="hasRelatedLogs ? `${relatedLogsCount} logs correlated via trace ID` : 'No correlated logs (error not linked to a trace)'"
          >
            <svg class="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
            </svg>
            <span v-if="checkingLogs">Checking...</span>
            <span v-else-if="hasRelatedLogs">View Logs ({{ relatedLogsCount }})</span>
            <span v-else>No Correlated Logs</span>
          </button>

          <!-- Metrics -->
          <button
            @click="viewMetrics"
            class="action-btn action-btn-metrics"
          >
            <svg class="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
            </svg>
            View Metrics
          </button>

          <!-- Flag Changes (only show if available) -->
          <button
            v-if="hasFlagChanges"
            @click="viewFlagChanges"
            class="action-btn action-btn-flags"
          >
            <svg class="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 21v-4m0 0V5a2 2 0 012-2h6.5l1 1H21l-3 6 3 6h-8.5l-1-1H5a2 2 0 00-2 2zm9-13.5V9" />
            </svg>
            Flag Changes ({{ flagChangesCount }})
          </button>
        </div>

        <!-- GitHub Commit Info (when version tracking is available) -->
        <div v-if="versionInfo && versionInfo.first_seen_version" class="mb-6 p-4 border border-gray-200 rounded-lg bg-gray-50">
          <div class="flex items-start gap-4">
            <div class="flex-shrink-0">
              <svg class="w-6 h-6 text-gray-600" fill="currentColor" viewBox="0 0 24 24">
                <path fill-rule="evenodd" d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.531 1.032 1.531 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.019 10.019 0 0022 12.017C22 6.484 17.522 2 12 2z" clip-rule="evenodd"/>
              </svg>
            </div>
            <div class="flex-1 min-w-0">
              <div class="flex items-center gap-2 mb-1">
                <span class="text-sm font-medium text-gray-900">
                  First seen in version:
                </span>
                <code class="px-2 py-0.5 text-xs font-mono bg-gray-200 rounded">
                  {{ versionInfo.first_seen_version.substring(0, 8) }}
                </code>
                <span v-if="versionInfo.is_new_in_version" class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-yellow-100 text-yellow-800">
                  New in this version
                </span>
              </div>
              <p v-if="commitInfo" class="text-sm text-gray-600 mb-2">
                {{ commitInfo.message.split('\n')[0] }}
              </p>
              <div v-if="commitInfo" class="flex items-center gap-4 text-xs text-gray-500">
                <span v-if="commitInfo.author_login">by @{{ commitInfo.author_login }}</span>
                <a v-if="commitInfo.html_url" :href="commitInfo.html_url" target="_blank" class="text-blue-600 hover:underline">
                  View on GitHub
                </a>
                <span v-if="commitInfo.pull_requests && commitInfo.pull_requests.length > 0">
                  <a v-for="pr in commitInfo.pull_requests" :key="pr.number" :href="pr.html_url" target="_blank" class="text-blue-600 hover:underline mr-2">
                    PR #{{ pr.number }}
                  </a>
                </span>
              </div>
              <p v-if="versionInfo.version_count > 1" class="text-xs text-gray-500 mt-1">
                Seen in {{ versionInfo.version_count }} different versions
              </p>
            </div>
          </div>
        </div>

        <!-- Trace Link -->
        <div class="dashed-container">
          <p class="text-sm text-gray-600">See trace graph</p>
          <button
            @click="navigateToTrace"
            :disabled="!hasTraceData"
            class="px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-md hover:bg-blue-700 focus:ring-2 focus:ring-blue-500 focus:outline-none disabled:opacity-50 disabled:cursor-not-allowed"
          >
            See error in trace graph
          </button>
        </div>

        <!-- LLM Context (when exception is from Prompt Hub) -->
        <div v-if="llmContext" class="mb-6 p-4 border border-purple-200 rounded-lg bg-purple-50">
          <div class="flex items-start gap-3">
            <div class="flex-shrink-0">
              <svg class="w-6 h-6 text-purple-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
              </svg>
            </div>
            <div class="flex-1">
              <div class="flex items-center gap-2 mb-2">
                <h3 class="text-sm font-semibold text-purple-900">Prompt Hub Context</h3>
                <span class="px-2 py-0.5 text-xs font-medium rounded bg-purple-200 text-purple-800">
                  {{ llmContext.provider }}
                </span>
              </div>
              <p class="text-sm text-purple-800 mb-2">
                This exception occurred during an LLM operation
              </p>
              <div class="grid grid-cols-2 gap-3 text-sm mb-3">
                <div>
                  <span class="text-purple-600">Model:</span>
                  <span class="ml-1 text-purple-900 font-medium">{{ llmContext.model }}</span>
                </div>
                <div v-if="llmContext.session_id">
                  <span class="text-purple-600">Session:</span>
                  <span class="ml-1 text-purple-900 font-mono text-xs">{{ llmContext.session_id.slice(0, 8) }}...</span>
                </div>
                <div v-if="llmContext.prompt_id">
                  <span class="text-purple-600">Prompt:</span>
                  <span class="ml-1 text-purple-900 font-mono text-xs">{{ llmContext.prompt_id }}</span>
                </div>
                <div v-if="llmContext.cost">
                  <span class="text-purple-600">Cost:</span>
                  <span class="ml-1 text-purple-900 font-medium">${{ llmContext.cost.toFixed(4) }}</span>
                </div>
              </div>
              <div v-if="llmContext.error_type" class="mb-3">
                <span class="text-sm text-purple-600">Error Type:</span>
                <span class="ml-1 text-sm text-red-600 font-medium">{{ llmContext.error_type }}</span>
              </div>
              <div v-if="llmContext.provider_message" class="mb-3 p-2 bg-purple-100 rounded text-sm">
                <span class="text-purple-600">Provider Message:</span>
                <p class="text-purple-900 mt-1">{{ llmContext.provider_message }}</p>
              </div>
              <div class="flex gap-3 pt-2">
                <router-link
                  v-if="llmContext.session_id"
                  :to="`/p/${projectId}/llm/sessions/${llmContext.session_id}`"
                  class="text-sm text-purple-700 hover:text-purple-800 font-medium"
                >
                  View LLM Session →
                </router-link>
                <router-link
                  v-if="llmContext.prompt_id"
                  :to="`/p/${projectId}/llm/prompts/${llmContext.prompt_id}`"
                  class="text-sm text-purple-700 hover:text-purple-800 font-medium"
                >
                  View Prompt →
                </router-link>
              </div>
            </div>
          </div>
        </div>

        <!-- Stack Trace -->
        <div class="mb-8">
          <h2 class="text-xl font-semibold text-gray-900 mb-4">Stack trace</h2>
          <div class="error-container">
            <div class="code-editor">
              <div class="code-header">
                <span class="code-language">JavaScript</span>
              </div>
              <pre class="code-content">{{ formatStackTrace(errorDetail.exception_stacktrace || errorDetail.exceptionStacktrace || errorDetail.stacktrace) }}</pre>
            </div>
          </div>
        </div>

        <!-- Error Details Table -->
        <div class="editor-container">
          <div class="space-y-4">
            <table class="min-w-full divide-y divide-gray-200">
              <thead class="bg-gray-50">
                <tr>
                  <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider w-1/4">
                    Key
                  </th>
                  <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Value
                  </th>
                </tr>
              </thead>
              <tbody class="bg-white divide-y divide-gray-200">
                <tr v-for="detail in errorDetails" :key="detail.key">
                  <td class="px-4 py-3 text-sm font-medium text-gray-900 w-1/4">
                    {{ detail.key }}
                  </td>
                  <td class="px-4 py-3 text-sm text-gray-500 break-words">
                    <span v-if="detail.key === 'Tags' && typeof detail.value === 'object'">
                      <span v-for="(value, key) in detail.value" :key="key" class="inline-block mr-2 mb-1">
                        <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-blue-100 text-blue-800">
                          {{ key }}: {{ value }}
                        </span>
                      </span>
                    </span>
                    <span v-else-if="detail.key === 'User Data' && typeof detail.value === 'object'">
                      <span v-for="(value, key) in detail.value" :key="key" class="inline-block mr-2 mb-1">
                        <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-green-100 text-green-800">
                          {{ key }}: {{ value }}
                        </span>
                      </span>
                    </span>
                    <span v-else>
                      {{ detail.value }}
                    </span>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>

    <!-- Correlated Signals Panel -->
    <CorrelatedSignalsPanel
      v-if="correlatedTraceIds.length > 0"
      :trace-ids="correlatedTraceIds"
      :service-name="errorDetail?.service_name || ''"
      :project-id="projectId"
      :timestamp="errorDetail?.timestamp || ''"
      :hide-tabs="['exceptions']"
    />
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useAuth } from '@/composables/useAuth'
import AppLayout from '@/Layouts/AppLayout.vue'
import CorrelatedSignalsPanel from '@/components/CorrelatedSignalsPanel.vue'
import axios from 'axios'
import { format } from 'date-fns'

const route = useRoute()
const router = useRouter()
const { user } = useAuth()

const projectId = computed(() => route.params.id)
const groupId = computed(() => route.params.group_id)
const currentProject = ref(null)

const loading = ref(true)
const error = ref(null)
const errorDetail = ref(null)
const prevErrorId = ref(null)
const prevTimestamp = ref(null)
const nextErrorId = ref(null)
const nextTimestamp = ref(null)
const relatedLogsCount = ref(0)
const checkingLogs = ref(false)

// GitHub integration - version tracking
const versionInfo = ref(null)
const commitInfo = ref(null)

// URL query parameters
const errorId = computed(() => route.query.errorId)
const timestamp = computed(() => route.query.timestamp)

// Check if trace data is available
const hasTraceData = computed(() => {
  return (errorDetail.value?.trace_id || errorDetail.value?.traceID) ||
         (errorDetail.value?.traces && errorDetail.value.traces.length > 0)
})

// Check if related logs are available
const hasRelatedLogs = computed(() => {
  return relatedLogsCount.value > 0
})

// Extract LLM context if this exception originated from Prompt Hub
const llmContext = computed(() => {
  if (!errorDetail.value) return null
  
  const attrs = errorDetail.value.attributes || errorDetail.value.context || {}
  const tags = errorDetail.value.tags || {}
  const message = errorDetail.value.exception_message || errorDetail.value.message || ''
  
  // Check if this is an LLM-related exception
  const isLlm = (
    attrs['gen_ai.system'] ||
    attrs['llm.provider'] ||
    tags['llm_gateway'] ||
    message.includes('OpenAI') ||
    message.includes('Anthropic') ||
    message.includes('Claude') ||
    message.includes('GPT') ||
    message.includes('rate limit') ||
    message.includes('API key')
  )
  
  if (!isLlm) return null
  
  // Extract LLM context
  return {
    provider: attrs['gen_ai.system'] || attrs['llm.provider'] || detectProvider(message),
    model: attrs['gen_ai.request.model'] || attrs['llm.model'] || 'Unknown',
    session_id: attrs['gen_ai.session_id'] || attrs['llm.session_id'] || null,
    prompt_id: attrs['gen_ai.prompt_id'] || attrs['llm.prompt_id'] || null,
    cost: parseFloat(attrs['gen_ai.usage.cost'] || attrs['llm.cost'] || 0),
    error_type: attrs['gen_ai.error_type'] || attrs['llm.error_type'] || extractErrorType(message),
    provider_message: attrs['gen_ai.error_message'] || attrs['llm.error_message'] || null,
  }
})

// Helper to detect provider from error message
const detectProvider = (message) => {
  if (message.includes('OpenAI') || message.includes('GPT')) return 'OpenAI'
  if (message.includes('Anthropic') || message.includes('Claude')) return 'Anthropic'
  if (message.includes('Google') || message.includes('Gemini')) return 'Google'
  if (message.includes('Bedrock')) return 'AWS Bedrock'
  return 'Unknown'
}

// Helper to extract error type from message
const extractErrorType = (message) => {
  if (message.includes('rate limit')) return 'RateLimitError'
  if (message.includes('API key')) return 'AuthenticationError'
  if (message.includes('timeout')) return 'TimeoutError'
  if (message.includes('context length')) return 'ContextLengthError'
  return null
}

// Keys to exclude from the details table
const keysToExclude = [
  'exceptionStacktrace',
  'exceptionType',
  'errorId',
  'error_id',
  'timestamp',
  'exceptionMessage',
  'exceptionEscaped',
  'id', 'group_id', 'groupID', 'service_name', 'serviceName',
  'trace_id', 'traceID', 'span_id', 'spanID',
  'count', 'level', 'status', 'first_seen', 'last_seen',
  'tags', 'user_data', 'context', 'traces', 'flag_changes',
  'fingerprint', 'exception_value', 'exceptionValue',
  'exception_type', 'exception_message', 'stacktrace',
  // Context fields are displayed separately
  'environment', 'version', 'deployment_id', 'region', 'host_name', 'runtime',
  'pod_name', 'cluster_name', 'container_id', 'http_method', 'http_url', 'user_id'
]

const errorDetails = computed(() => {
  if (!errorDetail.value) return []

  const details = []

  // Add specific fields we want to display
  if (errorDetail.value.count) {
    details.push({
      key: 'Count',
      value: errorDetail.value.count
    })
  }

  if (errorDetail.value.level) {
    details.push({
      key: 'Level',
      value: errorDetail.value.level
    })
  }

  if (errorDetail.value.status) {
    details.push({
      key: 'Status',
      value: errorDetail.value.status
    })
  }

  if (errorDetail.value.service_name || errorDetail.value.serviceName) {
    details.push({
      key: 'Service',
      value: errorDetail.value.service_name || errorDetail.value.serviceName
    })
  }

  if (errorDetail.value.first_seen) {
    details.push({
      key: 'First Seen',
      value: formatTimestamp(errorDetail.value.first_seen)
    })
  }

  if (errorDetail.value.last_seen) {
    details.push({
      key: 'Last Seen',
      value: formatTimestamp(errorDetail.value.last_seen)
    })
  }

  // Deployment & Environment Context
  if (errorDetail.value.environment) {
    details.push({ key: 'Environment', value: errorDetail.value.environment })
  }
  if (errorDetail.value.version) {
    details.push({ key: 'Version', value: errorDetail.value.version })
  }
  if (errorDetail.value.deployment_id) {
    details.push({ key: 'Deployment ID', value: errorDetail.value.deployment_id })
  }
  if (errorDetail.value.region) {
    details.push({ key: 'Region', value: errorDetail.value.region })
  }
  if (errorDetail.value.host_name) {
    details.push({ key: 'Host Name', value: errorDetail.value.host_name })
  }
  if (errorDetail.value.runtime) {
    details.push({ key: 'Runtime', value: errorDetail.value.runtime })
  }

  // Kubernetes / Container Context
  if (errorDetail.value.pod_name) {
    details.push({ key: 'Pod Name', value: errorDetail.value.pod_name })
  }
  if (errorDetail.value.cluster_name) {
    details.push({ key: 'Cluster Name', value: errorDetail.value.cluster_name })
  }
  if (errorDetail.value.container_id) {
    details.push({ key: 'Container ID', value: errorDetail.value.container_id })
  }

  // HTTP Context
  if (errorDetail.value.http_method) {
    details.push({ key: 'HTTP Method', value: errorDetail.value.http_method })
  }
  if (errorDetail.value.http_url) {
    details.push({ key: 'HTTP URL', value: errorDetail.value.http_url })
  }

  // User Context
  if (errorDetail.value.user_id) {
    details.push({ key: 'User ID', value: errorDetail.value.user_id })
  }

  if (errorDetail.value.tags && Object.keys(errorDetail.value.tags).length > 0) {
    details.push({
      key: 'Tags',
      value: errorDetail.value.tags
    })
  }

  if (errorDetail.value.user_data && Object.keys(errorDetail.value.user_data).length > 0) {
    details.push({
      key: 'User Data',
      value: errorDetail.value.user_data
    })
  }

  // Add any other fields not in the exclude list
  Object.keys(errorDetail.value)
    .filter(key => !keysToExclude.includes(key) && !['count', 'level', 'status', 'service_name', 'serviceName', 'first_seen', 'last_seen', 'tags', 'user_data'].includes(key))
    .forEach(key => {
      const value = errorDetail.value[key]
      if (value !== null && value !== undefined && value !== '') {
        details.push({
          key: key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase()),
          value: value
        })
      }
    })

  return details
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

const fetchErrorDetail = async () => {
  if (!projectId.value || !groupId.value) return

  loading.value = true
  error.value = null

  try {
    const response = await axios.get(`/api/projects/${projectId.value}/exceptions/${groupId.value}`)
    const data = response.data

    // Transform the backend data structure to match our expectations
    // Backend returns: { group: {...}, recent_exceptions: [...], traces: [], flag_changes: [] }
    // We need to flatten the group data and get the most recent exception for details
    const groupData = data.group
    const recentExceptions = data.recent_exceptions || []

    // Use the group data as base, and get stacktrace from the most recent exception
    const mostRecentException = recentExceptions.length > 0 ? recentExceptions[0] : null

    errorDetail.value = {
      // Group fields
      id: groupData.id,
      group_id: groupData.id,
      groupID: groupData.id,
      fingerprint: groupData.fingerprint,
      exception_type: groupData.exception_type,
      exceptionType: groupData.exception_type,
      exception_message: groupData.message,
      exceptionMessage: groupData.message,
      exception_value: groupData.exception_value,
      exceptionValue: groupData.exception_value,
      level: groupData.level,
      status: groupData.status,
      service_name: groupData.service_name,
      serviceName: groupData.service_name,
      count: groupData.count,
      first_seen: groupData.first_seen,
      last_seen: groupData.last_seen,

      // Deployment & environment context
      environment: groupData.environment,
      version: groupData.version,
      deployment_id: groupData.deployment_id,
      region: groupData.region,
      host_name: groupData.host_name,
      runtime: groupData.runtime,

      // Kubernetes / container context
      pod_name: groupData.pod_name,
      cluster_name: groupData.cluster_name,
      container_id: groupData.container_id,

      // HTTP context
      http_method: groupData.http_method,
      http_url: groupData.http_url,

      // User context
      user_id: groupData.user_id,

      // Exception instance fields (from most recent exception)
      error_id: mostRecentException?.id || groupData.id,
      errorId: mostRecentException?.id || groupData.id,
      timestamp: mostRecentException?.timestamp || groupData.last_seen,
      exception_stacktrace: mostRecentException?.stacktrace ? JSON.stringify(mostRecentException.stacktrace, null, 2) : null,
      exceptionStacktrace: mostRecentException?.stacktrace ? JSON.stringify(mostRecentException.stacktrace, null, 2) : null,
      stacktrace: mostRecentException?.stacktrace ? JSON.stringify(mostRecentException.stacktrace, null, 2) : null,

      // Additional data
      tags: mostRecentException?.tags || {},
      user_data: mostRecentException?.user_data || {},
      context: mostRecentException?.context || null,

      // Related data
      traces: data.traces || [],
      flag_changes: data.flag_changes || []
    }

    // Also fetch next/prev navigation data
    await fetchNavigationData()
  } catch (err) {
    console.error('Failed to fetch error detail:', err)
    error.value = err.response?.data?.message || 'Failed to load error details'
  } finally {
    loading.value = false
  }
}

const fetchNavigationData = async () => {
  if (!errorDetail.value) return

  try {
    // This would need a backend endpoint to get next/prev error IDs
    // For now, we'll skip this functionality
    console.log('Navigation data fetch would go here')
  } catch (err) {
    console.error('Failed to fetch navigation data:', err)
  }
}

// Fetch GitHub version tracking info
const fetchVersionInfo = async () => {
  if (!errorDetail.value?.fingerprint) return
  
  try {
    const response = await axios.get(
      `/api/projects/${projectId.value}/github/version-info/${errorDetail.value.fingerprint}`,
      { params: { current_version: errorDetail.value.version } }
    )
    versionInfo.value = response.data
    
    // If we have version info, try to fetch commit details
    if (versionInfo.value?.first_seen_version) {
      await fetchCommitInfo(versionInfo.value.first_seen_version)
    }
  } catch (err) {
    // Silently fail - GitHub integration may not be configured
    console.debug('Version info not available:', err.message)
    versionInfo.value = null
  }
}

// Fetch commit details from GitHub
const fetchCommitInfo = async (sha) => {
  if (!sha) return
  
  try {
    const response = await axios.get(`/api/projects/${projectId.value}/github/commit/${sha}`)
    commitInfo.value = response.data
  } catch (err) {
    // Silently fail - GitHub integration may not be configured or commit not found
    console.debug('Commit info not available:', err.message)
    commitInfo.value = null
  }
}

const checkRelatedLogs = async () => {
  if (!errorDetail.value) return

  checkingLogs.value = true
  try {
    // Get trace IDs linked to this error (Datadog-style trace-log correlation)
    const traceIds = getLinkedTraceIds()
    
    if (traceIds.length > 0) {
      // Trace-correlated logs: query by trace_id(s)
      const params = {
        event_type: 'logs',
        trace_id: traceIds.join(','),
      }

      const response = await axios.get(`/api/projects/${projectId.value}/events`, { params })
      relatedLogsCount.value = response.data?.length || 0
    } else {
      // Fallback: no trace correlation available
      relatedLogsCount.value = 0
    }
  } catch (err) {
    console.error('Failed to check for related logs:', err)
    relatedLogsCount.value = 0
  } finally {
    checkingLogs.value = false
  }
}

// Get trace IDs linked to this error (from error_traces junction table)
const getLinkedTraceIds = () => {
  if (!errorDetail.value) return []
  
  const traceIds = []
  
  // Direct trace_id on the error
  const directTraceId = errorDetail.value.trace_id || errorDetail.value.traceID
  if (directTraceId) {
    traceIds.push(directTraceId)
  }
  
  // Trace IDs from the traces array (populated from error_traces table)
  if (errorDetail.value.traces && errorDetail.value.traces.length > 0) {
    for (const trace of errorDetail.value.traces) {
      const tid = trace.trace_id || trace.id
      if (tid && !traceIds.includes(tid)) {
        traceIds.push(tid)
      }
    }
  }
  
  return traceIds
}

const correlatedTraceIds = computed(() => getLinkedTraceIds())

const navigateToError = (targetErrorId, targetTimestamp) => {
  if (!targetErrorId) return

  // Navigate to the same error group but different error instance
  router.push({
    name: route.name,
    params: route.params,
    query: {
      groupId: groupId.value,
      timestamp: targetTimestamp,
      errorId: targetErrorId
    }
  })
}

const navigateToTrace = () => {
  if (!errorDetail.value) return

  const traceId = errorDetail.value.trace_id || errorDetail.value.traceID

  let url
  if (traceId) {
    // Navigate to specific trace if we have a trace ID
    url = `/p/${projectId.value}/traces/${traceId}`
  } else if (errorDetail.value.traces && errorDetail.value.traces.length > 0) {
    // Navigate to first available trace
    const firstTrace = errorDetail.value.traces[0]
    url = `/p/${projectId.value}/traces/${firstTrace.id || firstTrace.trace_id}`
  } else {
    // No trace data available - navigate to traces list page
    url = `/p/${projectId.value}/traces`
  }

  // Open in new tab
  window.open(url, '_blank')
}

const viewRelatedLogs = () => {
  if (!errorDetail.value) return

  // Datadog-style trace-log correlation: navigate to logs filtered by trace_id
  const traceIds = getLinkedTraceIds()

  let url
  if (traceIds.length > 0) {
    // Navigate to logs with trace_id filter for correlated logs
    const params = new URLSearchParams({ trace_id: traceIds.join(',') })
    url = `/p/${projectId.value}/logs?${params.toString()}`
  } else {
    // Fallback: no trace correlation, just go to logs page
    url = `/p/${projectId.value}/logs`
  }

  // Open in new tab
  window.open(url, '_blank')
}

const viewMetrics = () => {
  if (!errorDetail.value) return

  // Navigate to dashboards/metrics with error-focused view - open in new tab
  window.open(`/p/${projectId.value}/dashboards`, '_blank')
}

const viewFlagChanges = () => {
  if (!errorDetail.value || !hasFlagChanges.value) return

  // For now, show an alert with the flag changes data
  // In the future, this could navigate to a dedicated page or show a modal
  const flagChanges = errorDetail.value.flag_changes
  console.log('Flag changes:', flagChanges)
  alert(`${flagChanges.length} flag change(s) detected around the time of this error. Check the console for details.`)
}

const hasFlagChanges = computed(() => {
  return errorDetail.value?.flag_changes && errorDetail.value.flag_changes.length > 0
})

const flagChangesCount = computed(() => {
  return errorDetail.value?.flag_changes?.length || 0
})

const formatTimestamp = (timestamp) => {
  if (!timestamp) return 'Unknown'

  try {
    // Handle different timestamp formats
    let date
    if (typeof timestamp === 'string') {
      date = new Date(timestamp)
    } else if (typeof timestamp === 'number') {
      // Assume nanoseconds, convert to milliseconds
      date = new Date(timestamp / 1000000)
    } else {
      date = new Date(timestamp)
    }

    return format(date, 'PPPppp') // Full date and time
  } catch {
    return timestamp.toString()
  }
}

const formatStackTrace = (stackTrace) => {
  if (!stackTrace) return 'No stack trace available'

  try {
    // If it's already a string, return it
    if (typeof stackTrace === 'string') {
      return stackTrace
    }

    // If it's an array of stack trace objects, format it nicely
    if (Array.isArray(stackTrace)) {
      return stackTrace.map(frame =>
        `${frame.filename}:${frame.lineno} in ${frame.function || 'anonymous'}\n  ${frame.code || ''}`
      ).join('\n\n')
    }

    return JSON.stringify(stackTrace, null, 2)
  } catch {
    return stackTrace.toString()
  }
}

// Watch for route changes (when navigating between errors in the same group)
watch(() => route.query, (newQuery) => {
  if (newQuery.errorId && newQuery.timestamp) {
    // Fetch specific error instance
    fetchSpecificError(newQuery.errorId, newQuery.timestamp)
  }
}, { deep: true })

const fetchSpecificError = async (errorId, timestamp) => {
  // This would fetch a specific error instance within the group
  // For now, we'll just reload the group details
  await fetchErrorDetail()
}

// Lifecycle
onMounted(async () => {
  await fetchProject()
  await fetchErrorDetail()
  await checkRelatedLogs()
  await fetchVersionInfo() // GitHub integration - version tracking
})
</script>

<style scoped>
.error-details-page {
  padding: 16px;
  @apply min-h-screen bg-gray-50;
}

.dashed-container {
  border: 1px dashed #d1d5db;
  box-sizing: border-box;
  border-radius: 0.25rem;
  display: flex;
  justify-content: space-between;
  padding: 1rem;
  margin-top: 1.875rem;
  margin-bottom: 1.625rem;
  align-items: center;
}

.error-container {
  height: 50vh;
  background-color: #111827;
  border-radius: 0.5rem;
  padding: 1rem;
  overflow: auto;
  font-family: 'Fira Code', 'Monaco', 'Consolas', monospace;
}

.error-container pre {
  margin: 0;
  white-space: pre;
  color: #f3f4f6;
}

/* Custom scrollbar for stack trace */
.error-container::-webkit-scrollbar {
  width: 8px;
  height: 8px;
}

.error-container::-webkit-scrollbar-track {
  background: #374151;
  border-radius: 4px;
}

.error-container::-webkit-scrollbar-thumb {
  background: #6b7280;
  border-radius: 4px;
}

.error-container::-webkit-scrollbar-thumb:hover {
  background: #9ca3af;
}

.code-editor {
  background-color: #1e1e1e;
  border-radius: 8px;
  overflow: hidden;
  font-family: 'Fira Code', 'Monaco', 'Consolas', 'Courier New', monospace;
}

.code-header {
  background-color: #2d2d2d;
  padding: 8px 16px;
  border-bottom: 1px solid #404040;
  display: flex;
  align-items: center;
}

.code-language {
  color: #cccccc;
  font-size: 12px;
  font-weight: 500;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}

.code-content {
  padding: 16px;
  margin: 0;
  color: #f8f8f2;
  font-size: 14px;
  line-height: 1.5;
  white-space: pre-wrap;
  word-wrap: break-word;
  overflow-x: auto;
  background-color: #1e1e1e;
}

.code-content::-webkit-scrollbar {
  height: 8px;
}

.code-content::-webkit-scrollbar-track {
  background: #2d2d2d;
}

.code-content::-webkit-scrollbar-thumb {
  background: #555;
  border-radius: 4px;
}

.code-content::-webkit-scrollbar-thumb:hover {
  background: #777;
}

.editor-container {
  margin-top: 1.5rem;
}.event-container {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.action-btn {
  @apply inline-flex items-center px-4 py-2 text-sm font-medium rounded-md transition-colors;
  @apply bg-white text-gray-700;
  @apply border border-gray-300;
  @apply hover:bg-gray-50;
  @apply focus:outline-none focus:ring-2 focus:ring-primary-500 focus:ring-offset-2;
}

.action-btn:disabled {
  @apply opacity-50 cursor-not-allowed;
}

.action-btn-trace {
  @apply text-blue-700 border-blue-300;
  @apply hover:bg-blue-50;
}

.action-btn-logs {
  @apply text-green-700 border-green-300;
  @apply hover:bg-green-50;
}

.action-btn-metrics {
  @apply text-purple-700 border-purple-300;
  @apply hover:bg-purple-50;
}

.action-btn-flags {
  @apply text-orange-700 border-orange-300;
  @apply hover:bg-orange-50;
}
</style>
