<template>
  <AppLayout :user="user" :current-project="currentProject">
    <div class="profile-detail-page max-w-full mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <!-- Header -->
      <div class="mb-6">
        <router-link
          :to="`/p/${projectId}/profiles`"
          class="text-primary-600 hover:text-primary-700 text-sm font-medium mb-2 inline-block"
        >
          &larr; Back to Profiles
        </router-link>

        <!-- Loading -->
        <div v-if="loading" class="mt-4 text-center py-12">
          <div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-primary-500"></div>
          <p class="mt-2 text-gray-500">Loading profile...</p>
        </div>

        <!-- Error -->
        <div v-else-if="error" class="mt-4 text-center py-12">
          <svg class="mx-auto h-12 w-12 text-red-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.082 16.5c-.77.833.192 2.5 1.732 2.5z" />
          </svg>
          <h3 class="mt-2 text-sm font-medium text-gray-900">Failed to load profile</h3>
          <p class="mt-1 text-sm text-gray-500">{{ error }}</p>
        </div>

        <!-- Profile content -->
        <template v-else-if="profile">
          <div class="flex items-start justify-between">
            <div>
              <h1 class="text-2xl font-bold text-gray-900">Profile Details</h1>
              <p class="text-sm text-gray-500 mt-1 font-mono">
                {{ profileId }}
              </p>
            </div>
            <a
              :href="`/api/profiles/projects/${projectId}/profiles/${profileId}/download`"
              download
              class="inline-flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium rounded-md border border-gray-300 text-gray-700 hover:bg-gray-50 transition-colors"
            >
              <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
              </svg>
              Download
            </a>
          </div>

          <!-- Metadata cards -->
          <div class="mt-4 grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-4">
            <div class="bg-white border border-gray-200 rounded-lg p-3">
              <p class="text-xs font-medium text-gray-500 uppercase">Service</p>
              <p class="mt-1 text-sm font-semibold text-gray-900">
                {{ profile.service_name || 'unknown' }}
              </p>
            </div>
            <div class="bg-white border border-gray-200 rounded-lg p-3">
              <p class="text-xs font-medium text-gray-500 uppercase">Type</p>
              <p class="mt-1 text-sm font-semibold text-gray-900">
                {{ profile.period_type || 'cpu' }}
              </p>
            </div>
            <div class="bg-white border border-gray-200 rounded-lg p-3">
              <p class="text-xs font-medium text-gray-500 uppercase">Samples</p>
              <p class="mt-1 text-sm font-semibold text-gray-900 font-mono">
                {{ formatNumber(profile.sample_count) }}
              </p>
            </div>
            <div class="bg-white border border-gray-200 rounded-lg p-3">
              <p class="text-xs font-medium text-gray-500 uppercase">Duration</p>
              <p class="mt-1 text-sm font-semibold text-gray-900 font-mono">
                {{ formatDuration(profile.duration_nano) }}
              </p>
            </div>
            <div class="bg-white border border-gray-200 rounded-lg p-3">
              <p class="text-xs font-medium text-gray-500 uppercase">Period</p>
              <p class="mt-1 text-sm font-semibold text-gray-900 font-mono">
                {{ formatNumber(profile.period) }}
              </p>
            </div>
            <div class="bg-white border border-gray-200 rounded-lg p-3">
              <p class="text-xs font-medium text-gray-500 uppercase">Timestamp</p>
              <p class="mt-1 text-sm font-semibold text-gray-900">
                {{ formatTimestamp(profile.timestamp) }}
              </p>
            </div>
          </div>

          <!-- Trace link -->
          <div v-if="profile.trace_id" class="mt-4">
            <router-link
              :to="`/p/${projectId}/traces/${profile.trace_id}`"
              class="inline-flex items-center gap-2 text-sm text-primary-600 hover:text-primary-700 font-medium"
            >
              <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1" />
              </svg>
              View linked trace {{ profile.trace_id.substring(0, 16) }}...
            </router-link>
          </div>

          <!-- Flamegraph -->
          <div class="mt-6 border border-gray-200 rounded-lg overflow-hidden">
            <ProfileFlamegraph
              v-if="profile.flame_graph"
              :flameGraph="profile.flame_graph"
              @view-source="handleViewSource"
            />
            <div v-else class="text-center py-12 text-gray-500">
              <svg class="mx-auto h-12 w-12 text-gray-400 mb-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17.657 18.657A8 8 0 016.343 7.343S7 9 9 10c0-2 .5-5 2.986-7C14 5 16.09 5.777 17.656 7.343A7.975 7.975 0 0120 13a7.975 7.975 0 01-2.343 5.657z" />
              </svg>
              <p class="text-sm">No flamegraph data available for this profile.</p>
            </div>
          </div>

          <!-- Annotated Source Panel -->
          <div v-if="sourceView.visible" class="mt-6">
            <!-- Loading source -->
            <div v-if="sourceView.loading" class="border border-gray-200 rounded-lg p-8 text-center">
              <div class="inline-block animate-spin rounded-full h-6 w-6 border-b-2 border-primary-500"></div>
              <p class="mt-2 text-sm text-gray-500">Loading source file...</p>
            </div>
            <!-- Source error -->
            <div v-else-if="sourceView.error" class="border border-gray-200 rounded-lg p-6">
              <div class="flex items-start gap-3">
                <svg class="w-5 h-5 text-amber-500 flex-shrink-0 mt-0.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.082 16.5c-.77.833.192 2.5 1.732 2.5z" />
                </svg>
                <div>
                  <p class="text-sm font-medium text-gray-900">Cannot display source code</p>
                  <p class="mt-1 text-sm text-gray-500">{{ sourceView.error }}</p>
                </div>
                <button
                  @click="closeSourceView"
                  class="ml-auto p-1 text-gray-400 hover:text-gray-600"
                >
                  <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              </div>
            </div>
            <!-- Source content -->
            <AnnotatedSource
              v-else-if="sourceView.content"
              :sourceCode="sourceView.content"
              :annotations="sourceView.annotations"
              :highlightLine="sourceView.highlightLine"
              :filePath="sourceView.filePath"
              :functionName="sourceView.functionName"
              :htmlUrl="sourceView.htmlUrl"
              @close="closeSourceView"
            />
          </div>
        </template>
      </div>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRoute } from 'vue-router'
import { useAuth } from '@/composables/useAuth'
import AppLayout from '@/Layouts/AppLayout.vue'
import ProfileFlamegraph from '@/components/ProfileFlamegraph.vue'
import AnnotatedSource from '@/components/AnnotatedSource.vue'
import axios from 'axios'

const route = useRoute()
const { user } = useAuth()

const projectId = computed(() => route.params.id)
const profileId = computed(() => route.params.profile_id)
const currentProject = ref(null)

// State
const loading = ref(false)
const error = ref(null)
const profile = ref(null)

// Source view state
const sourceView = ref({
  visible: false,
  loading: false,
  error: null,
  content: null,
  annotations: {},
  highlightLine: null,
  filePath: '',
  functionName: '',
  htmlUrl: null,
})

const fetchProject = async () => {
  try {
    const response = await axios.get(`/api/projects/${projectId.value}`)
    currentProject.value = response.data
  } catch (err) {
    console.error('Failed to fetch project:', err)
  }
}

const fetchProfile = async () => {
  loading.value = true
  error.value = null
  try {
    const response = await axios.get(
      `/api/profiles/projects/${projectId.value}/profiles/${profileId.value}`
    )
    profile.value = response.data
  } catch (err) {
    console.error('Failed to fetch profile:', err)
    error.value = err.response?.data?.message || err.message || 'Unknown error'
  } finally {
    loading.value = false
  }
}

// Compute per-line sample counts from the flamegraph tree for a given filename.
// Walks the tree and sums self-values for nodes matching the filename.
const computeAnnotations = (filename) => {
  const fg = profile.value?.flame_graph
  if (!fg?.root) return {}

  const lineMap = {}
  const walk = (node) => {
    if (node.filename === filename && node.line_number) {
      const selfValue = node.value - (node.children || []).reduce((s, c) => s + (c.value || 0), 0)
      lineMap[node.line_number] = (lineMap[node.line_number] || 0) + Math.max(0, selfValue)
    }
    for (const child of (node.children || [])) {
      walk(child)
    }
  }
  walk(fg.root)
  return lineMap
}

// Handle "View Source" event from flamegraph
const handleViewSource = async ({ filename, functionName, lineNumber }) => {
  sourceView.value = {
    visible: true,
    loading: true,
    error: null,
    content: null,
    annotations: {},
    highlightLine: lineNumber,
    filePath: filename,
    functionName: functionName || '',
    htmlUrl: null,
  }

  // Compute annotations while we fetch source
  const annotations = computeAnnotations(filename)
  sourceView.value.annotations = annotations

  // Build query params
  const params = new URLSearchParams({ file: filename })
  const serviceVersion = profile.value?.service_version
  if (serviceVersion) {
    params.set('ref', serviceVersion)
  }

  try {
    const response = await axios.get(
      `/api/profiles/projects/${projectId.value}/source?${params.toString()}`
    )
    sourceView.value.content = response.data.content
    sourceView.value.htmlUrl = response.data.html_url || null
    sourceView.value.loading = false
  } catch (err) {
    const status = err.response?.status
    let message = err.response?.data?.message || err.message || 'Failed to fetch source'

    if (status === 400 && message.includes('GitHub integration')) {
      message = 'Link a GitHub repository in Settings > Integrations to view annotated source code.'
    } else if (status === 404) {
      message = `File "${filename}" not found in the repository${serviceVersion ? ` at ref "${serviceVersion}"` : ''}.`
    }

    sourceView.value.error = message
    sourceView.value.loading = false
  }
}

const closeSourceView = () => {
  sourceView.value.visible = false
}

// Formatting helpers
const formatNumber = (n) => {
  if (n == null) return '--'
  return n.toLocaleString()
}

const formatDuration = (nanos) => {
  if (nanos == null) return '--'
  const ms = nanos / 1_000_000
  if (ms < 1000) return `${ms.toFixed(1)}ms`
  const sec = ms / 1000
  if (sec < 60) return `${sec.toFixed(1)}s`
  const min = sec / 60
  return `${min.toFixed(1)}m`
}

const formatTimestamp = (ts) => {
  if (!ts) return '--'
  const date = new Date(ts)
  return date.toLocaleString()
}

onMounted(async () => {
  await fetchProject()
  await fetchProfile()
})
</script>
