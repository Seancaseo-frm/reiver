<template>
  <AppLayout :user="user" :current-project="currentProject">
    <div class="max-w-full mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <div class="mb-6">
        <router-link
          :to="`/p/${projectId}/profiles`"
          class="text-primary-600 hover:text-primary-700 text-sm font-medium mb-2 inline-block"
        >
          &larr; Back to Profiles
        </router-link>
        <h1 class="text-2xl font-bold text-gray-900">Compare Versions</h1>
      </div>

      <!-- Controls -->
      <div class="bg-white border border-gray-200 rounded-lg p-4 mb-6">
        <div class="flex items-end gap-4 flex-wrap">
          <div class="w-48">
            <label class="block text-sm font-medium text-gray-700 mb-1">Service</label>
            <select v-model="service" class="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm bg-white text-gray-900 text-sm">
              <option value="">Select service</option>
              <option v-for="s in availableServices" :key="s" :value="s">{{ s }}</option>
            </select>
          </div>
          <div class="w-48">
            <label class="block text-sm font-medium text-gray-700 mb-1">Baseline (A)</label>
            <select v-model="versionA" :disabled="!versions.length" class="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm bg-white text-gray-900 text-sm disabled:opacity-50">
              <option value="">Select version</option>
              <option v-for="v in versions" :key="'a-' + v.version" :value="v.version">
                {{ v.version }} ({{ v.profile_count }} profiles)
              </option>
            </select>
          </div>
          <div class="w-48">
            <label class="block text-sm font-medium text-gray-700 mb-1">Target (B)</label>
            <select v-model="versionB" :disabled="!versions.length" class="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm bg-white text-gray-900 text-sm disabled:opacity-50">
              <option value="">Select version</option>
              <option v-for="v in versions" :key="'b-' + v.version" :value="v.version">
                {{ v.version }} ({{ v.profile_count }} profiles)
              </option>
            </select>
          </div>
          <div class="w-36">
            <label class="block text-sm font-medium text-gray-700 mb-1">Time Range</label>
            <select v-model="timeRange" class="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm bg-white text-gray-900 text-sm">
              <option value="1d">Last 1 day</option>
              <option value="3d">Last 3 days</option>
              <option value="7d">Last 7 days</option>
              <option value="30d">Last 30 days</option>
            </select>
          </div>
          <button
            @click="runComparison"
            :disabled="!versionA || !versionB || versionA === versionB || diffLoading"
            class="inline-flex items-center gap-2 px-4 py-2 text-sm font-medium rounded-md bg-primary-600 text-white hover:bg-primary-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
          >
            <svg v-if="diffLoading" class="animate-spin w-4 h-4" fill="none" viewBox="0 0 24 24">
              <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
              <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
            </svg>
            Compare
          </button>
        </div>
      </div>

      <!-- Stats Comparison -->
      <div v-if="statsComparison" class="bg-white border border-gray-200 rounded-lg p-4 mb-6">
        <h2 class="text-sm font-medium text-gray-700 mb-3">Statistics Comparison</h2>
        <div class="grid grid-cols-3 gap-4 text-sm">
          <div>
            <p class="text-gray-500">Profiles</p>
            <p class="font-mono">A: {{ formatNumber(statsComparison.version1?.profile_count) }} &rarr; B: {{ formatNumber(statsComparison.version2?.profile_count) }}</p>
          </div>
          <div>
            <p class="text-gray-500">Total Samples</p>
            <p class="font-mono">A: {{ formatNumber(statsComparison.version1?.total_samples) }} &rarr; B: {{ formatNumber(statsComparison.version2?.total_samples) }}</p>
          </div>
          <div>
            <p class="text-gray-500">Avg Duration</p>
            <p class="font-mono">
              A: {{ formatDuration(statsComparison.version1?.avg_duration_nano) }} &rarr;
              B: {{ formatDuration(statsComparison.version2?.avg_duration_nano) }}
              <span :class="(statsComparison.diff?.avg_duration_pct_change || 0) > 0 ? 'text-red-600' : 'text-green-600'">
                ({{ (statsComparison.diff?.avg_duration_pct_change || 0) > 0 ? '+' : '' }}{{ (statsComparison.diff?.avg_duration_pct_change || 0).toFixed(1) }}%)
              </span>
            </p>
          </div>
        </div>
      </div>

      <!-- Diff Flamegraph -->
      <div v-if="diffFlameGraph" class="border border-gray-200 rounded-lg overflow-hidden">
        <ProfileDiffFlamegraph :diffFlameGraph="diffFlameGraph" />
      </div>

      <div v-if="diffError" class="text-center py-12 text-red-500">
        <p>{{ diffError }}</p>
      </div>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import { useRoute } from 'vue-router'
import { useAuth } from '@/composables/useAuth'
import AppLayout from '@/Layouts/AppLayout.vue'
import ProfileDiffFlamegraph from '@/components/ProfileDiffFlamegraph.vue'
import axios from 'axios'

const route = useRoute()
const { user } = useAuth()

const projectId = computed(() => route.params.id)
const currentProject = ref(null)

const service = ref(route.query.service || '')
const versionA = ref('')
const versionB = ref('')
const timeRange = ref('7d')
const availableServices = ref([])
const versions = ref([])

const diffLoading = ref(false)
const diffFlameGraph = ref(null)
const statsComparison = ref(null)
const diffError = ref(null)

const getTimeRange = () => {
  const now = new Date()
  const map = { '1d': 1, '3d': 3, '7d': 7, '30d': 30 }
  const days = map[timeRange.value] || 7
  return {
    start_time: new Date(now.getTime() - days * 24 * 60 * 60 * 1000).toISOString(),
    end_time: now.toISOString(),
  }
}

const fetchProject = async () => {
  try {
    const response = await axios.get(`/api/projects/${projectId.value}`)
    currentProject.value = response.data
  } catch (e) { console.error(e) }
}

const fetchServices = async () => {
  try {
    const response = await axios.get(`/api/projects/${projectId.value}/services`)
    const services = response.data?.services || response.data || []
    availableServices.value = services.map(s => s.service_name || s.name || s).filter(Boolean)
  } catch (e) { console.error(e) }
}

const fetchVersions = async () => {
  if (!service.value) { versions.value = []; return }
  try {
    const { start_time, end_time } = getTimeRange()
    const response = await axios.get(
      `/api/profiles/projects/${projectId.value}/services/${encodeURIComponent(service.value)}/profiles/versions`,
      { params: { start_time, end_time } }
    )
    versions.value = response.data?.versions || []
  } catch (e) {
    console.error(e)
    versions.value = []
  }
}

const runComparison = async () => {
  diffLoading.value = true
  diffError.value = null
  diffFlameGraph.value = null
  statsComparison.value = null
  try {
    const { start_time, end_time } = getTimeRange()
    const response = await axios.get(
      `/api/profiles/projects/${projectId.value}/services/${encodeURIComponent(service.value)}/profiles/diff`,
      { params: { version1: versionA.value, version2: versionB.value, start_time, end_time } }
    )
    diffFlameGraph.value = response.data?.diff_flame_graph || null
    statsComparison.value = response.data?.stats_comparison || null
  } catch (e) {
    diffError.value = e.response?.data?.message || e.message || 'Comparison failed'
  } finally {
    diffLoading.value = false
  }
}

watch(service, () => {
  versionA.value = ''
  versionB.value = ''
  diffFlameGraph.value = null
  statsComparison.value = null
  fetchVersions()
})

watch(timeRange, () => fetchVersions())

const formatNumber = (n) => n != null ? n.toLocaleString() : '--'
const formatDuration = (nanos) => {
  if (nanos == null) return '--'
  const ms = nanos / 1_000_000
  if (ms < 1000) return `${ms.toFixed(1)}ms`
  return `${(ms / 1000).toFixed(1)}s`
}

onMounted(async () => {
  await fetchProject()
  await fetchServices()
  if (service.value) await fetchVersions()
})
</script>
