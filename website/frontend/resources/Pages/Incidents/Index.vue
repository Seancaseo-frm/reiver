<template>
  <AppLayout :user="user" :current-project="currentProject">
    <div class="incidents-page max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
      <div class="mb-6 flex flex-wrap items-center justify-between gap-4">
        <h1 class="text-2xl font-bold text-gray-900">Incidents</h1>
        <div class="flex items-center gap-2">
          <select
            v-model="timeRange"
            @change="applyTimeRange"
            class="text-sm border border-gray-300 rounded-md px-3 py-2 bg-white text-gray-900 focus:ring-2 focus:ring-primary-500"
          >
            <option value="1h">Last 1 hour</option>
            <option value="24h">Last 24 hours</option>
            <option value="7d">Last 7 days</option>
            <option value="30d">Last 30 days</option>
          </select>
          <button
            @click="load"
            :disabled="loading"
            class="px-3 py-2 text-sm font-medium rounded-md bg-primary-600 text-white hover:bg-primary-700 disabled:opacity-50"
          >
            Refresh
          </button>
        </div>
      </div>

      <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <!-- Exceptions -->
        <BaseCard>
          <template #header>
            <h2 class="text-lg font-semibold text-gray-900">Exceptions</h2>
          </template>
          <div v-if="exceptionsLoading" class="py-8 text-center text-gray-500">Loading...</div>
          <div v-else-if="exceptions.length === 0" class="py-8 text-center text-gray-500">
            No exceptions in this period.
          </div>
          <ul v-else class="divide-y divide-gray-200">
            <li
              v-for="ex in exceptions"
              :key="ex.id"
              @click="openException(ex)"
              class="py-3 px-2 -mx-2 rounded cursor-pointer hover:bg-gray-50 transition-colors"
            >
              <div class="font-medium text-gray-900 truncate" :title="ex.message">{{ ex.message || ex.exception_type || ex.fingerprint }}</div>
              <div class="text-sm text-gray-500 mt-0.5">
                {{ ex.count }} · {{ formatDate(ex.last_seen) }}
              </div>
            </li>
          </ul>
        </BaseCard>

        <!-- OTLP Errors -->
        <BaseCard>
          <template #header>
            <h2 class="text-lg font-semibold text-gray-900">Errors (OTLP)</h2>
          </template>
          <div v-if="errorsLoading" class="py-8 text-center text-gray-500">Loading...</div>
          <div v-else-if="otlpErrors.length === 0" class="py-8 text-center text-gray-500">
            No error-level logs in this period.
          </div>
          <ul v-else class="divide-y divide-gray-200">
            <li
              v-for="err in otlpErrors"
              :key="err.id"
              @click="openOtlpError(err)"
              class="py-3 px-2 -mx-2 rounded cursor-pointer hover:bg-gray-50 transition-colors"
            >
              <div class="font-mono text-sm text-gray-900 truncate" :title="err.template">{{ err.template || '—' }}</div>
              <div class="text-sm text-gray-500 mt-0.5">
                {{ err.count }} · {{ err.service_name }} · {{ formatDate(err.last_seen) }}
              </div>
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
import axios from 'axios'

const route = useRoute()
const router = useRouter()
const { user } = useAuth()

const projectId = computed(() => route.params.id)
const currentProject = ref(null)

const timeRange = ref('24h')
const loading = ref(false)
const exceptions = ref([])
const exceptionsLoading = ref(false)
const otlpErrors = ref([])
const errorsLoading = ref(false)

const fetchProject = async () => {
  try {
    const response = await axios.get(`/api/projects/${projectId.value}`)
    currentProject.value = response.data
  } catch (error) {
    console.error('Failed to fetch project:', error)
  }
}

function timeRangeToMs() {
  const end = Date.now()
  const m = { '1h': 3600e3, '24h': 24 * 3600e3, '7d': 7 * 24 * 3600e3, '30d': 30 * 24 * 3600e3 }
  const start = end - (m[timeRange.value] || m['24h'])
  return { start_ms: start, end_ms: end }
}

function formatDate(v) {
  if (!v) return '—'
  const d = new Date(v)
  return d.toLocaleString(undefined, { dateStyle: 'short', timeStyle: 'short' })
}

async function loadExceptions() {
  exceptionsLoading.value = true
  try {
    const { start_ms, end_ms } = timeRangeToMs()
    const { data } = await axios.get(`/api/projects/${projectId.value}/incidents/exceptions`, { params: { start_ms, end_ms } })
    exceptions.value = data
  } catch (e) {
    console.warn('Incidents exceptions:', e)
    exceptions.value = []
  } finally {
    exceptionsLoading.value = false
  }
}

async function loadErrors() {
  errorsLoading.value = true
  try {
    const { start_ms, end_ms } = timeRangeToMs()
    const { data } = await axios.get(`/api/projects/${projectId.value}/incidents/errors`, { params: { start_ms, end_ms } })
    otlpErrors.value = data
  } catch (e) {
    console.warn('Incidents errors:', e)
    otlpErrors.value = []
  } finally {
    errorsLoading.value = false
  }
}

async function load() {
  loading.value = true
  await Promise.all([loadExceptions(), loadErrors()])
  loading.value = false
}

function applyTimeRange() {
  load()
}

function openException(ex) {
  router.push(`/p/${projectId.value}/incidents/exceptions/${ex.id}`)
}

function openOtlpError(err) {
  // For now, still use the separate error detail page since the unified one isn't fully implemented for OTLP errors
  const pad = 15 * 60 * 1000
  const start = (err.first_seen ? new Date(err.first_seen).getTime() : Date.now() - 3600000) - pad
  const end = (err.last_seen ? new Date(err.last_seen).getTime() : Date.now()) + pad
  const q = new URLSearchParams({
    start_ms: String(start),
    end_ms: String(end),
    template: err.template ? err.template.slice(0, 500) : '',
    level: err.level || '',
    service_name: err.service_name || '',
    source: err.source || '',
    count: String(err.count || 0),
  })
  router.push(`/p/${projectId.value}/incidents/errors?${q.toString()}`)
}

onMounted(async () => {
  await fetchProject()
  load()
})
</script>
