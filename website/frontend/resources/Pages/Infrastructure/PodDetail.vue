<template>
  <AppLayout :user="user" :current-project="currentProject">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <div class="mb-6">
        <div class="flex items-center gap-3 mb-2">
          <button @click="goBack" class="text-gray-500 hover:text-gray-700">
            <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7" />
            </svg>
          </button>
          <div>
            <h1 class="text-2xl font-bold text-gray-900">{{ podName }}</h1>
            <p class="text-sm text-gray-500">Namespace: {{ namespace }}</p>
          </div>
          <span class="ml-3 px-2 py-0.5 text-xs font-medium bg-green-100 text-green-800 rounded">
            {{ data?.pod?.status || 'Unknown' }}
          </span>
        </div>
      </div>

      <div v-if="loading" class="flex items-center justify-center py-12">
        <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full"></div>
        <span class="ml-3 text-gray-600">Loading pod details...</span>
      </div>

      <div v-else class="space-y-6">
        <!-- CPU & Memory Charts -->
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <BaseCard>
            <template #header>
              <h3 class="text-lg font-semibold text-gray-900">CPU Usage</h3>
            </template>
            <div v-if="data?.timeseries?.length" class="h-48">
              <div class="flex items-end gap-1 h-full">
                <div
                  v-for="(point, i) in data.timeseries"
                  :key="i"
                  class="flex-1 bg-blue-400 rounded-t min-h-[2px] transition-all"
                  :style="{ height: `${Math.max((point.cpu / maxCpu) * 100, 2)}%` }"
                  :title="`${point.timestamp}: ${(point.cpu * 1000).toFixed(1)}m`"
                ></div>
              </div>
              <div class="text-xs text-gray-500 mt-2 text-center">
                Avg: {{ (avgCpu * 1000).toFixed(1) }}m CPU
              </div>
            </div>
            <div v-else class="text-center py-8 text-gray-400">No CPU data</div>
          </BaseCard>

          <BaseCard>
            <template #header>
              <h3 class="text-lg font-semibold text-gray-900">Memory Usage</h3>
            </template>
            <div v-if="data?.timeseries?.length" class="h-48">
              <div class="flex items-end gap-1 h-full">
                <div
                  v-for="(point, i) in data.timeseries"
                  :key="i"
                  class="flex-1 bg-purple-400 rounded-t min-h-[2px] transition-all"
                  :style="{ height: `${Math.max((point.memory / maxMemory) * 100, 2)}%` }"
                  :title="`${point.timestamp}: ${formatBytes(point.memory)}`"
                ></div>
              </div>
              <div class="text-xs text-gray-500 mt-2 text-center">
                Avg: {{ formatBytes(avgMemory) }}
              </div>
            </div>
            <div v-else class="text-center py-8 text-gray-400">No memory data</div>
          </BaseCard>
        </div>

        <!-- Containers -->
        <BaseCard v-if="data?.containers?.length">
          <template #header>
            <h3 class="text-lg font-semibold text-gray-900">Containers</h3>
          </template>
          <div class="overflow-x-auto">
            <table class="min-w-full divide-y divide-gray-200">
              <thead class="bg-gray-50">
                <tr>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">Name</th>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">CPU</th>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">Memory</th>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">Restarts</th>
                </tr>
              </thead>
              <tbody class="bg-white divide-y divide-gray-200">
                <tr v-for="container in data.containers" :key="container.name" class="hover:bg-gray-50">
                  <td class="px-4 py-3 text-sm font-medium text-gray-900">{{ container.name }}</td>
                  <td class="px-4 py-3 text-sm text-gray-600">{{ (container.cpu * 1000).toFixed(1) }}m</td>
                  <td class="px-4 py-3 text-sm text-gray-600">{{ formatBytes(container.memory) }}</td>
                  <td class="px-4 py-3 text-sm" :class="container.restarts > 0 ? 'text-yellow-600' : 'text-gray-600'">
                    {{ container.restarts }}
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </BaseCard>

        <!-- Raw Metrics -->
        <BaseCard v-if="data?.metrics?.length">
          <template #header>
            <h3 class="text-lg font-semibold text-gray-900">All Metrics</h3>
          </template>
          <div class="overflow-x-auto">
            <table class="min-w-full divide-y divide-gray-200">
              <thead class="bg-gray-50">
                <tr>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">Metric</th>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">Avg</th>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">Min</th>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">Max</th>
                </tr>
              </thead>
              <tbody class="bg-white divide-y divide-gray-200">
                <tr v-for="m in data.metrics" :key="m.metric_name" class="hover:bg-gray-50">
                  <td class="px-4 py-3 text-sm font-mono text-gray-900">{{ m.metric_name }}</td>
                  <td class="px-4 py-3 text-sm text-gray-600">{{ formatMetricValue(m.avg_val) }}</td>
                  <td class="px-4 py-3 text-sm text-gray-600">{{ formatMetricValue(m.min_val) }}</td>
                  <td class="px-4 py-3 text-sm text-gray-600">{{ formatMetricValue(m.max_val) }}</td>
                </tr>
              </tbody>
            </table>
          </div>
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
const namespace = computed(() => route.params.namespace)
const podName = computed(() => route.params.pod)
const currentProject = ref(null)
const loading = ref(true)
const data = ref(null)

const maxCpu = computed(() => {
  if (!data.value?.timeseries?.length) return 1
  return Math.max(...data.value.timeseries.map(p => p.cpu), 0.001)
})
const maxMemory = computed(() => {
  if (!data.value?.timeseries?.length) return 1
  return Math.max(...data.value.timeseries.map(p => p.memory), 1)
})
const avgCpu = computed(() => {
  if (!data.value?.timeseries?.length) return 0
  return data.value.timeseries.reduce((s, p) => s + p.cpu, 0) / data.value.timeseries.length
})
const avgMemory = computed(() => {
  if (!data.value?.timeseries?.length) return 0
  return data.value.timeseries.reduce((s, p) => s + p.memory, 0) / data.value.timeseries.length
})

const goBack = () => router.push(`/p/${projectId.value}/infrastructure`)

const formatBytes = (bytes) => {
  if (!bytes) return '0 B'
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(Math.abs(bytes)) / Math.log(1024))
  return `${(bytes / Math.pow(1024, i)).toFixed(1)} ${sizes[i]}`
}

const formatMetricValue = (val) => {
  if (val === null || val === undefined) return '-'
  const n = Number(val)
  if (Math.abs(n) >= 1e6) return `${(n / 1e6).toFixed(2)}M`
  if (Math.abs(n) >= 1e3) return `${(n / 1e3).toFixed(2)}K`
  if (Number.isInteger(n)) return n.toString()
  return n.toFixed(4)
}

onMounted(async () => {
  try {
    const [projectRes, detailRes] = await Promise.all([
      axios.get(`/api/projects/${projectId.value}`),
      axios.get(`/api/projects/${projectId.value}/infra/pods/${namespace.value}/${podName.value}`, {
        params: { time_range: '1h' },
      }),
    ])
    currentProject.value = projectRes.data
    data.value = detailRes.data
  } catch (error) {
    console.error('Failed to fetch pod details:', error)
  } finally {
    loading.value = false
  }
})
</script>

<style scoped>
.spinner {
  animation: spin 0.6s linear infinite;
}
@keyframes spin {
  to { transform: rotate(360deg); }
}
</style>
