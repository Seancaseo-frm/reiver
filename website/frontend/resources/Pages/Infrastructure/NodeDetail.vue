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
            <h1 class="text-2xl font-bold text-gray-900">{{ nodeName }}</h1>
            <p class="text-sm text-gray-500">Node</p>
          </div>
          <span class="ml-3 px-2 py-0.5 text-xs font-medium bg-green-100 text-green-800 rounded">
            {{ data?.node?.status || 'Unknown' }}
          </span>
        </div>
      </div>

      <div v-if="loading" class="flex items-center justify-center py-12">
        <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full"></div>
        <span class="ml-3 text-gray-600">Loading node details...</span>
      </div>

      <div v-else class="space-y-6">
        <!-- Resource Summary -->
        <BaseCard v-if="data?.capacity">
          <template #header>
            <h3 class="text-lg font-semibold text-gray-900">Resource Summary</h3>
          </template>
          <div class="space-y-5">
            <!-- CPU -->
            <div>
              <div class="flex items-center justify-between text-sm mb-2">
                <span class="font-medium text-gray-700">CPU</span>
                <span class="text-gray-500">
                  {{ avgCpu > 0 ? (avgCpu * (data.capacity.cpuCores || 1)).toFixed(2) : '0' }} used /
                  {{ ((data.capacity.cpuCores || 1) - avgCpu * (data.capacity.cpuCores || 1)).toFixed(2) }} idle
                  of {{ data.capacity.cpuCores || '?' }} cores
                </span>
              </div>
              <div class="h-4 bg-gray-100 rounded-full overflow-hidden flex">
                <div
                  class="h-full bg-blue-500 transition-all"
                  :style="{ width: `${cpuUsedPercent}%` }"
                  :title="`Used: ${cpuUsedPercent.toFixed(1)}%`"
                ></div>
                <div
                  class="h-full bg-gray-300 transition-all"
                  :style="{ width: `${100 - cpuUsedPercent}%` }"
                  :title="`Idle: ${(100 - cpuUsedPercent).toFixed(1)}%`"
                ></div>
              </div>
              <div class="flex items-center gap-4 mt-1 text-xs text-gray-400">
                <span class="flex items-center gap-1"><span class="w-2 h-2 rounded-full bg-blue-500 inline-block"></span> Used</span>
                <span class="flex items-center gap-1"><span class="w-2 h-2 rounded-full bg-gray-300 inline-block"></span> Idle</span>
              </div>
            </div>

            <!-- Memory -->
            <div>
              <div class="flex items-center justify-between text-sm mb-2">
                <span class="font-medium text-gray-700">Memory</span>
                <span class="text-gray-500">
                  {{ formatBytes(data.capacity.memoryUsed) }} used /
                  {{ formatBytes((data.capacity.memoryTotal || 0) - (data.capacity.memoryUsed || 0)) }} idle
                  of {{ formatBytes(data.capacity.memoryTotal) }}
                </span>
              </div>
              <div class="h-4 bg-gray-100 rounded-full overflow-hidden flex">
                <div
                  class="h-full bg-purple-500 transition-all"
                  :style="{ width: `${memUsedPercent}%` }"
                ></div>
                <div
                  class="h-full bg-gray-300 transition-all"
                  :style="{ width: `${100 - memUsedPercent}%` }"
                ></div>
              </div>
              <div class="flex items-center gap-4 mt-1 text-xs text-gray-400">
                <span class="flex items-center gap-1"><span class="w-2 h-2 rounded-full bg-purple-500 inline-block"></span> Used</span>
                <span class="flex items-center gap-1"><span class="w-2 h-2 rounded-full bg-gray-300 inline-block"></span> Idle</span>
              </div>
            </div>

            <!-- Disk -->
            <div v-if="data.capacity.diskTotal > 0">
              <div class="flex items-center justify-between text-sm mb-2">
                <span class="font-medium text-gray-700">Disk</span>
                <span class="text-gray-500">
                  {{ formatBytes(data.capacity.diskUsed) }} used /
                  {{ formatBytes((data.capacity.diskTotal || 0) - (data.capacity.diskUsed || 0)) }} idle
                  of {{ formatBytes(data.capacity.diskTotal) }}
                </span>
              </div>
              <div class="h-4 bg-gray-100 rounded-full overflow-hidden flex">
                <div
                  class="h-full bg-amber-500 transition-all"
                  :style="{ width: `${diskUsedPercent}%` }"
                ></div>
                <div
                  class="h-full bg-gray-300 transition-all"
                  :style="{ width: `${100 - diskUsedPercent}%` }"
                ></div>
              </div>
              <div class="flex items-center gap-4 mt-1 text-xs text-gray-400">
                <span class="flex items-center gap-1"><span class="w-2 h-2 rounded-full bg-amber-500 inline-block"></span> Used</span>
                <span class="flex items-center gap-1"><span class="w-2 h-2 rounded-full bg-gray-300 inline-block"></span> Idle</span>
              </div>
            </div>
          </div>
        </BaseCard>

        <!-- CPU, Memory & Disk Charts -->
        <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
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
                  :title="`${point.timestamp}: ${(point.cpu * 100).toFixed(1)}%`"
                ></div>
              </div>
              <div class="text-xs text-gray-500 mt-2 text-center">
                Avg: {{ (avgCpu * 100).toFixed(1) }}% CPU
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

          <BaseCard>
            <template #header>
              <h3 class="text-lg font-semibold text-gray-900">Disk Usage</h3>
            </template>
            <div v-if="hasDiskData" class="h-48">
              <div class="flex items-end gap-1 h-full">
                <div
                  v-for="(point, i) in data.timeseries"
                  :key="i"
                  class="flex-1 bg-amber-400 rounded-t min-h-[2px] transition-all"
                  :style="{ height: `${Math.max((point.disk / maxDisk) * 100, 2)}%` }"
                  :title="`${point.timestamp}: ${formatBytes(point.disk)}`"
                ></div>
              </div>
              <div class="text-xs text-gray-500 mt-2 text-center">
                Avg: {{ formatBytes(avgDisk) }}
              </div>
            </div>
            <div v-else class="text-center py-8 text-gray-400">No disk data</div>
          </BaseCard>
        </div>

        <!-- Pods on this Node -->
        <BaseCard v-if="data?.pods?.length">
          <template #header>
            <h3 class="text-lg font-semibold text-gray-900">Pods ({{ data.pods.length }})</h3>
          </template>
          <div class="overflow-x-auto">
            <table class="min-w-full divide-y divide-gray-200">
              <thead class="bg-gray-50">
                <tr>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">Pod</th>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">Namespace</th>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">CPU</th>
                  <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">Memory</th>
                </tr>
              </thead>
              <tbody class="bg-white divide-y divide-gray-200">
                <tr
                  v-for="pod in data.pods"
                  :key="pod.name"
                  class="hover:bg-gray-50 cursor-pointer"
                  @click="goToPod(pod)"
                >
                  <td class="px-4 py-3 text-sm font-medium text-gray-900">{{ pod.name }}</td>
                  <td class="px-4 py-3 text-sm text-gray-600">{{ pod.namespace }}</td>
                  <td class="px-4 py-3 text-sm text-gray-600">{{ (pod.cpu * 1000).toFixed(1) }}m</td>
                  <td class="px-4 py-3 text-sm text-gray-600">{{ formatBytes(pod.memory) }}</td>
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
const nodeName = computed(() => route.params.node)
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
const maxDisk = computed(() => {
  if (!data.value?.timeseries?.length) return 1
  return Math.max(...data.value.timeseries.map(p => p.disk || 0), 1)
})
const avgCpu = computed(() => {
  if (!data.value?.timeseries?.length) return 0
  return data.value.timeseries.reduce((s, p) => s + p.cpu, 0) / data.value.timeseries.length
})
const avgMemory = computed(() => {
  if (!data.value?.timeseries?.length) return 0
  return data.value.timeseries.reduce((s, p) => s + p.memory, 0) / data.value.timeseries.length
})
const avgDisk = computed(() => {
  if (!data.value?.timeseries?.length) return 0
  return data.value.timeseries.reduce((s, p) => s + (p.disk || 0), 0) / data.value.timeseries.length
})
const hasDiskData = computed(() => {
  return data.value?.timeseries?.some(p => p.disk > 0)
})

const cpuUsedPercent = computed(() => {
  const cap = data.value?.capacity
  if (!cap?.cpuCores || !avgCpu.value) return 0
  return Math.min((avgCpu.value / 1) * 100, 100)
})
const memUsedPercent = computed(() => {
  const cap = data.value?.capacity
  if (!cap?.memoryTotal) return 0
  return Math.min(((cap.memoryUsed || 0) / cap.memoryTotal) * 100, 100)
})
const diskUsedPercent = computed(() => {
  const cap = data.value?.capacity
  if (!cap?.diskTotal) return 0
  return Math.min(((cap.diskUsed || 0) / cap.diskTotal) * 100, 100)
})

const goBack = () => router.push(`/p/${projectId.value}/infrastructure`)
const goToPod = (pod) => router.push(`/p/${projectId.value}/infrastructure/pods/${pod.namespace}/${pod.name}`)

const formatBytes = (bytes) => {
  if (!bytes) return '0 B'
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(Math.abs(bytes)) / Math.log(1024))
  return `${(bytes / Math.pow(1024, i)).toFixed(1)} ${sizes[i]}`
}

onMounted(async () => {
  try {
    const [projectRes, detailRes] = await Promise.all([
      axios.get(`/api/projects/${projectId.value}`),
      axios.get(`/api/projects/${projectId.value}/infra/nodes/${nodeName.value}`, {
        params: { time_range: '1h' },
      }),
    ])
    currentProject.value = projectRes.data
    data.value = detailRes.data
  } catch (error) {
    console.error('Failed to fetch node details:', error)
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
