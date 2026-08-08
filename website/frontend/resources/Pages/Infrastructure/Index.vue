<template>
  <AppLayout :user="user" :current-project="currentProject">
    <div class="infra-page max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <!-- Header -->
      <div class="mb-6">
        <div class="flex items-center justify-between">
          <div>
            <h1 class="text-2xl font-bold text-gray-900">Infrastructure Monitoring</h1>
            <p class="text-sm text-gray-500 mt-1">
              Monitor your Kubernetes cluster resources
            </p>
          </div>
          <div class="flex items-center gap-3">
            <select v-model="selectedCluster" @change="refreshData" class="cluster-select">
              <option value="">All Clusters</option>
              <option v-for="cluster in clusters" :key="cluster" :value="cluster">
                {{ cluster }}
              </option>
            </select>
            <select v-model="selectedNamespace" @change="refreshData" class="namespace-select">
              <option value="">All Namespaces</option>
              <option v-for="ns in namespaces" :key="ns" :value="ns">
                {{ ns }}
              </option>
            </select>
            <select v-model="timeRange" @change="onTimeRangeChange" class="time-select">
              <option value="live">Live</option>
              <option value="15m">Last 15 minutes</option>
              <option value="1h">Last 1 hour</option>
              <option value="6h">Last 6 hours</option>
              <option value="24h">Last 24 hours</option>
            </select>
            <span v-if="timeRange === 'live'" class="inline-flex items-center gap-1.5 text-xs text-green-600 font-medium">
              <span class="relative flex h-2 w-2">
                <span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span>
                <span class="relative inline-flex rounded-full h-2 w-2 bg-green-500"></span>
              </span>
              Auto-refreshing
            </span>
          </div>
        </div>
      </div>

      <!-- Resource Tabs -->
      <div class="resource-tabs mb-6">
        <button
          v-for="tab in resourceTabs"
          :key="tab.id"
          @click="activeTab = tab.id"
          :class="['tab-btn', { active: activeTab === tab.id }]"
        >
          <component :is="tab.icon" class="w-4 h-4 mr-2" />
          {{ tab.label }}
          <span v-if="tab.count !== undefined" class="tab-count">{{ tab.count }}</span>
        </button>
      </div>

      <!-- Initial Loading State -->
      <div v-if="initialLoading" class="flex items-center justify-center py-12">
        <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full"></div>
        <span class="ml-3 text-gray-600">Loading resources...</span>
      </div>

      <div v-else class="space-y-6">
        <!-- Cluster Overview -->
        <div v-if="activeTab === 'overview'" class="space-y-6">
          <!-- Summary Cards -->
          <div class="grid grid-cols-1 md:grid-cols-4 gap-4">
            <BaseCard>
              <div class="flex items-center gap-3">
                <div class="p-2 bg-blue-100 rounded-lg">
                  <svg class="w-6 h-6 text-blue-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 00-2 2v4a2 2 0 002 2h14a2 2 0 002-2v-4a2 2 0 00-2-2m-2-4h.01M17 16h.01" />
                  </svg>
                </div>
                <div>
                  <div class="text-sm text-gray-500">Nodes</div>
                  <div class="text-2xl font-bold text-gray-900">{{ summary.nodes }}</div>
                </div>
              </div>
            </BaseCard>
            <BaseCard>
              <div class="flex items-center gap-3">
                <div class="p-2 bg-green-100 rounded-lg">
                  <svg class="w-6 h-6 text-green-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M20 7l-8-4-8 4m16 0l-8 4m8-4v10l-8 4m0-10L4 7m8 4v10M4 7v10l8 4" />
                  </svg>
                </div>
                <div>
                  <div class="text-sm text-gray-500">Pods</div>
                  <div class="text-2xl font-bold text-gray-900">
                    {{ summary.runningPods }}/{{ summary.totalPods }}
                  </div>
                </div>
              </div>
            </BaseCard>
            <BaseCard>
              <div class="flex items-center gap-3">
                <div class="p-2 bg-purple-100 rounded-lg">
                  <svg class="w-6 h-6 text-purple-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4" />
                  </svg>
                </div>
                <div>
                  <div class="text-sm text-gray-500">Deployments</div>
                  <div class="text-2xl font-bold text-gray-900">{{ summary.deployments }}</div>
                </div>
              </div>
            </BaseCard>
            <BaseCard>
              <div class="flex items-center gap-3">
                <div class="p-2 bg-yellow-100 rounded-lg">
                  <svg class="w-6 h-6 text-yellow-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                  </svg>
                </div>
                <div>
                  <div class="text-sm text-gray-500">Alerts</div>
                  <div class="text-2xl font-bold text-yellow-600">{{ summary.alerts }}</div>
                </div>
              </div>
            </BaseCard>
          </div>

          <!-- Resource Usage -->
          <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <BaseCard>
              <template #header>
                <h3 class="text-lg font-semibold text-gray-900">CPU Usage</h3>
              </template>
              <ResourceGauge
                :value="summary.cpuUsage"
                :max="100"
                unit="%"
                label="Cluster CPU"
                :thresholds="[70, 85]"
              />
            </BaseCard>
            <BaseCard>
              <template #header>
                <h3 class="text-lg font-semibold text-gray-900">Memory Usage</h3>
              </template>
              <ResourceGauge
                :value="summary.memoryUsage"
                :max="100"
                unit="%"
                label="Cluster Memory"
                :thresholds="[70, 85]"
              />
            </BaseCard>
          </div>
        </div>

        <!-- Pods Tab -->
        <div v-if="activeTab === 'pods'">
          <BaseCard>
            <template #header>
              <div class="flex items-center justify-between">
                <h2 class="text-lg font-semibold text-gray-900">Pods</h2>
                <input
                  v-model="podSearch"
                  type="text"
                  placeholder="Search pods..."
                  class="search-input"
                />
              </div>
            </template>
            <div class="overflow-x-auto">
              <table class="min-w-full divide-y divide-gray-200">
                <thead class="bg-gray-50">
                  <tr>
                    <th v-for="col in podColumns" :key="col.key"
                        class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase cursor-pointer select-none hover:text-gray-900 group"
                        @click="togglePodSort(col.key)"
                    >
                      <span class="inline-flex items-center gap-1">
                        {{ col.label }}
                        <svg v-if="podSortKey === col.key" class="w-3 h-3 text-gray-900" :class="{ 'rotate-180': podSortDir === 'desc' }" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 15l7-7 7 7" />
                        </svg>
                        <svg v-else class="w-3 h-3 text-gray-300 opacity-0 group-hover:opacity-100" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 16V4m0 0L3 8m4-4l4 4m6 0v12m0 0l4-4m-4 4l-4-4" />
                        </svg>
                      </span>
                    </th>
                  </tr>
                </thead>
                <tbody class="bg-white divide-y divide-gray-200">
                  <tr
                    v-for="pod in filteredPods"
                    :key="pod.name"
                    class="hover:bg-gray-50 cursor-pointer"
                    @click="showPodDetail(pod)"
                  >
                    <td class="px-4 py-3">
                      <div class="flex items-center gap-2">
                        <span :class="['status-dot', getPodStatusClass(pod.status)]"></span>
                        <span class="text-sm font-medium text-gray-900">{{ pod.name }}</span>
                      </div>
                    </td>
                    <td class="px-4 py-3">
                      <span class="text-sm text-gray-600">{{ pod.namespace }}</span>
                    </td>
                    <td class="px-4 py-3">
                      <span :class="['status-badge', getPodStatusBadgeClass(pod.status)]">
                        {{ pod.status }}
                      </span>
                    </td>
                    <td class="px-4 py-3">
                      <span class="text-sm text-gray-900">
                        {{ pod.readyContainers }}/{{ pod.totalContainers }}
                      </span>
                    </td>
                    <td class="px-4 py-3">
                      <span :class="['text-sm', pod.restarts > 0 ? 'text-yellow-600' : 'text-gray-600']">
                        {{ pod.restarts }}
                      </span>
                    </td>
                    <td class="px-4 py-3">
                      <div class="flex items-center gap-2">
                        <div class="w-16 h-2 bg-gray-200 rounded-full overflow-hidden">
                          <div
                            :class="['h-full rounded-full', getUsageClass(pod.cpuPercent)]"
                            :style="{ width: `${pod.cpuPercent}%` }"
                          ></div>
                        </div>
                        <span class="text-xs text-gray-500">{{ pod.cpuPercent }}%</span>
                      </div>
                    </td>
                    <td class="px-4 py-3">
                      <div class="flex items-center gap-2">
                        <div class="w-16 h-2 bg-gray-200 rounded-full overflow-hidden">
                          <div
                            :class="['h-full rounded-full', getUsageClass(pod.memoryPercent)]"
                            :style="{ width: `${pod.memoryPercent}%` }"
                          ></div>
                        </div>
                        <span class="text-xs text-gray-500">{{ pod.memoryPercent }}%</span>
                      </div>
                    </td>
                    <td class="px-4 py-3">
                      <span class="text-sm text-gray-600">{{ formatAge(pod.createdAt) }}</span>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </BaseCard>
        </div>

        <!-- Nodes Tab -->
        <div v-if="activeTab === 'nodes'">
          <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            <BaseCard
              v-for="node in nodes"
              :key="node.name"
              class="cursor-pointer hover:border-primary-500 transition-colors"
              @click="showNodeDetail(node)"
            >
              <div class="flex items-center justify-between mb-4">
                <div class="flex items-center gap-2">
                  <span :class="['status-dot', getNodeStatusClass(node.status)]"></span>
                  <span class="font-medium text-gray-900">{{ node.name }}</span>
                </div>
                <span :class="['status-badge', getNodeStatusBadgeClass(node.status)]">
                  {{ node.status }}
                </span>
              </div>
              
              <div class="space-y-3">
                <div>
                  <div class="flex items-center justify-between text-xs text-gray-500 mb-1">
                    <span>CPU</span>
                    <span>{{ node.cpuUsed?.toFixed(1) }} / {{ node.cpuCores || node.cpuTotal }} cores</span>
                  </div>
                  <div class="h-2 bg-gray-200 rounded-full overflow-hidden">
                    <div
                      :class="['h-full rounded-full', getUsageClass(node.cpuPercent)]"
                      :style="{ width: `${Math.min(node.cpuPercent, 100)}%` }"
                    ></div>
                  </div>
                </div>
                
                <div>
                  <div class="flex items-center justify-between text-xs text-gray-500 mb-1">
                    <span>Memory</span>
                    <span>{{ formatBytes(node.memoryUsed) }} / {{ formatBytes(node.memoryTotal) }}</span>
                  </div>
                  <div class="h-2 bg-gray-200 rounded-full overflow-hidden">
                    <div
                      :class="['h-full rounded-full', getUsageClass(node.memoryPercent)]"
                      :style="{ width: `${Math.min(node.memoryPercent, 100)}%` }"
                    ></div>
                  </div>
                </div>

                <div v-if="node.diskTotal > 0">
                  <div class="flex items-center justify-between text-xs text-gray-500 mb-1">
                    <span>Disk</span>
                    <span>{{ formatBytes(node.diskUsed) }} / {{ formatBytes(node.diskTotal) }}</span>
                  </div>
                  <div class="h-2 bg-gray-200 rounded-full overflow-hidden">
                    <div
                      :class="['h-full rounded-full', getUsageClass(node.diskPercent)]"
                      :style="{ width: `${Math.min(node.diskPercent, 100)}%` }"
                    ></div>
                  </div>
                </div>
                
                <div class="flex items-center justify-between text-xs text-gray-500 pt-2 border-t border-gray-200">
                  <span>Pods: {{ node.podCount }}</span>
                  <span v-if="node.cpuCores">{{ node.cpuCores }} cores</span>
                </div>
              </div>
            </BaseCard>
          </div>
        </div>

        <!-- Deployments Tab -->
        <div v-if="activeTab === 'deployments'">
          <BaseCard>
            <template #header>
              <div class="flex items-center justify-between">
                <h2 class="text-lg font-semibold text-gray-900">Deployments</h2>
                <input
                  v-model="deploymentSearch"
                  type="text"
                  placeholder="Search deployments..."
                  class="search-input"
                />
              </div>
            </template>
            <div class="overflow-x-auto">
              <table class="min-w-full divide-y divide-gray-200">
                <thead class="bg-gray-50">
                  <tr>
                    <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                      Deployment
                    </th>
                    <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                      Namespace
                    </th>
                    <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                      Ready
                    </th>
                    <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                      Up-to-date
                    </th>
                    <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                      Available
                    </th>
                    <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                      Age
                    </th>
                  </tr>
                </thead>
                <tbody class="bg-white divide-y divide-gray-200">
                  <tr
                    v-for="deployment in filteredDeployments"
                    :key="deployment.name"
                    class="hover:bg-gray-50"
                  >
                    <td class="px-4 py-3">
                      <span class="text-sm font-medium text-gray-900">{{ deployment.name }}</span>
                    </td>
                    <td class="px-4 py-3">
                      <span class="text-sm text-gray-600">{{ deployment.namespace }}</span>
                    </td>
                    <td class="px-4 py-3">
                      <span :class="['text-sm font-medium', deployment.ready === deployment.desired ? 'text-green-600' : 'text-yellow-600']">
                        {{ deployment.ready }}/{{ deployment.desired }}
                      </span>
                    </td>
                    <td class="px-4 py-3">
                      <span class="text-sm text-gray-900">{{ deployment.upToDate }}</span>
                    </td>
                    <td class="px-4 py-3">
                      <span class="text-sm text-gray-900">{{ deployment.available }}</span>
                    </td>
                    <td class="px-4 py-3">
                      <span class="text-sm text-gray-600">{{ formatAge(deployment.createdAt) }}</span>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </BaseCard>
        </div>

        <!-- Services / Workloads Tab -->
        <div v-if="activeTab === 'services'">
          <BaseCard>
            <template #header>
              <h2 class="text-lg font-semibold text-gray-900">Workloads</h2>
            </template>
            <div class="overflow-x-auto">
              <table class="min-w-full divide-y divide-gray-200">
                <thead class="bg-gray-50">
                  <tr>
                    <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                      Name
                    </th>
                    <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                      Namespace
                    </th>
                    <th class="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase">
                      Kind
                    </th>
                  </tr>
                </thead>
                <tbody class="bg-white divide-y divide-gray-200">
                  <tr
                    v-for="svc in k8sServices"
                    :key="`${svc.type}-${svc.namespace}-${svc.name}`"
                    class="hover:bg-gray-50"
                  >
                    <td class="px-4 py-3">
                      <span class="text-sm font-medium text-gray-900">{{ svc.name }}</span>
                    </td>
                    <td class="px-4 py-3">
                      <span class="text-sm text-gray-600">{{ svc.namespace }}</span>
                    </td>
                    <td class="px-4 py-3">
                      <span :class="[
                        'px-2 py-0.5 text-xs font-medium rounded',
                        svc.type === 'Deployment' ? 'bg-blue-100 text-blue-700' :
                        svc.type === 'StatefulSet' ? 'bg-purple-100 text-purple-700' :
                        svc.type === 'DaemonSet' ? 'bg-orange-100 text-orange-700' :
                        'bg-gray-100 text-gray-700'
                      ]">
                        {{ svc.type }}
                      </span>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </BaseCard>
        </div>
      </div>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted, onBeforeUnmount, watch, h } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useAuth } from '@/composables/useAuth'
import { formatDistanceToNow } from 'date-fns'
import AppLayout from '@/Layouts/AppLayout.vue'
import BaseCard from '@/components/BaseCard.vue'
import ResourceGauge from '@/components/infra/ResourceGauge.vue'
import axios from 'axios'

const route = useRoute()
const router = useRouter()
const { user } = useAuth()

const projectId = computed(() => route.params.id)
const currentProject = ref(null)
const initialLoading = ref(true)
const timeRange = ref('1h')
const selectedCluster = ref('')
const selectedNamespace = ref('')
const activeTab = ref('overview')
const podSearch = ref('')
const deploymentSearch = ref('')
const podSortKey = ref('name')
const podSortDir = ref('asc')

// Data
const clusters = ref([])
const namespaces = ref([])
const pods = ref([])
const nodes = ref([])
const deployments = ref([])
const k8sServices = ref([])
const summary = ref({
  nodes: 0,
  totalPods: 0,
  runningPods: 0,
  deployments: 0,
  alerts: 0,
  cpuUsage: 0,
  memoryUsage: 0,
})

// Icons
const OverviewIcon = { render: () => h('svg', { fill: 'none', stroke: 'currentColor', viewBox: '0 0 24 24' }, [
  h('path', { 'stroke-linecap': 'round', 'stroke-linejoin': 'round', 'stroke-width': '2', d: 'M4 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2V6zM14 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2V6zM4 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2v-2zM14 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2v-2z' })
])}
const PodIcon = { render: () => h('svg', { fill: 'none', stroke: 'currentColor', viewBox: '0 0 24 24' }, [
  h('path', { 'stroke-linecap': 'round', 'stroke-linejoin': 'round', 'stroke-width': '2', d: 'M20 7l-8-4-8 4m16 0l-8 4m8-4v10l-8 4m0-10L4 7m8 4v10M4 7v10l8 4' })
])}
const NodeIcon = { render: () => h('svg', { fill: 'none', stroke: 'currentColor', viewBox: '0 0 24 24' }, [
  h('path', { 'stroke-linecap': 'round', 'stroke-linejoin': 'round', 'stroke-width': '2', d: 'M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 00-2 2v4a2 2 0 002 2h14a2 2 0 002-2v-4a2 2 0 00-2-2m-2-4h.01M17 16h.01' })
])}
const DeploymentIcon = { render: () => h('svg', { fill: 'none', stroke: 'currentColor', viewBox: '0 0 24 24' }, [
  h('path', { 'stroke-linecap': 'round', 'stroke-linejoin': 'round', 'stroke-width': '2', d: 'M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4' })
])}
const ServiceIcon = { render: () => h('svg', { fill: 'none', stroke: 'currentColor', viewBox: '0 0 24 24' }, [
  h('path', { 'stroke-linecap': 'round', 'stroke-linejoin': 'round', 'stroke-width': '2', d: 'M21 12a9 9 0 01-9 9m9-9a9 9 0 00-9-9m9 9H3m9 9a9 9 0 01-9-9m9 9c1.657 0 3-4.03 3-9s-1.343-9-3-9m0 18c-1.657 0-3-4.03-3-9s1.343-9 3-9m-9 9a9 9 0 019-9' })
])}

// Resource tabs
const resourceTabs = computed(() => [
  { id: 'overview', label: 'Overview', icon: OverviewIcon },
  { id: 'pods', label: 'Pods', icon: PodIcon, count: pods.value.length },
  { id: 'nodes', label: 'Nodes', icon: NodeIcon, count: nodes.value.length },
  { id: 'deployments', label: 'Deployments', icon: DeploymentIcon, count: deployments.value.length },
  { id: 'services', label: 'Workloads', icon: ServiceIcon, count: k8sServices.value.length },
])

// Pod table columns
const podColumns = [
  { key: 'name', label: 'Pod' },
  { key: 'namespace', label: 'Namespace' },
  { key: 'status', label: 'Status' },
  { key: 'readyContainers', label: 'Ready' },
  { key: 'restarts', label: 'Restarts' },
  { key: 'cpuPercent', label: 'CPU' },
  { key: 'memoryPercent', label: 'Memory' },
  { key: 'createdAt', label: 'Age' },
]

const togglePodSort = (key) => {
  if (podSortKey.value === key) {
    podSortDir.value = podSortDir.value === 'asc' ? 'desc' : 'asc'
  } else {
    podSortKey.value = key
    podSortDir.value = key === 'name' || key === 'namespace' || key === 'status' ? 'asc' : 'desc'
  }
}

// Filtered + sorted data
const filteredPods = computed(() => {
  let result = pods.value
  if (podSearch.value) {
    const query = podSearch.value.toLowerCase()
    result = result.filter(p =>
      p.name.toLowerCase().includes(query) ||
      p.namespace.toLowerCase().includes(query)
    )
  }
  const key = podSortKey.value
  const dir = podSortDir.value === 'asc' ? 1 : -1
  return [...result].sort((a, b) => {
    const av = a[key] ?? ''
    const bv = b[key] ?? ''
    if (typeof av === 'number' && typeof bv === 'number') return (av - bv) * dir
    return String(av).localeCompare(String(bv)) * dir
  })
})

const filteredDeployments = computed(() => {
  if (!deploymentSearch.value) return deployments.value
  const query = deploymentSearch.value.toLowerCase()
  return deployments.value.filter(d => 
    d.name.toLowerCase().includes(query) ||
    d.namespace.toLowerCase().includes(query)
  )
})

// API calls
const fetchProject = async () => {
  try {
    const response = await axios.get(`/api/projects/${projectId.value}`)
    currentProject.value = response.data
  } catch (error) {
    console.error('Failed to fetch project:', error)
  }
}

const fetchData = async () => {
  try {
    const params = { time_range: timeRange.value }
    if (selectedCluster.value) params.cluster = selectedCluster.value
    if (selectedNamespace.value) params.namespace = selectedNamespace.value

    const [summaryRes, podsRes, nodesRes, deploymentsRes, servicesRes] = await Promise.all([
      axios.get(`/api/projects/${projectId.value}/infra/summary`, { params }),
      axios.get(`/api/projects/${projectId.value}/infra/pods`, { params }),
      axios.get(`/api/projects/${projectId.value}/infra/nodes`, { params }),
      axios.get(`/api/projects/${projectId.value}/infra/deployments`, { params }),
      axios.get(`/api/projects/${projectId.value}/infra/services`, { params }),
    ])

    summary.value = summaryRes.data.summary || summary.value
    clusters.value = summaryRes.data.clusters || []
    namespaces.value = summaryRes.data.namespaces || []
    pods.value = podsRes.data.pods || []
    nodes.value = nodesRes.data.nodes || []
    deployments.value = deploymentsRes.data.deployments || []
    k8sServices.value = servicesRes.data.services || []
  } catch (error) {
    console.error('Failed to fetch infra data:', error)
  } finally {
    initialLoading.value = false
  }
}

const refreshData = () => {
  fetchData()
}

let liveInterval = null
const LIVE_POLL_MS = 15_000

const startLivePolling = () => {
  stopLivePolling()
  liveInterval = setInterval(() => fetchData(), LIVE_POLL_MS)
}

const stopLivePolling = () => {
  if (liveInterval) {
    clearInterval(liveInterval)
    liveInterval = null
  }
}

const onTimeRangeChange = () => {
  if (timeRange.value === 'live') {
    startLivePolling()
  } else {
    stopLivePolling()
  }
  fetchData()
}

onBeforeUnmount(() => {
  stopLivePolling()
})

const showPodDetail = (pod) => {
  router.push(`/p/${projectId.value}/infrastructure/pods/${pod.namespace}/${pod.name}`)
}

const showNodeDetail = (node) => {
  router.push(`/p/${projectId.value}/infrastructure/nodes/${node.name}`)
}

// Formatting
const formatAge = (dateString) => {
  if (!dateString) return 'Unknown'
  try {
    return formatDistanceToNow(new Date(dateString), { addSuffix: false })
  } catch {
    return dateString
  }
}

const formatBytes = (bytes) => {
  if (!bytes) return '0 B'
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(1024))
  return `${(bytes / Math.pow(1024, i)).toFixed(1)} ${sizes[i]}`
}

// Styling
const getPodStatusClass = (status) => {
  const classes = {
    Running: 'bg-green-500',
    Pending: 'bg-yellow-500',
    Failed: 'bg-red-500',
    Succeeded: 'bg-blue-500',
    Unknown: 'bg-gray-400',
  }
  return classes[status] || 'bg-gray-400'
}

const getPodStatusBadgeClass = (status) => {
  const classes = {
    Running: 'bg-green-100 text-green-800',
    Pending: 'bg-yellow-100 text-yellow-800',
    Failed: 'bg-red-100 text-red-800',
    Succeeded: 'bg-blue-100 text-blue-800',
    Unknown: 'bg-gray-100 text-gray-800',
  }
  return classes[status] || 'bg-gray-100 text-gray-800'
}

const getNodeStatusClass = (status) => {
  return status === 'Ready' ? 'bg-green-500' : 'bg-red-500'
}

const getNodeStatusBadgeClass = (status) => {
  return status === 'Ready'
    ? 'bg-green-100 text-green-800'
    : 'bg-red-100 text-red-800'
}

const getUsageClass = (percent) => {
  if (percent >= 85) return 'bg-red-500'
  if (percent >= 70) return 'bg-yellow-500'
  return 'bg-green-500'
}

onMounted(async () => {
  await fetchProject()
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

.cluster-select,
.namespace-select,
.time-select {
  @apply px-3 py-2 text-sm bg-white border border-gray-300 text-gray-900 rounded-md focus:ring-2 focus:ring-primary-500;
}

.resource-tabs {
  @apply flex items-center gap-1 bg-gray-100 rounded-lg p-1;
}

.tab-btn {
  @apply flex items-center px-4 py-2 text-sm font-medium text-gray-600 rounded-md transition-colors;
}

.tab-btn:hover {
  @apply text-gray-900;
}

.tab-btn.active {
  @apply bg-white text-gray-900 shadow-sm;
}

.tab-count {
  @apply ml-2 px-1.5 py-0.5 text-xs bg-gray-200 rounded-full;
}

.search-input {
  @apply px-3 py-2 text-sm bg-white border border-gray-300 text-gray-900 rounded-md focus:ring-2 focus:ring-primary-500 w-64;
}

.status-dot {
  @apply w-2 h-2 rounded-full;
}

.status-badge {
  @apply px-2 py-0.5 text-xs font-medium rounded;
}
</style>
