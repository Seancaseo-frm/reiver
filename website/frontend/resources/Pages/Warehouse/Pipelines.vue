<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-[1200px] mx-auto px-8 py-6">
      <!-- Header -->
      <div class="mb-6 flex items-center justify-between">
        <div>
          <h1 class="text-2xl font-semibold text-gray-900">Transformation Pipelines</h1>
          <p class="text-sm text-gray-500 dark:text-gray-400 mt-1">
            Create DAG-based data transformation workflows
          </p>
        </div>
        <router-link
          :to="`/p/${projectId}/warehouse/pipelines/new`"
          class="px-4 py-2 text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 rounded-lg transition-colors"
        >
          New Pipeline
        </router-link>
      </div>

      <!-- Loading -->
      <div v-if="loading" class="flex items-center justify-center py-32">
        <div class="spinner"></div>
      </div>

      <!-- Error -->
      <div v-else-if="error" class="flex flex-col items-center justify-center py-32 text-center">
        <h3 class="text-lg font-medium text-gray-900 dark:text-gray-100 mb-1">{{ error }}</h3>
        <button @click="loadPipelines" class="mt-4 px-4 py-2 text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 rounded-lg transition-colors">
          Retry
        </button>
      </div>

      <!-- Empty -->
      <div v-else-if="pipelines.length === 0" class="flex flex-col items-center justify-center py-32 text-center">
        <svg class="w-16 h-16 text-gray-300 dark:text-gray-600 mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1" />
        </svg>
        <h3 class="text-lg font-medium text-gray-900 dark:text-gray-100 mb-1">No pipelines yet</h3>
        <p class="text-sm text-gray-500 dark:text-gray-400 max-w-md mb-4">
          Create your first transformation pipeline to move and transform data between connectors.
        </p>
        <router-link
          :to="`/p/${projectId}/warehouse/pipelines/new`"
          class="px-4 py-2 text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 rounded-lg transition-colors"
        >
          Create Pipeline
        </router-link>
      </div>

      <!-- Pipeline Table -->
      <div v-else class="mt-4">
        <div class="overflow-hidden rounded-lg border border-gray-200 dark:border-gray-800">
          <table class="min-w-full divide-y divide-gray-200 dark:divide-gray-800">
            <thead class="bg-gray-50 dark:bg-gray-900/50">
              <tr>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Name</th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Mode</th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Schedule</th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Nodes</th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Status</th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Last Run</th>
                <th class="px-6 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Actions</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-gray-200 dark:divide-gray-800">
              <tr v-for="p in pipelines" :key="p.id" class="hover:bg-gray-50 dark:hover:bg-gray-900/30">
                <td class="px-6 py-4">
                  <router-link :to="`/p/${projectId}/warehouse/pipelines/${p.id}/edit`" class="text-sm font-medium text-primary-500 hover:text-primary-400">
                    {{ p.name }}
                  </router-link>
                  <p v-if="p.description" class="text-xs text-gray-500 mt-0.5">{{ p.description }}</p>
                </td>
                <td class="px-6 py-4">
                  <span :class="p.mode === 'streaming' ? 'mode-badge-streaming' : 'mode-badge-batch'">
                    {{ p.mode === 'streaming' ? 'Streaming' : 'Batch' }}
                  </span>
                </td>
                <td class="px-6 py-4 text-sm text-gray-500 dark:text-gray-400">
                  <code v-if="p.schedule" class="text-xs bg-gray-100 dark:bg-gray-800 px-1.5 py-0.5 rounded">{{ p.schedule }}</code>
                  <span v-else class="text-xs text-gray-400">Manual</span>
                </td>
                <td class="px-6 py-4 text-sm text-gray-500 dark:text-gray-400">{{ p.node_count }}</td>
                <td class="px-6 py-4">
                  <span :class="p.enabled ? 'text-green-500' : 'text-gray-400'" class="text-xs font-medium">
                    {{ p.enabled ? 'Enabled' : 'Disabled' }}
                  </span>
                </td>
                <td class="px-6 py-4">
                  <template v-if="p.last_run_status">
                    <span :class="runStatusClass(p.last_run_status)" class="text-xs font-medium">
                      {{ p.last_run_status }}
                    </span>
                    <span v-if="p.last_run_at" class="text-xs text-gray-400 ml-1">{{ timeAgo(p.last_run_at) }}</span>
                  </template>
                  <span v-else class="text-xs text-gray-400">Never</span>
                </td>
                <td class="px-6 py-4 text-right">
                  <div class="flex items-center justify-end gap-2">
                    <button @click="runPipeline(p.id)" :disabled="runningId === p.id" class="text-xs text-green-500 hover:text-green-400 disabled:opacity-50">
                      Run
                    </button>
                    <button @click="deletePipeline(p.id)" class="text-xs text-red-500 hover:text-red-400">
                      Delete
                    </button>
                  </div>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import { useRoute } from 'vue-router'
import axios from 'axios'
import AppLayout from '@/Layouts/AppLayout.vue'
import { useAuth } from '@/composables/useAuth'

const route = useRoute()
const { user } = useAuth()

const projectId = computed(() => route.params.id)
const project = computed(() => ({ id: projectId.value }))

const loading = ref(false)
const error = ref(null)
const pipelines = ref([])
const runningId = ref(null)

function formatDate(iso) {
  if (!iso) return ''
  const d = new Date(iso)
  return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
}

function timeAgo(iso) {
  if (!iso) return ''
  const diff = Math.floor((Date.now() - new Date(iso).getTime()) / 1000)
  if (diff < 60) return `${diff}s ago`
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`
  return `${Math.floor(diff / 86400)}d ago`
}

function runStatusClass(status) {
  const map = {
    succeeded: 'text-green-500',
    running: 'text-blue-500',
    pending: 'text-yellow-500',
    failed: 'text-red-500',
    crashed: 'text-red-400',
  }
  return map[status] || 'text-gray-400'
}

async function loadPipelines() {
  loading.value = true
  error.value = null
  try {
    const res = await axios.get(`/api/projects/${projectId.value}/warehouse/pipelines`)
    pipelines.value = res.data.pipelines || []
  } catch (err) {
    error.value = 'Failed to load pipelines'
  } finally {
    loading.value = false
  }
}

async function runPipeline(id) {
  runningId.value = id
  try {
    await axios.post(`/api/projects/${projectId.value}/warehouse/pipelines/${id}/run`)
  } catch { /* ignore */ }
  finally {
    runningId.value = null
  }
}

async function deletePipeline(id) {
  if (!confirm('Delete this pipeline?')) return
  try {
    await axios.delete(`/api/projects/${projectId.value}/warehouse/pipelines/${id}`)
    pipelines.value = pipelines.value.filter(p => p.id !== id)
  } catch { /* ignore */ }
}

onMounted(loadPipelines)
watch(projectId, loadPipelines)
</script>

<style scoped>
.spinner {
  @apply w-8 h-8 border-4 border-gray-200 dark:border-gray-700 border-t-primary-500 rounded-full animate-spin;
}
.mode-badge-batch {
  @apply text-xs font-medium px-2 py-0.5 rounded-full bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300;
}
.mode-badge-streaming {
  @apply text-xs font-medium px-2 py-0.5 rounded-full bg-brand-100 text-brand-700 dark:bg-brand-900/40 dark:text-brand-300;
}
</style>
