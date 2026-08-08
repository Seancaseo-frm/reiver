<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-[1200px] mx-auto px-8 py-6">
      <!-- Header -->
      <div class="mb-6 flex items-center justify-between">
        <div>
          <h1 class="text-2xl font-semibold text-gray-900">User-Defined Functions</h1>
          <p class="text-sm text-gray-500 dark:text-gray-400 mt-1">
            Go-based data transformation functions compiled to WebAssembly
          </p>
        </div>
        <button @click="showCreate = true" class="px-4 py-2 text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 rounded-lg transition-colors">
          New UDF
        </button>
      </div>

      <!-- Loading -->
      <div v-if="loading" class="flex items-center justify-center py-32">
        <div class="spinner"></div>
      </div>

      <!-- Error -->
      <div v-else-if="error" class="flex flex-col items-center justify-center py-32 text-center">
        <h3 class="text-lg font-medium text-gray-900 dark:text-gray-100 mb-1">{{ error }}</h3>
        <button @click="loadUdfs" class="mt-4 px-4 py-2 text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 rounded-lg">
          Retry
        </button>
      </div>

      <!-- Empty -->
      <div v-else-if="udfs.length === 0 && !showCreate" class="flex flex-col items-center justify-center py-32 text-center">
        <svg class="w-16 h-16 text-gray-300 dark:text-gray-600 mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M10 20l4-16m4 4l4 4-4 4M6 16l-4-4 4-4" />
        </svg>
        <h3 class="text-lg font-medium text-gray-900 dark:text-gray-100 mb-1">No UDFs yet</h3>
        <p class="text-sm text-gray-500 dark:text-gray-400 max-w-md mb-4">
          Write Go transformation functions and compile them to Wasm for use in pipelines.
        </p>
        <button @click="showCreate = true" class="px-4 py-2 text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 rounded-lg">
          Create UDF
        </button>
      </div>

      <!-- UDF List -->
      <div v-else class="mt-4 space-y-4">
        <div v-for="udf in udfs" :key="udf.name" class="rounded-lg border border-gray-200 dark:border-gray-800 overflow-hidden">
          <div class="flex items-center justify-between px-5 py-4 bg-gray-50 dark:bg-gray-900/50">
            <div>
              <h3 class="text-sm font-semibold text-gray-900 dark:text-gray-100">{{ udf.name }}</h3>
              <p class="text-xs text-gray-500 dark:text-gray-400 mt-0.5">
                {{ udf.execution_mode }} &middot; Created {{ formatDate(udf.created_at) }}
              </p>
            </div>
            <div class="flex items-center gap-3">
              <button @click="toggleSource(udf.name)" class="text-xs text-primary-500 hover:text-primary-400">
                {{ expandedUdf === udf.name ? 'Hide Source' : 'View Source' }}
              </button>
              <button @click="deleteUdf(udf.name)" class="text-xs text-red-500 hover:text-red-400">
                Delete
              </button>
            </div>
          </div>
          <div v-if="expandedUdf === udf.name && udfSources[udf.name]" class="px-5 py-3 border-t border-gray-200 dark:border-gray-800">
            <pre class="text-xs text-gray-300 bg-gray-950 rounded-md p-4 overflow-x-auto"><code>{{ udfSources[udf.name] }}</code></pre>
          </div>
        </div>
      </div>

      <!-- Create Modal -->
      <div v-if="showCreate" class="fixed inset-0 z-50 flex items-center justify-center bg-black/60" @click.self="closeCreate">
        <div class="bg-white dark:bg-gray-900 rounded-xl shadow-2xl w-full max-w-2xl max-h-[80vh] overflow-hidden flex flex-col">
          <div class="px-6 py-4 border-b border-gray-200 dark:border-gray-800 flex items-center justify-between">
            <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">Create UDF</h2>
            <button @click="closeCreate" class="text-gray-400 hover:text-gray-200">
              <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>
          <div class="p-6 space-y-4 overflow-y-auto flex-1">
            <div>
              <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Function Name</label>
              <input v-model="newUdf.name" class="form-input" placeholder="e.g. enrich_orders" />
            </div>
            <div>
              <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Go Source Code</label>
              <textarea v-model="newUdf.source" class="form-input font-mono text-sm h-64 resize-none" placeholder="package main

import &quot;github.com/example/sdk&quot;

func Transform(batch sdk.RecordBatch) (sdk.RecordBatch, error) {
    // your logic here
    return batch, nil
}"></textarea>
            </div>
            <div v-if="createError" class="text-sm text-red-500">{{ createError }}</div>
          </div>
          <div class="px-6 py-4 border-t border-gray-200 dark:border-gray-800 flex justify-end gap-3">
            <button @click="closeCreate" class="px-4 py-2 text-sm font-medium text-gray-700 dark:text-gray-300 bg-gray-100 dark:bg-gray-800 hover:bg-gray-200 dark:hover:bg-gray-700 rounded-lg transition-colors">
              Cancel
            </button>
            <button @click="createUdf" :disabled="creating" class="px-4 py-2 text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 rounded-lg transition-colors disabled:opacity-50">
              {{ creating ? 'Compiling...' : 'Create & Compile' }}
            </button>
          </div>
        </div>
      </div>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, reactive, onMounted, watch } from 'vue'
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
const udfs = ref([])
const expandedUdf = ref(null)
const udfSources = reactive({})

const showCreate = ref(false)
const creating = ref(false)
const createError = ref(null)
const newUdf = reactive({ name: '', source: '' })

function formatDate(iso) {
  if (!iso) return ''
  const d = new Date(iso)
  return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: 'numeric' })
}

async function loadUdfs() {
  loading.value = true
  error.value = null
  try {
    const res = await axios.get(`/api/projects/${projectId.value}/warehouse/udfs`)
    udfs.value = res.data.udfs || []
  } catch (err) {
    error.value = 'Failed to load UDFs'
  } finally {
    loading.value = false
  }
}

async function toggleSource(name) {
  if (expandedUdf.value === name) {
    expandedUdf.value = null
    return
  }
  expandedUdf.value = name
  if (!udfSources[name]) {
    try {
      const res = await axios.get(`/api/projects/${projectId.value}/warehouse/udfs/${name}`)
      udfSources[name] = res.data.source || '(no source available)'
    } catch {
      udfSources[name] = '(failed to load source)'
    }
  }
}

async function createUdf() {
  creating.value = true
  createError.value = null
  try {
    await axios.post(`/api/projects/${projectId.value}/warehouse/udfs`, {
      name: newUdf.name,
      source_code: newUdf.source,
      execution_mode: 'pipeline',
    })
    closeCreate()
    await loadUdfs()
  } catch (err) {
    createError.value = err.response?.data?.message || err.message || 'Failed to create UDF'
  } finally {
    creating.value = false
  }
}

async function deleteUdf(name) {
  if (!confirm(`Delete UDF "${name}"?`)) return
  try {
    await axios.delete(`/api/projects/${projectId.value}/warehouse/udfs/${name}`)
    udfs.value = udfs.value.filter(u => u.name !== name)
  } catch { /* ignore */ }
}

function closeCreate() {
  showCreate.value = false
  createError.value = null
  newUdf.name = ''
  newUdf.source = ''
}

onMounted(loadUdfs)
watch(projectId, loadUdfs)
</script>

<style scoped>
.spinner {
  @apply w-8 h-8 border-4 border-gray-200 dark:border-gray-700 border-t-primary-500 rounded-full animate-spin;
}
.form-input {
  @apply w-full bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-700 rounded-lg px-3 py-2 text-sm text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-500 focus:border-primary-500 focus:ring-1 focus:ring-primary-500 focus:outline-none;
}
</style>
