<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
      <!-- Header -->
      <div class="mb-8">
        <h1 class="text-2xl font-bold text-gray-900">Dashboards</h1>
        <p class="mt-1 text-sm text-gray-400">
          Create custom dashboards or import from Grafana
        </p>
      </div>

      <!-- Actions Row -->
      <div class="flex items-center gap-3 mb-6">
        <button
          @click="showImportModal = true"
          class="inline-flex items-center px-4 py-2 bg-orange-600 hover:bg-orange-700 text-white rounded-lg text-sm font-medium transition-colors"
        >
          <svg class="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4-4m0 0L8 8m4-4v12" />
          </svg>
          Import from Grafana
        </button>

        <router-link
          :to="`/p/${projectId}/dashboards/new`"
          class="inline-flex items-center px-4 py-2 bg-gray-50 border border-gray-200 rounded-lg text-sm font-medium text-gray-600 hover:bg-gray-100 hover:text-gray-900 transition-colors ml-auto"
        >
          <svg class="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" />
          </svg>
          New Dashboard
        </router-link>
      </div>

      <!-- Template Search Trigger -->
      <div class="mb-8">
        <div
          class="relative w-full max-w-md cursor-text"
          @click="showTemplateModal = true"
        >
          <div class="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
            <svg class="w-4 h-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
            </svg>
          </div>
          <div class="w-full pl-10 pr-4 py-2 bg-gray-50 border border-gray-200 rounded-lg text-sm text-gray-400 select-none">
            Search dashboards...
          </div>
        </div>
      </div>

      <!-- Template Search Modal -->
      <div v-if="showTemplateModal" class="fixed inset-0 z-50 overflow-y-auto" aria-labelledby="template-modal-title" role="dialog" aria-modal="true">
        <div class="flex items-start justify-center min-h-screen pt-16 px-4 pb-20 sm:pt-24">
          <div class="fixed inset-0 bg-black/60 transition-opacity" @click="closeTemplateModal"></div>

          <div class="relative bg-white rounded-xl shadow-xl w-full max-w-3xl border border-gray-200">
            <div class="px-6 pt-5 pb-4">
              <div class="flex items-center gap-3 mb-4">
                <div class="flex-1 relative">
                  <div class="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                    <svg class="w-4 h-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
                    </svg>
                  </div>
                  <input
                    ref="templateSearchInputRef"
                    v-model="templateSearchQuery"
                    type="text"
                    placeholder="Search dashboards..."
                    class="w-full pl-10 pr-4 py-2.5 bg-gray-50 border border-gray-200 rounded-lg text-sm text-gray-900 placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent"
                    @input="onTemplateSearch"
                  />
                </div>
                <button @click="closeTemplateModal" class="text-gray-400 hover:text-gray-700 transition-colors p-1">
                  <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              </div>

              <!-- Category filter pills -->
              <div v-if="templateCategories.length > 1" class="flex flex-wrap gap-2 mb-4">
                <button
                  @click="templateCategoryFilter = ''"
                  :class="[
                    'px-3 py-1 rounded-full text-xs font-medium transition-colors',
                    templateCategoryFilter === '' ? 'bg-primary-100 text-primary-700' : 'bg-gray-100 text-gray-500 hover:bg-gray-200'
                  ]"
                >All</button>
                <button
                  v-for="cat in templateCategories"
                  :key="cat"
                  @click="templateCategoryFilter = cat"
                  :class="[
                    'px-3 py-1 rounded-full text-xs font-medium transition-colors capitalize',
                    templateCategoryFilter === cat ? 'bg-primary-100 text-primary-700' : 'bg-gray-100 text-gray-500 hover:bg-gray-200'
                  ]"
                >{{ cat }}</button>
              </div>

              <!-- Template results grid -->
              <div class="max-h-[60vh] overflow-y-auto">
                <div v-if="filteredTemplates.length === 0" class="py-12 text-center">
                  <p class="text-sm text-gray-400">No templates found</p>
                </div>
                <div v-else class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
                  <button
                    v-for="template in filteredTemplates"
                    :key="template.id"
                    @click="createFromTemplate(template)"
                    :disabled="creatingFromTemplate === template.id"
                    class="text-left p-4 bg-gray-50 border border-gray-200 rounded-lg hover:border-primary-400 hover:bg-primary-50/30 transition-colors group"
                  >
                    <div class="flex items-start gap-3">
                      <div class="w-8 h-8 rounded-lg bg-gray-100 group-hover:bg-primary-100 flex items-center justify-center transition-colors shrink-0 mt-0.5">
                        <svg class="w-4 h-4 text-gray-500 group-hover:text-primary-600 transition-colors" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 5a1 1 0 011-1h14a1 1 0 011 1v2a1 1 0 01-1 1H5a1 1 0 01-1-1V5zM4 13a1 1 0 011-1h6a1 1 0 011 1v6a1 1 0 01-1 1H5a1 1 0 01-1-1v-6zM16 13a1 1 0 011-1h2a1 1 0 011 1v6a1 1 0 01-1 1h-2a1 1 0 01-1-1v-6z" />
                        </svg>
                      </div>
                      <div class="min-w-0">
                        <h3 class="text-sm font-medium text-gray-900 group-hover:text-primary-700 truncate">{{ template.name }}</h3>
                        <p v-if="template.description" class="text-xs text-gray-400 mt-0.5 line-clamp-2">{{ template.description }}</p>
                        <div v-if="template.tags && template.tags.length > 0" class="flex flex-wrap gap-1 mt-1.5">
                          <span
                            v-for="tag in template.tags.slice(0, 3)"
                            :key="tag"
                            class="px-1.5 py-0.5 text-[10px] font-medium bg-gray-100 text-gray-500 rounded"
                          >{{ tag }}</span>
                        </div>
                      </div>
                    </div>
                    <div v-if="creatingFromTemplate === template.id" class="mt-2 flex items-center gap-2 text-xs text-primary-600">
                      <div class="spinner w-3 h-3 border-2 border-primary-600 border-t-transparent rounded-full"></div>
                      Creating...
                    </div>
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      <!-- User Dashboards -->
      <div>
        <h2 class="text-lg font-semibold text-gray-900 mb-4">Your Dashboards</h2>

        <div v-if="loadingDashboards" class="flex items-center justify-center py-8">
          <div class="spinner w-6 h-6 border-2 border-primary-600 border-t-transparent rounded-full"></div>
          <span class="ml-3 text-gray-400 text-sm">Loading dashboards...</span>
        </div>

        <div v-else-if="dashboards.length === 0" class="text-center py-12 bg-gray-50/50 rounded-lg border border-gray-200">
          <svg class="mx-auto h-12 w-12 text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
          </svg>
          <h3 class="mt-3 text-sm font-medium text-gray-900">No dashboards yet</h3>
          <p class="mt-1 text-sm text-gray-400">
            Import a Grafana dashboard or create a custom one
          </p>
        </div>

        <div v-else class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
          <div
            v-for="dashboard in dashboards"
            :key="dashboard.id"
            class="bg-gray-50 border border-gray-200 rounded-lg p-4 hover:border-primary-500 transition-colors cursor-pointer group relative"
            @click="$router.push(`/p/${projectId}/dashboards/${dashboard.id}`)"
          >
            <div class="flex items-start justify-between mb-2">
              <div class="w-9 h-9 rounded-lg bg-gray-100 group-hover:bg-primary-50 flex items-center justify-center transition-colors shrink-0">
                <svg class="w-5 h-5 text-gray-400 group-hover:text-primary-500 transition-colors" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
                </svg>
              </div>
              <div class="flex items-center gap-1">
                <span
                  v-if="dashboard.is_default"
                  class="px-2 py-0.5 text-xs font-medium rounded bg-primary-50 text-primary-700"
                >
                  Default
                </span>
                <button
                  @click.stop="$router.push(`/p/${projectId}/dashboards/${dashboard.id}/edit`)"
                  class="p-1.5 text-gray-400 hover:text-gray-700 hover:bg-gray-100 rounded-lg transition-colors opacity-0 group-hover:opacity-100"
                  title="Edit"
                >
                  <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z" />
                  </svg>
                </button>
                <button
                  v-if="!dashboard.is_default"
                  @click.stop="deleteDashboard(dashboard.id)"
                  class="p-1.5 text-gray-400 hover:text-red-400 hover:bg-gray-100 rounded-lg transition-colors opacity-0 group-hover:opacity-100"
                  title="Delete"
                >
                  <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
                  </svg>
                </button>
              </div>
            </div>
            <h3 class="font-medium text-gray-900 group-hover:text-primary-400 transition-colors truncate">
              {{ dashboard.name }}
            </h3>
            <p v-if="dashboard.description" class="text-sm text-gray-400 line-clamp-2 mt-1">
              {{ dashboard.description }}
            </p>
            <p class="text-xs text-gray-400 mt-2">
              Updated {{ formatDate(dashboard.updated_at || dashboard.updatedAt) }}
            </p>
          </div>
        </div>
      </div>
      <!-- Grafana Import Modal -->
      <div v-if="showImportModal" class="fixed inset-0 z-50 overflow-y-auto" aria-labelledby="modal-title" role="dialog" aria-modal="true">
        <div class="flex items-end justify-center min-h-screen pt-4 px-4 pb-20 text-center sm:block sm:p-0">
          <!-- Backdrop -->
          <div class="fixed inset-0 bg-black/60 transition-opacity" @click="closeImportModal"></div>

          <!-- Modal panel -->
          <div class="inline-block align-bottom bg-white rounded-xl text-left overflow-hidden shadow-xl transform transition-all sm:my-8 sm:align-middle sm:max-w-2xl sm:w-full border border-gray-200">
            <div class="px-6 pt-6 pb-4">
              <div class="flex items-center justify-between mb-4">
                <h3 class="text-lg font-semibold text-gray-900" id="modal-title">Import from Grafana</h3>
                <button @click="closeImportModal" class="text-gray-400 hover:text-gray-700 transition-colors">
                  <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              </div>

              <p class="text-sm text-gray-400 mb-4">
                Export your Grafana dashboard as JSON (Dashboard settings &rarr; JSON Model), then paste it below. PromQL queries will be stored natively and transpiled at query time.
              </p>

              <!-- Paste JSON -->
              <div class="mb-4">
                <textarea
                  ref="importTextareaRef"
                  v-model="importJsonText"
                  placeholder='Paste Grafana dashboard JSON here...'
                  rows="10"
                  class="w-full px-4 py-3 bg-gray-50 border border-gray-200 rounded-lg text-sm text-gray-900 placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-orange-500 focus:border-transparent font-mono"
                ></textarea>
              </div>

              <!-- Dashboard name override -->
              <div class="mb-4">
                <label class="block text-sm font-medium text-gray-600 mb-1">Dashboard name (optional)</label>
                <input
                  v-model="importDashboardName"
                  type="text"
                  placeholder="Leave blank to use the Grafana dashboard title"
                  class="w-full px-4 py-2 bg-gray-50 border border-gray-200 rounded-lg text-sm text-gray-900 placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-orange-500 focus:border-transparent"
                />
              </div>

              <!-- Preview results -->
              <div v-if="importPreview" class="mb-4">
                <div class="bg-gray-50 rounded-lg p-4 border border-gray-200">
                  <div class="flex items-center gap-4 mb-3">
                    <div class="flex items-center gap-2">
                      <span class="inline-flex items-center justify-center w-6 h-6 rounded-full bg-green-100 text-green-700 text-xs font-bold">{{ importPreview.converted_count }}</span>
                      <span class="text-sm text-gray-600">Converted</span>
                    </div>
                    <div v-if="importPreview.skipped_count > 0" class="flex items-center gap-2">
                      <span class="inline-flex items-center justify-center w-6 h-6 rounded-full bg-yellow-100 text-yellow-700 text-xs font-bold">{{ importPreview.skipped_count }}</span>
                      <span class="text-sm text-gray-600">Skipped</span>
                    </div>
                  </div>

                  <!-- Warnings -->
                  <div v-if="importPreview.warnings && importPreview.warnings.length > 0" class="space-y-2">
                    <p class="text-xs font-medium text-yellow-400 uppercase tracking-wider">Warnings</p>
                    <div class="max-h-40 overflow-y-auto space-y-1">
                      <div
                        v-for="(warning, idx) in importPreview.warnings"
                        :key="idx"
                        class="flex items-start gap-2 text-sm"
                      >
                        <svg class="w-4 h-4 text-yellow-400 mt-0.5 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.07 16.5c-.77.833.192 2.5 1.732 2.5z" />
                        </svg>
                        <span class="text-gray-400">{{ warning }}</span>
                      </div>
                    </div>
                  </div>
                </div>
              </div>

              <!-- Error message -->
              <div v-if="importError" class="mb-4 p-3 bg-red-50 border border-red-200 rounded-lg">
                <p class="text-sm text-red-700">{{ importError }}</p>
              </div>
            </div>

            <!-- Footer -->
            <div class="px-6 py-4 bg-gray-50/50 border-t border-gray-200 flex justify-end gap-3">
              <button
                @click="closeImportModal"
                class="px-4 py-2 bg-gray-100 text-gray-600 rounded-lg text-sm font-medium hover:bg-gray-200 transition-colors"
              >
                Cancel
              </button>
              <button
                v-if="!importPreview"
                @click="previewImport"
                :disabled="importLoading || !importJsonText.trim()"
                class="px-4 py-2 bg-orange-600 text-white rounded-lg text-sm font-medium hover:bg-orange-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                <span v-if="importLoading" class="inline-flex items-center">
                  <div class="spinner w-4 h-4 border-2 border-white border-t-transparent rounded-full mr-2"></div>
                  Processing...
                </span>
                <span v-else>Preview</span>
              </button>
              <button
                v-if="importPreview"
                @click="confirmImport"
                :disabled="importLoading"
                class="px-4 py-2 bg-orange-600 text-white rounded-lg text-sm font-medium hover:bg-orange-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                <span v-if="importLoading" class="inline-flex items-center">
                  <div class="spinner w-4 h-4 border-2 border-white border-t-transparent rounded-full mr-2"></div>
                  Importing...
                </span>
                <span v-else>Import Dashboard</span>
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted, watch, nextTick } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { format } from 'date-fns'
import AppLayout from '@/Layouts/AppLayout.vue'
import { useAuth } from '@/composables/useAuth'
import axios from 'axios'

const route = useRoute()
const router = useRouter()
const { user, fetchUser } = useAuth()
const projectId = ref(route.params.id)
const project = ref(null)

// Dashboards state
const dashboards = ref([])
const loadingDashboards = ref(true)

// Templates state
const templates = ref([])
const creatingFromTemplate = ref(null)
const showTemplateModal = ref(false)
const templateSearchQuery = ref('')
const templateCategoryFilter = ref('')
const templateSearchInputRef = ref(null)
let searchDebounceTimer = null

// Import state
const showImportModal = ref(false)
const importJsonText = ref('')
const importDashboardName = ref('')
const importPreview = ref(null)
const importError = ref('')
const importLoading = ref(false)
const importTextareaRef = ref(null)

const formatDate = (dateString) => {
  if (!dateString) return ''
  return format(new Date(dateString), 'MMM d, yyyy')
}

const loadDashboards = async () => {
  loadingDashboards.value = true
  try {
    const response = await axios.get(`/api/dashboards/${projectId.value}/dashboards`)
    dashboards.value = response.data.map(d => ({
      ...d,
      updatedAt: d.updated_at,
    }))
  } catch (err) {
    console.error('Failed to load dashboards:', err)
  } finally {
    loadingDashboards.value = false
  }
}

const loadTemplates = async (search = '') => {
  try {
    const params = search ? { search } : {}
    const response = await axios.get('/api/dashboards/dashboard-templates', { params })
    templates.value = response.data || []
  } catch (err) {
    console.error('Failed to load templates:', err)
  }
}

const templateCategories = computed(() => {
  const cats = new Set(templates.value.map(t => t.category).filter(Boolean))
  return [...cats].sort()
})

const filteredTemplates = computed(() => {
  if (!templateCategoryFilter.value) return templates.value
  const filter = templateCategoryFilter.value
  return templates.value.filter(t =>
    t.category === filter || (t.tags && t.tags.includes(filter))
  )
})

const onTemplateSearch = () => {
  clearTimeout(searchDebounceTimer)
  searchDebounceTimer = setTimeout(() => {
    loadTemplates(templateSearchQuery.value)
  }, 300)
}

const closeTemplateModal = () => {
  showTemplateModal.value = false
  templateSearchQuery.value = ''
  templateCategoryFilter.value = ''
  loadTemplates()
}

watch(showTemplateModal, (val) => {
  if (val) {
    nextTick(() => {
      templateSearchInputRef.value?.focus()
    })
  }
})

const createFromTemplate = async (template) => {
  creatingFromTemplate.value = template.id
  try {
    const response = await axios.post(`/api/dashboards/${projectId.value}/dashboards/from-template`, {
      template_id: template.id,
    })
    if (response.data?.id) {
      router.push(`/p/${projectId.value}/dashboards/${response.data.id}`)
    }
  } catch (err) {
    console.error('Failed to create from template:', err)
    alert(err.response?.data?.error || 'Failed to create dashboard from template')
  } finally {
    creatingFromTemplate.value = null
  }
}

const deleteDashboard = async (dashboardId) => {
  if (!confirm('Are you sure you want to delete this dashboard?')) {
    return
  }

  try {
    await axios.delete(`/api/dashboards/${projectId.value}/dashboards/${dashboardId}`)
    dashboards.value = dashboards.value.filter(d => d.id !== dashboardId)
  } catch (err) {
    console.error('Failed to delete dashboard:', err)
    alert(err.response?.data?.message || 'Failed to delete dashboard')
  }
}

const getImportPayload = () => {
  if (importJsonText.value.trim()) {
    try {
      return JSON.parse(importJsonText.value)
    } catch {
      throw new Error('Invalid JSON. Please check the format and try again.')
    }
  }
  throw new Error('Please paste Grafana dashboard JSON.')
}

const previewImport = async () => {
  importError.value = ''
  importPreview.value = null
  importLoading.value = true

  try {
    const payload = getImportPayload()

    if (importDashboardName.value.trim()) {
      if (payload.dashboard) {
        payload.dashboard.title = importDashboardName.value.trim()
      } else {
        payload.title = importDashboardName.value.trim()
      }
    }

    const response = await axios.post(
      `/api/dashboards/${projectId.value}/dashboards/import-grafana?dry_run=true`,
      payload
    )

    importPreview.value = response.data
  } catch (err) {
    if (err.message && !err.response) {
      importError.value = err.message
    } else {
      importError.value = err.response?.data?.message || 'Failed to process the dashboard. Please check the JSON format.'
    }
  } finally {
    importLoading.value = false
  }
}

const confirmImport = async () => {
  importError.value = ''
  importLoading.value = true

  try {
    const payload = getImportPayload()

    if (importDashboardName.value.trim()) {
      if (payload.dashboard) {
        payload.dashboard.title = importDashboardName.value.trim()
      } else {
        payload.title = importDashboardName.value.trim()
      }
    }

    const response = await axios.post(
      `/api/dashboards/${projectId.value}/dashboards/import-grafana`,
      payload
    )

    if (response.data?.dashboard?.id) {
      router.push(`/p/${projectId.value}/dashboards/${response.data.dashboard.id}`)
    }
  } catch (err) {
    importError.value = err.response?.data?.message || 'Failed to import dashboard.'
  } finally {
    importLoading.value = false
  }
}

const closeImportModal = () => {
  showImportModal.value = false
  importJsonText.value = ''
  importDashboardName.value = ''
  importPreview.value = null
  importError.value = ''
  importLoading.value = false
}

const loadProject = async () => {
  try {
    const response = await axios.get(`/api/projects/${projectId.value}`)
    project.value = response.data
  } catch (err) {
    console.error('Failed to load project:', err)
  }
}

onMounted(async () => {
  await fetchUser()
  await Promise.all([
    loadProject(),
    loadDashboards(),
    loadTemplates(),
  ])
})

onUnmounted(() => {
  clearTimeout(searchDebounceTimer)
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

.line-clamp-2 {
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  overflow: hidden;
}
</style>
