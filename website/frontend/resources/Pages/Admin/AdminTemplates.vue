<template>
  <div>
    <!-- Header -->
    <div class="flex items-center justify-between mb-6">
      <h2 class="text-lg font-semibold text-gray-900">Dashboard Templates</h2>
      <button
        @click="openCreateModal"
        class="inline-flex items-center px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg text-sm font-medium transition-colors"
      >
        <svg class="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" />
        </svg>
        Create Template
      </button>
    </div>

    <!-- Error banner -->
    <div v-if="error" class="mb-4 p-3 bg-red-50 border border-red-200 rounded-lg">
      <p class="text-sm text-red-700">{{ error }}</p>
    </div>

    <!-- Success banner -->
    <div v-if="success" class="mb-4 p-3 bg-green-50 border border-green-200 rounded-lg">
      <p class="text-sm text-green-700">{{ success }}</p>
    </div>

    <!-- Loading -->
    <div v-if="loading" class="flex items-center justify-center py-12">
      <div class="spinner w-6 h-6 border-2 border-blue-600 border-t-transparent rounded-full"></div>
      <span class="ml-3 text-gray-400 text-sm">Loading templates...</span>
    </div>

    <!-- Table -->
    <div v-else-if="templates.length > 0" class="overflow-x-auto">
      <table class="min-w-full divide-y divide-gray-200">
        <thead class="bg-gray-50">
          <tr>
            <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Name</th>
            <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Category</th>
            <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Tags</th>
            <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Featured</th>
            <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Order</th>
            <th class="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
          </tr>
        </thead>
        <tbody class="bg-white divide-y divide-gray-200">
          <tr v-for="tpl in templates" :key="tpl.id" class="hover:bg-gray-50">
            <td class="px-4 py-3 text-sm font-medium text-gray-900">{{ tpl.name }}</td>
            <td class="px-4 py-3 text-sm text-gray-500 capitalize">{{ tpl.category }}</td>
            <td class="px-4 py-3">
              <div class="flex flex-wrap gap-1">
                <span
                  v-for="tag in (tpl.tags || []).slice(0, 4)"
                  :key="tag"
                  class="px-1.5 py-0.5 text-[10px] font-medium bg-gray-100 text-gray-500 rounded"
                >{{ tag }}</span>
              </div>
            </td>
            <td class="px-4 py-3 text-sm">
              <span v-if="tpl.is_featured" class="text-green-600 font-medium">Yes</span>
              <span v-else class="text-gray-400">No</span>
            </td>
            <td class="px-4 py-3 text-sm text-gray-500">{{ tpl.display_order }}</td>
            <td class="px-4 py-3 text-right">
              <div class="flex items-center justify-end gap-2">
                <button
                  @click="openEditModal(tpl)"
                  class="text-blue-600 hover:text-blue-800 text-sm font-medium"
                >Edit</button>
                <button
                  @click="confirmDelete(tpl)"
                  class="text-red-600 hover:text-red-800 text-sm font-medium"
                >Delete</button>
              </div>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <!-- Empty state -->
    <div v-else class="text-center py-12 bg-gray-50/50 rounded-lg border border-gray-200">
      <h3 class="text-sm font-medium text-gray-900">No templates</h3>
      <p class="mt-1 text-sm text-gray-400">Create a dashboard template to get started.</p>
    </div>

    <!-- Delete confirmation modal -->
    <div v-if="deleteTarget" class="fixed inset-0 z-50 flex items-center justify-center" aria-modal="true">
      <div class="fixed inset-0 bg-black/40" @click="deleteTarget = null"></div>
      <div class="relative bg-white rounded-xl shadow-xl p-6 max-w-sm w-full mx-4">
        <h3 class="text-lg font-semibold text-gray-900 mb-2">Delete Template</h3>
        <p class="text-sm text-gray-500 mb-4">
          Are you sure you want to delete <strong>{{ deleteTarget.name }}</strong>? This cannot be undone.
        </p>
        <div class="flex justify-end gap-3">
          <button
            @click="deleteTarget = null"
            class="px-4 py-2 bg-gray-100 text-gray-600 rounded-lg text-sm font-medium hover:bg-gray-200 transition-colors"
          >Cancel</button>
          <button
            @click="doDelete"
            :disabled="deleting"
            class="px-4 py-2 bg-red-600 text-white rounded-lg text-sm font-medium hover:bg-red-700 transition-colors disabled:opacity-50"
          >
            <span v-if="deleting">Deleting...</span>
            <span v-else>Delete</span>
          </button>
        </div>
      </div>
    </div>

    <!-- Create/Edit Modal -->
    <div v-if="showModal" class="fixed inset-0 z-50 overflow-y-auto" aria-modal="true">
      <div class="flex items-start justify-center min-h-screen pt-8 px-4 pb-20">
        <div class="fixed inset-0 bg-black/60" @click="closeModal"></div>

        <div class="relative bg-white rounded-xl shadow-xl w-full max-w-4xl border border-gray-200">
          <div class="px-6 pt-5 pb-4 border-b border-gray-200">
            <div class="flex items-center justify-between">
              <h3 class="text-lg font-semibold text-gray-900">
                {{ editingTemplate ? 'Edit Template' : 'Create Template' }}
              </h3>
              <button @click="closeModal" class="text-gray-400 hover:text-gray-700 transition-colors">
                <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>
          </div>

          <div class="px-6 py-4 max-h-[75vh] overflow-y-auto space-y-6">
            <!-- Section: Metadata -->
            <div>
              <h4 class="text-sm font-semibold text-gray-700 uppercase tracking-wider mb-3">Metadata</h4>
              <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label class="block text-sm font-medium text-gray-600 mb-1">Name *</label>
                  <input v-model="form.name" type="text" class="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500" />
                </div>
                <div>
                  <label class="block text-sm font-medium text-gray-600 mb-1">Category</label>
                  <select v-model="form.category" class="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500">
                    <option value="general">General</option>
                    <option value="infrastructure">Infrastructure</option>
                    <option value="database">Database</option>
                    <option value="apm">APM</option>
                    <option value="ai">AI</option>
                    <option value="runtime">Runtime</option>
                    <option value="web">Web</option>
                    <option value="messaging">Messaging</option>
                  </select>
                </div>
                <div class="md:col-span-2">
                  <label class="block text-sm font-medium text-gray-600 mb-1">Description</label>
                  <input v-model="form.description" type="text" class="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500" />
                </div>
                <div>
                  <label class="block text-sm font-medium text-gray-600 mb-1">Tags (comma-separated)</label>
                  <input v-model="form.tagsStr" type="text" placeholder="otel, postgres, ..." class="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500" />
                </div>
                <div class="flex items-center gap-6">
                  <label class="flex items-center gap-2 text-sm text-gray-600 cursor-pointer">
                    <input v-model="form.is_featured" type="checkbox" class="w-4 h-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500" />
                    Featured
                  </label>
                  <div class="flex items-center gap-2">
                    <label class="text-sm text-gray-600">Order</label>
                    <input v-model.number="form.display_order" type="number" class="w-20 px-2 py-1 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500" />
                  </div>
                </div>
              </div>
            </div>

            <!-- Section: Template Config -->
            <div>
              <div class="flex items-center justify-between mb-3">
                <h4 class="text-sm font-semibold text-gray-700 uppercase tracking-wider">Template Config</h4>
                <button
                  @click="toggleJsonMode"
                  class="text-xs font-medium text-blue-600 hover:text-blue-800"
                >{{ jsonMode ? 'Structured Editor' : 'Edit as JSON' }}</button>
              </div>

              <!-- JSON mode -->
              <div v-if="jsonMode">
                <textarea
                  v-model="form.configJson"
                  rows="20"
                  class="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm font-mono focus:outline-none focus:ring-2 focus:ring-blue-500"
                  @blur="syncJsonToStructured"
                ></textarea>
                <p v-if="jsonError" class="mt-1 text-xs text-red-600">{{ jsonError }}</p>
              </div>

              <!-- Structured mode -->
              <div v-else class="space-y-4">
                <!-- Variables -->
                <div class="border border-gray-200 rounded-lg p-4">
                  <div class="flex items-center justify-between mb-3">
                    <h5 class="text-sm font-medium text-gray-700">Variables</h5>
                    <button @click="addVariable" class="text-xs text-blue-600 hover:text-blue-800 font-medium">+ Add Variable</button>
                  </div>
                  <div v-if="form.variables.length === 0" class="text-sm text-gray-400 italic">No variables defined</div>
                  <div v-for="(v, vi) in form.variables" :key="vi" class="flex items-start gap-2 mb-2 p-2 bg-gray-50 rounded">
                    <div class="grid grid-cols-2 md:grid-cols-4 gap-2 flex-1">
                      <input v-model="v.name" placeholder="name" class="px-2 py-1 border border-gray-200 rounded text-xs" />
                      <input v-model="v.label" placeholder="label" class="px-2 py-1 border border-gray-200 rounded text-xs" />
                      <select v-model="v.type" class="px-2 py-1 border border-gray-200 rounded text-xs">
                        <option value="query">query</option>
                        <option value="select">select</option>
                        <option value="text">text</option>
                      </select>
                      <input v-if="v.type === 'query'" v-model="v.query" placeholder="label_values(...)" class="px-2 py-1 border border-gray-200 rounded text-xs" />
                      <input v-if="v.type === 'select'" v-model="v.options" placeholder="opt1, opt2" class="px-2 py-1 border border-gray-200 rounded text-xs" />
                    </div>
                    <button @click="form.variables.splice(vi, 1)" class="text-red-400 hover:text-red-600 p-1 shrink-0">
                      <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>
                    </button>
                  </div>
                </div>

                <!-- Tabs -->
                <div class="border border-gray-200 rounded-lg p-4">
                  <div class="flex items-center justify-between mb-3">
                    <h5 class="text-sm font-medium text-gray-700">Tabs</h5>
                    <button @click="addTab" class="text-xs text-blue-600 hover:text-blue-800 font-medium">+ Add Tab</button>
                  </div>
                  <div v-if="form.tabs.length === 0" class="text-sm text-gray-400 italic">No tabs defined</div>
                  <div v-for="(tab, ti) in form.tabs" :key="ti" class="mb-4 border border-gray-100 rounded-lg bg-white">
                    <div class="flex items-center gap-2 p-3 bg-gray-50 rounded-t-lg cursor-pointer" @click="tab._expanded = !tab._expanded">
                      <svg :class="['w-4 h-4 text-gray-400 transition-transform', tab._expanded ? 'rotate-90' : '']" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"/></svg>
                      <span class="text-sm font-medium text-gray-700 flex-1">{{ tab.name || `Tab ${ti + 1}` }}</span>
                      <span class="text-xs text-gray-400">{{ tab.widgets.length }} widget(s)</span>
                      <button @click.stop="form.tabs.splice(ti, 1)" class="text-red-400 hover:text-red-600 p-1">
                        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>
                      </button>
                    </div>
                    <div v-if="tab._expanded" class="p-3 space-y-3">
                      <div class="flex gap-2">
                        <div class="flex-1">
                          <label class="text-xs text-gray-500">Tab Name</label>
                          <input v-model="tab.name" class="w-full px-2 py-1 border border-gray-200 rounded text-sm" />
                        </div>
                        <div class="w-32">
                          <label class="text-xs text-gray-500">Icon</label>
                          <select v-model="tab.icon" class="w-full px-2 py-1 border border-gray-200 rounded text-sm">
                            <option value="">None</option>
                            <option value="server">server</option>
                            <option value="database">database</option>
                            <option value="zap">zap</option>
                            <option value="cloud">cloud</option>
                            <option value="hard-drive">hard-drive</option>
                            <option value="wifi">wifi</option>
                            <option value="activity">activity</option>
                            <option value="cpu">cpu</option>
                          </select>
                        </div>
                      </div>

                      <!-- Widgets -->
                      <div class="border-t border-gray-100 pt-3">
                        <div class="flex items-center justify-between mb-2">
                          <span class="text-xs font-medium text-gray-500 uppercase">Widgets</span>
                          <button @click="addWidget(tab)" class="text-xs text-blue-600 hover:text-blue-800 font-medium">+ Add Widget</button>
                        </div>
                        <div v-for="(w, wi) in tab.widgets" :key="wi" class="mb-3 p-3 bg-gray-50 rounded-lg border border-gray-100">
                          <div class="flex items-center justify-between mb-2">
                            <span class="text-xs font-medium text-gray-600">{{ w.title || `Widget ${wi + 1}` }}</span>
                            <button @click="tab.widgets.splice(wi, 1)" class="text-red-400 hover:text-red-600">
                              <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>
                            </button>
                          </div>
                          <div class="grid grid-cols-2 md:grid-cols-4 gap-2 mb-2">
                            <div>
                              <label class="text-[10px] text-gray-400">Title</label>
                              <input v-model="w.title" class="w-full px-2 py-1 border border-gray-200 rounded text-xs" />
                            </div>
                            <div>
                              <label class="text-[10px] text-gray-400">Type</label>
                              <select v-model="w.type" class="w-full px-2 py-1 border border-gray-200 rounded text-xs">
                                <option value="stat">stat</option>
                                <option value="timeseries">timeseries</option>
                                <option value="top_list">top_list</option>
                                <option value="bar">bar</option>
                                <option value="pie">pie</option>
                                <option value="table">table</option>
                                <option value="heatmap">heatmap</option>
                                <option value="histogram">histogram</option>
                                <option value="text">text</option>
                              </select>
                            </div>
                            <div>
                              <label class="text-[10px] text-gray-400">Unit</label>
                              <input v-model="w.unit" placeholder="s, bytes, ..." class="w-full px-2 py-1 border border-gray-200 rounded text-xs" />
                            </div>
                            <div>
                              <label class="text-[10px] text-gray-400">Format</label>
                              <input v-model="w.format" placeholder="percentage, ..." class="w-full px-2 py-1 border border-gray-200 rounded text-xs" />
                            </div>
                          </div>
                          <div class="grid grid-cols-4 gap-2 mb-2">
                            <div>
                              <label class="text-[10px] text-gray-400">X</label>
                              <input v-model.number="w.x" type="number" class="w-full px-2 py-1 border border-gray-200 rounded text-xs" />
                            </div>
                            <div>
                              <label class="text-[10px] text-gray-400">Y</label>
                              <input v-model.number="w.y" type="number" class="w-full px-2 py-1 border border-gray-200 rounded text-xs" />
                            </div>
                            <div>
                              <label class="text-[10px] text-gray-400">W</label>
                              <input v-model.number="w.w" type="number" class="w-full px-2 py-1 border border-gray-200 rounded text-xs" />
                            </div>
                            <div>
                              <label class="text-[10px] text-gray-400">H</label>
                              <input v-model.number="w.h" type="number" class="w-full px-2 py-1 border border-gray-200 rounded text-xs" />
                            </div>
                          </div>
                          <!-- Query section -->
                          <div>
                            <div class="flex items-center gap-2 mb-1">
                              <label class="text-[10px] text-gray-400">Query Mode</label>
                              <button
                                @click="w._multiQuery = !w._multiQuery"
                                class="text-[10px] text-blue-600 hover:text-blue-800"
                              >{{ w._multiQuery ? 'Single' : 'Multi-series' }}</button>
                              <label class="flex items-center gap-1 text-[10px] text-gray-400 ml-auto">
                                <input v-model="w.instant" type="checkbox" class="w-3 h-3 rounded" />
                                Instant
                              </label>
                            </div>
                            <div v-if="!w._multiQuery">
                              <textarea v-model="w.promql" rows="2" placeholder="PromQL expression..." class="w-full px-2 py-1 border border-gray-200 rounded text-xs font-mono"></textarea>
                            </div>
                            <div v-else class="space-y-1">
                              <div v-for="(q, qi) in w.queries" :key="qi" class="flex gap-1">
                                <input v-model="q.promql" placeholder="PromQL..." class="flex-1 px-2 py-1 border border-gray-200 rounded text-xs font-mono" />
                                <input v-model="q.legend_format" placeholder="legend" class="w-24 px-2 py-1 border border-gray-200 rounded text-xs" />
                                <button @click="w.queries.splice(qi, 1)" class="text-red-400 hover:text-red-600 px-1">
                                  <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>
                                </button>
                              </div>
                              <button @click="w.queries.push({ promql: '', legend_format: '' })" class="text-[10px] text-blue-600 hover:text-blue-800">+ Add series</button>
                            </div>
                          </div>
                          <!-- Thresholds -->
                          <div class="mt-2">
                            <div class="flex items-center justify-between">
                              <label class="text-[10px] text-gray-400">Thresholds</label>
                              <button @click="w.thresholds.push({ value: 0, color: 'green' })" class="text-[10px] text-blue-600 hover:text-blue-800">+ Add</button>
                            </div>
                            <div v-for="(th, thi) in w.thresholds" :key="thi" class="flex gap-1 mt-1">
                              <input v-model.number="th.value" type="number" placeholder="value" class="w-20 px-2 py-1 border border-gray-200 rounded text-xs" />
                              <select v-model="th.color" class="px-2 py-1 border border-gray-200 rounded text-xs">
                                <option value="green">green</option>
                                <option value="orange">orange</option>
                                <option value="red">red</option>
                              </select>
                              <button @click="w.thresholds.splice(thi, 1)" class="text-red-400 hover:text-red-600 px-1">
                                <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>
                              </button>
                            </div>
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>

          <!-- Footer -->
          <div class="px-6 py-4 bg-gray-50/50 border-t border-gray-200 flex justify-end gap-3">
            <button
              @click="closeModal"
              class="px-4 py-2 bg-gray-100 text-gray-600 rounded-lg text-sm font-medium hover:bg-gray-200 transition-colors"
            >Cancel</button>
            <button
              @click="saveTemplate"
              :disabled="saving"
              class="px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 transition-colors disabled:opacity-50"
            >
              <span v-if="saving">Saving...</span>
              <span v-else>{{ editingTemplate ? 'Update' : 'Create' }}</span>
            </button>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import axios from 'axios'

const templates = ref([])
const loading = ref(true)
const error = ref('')
const success = ref('')

const showModal = ref(false)
const editingTemplate = ref(null)
const saving = ref(false)

const deleteTarget = ref(null)
const deleting = ref(false)

const jsonMode = ref(false)
const jsonError = ref('')

const form = ref(getEmptyForm())

function getEmptyForm() {
  return {
    name: '',
    description: '',
    category: 'general',
    tagsStr: '',
    is_featured: false,
    display_order: 0,
    variables: [],
    tabs: [],
    configJson: '{\n  "variables": [],\n  "tabs": []\n}',
  }
}

function makeWidget() {
  return {
    title: '',
    type: 'stat',
    x: 0, y: 0, w: 3, h: 2,
    unit: '',
    format: '',
    promql: '',
    instant: false,
    _multiQuery: false,
    queries: [],
    thresholds: [],
  }
}

async function loadTemplates() {
  loading.value = true
  error.value = ''
  try {
    const { data } = await axios.get('/api/admin/dashboard-templates')
    templates.value = data
  } catch (e) {
    error.value = e.response?.data?.error || 'Failed to load templates'
  } finally {
    loading.value = false
  }
}

function openCreateModal() {
  editingTemplate.value = null
  form.value = getEmptyForm()
  jsonMode.value = false
  jsonError.value = ''
  showModal.value = true
}

function openEditModal(tpl) {
  editingTemplate.value = tpl
  jsonMode.value = false
  jsonError.value = ''

  const config = tpl.template_config || {}
  form.value = {
    name: tpl.name,
    description: tpl.description || '',
    category: tpl.category,
    tagsStr: (tpl.tags || []).join(', '),
    is_featured: tpl.is_featured,
    display_order: tpl.display_order,
    variables: (config.variables || []).map(v => ({
      name: v.name || '',
      label: v.label || '',
      type: v.type || 'query',
      query: v.query || '',
      options: Array.isArray(v.options) ? v.options.join(', ') : (v.options || ''),
    })),
    tabs: (config.tabs || []).map(tab => ({
      name: tab.name || '',
      icon: tab.icon || '',
      _expanded: false,
      widgets: (tab.widgets || []).map(w => ({
        title: w.title || '',
        type: w.type || 'stat',
        x: w.x || 0,
        y: w.y || 0,
        w: w.w || 3,
        h: w.h || 2,
        unit: w.config?.unit || '',
        format: w.config?.format || '',
        promql: w.config?.query?.promql || '',
        instant: w.config?.query?.instant || false,
        _multiQuery: Array.isArray(w.config?.query?.queries),
        queries: (w.config?.query?.queries || []).map(q => ({
          promql: q.promql || '',
          legend_format: q.legend_format || '',
        })),
        thresholds: (w.config?.thresholds || w.config?.query?.thresholds || []).map(t => ({
          value: t.value ?? 0,
          color: t.color || 'green',
        })),
      })),
    })),
    configJson: JSON.stringify(config, null, 2),
  }
  showModal.value = true
}

function closeModal() {
  showModal.value = false
  editingTemplate.value = null
}

function addVariable() {
  form.value.variables.push({ name: '', label: '', type: 'query', query: '', options: '' })
}

function addTab() {
  form.value.tabs.push({ name: '', icon: '', _expanded: true, widgets: [] })
}

function addWidget(tab) {
  tab.widgets.push(makeWidget())
}

function toggleJsonMode() {
  if (!jsonMode.value) {
    try {
      form.value.configJson = JSON.stringify(buildTemplateConfig(), null, 2)
    } catch (_) {}
  }
  jsonMode.value = !jsonMode.value
}

function syncJsonToStructured() {
  jsonError.value = ''
  try {
    const parsed = JSON.parse(form.value.configJson)
    form.value.variables = (parsed.variables || []).map(v => ({
      name: v.name || '',
      label: v.label || '',
      type: v.type || 'query',
      query: v.query || '',
      options: Array.isArray(v.options) ? v.options.join(', ') : (v.options || ''),
    }))
    form.value.tabs = (parsed.tabs || []).map(tab => ({
      name: tab.name || '',
      icon: tab.icon || '',
      _expanded: false,
      widgets: (tab.widgets || []).map(w => ({
        title: w.title || '',
        type: w.type || 'stat',
        x: w.x || 0,
        y: w.y || 0,
        w: w.w || 3,
        h: w.h || 2,
        unit: w.config?.unit || '',
        format: w.config?.format || '',
        promql: w.config?.query?.promql || '',
        instant: w.config?.query?.instant || false,
        _multiQuery: Array.isArray(w.config?.query?.queries),
        queries: (w.config?.query?.queries || []).map(q => ({
          promql: q.promql || '',
          legend_format: q.legend_format || '',
        })),
        thresholds: (w.config?.thresholds || w.config?.query?.thresholds || []).map(t => ({
          value: t.value ?? 0,
          color: t.color || 'green',
        })),
      })),
    }))
  } catch (e) {
    jsonError.value = 'Invalid JSON: ' + e.message
  }
}

function buildTemplateConfig() {
  if (jsonMode.value) {
    try {
      return JSON.parse(form.value.configJson)
    } catch (e) {
      throw new Error('Invalid JSON in config editor: ' + e.message)
    }
  }

  const variables = form.value.variables.map(v => {
    const obj = { name: v.name, label: v.label, type: v.type }
    if (v.type === 'query') obj.query = v.query
    if (v.type === 'select') obj.options = v.options.split(',').map(s => s.trim()).filter(Boolean)
    return obj
  })

  const tabs = form.value.tabs.map(tab => ({
    name: tab.name,
    icon: tab.icon || undefined,
    widgets: tab.widgets.map(w => {
      const config = {}
      if (w.unit) config.unit = w.unit
      if (w.format) config.format = w.format
      if (w.thresholds.length > 0) config.thresholds = w.thresholds

      if (w._multiQuery && w.queries.length > 0) {
        config.query = { queries: w.queries.filter(q => q.promql) }
        if (w.instant) config.query.instant = true
      } else if (w.promql) {
        config.query = { promql: w.promql }
        if (w.instant) config.query.instant = true
      }

      return {
        type: w.type,
        title: w.title,
        x: w.x, y: w.y, w: w.w, h: w.h,
        config,
      }
    }),
  }))

  return { variables, tabs }
}

async function saveTemplate() {
  error.value = ''
  saving.value = true

  try {
    const template_config = buildTemplateConfig()
    const tags = form.value.tagsStr.split(',').map(s => s.trim()).filter(Boolean)

    const payload = {
      name: form.value.name,
      description: form.value.description || null,
      category: form.value.category,
      tags,
      is_featured: form.value.is_featured,
      display_order: form.value.display_order,
      template_config,
    }

    if (editingTemplate.value) {
      await axios.put(`/api/admin/dashboard-templates/${editingTemplate.value.id}`, payload)
      success.value = `Template "${payload.name}" updated`
    } else {
      await axios.post('/api/admin/dashboard-templates', payload)
      success.value = `Template "${payload.name}" created`
    }

    closeModal()
    await loadTemplates()
    setTimeout(() => { success.value = '' }, 3000)
  } catch (e) {
    error.value = e.response?.data?.error || e.message || 'Failed to save template'
  } finally {
    saving.value = false
  }
}

function confirmDelete(tpl) {
  deleteTarget.value = tpl
}

async function doDelete() {
  deleting.value = true
  error.value = ''

  try {
    await axios.delete(`/api/admin/dashboard-templates/${deleteTarget.value.id}`)
    success.value = `Template "${deleteTarget.value.name}" deleted`
    deleteTarget.value = null
    await loadTemplates()
    setTimeout(() => { success.value = '' }, 3000)
  } catch (e) {
    error.value = e.response?.data?.error || 'Failed to delete template'
  } finally {
    deleting.value = false
  }
}

onMounted(() => {
  loadTemplates()
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
