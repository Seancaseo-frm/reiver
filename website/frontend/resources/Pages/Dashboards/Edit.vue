<template>
  <AppLayout :user="user" :current-project="project">
    <div class="dashboard-builder">
      <!-- Builder Header -->
      <div class="builder-header">
        <div class="builder-header-left">
          <router-link :to="`/p/${projectId}/dashboards/${dashboardId}`" class="back-link">
            ← Back to Dashboard
          </router-link>
          <div>
            <h1 class="builder-title">{{ dashboard?.name }} - Edit</h1>
            <p v-if="dashboard?.description" class="builder-description">{{ dashboard.description }}</p>
          </div>
        </div>
        <div class="builder-header-right">
          <button
            @click="saveDashboard"
            :disabled="saving"
            class="save-btn"
          >
            {{ saving ? 'Saving...' : 'Save Dashboard' }}
          </button>
        </div>
      </div>

      <div class="builder-content">
        <!-- Widget Palette (Left Sidebar) -->
        <div class="widget-palette">
          <h3 class="palette-title">Add Widget</h3>
          <div class="palette-widgets">
            <button
              v-for="widgetType in availableWidgetTypes"
              :key="widgetType.type"
              @click="addWidget(widgetType.type)"
              class="palette-widget-btn"
            >
              <component :is="widgetType.icon" class="w-5 h-5" />
              <span>{{ widgetType.label }}</span>
            </button>
          </div>
        </div>

        <!-- Dashboard Canvas -->
        <div class="builder-canvas">
          <div v-if="loading" class="flex items-center justify-center py-12">
            <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full"></div>
            <span class="ml-3 text-gray-400">Loading dashboard...</span>
          </div>

          <div v-else class="canvas-content">
            <!-- Widget Grid -->
            <div class="widget-grid-builder">
              <div
                v-for="widget in sortedWidgets"
                :key="widget.id || widget._tempId"
                :style="{
                  gridColumn: `span ${widget.width}`,
                  gridRow: `span ${widget.height}`,
                }"
                class="widget-builder-container"
              >
                <div class="widget-builder-card">
                  <!-- Widget Controls -->
                  <div class="widget-controls">
                    <button
                      @click="editWidget(widget)"
                      class="control-btn"
                      title="Edit widget"
                    >
                      <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z" />
                      </svg>
                    </button>
                    <button
                      @click="removeWidget(widget)"
                      class="control-btn text-danger-400 hover:text-danger-300"
                      title="Remove widget"
                    >
                      <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
                      </svg>
                    </button>
                  </div>

                  <!-- Size Controls -->
                  <div class="size-controls">
                    <div class="size-control-group">
                      <label class="size-label">W:</label>
                      <input
                        v-model.number="widget.width"
                        type="number"
                        min="1"
                        max="12"
                        class="size-input"
                        @change="updateWidgetPosition(widget)"
                      />
                    </div>
                    <div class="size-control-group">
                      <label class="size-label">H:</label>
                      <input
                        v-model.number="widget.height"
                        type="number"
                        min="1"
                        max="10"
                        class="size-input"
                        @change="updateWidgetPosition(widget)"
                      />
                    </div>
                  </div>

                  <!-- Widget Preview -->
                  <div class="widget-preview">
                    <div class="widget-preview-header">
                      <h4 class="widget-preview-title">{{ widget.title || getWidgetTypeLabel(widget.widget_type) }}</h4>
                    </div>
                    <div class="widget-preview-content">
                      <div class="widget-type-badge">
                        {{ getWidgetTypeLabel(widget.widget_type) }}
                      </div>
                    </div>
                  </div>
                </div>
              </div>

              <!-- Empty State -->
              <div v-if="widgets.length === 0" class="empty-canvas">
                <svg class="mx-auto h-12 w-12 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 6v6m0 0v6m0-6h6m-6 0H6" />
                </svg>
                <h3 class="mt-2 text-sm font-medium text-gray-900">No widgets</h3>
                <p class="mt-1 text-sm text-gray-400">Click on a widget type on the left to add one</p>
              </div>
            </div>
          </div>
        </div>

        <!-- Widget Settings Panel (Right Sidebar) -->
        <div v-if="selectedWidget" class="widget-settings-panel">
          <div class="settings-header">
            <h3 class="settings-title">Widget Settings</h3>
            <button
              @click="selectedWidget = null"
              class="close-btn"
            >
              <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>

          <div class="settings-content">
            <!-- Widget Type -->
            <div class="settings-section">
              <label class="settings-label">Widget Type</label>
              <select
                v-model="selectedWidget.widget_type"
                class="settings-input"
                disabled
              >
                <option v-for="type in availableWidgetTypes" :key="type.type" :value="type.type">
                  {{ type.label }}
                </option>
              </select>
            </div>

            <!-- Title -->
            <div class="settings-section">
              <label class="settings-label">Title</label>
              <input
                v-model="selectedWidget.title"
                type="text"
                class="settings-input"
                placeholder="Widget title"
              />
            </div>

            <!-- Position -->
            <div class="settings-section">
              <label class="settings-label">Position</label>
              <div class="grid grid-cols-2 gap-2">
                <div>
                  <label class="settings-sub-label">X:</label>
                  <input
                    v-model.number="selectedWidget.position_x"
                    type="number"
                    min="0"
                    class="settings-input"
                    @change="updateWidgetPosition(selectedWidget)"
                  />
                </div>
                <div>
                  <label class="settings-sub-label">Y:</label>
                  <input
                    v-model.number="selectedWidget.position_y"
                    type="number"
                    min="0"
                    class="settings-input"
                    @change="updateWidgetPosition(selectedWidget)"
                  />
                </div>
              </div>
            </div>

            <!-- Size -->
            <div class="settings-section">
              <label class="settings-label">Size</label>
              <div class="grid grid-cols-2 gap-2">
                <div>
                  <label class="settings-sub-label">Width:</label>
                  <input
                    v-model.number="selectedWidget.width"
                    type="number"
                    min="1"
                    max="12"
                    class="settings-input"
                    @change="updateWidgetPosition(selectedWidget)"
                  />
                </div>
                <div>
                  <label class="settings-sub-label">Height:</label>
                  <input
                    v-model.number="selectedWidget.height"
                    type="number"
                    min="1"
                    max="10"
                    class="settings-input"
                    @change="updateWidgetPosition(selectedWidget)"
                  />
                </div>
              </div>
            </div>

            <!-- Widget-Specific Configuration -->
            <div class="settings-section">
              <label class="settings-label">Configuration</label>
              
              <!-- Stat Widget Config -->
              <template v-if="selectedWidget.widget_type === 'stat'">
                <div class="mb-3">
                  <label class="settings-sub-label">Stat Type</label>
                  <select
                    v-model="selectedWidget.widget_config.stat_type"
                    class="settings-input"
                  >
                    <option value="total_exceptions">Total Exceptions</option>
                    <option value="unresolved_exceptions">Unresolved Exceptions</option>
                    <option value="resolved_exceptions">Resolved Exceptions</option>
                  </select>
                </div>
                <div class="mb-3">
                  <label class="settings-sub-label">Format</label>
                  <select
                    v-model="selectedWidget.widget_config.format"
                    class="settings-input"
                  >
                    <option value="">Number</option>
                    <option value="currency">Currency</option>
                    <option value="percentage">Percentage</option>
                  </select>
                </div>
              </template>

              <!-- PromQL Widget Config (for timeseries, bar, histogram, table, pie, heatmap, top_list widgets) -->
              <template v-if="selectedWidget.widget_config?.query">
                <div class="mb-3">
                  <label class="settings-sub-label">PromQL Expression</label>
                  <textarea
                    v-model="selectedWidget.widget_config.query.promql"
                    rows="3"
                    class="settings-input font-mono text-sm"
                    placeholder='e.g., rate(http_requests_total[5m])'
                  />
                </div>
                <div class="mb-3">
                  <label class="settings-sub-label">Legend Format</label>
                  <input
                    v-model="selectedWidget.widget_config.query.legend_format"
                    type="text"
                    class="settings-input font-mono text-sm"
                    placeholder='e.g., {{method}} {{status}}'
                  />
                </div>
                <div class="mb-3 flex items-center gap-2">
                  <input
                    v-model="selectedWidget.widget_config.query.instant"
                    type="checkbox"
                    class="rounded border-gray-300"
                    id="instant-query"
                  />
                  <label for="instant-query" class="settings-sub-label mb-0">Instant query</label>
                </div>
              </template>
            </div>
          </div>
        </div>
      </div>

      <!-- Widget Edit Modal -->
      <div v-if="showWidgetModal" class="modal-overlay" @click="showWidgetModal = false">
        <div class="modal-content" @click.stop>
          <div class="modal-header">
            <h3 class="modal-title">{{ editingWidget ? 'Edit Widget' : 'Add Widget' }}</h3>
            <button @click="showWidgetModal = false" class="modal-close">
              <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>
          <div class="modal-body">
            <!-- Widget configuration form would go here -->
          </div>
        </div>
      </div>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import AppLayout from '@/Layouts/AppLayout.vue'
import { useAuth } from '@/composables/useAuth'
import axios from 'axios'

const route = useRoute()
const router = useRouter()
const { user, fetchUser } = useAuth()
const projectId = ref(route.params.id)
const dashboardId = ref(route.params.dashboard_id)
const project = ref(null)
const dashboard = ref(null)
const widgets = ref([])
const loading = ref(true)
const saving = ref(false)
const selectedWidget = ref(null)
const showWidgetModal = ref(false)
const editingWidget = ref(null)
let nextTempId = 1

const StatIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
    </svg>
  `
}

const LineChartIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 12l3-3 3 3 4-4M8 21l4-4 4 4M3 4h18M4 4h16v12a1 1 0 01-1 1H5a1 1 0 01-1-1V4z" />
    </svg>
  `
}

const BarChartIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
    </svg>
  `
}

const PieChartIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 3.055A9.001 9.001 0 1020.945 13H11V3.055z" />
      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M20.488 9H15V3.512A9.025 9.025 0 0120.488 9z" />
    </svg>
  `
}

const ListIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 12h16M4 18h16" />
    </svg>
  `
}

const availableWidgetTypes = [
  { type: 'stat', label: 'Stat', icon: StatIcon },
  { type: 'timeseries', label: 'Time Series', icon: LineChartIcon },
  { type: 'bar', label: 'Bar Chart', icon: BarChartIcon },
  { type: 'pie', label: 'Pie Chart', icon: PieChartIcon },
  { type: 'histogram', label: 'Histogram', icon: BarChartIcon },
  { type: 'heatmap', label: 'Heatmap', icon: BarChartIcon },
  { type: 'top_list', label: 'Top List', icon: ListIcon },
  { type: 'table', label: 'Table', icon: ListIcon },
]

const sortedWidgets = computed(() => {
  return [...widgets.value].sort((a, b) => {
    if (a.position_y !== b.position_y) {
      return a.position_y - b.position_y
    }
    return a.position_x - b.position_x
  })
})

const getWidgetTypeLabel = (type) => {
  const widget = availableWidgetTypes.find(w => w.type === type)
  return widget ? widget.label : type
}

const loadDashboard = async () => {
  loading.value = true
  try {
    const [dashboardRes, widgetsRes] = await Promise.all([
      axios.get(`/api/dashboards/${projectId.value}/dashboards/${dashboardId.value}`),
      axios.get(`/api/dashboards/${projectId.value}/dashboards/${dashboardId.value}/widgets`)
    ])
    
    dashboard.value = dashboardRes.data
    if (dashboard.value.locked) {
      alert('This dashboard is locked. Unlock it from the dashboard view before editing.')
      router.push(`/p/${projectId.value}/dashboards/${dashboardId.value}`)
      return
    }
    widgets.value = widgetsRes.data.map(w => ({
      ...w,
      widget_config: w.widget_config || {},
    }))
  } catch (err) {
    console.error('Failed to load dashboard:', err)
    if (err.response?.status === 404) {
      router.push(`/p/${projectId.value}/dashboards`)
    }
  } finally {
    loading.value = false
  }
}

const getDefaultQuery = (widgetType) => {
  if (['timeseries', 'bar', 'histogram', 'table', 'pie', 'heatmap', 'top_list'].includes(widgetType)) {
    return { promql: '' }
  }
  return null
}

const addWidget = (widgetType) => {
  let maxY = 0
  if (widgets.value.length > 0) {
    maxY = Math.max(...widgets.value.map(w => w.position_y + w.height))
  }

  const query = getDefaultQuery(widgetType)

  const newWidget = {
    _tempId: `new-${nextTempId++}`,
    widget_type: widgetType,
    title: getWidgetTypeLabel(widgetType),
    position_x: 0,
    position_y: maxY,
    width: widgetType === 'stat' ? 3 : 6,
    height: widgetType === 'stat' ? 2 : 4,
    widget_config: {
      stat_type: widgetType === 'stat' ? 'total_exceptions' : undefined,
      query: query || undefined,
    },
  }

  widgets.value.push(newWidget)
  selectedWidget.value = newWidget
}

const removeWidget = async (widget) => {
  if (!confirm('Are you sure you want to remove this widget?')) {
    return
  }

  if (!widget.id) {
    widgets.value = widgets.value.filter(w => w !== widget)
    if (selectedWidget.value === widget) {
      selectedWidget.value = null
    }
    return
  }

  try {
    await axios.delete(`/api/dashboards/${projectId.value}/dashboards/${dashboardId.value}/widgets/${widget.id}`)
    widgets.value = widgets.value.filter(w => w.id !== widget.id)
    if (selectedWidget.value?.id === widget.id) {
      selectedWidget.value = null
    }
  } catch (err) {
    console.error('Failed to remove widget:', err)
    alert('Failed to remove widget')
  }
}

const editWidget = (widget) => {
  if (!widget.widget_config) {
    widget.widget_config = {}
  }
  if (!widget.widget_config.query && widget.widget_type !== 'stat') {
    widget.widget_config.query = { promql: '' }
  }
  
  selectedWidget.value = widget
}

const updateWidgetPosition = async (widget) => {
  // Validate bounds
  if (widget.width < 1) widget.width = 1
  if (widget.width > 12) widget.width = 12
  if (widget.height < 1) widget.height = 1
  if (widget.height > 10) widget.height = 10
  if (widget.position_x < 0) widget.position_x = 0
  if (widget.position_y < 0) widget.position_y = 0

  // If widget has an id, update it in backend immediately
  if (widget.id) {
    try {
      await axios.put(`/api/dashboards/${projectId.value}/dashboards/${dashboardId.value}/widgets/${widget.id}`, {
        position_x: widget.position_x,
        position_y: widget.position_y,
        width: widget.width,
        height: widget.height,
        title: widget.title,
        widget_config: widget.widget_config,
      })
    } catch (err) {
      console.error('Failed to update widget position:', err)
    }
  }
}

const saveDashboard = async () => {
  saving.value = true
  try {
    // Save all widgets that don't have an ID (new widgets)
    for (const widget of widgets.value) {
      if (!widget.id) {
        try {
          const response = await axios.post(
            `/api/dashboards/${projectId.value}/dashboards/${dashboardId.value}/widgets`,
            widget
          )
          // Update widget with new ID
          widget.id = response.data.id
        } catch (err) {
          console.error('Failed to save widget:', err)
        }
      } else {
        // Update existing widget
        try {
          await axios.put(
            `/api/dashboards/${projectId.value}/dashboards/${dashboardId.value}/widgets/${widget.id}`,
            {
              widget_type: widget.widget_type,
              title: widget.title,
              position_x: widget.position_x,
              position_y: widget.position_y,
              width: widget.width,
              height: widget.height,
              widget_config: widget.widget_config,
            }
          )
        } catch (err) {
          console.error('Failed to update widget:', err)
        }
      }
    }

    // Reload dashboard to get fresh data
    await loadDashboard()
    
    // Navigate to dashboard view
    router.push(`/p/${projectId.value}/dashboards/${dashboardId.value}`)
  } catch (err) {
    console.error('Failed to save dashboard:', err)
    alert('Failed to save dashboard')
  } finally {
    saving.value = false
  }
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
  await loadProject()
  await loadDashboard()
})
</script>

<style scoped>
.dashboard-builder {
  @apply min-h-screen bg-white;
}

.builder-header {
  @apply flex items-center justify-between px-6 py-4 border-b border-gray-200 bg-gray-50;
}

.builder-header-left {
  @apply flex items-center gap-4;
}

.back-link {
  @apply text-sm text-primary-400 hover:text-primary-300;
}

.builder-title {
  @apply text-xl font-semibold text-gray-900;
}

.builder-description {
  @apply text-sm text-gray-400 mt-1;
}

.builder-header-right {
  @apply flex items-center gap-2;
}

.save-btn {
  @apply px-4 py-2 bg-primary-600 text-white rounded-md hover:bg-primary-700 disabled:opacity-50 disabled:cursor-not-allowed font-medium;
}

.builder-content {
  @apply flex h-[calc(100vh-64px)];
}

/* Widget Palette */
.widget-palette {
  @apply w-64 border-r border-gray-200 bg-gray-50 overflow-y-auto;
}

.palette-title {
  @apply px-4 py-3 text-sm font-semibold text-gray-600 border-b border-gray-200;
}

.palette-widgets {
  @apply p-3 space-y-2;
}

.palette-widget-btn {
  @apply w-full px-3 py-2 text-left text-sm text-gray-600 hover:bg-gray-100 rounded-md transition-colors flex items-center gap-2;
}

/* Builder Canvas */
.builder-canvas {
  @apply flex-1 overflow-y-auto;
}

.canvas-content {
  @apply p-6;
}

.widget-grid-builder {
  @apply grid grid-cols-12 gap-4;
  grid-auto-rows: minmax(80px, auto);
}

.widget-builder-container {
  @apply min-h-0;
}

.widget-builder-card {
  @apply relative bg-gray-50 border-2 border-gray-200 rounded-lg hover:border-primary-500 transition-colors;
}

.widget-controls {
  @apply absolute top-2 right-2 flex gap-1 z-10;
}

.control-btn {
  @apply p-1.5 bg-gray-100 hover:bg-gray-200 text-gray-600 rounded transition-colors;
}

.size-controls {
  @apply absolute top-2 left-2 flex gap-2 z-10;
}

.size-control-group {
  @apply flex items-center gap-1 bg-gray-100 px-2 py-1 rounded;
}

.size-label {
  @apply text-xs text-gray-600;
}

.size-input {
  @apply w-10 px-1 py-0.5 text-xs bg-gray-50 border border-gray-300 text-gray-900 rounded;
}

.widget-preview {
  @apply p-4 min-h-[80px] flex flex-col;
}

.widget-preview-header {
  @apply mb-2;
}

.widget-preview-title {
  @apply text-sm font-semibold text-gray-900;
}

.widget-preview-content {
  @apply flex-1 flex items-center justify-center;
}

.widget-type-badge {
  @apply px-2 py-1 text-xs font-medium rounded bg-gray-100 text-gray-600;
}

.empty-canvas {
  @apply col-span-12 text-center py-24;
}

/* Widget Settings Panel */
.widget-settings-panel {
  @apply w-80 border-l border-gray-200 bg-gray-50 overflow-y-auto;
}

.settings-header {
  @apply flex items-center justify-between px-4 py-3 border-b border-gray-200;
}

.settings-title {
  @apply text-sm font-semibold text-gray-900;
}

.close-btn {
  @apply p-1 text-gray-400 hover:text-gray-700 rounded transition-colors;
}

.settings-content {
  @apply p-4 space-y-4;
}

.settings-section {
  @apply space-y-2;
}

.settings-label {
  @apply block text-sm font-medium text-gray-600;
}

.settings-sub-label {
  @apply block text-xs text-gray-400 mb-1;
}

.settings-input {
  @apply w-full px-3 py-2 text-sm bg-gray-100 border border-gray-300 text-gray-900 rounded focus:outline-none focus:ring-2 focus:ring-primary-500;
}

.modal-overlay {
  @apply fixed inset-0 bg-black bg-opacity-50 z-50 flex items-center justify-center;
}

.modal-content {
  @apply bg-gray-50 rounded-lg shadow-xl max-w-2xl w-full max-h-[90vh] overflow-y-auto;
}

.modal-header {
  @apply flex items-center justify-between px-6 py-4 border-b border-gray-200;
}

.modal-title {
  @apply text-lg font-semibold text-gray-900;
}

.modal-close {
  @apply p-1 text-gray-400 hover:text-gray-700 rounded transition-colors;
}

.modal-body {
  @apply p-6;
}

.spinner {
  animation: spin 0.6s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}
</style>

