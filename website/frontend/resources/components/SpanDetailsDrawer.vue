<template>
  <Transition name="drawer">
    <div v-if="isOpen && span" class="span-details-drawer">
      <!-- Header -->
      <div class="drawer-header">
        <div class="flex items-center gap-3">
          <span
            :class="['status-dot', getStatusClass(span.status_code)]"
          ></span>
          <div>
            <h3 class="drawer-title">{{ span.span_name }}</h3>
            <p class="drawer-subtitle">{{ span.service_name }}</p>
          </div>
        </div>
        <button @click="close" class="close-btn">
          <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      </div>

      <!-- Content -->
      <div class="drawer-content">
        <!-- Quick Stats -->
        <div class="stats-grid">
          <div class="stat-item">
            <span class="stat-label">Duration</span>
            <span class="stat-value">{{ formatDuration(span.duration_ns / 1_000_000) }}</span>
          </div>
          <div class="stat-item">
            <span class="stat-label">Self Time</span>
            <span class="stat-value">{{ formatDuration(selfTime) }}</span>
          </div>
          <div class="stat-item">
            <span class="stat-label">Kind</span>
            <span class="stat-value">{{ formatSpanKind(span.span_kind) }}</span>
          </div>
          <div class="stat-item">
            <span class="stat-label">Status</span>
            <span :class="['stat-value', getStatusTextClass(span.status_code)]">
              {{ formatStatus(span.status_code) }}
            </span>
          </div>
        </div>

        <!-- Tabs -->
        <div class="drawer-tabs">
          <button
            v-for="tab in tabs"
            :key="tab.id"
            @click="activeTab = tab.id"
            :class="['tab-btn', { active: activeTab === tab.id }]"
          >
            {{ tab.label }}
            <span v-if="tab.count" class="tab-count">{{ tab.count }}</span>
          </button>
        </div>

        <!-- Tab Content -->
        <div class="tab-content">
          <!-- Attributes Tab -->
          <div v-if="activeTab === 'attributes'" class="attributes-section">
            <div v-if="Object.keys(spanAttributes).length === 0" class="empty-state">
              No attributes recorded for this span
            </div>
            <div v-else class="attribute-groups">
              <!-- Resource Attributes -->
              <div v-if="resourceAttributes.length > 0" class="attribute-group">
                <h4 class="group-title">Resource</h4>
                <div class="attribute-list">
                  <div
                    v-for="attr in resourceAttributes"
                    :key="attr.key"
                    class="attribute-row"
                  >
                    <span class="attribute-key">{{ attr.key }}</span>
                    <span class="attribute-value">{{ formatValue(attr.value) }}</span>
                  </div>
                </div>
              </div>

              <!-- Span Attributes -->
              <div v-if="spanAttributesList.length > 0" class="attribute-group">
                <h4 class="group-title">Span</h4>
                <div class="attribute-list">
                  <div
                    v-for="attr in spanAttributesList"
                    :key="attr.key"
                    class="attribute-row"
                  >
                    <span class="attribute-key">{{ attr.key }}</span>
                    <span class="attribute-value">{{ formatValue(attr.value) }}</span>
                  </div>
                </div>
              </div>

              <!-- HTTP Attributes -->
              <div v-if="httpAttributes.length > 0" class="attribute-group">
                <h4 class="group-title">HTTP</h4>
                <div class="attribute-list">
                  <div
                    v-for="attr in httpAttributes"
                    :key="attr.key"
                    class="attribute-row"
                  >
                    <span class="attribute-key">{{ attr.key }}</span>
                    <span class="attribute-value">{{ formatValue(attr.value) }}</span>
                  </div>
                </div>
              </div>

              <!-- Database Attributes -->
              <div v-if="dbAttributes.length > 0" class="attribute-group">
                <h4 class="group-title">Database</h4>
                <div class="attribute-list">
                  <div
                    v-for="attr in dbAttributes"
                    :key="attr.key"
                    class="attribute-row"
                  >
                    <span class="attribute-key">{{ attr.key }}</span>
                    <span class="attribute-value">{{ formatValue(attr.value) }}</span>
                  </div>
                </div>
              </div>
            </div>
          </div>

          <!-- Events Tab -->
          <div v-if="activeTab === 'events'" class="events-section">
            <div v-if="!spanEvents || spanEvents.length === 0" class="empty-state">
              No events recorded for this span
            </div>
            <div v-else class="event-list">
              <div
                v-for="(event, index) in spanEvents"
                :key="index"
                class="event-item"
              >
                <div class="event-header">
                  <span class="event-name">{{ event.name }}</span>
                  <span class="event-time">{{ formatEventTime(event.timestamp) }}</span>
                </div>
                <div v-if="event.attributes" class="event-attributes">
                  <div
                    v-for="(value, key) in event.attributes"
                    :key="key"
                    class="attribute-row"
                  >
                    <span class="attribute-key">{{ key }}</span>
                    <span class="attribute-value">{{ formatValue(value) }}</span>
                  </div>
                </div>
              </div>
            </div>
          </div>

          <!-- Logs Tab -->
          <div v-if="activeTab === 'logs'" class="logs-section">
            <div v-if="!relatedLogs || relatedLogs.length === 0" class="empty-state">
              No logs correlated with this span
            </div>
            <div v-else class="log-list">
              <div
                v-for="log in relatedLogs"
                :key="log.id"
                class="log-item"
              >
                <div class="log-header">
                  <span :class="['log-level', `level-${log.severity_text?.toLowerCase()}`]">
                    {{ log.severity_text }}
                  </span>
                  <span class="log-time">{{ formatEventTime(log.timestamp) }}</span>
                </div>
                <div class="log-body">{{ log.body }}</div>
              </div>
            </div>
          </div>

          <!-- Exception Tab -->
          <div v-if="activeTab === 'exception'" class="exception-section">
            <div v-if="!spanException" class="empty-state">
              No exception occurred in this span
            </div>
            <div v-else class="exception-content">
              <div class="exception-header">
                <span class="exception-type">{{ spanException.exception_type }}</span>
              </div>
              <div class="exception-message">{{ spanException.message }}</div>
              <div v-if="spanException.stacktrace" class="exception-stacktrace">
                <h5 class="stacktrace-title">Stack Trace</h5>
                <pre class="stacktrace-content">{{ spanException.stacktrace }}</pre>
              </div>
            </div>
          </div>

          <!-- Context Tab -->
          <div v-if="activeTab === 'context'" class="context-section">
            <div class="context-grid">
              <div class="context-item">
                <span class="context-label">Trace ID</span>
                <span class="context-value font-mono">{{ span.trace_id }}</span>
              </div>
              <div class="context-item">
                <span class="context-label">Span ID</span>
                <span class="context-value font-mono">{{ span.span_id }}</span>
              </div>
              <div v-if="span.parent_span_id" class="context-item">
                <span class="context-label">Parent Span ID</span>
                <span class="context-value font-mono">{{ span.parent_span_id }}</span>
              </div>
              <div class="context-item">
                <span class="context-label">Start Time</span>
                <span class="context-value">{{ formatTimestamp(span.timestamp) }}</span>
              </div>
              <div class="context-item">
                <span class="context-label">End Time</span>
                <span class="context-value">{{ formatTimestamp(span.end_timestamp) }}</span>
              </div>
            </div>

            <!-- Links -->
            <div v-if="span.links && span.links.length > 0" class="links-section">
              <h4 class="section-title">Span Links</h4>
              <div class="link-list">
                <div v-for="(link, index) in span.links" :key="index" class="link-item">
                  <span class="link-trace">{{ link.trace_id }}</span>
                  <span class="link-span">{{ link.span_id }}</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      <!-- Footer -->
      <div class="drawer-footer">
        <button v-if="span.parent_span_id" @click="goToParent" class="footer-btn">
          <svg class="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 16l-4-4m0 0l4-4m-4 4h18" />
          </svg>
          Go to Parent
        </button>
        <button @click="copySpanId" class="footer-btn">
          <svg class="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
          </svg>
          Copy Span ID
        </button>
      </div>
    </div>
  </Transition>
</template>

<script setup>
import { ref, computed, watch } from 'vue'
import { format } from 'date-fns'

const props = defineProps({
  isOpen: {
    type: Boolean,
    default: false,
  },
  span: {
    type: Object,
    default: null,
  },
  allSpans: {
    type: Array,
    default: () => [],
  },
  exceptions: {
    type: Array,
    default: () => [],
  },
  logs: {
    type: Array,
    default: () => [],
  },
})

const emit = defineEmits(['close', 'select-span'])

const activeTab = ref('attributes')

// Calculate self time
const selfTime = computed(() => {
  if (!props.span) return 0
  
  const spanDuration = (props.span.duration_ns || 0) / 1_000_000
  const children = props.allSpans.filter(s => s.parent_span_id === props.span.span_id)
  const childrenDuration = children.reduce((sum, child) => {
    return sum + (child.duration_ns || 0) / 1_000_000
  }, 0)
  
  return Math.max(0, spanDuration - childrenDuration)
})

// Span attributes
const spanAttributes = computed(() => {
  return props.span?.attributes || {}
})

// Categorize attributes
const resourceAttributes = computed(() => {
  return Object.entries(spanAttributes.value)
    .filter(([key]) => key.startsWith('resource.') || key.startsWith('service.'))
    .map(([key, value]) => ({ key, value }))
})

const httpAttributes = computed(() => {
  return Object.entries(spanAttributes.value)
    .filter(([key]) => key.startsWith('http.'))
    .map(([key, value]) => ({ key, value }))
})

const dbAttributes = computed(() => {
  return Object.entries(spanAttributes.value)
    .filter(([key]) => key.startsWith('db.'))
    .map(([key, value]) => ({ key, value }))
})

const spanAttributesList = computed(() => {
  const excluded = ['resource.', 'service.', 'http.', 'db.']
  return Object.entries(spanAttributes.value)
    .filter(([key]) => !excluded.some(prefix => key.startsWith(prefix)))
    .map(([key, value]) => ({ key, value }))
})

// Span events
const spanEvents = computed(() => {
  return props.span?.events || []
})

// Related logs (correlated by span_id)
const relatedLogs = computed(() => {
  if (!props.span || !props.logs) return []
  return props.logs.filter(log => log.span_id === props.span.span_id)
})

// Exception for this span
const spanException = computed(() => {
  if (!props.span || !props.exceptions) return null
  return props.exceptions.find(e => e.span_id === props.span.span_id)
})

// Dynamic tabs based on available data
const tabs = computed(() => {
  const tabList = [
    { id: 'attributes', label: 'Attributes', count: Object.keys(spanAttributes.value).length },
    { id: 'events', label: 'Events', count: spanEvents.value.length },
    { id: 'logs', label: 'Logs', count: relatedLogs.value.length },
  ]
  
  if (spanException.value) {
    tabList.push({ id: 'exception', label: 'Exception', count: 1 })
  }
  
  tabList.push({ id: 'context', label: 'Context' })
  
  return tabList
})

// Format helpers
const formatDuration = (ms) => {
  if (ms < 1) return '<1ms'
  if (ms < 1000) return `${Math.round(ms)}ms`
  if (ms < 60000) return `${(ms / 1000).toFixed(2)}s`
  return `${(ms / 60000).toFixed(2)}m`
}

const formatSpanKind = (kind) => {
  if (!kind) return 'Internal'
  return kind.replace('SPAN_KIND_', '').toLowerCase().replace(/^\w/, c => c.toUpperCase())
}

const formatStatus = (status) => {
  if (!status) return 'Unset'
  return status.replace('STATUS_CODE_', '')
}

const formatValue = (value) => {
  if (typeof value === 'object') {
    return JSON.stringify(value, null, 2)
  }
  return String(value)
}

const formatEventTime = (timestamp) => {
  if (!timestamp) return ''
  try {
    return format(new Date(timestamp), 'HH:mm:ss.SSS')
  } catch {
    return timestamp
  }
}

const formatTimestamp = (timestamp) => {
  if (!timestamp) return ''
  try {
    return format(new Date(timestamp), 'yyyy-MM-dd HH:mm:ss.SSS')
  } catch {
    return timestamp
  }
}

const getStatusClass = (status) => {
  if (status === 'STATUS_CODE_ERROR') return 'status-error'
  if (status === 'STATUS_CODE_OK') return 'status-ok'
  return 'status-unset'
}

const getStatusTextClass = (status) => {
  if (status === 'STATUS_CODE_ERROR') return 'text-red-500'
  if (status === 'STATUS_CODE_OK') return 'text-green-500'
  return 'text-gray-500'
}

// Actions
const close = () => {
  emit('close')
}

const goToParent = () => {
  if (props.span?.parent_span_id) {
    const parentSpan = props.allSpans.find(s => s.span_id === props.span.parent_span_id)
    if (parentSpan) {
      emit('select-span', parentSpan)
    }
  }
}

const copySpanId = async () => {
  if (props.span?.span_id) {
    try {
      await navigator.clipboard.writeText(props.span.span_id)
    } catch (err) {
      console.error('Failed to copy:', err)
    }
  }
}

// Reset tab when span changes
watch(() => props.span, () => {
  activeTab.value = 'attributes'
})
</script>

<style scoped>
.span-details-drawer {
  @apply fixed right-0 top-0 h-full w-[420px] max-w-full bg-white dark:bg-gray-800 border-l border-gray-200 dark:border-gray-700 shadow-xl z-50 flex flex-col;
}

.drawer-header {
  @apply flex items-center justify-between px-4 py-4 border-b border-gray-200 dark:border-gray-700;
}

.drawer-title {
  @apply text-lg font-semibold text-gray-900 dark:text-gray-100 truncate max-w-[280px];
}

.drawer-subtitle {
  @apply text-sm text-gray-500 dark:text-gray-400;
}

.status-dot {
  @apply w-3 h-3 rounded-full flex-shrink-0;
}

.status-ok {
  @apply bg-green-500;
}

.status-error {
  @apply bg-red-500;
}

.status-unset {
  @apply bg-gray-400;
}

.close-btn {
  @apply p-2 text-gray-400 hover:text-gray-600 dark:hover:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700 rounded-md transition-colors;
}

.drawer-content {
  @apply flex-1 overflow-y-auto;
}

.stats-grid {
  @apply grid grid-cols-2 gap-3 p-4 border-b border-gray-200 dark:border-gray-700;
}

.stat-item {
  @apply flex flex-col;
}

.stat-label {
  @apply text-xs text-gray-500 dark:text-gray-400;
}

.stat-value {
  @apply text-sm font-semibold text-gray-900 dark:text-gray-100;
}

.drawer-tabs {
  @apply flex items-center gap-1 px-4 py-2 border-b border-gray-200 dark:border-gray-700 overflow-x-auto;
}

.tab-btn {
  @apply flex items-center gap-1 px-3 py-1.5 text-sm text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-100 rounded-md transition-colors whitespace-nowrap;
}

.tab-btn.active {
  @apply bg-primary-50 dark:bg-primary-900/20 text-primary-600 dark:text-primary-400;
}

.tab-count {
  @apply px-1.5 py-0.5 text-xs bg-gray-100 dark:bg-gray-700 rounded-full;
}

.tab-content {
  @apply p-4;
}

.empty-state {
  @apply text-center py-8 text-gray-500 dark:text-gray-400;
}

.attribute-groups {
  @apply space-y-4;
}

.attribute-group {
  @apply space-y-2;
}

.group-title {
  @apply text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider;
}

.attribute-list {
  @apply space-y-1;
}

.attribute-row {
  @apply flex items-start justify-between gap-2 py-1.5 px-2 rounded hover:bg-gray-50 dark:hover:bg-gray-700/50;
}

.attribute-key {
  @apply text-sm text-gray-600 dark:text-gray-400 font-mono flex-shrink-0;
}

.attribute-value {
  @apply text-sm text-gray-900 dark:text-gray-100 text-right break-all;
}

.event-list {
  @apply space-y-3;
}

.event-item {
  @apply border border-gray-200 dark:border-gray-700 rounded-lg overflow-hidden;
}

.event-header {
  @apply flex items-center justify-between px-3 py-2 bg-gray-50 dark:bg-gray-900;
}

.event-name {
  @apply text-sm font-medium text-gray-900 dark:text-gray-100;
}

.event-time {
  @apply text-xs text-gray-500 dark:text-gray-400 font-mono;
}

.event-attributes {
  @apply p-3 space-y-1;
}

.log-list {
  @apply space-y-3;
}

.log-item {
  @apply border border-gray-200 dark:border-gray-700 rounded-lg overflow-hidden;
}

.log-header {
  @apply flex items-center justify-between px-3 py-2 bg-gray-50 dark:bg-gray-900;
}

.log-level {
  @apply text-xs font-medium px-2 py-0.5 rounded;
}

.level-error {
  @apply bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-300;
}

.level-warn {
  @apply bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-300;
}

.level-info {
  @apply bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300;
}

.level-debug {
  @apply bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300;
}

.log-time {
  @apply text-xs text-gray-500 dark:text-gray-400 font-mono;
}

.log-body {
  @apply px-3 py-2 text-sm text-gray-700 dark:text-gray-300;
}

.exception-content {
  @apply space-y-3;
}

.exception-header {
  @apply flex items-center gap-2;
}

.exception-type {
  @apply text-sm font-medium text-red-600 dark:text-red-400 font-mono;
}

.exception-message {
  @apply text-sm text-gray-900 dark:text-gray-100 p-3 bg-red-50 dark:bg-red-900/20 rounded-lg;
}

.exception-stacktrace {
  @apply mt-4;
}

.stacktrace-title {
  @apply text-xs font-medium text-gray-500 dark:text-gray-400 mb-2;
}

.stacktrace-content {
  @apply text-xs font-mono text-gray-700 dark:text-gray-300 bg-gray-50 dark:bg-gray-900 p-3 rounded-lg overflow-x-auto max-h-64;
}

.context-grid {
  @apply space-y-3;
}

.context-item {
  @apply flex flex-col gap-1;
}

.context-label {
  @apply text-xs text-gray-500 dark:text-gray-400;
}

.context-value {
  @apply text-sm text-gray-900 dark:text-gray-100 break-all;
}

.section-title {
  @apply text-sm font-medium text-gray-900 dark:text-gray-100 mt-4 mb-2;
}

.link-list {
  @apply space-y-2;
}

.link-item {
  @apply flex items-center gap-2 text-xs font-mono;
}

.link-trace {
  @apply text-gray-600 dark:text-gray-400;
}

.link-span {
  @apply text-gray-900 dark:text-gray-100;
}

.drawer-footer {
  @apply flex items-center gap-2 px-4 py-3 border-t border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-900;
}

.footer-btn {
  @apply flex items-center px-3 py-1.5 text-sm text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-100 hover:bg-gray-100 dark:hover:bg-gray-800 rounded-md transition-colors;
}

/* Drawer transition */
.drawer-enter-active,
.drawer-leave-active {
  transition: transform 0.3s ease;
}

.drawer-enter-from,
.drawer-leave-to {
  transform: translateX(100%);
}
</style>
