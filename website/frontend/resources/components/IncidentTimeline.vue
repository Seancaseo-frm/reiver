<template>
  <div class="incident-timeline">
    <div v-if="!events || events.length === 0" class="text-sm text-gray-500 py-4">No events in this window.</div>
    <div v-else class="relative">
      <div class="space-y-1 max-h-80 overflow-y-auto">
        <div
          v-for="(e, i) in events"
          :key="i"
          class="flex items-start gap-3 py-1.5 px-2 rounded hover:bg-gray-50"
        >
          <span class="text-xs text-gray-500 whitespace-nowrap">{{ formatTime(e.time) }}</span>
          <span
            :class="[
              'shrink-0 px-1.5 py-0.5 rounded text-xs font-medium',
              typeClass(e.type)
            ]"
          >{{ e.type }}</span>
          <span class="text-sm text-gray-900 truncate min-w-0">
            {{ summary(e) }}
          </span>
          <router-link
            v-if="e.trace_id && projectId"
            :to="`/p/${projectId}/traces/${e.trace_id}`"
            class="shrink-0 text-xs text-primary-600 hover:underline"
          >View trace</router-link>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed } from 'vue'

const props = defineProps({
  events: { type: Array, default: () => [] },
  projectId: { type: String, default: '' },
})

function formatTime(t) {
  if (!t) return '—'
  return new Date(t).toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit', fractionalSecondDigits: 3 })
}

function typeClass(type) {
  switch (type) {
    case 'log': return 'bg-blue-100 text-blue-800'
    case 'trace': return 'bg-purple-100 text-purple-800'
    case 'alert': return 'bg-amber-100 text-amber-800'
    default: return 'bg-gray-100 text-gray-800'
  }
}

function summary(e) {
  if (e.template) return e.template.length > 120 ? e.template.slice(0, 120) + '…' : e.template
  if (e.message) return e.message.length > 120 ? e.message.slice(0, 120) + '…' : e.message
  if (e.type === 'trace' && e.trace_id) return `${e.trace_id} · ${e.span_count || 0} spans · ${((e.duration_ns || 0) / 1_000_000).toFixed(0)}ms`
  if (e.type === 'alert' && e.rule_name) return `${e.rule_name} · ${e.alert_state || ''}`
  return '—'
}
</script>
