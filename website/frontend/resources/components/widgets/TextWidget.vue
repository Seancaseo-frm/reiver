<template>
  <div class="text-widget" v-html="renderedContent"></div>
</template>

<script setup>
import { computed } from 'vue'

const props = defineProps({
  config: {
    type: Object,
    required: true,
  },
  projectId: { type: String, default: '' },
  timeRange: { type: String, default: '1h' },
  variables: { type: Object, default: () => ({}) },
})

const renderedContent = computed(() => {
  const raw = props.config.content || ''
  // Basic markdown-like rendering for section headers
  return raw
    .replace(/^### (.+)$/gm, '<h3>$1</h3>')
    .replace(/^## (.+)$/gm, '<h2>$1</h2>')
    .replace(/^# (.+)$/gm, '<h1>$1</h1>')
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    .replace(/\*(.+?)\*/g, '<em>$1</em>')
    .replace(/\n/g, '<br/>')
})
</script>

<style scoped>
.text-widget {
  @apply px-2 py-1 text-gray-700 text-sm leading-relaxed flex items-center h-full;
}

.text-widget :deep(h1) {
  @apply text-lg font-semibold text-gray-900 mb-0;
}

.text-widget :deep(h2) {
  @apply text-base font-semibold text-gray-800 mb-0;
}

.text-widget :deep(h3) {
  @apply text-sm font-medium text-gray-700 mb-0;
}
</style>
