<template>
  <div class="flex justify-start">
    <div class="max-w-[85%] rounded-lg border border-gray-200 dark:border-gray-700 overflow-hidden text-sm">
      <!-- Header (always visible) -->
      <button
        @click="expanded = !expanded"
        class="w-full flex items-center gap-2 px-3 py-2 bg-gray-50 dark:bg-gray-800 hover:bg-gray-100 dark:hover:bg-gray-750 transition-colors"
      >
        <div v-if="isAnyRunning" class="tool-spinner flex-shrink-0"></div>
        <svg v-else class="w-4 h-4 text-green-500 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12.75L11.25 15 15 9.75M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
        </svg>
        <span class="font-mono text-xs text-gray-700 dark:text-gray-300 truncate">{{ summaryText }}</span>
        <svg
          class="w-3.5 h-3.5 text-gray-400 ml-auto flex-shrink-0 transition-transform"
          :class="{ 'rotate-180': expanded }"
          fill="none" stroke="currentColor" viewBox="0 0 24 24"
        >
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
        </svg>
      </button>

      <!-- Expanded details -->
      <div v-if="expanded" class="border-t border-gray-200 dark:border-gray-700 p-2 space-y-2">
        <template v-for="item in items" :key="item.id">
          <p v-if="item.role === 'assistant'" class="text-xs text-gray-500 dark:text-gray-400 px-1 italic">
            {{ item.content }}
          </p>
          <AgentToolCall
            v-else-if="item.role === 'tool_call'"
            :name="item.tool_name"
            :input="item.tool_input"
            :output="item.tool_output"
            :status="item.tool_status"
          />
          <AgentToolCall
            v-else-if="item.role === 'tool'"
            :name="item.tool_name"
            :input="null"
            :output="parseToolOutput(item.content)"
            status="done"
          />
        </template>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, watch } from 'vue';
import AgentToolCall from '@/components/AgentToolCall.vue';

const props = defineProps({
  items: { type: Array, required: true },
  projectId: { type: String, default: '' },
});

const expanded = ref(false);

const isAnyRunning = computed(() =>
  props.items.some(m => m.tool_status === 'running')
);

watch(isAnyRunning, (running) => {
  expanded.value = running;
});

const summaryText = computed(() => {
  const counts = {};
  for (const item of props.items) {
    if (item.role !== 'tool_call' && item.role !== 'tool') continue;
    const name = item.tool_name || 'tool';
    counts[name] = (counts[name] || 0) + 1;
  }
  return Object.entries(counts)
    .map(([name, count]) => count > 1 ? `${name} \u00d7${count}` : name)
    .join(', ');
});

function parseToolOutput(content) {
  if (!content) return null;
  try {
    return JSON.parse(content);
  } catch {
    return content;
  }
}
</script>

<style scoped>
.tool-spinner {
  width: 14px;
  height: 14px;
  border: 2px solid #d1d5db;
  border-top-color: #6366f1;
  border-radius: 50%;
  animation: tool-spin 0.6s linear infinite;
}

@keyframes tool-spin {
  to { transform: rotate(360deg); }
}
</style>
