<template>
  <div class="max-w-[85%] rounded-lg border border-gray-200 dark:border-gray-700 overflow-hidden text-sm">
    <!-- Header -->
    <button
      @click="expanded = !expanded"
      class="w-full flex items-center gap-2 px-3 py-2 bg-gray-50 dark:bg-gray-800 hover:bg-gray-100 dark:hover:bg-gray-750 transition-colors"
    >
      <div v-if="status === 'running'" class="tool-spinner flex-shrink-0"></div>
      <svg v-else class="w-4 h-4 text-green-500 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12.75L11.25 15 15 9.75M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
      </svg>
      <span class="font-mono text-xs text-gray-700 dark:text-gray-300 truncate">{{ name }}</span>
      <svg
        class="w-3.5 h-3.5 text-gray-400 ml-auto flex-shrink-0 transition-transform"
        :class="{ 'rotate-180': expanded }"
        fill="none" stroke="currentColor" viewBox="0 0 24 24"
      >
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
      </svg>
    </button>

    <!-- Expanded details -->
    <div v-if="expanded" class="border-t border-gray-200 dark:border-gray-700">
      <div v-if="input" class="px-3 py-2 border-b border-gray-100 dark:border-gray-750">
        <p class="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">Input</p>
        <pre class="text-xs text-gray-600 dark:text-gray-400 overflow-x-auto whitespace-pre-wrap break-words">{{ formatJson(input) }}</pre>
      </div>
      <div v-if="output != null" class="px-3 py-2">
        <p class="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">Output</p>
        <pre class="text-xs text-gray-600 dark:text-gray-400 overflow-x-auto whitespace-pre-wrap break-words max-h-48 overflow-y-auto">{{ formatJson(output) }}</pre>
      </div>
      <div v-else-if="status === 'running'" class="px-3 py-2">
        <p class="text-xs text-gray-400">Running...</p>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue';

defineProps({
  name: { type: String, required: true },
  input: { type: [Object, String, null], default: null },
  output: { type: [Object, String, null], default: null },
  status: { type: String, default: 'done' },
});

const expanded = ref(false);

function formatJson(val) {
  if (val == null) return '';
  if (typeof val === 'string') return val;
  try {
    return JSON.stringify(val, null, 2);
  } catch {
    return String(val);
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
