<template>
  <div class="annotated-source border border-gray-200 rounded-lg overflow-hidden">
    <!-- Header -->
    <div class="flex items-center justify-between px-4 py-2 bg-gray-50 border-b border-gray-200">
      <div class="flex items-center gap-2 min-w-0">
        <svg class="w-4 h-4 text-gray-400 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 20l4-16m4 4l4 4-4 4M6 16l-4-4 4-4" />
        </svg>
        <span class="text-sm font-medium text-gray-700 truncate" :title="filePath">
          {{ filePath }}
        </span>
        <span v-if="functionName" class="text-xs text-gray-500 flex-shrink-0">
          {{ functionName }}
        </span>
      </div>
      <div class="flex items-center gap-2 flex-shrink-0">
        <a
          v-if="htmlUrl"
          :href="htmlUrl"
          target="_blank"
          rel="noopener noreferrer"
          class="inline-flex items-center gap-1 px-2 py-1 text-xs font-medium text-gray-600 hover:text-gray-900 transition-colors"
        >
          <svg class="w-3.5 h-3.5" fill="currentColor" viewBox="0 0 24 24">
            <path d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z"/>
          </svg>
          View on GitHub
        </a>
        <button
          @click="$emit('close')"
          class="p-1 text-gray-400 hover:text-gray-600 transition-colors"
        >
          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      </div>
    </div>

    <!-- Source code -->
    <div class="source-lines overflow-auto max-h-[500px]" ref="scrollContainer">
      <table class="w-full text-xs font-mono">
        <tbody>
          <tr
            v-for="(line, idx) in sourceLines"
            :key="idx"
            :ref="el => { if (idx + 1 === highlightLine) highlightRef = el }"
            :class="[
              'source-line',
              idx + 1 === highlightLine ? 'bg-yellow-100' : '',
              annotations[idx + 1] ? 'has-samples' : '',
            ]"
          >
            <!-- Sample count gutter -->
            <td class="gutter-samples text-right pr-1 select-none whitespace-nowrap" :style="gutterStyle(idx + 1)">
              <span v-if="annotations[idx + 1]" class="text-[10px]">
                {{ annotations[idx + 1] }}
              </span>
            </td>
            <!-- Heat bar -->
            <td class="gutter-heat w-1 p-0">
              <div
                v-if="annotations[idx + 1]"
                class="h-full"
                :style="{ backgroundColor: heatColor(annotations[idx + 1]), width: '4px', minHeight: '16px' }"
              ></div>
            </td>
            <!-- Line number -->
            <td class="gutter-line text-right pr-3 pl-2 select-none text-gray-400 whitespace-nowrap">
              {{ idx + 1 }}
            </td>
            <!-- Code -->
            <td class="code-content pr-4 whitespace-pre text-gray-800">{{ line }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, watch, nextTick, onMounted } from 'vue'

const props = defineProps({
  sourceCode: { type: String, required: true },
  annotations: { type: Object, default: () => ({}) },
  highlightLine: { type: Number, default: null },
  filePath: { type: String, default: '' },
  functionName: { type: String, default: '' },
  htmlUrl: { type: String, default: null },
})

defineEmits(['close'])

const scrollContainer = ref(null)
const highlightRef = ref(null)

const sourceLines = computed(() => {
  return props.sourceCode.split('\n')
})

const maxSamples = computed(() => {
  const values = Object.values(props.annotations)
  return values.length > 0 ? Math.max(...values) : 0
})

const gutterStyle = (lineNum) => {
  const count = props.annotations[lineNum]
  if (!count || !maxSamples.value) return {}
  const intensity = count / maxSamples.value
  const alpha = 0.05 + intensity * 0.2
  return {
    backgroundColor: `rgba(239, 68, 68, ${alpha})`,
  }
}

const heatColor = (count) => {
  if (!count || !maxSamples.value) return 'transparent'
  const intensity = count / maxSamples.value
  // Gradient from yellow to red
  const r = 239
  const g = Math.round(180 - intensity * 112)
  const b = Math.round(68 - intensity * 40)
  return `rgb(${r}, ${g}, ${b})`
}

// Scroll to highlighted line on mount / when it changes
const scrollToHighlight = async () => {
  await nextTick()
  if (highlightRef.value && scrollContainer.value) {
    highlightRef.value.scrollIntoView({ behavior: 'smooth', block: 'center' })
  }
}

watch(() => props.highlightLine, scrollToHighlight)
onMounted(scrollToHighlight)
</script>

<style scoped>
.source-lines {
  font-size: 12px;
  line-height: 1.4;
}

.source-line:hover {
  background-color: rgba(59, 130, 246, 0.06);
}

.gutter-samples {
  min-width: 40px;
  color: #dc2626;
  font-weight: 600;
}

.code-content {
  tab-size: 4;
}
</style>
