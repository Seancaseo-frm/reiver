<template>
  <div class="top-list-widget h-full flex flex-col">
    <div v-if="loading" class="flex-1 flex items-center justify-center">
      <div class="spinner w-6 h-6 border-2 border-primary-500 border-t-transparent rounded-full animate-spin"></div>
    </div>
    <div v-else-if="error" class="flex-1 flex items-center justify-center">
      <p class="text-sm text-danger-400">{{ error }}</p>
    </div>
    <div v-else-if="items.length === 0" class="flex-1 flex items-center justify-center">
      <p class="text-sm text-gray-400">No data available</p>
    </div>
    <div v-else class="flex-1 overflow-y-auto">
      <div
        v-for="(item, idx) in items"
        :key="idx"
        class="top-list-item"
      >
        <div class="top-list-bar-container">
          <div class="top-list-label">{{ item.label }}</div>
          <div class="top-list-value">{{ formatValue(item.value) }}</div>
        </div>
        <div class="top-list-bar-track">
          <div
            class="top-list-bar-fill"
            :style="{ width: barWidth(item.value) + '%' }"
          ></div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import { useWidgetQuery, parseTimeRange } from '@/composables/useWidgetQuery'
import { formatGrafanaUnit } from '@/utils/widgetTransforms'

const props = defineProps({
  config: { type: Object, required: true },
  projectId: { type: String, required: true },
  timeRange: { type: String, default: '1h' },
  variables: { type: Object, default: () => ({}) },
})

const { loading, error, executeQuery } = useWidgetQuery()
const items = ref([])

const widgetUnit = computed(() => props.config.unit || props.config.query?.unit)

const maxValue = computed(() => {
  if (items.value.length === 0) return 1
  return Math.max(...items.value.map(i => i.value), 1)
})

const barWidth = (value) => Math.min((value / maxValue.value) * 100, 100)

const formatValue = (value) => {
  return formatGrafanaUnit(value, widgetUnit.value || 'short')
}

const fetchData = async () => {
  if (!props.config.query) return
  try {
    const range = parseTimeRange(props.timeRange)
    const result = await executeQuery(props.projectId, props.config.query, range, props.variables)
    if (result?.data && result.data.length > 0) {
      const skipCols = new Set(['project_id', 'fingerprint', 'time_bucket', 'time', 'timestamp', 'unix_milli'])
      const labelCol = result.columns.find(c => !skipCols.has(c) && c !== 'value' && typeof result.data[0][c] === 'string')
      const valueCol = result.columns.find(c => c === 'value') || result.columns.find(c => !skipCols.has(c) && c !== labelCol)

      const grouped = new Map()
      for (const row of result.data) {
        const label = labelCol ? row[labelCol] : 'value'
        const value = parseFloat(row[valueCol]) || 0
        const existing = grouped.get(label)
        if (!existing || value > existing) {
          grouped.set(label, value)
        }
      }
      items.value = Array.from(grouped, ([label, value]) => ({ label, value }))
        .sort((a, b) => b.value - a.value)
        .slice(0, props.config.limit || 10)
    }
  } catch (err) {
    console.error('TopList query failed:', err)
  }
}

onMounted(fetchData)
watch([() => props.timeRange, () => props.variables], fetchData, { deep: true })
</script>

<style scoped>
.top-list-item {
  @apply px-3 py-2;
}

.top-list-item + .top-list-item {
  @apply border-t border-gray-100;
}

.top-list-bar-container {
  @apply flex items-center justify-between mb-1;
}

.top-list-label {
  @apply text-sm text-gray-700 truncate mr-3;
}

.top-list-value {
  @apply text-sm font-mono font-medium text-gray-900 flex-shrink-0;
}

.top-list-bar-track {
  @apply h-1.5 bg-gray-100 rounded-full overflow-hidden;
}

.top-list-bar-fill {
  @apply h-full bg-primary-500 rounded-full transition-all duration-300;
}
</style>
