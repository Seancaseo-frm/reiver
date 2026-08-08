<template>
  <BaseCard class="widget-card h-full flex flex-col">
    <!-- Widget Header -->
    <div v-if="widget.title" class="widget-header px-4 py-3 border-b border-gray-200">
      <h3 class="text-sm font-semibold text-gray-900">{{ widget.title }}</h3>
    </div>

    <!-- Widget Content -->
    <div class="widget-content flex-1 p-4 overflow-auto">
      <!-- Stat Widget -->
      <StatWidget
        v-if="widget.widget_type === 'stat'"
        :config="widget.widget_config"
        :project-id="projectId"
        :time-range="timeRange"
      />

      <!-- Error List Widget -->
      <ErrorListWidget
        v-else-if="widget.widget_type === 'error_list'"
        :config="widget.widget_config"
        :project-id="projectId"
        :dashboard-id="dashboardId"
      />

      <!-- Trace List Widget -->
      <TraceListWidget
        v-else-if="widget.widget_type === 'trace_list'"
        :config="widget.widget_config"
        :project-id="projectId"
        :time-range="timeRange"
      />

      <!-- Unknown Widget Type -->
      <div v-else class="text-center text-gray-400 py-8">
        <p>Unknown widget type: {{ widget.widget_type }}</p>
      </div>
    </div>
  </BaseCard>
</template>

<script setup>
import BaseCard from './BaseCard.vue'
import StatWidget from './widgets/StatWidget.vue'
import ErrorListWidget from './widgets/ErrorListWidget.vue'
import TraceListWidget from './widgets/TraceListWidget.vue'

defineProps({
  widget: {
    type: Object,
    required: true,
  },
  projectId: {
    type: String,
    required: true,
  },
  timeRange: {
    type: String,
    default: '1h',
  },
  dashboardId: {
    type: String,
    required: true,
  },
})
</script>

<style scoped>
.widget-card {
  @apply bg-gray-50 border-gray-200;
}

.widget-header {
  @apply flex-shrink-0;
}

.widget-content {
  @apply flex-1 min-h-0;
}
</style>


