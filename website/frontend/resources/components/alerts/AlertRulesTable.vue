<template>
  <div class="overflow-x-auto">
    <table class="min-w-full divide-y divide-gray-200">
      <thead class="bg-gray-50">
        <tr>
          <th
            v-for="column in columns"
            :key="column.key"
            scope="col"
            class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
            :class="column.align === 'right' ? 'text-right' : 'text-left'"
          >
            {{ column.label }}
          </th>
        </tr>
      </thead>
      <tbody class="bg-white divide-y divide-gray-200">
        <tr
          v-for="rule in rules"
          :key="rule.id"
          class="hover:bg-gray-50 transition-colors cursor-pointer"
          @click="() => $emit('row-click', rule)"
        >
          <td class="px-6 py-4 whitespace-nowrap">
            <div class="flex flex-col">
              <div class="text-sm font-medium text-gray-900">
                {{ rule.name }}
              </div>
              <div v-if="rule.description" class="text-xs text-gray-500 mt-1">
                {{ rule.description }}
              </div>
            </div>
          </td>
          <td class="px-6 py-4">
            <div class="text-sm text-gray-900">
              <div class="font-mono text-xs">{{ getQueryDisplay(rule) }}</div>
              <div class="text-xs text-gray-500 mt-1">
                {{ formatCondition(rule) }}
              </div>
            </div>
          </td>
          <td class="px-6 py-4 whitespace-nowrap">
            <AlertStatusBadge type="enabled" :value="rule.enabled" />
          </td>
          <td class="px-6 py-4 whitespace-nowrap">
            <div class="text-sm text-gray-500">
              {{ rule.notification_channels?.length || 0 }} channel{{ rule.notification_channels?.length !== 1 ? 's' : '' }}
            </div>
          </td>
          <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
            <div v-if="rule.last_evaluated_at">
              {{ formatRelativeTime(rule.last_evaluated_at) }}
            </div>
            <div v-else class="text-gray-400 italic">Never</div>
          </td>
          <td class="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
            <div class="flex items-center justify-end gap-2" @click.stop>
              <button
                @click="() => $emit('toggle', rule)"
                class="text-primary-600 hover:text-primary-900"
                :title="rule.enabled ? 'Disable' : 'Enable'"
              >
                <svg v-if="rule.enabled" class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M18.364 18.364A9 9 0 005.636 5.636m12.728 12.728A9 9 0 015.636 5.636m12.728 12.728L5.636 5.636" />
                </svg>
                <svg v-else class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
                </svg>
              </button>
              <button
                @click="() => $emit('edit', rule)"
                class="text-primary-600 hover:text-primary-900"
                title="Edit"
              >
                <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z" />
                </svg>
              </button>
              <button
                @click="() => $emit('delete', rule)"
                class="text-red-600 hover:text-red-900"
                title="Delete"
              >
                <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
                </svg>
              </button>
            </div>
          </td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<script setup>
import { formatDistanceToNow } from 'date-fns';
import AlertStatusBadge from './AlertStatusBadge.vue';

const props = defineProps({
  rules: {
    type: Array,
    required: true,
    default: () => [],
  },
  columns: {
    type: Array,
    default: () => [
      { key: 'name', label: 'Name', align: 'left' },
      { key: 'condition', label: 'Query / Condition', align: 'left' },
      { key: 'status', label: 'Status', align: 'left' },
      { key: 'channels', label: 'Channels', align: 'left' },
      { key: 'last_evaluated', label: 'Last Evaluated', align: 'left' },
      { key: 'actions', label: '', align: 'right' },
    ],
  },
});

defineEmits(['row-click', 'toggle', 'edit', 'delete']);

const getQueryDisplay = (rule) => {
  const qc = rule.query_config;
  if (!qc) return 'N/A';

  switch (qc.query_type) {
    case 'promql': {
      const expr = qc.promql || '';
      return expr.length > 40 ? `PromQL: ${expr.slice(0, 37)}…` : `PromQL: ${expr}`;
    }
    case 'log_pattern': {
      const patterns = qc.patterns || [];
      if (patterns.length === 1) return `Log: "${patterns[0]}"`;
      return `Log: ${patterns.length} patterns`;
    }
    case 'llm': {
      const model = qc.filters?.model;
      return model ? `${qc.metric_name} (${model})` : qc.metric_name || 'N/A';
    }
    case 'metrics':
    default: {
      if (qc.patterns && qc.patterns.length > 0) {
        return qc.patterns.length === 1 ? `Log: "${qc.patterns[0]}"` : `Log: ${qc.patterns.length} patterns`;
      }
      if (qc.promql) {
        return qc.promql.length > 40 ? `PromQL: ${qc.promql.slice(0, 37)}…` : `PromQL: ${qc.promql}`;
      }
      if (!qc.metric_name) return 'N/A';
      const model = qc.filters?.model;
      return model ? `${qc.metric_name} (${model})` : qc.metric_name;
    }
  }
};

// Format condition string - simplified single threshold
const formatCondition = (rule) => {
  const threshold = rule.threshold;
  const thresholdType = rule.threshold_type || 'above';
  
  if (threshold !== null && threshold !== undefined) {
    const op = thresholdType === 'above' ? '>' : '<';
    return `${op} ${threshold}`;
  }
  return 'N/A';
};

// Format relative time
const formatRelativeTime = (dateString) => {
  try {
    const date = new Date(dateString);
    return formatDistanceToNow(date, { addSuffix: true });
  } catch {
    return dateString;
  }
};
</script>
