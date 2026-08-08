<template>
  <span
    class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium"
    :class="badgeClass"
  >
    <span v-if="showDot" class="w-1.5 h-1.5 rounded-full mr-1.5" :class="dotClass"></span>
    {{ displayText }}
  </span>
</template>

<script setup>
import { computed } from 'vue';

const props = defineProps({
  type: {
    type: String,
    required: true,
    validator: (value) => ['enabled', 'state'].includes(value),
  },
  value: {
    type: [String, Boolean],
    required: true,
  },
  showDot: {
    type: Boolean,
    default: false,
  },
});

const badgeClass = computed(() => {
  if (props.type === 'enabled') {
    return props.value
      ? 'bg-green-100 text-green-800'
      : 'bg-gray-100 text-gray-800';
  }
  
  // Simplified state: OK or ALERT
  if (props.type === 'state') {
    const stateMap = {
      OK: 'bg-green-100 text-green-800',
      ALERT: 'bg-red-100 text-red-800',
    };
    return stateMap[props.value] || 'bg-gray-100 text-gray-800';
  }
  
  return 'bg-gray-100 text-gray-800';
});

const dotClass = computed(() => {
  if (props.type === 'enabled') {
    return props.value ? 'bg-green-600' : 'bg-gray-400';
  }
  
  if (props.type === 'state') {
    const stateMap = {
      OK: 'bg-green-600',
      ALERT: 'bg-red-600',
    };
    return stateMap[props.value] || 'bg-gray-400';
  }
  
  return 'bg-gray-400';
});

const displayText = computed(() => {
  if (props.type === 'enabled') {
    return props.value ? 'Enabled' : 'Disabled';
  }
  
  if (props.type === 'state') {
    const stateMap = {
      OK: 'OK',
      ALERT: 'Alert',
    };
    return stateMap[props.value] || props.value || 'Unknown';
  }
  
  return String(props.value);
});
</script>
