<template>
  <label class="checkbox-label">
    <input
      type="checkbox"
      :checked="isChecked"
      @change="handleChange"
      class="checkbox-input"
    />
    <span class="checkbox-text">{{ label }}</span>
  </label>
</template>

<script setup>
import { inject, computed } from 'vue'

const props = defineProps({
  value: {
    type: String,
    required: true
  },
  label: {
    type: String,
    required: true
  }
})

const group = inject('checkboxGroup', null)

const isChecked = computed(() => {
  return group?.value?.includes(props.value) || false
})

const handleChange = () => {
  if (group) {
    group.toggle(props.value)
  }
}
</script>

<style scoped>
.checkbox-label {
  @apply flex items-center gap-2 cursor-pointer text-sm text-gray-700 hover:text-gray-900;
}

.checkbox-input {
  @apply w-4 h-4 text-primary-600 bg-white border-gray-300 rounded focus:ring-primary-500 focus:ring-2;
}

.checkbox-text {
  @apply select-none;
}
</style>
