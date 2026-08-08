<template>
  <div class="checkbox-group">
    <slot></slot>
  </div>
</template>

<script setup>
import { provide } from 'vue'

const props = defineProps({
  modelValue: {
    type: Array,
    default: () => []
  }
})

const emit = defineEmits(['update:modelValue'])

provide('checkboxGroup', {
  value: props.modelValue,
  toggle: (val) => {
    const newValue = [...props.modelValue]
    const index = newValue.indexOf(val)
    if (index > -1) {
      newValue.splice(index, 1)
    } else {
      newValue.push(val)
    }
    emit('update:modelValue', newValue)
  }
})
</script>

<style scoped>
.checkbox-group {
  @apply space-y-2;
}
</style>
