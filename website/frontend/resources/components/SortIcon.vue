<template>
  <span class="sort-icon" :class="{ active }">
    <svg v-if="!active" class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 16V4m0 0L3 8m4-4l4 4m6 0v12m0 0l4-4m-4 4l-4-4" />
    </svg>
    <svg v-else-if="direction === 'asc'" class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 15l7-7 7 7" />
    </svg>
    <svg v-else class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
    </svg>
  </span>
</template>

<script setup>
defineProps({
  active: {
    type: Boolean,
    default: false,
  },
  direction: {
    type: String,
    default: 'desc',
    validator: (value) => ['asc', 'desc'].includes(value),
  },
})
</script>

<style scoped>
.sort-icon {
  @apply text-gray-400;
}

.sort-icon.active {
  @apply text-primary-600 dark:text-primary-400;
}
</style>
