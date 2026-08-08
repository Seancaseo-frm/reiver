<template>
  <div class="filter-section">
    <h4 class="filter-title">{{ title }}</h4>
    <div class="filter-content">
      <slot></slot>
    </div>
  </div>
</template>

<script setup>
defineProps({
  title: {
    type: String,
    required: true
  }
})
</script>

<style scoped>
.filter-section {
  @apply mb-4;
}

.filter-title {
  @apply text-xs font-semibold text-gray-700 dark:text-gray-300 mb-2 uppercase tracking-wide;
}

.filter-content {
  @apply space-y-1;
}
</style>
