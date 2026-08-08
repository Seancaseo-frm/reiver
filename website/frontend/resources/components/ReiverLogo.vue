<template>
  <svg
    :class="props.class"
    viewBox="0 0 32 32"
    fill="currentColor"
    xmlns="http://www.w3.org/2000/svg"
  >
    <!-- Geometric R mark: intersecting directional shapes -->
    <path d="M6 6 L16 16 L6 26 L12 26 L19 19 L26 26 L26 20 L22 16 L26 12 L26 6 L20 6 L16 10 L12 6 Z" />
  </svg>
</template>

<script setup>
const props = defineProps({
  class: { type: String, default: 'w-8 h-8' },
});
</script>
