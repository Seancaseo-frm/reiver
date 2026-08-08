<template>
  <div class="tooltip-wrapper" @mouseenter="showTooltip" @mouseleave="hideTooltip">
    <slot />
    <div
      v-if="isVisible"
      ref="tooltip"
      class="tooltip-content"
      :class="placement"
      :style="{ left: tooltipLeft + 'px', top: tooltipTop + 'px' }"
    >
      {{ title }}
    </div>
  </div>
</template>

<script setup>
import { ref, computed } from 'vue'

const props = defineProps({
  title: {
    type: String,
    required: true,
  },
  placement: {
    type: String,
    default: 'top',
    validator: (value) => ['top', 'bottom', 'left', 'right'].includes(value),
  },
})

const isVisible = ref(false)
const tooltipLeft = ref(0)
const tooltipTop = ref(0)
const tooltip = ref(null)

const showTooltip = (event) => {
  isVisible.value = true

  // Position the tooltip
  const rect = event.target.getBoundingClientRect()
  const tooltipRect = tooltip.value?.getBoundingClientRect()

  if (tooltipRect) {
    switch (props.placement) {
      case 'top':
        tooltipLeft.value = rect.left + (rect.width / 2) - (tooltipRect.width / 2)
        tooltipTop.value = rect.top - tooltipRect.height - 8
        break
      case 'bottom':
        tooltipLeft.value = rect.left + (rect.width / 2) - (tooltipRect.width / 2)
        tooltipTop.value = rect.bottom + 8
        break
      case 'left':
        tooltipLeft.value = rect.left - tooltipRect.width - 8
        tooltipTop.value = rect.top + (rect.height / 2) - (tooltipRect.height / 2)
        break
      case 'right':
        tooltipLeft.value = rect.right + 8
        tooltipTop.value = rect.top + (rect.height / 2) - (tooltipRect.height / 2)
        break
    }
  }
}

const hideTooltip = () => {
  isVisible.value = false
}
</script>

<style scoped>
.tooltip-wrapper {
  @apply relative inline-block;
}

.tooltip-content {
  @apply absolute z-50 px-2 py-1 text-xs font-medium text-gray-900 bg-white border border-gray-200 rounded shadow-lg whitespace-nowrap pointer-events-none;
  max-width: 200px;
  word-wrap: break-word;
}

.tooltip-content.top {
  @apply transform -translate-x-1/2;
}

.tooltip-content.bottom {
  @apply transform -translate-x-1/2;
}

.tooltip-content.left {
  @apply transform -translate-y-1/2;
}

.tooltip-content.right {
  @apply transform -translate-y-1/2;
}
</style>