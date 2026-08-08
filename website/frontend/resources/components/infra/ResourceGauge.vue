<template>
  <div class="resource-gauge">
    <div class="gauge-container">
      <svg :width="size" :height="size / 2 + 20" class="gauge-svg">
        <!-- Background arc -->
        <path
          :d="backgroundArc"
          fill="none"
          stroke="currentColor"
          :stroke-width="strokeWidth"
          class="text-gray-200 dark:text-gray-700"
          stroke-linecap="round"
        />
        
        <!-- Value arc -->
        <path
          :d="valueArc"
          fill="none"
          :stroke="gaugeColor"
          :stroke-width="strokeWidth"
          stroke-linecap="round"
        />
        
        <!-- Threshold markers -->
        <line
          v-for="(threshold, index) in thresholds"
          :key="index"
          :x1="getThresholdX(threshold, 'start')"
          :y1="getThresholdY(threshold, 'start')"
          :x2="getThresholdX(threshold, 'end')"
          :y2="getThresholdY(threshold, 'end')"
          :stroke="getThresholdColor(index)"
          stroke-width="2"
        />
      </svg>
      
      <!-- Value display -->
      <div class="gauge-value">
        <span class="value-number">{{ displayValue }}</span>
        <span class="value-unit">{{ unit }}</span>
      </div>
    </div>
    
    <!-- Label -->
    <div class="gauge-label">{{ label }}</div>
    
    <!-- Legend -->
    <div v-if="thresholds && thresholds.length > 0" class="gauge-legend">
      <div class="legend-item">
        <span class="legend-dot bg-green-500"></span>
        <span class="legend-text">&lt; {{ thresholds[0] }}{{ unit }}</span>
      </div>
      <div v-if="thresholds.length > 1" class="legend-item">
        <span class="legend-dot bg-yellow-500"></span>
        <span class="legend-text">{{ thresholds[0] }}-{{ thresholds[1] }}{{ unit }}</span>
      </div>
      <div class="legend-item">
        <span class="legend-dot bg-red-500"></span>
        <span class="legend-text">&gt; {{ thresholds[thresholds.length - 1] }}{{ unit }}</span>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed } from 'vue'

const props = defineProps({
  value: {
    type: Number,
    default: 0,
  },
  max: {
    type: Number,
    default: 100,
  },
  unit: {
    type: String,
    default: '%',
  },
  label: {
    type: String,
    default: '',
  },
  thresholds: {
    type: Array,
    default: () => [70, 85],
  },
  size: {
    type: Number,
    default: 200,
  },
})

const strokeWidth = 16
const radius = computed(() => (props.size - strokeWidth) / 2)
const centerX = computed(() => props.size / 2)
const centerY = computed(() => props.size / 2)

// Calculate display value (clamped)
const displayValue = computed(() => {
  return Math.min(Math.max(props.value, 0), props.max).toFixed(1)
})

// Calculate percentage
const percentage = computed(() => {
  return Math.min(Math.max(props.value / props.max, 0), 1)
})

// Generate arc path
const generateArc = (startAngle, endAngle) => {
  const startRad = (startAngle * Math.PI) / 180
  const endRad = (endAngle * Math.PI) / 180
  
  const x1 = centerX.value + radius.value * Math.cos(startRad)
  const y1 = centerY.value + radius.value * Math.sin(startRad)
  const x2 = centerX.value + radius.value * Math.cos(endRad)
  const y2 = centerY.value + radius.value * Math.sin(endRad)
  
  const largeArc = endAngle - startAngle > 180 ? 1 : 0
  
  return `M ${x1} ${y1} A ${radius.value} ${radius.value} 0 ${largeArc} 1 ${x2} ${y2}`
}

// Background arc (full semicircle)
const backgroundArc = computed(() => {
  return generateArc(180, 360)
})

// Value arc
const valueArc = computed(() => {
  const endAngle = 180 + percentage.value * 180
  return generateArc(180, endAngle)
})

// Gauge color based on thresholds
const gaugeColor = computed(() => {
  if (!props.thresholds || props.thresholds.length === 0) {
    return '#3B82F6'
  }
  
  if (props.value >= props.thresholds[props.thresholds.length - 1]) {
    return '#EF4444' // Red
  }
  
  if (props.thresholds.length > 1 && props.value >= props.thresholds[0]) {
    return '#F59E0B' // Yellow
  }
  
  return '#10B981' // Green
})

// Threshold marker positions
const getThresholdX = (threshold, position) => {
  const angle = 180 + (threshold / props.max) * 180
  const rad = (angle * Math.PI) / 180
  const offset = position === 'start' ? radius.value - 10 : radius.value + 10
  return centerX.value + offset * Math.cos(rad)
}

const getThresholdY = (threshold, position) => {
  const angle = 180 + (threshold / props.max) * 180
  const rad = (angle * Math.PI) / 180
  const offset = position === 'start' ? radius.value - 10 : radius.value + 10
  return centerY.value + offset * Math.sin(rad)
}

const getThresholdColor = (index) => {
  return index === 0 ? '#F59E0B' : '#EF4444'
}
</script>

<style scoped>
.resource-gauge {
  @apply flex flex-col items-center;
}

.gauge-container {
  @apply relative;
}

.gauge-svg {
  @apply block;
}

.gauge-value {
  @apply absolute bottom-4 left-1/2 transform -translate-x-1/2 text-center;
}

.value-number {
  @apply text-3xl font-bold text-gray-900 dark:text-gray-100;
}

.value-unit {
  @apply text-lg text-gray-500 dark:text-gray-400 ml-1;
}

.gauge-label {
  @apply text-sm font-medium text-gray-600 dark:text-gray-400 mt-2;
}

.gauge-legend {
  @apply flex items-center justify-center gap-4 mt-4;
}

.legend-item {
  @apply flex items-center gap-1;
}

.legend-dot {
  @apply w-2 h-2 rounded-full;
}

.legend-text {
  @apply text-xs text-gray-500 dark:text-gray-400;
}
</style>
