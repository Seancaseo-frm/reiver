<template>
  <div class="gauge-container">
    <svg :viewBox="`0 0 ${size} ${size * 0.65}`" class="gauge-svg">
      <!-- Background arc -->
      <path
        :d="arcPath(startAngle, endAngle)"
        fill="none"
        :stroke="trackColor"
        :stroke-width="strokeWidth"
        stroke-linecap="round"
      />
      <!-- Threshold segments -->
      <path
        v-for="(seg, i) in thresholdSegments"
        :key="i"
        :d="arcPath(seg.startAngle, seg.endAngle)"
        fill="none"
        :stroke="seg.color"
        :stroke-width="strokeWidth"
        stroke-linecap="round"
      />
      <!-- Value arc -->
      <path
        v-if="valueAngle > startAngle"
        :d="arcPath(startAngle, valueAngle)"
        fill="none"
        :stroke="currentColor"
        :stroke-width="strokeWidth + 2"
        stroke-linecap="round"
      />
    </svg>
    <div class="gauge-value" :style="{ color: currentColor }">
      {{ formattedValue }}
    </div>
  </div>
</template>

<script setup>
import { computed } from 'vue'
import { formatGrafanaUnit } from '@/utils/widgetTransforms'

const props = defineProps({
  value: { type: Number, default: 0 },
  min: { type: Number, default: 0 },
  max: { type: Number, default: 100 },
  unit: { type: String, default: null },
  thresholds: { type: Array, default: () => [] },
})

const size = 200
const strokeWidth = 16
const radius = (size - strokeWidth) / 2
const cx = size / 2
const cy = size * 0.55

// Arc from 210° to 330° (a 240° sweep across the bottom)
const startAngle = -210
const endAngle = -330 + 360 // = 30

const toRad = (deg) => (deg * Math.PI) / 180

const polarToCartesian = (angle) => {
  const r = toRad(angle)
  return {
    x: cx + radius * Math.cos(r),
    y: cy - radius * Math.sin(r),
  }
}

const arcPath = (start, end) => {
  const s = polarToCartesian(start)
  const e = polarToCartesian(end)
  const sweep = start > end ? 1 : 0
  const largeArc = Math.abs(start - end) > 180 ? 1 : 0
  return `M ${s.x} ${s.y} A ${radius} ${radius} 0 ${largeArc} ${sweep} ${e.x} ${e.y}`
}

const fraction = computed(() => {
  const range = props.max - props.min
  if (range <= 0) return 0
  return Math.max(0, Math.min(1, (props.value - props.min) / range))
})

const computeAngle = (frac) => startAngle - frac * 240

const valueAngle = computed(() => computeAngle(fraction.value))

const trackColor = '#e5e7eb'

const defaultThresholds = [
  { value: 0, color: '#22c55e' },
  { value: 0.7, color: '#eab308' },
  { value: 0.9, color: '#ef4444' },
]

const resolvedThresholds = computed(() => {
  if (props.thresholds && props.thresholds.length > 0) {
    return props.thresholds.map(t => ({
      value: t.value,
      color: resolveGrafanaColor(t.color),
    }))
  }
  // Default thresholds based on fraction of max
  return defaultThresholds.map(t => ({
    value: props.min + t.value * (props.max - props.min),
    color: t.color,
  }))
})

const thresholdSegments = computed(() => {
  const segs = []
  const sorted = [...resolvedThresholds.value].sort((a, b) => a.value - b.value)
  for (let i = 0; i < sorted.length; i++) {
    const fracStart = (sorted[i].value - props.min) / (props.max - props.min || 1)
    const fracEnd = i < sorted.length - 1
      ? (sorted[i + 1].value - props.min) / (props.max - props.min || 1)
      : 1
    segs.push({
      startAngle: computeAngle(Math.max(0, Math.min(1, fracStart))),
      endAngle: computeAngle(Math.max(0, Math.min(1, fracEnd))),
      color: sorted[i].color,
    })
  }
  return segs
})

const currentColor = computed(() => {
  const sorted = [...resolvedThresholds.value].sort((a, b) => a.value - b.value)
  let color = sorted[0]?.color || '#22c55e'
  for (const t of sorted) {
    if (props.value >= t.value) color = t.color
  }
  return color
})

const formattedValue = computed(() => {
  if (props.unit) {
    return formatGrafanaUnit(props.value, props.unit)
  }
  if (props.max <= 1 && props.min >= 0) {
    return `${(props.value * 100).toFixed(1)}%`
  }
  return formatGrafanaUnit(props.value, 'short')
})

function resolveGrafanaColor(color) {
  const grafanaColors = {
    'green': '#22c55e',
    'red': '#ef4444',
    'orange': '#f97316',
    'yellow': '#eab308',
    'blue': '#3b82f6',
    'purple': '#a855f7',
    'super-light-green': '#73BF69',
    'light-green': '#56c05a',
    'semi-dark-green': '#37872D',
    'dark-green': '#1a7c11',
    'super-light-red': '#FF7383',
    'light-red': '#F2495C',
    'semi-dark-red': '#C4162A',
    'dark-red': '#AD0317',
    'super-light-orange': '#FFCB7D',
    'light-orange': '#FF9830',
    'semi-dark-orange': '#E55400',
    'dark-orange': '#C44019',
    'super-light-yellow': '#FADE2A',
    'light-yellow': '#F2CC0C',
    'semi-dark-yellow': '#CC9D00',
    'dark-yellow': '#B5A300',
    'super-light-blue': '#8AB8FF',
    'light-blue': '#5794F2',
    'semi-dark-blue': '#3274D9',
    'dark-blue': '#1F60C4',
    'super-light-purple': '#CA95E5',
    'light-purple': '#B877D9',
    'semi-dark-purple': '#8F3BB8',
    'dark-purple': '#6C2EA1',
  }
  return grafanaColors[color] || color
}
</script>

<style scoped>
.gauge-container {
  @apply flex flex-col items-center justify-center h-full relative;
}

.gauge-svg {
  @apply w-full max-w-[200px];
}

.gauge-value {
  @apply text-2xl font-bold mt-[-1rem];
}
</style>
