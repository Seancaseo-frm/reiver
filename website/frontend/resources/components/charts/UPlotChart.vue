<template>
  <div ref="container" class="uplot-chart"></div>
</template>

<script setup>
import { ref, onMounted, onUnmounted, watch, nextTick } from 'vue'
import uPlot from 'uplot'
import 'uplot/dist/uPlot.min.css'
import { formatGrafanaUnit } from '@/utils/widgetTransforms'

const props = defineProps({
  data: { type: [Array, Object], required: true },
  unit: { type: String, default: null },
  showLegend: { type: Boolean, default: true },
  yScale: { type: String, default: null },
  stacking: { type: String, default: null },
})

const container = ref(null)
let chart = null
let resizeObserver = null
let lastWidth = 0
let lastHeight = 0

const seriesColors = [
  '#6366F1', '#10B981', '#F59E0B', '#EF4444',
  '#8B5CF6', '#3B82F6', '#EC4899', '#14B8A6',
]

const buildUPlotData = () => {
  const d = props.data
  if (!d) return null

  if (d.timestamps && d.datasets) {
    return [d.timestamps, ...d.datasets.map(ds => ds.data.map(v => v ?? null))]
  }

  if (d.labels && d.datasets) {
    const timestamps = d.labels.map((label, i) => {
      if (typeof label === 'number') return label
      const t = new Date(label)
      return isNaN(t.getTime()) ? i : t.getTime() / 1000
    })
    return [timestamps, ...d.datasets.map(ds => ds.data.map(v => v ?? null))]
  }

  if (Array.isArray(d) && d.length > 0) {
    const timestamps = d.map((item, i) => {
      if (!item) return i
      if (typeof item.x === 'number') return item.x > 1e12 ? item.x / 1000 : item.x
      const t = new Date(item.x)
      return isNaN(t.getTime()) ? i : t.getTime() / 1000
    })
    const values = d.map(item => {
      if (!item) return null
      const v = item.y ?? item.value ?? null
      return v === null ? null : Number(v)
    })
    return [timestamps, values]
  }

  return null
}

const isStacked = () => props.stacking === 'normal' || props.stacking === 'percent'

const stackData = (raw) => {
  if (!isStacked() || raw.length <= 2) return raw
  const timestamps = raw[0]
  const len = timestamps.length
  const stacked = [timestamps]

  const allNullAt = new Uint8Array(len)
  for (let i = 0; i < len; i++) {
    let allNull = true
    for (let j = 1; j < raw.length; j++) {
      if (raw[j][i] != null) { allNull = false; break }
    }
    allNullAt[i] = allNull ? 1 : 0
  }

  const cumulative = new Float64Array(len)
  for (let si = 1; si < raw.length; si++) {
    const src = raw[si]
    const dest = new Array(len)
    for (let i = 0; i < len; i++) {
      if (allNullAt[i]) {
        dest[i] = null
      } else {
        const v = src[i] ?? 0
        cumulative[i] += v
        dest[i] = cumulative[i]
      }
    }
    stacked.push(dest)
  }

  if (props.stacking === 'percent') {
    const totals = stacked[stacked.length - 1]
    for (let si = 1; si < stacked.length; si++) {
      const arr = stacked[si]
      for (let i = 0; i < len; i++) {
        if (arr[i] == null) continue
        arr[i] = totals[i] ? (arr[i] / totals[i]) * 100 : 0
      }
    }
  }

  return stacked
}

const computeGapThreshold = (timestamps) => {
  if (timestamps.length < 3) return Infinity
  const steps = []
  for (let i = 1; i < timestamps.length; i++) {
    const d = timestamps[i] - timestamps[i - 1]
    if (d > 0) steps.push(d)
  }
  if (steps.length === 0) return Infinity
  steps.sort((a, b) => a - b)
  const median = steps[Math.floor(steps.length / 2)]
  return median * 3
}

const buildGapsPlugin = (timestamps) => {
  const threshold = computeGapThreshold(timestamps)
  if (threshold === Infinity) return null

  return (u, seriesIdx, idx0, idx1) => {
    const gaps = []
    const xData = u.data[0]
    const yData = u.data[seriesIdx]
    let prevNonNull = -1
    for (let i = idx0; i <= idx1; i++) {
      if (yData[i] != null) {
        if (prevNonNull >= 0 && (xData[i] - xData[prevNonNull]) > threshold) {
          gaps.push([u.valToPos(xData[prevNonNull], 'x', true), u.valToPos(xData[i], 'x', true)])
        }
        prevNonNull = i
      }
    }
    return gaps
  }
}

const buildSeries = (timestamps) => {
  const d = props.data
  const series = [{}]
  const stacked = isStacked()
  const fillAlpha = stacked ? '80' : '1A'
  const gapsFn = buildGapsPlugin(timestamps || [])

  const dsArray = d?.datasets || []
  if (dsArray.length > 0) {
    dsArray.forEach((ds, i) => {
      const s = {
        label: ds.label || `Series ${i + 1}`,
        stroke: seriesColors[i % seriesColors.length],
        width: stacked ? 1 : 1.5,
        fill: seriesColors[i % seriesColors.length] + fillAlpha,
      }
      if (gapsFn) s.gaps = gapsFn
      series.push(s)
    })
  } else if (Array.isArray(d) && d.length > 0) {
    const s = {
      label: 'Value',
      stroke: seriesColors[0],
      width: 1.5,
      fill: seriesColors[0] + fillAlpha,
    }
    if (gapsFn) s.gaps = gapsFn
    series.push(s)
  }
  return series
}

const buildBands = (seriesCount) => {
  if (!isStacked() || seriesCount <= 1) return undefined
  const bands = []
  for (let i = seriesCount; i > 1; i--) {
    bands.push({ series: [i, i - 1] })
  }
  return bands
}

const MAX_LEGEND_RATIO = 0.4

const constrainLegend = (containerH) => {
  if (!container.value || !props.showLegend) return 0
  const legend = container.value.querySelector('.u-legend')
  if (!legend) return 0

  // Temporarily unconstrain to measure natural height
  legend.style.maxHeight = 'none'
  legend.style.overflowY = ''
  const naturalH = legend.scrollHeight

  const maxH = Math.floor(containerH * MAX_LEGEND_RATIO)
  if (naturalH > maxH) {
    legend.style.maxHeight = maxH + 'px'
    legend.style.overflowY = 'auto'
    return maxH
  }

  legend.style.maxHeight = ''
  return naturalH
}

const syncSize = () => {
  if (!chart || !container.value) return
  const rect = container.value.getBoundingClientRect()
  const w = Math.max(rect.width, 200)
  const containerH = rect.height
  const legendH = constrainLegend(containerH)
  const h = Math.max(containerH - legendH, 60)

  if (w !== lastWidth || Math.abs(h - lastHeight) > 1) {
    lastWidth = w
    lastHeight = h
    chart.setSize({ width: w, height: h })
  }
}

let tooltipEl = null

const ensureTooltip = () => {
  if (tooltipEl) return tooltipEl
  tooltipEl = document.createElement('div')
  tooltipEl.className = 'uplot-tooltip'
  tooltipEl.style.cssText =
    'display:none;position:absolute;pointer-events:none;z-index:50;' +
    'background:rgba(17,24,39,0.92);color:#e5e7eb;font-size:11px;' +
    'border-radius:6px;padding:6px 10px;line-height:1.6;white-space:nowrap;' +
    'box-shadow:0 4px 12px rgba(0,0,0,0.3);'
  container.value.appendChild(tooltipEl)
  return tooltipEl
}

const tooltipPlugin = () => ({
  hooks: {
    setCursor: (u) => {
      const tip = ensureTooltip()
      const idx = u.cursor.idx
      if (idx == null) { tip.style.display = 'none'; return }

      const unit = props.unit
      const fmt = (v) => {
        if (v == null) return '-'
        if (isStacked() && props.stacking === 'percent') return v.toFixed(1) + '%'
        return unit ? formatGrafanaUnit(v, unit) : formatGrafanaUnit(v, 'short')
      }

      const ts = u.data[0][idx]
      const time = new Date(ts * 1000)
      const timeStr = time.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })

      let html = `<div style="margin-bottom:3px;font-weight:600;color:#9ca3af">${timeStr}</div>`
      for (let si = 1; si < u.series.length; si++) {
        const s = u.series[si]
        if (!s.show) continue
        const val = u.data[si][idx]
        const color = s._stroke || s.stroke
        html += `<div style="display:flex;align-items:center;gap:6px">` +
          `<span style="width:8px;height:8px;border-radius:50%;background:${color};flex-shrink:0"></span>` +
          `<span style="flex:1">${s.label}</span>` +
          `<span style="font-weight:600;margin-left:12px">${fmt(val)}</span>` +
          `</div>`
      }
      tip.innerHTML = html
      tip.style.display = 'block'

      const left = u.cursor.left
      const top = u.cursor.top
      const wrapW = container.value.getBoundingClientRect().width
      const tipW = tip.offsetWidth
      const xPos = (left + tipW + 20 > wrapW) ? left - tipW - 12 : left + 12
      tip.style.left = xPos + 'px'
      tip.style.top = Math.max(0, top - 10) + 'px'
    },
  },
})

const createChart = () => {
  if (!container.value) return
  const rawData = buildUPlotData()
  if (!rawData) return

  if (chart) { chart.destroy(); chart = null }
  if (tooltipEl) { tooltipEl.remove(); tooltipEl = null }

  const plotData = stackData(rawData)

  const rect = container.value.getBoundingClientRect()
  const width = Math.max(rect.width, 200)
  const height = Math.max(rect.height, 60)

  const unit = props.unit
  const effectiveUnit = isStacked() && props.stacking === 'percent' ? 'percent' : unit
  const yAxisValues = effectiveUnit
    ? (u, vals) => vals.map(v => formatGrafanaUnit(v, effectiveUnit))
    : undefined

  const isLog = props.yScale && props.yScale.startsWith('log')
  const logBase = isLog ? parseInt(props.yScale.replace('log', '')) || 2 : undefined

  const yScaleConfig = isLog
    ? { distr: 3, log: logBase }
    : { range: (u, min, max) => [Math.min(0, min || 0), max || 1] }

  const seriesDefs = buildSeries(plotData[0])
  const bands = buildBands(seriesDefs.length - 1)

  const opts = {
    width,
    height,
    cursor: { drag: { x: true, y: false } },
    legend: { show: props.showLegend },
    axes: [
      {
        stroke: '#9CA3AF',
        grid: { stroke: '#37415140' },
      },
      {
        stroke: '#9CA3AF',
        grid: { stroke: '#37415140' },
        values: yAxisValues,
      },
    ],
    series: seriesDefs,
    scales: {
      x: { time: true },
      y: yScaleConfig,
    },
    bands,
    plugins: [tooltipPlugin()],
  }

  lastWidth = width
  lastHeight = height
  chart = new uPlot(opts, plotData, container.value)

  nextTick(syncSize)
}

const handleResize = () => {
  syncSize()
}

onMounted(async () => {
  await nextTick()
  createChart()
  resizeObserver = new ResizeObserver(handleResize)
  if (container.value) resizeObserver.observe(container.value)
})

onUnmounted(() => {
  if (chart) { chart.destroy(); chart = null }
  if (tooltipEl) { tooltipEl.remove(); tooltipEl = null }
  if (resizeObserver) resizeObserver.disconnect()
})

watch(() => props.data, () => {
  nextTick(createChart)
}, { deep: true })
</script>

<style scoped>
.uplot-chart {
  width: 100%;
  height: 100%;
  position: relative;
  overflow: hidden;
}

.uplot-chart :deep(.u-legend) {
  display: block;
  font-size: 11px;
  line-height: 1.4;
  color: #9CA3AF;
  padding: 2px 4px;
}

.uplot-chart :deep(.u-legend .u-series) {
  display: inline-flex;
  align-items: center;
  gap: 2px;
  padding: 1px 6px;
}

.uplot-chart :deep(.u-legend .u-series th) {
  padding: 0;
}
</style>
