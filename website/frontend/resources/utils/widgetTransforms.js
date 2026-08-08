/**
 * Pure data transformation functions for dashboard widgets.
 *
 * These are extracted from the Vue components so they can be unit-tested
 * independently. Each component imports and calls the relevant function.
 */

/**
 * Format a numeric value using a Grafana unit ID.
 *
 * Covers the most common Grafana unit identifiers. Unknown units fall through
 * to SI-suffix formatting (K/M/B).
 *
 * @param {number} value - the raw numeric value
 * @param {string} unit  - Grafana unit identifier (e.g. 's', 'bytes', 'percentunit')
 * @returns {string}
 */
export function formatGrafanaUnit(value, unit) {
  if (value === null || value === undefined || isNaN(value)) return '-'

  switch (unit) {
    // Duration
    case 'ns':
      if (Math.abs(value) >= 1e9) return `${(value / 1e9).toFixed(2)}s`
      if (Math.abs(value) >= 1e6) return `${(value / 1e6).toFixed(2)}ms`
      if (Math.abs(value) >= 1e3) return `${(value / 1e3).toFixed(2)}µs`
      return `${Math.round(value)}ns`
    case 'µs':
      if (Math.abs(value) >= 1e6) return `${(value / 1e6).toFixed(2)}s`
      if (Math.abs(value) >= 1e3) return `${(value / 1e3).toFixed(2)}ms`
      return `${value.toFixed(1)}µs`
    case 'ms':
      if (Math.abs(value) >= 60000) return `${(value / 60000).toFixed(1)}m`
      if (Math.abs(value) >= 1000) return `${(value / 1000).toFixed(2)}s`
      return `${value.toFixed(1)}ms`
    case 's':
    case 'dtdurations':
      if (Math.abs(value) >= 86400) return `${(value / 86400).toFixed(1)}d`
      if (Math.abs(value) >= 3600) return `${(value / 3600).toFixed(1)}h`
      if (Math.abs(value) >= 60) return `${(value / 60).toFixed(1)}m`
      if (Math.abs(value) >= 1) return `${value.toFixed(1)}s`
      return `${(value * 1000).toFixed(1)}ms`

    // Bytes (decimal)
    case 'bytes':
    case 'decbytes':
      if (Math.abs(value) >= 1e12) return `${(value / 1e12).toFixed(2)} TB`
      if (Math.abs(value) >= 1e9) return `${(value / 1e9).toFixed(2)} GB`
      if (Math.abs(value) >= 1e6) return `${(value / 1e6).toFixed(2)} MB`
      if (Math.abs(value) >= 1e3) return `${(value / 1e3).toFixed(2)} KB`
      return `${Math.round(value)} B`

    // Bytes (binary)
    case 'binbytes':
    case 'bibytes':
      if (Math.abs(value) >= 1099511627776) return `${(value / 1099511627776).toFixed(2)} TiB`
      if (Math.abs(value) >= 1073741824) return `${(value / 1073741824).toFixed(2)} GiB`
      if (Math.abs(value) >= 1048576) return `${(value / 1048576).toFixed(2)} MiB`
      if (Math.abs(value) >= 1024) return `${(value / 1024).toFixed(2)} KiB`
      return `${Math.round(value)} B`

    // Throughput
    case 'Bps':
    case 'binBps':
      if (Math.abs(value) >= 1e9) return `${(value / 1e9).toFixed(2)} GB/s`
      if (Math.abs(value) >= 1e6) return `${(value / 1e6).toFixed(2)} MB/s`
      if (Math.abs(value) >= 1e3) return `${(value / 1e3).toFixed(2)} KB/s`
      return `${Math.round(value)} B/s`
    case 'KBs':
      return formatGrafanaUnit(value * 1000, 'Bps')
    case 'MBs':
      return formatGrafanaUnit(value * 1e6, 'Bps')
    case 'GBs':
      return formatGrafanaUnit(value * 1e9, 'Bps')
    case 'bps':
      if (Math.abs(value) >= 1e9) return `${(value / 1e9).toFixed(2)} Gbps`
      if (Math.abs(value) >= 1e6) return `${(value / 1e6).toFixed(2)} Mbps`
      if (Math.abs(value) >= 1e3) return `${(value / 1e3).toFixed(2)} Kbps`
      return `${Math.round(value)} bps`

    // Percentage
    case 'percent':
      return `${value.toFixed(1)}%`
    case 'percentunit':
      return `${(value * 100).toFixed(1)}%`

    // Ops / requests
    case 'ops':
    case 'reqps':
    case 'rps':
      if (Math.abs(value) >= 1e6) return `${(value / 1e6).toFixed(2)}M ops/s`
      if (Math.abs(value) >= 1e3) return `${(value / 1e3).toFixed(2)}K ops/s`
      return `${value.toFixed(1)} ops/s`
    case 'iops':
      return `${value.toFixed(1)} iops`
    case 'opm':
      return `${value.toFixed(1)} ops/min`

    // Short / none — SI suffixes
    case 'short':
    case 'none':
    case '':
    case undefined:
      if (Math.abs(value) >= 1e12) return `${(value / 1e12).toFixed(2)}T`
      if (Math.abs(value) >= 1e9) return `${(value / 1e9).toFixed(2)}B`
      if (Math.abs(value) >= 1e6) return `${(value / 1e6).toFixed(1)}M`
      if (Math.abs(value) >= 1e3) return `${(value / 1e3).toFixed(1)}K`
      if (Number.isInteger(value)) return value.toString()
      return value.toFixed(2)

    // Misc
    case 'watt':
      if (Math.abs(value) >= 1e6) return `${(value / 1e6).toFixed(2)} MW`
      if (Math.abs(value) >= 1e3) return `${(value / 1e3).toFixed(2)} kW`
      return `${value.toFixed(1)} W`
    case 'celsius':
      return `${value.toFixed(1)}°C`
    case 'fahrenheit':
      return `${value.toFixed(1)}°F`
    case 'pressurehpa':
      return `${value.toFixed(1)} hPa`

    default:
      // Unknown unit — use SI suffixes
      if (Math.abs(value) >= 1e9) return `${(value / 1e9).toFixed(2)}B`
      if (Math.abs(value) >= 1e6) return `${(value / 1e6).toFixed(1)}M`
      if (Math.abs(value) >= 1e3) return `${(value / 1e3).toFixed(1)}K`
      if (Number.isInteger(value)) return value.toString()
      return value.toFixed(2)
  }
}

/**
 * Transform query results into flat [{x, y}] data for a bar chart.
 *
 * When `config.labelField` and `config.valueField` are declared, uses them
 * directly (the "data contract" path). Falls back to heuristic column
 * detection for legacy configs.
 *
 * @param {Object} result         - { columns: string[], data: Object[] }
 * @param {Object} config         - widget config (may contain labelField, valueField, unit)
 * @returns {Array|null}          - [{x: string, y: number}, ...] or null
 */
export function transformBarData(result, config) {
  if (!result || !result.data || result.data.length === 0) {
    return null
  }

  const labelField = config.labelField
  const valueField = config.valueField
  const isNanos = config.unit === 'ns'

  // If config declares fields, use them directly
  if (labelField && valueField) {
    return result.data.map(row => ({
      x: String(row[labelField] || 'Unknown'),
      y: isNanos ? Math.round((parseFloat(row[valueField]) || 0) / 1e6) : (parseFloat(row[valueField]) || 0),
    }))
  }

  // Fallback for legacy configs without labelField/valueField:
  // pick the first string column as label, first numeric column as value
  const skipCols = new Set(['project_id', 'unix_milli', 'fingerprint'])
  const usableCols = result.columns.filter(c => !skipCols.has(c))
  const labelCol = usableCols.find(c =>
    typeof result.data[0][c] === 'string' && isNaN(Number(result.data[0][c]))
  )
  const valueCol = usableCols.find(c => c === 'value' && c !== labelCol)
    || usableCols.find(c =>
      typeof result.data[0][c] === 'number' && c !== labelCol
    )

  if (!valueCol) {
    return null
  }

  if (!labelCol) {
    // PromQL aggregated result with no string label column.
    // Check for lbl_* columns that could serve as labels.
    const lblCols = usableCols.filter(c => c.startsWith('lbl_'))
    if (lblCols.length > 0) {
      const lblCol = lblCols[0]
      const grouped = {}
      for (const row of result.data) {
        const label = String(row[lblCol] || 'Unknown')
        grouped[label] = isNanos
          ? Math.round((parseFloat(row[valueCol]) || 0) / 1e6)
          : (parseFloat(row[valueCol]) || 0)
      }
      return Object.entries(grouped).map(([x, y]) => ({ x, y }))
    }
    // Single scalar — take the last non-null value
    for (let i = result.data.length - 1; i >= 0; i--) {
      const v = parseFloat(result.data[i][valueCol])
      if (!isNaN(v)) {
        return [{ x: 'Value', y: isNanos ? Math.round(v / 1e6) : v }]
      }
    }
    return null
  }

  // Group by label, keep last value per group (deduplicates time series)
  const grouped = {}
  for (const row of result.data) {
    const label = String(row[labelCol] || 'Unknown')
    const val = parseFloat(row[valueCol])
    if (!isNaN(val)) {
      grouped[label] = isNanos ? Math.round(val / 1e6) : val
    }
  }
  return Object.entries(grouped).map(([x, y]) => ({ x, y }))
}

/**
 * Extract a short, human-readable column name from a PromQL expression.
 * E.g. "ClickHouseMetrics_VersionInteger" → "version_integer"
 *      "rate(http_requests_total{...}[5m])" → "http_requests_total"
 *
 * @param {string} promql - the PromQL expression
 * @returns {string}
 */
export function extractMetricDisplayName(promql) {
  if (!promql) return 'value'
  // Strip wrapping functions: rate(...), irate(...), max_over_time(...), etc.
  let inner = promql
  const funcMatch = inner.match(/^\w+\(\s*(.+)\s*\)$/)
  if (funcMatch) inner = funcMatch[1]
  // Strip nested functions too
  const funcMatch2 = inner.match(/^\w+\(\s*(.+)\s*\)$/)
  if (funcMatch2) inner = funcMatch2[1]
  // Extract metric name (word chars before any { or [)
  const metricMatch = inner.match(/^([\w:.]+)/)
  if (!metricMatch) return promql.substring(0, 30)
  let name = metricMatch[1]
  // Remove common prefixes like "ClickHouseMetrics_", "ClickHouseProfileEvents_", "ClickHouseAsyncMetrics_"
  name = name.replace(/^ClickHouse(?:Async)?(?:Metrics|ProfileEvents)_/, '')
  // Convert CamelCase to readable: "VersionInteger" → "version_integer"
  name = name.replace(/([a-z])([A-Z])/g, '$1_$2').toLowerCase()
  return name
}

/**
 * Pivot multi-series table data so each series becomes a named column.
 *
 * When a table widget has multiple sub-queries, the backend concatenates all
 * rows into a flat array with `lbl__series` distinguishing each sub-query.
 * This function pivots that flat list into one row per unique label combination,
 * with each series value as a separate column — matching Grafana's table
 * behaviour.
 *
 * @param {Object}   result  - { columns: string[], data: Object[] }
 * @param {Object}   config  - widget config (must have queries array)
 * @returns {{ columns: string[], data: Object[] }} pivoted result, or the
 *          original result unchanged if pivoting doesn't apply.
 */
export function pivotMultiSeries(result, config) {
  if (!result || !result.data || result.data.length === 0) return result
  if (!result.columns.includes('lbl__series')) return result

  const queries = config.query?.queries || config.queries
  if (!queries || queries.length <= 1) return result

  // Identify label dimension columns (lbl_* except lbl__series).
  const dimensionCols = result.columns.filter(
    c => c.startsWith('lbl_') && c !== 'lbl__series'
  )

  // Collect all unique series names (from lbl__series).
  const seriesNames = [...new Set(result.data.map(r => r.lbl__series || ''))]
  if (seriesNames.length <= 1 && !seriesNames[0]) return result

  // Build short display names for each series column.
  const seriesDisplayNames = seriesNames.map(s => extractMetricDisplayName(s))

  // Deduplicate display names by appending index if needed.
  const nameCount = {}
  for (const n of seriesDisplayNames) nameCount[n] = (nameCount[n] || 0) + 1
  const nameSeen = {}
  const finalNames = seriesDisplayNames.map(n => {
    if (nameCount[n] > 1) {
      nameSeen[n] = (nameSeen[n] || 0) + 1
      return `${n}_${nameSeen[n]}`
    }
    return n
  })

  // Map from series value → display column name.
  const seriesToCol = new Map()
  seriesNames.forEach((s, i) => seriesToCol.set(s, finalNames[i]))

  // Group rows by dimension key.
  const groups = new Map()
  for (const row of result.data) {
    const key = dimensionCols.map(c => row[c] ?? '').join('\0')
    if (!groups.has(key)) {
      const base = {}
      for (const c of dimensionCols) base[c] = row[c]
      groups.set(key, base)
    }
    const colName = seriesToCol.get(row.lbl__series || '') || 'value'
    const group = groups.get(key)
    // Keep the latest value per series (highest unix_milli).
    if (group[colName] === undefined || (row.unix_milli || 0) > (group[`__ts_${colName}`] || 0)) {
      group[colName] = row.value
      group[`__ts_${colName}`] = row.unix_milli
    }
  }

  // Build output.
  const valueCols = [...seriesToCol.values()]
  const pivotedData = [...groups.values()].map(g => {
    const row = {}
    for (const c of dimensionCols) row[c] = g[c]
    for (const vc of valueCols) row[vc] = g[vc] ?? null
    return row
  })

  // Rename dimension columns for display (strip lbl_ prefix).
  const renamedDimCols = dimensionCols.map(c => c.startsWith('lbl_') ? c.slice(4) : c)
  const colMapping = new Map()
  dimensionCols.forEach((orig, i) => colMapping.set(orig, renamedDimCols[i]))

  const finalData = pivotedData.map(row => {
    const out = {}
    for (const c of dimensionCols) out[colMapping.get(c)] = row[c]
    for (const vc of valueCols) out[vc] = row[vc]
    return out
  })

  return {
    columns: [...renamedDimCols, ...valueCols],
    data: finalData,
  }
}

/**
 * Format a table cell value for display.
 *
 * Handles timestamps, nanosecond durations, and long strings.
 * Numbers are displayed with locale formatting; unit-aware formatting
 * is handled separately via formatGrafanaUnit().
 *
 * @param {*}      value  - the raw cell value
 * @param {string} col    - the column name (used for format detection)
 * @returns {string}
 */
export function formatCellValue(value, col) {
  if (value === null || value === undefined) return '-'

  // Format timestamps (including PromQL's unix_milli column)
  if (col === 'unix_milli' || col.toLowerCase().includes('time') || col.toLowerCase().includes('timestamp')) {
    try {
      const date = new Date(value)
      if (!isNaN(date)) {
        return date.toLocaleString()
      }
    } catch { /* ignore */ }
  }

  // Format durations (nanoseconds) — detect by _ns suffix, duration, or latency keyword
  const lc = col.toLowerCase()
  if (lc.includes('_ns') || lc.includes('duration') || lc.includes('latency')) {
    const ns = parseFloat(value)
    if (!isNaN(ns)) {
      if (ns >= 1e9) return `${(ns / 1e9).toFixed(2)}s`
      if (ns >= 1e6) return `${(ns / 1e6).toFixed(2)}ms`
      if (ns >= 1e3) return `${(ns / 1e3).toFixed(2)}µs`
      return `${ns.toFixed(0)}ns`
    }
  }

  if (typeof value === 'number') {
    if (Number.isInteger(value)) return value.toLocaleString()
    return value.toFixed(2)
  }

  // Truncate long strings
  if (typeof value === 'string' && value.length > 100) {
    return value.substring(0, 100) + '...'
  }

  return String(value)
}
