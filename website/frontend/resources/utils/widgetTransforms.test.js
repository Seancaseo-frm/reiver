import { describe, it, expect } from 'vitest'
import { transformBarData, formatCellValue } from './widgetTransforms'

// ── transformBarData ──────────────────────────────────────────────────

describe('transformBarData', () => {
  it('returns null for empty result', () => {
    expect(transformBarData(null, {})).toBeNull()
    expect(transformBarData({ data: [] }, {})).toBeNull()
    expect(transformBarData({ data: null }, {})).toBeNull()
  })

  it('uses labelField and valueField from config', () => {
    const result = {
      columns: ['endpoint', 'total_time', 'requests'],
      data: [
        { endpoint: '/api/users', total_time: '500000000', requests: 10 },
        { endpoint: '/api/orders', total_time: '300000000', requests: 5 },
      ],
    }
    const config = { labelField: 'endpoint', valueField: 'total_time' }
    const data = transformBarData(result, config)

    expect(data).toHaveLength(2)
    expect(data[0]).toEqual({ x: '/api/users', y: 500000000 })
    expect(data[1]).toEqual({ x: '/api/orders', y: 300000000 })
  })

  it('converts nanoseconds to milliseconds when unit=ns', () => {
    const result = {
      columns: ['endpoint', 'total_time'],
      data: [
        { endpoint: '/api/users', total_time: '1500000000' },  // 1.5 billion ns = 1500 ms
        { endpoint: '/api/orders', total_time: '250000000' },  // 250 million ns = 250 ms
      ],
    }
    const config = { labelField: 'endpoint', valueField: 'total_time', unit: 'ns' }
    const data = transformBarData(result, config)

    expect(data).toHaveLength(2)
    expect(data[0]).toEqual({ x: '/api/users', y: 1500 })
    expect(data[1]).toEqual({ x: '/api/orders', y: 250 })
  })

  it('does not convert values when unit is not ns', () => {
    const result = {
      columns: ['endpoint', 'count'],
      data: [
        { endpoint: '/api/users', count: '42' },
      ],
    }
    const config = { labelField: 'endpoint', valueField: 'count', unit: 'count' }
    const data = transformBarData(result, config)

    expect(data[0]).toEqual({ x: '/api/users', y: 42 })
  })

  it('uses "Unknown" for missing label values', () => {
    const result = {
      columns: ['endpoint', 'total_time'],
      data: [
        { endpoint: null, total_time: '100' },
        { total_time: '200' },
      ],
    }
    const config = { labelField: 'endpoint', valueField: 'total_time' }
    const data = transformBarData(result, config)

    expect(data[0].x).toBe('Unknown')
    expect(data[1].x).toBe('Unknown')
  })

  it('falls back to heuristic when labelField/valueField not in config', () => {
    const result = {
      columns: ['name', 'count'],
      data: [
        { name: 'service-a', count: 10 },
        { name: 'service-b', count: 20 },
      ],
    }
    const config = {}  // No labelField/valueField
    const data = transformBarData(result, config)

    expect(data).toHaveLength(2)
    expect(data[0]).toEqual({ x: 'service-a', y: 10 })
    expect(data[1]).toEqual({ x: 'service-b', y: 20 })
  })

  it('fallback skips numeric strings as label columns', () => {
    // This was the original bug: numeric strings like "1917911000" were
    // incorrectly picked as labels.
    const result = {
      columns: ['total_time', 'requests', 'endpoint'],
      data: [
        { total_time: '1917911000', requests: 5, endpoint: '/api/query' },
      ],
    }
    const config = {}  // Legacy config without declared fields
    const data = transformBarData(result, config)

    // endpoint (a non-numeric string) should be picked as label, not total_time
    expect(data[0].x).toBe('/api/query')
  })

  it('handles all-numeric columns by using last value', () => {
    const result = {
      columns: ['a', 'b'],
      data: [
        { a: 123, b: 456 },
      ],
    }
    const config = {}
    // All columns are numbers — use last row value with generic label
    const data = transformBarData(result, config)
    expect(data).toHaveLength(1)
    expect(data[0]).toEqual({ x: 'Value', y: 123 })
  })

  it('uses lbl_ column as label when present', () => {
    const result = {
      columns: ['unix_milli', 'value', 'fingerprint', 'lbl_node'],
      data: [
        { unix_milli: 1000, value: 12, fingerprint: 0, lbl_node: 'node-1' },
        { unix_milli: 2000, value: 8, fingerprint: 0, lbl_node: 'node-2' },
      ],
    }
    const config = {}
    const data = transformBarData(result, config)
    expect(data).toHaveLength(2)
    expect(data[0]).toEqual({ x: 'node-1', y: 12 })
    expect(data[1]).toEqual({ x: 'node-2', y: 8 })
  })

  it('handles PromQL scalar result (no label column)', () => {
    const result = {
      columns: ['unix_milli', 'value', 'fingerprint'],
      data: [
        { unix_milli: 1000, value: 12, fingerprint: 0 },
        { unix_milli: 2000, value: 12, fingerprint: 0 },
      ],
    }
    const config = {}
    const data = transformBarData(result, config)
    expect(data).toHaveLength(1)
    expect(data[0]).toEqual({ x: 'Value', y: 12 })
  })
})

// ── formatCellValue ───────────────────────────────────────────────────

describe('formatCellValue', () => {
  it('returns dash for null or undefined', () => {
    expect(formatCellValue(null, 'col')).toBe('-')
    expect(formatCellValue(undefined, 'col')).toBe('-')
  })

  it('formats nanoseconds to seconds', () => {
    expect(formatCellValue(2500000000, 'p95_ns')).toBe('2.50s')
  })

  it('formats nanoseconds to milliseconds', () => {
    expect(formatCellValue(15000000, 'median_ns')).toBe('15.00ms')
  })

  it('formats nanoseconds to microseconds', () => {
    expect(formatCellValue(5000, 'total_ns')).toBe('5.00µs')
  })

  it('formats small nanosecond values', () => {
    expect(formatCellValue(42, 'duration')).toBe('42ns')
  })

  it('formats string nanosecond values', () => {
    // ClickHouse may return numbers as strings
    expect(formatCellValue('1500000000', 'p95_ns')).toBe('1.50s')
  })

  it('does not apply ns formatting to non-duration columns', () => {
    const result = formatCellValue(5000, 'requests')
    // 5000 is >= 1e3, so it gets the K formatter
    expect(result).toBe('5.00K')
  })

  it('formats large numbers with K/M/B suffixes', () => {
    expect(formatCellValue(1500, 'count')).toBe('1.50K')
    expect(formatCellValue(2500000, 'total')).toBe('2.50M')
    expect(formatCellValue(3500000000, 'big')).toBe('3.50B')
  })

  it('formats small numbers with locale', () => {
    expect(formatCellValue(42, 'count')).toBe('42')
  })

  it('truncates long strings', () => {
    const longStr = 'x'.repeat(150)
    const result = formatCellValue(longStr, 'message')
    expect(result.length).toBe(103) // 100 + '...'
    expect(result.endsWith('...')).toBe(true)
  })

  it('passes through short strings', () => {
    expect(formatCellValue('hello', 'name')).toBe('hello')
  })

  it('detects duration columns by _ns suffix', () => {
    expect(formatCellValue(5000000, 'custom_ns')).toBe('5.00ms')
  })

  it('detects duration columns by duration keyword', () => {
    expect(formatCellValue(5000000, 'request_duration')).toBe('5.00ms')
  })

  it('detects duration columns by latency keyword', () => {
    expect(formatCellValue(273600000, 'avg_latency')).toBe('273.60ms')
    expect(formatCellValue(1500000000, 'p95_latency')).toBe('1.50s')
  })

  it('does not apply duration formatting for columns without _ns, duration, or latency', () => {
    // Column 'errors' should not get ns formatting
    expect(formatCellValue(5, 'errors')).toBe('5')
  })
})
