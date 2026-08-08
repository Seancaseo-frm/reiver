import { ref } from 'vue'
import axios from 'axios'

/**
 * Composable for executing widget queries against the backend
 */
export function useWidgetQuery() {
  const loading = ref(false)
  const error = ref(null)
  const data = ref(null)

  /**
   * Execute a widget query
   * @param {string} projectId - Project UUID
   * @param {Object} queryConfig - Widget query configuration
   * @param {Object} timeRange - Time range { from, to }
   * @param {Object} variables - Template variables (e.g., { service: 'api' })
   */
  async function executeQuery(projectId, queryConfig, timeRange, variables = {}) {
    loading.value = true
    error.value = null

    try {
      const response = await axios.post(`/api/${projectId}/widget-query`, {
        query: queryConfig,
        time_range: timeRange,
        variables,
      })

      data.value = response.data
      return response.data
    } catch (err) {
      const message = err.response?.data?.error || err.message || 'Query failed'
      error.value = message
      throw new Error(message)
    } finally {
      loading.value = false
    }
  }

  /**
   * Fetch discovered services for a project
   * @param {string} projectId - Project UUID
   */
  async function fetchServices(projectId) {
    try {
      const response = await axios.get(`/api/${projectId}/discovered-services`)
      return response.data
    } catch (err) {
      console.error('Failed to fetch services:', err)
      return []
    }
  }

  return {
    loading,
    error,
    data,
    executeQuery,
    fetchServices,
  }
}

/**
 * Parse relative time strings like 'now-1h' to ISO timestamps
 */
export function parseTimeRange(timeRange) {
  const now = new Date()
  
  // Common presets
  const presets = {
    '15m': 15 * 60 * 1000,
    '30m': 30 * 60 * 1000,
    '1h': 60 * 60 * 1000,
    '3h': 3 * 60 * 60 * 1000,
    '6h': 6 * 60 * 60 * 1000,
    '12h': 12 * 60 * 60 * 1000,
    '24h': 24 * 60 * 60 * 1000,
    '2d': 2 * 24 * 60 * 60 * 1000,
    '7d': 7 * 24 * 60 * 60 * 1000,
    '14d': 14 * 24 * 60 * 60 * 1000,
    '30d': 30 * 24 * 60 * 60 * 1000,
  }
  
  if (presets[timeRange]) {
    return {
      from: `now-${timeRange}`,
      to: 'now',
    }
  }
  
  // Default to 1h
  return {
    from: 'now-1h',
    to: 'now',
  }
}
