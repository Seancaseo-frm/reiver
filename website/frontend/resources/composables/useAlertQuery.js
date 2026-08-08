import { ref, computed } from 'vue';
import { alertService } from '../services/alertService';

/**
 * Composable for managing alert queries and alert instances
 * Provides reactive state and methods for querying alerts (fired/resolved alerts)
 */
export function useAlertQuery(projectId) {
  const alerts = ref([]);
  const loading = ref(false);
  const error = ref(null);

  /**
   * Computed properties
   */
  const firingAlerts = computed(() => alerts.value.filter((a) => a.state === 'firing'));
  const resolvedAlerts = computed(() => alerts.value.filter((a) => a.state === 'resolved'));
  const pendingAlerts = computed(() => alerts.value.filter((a) => a.state === 'pending'));
  const missingAlerts = computed(() => alerts.value.filter((a) => a.is_missing));

  const alertsByState = computed(() => {
    return {
      firing: firingAlerts.value,
      resolved: resolvedAlerts.value,
      pending: pendingAlerts.value,
    };
  });

  /**
   * Load alerts for the project
   * @param {Object} params - Query parameters (rule_id, state, limit)
   */
  const loadAlerts = async (params = {}) => {
    loading.value = true;
    error.value = null;
    try {
      alerts.value = await alertService.listAlerts(projectId, params);
    } catch (err) {
      console.error('Failed to load alerts:', err);
      error.value = err.response?.data?.error || err.message || 'Failed to load alerts';
      throw err;
    } finally {
      loading.value = false;
    }
  };

  /**
   * Refresh alerts (reload from server)
   * @param {Object} params - Query parameters (rule_id, state, limit)
   */
  const refreshAlerts = async (params = {}) => {
    return loadAlerts(params);
  };

  /**
   * Load alerts for a specific rule
   * @param {string} ruleId - Rule ID
   */
  const loadRuleAlerts = async (ruleId) => {
    loading.value = true;
    error.value = null;
    try {
      alerts.value = await alertService.getRuleAlerts(projectId, ruleId);
    } catch (err) {
      console.error('Failed to load rule alerts:', err);
      error.value = err.response?.data?.error || err.message || 'Failed to load rule alerts';
      throw err;
    } finally {
      loading.value = false;
    }
  };

  /**
   * Filter alerts by state
   * @param {string} state - Alert state ('firing', 'resolved', 'pending')
   * @returns {Array} Filtered alerts
   */
  const filterByState = (state) => {
    if (!state) return alerts.value;
    return alerts.value.filter((a) => a.state === state);
  };

  /**
   * Filter alerts by rule ID
   * @param {string} ruleId - Rule ID
   * @returns {Array} Filtered alerts
   */
  const filterByRule = (ruleId) => {
    if (!ruleId) return alerts.value;
    return alerts.value.filter((a) => a.rule_id === ruleId);
  };

  /**
   * Get alert statistics
   * @returns {Object} Alert statistics
   */
  const getStats = () => {
    return {
      total: alerts.value.length,
      firing: firingAlerts.value.length,
      resolved: resolvedAlerts.value.length,
      pending: pendingAlerts.value.length,
      missing: missingAlerts.value.length,
    };
  };

  /**
   * Clear error state
   */
  const clearError = () => {
    error.value = null;
  };

  return {
    // State
    alerts,
    loading,
    error,
    // Computed
    firingAlerts,
    resolvedAlerts,
    pendingAlerts,
    missingAlerts,
    alertsByState,
    // Methods
    loadAlerts,
    refreshAlerts,
    loadRuleAlerts,
    filterByState,
    filterByRule,
    getStats,
    clearError,
  };
}
