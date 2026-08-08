import { ref, computed } from 'vue';
import { alertService } from '../services/alertService';

/**
 * Composable for managing alert rules
 * Provides reactive state and methods for CRUD operations on alert rules
 */
export function useAlertRules(projectId) {
  const rules = ref([]);
  const loading = ref(false);
  const error = ref(null);

  /**
   * Computed properties
   */
  const enabledRules = computed(() => rules.value.filter((r) => r.enabled));
  const disabledRules = computed(() => rules.value.filter((r) => !r.enabled));
  const rulesByHealth = computed(() => {
    const grouped = {
      ok: [],
      err: [],
      no_data: [],
      unknown: [],
    };
    rules.value.forEach((rule) => {
      const health = rule.health || 'unknown';
      if (grouped[health]) {
        grouped[health].push(rule);
      } else {
        grouped.unknown.push(rule);
      }
    });
    return grouped;
  });

  /**
   * Load all alert rules for the project
   * @param {Object} params - Query parameters (enabled, limit)
   */
  const loadRules = async (params = {}) => {
    loading.value = true;
    error.value = null;
    try {
      rules.value = await alertService.listRules(projectId, params);
    } catch (err) {
      console.error('Failed to load alert rules:', err);
      error.value = err.response?.data?.error || err.message || 'Failed to load alert rules';
      throw err;
    } finally {
      loading.value = false;
    }
  };

  /**
   * Refresh alert rules (reload from server)
   * @param {Object} params - Query parameters (enabled, limit)
   */
  const refreshRules = async (params = {}) => {
    return loadRules(params);
  };

  /**
   * Get a single alert rule by ID
   * @param {string} ruleId - Rule ID
   * @returns {Promise<Object>} Alert rule
   */
  const getRule = async (ruleId) => {
    loading.value = true;
    error.value = null;
    try {
      const rule = await alertService.getRule(projectId, ruleId);
      // Update local rule if it exists in the list
      const index = rules.value.findIndex((r) => r.id === ruleId);
      if (index !== -1) {
        rules.value[index] = rule;
      }
      return rule;
    } catch (err) {
      console.error('Failed to get alert rule:', err);
      error.value = err.response?.data?.error || err.message || 'Failed to get alert rule';
      throw err;
    } finally {
      loading.value = false;
    }
  };

  /**
   * Create a new alert rule
   * @param {Object} ruleData - Alert rule data
   * @returns {Promise<Object>} Created alert rule
   */
  const createRule = async (ruleData) => {
    loading.value = true;
    error.value = null;
    try {
      const rule = await alertService.createRule(projectId, ruleData);
      // Add to local list
      rules.value.unshift(rule);
      return rule;
    } catch (err) {
      console.error('Failed to create alert rule:', err);
      error.value = err.response?.data?.error || err.message || 'Failed to create alert rule';
      throw err;
    } finally {
      loading.value = false;
    }
  };

  /**
   * Update an existing alert rule
   * @param {string} ruleId - Rule ID
   * @param {Object} ruleData - Updated alert rule data
   * @returns {Promise<Object>} Updated alert rule
   */
  const updateRule = async (ruleId, ruleData) => {
    loading.value = true;
    error.value = null;
    try {
      const rule = await alertService.updateRule(projectId, ruleId, ruleData);
      // Update local rule
      const index = rules.value.findIndex((r) => r.id === ruleId);
      if (index !== -1) {
        rules.value[index] = rule;
      }
      return rule;
    } catch (err) {
      console.error('Failed to update alert rule:', err);
      error.value = err.response?.data?.error || err.message || 'Failed to update alert rule';
      throw err;
    } finally {
      loading.value = false;
    }
  };

  /**
   * Delete an alert rule
   * @param {string} ruleId - Rule ID
   */
  const deleteRule = async (ruleId) => {
    loading.value = true;
    error.value = null;
    try {
      await alertService.deleteRule(projectId, ruleId);
      // Remove from local list
      rules.value = rules.value.filter((r) => r.id !== ruleId);
    } catch (err) {
      console.error('Failed to delete alert rule:', err);
      error.value = err.response?.data?.error || err.message || 'Failed to delete alert rule';
      throw err;
    } finally {
      loading.value = false;
    }
  };

  /**
   * Toggle rule enabled/disabled status
   * @param {string} ruleId - Rule ID
   * @param {boolean} enabled - New enabled status (optional, will toggle if not provided)
   * @returns {Promise<Object>} Updated alert rule
   */
  const toggleRule = async (ruleId, enabled = null) => {
    const rule = rules.value.find((r) => r.id === ruleId);
    if (!rule) {
      throw new Error('Rule not found');
    }
    const newEnabled = enabled !== null ? enabled : !rule.enabled;
    return updateRule(ruleId, { enabled: newEnabled });
  };

  /**
   * Clear error state
   */
  const clearError = () => {
    error.value = null;
  };

  return {
    // State
    rules,
    loading,
    error,
    // Computed
    enabledRules,
    disabledRules,
    rulesByHealth,
    // Methods
    loadRules,
    refreshRules,
    getRule,
    createRule,
    updateRule,
    deleteRule,
    toggleRule,
    clearError,
  };
}
