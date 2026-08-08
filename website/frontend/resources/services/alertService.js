import axios from 'axios';

/**
 * Threshold unit types for unit conversion
 * @typedef {'none' | 'nanoseconds' | 'microseconds' | 'milliseconds' | 'seconds' | 'minutes' | 'hours' | 'days' | 'bytes' | 'kilobytes' | 'megabytes' | 'gigabytes' | 'terabytes' | 'per_second' | 'per_minute' | 'per_hour' | 'percent' | 'decimal_percent'} ThresholdUnit
 */

/**
 * Threshold severity levels
 * @typedef {'critical' | 'warning' | 'info' | string} ThresholdSeverity
 */

/**
 * Alert threshold for metric-based alerts
 * @typedef {Object} AlertThreshold
 * @property {string} id - Unique identifier (UUID)
 * @property {ThresholdSeverity} name - Severity name (critical, warning, info, or custom)
 * @property {number} target_value - The threshold value
 * @property {number|null} recovery_target - Optional recovery threshold (prevents flapping)
 * @property {ThresholdUnit} target_unit - Unit for the threshold value
 * @property {string} match_type - How to evaluate (at_least_once, all_the_time, on_average, in_total, last)
 * @property {string} compare_op - Comparison operator (above, below, equal, etc.)
 * @property {string[]} channels - Notification channel IDs for this severity
 */

/**
 * Anomaly threshold for anomaly detection alerts
 * @typedef {Object} AnomalyThreshold
 * @property {string} id - Unique identifier (UUID)
 * @property {ThresholdSeverity} name - Severity name (critical, warning, info, or custom)
 * @property {number} spike_multiplier - Fire when value >= baseline * multiplier
 * @property {number} min_baseline - Minimum baseline to avoid noise
 * @property {string[]} channels - Notification channel IDs for this severity
 */

/**
 * Alert rule request/response structure
 * @typedef {Object} AlertRule
 * @property {string} id - Rule ID
 * @property {string} project_id - Project ID
 * @property {string} name - Rule name
 * @property {string|null} description - Optional description
 * @property {string} rule_type - 'threshold' or 'anomaly'
 * @property {Object} query_config - Query configuration
 * @property {string} compare_op - Comparison operator (legacy, still used)
 * @property {number} threshold_value - Single threshold value (legacy, still used)
 * @property {string} match_type - Match type
 * @property {number} eval_window_seconds - Evaluation window in seconds
 * @property {number} eval_interval_seconds - Evaluation interval in seconds
 * @property {number} pending_period_seconds - Pending period before firing
 * @property {number} eval_delay_seconds - Delay for late-arriving data
 * @property {boolean} alert_on_absent - Alert on absent data
 * @property {number} absent_for_seconds - Duration of absence to trigger
 * @property {string[]} notification_channels - Global notification channels (legacy)
 * @property {AlertThreshold[]} thresholds - Multi-severity thresholds
 * @property {AnomalyThreshold[]} anomaly_thresholds - Multi-severity anomaly thresholds
 * @property {number} schema_version - Schema version (1 for multi-threshold)
 * @property {boolean} enabled - Whether rule is enabled
 * @property {Object} labels - Custom labels
 * @property {Object} annotations - Custom annotations
 */

/**
 * Service for alert-related API calls
 */
export const alertService = {
  /**
   * List alert rules
   * @param {string} projectId - Project ID
   * @param {Object} params - Query parameters (enabled, limit)
   * @returns {Promise<AlertRule[]>} Array of alert rules
   */
  async listRules(projectId, params = {}) {
    const response = await axios.get('/api/alerting/rules', {
      params: {
        project_id: projectId,
        ...params,
      },
      headers: {
        'x-project-id': projectId,
      },
    });
    return response.data || [];
  },

  /**
   * Get a single alert rule by ID
   * @param {string} projectId - Project ID
   * @param {string} ruleId - Rule ID
   * @returns {Promise<AlertRule>} Alert rule object
   */
  async getRule(projectId, ruleId) {
    const response = await axios.get(`/api/alerting/rules/${ruleId}`, {
      headers: {
        'x-project-id': projectId,
      },
    });
    return response.data;
  },

  /**
   * Create a new alert rule
   * @param {string} projectId - Project ID
   * @param {Object} ruleData - Alert rule data
   * @returns {Promise<AlertRule>} Created alert rule
   */
  async createRule(projectId, ruleData) {
    const payload = this.buildRulePayload(projectId, ruleData);
    const response = await axios.post('/api/alerting/rules', payload, {
      headers: {
        'x-project-id': projectId,
      },
    });
    return response.data;
  },

  /**
   * Update an existing alert rule
   * @param {string} projectId - Project ID
   * @param {string} ruleId - Rule ID
   * @param {Object} ruleData - Updated alert rule data
   * @returns {Promise<AlertRule>} Updated alert rule
   */
  async updateRule(projectId, ruleId, ruleData) {
    const payload = this.buildRulePayload(projectId, ruleData, false);
    const response = await axios.put(`/api/alerting/rules/${ruleId}`, payload, {
      headers: {
        'x-project-id': projectId,
      },
    });
    return response.data;
  },

  /**
   * Delete an alert rule
   * @param {string} projectId - Project ID
   * @param {string} ruleId - Rule ID
   * @returns {Promise<void>}
   */
  async deleteRule(projectId, ruleId) {
    await axios.delete(`/api/alerting/rules/${ruleId}`, {
      headers: {
        'x-project-id': projectId,
      },
    });
  },

  /**
   * Toggle rule enabled/disabled status
   * @param {string} projectId - Project ID
   * @param {string} ruleId - Rule ID
   * @param {boolean} enabled - New enabled status
   * @returns {Promise<AlertRule>} Updated alert rule
   */
  async toggleRule(projectId, ruleId, enabled) {
    return this.updateRule(projectId, ruleId, { enabled });
  },

  /**
   * List alerts (fired/resolved alerts)
   * @param {string} projectId - Project ID
   * @param {Object} params - Query parameters (rule_id, state, limit)
   * @returns {Promise<Array>} Array of alerts
   */
  async listAlerts(projectId, params = {}) {
    const response = await axios.get('/api/alerting/alerts', {
      params: {
        project_id: projectId,
        ...params,
      },
      headers: {
        'x-project-id': projectId,
      },
    });
    return response.data || [];
  },

  /**
   * Get alerts for a specific rule
   * @param {string} projectId - Project ID
   * @param {string} ruleId - Rule ID
   * @returns {Promise<Array>} Array of alerts for the rule
   */
  async getRuleAlerts(projectId, ruleId) {
    const response = await axios.get(`/api/alerting/rules/${ruleId}/alerts`, {
      headers: {
        'x-project-id': projectId,
      },
    });
    return response.data || [];
  },

  /**
   * Send test notification to selected channels
   * @param {string} projectId - Project ID
   * @param {Array<string>} channelIds - Array of channel IDs
   * @returns {Promise<Object>} Test notification result
   */
  async testNotification(projectId, channelIds) {
    const response = await axios.post(
      '/api/alerting/test-notification',
      { channel_ids: channelIds },
      {
        headers: {
          'x-project-id': projectId,
        },
      }
    );
    return response.data;
  },

  /**
   * Build rule payload for create/update requests
   * @param {string} projectId - Project ID
   * @param {Object} ruleData - Rule data from form
   * @param {boolean} includeProjectId - Whether to include project_id (for create)
   * @returns {Object} API payload
   */
  buildRulePayload(projectId, ruleData, includeProjectId = true) {
    const isAnomaly = ruleData.rule_type === 'anomaly' || ruleData.alertType === 'anomaly';

    // Build thresholds array
    const thresholds = (ruleData.thresholds || []).map((t) => ({
      name: t.name,
      target_value: t.target_value,
      recovery_target: t.recovery_target,
      target_unit: t.target_unit || 'none',
      match_type: ruleData.match_type || t.match_type || 'at_least_once',
      compare_op: ruleData.compare_op || t.compare_op || 'above',
      channels: t.channels || [],
    }));

    // Build anomaly thresholds array
    const anomalyThresholds = (ruleData.anomaly_thresholds || []).map((t) => ({
      name: t.name,
      spike_multiplier: t.spike_multiplier,
      min_baseline: t.min_baseline,
      channels: t.channels || [],
    }));

    const payload = {
      name: ruleData.name,
      description: ruleData.description || null,
      rule_type: isAnomaly ? 'anomaly' : 'threshold',
      query_config: ruleData.query_config,
      thresholds: isAnomaly ? [] : thresholds,
      anomaly_thresholds: isAnomaly ? anomalyThresholds : [],
      eval_window_seconds: ruleData.eval_window_seconds || 300,
      eval_interval_seconds: ruleData.eval_interval_seconds || 60,
      pending_period_seconds: ruleData.pending_period_seconds || 0,
      eval_delay_seconds: ruleData.eval_delay_seconds || 0,
      alert_on_absent: ruleData.alert_on_absent || false,
      absent_for_seconds: ruleData.absent_for_seconds || 300,
      labels: ruleData.labels || {},
      annotations: ruleData.annotations || {},
      enabled: ruleData.enabled !== false,
    };

    if (includeProjectId) {
      payload.project_id = projectId;
    }

    return payload;
  },

  /**
   * Create a default threshold object
   * @param {ThresholdSeverity} severity - Severity level
   * @param {number} value - Initial threshold value
   * @returns {AlertThreshold} Default threshold object
   */
  createDefaultThreshold(severity = 'critical', value = 0) {
    return {
      id: crypto.randomUUID(),
      name: severity,
      target_value: value,
      recovery_target: null,
      target_unit: 'none',
      match_type: 'at_least_once',
      compare_op: 'above',
      channels: [],
    };
  },

  /**
   * Create a default anomaly threshold object
   * @param {ThresholdSeverity} severity - Severity level
   * @param {number} multiplier - Spike multiplier
   * @returns {AnomalyThreshold} Default anomaly threshold object
   */
  createDefaultAnomalyThreshold(severity = 'critical', multiplier = 10) {
    return {
      id: crypto.randomUUID(),
      name: severity,
      spike_multiplier: multiplier,
      min_baseline: 1,
      channels: [],
    };
  },

  /**
   * Get default multiplier for a severity level
   * @param {ThresholdSeverity} severity - Severity level
   * @returns {number} Default spike multiplier
   */
  getDefaultMultiplier(severity) {
    const defaults = {
      critical: 20,
      warning: 10,
      info: 5,
    };
    return defaults[severity] || 10;
  },

  /**
   * Convert threshold value between units
   * @param {number} value - Value to convert
   * @param {ThresholdUnit} fromUnit - Source unit
   * @param {ThresholdUnit} toUnit - Target unit
   * @returns {number|null} Converted value or null if incompatible
   */
  convertUnit(value, fromUnit, toUnit) {
    const unitCategories = {
      time: ['nanoseconds', 'microseconds', 'milliseconds', 'seconds', 'minutes', 'hours', 'days'],
      data: ['bytes', 'kilobytes', 'megabytes', 'gigabytes', 'terabytes'],
      rate: ['per_second', 'per_minute', 'per_hour'],
      percentage: ['percent', 'decimal_percent'],
      none: ['none'],
    };

    // Find categories for both units
    let fromCategory = null;
    let toCategory = null;
    for (const [category, units] of Object.entries(unitCategories)) {
      if (units.includes(fromUnit)) fromCategory = category;
      if (units.includes(toUnit)) toCategory = category;
    }

    // Units must be in the same category to convert
    if (fromCategory !== toCategory) {
      return null;
    }

    // Convert to base unit first, then to target unit
    const toBase = this.unitToBase(value, fromUnit);
    return this.baseToUnit(toBase, toUnit);
  },

  /**
   * Convert value to base unit
   * @param {number} value - Value to convert
   * @param {ThresholdUnit} unit - Source unit
   * @returns {number} Value in base unit
   */
  unitToBase(value, unit) {
    const conversions = {
      // Time -> seconds
      nanoseconds: (v) => v / 1e9,
      microseconds: (v) => v / 1e6,
      milliseconds: (v) => v / 1000,
      seconds: (v) => v,
      minutes: (v) => v * 60,
      hours: (v) => v * 3600,
      days: (v) => v * 86400,
      // Data -> bytes
      bytes: (v) => v,
      kilobytes: (v) => v * 1024,
      megabytes: (v) => v * 1024 * 1024,
      gigabytes: (v) => v * 1024 * 1024 * 1024,
      terabytes: (v) => v * 1024 * 1024 * 1024 * 1024,
      // Rate -> per second
      per_second: (v) => v,
      per_minute: (v) => v / 60,
      per_hour: (v) => v / 3600,
      // Percentage -> percent
      percent: (v) => v,
      decimal_percent: (v) => v * 100,
      // None
      none: (v) => v,
    };
    return conversions[unit]?.(value) ?? value;
  },

  /**
   * Convert value from base unit
   * @param {number} value - Value in base unit
   * @param {ThresholdUnit} unit - Target unit
   * @returns {number} Converted value
   */
  baseToUnit(value, unit) {
    const conversions = {
      // Seconds -> time
      nanoseconds: (v) => v * 1e9,
      microseconds: (v) => v * 1e6,
      milliseconds: (v) => v * 1000,
      seconds: (v) => v,
      minutes: (v) => v / 60,
      hours: (v) => v / 3600,
      days: (v) => v / 86400,
      // Bytes -> data
      bytes: (v) => v,
      kilobytes: (v) => v / 1024,
      megabytes: (v) => v / (1024 * 1024),
      gigabytes: (v) => v / (1024 * 1024 * 1024),
      terabytes: (v) => v / (1024 * 1024 * 1024 * 1024),
      // Per second -> rate
      per_second: (v) => v,
      per_minute: (v) => v * 60,
      per_hour: (v) => v * 3600,
      // Percent -> percentage
      percent: (v) => v,
      decimal_percent: (v) => v / 100,
      // None
      none: (v) => v,
    };
    return conversions[unit]?.(value) ?? value;
  },

  /**
   * Get display label for a unit
   * @param {ThresholdUnit} unit - Unit
   * @returns {string} Display label
   */
  getUnitLabel(unit) {
    const labels = {
      nanoseconds: 'ns',
      microseconds: 'μs',
      milliseconds: 'ms',
      seconds: 's',
      minutes: 'min',
      hours: 'h',
      days: 'd',
      bytes: 'B',
      kilobytes: 'KB',
      megabytes: 'MB',
      gigabytes: 'GB',
      terabytes: 'TB',
      per_second: '/s',
      per_minute: '/min',
      per_hour: '/h',
      percent: '%',
      decimal_percent: 'ratio',
      none: '',
    };
    return labels[unit] || '';
  },

  /**
   * Get severity color for display
   * @param {ThresholdSeverity} severity - Severity level
   * @returns {string} Hex color code
   */
  getSeverityColor(severity) {
    const colors = {
      critical: '#ef4444',
      warning: '#f59e0b',
      info: '#3b82f6',
    };
    return colors[severity] || '#6b7280';
  },

  /**
   * Sort thresholds by severity (critical first)
   * @param {AlertThreshold[]|AnomalyThreshold[]} thresholds - Array of thresholds
   * @returns {AlertThreshold[]|AnomalyThreshold[]} Sorted thresholds
   */
  sortBySeverity(thresholds) {
    const order = { critical: 0, warning: 1, info: 2 };
    return [...thresholds].sort((a, b) => {
      const orderA = order[a.name] ?? 99;
      const orderB = order[b.name] ?? 99;
      return orderA - orderB;
    });
  },
};

export default alertService;
