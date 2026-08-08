<template>
  <AppLayout :user="user" :current-project="currentProject">
    <div class="max-w-[1200px] mx-auto px-8 py-6">
      <!-- Header -->
      <div class="mb-6">
        <div>
          <h1 class="text-2xl font-semibold text-gray-900">Billing & Usage</h1>
          <p class="billing-subtitle">
            Organization usage for the current billing period, including observability ingestion and AI Gateway costs.
          </p>
        </div>
      </div>

      <!-- Loading State -->
      <div v-if="loading" class="billing-loading">
        <div class="spinner"></div>
        <p>Loading usage data...</p>
      </div>

      <!-- Error State -->
      <div v-else-if="error" class="billing-error">
        <svg class="w-12 h-12 mx-auto mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
        </svg>
        <p class="error-title">Failed to load billing data</p>
        <p class="error-detail">{{ error }}</p>
        <button class="retry-btn" @click="fetchAll">Retry</button>
      </div>

      <!-- Content -->
      <template v-else>
        <!-- Current Plan -->
        <BaseCard class="billing-card">
          <template #header>
            <h2 class="card-title">Current Plan</h2>
          </template>
          <div class="plan-content" v-if="currentTier">
            <div class="plan-summary">
              <div class="plan-name">{{ currentTier.display_name }}</div>
            </div>
            <div class="plan-features" v-if="enabledProducts(currentTier).length">
              <span
                v-for="product in enabledProducts(currentTier)"
                :key="product"
                class="plan-feature-badge"
              >{{ formatLabel(product) }}</span>
            </div>
            <div class="plan-actions">
              <button
                class="portal-btn plan-change-btn"
                @click="showTierPicker = !showTierPicker"
              >
                {{ showTierPicker ? 'Cancel' : 'Change plan' }}
              </button>
            </div>
          </div>
          <div v-else class="text-sm text-gray-400 py-2">Loading plan...</div>

          <!-- Tier picker -->
          <div v-if="showTierPicker && availableTiers.length > 0" class="tier-picker">
            <div
              v-for="tier in availableTiers"
              :key="tier.id"
              class="tier-option"
              :class="{ 'tier-current': tier.is_current }"
            >
              <div class="tier-option-header">
                <span class="tier-option-name">{{ tier.display_name }}</span>
              </div>
              <div class="tier-option-products" v-if="enabledProducts(tier).length">
                <span v-for="p in enabledProducts(tier)" :key="p" class="plan-feature-badge plan-feature-badge-sm">{{ formatLabel(p) }}</span>
              </div>
              <button
                v-if="!tier.is_current && tier.is_public"
                class="tier-select-btn"
                :disabled="tierChangeLoading"
                @click="changeTier(tier)"
              >
                Select
              </button>
              <span v-else-if="tier.is_current" class="tier-current-label">Current plan</span>
            </div>
          </div>
          <div v-if="tierChangeError" class="mt-3 rounded-lg bg-red-50 border border-red-200 px-4 py-2 text-sm text-red-700">
            {{ tierChangeError }}
          </div>
        </BaseCard>

        <!-- Payment Method -->
        <BaseCard class="billing-card">
          <template #header>
            <h2 class="card-title">Payment Method</h2>
          </template>
          <div class="subscription-content">
            <div v-if="hasPaymentMethod === true" class="subscription-details">
              <div class="sub-grid">
                <div class="sub-item">
                  <span class="sub-label">Status</span>
                  <span class="sub-value">
                    <span class="status-badge status-active">Active</span>
                  </span>
                </div>
              </div>
              <div class="sub-actions">
                <button class="portal-btn" @click="openPortal" :disabled="portalLoading">
                  {{ portalLoading ? 'Opening...' : 'Manage billing' }}
                </button>
              </div>
            </div>
            <div v-else-if="hasPaymentMethod === false" class="no-subscription">
              <p>No payment method linked.</p>
              <button class="portal-btn" @click="openPortal" :disabled="portalLoading">
                {{ portalLoading ? 'Opening...' : 'Link payment method' }}
              </button>
            </div>
          </div>
        </BaseCard>

        <!-- Invoices -->
        <BaseCard class="billing-card" v-if="invoices.length > 0">
          <template #header>
            <h2 class="card-title">Recent Invoices</h2>
          </template>
          <table class="billing-table">
            <thead>
              <tr>
                <th>Invoice</th>
                <th>Status</th>
                <th class="text-right">Amount</th>
                <th>Date</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="inv in invoices" :key="inv.id">
                <td>{{ inv.invoice_number }}</td>
                <td>
                  <span class="status-badge" :class="'status-' + inv.status">{{ inv.status }}</span>
                </td>
                <td class="text-right">{{ formatCentsCurrency(inv.total_cents, inv.currency) }}</td>
                <td>{{ inv.paid_at ? formatDate(inv.paid_at) : (inv.period_start ? formatDate(inv.period_start) : '—') }}</td>
                <td class="text-right">
                  <a v-if="inv.hosted_invoice_url" :href="inv.hosted_invoice_url" target="_blank" rel="noopener" class="invoice-link">View</a>
                  <a v-if="inv.invoice_pdf_url" :href="inv.invoice_pdf_url" target="_blank" rel="noopener" class="invoice-link ml-2">PDF</a>
                </td>
              </tr>
            </tbody>
          </table>
        </BaseCard>

        <!-- Flow Credits (hidden when credit system is disabled) -->
        <BaseCard v-if="creditsEnabled" class="billing-card">
          <template #header>
            <div class="flex items-center justify-between w-full">
              <h2 class="card-title">Flow Credits</h2>
              <span v-if="creditBalance !== null" class="text-lg font-semibold text-brand-600 dark:text-brand-400">
                {{ formatCurrency(creditBalance) }}
              </span>
            </div>
          </template>
          <div v-if="creditsLoading" class="text-center py-4 text-sm text-gray-500">Loading credits...</div>
          <div v-else>
            <p class="text-sm text-gray-500 dark:text-gray-400 mb-4">
              Credits are used when making requests with platform-managed API keys. Purchase credits to fund your AI usage.
            </p>
            <div class="flex flex-wrap gap-2 mb-6">
              <button
                v-for="amt in creditAmounts"
                :key="amt"
                @click="purchaseCredits(amt)"
                :disabled="creditCheckoutLoading"
                class="px-4 py-2 text-sm font-medium rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors disabled:opacity-50"
              >
                {{ creditCheckoutLoading ? '...' : `$${amt}` }}
              </button>
            </div>

            <!-- Recent Transactions -->
            <div v-if="creditTransactions.length > 0">
              <h3 class="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">Recent Transactions</h3>
              <table class="billing-table">
                <thead>
                  <tr>
                    <th>Type</th>
                    <th>Details</th>
                    <th class="text-right">Amount</th>
                    <th class="text-right">Balance After</th>
                    <th>Date</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="tx in creditTransactions" :key="tx.id">
                    <td>
                      <span
                        class="inline-flex items-center px-2 py-0.5 text-xs font-medium rounded"
                        :class="{
                          'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300': tx.transaction_type === 'top_up',
                          'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300': tx.transaction_type === 'usage_deduction',
                          'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300': tx.transaction_type === 'refund',
                          'bg-gray-100 text-gray-600 dark:bg-gray-700 dark:text-gray-300': tx.transaction_type === 'adjustment',
                        }"
                      >{{ formatTransactionType(tx.transaction_type) }}</span>
                    </td>
                    <td class="text-sm text-gray-600 dark:text-gray-400 max-w-xs break-words">
                      {{ formatTransactionDetail(tx) }}
                    </td>
                    <td class="text-right" :class="parseFloat(tx.amount_usd) >= 0 ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'">
                      {{ parseFloat(tx.amount_usd) >= 0 ? '+' : '' }}{{ formatCurrency(tx.amount_usd, 6) }}
                    </td>
                    <td class="text-right">{{ formatCurrency(tx.balance_after_usd, 6) }}</td>
                    <td>{{ formatDate(tx.created_at) }}</td>
                  </tr>
                </tbody>
              </table>
            </div>

          </div>
        </BaseCard>

        <!-- Card 1: Period Summary -->
        <BaseCard class="billing-card">
          <template #header>
            <h2 class="card-title">Current Period Summary</h2>
            <span class="period-label" v-if="usage">
              {{ formatDate(usage.period_start) }} &mdash; {{ formatDate(usage.period_end) }}
            </span>
          </template>
          <div class="stat-grid">
            <div class="stat-box">
              <div class="stat-label">Observability</div>
              <div class="stat-value stat-value-currency">{{ formatCurrency(observabilityCost) }}</div>
            </div>
            <div class="stat-box">
              <div class="stat-label">AI Gateway</div>
              <div class="stat-value stat-value-currency">{{ formatCurrency(gatewayCost) }}</div>
            </div>
            <div class="stat-box">
              <div class="stat-label">Total Cost</div>
              <div class="stat-value stat-value-currency">{{ formatCurrency(totalCost) }}</div>
            </div>
          </div>
        </BaseCard>

        <!-- Card 2: Breakdown by Event Type -->
        <BaseCard class="billing-card">
          <template #header>
            <h2 class="card-title">Usage by Event Type</h2>
          </template>
          <div class="type-breakdown" v-if="usage">
            <table class="billing-table">
              <thead>
                <tr>
                  <th>Type</th>
                  <th class="text-right">Usage</th>
                  <th class="text-right">% of Cost</th>
                  <th class="text-right">Cost</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="row in eventTypeRows" :key="row.type">
                  <td>
                    <div class="type-cell">
                      <span class="type-dot" :style="{ backgroundColor: row.color }"></span>
                      {{ row.type }}
                    </div>
                  </td>
                  <td class="text-right">{{ row.usage }}</td>
                  <td class="text-right">{{ row.percent }}%</td>
                  <td class="text-right">{{ formatCurrency(row.cost) }}</td>
                </tr>
              </tbody>
              <tfoot>
                <tr>
                  <td><strong>Total</strong></td>
                  <td class="text-right"></td>
                  <td class="text-right"><strong>100%</strong></td>
                  <td class="text-right"><strong>{{ formatCurrency(usage.estimated_cost_usd) }}</strong></td>
                </tr>
              </tfoot>
            </table>
            <!-- Proportion bars -->
            <div class="proportion-bar">
              <div
                v-for="row in eventTypeRows"
                :key="'bar-' + row.type"
                class="proportion-segment"
                :style="{ width: row.percent + '%', backgroundColor: row.color }"
                :title="row.type + ': ' + row.percent + '%'"
              ></div>
            </div>
          </div>
          <div v-else class="empty-state">No usage data available.</div>
        </BaseCard>

        <!-- Card 3: AI Gateway Usage -->
        <BaseCard class="billing-card" v-if="usage && (usage.gateway_requests > 0 || gatewayModels.length > 0)">
          <template #header>
            <h2 class="card-title">AI Gateway Usage</h2>
          </template>
          <div class="stat-grid stat-grid-4 mb-4">
            <div class="stat-box">
              <div class="stat-label">Requests</div>
              <div class="stat-value">{{ formatNumber(usage.gateway_requests) }}</div>
            </div>
            <div class="stat-box">
              <div class="stat-label">Input Tokens</div>
              <div class="stat-value">{{ formatNumber(usage.gateway_input_tokens) }}</div>
            </div>
            <div class="stat-box">
              <div class="stat-label">Output Tokens</div>
              <div class="stat-value">{{ formatNumber(usage.gateway_output_tokens) }}</div>
            </div>
            <div class="stat-box">
              <div class="stat-label">Cost</div>
              <div class="stat-value stat-value-currency">{{ formatCurrency(usage.gateway_cost_usd) }}</div>
            </div>
          </div>
          <div v-if="gatewayModels.length > 0">
            <h3 class="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">Cost by Model</h3>
            <table class="billing-table">
              <thead>
                <tr>
                  <th>Provider</th>
                  <th>Model</th>
                  <th class="text-right">Requests</th>
                  <th class="text-right">Tokens</th>
                  <th class="text-right">Cost</th>
                  <th class="text-right">%</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="m in gatewayModels" :key="m.provider + '/' + m.model">
                  <td>{{ m.provider }}</td>
                  <td>{{ m.model }}</td>
                  <td class="text-right">{{ formatNumber(m.request_count) }}</td>
                  <td class="text-right">{{ formatNumber(m.input_tokens + m.output_tokens) }}</td>
                  <td class="text-right">{{ formatCurrency(m.cost_usd) }}</td>
                  <td class="text-right">{{ gatewayModelPercent(m) }}%</td>
                </tr>
              </tbody>
            </table>
          </div>
        </BaseCard>

        <!-- Card 4: Usage by Project -->
        <BaseCard class="billing-card">
          <template #header>
            <h2 class="card-title">Usage by Project</h2>
          </template>
          <div v-if="projectUsage && projectUsage.length > 0">
            <table class="billing-table">
              <thead>
                <tr>
                  <th>Project</th>
                  <th class="text-right">Spans</th>
                  <th class="text-right">Logs</th>
                  <th class="text-right">Metrics</th>
                  <th class="text-right">Obs. Cost</th>
                  <th class="text-right">Gateway Cost</th>
                  <th class="text-right">Total</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="proj in projectUsage" :key="proj.project_id">
                  <td>{{ proj.project_name || proj.project_id }}</td>
                  <td class="text-right">{{ formatBytes(proj.spans_ingested_bytes) }}</td>
                  <td class="text-right">{{ formatBytes(proj.logs_ingested_bytes) }}</td>
                  <td class="text-right">{{ formatNumber(proj.metrics_count) }}</td>
                  <td class="text-right">{{ formatCurrency(proj.estimated_cost_usd) }}</td>
                  <td class="text-right">{{ formatCurrency(proj.gateway_cost_usd) }}</td>
                  <td class="text-right">{{ formatCurrency(projectTotalCost(proj)) }}</td>
                </tr>
              </tbody>
            </table>
          </div>
          <div v-else class="empty-state">
            <p>No project usage data for this period.</p>
          </div>
        </BaseCard>

        <!-- Card 4: Budget Status (conditional) -->
        <BaseCard class="billing-card" v-if="budgetStatus">
          <template #header>
            <h2 class="card-title">Budget Status</h2>
          </template>
          <div class="budget-content">
            <div class="budget-summary">
              <div class="budget-amount">
                <span class="budget-label">Monthly Budget</span>
                <span class="budget-value">{{ formatCurrency(budgetStatus.budget.monthly_budget_usd) }}</span>
              </div>
              <div class="budget-amount">
                <span class="budget-label">Current Spend</span>
                <span class="budget-value">{{ formatCurrency(budgetStatus.current_cost_usd) }}</span>
              </div>
            </div>
            <!-- Progress bar -->
            <div class="budget-bar-container">
              <div class="budget-bar-track">
                <div
                  class="budget-bar-fill"
                  :class="{
                    'budget-bar-warning': budgetStatus.usage_percent >= budgetStatus.budget.alert_threshold_percent,
                    'budget-bar-exceeded': budgetStatus.budget_exceeded
                  }"
                  :style="{ width: Math.min(budgetStatus.usage_percent, 100) + '%' }"
                ></div>
                <!-- Threshold marker -->
                <div
                  class="budget-threshold-marker"
                  :style="{ left: budgetStatus.budget.alert_threshold_percent + '%' }"
                  :title="'Alert threshold: ' + budgetStatus.budget.alert_threshold_percent + '%'"
                ></div>
              </div>
              <div class="budget-bar-labels">
                <span>{{ Math.round(budgetStatus.usage_percent) }}% used</span>
              </div>
            </div>
          </div>
        </BaseCard>
      </template>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue';
import axios from 'axios';
import { chartTheme } from '../../../utils/chartTheme.js';
import { useAuth } from '../../../composables/useAuth.js';
import AppLayout from '../../../Layouts/AppLayout.vue';
import BaseCard from '../../../components/BaseCard.vue';

const { user, fetchUser } = useAuth();

const currentProject = ref(null);
const loading = ref(true);
const error = ref(null);

// Billing data
const invoices = ref([]);
const portalLoading = ref(false);
/** null until loaded; drives portal CTA label */
const hasPaymentMethod = ref(null);

// API data
const usage = ref(null);
const projectUsage = ref([]);
const budgetStatus = ref(null);
const gatewayModels = ref([]);

// Current plan / tier picker
const currentTier = ref(null);
const availableTiers = ref([]);
const showTierPicker = ref(false);
const tierChangeLoading = ref(false);
const tierChangeError = ref('');

// Flow credits
const creditsEnabled = ref(false);
const creditBalance = ref(null);
const creditTransactions = ref([]);
const creditsLoading = ref(false);
const creditCheckoutLoading = ref(false);
const creditAmounts = [10, 25, 50, 100, 250];

// Computed
const observabilityCost = computed(() => {
  if (!usage.value) return 0;
  return parseFloat(usage.value.estimated_cost_usd) || 0;
});

const gatewayCost = computed(() => {
  if (!usage.value) return 0;
  return parseFloat(usage.value.gateway_cost_usd) || 0;
});

const totalCost = computed(() => {
  return observabilityCost.value + gatewayCost.value;
});

const eventTypeRows = computed(() => {
  if (!usage.value) return [];
  const totalCost = Number(usage.value.estimated_cost_usd) || 1;

  const spanBytes = usage.value.spans_ingested_bytes || 0;
  const logBytes = usage.value.logs_ingested_bytes || 0;
  const metricsCount = usage.value.metrics_count || 0;

  const perGbUsd = 0.20;
  const perMillionUsd = 0.10;
  const spanCost = (spanBytes / 1_000_000_000) * perGbUsd;
  const logCost = (logBytes / 1_000_000_000) * perGbUsd;
  const metricCost = (metricsCount / 1_000_000) * perMillionUsd;
  const computedTotal = spanCost + logCost + metricCost || 1;

  const rows = [
    {
      type: 'Spans',
      usage: formatBytes(spanBytes),
      percent: ((spanCost / computedTotal) * 100).toFixed(1),
      color: chartTheme.colors.lineColors[0],
      cost: spanCost,
    },
    {
      type: 'Logs',
      usage: formatBytes(logBytes),
      percent: ((logCost / computedTotal) * 100).toFixed(1),
      color: chartTheme.colors.lineColors[1],
      cost: logCost,
    },
    {
      type: 'Metrics',
      usage: formatNumber(metricsCount) + ' data points',
      percent: ((metricCost / computedTotal) * 100).toFixed(1),
      color: chartTheme.colors.lineColors[2],
      cost: metricCost,
    },
  ];
  return rows;
});

// Formatting helpers
function formatNumber(n) {
  if (n == null) return '—';
  const num = Number(n);
  if (num >= 1_000_000_000) return (num / 1_000_000_000).toFixed(1) + 'B';
  if (num >= 1_000_000) return (num / 1_000_000).toFixed(1) + 'M';
  if (num >= 1_000) return (num / 1_000).toFixed(1) + 'K';
  return num.toLocaleString();
}

function formatBytes(bytes) {
  if (bytes == null || bytes === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const k = 1024;
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return (bytes / Math.pow(k, i)).toFixed(1) + ' ' + units[i];
}

function formatCurrency(value, maxDecimals = 2) {
  if (value == null) return '—';
  const num = typeof value === 'string' ? parseFloat(value) : Number(value);
  if (isNaN(num)) return '—';
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 2,
    maximumFractionDigits: maxDecimals,
  }).format(num);
}

function formatDate(dateStr) {
  if (!dateStr) return '';
  const d = new Date(dateStr);
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
}

function formatCentsCurrency(cents, currency = 'usd') {
  if (cents == null) return '—';
  const amount = cents / 100;
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: currency.toUpperCase(),
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(amount);
}


function formatLabel(key) {
  return key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
}

function enabledProducts(tier) {
  const config = tier?.config;
  if (!config) return [];
  const products = [];
  if (config.prompt_hub?.enabled) products.push('prompt_hub');
  if (config.watch?.enabled) products.push('watch');
  if (config.herd?.enabled) products.push('herd');
  return products;
}

async function fetchTiers() {
  try {
    const { data } = await axios.get('/api/payments/tiers');
    if (data?.success && data.data) {
      availableTiers.value = data.data;
      currentTier.value = data.data.find(t => t.is_current) || null;
    }
  } catch (e) {
    // Non-critical
  }
}

async function changeTier(tier) {
  tierChangeLoading.value = true;
  tierChangeError.value = '';
  try {
    await axios.post('/api/payments/tier', { tier_id: tier.id });
    showTierPicker.value = false;
    await fetchTiers();
  } catch (e) {
    tierChangeError.value = e.response?.data?.error || 'Failed to change plan. Please try again.';
  } finally {
    tierChangeLoading.value = false;
  }
}

function gatewayModelPercent(m) {
  const total = parseFloat(usage.value?.gateway_cost_usd) || 0;
  if (total <= 0) return '0.0';
  const cost = parseFloat(m.cost_usd) || 0;
  return ((cost / total) * 100).toFixed(1);
}

function projectTotalCost(proj) {
  const obs = parseFloat(proj.estimated_cost_usd) || 0;
  const gw = parseFloat(proj.gateway_cost_usd) || 0;
  return obs + gw;
}

async function fetchPaymentMethodStatus() {
  try {
    const res = await axios.get('/api/payments/methods/status');
    if (res.data?.success && res.data.data && typeof res.data.data.has_payment_method === 'boolean') {
      hasPaymentMethod.value = res.data.data.has_payment_method;
    } else {
      hasPaymentMethod.value = false;
    }
  } catch {
    hasPaymentMethod.value = false;
  }
}

async function fetchInvoices() {
  try {
    const res = await axios.get('/api/payments/invoices?limit=10');
    if (res.data?.success && res.data.data?.invoices) {
      invoices.value = res.data.data.invoices;
    }
  } catch (e) {
    // Non-critical
  }
}

async function openPortal() {
  portalLoading.value = true;
  try {
    const res = await axios.post('/api/payments/portal');
    if (res.data?.success && res.data.data?.url) {
      window.location.href = res.data.data.url;
    }
  } catch (e) {
    alert(e.response?.data?.error || 'Failed to open billing portal');
  } finally {
    portalLoading.value = false;
  }
}

// Data fetching
async function fetchAll() {
  loading.value = true;
  error.value = null;

  try {
    await fetchUser();

    // Fetch the user's first project for sidebar navigation context
    try {
      const projectsRes = await axios.get('/api/projects');
      const projects = projectsRes.data || [];
      if (projects.length > 0) {
        currentProject.value = projects[0];
      }
    } catch (e) {
      // Non-critical: sidebar will show "Loading project..." but page still works
    }

    const [usageRes, projectRes, budgetRes, gwModelsRes] = await Promise.allSettled([
      axios.get('/api/billing/usage'),
      axios.get('/api/billing/usage/by-project'),
      axios.get('/api/billing/budget/status'),
      axios.get('/api/billing/usage/gateway-models'),
    ]);

    if (usageRes.status === 'fulfilled' && usageRes.value.data?.success) {
      usage.value = usageRes.value.data.data;
    }
    if (projectRes.status === 'fulfilled' && projectRes.value.data?.success) {
      projectUsage.value = projectRes.value.data.data || [];
    }
    if (budgetRes.status === 'fulfilled' && budgetRes.value.data?.success) {
      budgetStatus.value = budgetRes.value.data.data; // null if no budget
    }
    if (gwModelsRes.status === 'fulfilled' && gwModelsRes.value.data?.success) {
      gatewayModels.value = gwModelsRes.value.data.data || [];
    }

    // If usage call failed completely, show error
    if (usageRes.status === 'rejected') {
      throw usageRes.reason;
    }

    // Check if credits are enabled and fetch credit data if so
    if (currentProject.value?.id) {
      try {
        const overviewRes = await axios.get(`/api/projects/${currentProject.value.id}/llm/metrics/overview`);
        creditsEnabled.value = overviewRes.data?.credits_enabled === true;
      } catch (_) {
        creditsEnabled.value = false;
      }
    }
    if (creditsEnabled.value) {
      fetchCredits();
    }
  } catch (e) {
    error.value = e.response?.data?.error || e.message || 'An unexpected error occurred';
  } finally {
    loading.value = false;
  }
}

async function fetchCredits() {
  creditsLoading.value = true;
  try {
    const [balRes, txRes] = await Promise.allSettled([
      axios.get('/api/payments/credits/balance'),
      axios.get('/api/payments/credits/transactions', { params: { limit: 10 } }),
    ]);
    if (balRes.status === 'fulfilled') creditBalance.value = balRes.value.data?.balance_usd ?? null;
    if (txRes.status === 'fulfilled') creditTransactions.value = txRes.value.data?.transactions ?? [];
  } catch (e) {
    console.error('Failed to fetch credits:', e);
  } finally {
    creditsLoading.value = false;
  }
}

async function purchaseCredits(amount) {
  creditCheckoutLoading.value = true;
  try {
    const res = await axios.post('/api/payments/credits/checkout', {
      credit_amount_usd: amount,
      success_url: window.location.origin + '/settings/billing?credits=success',
      cancel_url: window.location.origin + '/settings/billing?credits=cancelled',
    });
    const url = res.data?.data?.checkout_url;
    if (url) {
      const w = 500;
      const h = 700;
      const left = window.screenX + (window.outerWidth - w) / 2;
      const top = window.screenY + (window.outerHeight - h) / 2;
      const checkoutWindow = window.open(
        url,
        'stripe_checkout',
        `width=${w},height=${h},left=${left},top=${top},toolbar=no,menubar=no,scrollbars=yes,resizable=yes`
      );
      pollForCreditUpdate(checkoutWindow);
    }
  } catch (e) {
    console.error('Failed to create checkout:', e);
    alert(e.response?.data?.error || 'Failed to create checkout session');
  } finally {
    creditCheckoutLoading.value = false;
  }
}

function pollForCreditUpdate(checkoutWindow) {
  const startBalance = creditBalance.value;
  const pollInterval = setInterval(async () => {
    try {
      const res = await axios.get('/api/payments/credits/balance');
      const newBalance = res.data?.balance_usd;
      if (newBalance !== null && newBalance !== startBalance) {
        clearInterval(pollInterval);
        creditBalance.value = newBalance;
        if (checkoutWindow && !checkoutWindow.closed) checkoutWindow.close();
        await fetchCredits();
      }
    } catch (_) { /* ignore polling errors */ }

    if (checkoutWindow && checkoutWindow.closed) {
      clearInterval(pollInterval);
      fetchCredits();
    }
  }, 3000);

  setTimeout(() => clearInterval(pollInterval), 600000);
}

function formatTransactionType(type) {
  const map = { top_up: 'Top Up', usage_deduction: 'Usage', refund: 'Refund', adjustment: 'Adjustment' };
  return map[type] || type;
}

/** Human-readable line item for ledger rows (provider/model/tokens for usage, etc.). */
function formatTransactionDetail(tx) {
  const t = tx.transaction_type;
  if (t === 'usage_deduction') {
    const parts = [];
    const prov = tx.provider?.trim();
    const model = tx.model?.trim();
    if (prov && model) parts.push(`${prov} · ${model}`);
    else if (model) parts.push(model);
    else if (prov) parts.push(prov);
    const tin = tx.input_tokens;
    const tout = tx.output_tokens;
    if (tin != null || tout != null) {
      parts.push(`${tin ?? '—'} in / ${tout ?? '—'} out tokens`);
    }
    if (tx.llm_request_id) {
      const id = String(tx.llm_request_id);
      parts.push(id.length > 14 ? `Request ${id.slice(0, 10)}…` : `Request ${id}`);
    }
    if (parts.length) return parts.join(' · ');
    return tx.description || '—';
  }
  if (tx.description?.trim()) return tx.description.trim();
  if (t === 'top_up' && tx.paid_amount != null && tx.paid_currency) {
    return `Paid ${tx.paid_amount} ${tx.paid_currency}`;
  }
  if (t === 'top_up') return 'Credit purchase';
  return '—';
}

onMounted(async () => {
  const params = new URLSearchParams(window.location.search);
  if ((params.get('credits') === 'success' || params.get('credits') === 'cancelled') && window.opener) {
    window.close();
    return;
  }

  fetchPaymentMethodStatus();
  fetchInvoices();
  fetchTiers();
  await fetchAll();
});
</script>

<style scoped>
.billing-subtitle {
  font-size: 14px;
  color: #6b7280;
  margin-top: 6px;
}

.estimated-badge {
  display: inline-block;
  font-size: 11px;
  color: #f59e0b;
  background-color: rgba(245, 158, 11, 0.1);
  border: 1px solid rgba(245, 158, 11, 0.3);
  border-radius: 4px;
  padding: 1px 6px;
  margin-left: 6px;
  vertical-align: middle;
}

.estimated-pill {
  display: inline-block;
  font-size: 10px;
  color: #f59e0b;
  background-color: rgba(245, 158, 11, 0.1);
  border-radius: 3px;
  padding: 0 4px;
  margin-left: 4px;
  vertical-align: middle;
  font-weight: 500;
}

/* Loading */
.billing-loading {
  text-align: center;
  padding: 80px 0;
  color: #6b7280;
}

.spinner {
  width: 36px;
  height: 36px;
  border: 3px solid #e5e7eb;
  border-top-color: #4f46e5;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
  margin: 0 auto 16px;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

/* Error */
.billing-error {
  text-align: center;
  padding: 60px 0;
  color: #6b7280;
}

.error-title {
  font-size: 16px;
  font-weight: 500;
  color: #ef4444;
  margin-bottom: 4px;
}

.error-detail {
  font-size: 13px;
  color: #6b7280;
  margin-bottom: 16px;
}

.retry-btn {
  background-color: #4f46e5;
  color: #ffffff;
  border: none;
  border-radius: 6px;
  padding: 8px 20px;
  font-size: 13px;
  font-weight: 500;
  cursor: pointer;
  transition: background-color 0.2s;
}

.retry-btn:hover {
  background-color: #4338ca;
}

/* Cards */
.billing-card {
  margin-bottom: 20px;
}

.card-title {
  font-size: 15px;
  font-weight: 600;
  color: #111827;
  margin: 0;
}

.period-label {
  font-size: 12px;
  color: #6b7280;
  margin-left: 12px;
}

/* Stat Grid */
.stat-grid {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 16px;
}

.stat-grid-4 {
  grid-template-columns: repeat(4, 1fr);
}

@media (max-width: 900px) {
  .stat-grid,
  .stat-grid-4 {
    grid-template-columns: repeat(2, 1fr);
  }
}

.stat-box {
  background-color: #f9fafb;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  padding: 16px;
}

.stat-label {
  font-size: 12px;
  font-weight: 500;
  color: #6b7280;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  margin-bottom: 8px;
}

.stat-value {
  font-size: 28px;
  font-weight: 700;
  color: #111827;
  line-height: 1.2;
}

.stat-value-currency {
  color: #22c55e;
}

.stat-detail {
  font-size: 12px;
  color: #9ca3af;
  margin-top: 4px;
}

/* Type Breakdown Table */
.billing-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 13px;
}

.billing-table th {
  text-align: left;
  font-size: 11px;
  font-weight: 600;
  color: #6b7280;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  padding: 8px 12px;
  border-bottom: 1px solid #e5e7eb;
}

.billing-table td {
  padding: 10px 12px;
  color: #374151;
  border-bottom: 1px solid #f3f4f6;
}

.billing-table tbody tr:hover {
  background-color: #f9fafb;
}

.billing-table tfoot td {
  border-top: 1px solid #e5e7eb;
  border-bottom: none;
  color: #111827;
}

.text-right {
  text-align: right;
}

.type-cell {
  display: flex;
  align-items: center;
  gap: 8px;
}

.type-dot {
  width: 10px;
  height: 10px;
  border-radius: 50%;
  flex-shrink: 0;
}

/* Proportion bar */
.proportion-bar {
  display: flex;
  height: 8px;
  border-radius: 4px;
  overflow: hidden;
  margin-top: 16px;
  background-color: #e5e7eb;
}

.proportion-segment {
  height: 100%;
  transition: width 0.3s ease;
}

/* Budget */
.budget-content {
  display: flex;
  flex-direction: column;
  gap: 20px;
}

.budget-summary {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 16px;
}

@media (max-width: 700px) {
  .budget-summary {
    grid-template-columns: 1fr;
  }
}

.budget-amount {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.budget-label {
  font-size: 12px;
  color: #6b7280;
  font-weight: 500;
}

.budget-value {
  font-size: 20px;
  font-weight: 600;
  color: #111827;
}

.budget-over {
  color: #ef4444;
}

.budget-bar-container {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.budget-bar-track {
  position: relative;
  height: 12px;
  background-color: #e5e7eb;
  border-radius: 6px;
  overflow: visible;
}

.budget-bar-fill {
  height: 100%;
  border-radius: 6px;
  background-color: #4f46e5;
  transition: width 0.5s ease;
}

.budget-bar-warning {
  background-color: #f59e0b;
}

.budget-bar-exceeded {
  background-color: #ef4444;
}

.budget-threshold-marker {
  position: absolute;
  top: -3px;
  width: 2px;
  height: 18px;
  background-color: #374151;
  border-radius: 1px;
  opacity: 0.6;
}

.budget-bar-labels {
  display: flex;
  justify-content: space-between;
  font-size: 12px;
  color: #6b7280;
}

.budget-warning-text {
  color: #f59e0b;
  font-weight: 500;
}

/* Empty state */
.empty-state {
  text-align: center;
  padding: 40px 0;
  color: #9ca3af;
  font-size: 14px;
}

/* Subscription */
.subscription-content {
  padding: 4px 0;
}

.subscription-loading {
  color: #6b7280;
  font-size: 13px;
  padding: 12px 0;
}

.subscription-details {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.sub-grid {
  display: flex;
  flex-wrap: wrap;
  gap: 24px;
}

.sub-item {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.sub-label {
  font-size: 11px;
  font-weight: 600;
  color: #6b7280;
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.sub-value {
  font-size: 14px;
  color: #111827;
}

.sub-cancel-notice {
  color: #f59e0b;
  font-weight: 500;
}

.sub-actions {
  display: flex;
  gap: 8px;
}

.portal-btn {
  background-color: #4f46e5;
  color: #ffffff;
  border: none;
  border-radius: 6px;
  padding: 8px 20px;
  font-size: 13px;
  font-weight: 500;
  cursor: pointer;
  transition: background-color 0.2s;
}

.portal-btn:hover {
  background-color: #4338ca;
}

.portal-btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.no-subscription {
  display: flex;
  align-items: center;
  gap: 16px;
  color: #6b7280;
  font-size: 14px;
}

.status-badge {
  display: inline-block;
  font-size: 12px;
  font-weight: 500;
  padding: 2px 8px;
  border-radius: 4px;
  text-transform: capitalize;
}

.status-active {
  color: #166534;
  background-color: #dcfce7;
}

.status-trialing {
  color: #1e40af;
  background-color: #dbeafe;
}

.status-past_due {
  color: #9a3412;
  background-color: #ffedd5;
}

.status-canceled, .status-cancelled {
  color: #6b7280;
  background-color: #f3f4f6;
}

.status-paid {
  color: #166534;
  background-color: #dcfce7;
}

.status-open, .status-draft {
  color: #92400e;
  background-color: #fef3c7;
}

.status-void, .status-uncollectible {
  color: #6b7280;
  background-color: #f3f4f6;
}

.invoice-link {
  color: #4f46e5;
  font-size: 12px;
  text-decoration: none;
  font-weight: 500;
}

.invoice-link:hover {
  text-decoration: underline;
}

/* Plan section */
.plan-content {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.plan-summary {
  display: flex;
  align-items: baseline;
  gap: 12px;
}

.plan-name {
  font-size: 20px;
  font-weight: 700;
  color: #111827;
}

.plan-price {
  font-size: 15px;
  font-weight: 500;
  color: #4f46e5;
}

.plan-features {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}

.plan-feature-badge {
  display: inline-block;
  font-size: 12px;
  font-weight: 500;
  padding: 2px 10px;
  border-radius: 4px;
  background-color: #f3f4f6;
  color: #374151;
}

.plan-feature-badge-sm {
  font-size: 11px;
  padding: 1px 7px;
}

.plan-actions {
  margin-top: 4px;
}

.plan-change-btn {
  background-color: #f3f4f6;
  color: #374151;
  font-size: 13px;
  padding: 6px 16px;
}

.plan-change-btn:hover {
  background-color: #e5e7eb;
}

/* Tier picker */
.tier-picker {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
  gap: 12px;
  margin-top: 16px;
  padding-top: 16px;
  border-top: 1px solid #e5e7eb;
}

.tier-option {
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  padding: 16px;
  display: flex;
  flex-direction: column;
  gap: 8px;
  transition: border-color 0.2s;
}

.tier-option:hover {
  border-color: #a5b4fc;
}

.tier-current {
  border-color: #4f46e5;
  background-color: #f5f3ff;
}

.tier-option-header {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
}

.tier-option-name {
  font-size: 15px;
  font-weight: 600;
  color: #111827;
}

.tier-option-price {
  font-size: 13px;
  font-weight: 500;
  color: #4f46e5;
}

.tier-option-products {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
}

.tier-select-btn {
  margin-top: auto;
  background-color: #4f46e5;
  color: #ffffff;
  border: none;
  border-radius: 6px;
  padding: 6px 16px;
  font-size: 13px;
  font-weight: 500;
  cursor: pointer;
  transition: background-color 0.2s;
}

.tier-select-btn:hover {
  background-color: #4338ca;
}

.tier-select-btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.tier-current-label {
  margin-top: auto;
  font-size: 12px;
  font-weight: 500;
  color: #4f46e5;
  text-align: center;
}

.ml-2 {
  margin-left: 8px;
}
</style>
