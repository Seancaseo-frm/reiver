<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-[1200px] mx-auto px-8 py-6">
      <!-- Header -->
      <div class="mb-6">
        <div>
          <h1 class="text-2xl font-semibold text-gray-900">PII Compliance</h1>
          <p class="text-sm text-gray-500 dark:text-gray-400 mt-1">
            PII detected in warehouse data during sync
          </p>
        </div>
      </div>

      <!-- Summary Cards -->
      <div v-if="summary" class="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
        <div class="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-5">
          <div class="text-sm font-medium text-gray-500 dark:text-gray-400">Total Findings</div>
          <div class="mt-1 text-2xl font-semibold text-gray-900 dark:text-gray-100">{{ summary.total_findings }}</div>
          <div class="mt-1 text-sm text-red-600 dark:text-red-400">{{ summary.open_findings }} open</div>
        </div>
        <div class="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-5">
          <div class="text-sm font-medium text-gray-500 dark:text-gray-400">Sources with PII</div>
          <div class="mt-1 text-2xl font-semibold text-gray-900 dark:text-gray-100">{{ summary.sources_with_pii }}</div>
        </div>
        <div class="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-5">
          <div class="text-sm font-medium text-gray-500 dark:text-gray-400">PII Types Detected</div>
          <div class="mt-2 flex flex-wrap gap-1">
            <span
              v-for="pt in summary.by_pii_type"
              :key="pt.pii_type"
              class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium"
              :class="piiTypeBadgeClass(pt.pii_type)"
            >
              {{ formatPiiType(pt.pii_type) }} ({{ pt.count }})
            </span>
            <span v-if="summary.by_pii_type.length === 0" class="text-sm text-gray-400">None</span>
          </div>
        </div>
      </div>

      <!-- Filters -->
      <div class="mb-6 flex gap-4 items-center flex-wrap">
        <select
          v-model="sourceFilter"
          class="px-4 py-2 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500 text-gray-900 dark:text-gray-100"
        >
          <option value="all">All Sources</option>
          <option v-for="src in availableSources" :key="src.source_id" :value="src.source_id">
            {{ src.source_name }}
          </option>
        </select>
        <select
          v-model="statusFilter"
          class="px-4 py-2 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500 text-gray-900 dark:text-gray-100"
        >
          <option value="all">All Statuses</option>
          <option value="open">Open</option>
          <option value="acknowledged">Acknowledged</option>
          <option value="false_positive">False Positive</option>
        </select>
        <select
          v-model="piiTypeFilter"
          class="px-4 py-2 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500 text-gray-900 dark:text-gray-100"
        >
          <option value="all">All PII Types</option>
          <option v-for="pt in allPiiTypes" :key="pt" :value="pt">
            {{ formatPiiType(pt) }}
          </option>
        </select>
      </div>

      <!-- Findings Table -->
      <BaseCard>
        <template #header>
          <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">
            Findings ({{ filteredFindings.length }})
          </h2>
        </template>

        <div v-if="loading" class="text-center py-8 text-gray-500 dark:text-gray-400">
          <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full mx-auto mb-3"></div>
          <p>Loading findings...</p>
        </div>

        <div v-else-if="filteredFindings.length === 0" class="text-center py-12 text-gray-500 dark:text-gray-400">
          <svg class="w-12 h-12 mx-auto mb-4 text-green-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
          <p class="text-lg font-medium mb-2">No PII findings</p>
          <p class="text-sm">No PII has been detected in synced warehouse data</p>
        </div>

        <div v-else class="overflow-x-auto">
          <table class="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
            <thead class="bg-gray-50 dark:bg-gray-800">
              <tr>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Source</th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Table</th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Column</th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">PII Types</th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Rows with PII</th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Status</th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Last Scanned</th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Actions</th>
              </tr>
            </thead>
            <tbody class="bg-white dark:bg-gray-900 divide-y divide-gray-200 dark:divide-gray-700">
              <tr v-for="finding in filteredFindings" :key="finding.id">
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900 dark:text-gray-100">
                  {{ finding.source_name }}
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-700 dark:text-gray-300 font-mono">
                  {{ finding.table_name }}
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-700 dark:text-gray-300 font-mono">
                  {{ finding.column_name }}
                </td>
                <td class="px-6 py-4">
                  <div class="flex flex-wrap gap-1">
                    <span
                      v-for="pt in finding.pii_types"
                      :key="pt"
                      class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium"
                      :class="piiTypeBadgeClass(pt)"
                    >
                      {{ formatPiiType(pt) }}
                    </span>
                  </div>
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-700 dark:text-gray-300">
                  {{ formatNumber(finding.rows_with_pii) }} / {{ formatNumber(finding.total_rows_scanned) }}
                </td>
                <td class="px-6 py-4 whitespace-nowrap">
                  <span
                    class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium"
                    :class="statusBadgeClass(finding.status)"
                  >
                    {{ formatStatus(finding.status) }}
                  </span>
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500 dark:text-gray-400">
                  {{ formatDate(finding.last_scanned_at) }}
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm">
                  <template v-if="finding.status === 'open'">
                    <button
                      @click="updateFinding(finding, 'acknowledged')"
                      class="text-blue-600 hover:text-blue-900 dark:text-blue-400 dark:hover:text-blue-300 mr-3"
                    >
                      Acknowledge
                    </button>
                    <button
                      @click="updateFinding(finding, 'false_positive')"
                      class="text-gray-600 hover:text-gray-900 dark:text-gray-400 dark:hover:text-gray-300"
                    >
                      False Positive
                    </button>
                  </template>
                  <template v-else>
                    <button
                      @click="updateFinding(finding, 'open')"
                      class="text-amber-600 hover:text-amber-900 dark:text-amber-400 dark:hover:text-amber-300"
                    >
                      Reopen
                    </button>
                  </template>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </BaseCard>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue';
import { useRoute } from 'vue-router';
import axios from 'axios';
import AppLayout from '@/Layouts/AppLayout.vue';
import BaseCard from '@/components/BaseCard.vue';
import { useAuth } from '@/composables/useAuth';

const route = useRoute();
const { user } = useAuth();

const projectId = computed(() => route.params.id);
const project = computed(() => ({ id: projectId.value }));

const loading = ref(false);
const findings = ref([]);
const summary = ref(null);

const sourceFilter = ref('all');
const statusFilter = ref('all');
const piiTypeFilter = ref('all');

const allPiiTypes = [
  'ssn', 'credit_card', 'email', 'ipv4', 'aws_access_key',
  'phone_us', 'phone_international', 'passport', 'iban',
  'bank_account', 'routing_number', 'aws_secret_key', 'api_key',
];

const availableSources = computed(() => {
  return summary.value?.by_source || [];
});

const filteredFindings = computed(() => {
  let result = findings.value;
  if (sourceFilter.value !== 'all') {
    result = result.filter(f => f.source_id === sourceFilter.value);
  }
  if (statusFilter.value !== 'all') {
    result = result.filter(f => f.status === statusFilter.value);
  }
  if (piiTypeFilter.value !== 'all') {
    result = result.filter(f => f.pii_types.includes(piiTypeFilter.value));
  }
  return result;
});

const formatPiiType = (type) => {
  const labels = {
    ssn: 'SSN',
    credit_card: 'Credit Card',
    email: 'Email',
    ipv4: 'IPv4',
    aws_access_key: 'AWS Key',
    phone_us: 'Phone (US)',
    phone_international: 'Phone (Intl)',
    passport: 'Passport',
    iban: 'IBAN',
    bank_account: 'Bank Account',
    routing_number: 'Routing #',
    aws_secret_key: 'AWS Secret',
    api_key: 'API Key',
  };
  return labels[type] || type;
};

const piiTypeBadgeClass = (type) => {
  const classes = {
    ssn: 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200',
    credit_card: 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200',
    email: 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200',
    ipv4: 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200',
    aws_access_key: 'bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200',
    aws_secret_key: 'bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200',
    api_key: 'bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200',
    phone_us: 'bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200',
    phone_international: 'bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200',
    passport: 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200',
    iban: 'bg-indigo-100 text-indigo-800 dark:bg-indigo-900 dark:text-indigo-200',
    bank_account: 'bg-indigo-100 text-indigo-800 dark:bg-indigo-900 dark:text-indigo-200',
    routing_number: 'bg-indigo-100 text-indigo-800 dark:bg-indigo-900 dark:text-indigo-200',
  };
  return classes[type] || 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-200';
};

const statusBadgeClass = (status) => {
  const classes = {
    open: 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200',
    acknowledged: 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200',
    false_positive: 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-200',
  };
  return classes[status] || 'bg-gray-100 text-gray-800';
};

const formatStatus = (status) => {
  const labels = { open: 'Open', acknowledged: 'Acknowledged', false_positive: 'False Positive' };
  return labels[status] || status;
};

const formatNumber = (n) => {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M';
  if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K';
  return String(n);
};

const formatDate = (dateStr) => {
  if (!dateStr) return '-';
  const d = new Date(dateStr);
  return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
};

const fetchFindings = async () => {
  loading.value = true;
  try {
    const [findingsRes, summaryRes] = await Promise.all([
      axios.get(`/api/projects/${projectId.value}/warehouse/compliance/findings`),
      axios.get(`/api/projects/${projectId.value}/warehouse/compliance/summary`),
    ]);
    findings.value = findingsRes.data;
    summary.value = summaryRes.data;
  } catch (err) {
    console.error('Failed to load compliance data:', err);
  } finally {
    loading.value = false;
  }
};

const updateFinding = async (finding, newStatus) => {
  try {
    const res = await axios.patch(
      `/api/projects/${projectId.value}/warehouse/compliance/findings/${finding.id}`,
      { status: newStatus },
    );
    const idx = findings.value.findIndex(f => f.id === finding.id);
    if (idx !== -1) {
      findings.value[idx] = res.data;
    }
    // Refresh summary counts
    const summaryRes = await axios.get(`/api/projects/${projectId.value}/warehouse/compliance/summary`);
    summary.value = summaryRes.data;
  } catch (err) {
    console.error('Failed to update finding:', err);
  }
};

onMounted(fetchFindings);
watch(projectId, fetchFindings);
</script>
