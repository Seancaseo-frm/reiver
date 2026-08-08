<template>
  <div>
    <div class="border-b border-gray-200 mb-6">
      <nav class="-mb-px flex gap-x-6" aria-label="Tabs">
        <button
          v-for="tab in tabs"
          :key="tab.id"
          type="button"
          @click="setTab(tab.id)"
          :class="[
            'whitespace-nowrap py-3 px-1 border-b-2 text-sm font-medium transition-colors',
            activeTab === tab.id
              ? 'border-blue-500 text-blue-600'
              : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
          ]"
        >
          {{ tab.label }}
          <span
            v-if="tab.id === 'pending' && pendingCount > 0"
            class="ml-1.5 inline-flex items-center justify-center px-1.5 py-0.5 rounded-full text-xs font-semibold bg-yellow-100 text-yellow-800"
          >{{ pendingCount }}</span>
        </button>
      </nav>
    </div>

    <div v-if="activeTab === 'pending'">
      <div class="flex items-center gap-3 mb-4">
        <button
          @click="triggerBilling"
          :disabled="generating"
          class="inline-flex items-center px-4 py-2 rounded-lg text-sm font-medium bg-indigo-600 text-white hover:bg-indigo-700 transition-colors disabled:opacity-50"
        >
          <svg v-if="generating" class="animate-spin -ml-0.5 mr-2 h-4 w-4 text-white" fill="none" viewBox="0 0 24 24">
            <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
            <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"></path>
          </svg>
          {{ generating ? 'Generating...' : 'Trigger Billing' }}
        </button>
        <span v-if="generateResult" class="text-sm" :class="generateResult.success ? 'text-green-600' : 'text-red-600'">
          {{ generateResult.message }}
        </span>
      </div>

      <div v-if="loading" class="text-gray-400 text-sm py-8">Loading...</div>
      <div v-else-if="pendingCharges.length === 0" class="px-4 py-12 text-center text-gray-500 text-sm">
        No charges waiting for approval.
      </div>
      <div v-else class="overflow-x-auto">
        <table class="min-w-full divide-y divide-gray-200">
          <thead>
            <tr>
              <th v-for="col in pendingColumns" :key="col.key"
                @click="col.sortable && toggleSort(col.key)"
                :class="[
                  'px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider',
                  col.sortable ? 'cursor-pointer select-none hover:text-gray-700' : ''
                ]"
              >
                {{ col.label }}
                <span v-if="sortCol === col.key" class="ml-1">{{ sortDir === 'asc' ? '&#9650;' : '&#9660;' }}</span>
              </th>
            </tr>
          </thead>
          <tbody class="divide-y divide-gray-200">
            <template v-for="c in sortedPending" :key="c.id">
              <tr class="hover:bg-gray-50 cursor-pointer" @click="toggleExpand(c.id)">
                <td class="px-4 py-3 text-sm text-gray-900">{{ c.organization_name || c.organization_id }}</td>
                <td class="px-4 py-3 text-sm">{{ chargeTypeLabel(c.charge_type) }}</td>
                <td class="px-4 py-3 text-sm text-gray-500">{{ formatPeriod(c.billing_period_start) }}</td>
                <td class="px-4 py-3 text-sm font-mono text-gray-900">${{ parseFloat(c.amount_usd).toFixed(2) }}</td>
                <td class="px-4 py-3 text-sm text-gray-500">{{ timeAgo(c.created_at) }}</td>
                <td class="px-4 py-3 text-sm flex gap-2" @click.stop>
                  <button
                    @click="approveCharge(c.id)"
                    :disabled="c._acting"
                    class="inline-flex items-center px-3 py-1 rounded text-xs font-medium bg-green-600 text-white hover:bg-green-700 transition-colors disabled:opacity-50"
                  >{{ c._acting ? '...' : 'Approve' }}</button>
                  <button
                    @click="rejectCharge(c.id)"
                    :disabled="c._acting"
                    class="inline-flex items-center px-3 py-1 rounded text-xs font-medium bg-red-600 text-white hover:bg-red-700 transition-colors disabled:opacity-50"
                  >Reject</button>
                </td>
              </tr>
              <tr v-if="expandedRows.has(c.id)" class="bg-gray-50">
                <td :colspan="pendingColumns.length" class="px-6 py-3">
                  <line-items-detail :line-items="c.line_items" />
                </td>
              </tr>
            </template>
          </tbody>
        </table>
      </div>
    </div>

    <div v-if="activeTab === 'history'">
      <div v-if="loading" class="text-gray-400 text-sm py-8">Loading...</div>
      <div v-else-if="historyCharges.length === 0" class="px-4 py-12 text-center text-gray-500 text-sm">
        No charge history yet.
      </div>
      <div v-else class="overflow-x-auto">
        <table class="min-w-full divide-y divide-gray-200">
          <thead>
            <tr>
              <th v-for="col in historyColumns" :key="col.key"
                @click="col.sortable && toggleSort(col.key)"
                :class="[
                  'px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider',
                  col.sortable ? 'cursor-pointer select-none hover:text-gray-700' : ''
                ]"
              >
                {{ col.label }}
                <span v-if="sortCol === col.key" class="ml-1">{{ sortDir === 'asc' ? '&#9650;' : '&#9660;' }}</span>
              </th>
            </tr>
          </thead>
          <tbody class="divide-y divide-gray-200">
            <template v-for="c in sortedHistory" :key="c.id">
              <tr class="hover:bg-gray-50 cursor-pointer" @click="toggleExpand(c.id)">
                <td class="px-4 py-3 text-sm text-gray-900">{{ c.organization_name || c.organization_id }}</td>
                <td class="px-4 py-3 text-sm">{{ chargeTypeLabel(c.charge_type) }}</td>
                <td class="px-4 py-3 text-sm text-gray-500">{{ formatPeriod(c.billing_period_start) }}</td>
                <td class="px-4 py-3 text-sm font-mono text-gray-900">${{ parseFloat(c.amount_usd).toFixed(2) }}</td>
                <td class="px-4 py-3 text-sm">
                  <span :class="statusBadge(c.status)">{{ c.status }}</span>
                </td>
                <td class="px-4 py-3 text-sm text-gray-500 max-w-md break-words whitespace-normal">
                  {{ c.error_message || '-' }}
                </td>
                <td class="px-4 py-3 text-sm text-gray-500">{{ timeAgo(c.created_at) }}</td>
                <td class="px-4 py-3 text-sm" @click.stop>
                  <button
                    v-if="c.status === 'payment_failed'"
                    @click="retryCharge(c.id)"
                    :disabled="c._acting"
                    class="inline-flex items-center px-3 py-1 rounded text-xs font-medium bg-orange-600 text-white hover:bg-orange-700 transition-colors disabled:opacity-50"
                  >{{ c._acting ? '...' : 'Retry' }}</button>
                  <span v-else class="text-gray-400">-</span>
                </td>
              </tr>
              <tr v-if="expandedRows.has(c.id)" class="bg-gray-50">
                <td :colspan="historyColumns.length" class="px-6 py-3">
                  <line-items-detail :line-items="c.line_items" />
                </td>
              </tr>
            </template>
          </tbody>
        </table>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted, h } from 'vue';
import axios from 'axios';

const LineItemsDetail = {
  props: { lineItems: Object },
  setup(props) {
    return () => {
      const li = props.lineItems;
      if (!li) return h('span', { class: 'text-gray-400 text-xs' }, 'No breakdown available');
      const items = [];
      if (li.watch_usage) {
        const amt = parseFloat(li.watch_usage.amount_usd || 0).toFixed(2);
        items.push(h('div', { class: 'flex justify-between' }, [
          h('span', {}, 'Watch Usage'),
          h('span', { class: 'font-mono' }, `$${amt}`),
        ]));
      }
      if (li.flow_byok_fees) {
        const amt = parseFloat(li.flow_byok_fees.amount_usd || 0).toFixed(2);
        items.push(h('div', { class: 'flex justify-between' }, [
          h('span', {}, `BYOK Fees (${li.flow_byok_fees.fee_count || 0} transactions)`),
          h('span', { class: 'font-mono' }, `$${amt}`),
        ]));
      }
      if (items.length === 0) {
        return h('span', { class: 'text-gray-400 text-xs' }, 'No breakdown available');
      }
      return h('div', { class: 'space-y-1 text-sm max-w-sm' }, items);
    };
  },
};

const tabs = [
  { id: 'pending', label: 'Pending Approval' },
  { id: 'history', label: 'History' },
];

const activeTab = ref('pending');

function setTab(id) {
  activeTab.value = id;
}

const pendingColumns = [
  { key: 'organization_name', label: 'Organization', sortable: true },
  { key: 'charge_type', label: 'Type', sortable: true },
  { key: 'billing_period_start', label: 'Period', sortable: true },
  { key: 'amount_usd', label: 'Amount', sortable: true },
  { key: 'created_at', label: 'Created', sortable: true },
  { key: 'action', label: 'Action', sortable: false },
];

const historyColumns = [
  { key: 'organization_name', label: 'Organization', sortable: true },
  { key: 'charge_type', label: 'Type', sortable: true },
  { key: 'billing_period_start', label: 'Period', sortable: true },
  { key: 'amount_usd', label: 'Amount', sortable: true },
  { key: 'status', label: 'Status', sortable: true },
  { key: 'error_message', label: 'Error', sortable: false },
  { key: 'created_at', label: 'Created', sortable: true },
  { key: 'action', label: 'Action', sortable: false },
];

const loading = ref(false);
const allCharges = ref([]);
const generating = ref(false);
const generateResult = ref(null);
const expandedRows = reactive(new Set());

function toggleExpand(id) {
  if (expandedRows.has(id)) expandedRows.delete(id);
  else expandedRows.add(id);
}

const sortCol = ref('created_at');
const sortDir = ref('desc');

function toggleSort(col) {
  if (sortCol.value === col) {
    sortDir.value = sortDir.value === 'asc' ? 'desc' : 'asc';
  } else {
    sortCol.value = col;
    sortDir.value = 'asc';
  }
}

function sortRows(rows) {
  const arr = [...rows];
  const col = sortCol.value;
  const dir = sortDir.value === 'asc' ? 1 : -1;
  arr.sort((a, b) => {
    let va = a[col], vb = b[col];
    if (col === 'amount_usd') { va = parseFloat(va) || 0; vb = parseFloat(vb) || 0; }
    if (va == null) return dir;
    if (vb == null) return -dir;
    if (va < vb) return -dir;
    if (va > vb) return dir;
    return 0;
  });
  return arr;
}

const pendingCharges = computed(() => allCharges.value.filter(c => c.status === 'pending'));
const historyCharges = computed(() => allCharges.value.filter(c => c.status !== 'pending'));
const pendingCount = computed(() => pendingCharges.value.length);

const sortedPending = computed(() => sortRows(pendingCharges.value));
const sortedHistory = computed(() => sortRows(historyCharges.value));

async function fetchCharges() {
  loading.value = true;
  try {
    const { data } = await axios.get('/api/admin/charges');
    allCharges.value = data.map(c => ({ ...c, _acting: false }));
  } catch (_) {
    allCharges.value = [];
  } finally {
    loading.value = false;
  }
}

async function triggerBilling() {
  generating.value = true;
  generateResult.value = null;
  try {
    const { data } = await axios.post('/api/admin/charges/generate');
    generateResult.value = {
      success: true,
      message: `Charges generated for ${data.period_start} to ${data.period_end}`,
    };
    await fetchCharges();
  } catch (e) {
    generateResult.value = {
      success: false,
      message: e.response?.data?.error || 'Failed to generate charges',
    };
  } finally {
    generating.value = false;
  }
}

async function approveCharge(id) {
  const c = allCharges.value.find(x => x.id === id);
  if (c) c._acting = true;
  try {
    await axios.post(`/api/admin/charges/${id}/approve`);
    await fetchCharges();
  } catch (e) {
    alert(e.response?.data?.error || 'Failed to approve charge');
    if (c) c._acting = false;
  }
}

async function rejectCharge(id) {
  const reason = prompt('Rejection reason (optional):');
  if (reason === null) return;
  const c = allCharges.value.find(x => x.id === id);
  if (c) c._acting = true;
  try {
    await axios.post(`/api/admin/charges/${id}/reject`, { reason: reason || undefined });
    await fetchCharges();
  } catch (e) {
    alert(e.response?.data?.error || 'Failed to reject charge');
    if (c) c._acting = false;
  }
}

async function retryCharge(id) {
  const c = allCharges.value.find(x => x.id === id);
  if (c) c._acting = true;
  try {
    await axios.post(`/api/admin/charges/${id}/retry`);
    await fetchCharges();
  } catch (e) {
    alert(e.response?.data?.error || 'Failed to retry charge');
    if (c) c._acting = false;
  }
}

const statusBadge = (s) => {
  const map = {
    pending: 'bg-yellow-100 text-yellow-800',
    approved: 'bg-blue-100 text-blue-800',
    paid: 'bg-green-100 text-green-800',
    rejected: 'bg-gray-100 text-gray-600',
    payment_failed: 'bg-red-100 text-red-800',
  };
  return `inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${map[s] || 'bg-gray-100 text-gray-600'}`;
};

const chargeTypeLabel = (t) =>
  t === 'platform_usage' ? 'Platform Usage' :
  t === 'watch_usage' ? 'Watch Usage' :
  t === 'flow_byok_fees' ? 'Flow BYOK Fees' : t;

function formatPeriod(start) {
  if (!start) return '-';
  const d = new Date(start + 'T00:00:00');
  return d.toLocaleDateString('en-US', { month: 'short', year: 'numeric' });
}

function timeAgo(iso) {
  if (!iso) return '-';
  const seconds = Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}

onMounted(() => {
  fetchCharges();
});
</script>
