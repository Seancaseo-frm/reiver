<template>
  <div>
    <div v-if="error" class="mb-4 rounded-lg bg-red-50 border border-red-200 px-4 py-3 text-sm text-red-700 flex items-center justify-between">
      <span>{{ error }}</span>
      <button @click="error = ''" class="ml-4 text-red-500 hover:text-red-700">&times;</button>
    </div>

    <div v-if="success" class="mb-4 rounded-lg bg-green-50 border border-green-200 px-4 py-3 text-sm text-green-700 flex items-center justify-between">
      <span>{{ success }}</span>
      <button @click="success = ''" class="ml-4 text-green-500 hover:text-green-700">&times;</button>
    </div>

    <!-- ── Org List View ─────────────────────────────────────────── -->
    <template v-if="!selectedOrg">
      <div v-if="orgsLoading" class="text-gray-400 text-sm py-8 text-center">Loading organizations...</div>
      <template v-else>
        <div class="mb-4">
          <input
            v-model="orgFilter"
            type="text"
            placeholder="Filter by name, domain, or email..."
            class="w-80 border-gray-300 rounded-md shadow-sm text-sm"
          />
        </div>

        <div class="overflow-x-auto">
          <table class="min-w-full divide-y divide-gray-200 bg-white border border-gray-200 rounded-lg">
            <thead class="bg-gray-50">
              <tr>
                <th
                  v-for="col in columns"
                  :key="col.key"
                  @click="col.sortable && toggleSort(col.key)"
                  :class="[
                    'px-4 py-3 text-xs font-medium text-gray-500 uppercase tracking-wider',
                    col.align === 'center' ? 'text-center' : col.align === 'right' ? 'text-right' : 'text-left',
                    col.sortable ? 'cursor-pointer select-none hover:text-gray-700' : ''
                  ]"
                >
                  {{ col.label }}
                  <span v-if="sortCol === col.key" class="ml-1" v-html="sortDir === 'asc' ? '&#9650;' : '&#9660;'" />
                </th>
              </tr>
            </thead>
            <tbody class="divide-y divide-gray-200">
              <tr
                v-for="org in sortedOrgs"
                :key="org.id"
                class="hover:bg-gray-50 cursor-pointer"
                @click="openOrg(org)"
              >
                <td class="px-4 py-3 text-sm text-gray-900">
                  <div class="font-medium">{{ orgDisplayName(org) }}</div>
                  <div class="text-xs text-gray-400 font-mono">{{ org.id }}</div>
                </td>
                <td class="px-4 py-3 text-sm text-gray-500">{{ org.domain || '—' }}</td>
                <td class="px-4 py-3 text-sm">
                  <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium"
                    :class="tierBadgeClass(org.tier_name)">
                    {{ org.tier_display_name }}
                  </span>
                </td>
                <td class="px-4 py-3 text-center text-sm text-gray-500">{{ org.member_count }}</td>
                <td class="px-4 py-3 text-center text-sm">
                  <span v-if="org.has_overrides" class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-yellow-100 text-yellow-800">Yes</span>
                  <span v-else class="text-gray-400">—</span>
                </td>
              </tr>
              <tr v-if="sortedOrgs.length === 0">
                <td :colspan="columns.length" class="px-4 py-8 text-center text-sm text-gray-400">
                  {{ orgFilter ? 'No organizations match your filter.' : 'No organizations found.' }}
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </template>
    </template>

    <!-- ── Org Detail View ───────────────────────────────────────── -->
    <template v-else>
      <button
        @click="closeOrg"
        class="mb-4 inline-flex items-center gap-1.5 text-sm text-gray-500 hover:text-gray-700"
      >
        <svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M15 19l-7-7 7-7" /></svg>
        Back to organizations
      </button>

      <div class="mb-6">
        <div class="flex items-center gap-3 mb-1">
          <h2 class="text-xl font-bold text-gray-900">{{ orgDisplayName(selectedOrg) }}</h2>
          <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium"
            :class="tierBadgeClass(selectedOrg.tier_name)">
            {{ selectedOrg.tier_display_name }}
          </span>
        </div>
        <p class="text-xs text-gray-400 font-mono">{{ selectedOrg.id }}</p>
        <p v-if="selectedOrg.domain" class="text-sm text-gray-500 mt-0.5">{{ selectedOrg.domain }}</p>
      </div>

      <!-- Members Section -->
      <div class="mb-8">
        <h3 class="text-sm font-semibold text-gray-900 mb-3">Members ({{ members.length }})</h3>
        <div v-if="membersLoading" class="text-gray-400 text-sm py-4">Loading members...</div>
        <div v-else-if="members.length === 0" class="text-gray-400 text-sm py-4">No members found.</div>
        <div v-else class="overflow-x-auto">
          <table class="min-w-full divide-y divide-gray-200 bg-white border border-gray-200 rounded-lg">
            <thead class="bg-gray-50">
              <tr>
                <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Email</th>
                <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Role</th>
                <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Status</th>
                <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Platform</th>
                <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Joined</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-gray-200">
              <tr v-for="m in members" :key="m.user_id">
                <td class="px-4 py-3 text-sm font-medium text-gray-900">{{ m.email }}</td>
                <td class="px-4 py-3 text-sm">
                  <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium"
                    :class="roleBadgeClass(m.role)">
                    {{ m.role }}
                  </span>
                </td>
                <td class="px-4 py-3 text-sm">
                  <span class="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium"
                    :class="memberStatusClass(m.membership_status)">
                    {{ m.membership_status }}
                  </span>
                </td>
                <td class="px-4 py-3 text-sm">
                  <span v-if="m.is_platform_admin" class="text-indigo-700 font-medium">Admin</span>
                  <span v-else-if="!m.is_approved" class="text-amber-600">Pending approval</span>
                  <span v-else class="text-gray-400">—</span>
                </td>
                <td class="px-4 py-3 text-sm text-gray-500">{{ formatDate(m.joined_at) }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <!-- Tier Assignment -->
      <div class="mb-8 space-y-3">
        <h3 class="text-sm font-semibold text-gray-900">Tier Assignment</h3>
        <div class="flex items-center gap-3">
          <select
            v-model="editingOrgTierId"
            class="flex-1 max-w-sm border-gray-300 rounded-md shadow-sm text-sm"
          >
            <option v-for="t in tiers" :key="t.id" :value="t.id">{{ t.display_name }} ({{ t.name }})</option>
          </select>
          <button
            @click="assignTierToOrg"
            :disabled="editingOrgTierId === selectedOrg.tier_definition_id"
            class="px-3 py-1.5 rounded-lg bg-blue-600 text-white text-sm hover:bg-blue-700 disabled:opacity-50"
          >
            Save
          </button>
        </div>
      </div>

      <!-- Overrides -->
      <div class="mb-8 space-y-4">
        <h3 class="text-sm font-semibold text-gray-900">Per-Org Config Overrides</h3>
        <p class="text-xs text-gray-500">Overrides are a sparse JSON object matching the tier config shape. Only set fields you want to override; everything else inherits from the base tier.</p>

        <div v-if="editingOrgData?.overrides" class="text-sm text-gray-500">
          <span>Existing reason: {{ editingOrgData.overrides.reason || '(none)' }}</span>
        </div>

        <div>
          <label class="block text-xs font-bold text-gray-500 uppercase tracking-wide mb-2">Config Overrides (JSON)</label>
          <textarea
            v-model="overrideForm.config_overrides_json"
            rows="10"
            class="w-full border-gray-300 rounded-md shadow-sm text-sm font-mono"
            placeholder='{ "platform": { "sso": true }, "gateway": { "fee_percent": 0.01 } }'
          ></textarea>
          <p class="mt-1 text-xs text-gray-400">Only include fields you want to override. Use {} for no overrides.</p>
        </div>

        <div>
          <label class="block text-xs font-medium text-gray-500 mb-1">Reason</label>
          <input v-model="overrideForm.reason" class="w-full max-w-lg border-gray-300 rounded-md shadow-sm text-sm" placeholder="e.g. Enterprise contract, special deal" />
        </div>
        <div class="flex gap-3">
          <button
            @click="saveOverrides"
            class="px-4 py-2 rounded-lg bg-blue-600 text-white text-sm font-medium hover:bg-blue-700"
          >
            Save Overrides
          </button>
          <button
            v-if="editingOrgData?.overrides"
            @click="removeOverrides"
            class="px-4 py-2 rounded-lg bg-red-600 text-white text-sm font-medium hover:bg-red-700"
          >
            Remove Overrides
          </button>
        </div>
      </div>

      <!-- Resolved Entitlements -->
      <div v-if="editingOrgData?.resolved" class="mb-8">
        <h3 class="text-sm font-semibold text-gray-900 mb-2">Resolved Entitlements</h3>
        <div class="bg-gray-50 rounded-lg p-3 text-xs font-mono text-gray-700 overflow-auto max-h-48">
          <pre>{{ JSON.stringify(editingOrgData.resolved, null, 2) }}</pre>
        </div>
      </div>
    </template>
  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted } from 'vue';
import axios from 'axios';

const props = defineProps({
  schema: { type: Object, required: true },
});

const error = ref('');
const success = ref('');
const tiers = ref([]);
const organizations = ref([]);
const orgsLoading = ref(false);
const orgFilter = ref('');

const selectedOrg = ref(null);
const members = ref([]);
const membersLoading = ref(false);

const columns = [
  { key: 'display_name', label: 'Organization', sortable: true, align: 'left' },
  { key: 'domain', label: 'Domain', sortable: true, align: 'left' },
  { key: 'tier_display_name', label: 'Tier', sortable: true, align: 'left' },
  { key: 'member_count', label: 'Members', sortable: true, align: 'center' },
  { key: 'has_overrides', label: 'Overrides', sortable: true, align: 'center' },
];

const sortCol = ref('display_name');
const sortDir = ref('asc');

function toggleSort(col) {
  if (sortCol.value === col) {
    sortDir.value = sortDir.value === 'asc' ? 'desc' : 'asc';
  } else {
    sortCol.value = col;
    sortDir.value = 'asc';
  }
}

function orgDisplayName(org) {
  if (org.domain) return org.domain;
  if (org.owner_email) return `${org.owner_email}'s workspace`;
  return org.name;
}

const filteredOrgs = computed(() => {
  let list = organizations.value;
  if (orgFilter.value.trim()) {
    const q = orgFilter.value.toLowerCase();
    list = list.filter(o =>
      orgDisplayName(o).toLowerCase().includes(q) ||
      o.name.toLowerCase().includes(q) ||
      (o.domain && o.domain.toLowerCase().includes(q)) ||
      (o.owner_email && o.owner_email.toLowerCase().includes(q)) ||
      o.tier_name.toLowerCase().includes(q) ||
      o.id.toLowerCase().includes(q)
    );
  }
  return list;
});

const sortedOrgs = computed(() => {
  const arr = [...filteredOrgs.value];
  const col = sortCol.value;
  const dir = sortDir.value === 'asc' ? 1 : -1;
  arr.sort((a, b) => {
    let va, vb;
    if (col === 'display_name') {
      va = orgDisplayName(a).toLowerCase();
      vb = orgDisplayName(b).toLowerCase();
    } else if (col === 'member_count') {
      va = a.member_count || 0;
      vb = b.member_count || 0;
    } else if (col === 'has_overrides') {
      va = a.has_overrides ? 1 : 0;
      vb = b.has_overrides ? 1 : 0;
    } else {
      va = (a[col] || '').toString().toLowerCase();
      vb = (b[col] || '').toString().toLowerCase();
    }
    if (va < vb) return -dir;
    if (va > vb) return dir;
    return 0;
  });
  return arr;
});

const editingOrgTierId = ref('');
const editingOrgData = ref(null);
const overrideForm = reactive({
  config_overrides_json: '{}',
  reason: '',
});

const baseTier = computed(() => {
  if (!selectedOrg.value) return null;
  return tiers.value.find(t => t.id === editingOrgTierId.value) || null;
});

function tierBadgeClass(tierName) {
  switch (tierName) {
    case 'free': return 'bg-gray-100 text-gray-800';
    case 'starter': return 'bg-blue-100 text-blue-800';
    case 'scale': return 'bg-purple-100 text-purple-800';
    default: return 'bg-indigo-100 text-indigo-800';
  }
}

function roleBadgeClass(role) {
  switch (role) {
    case 'owner': return 'bg-indigo-100 text-indigo-800';
    case 'admin': return 'bg-blue-100 text-blue-800';
    case 'member': return 'bg-gray-100 text-gray-700';
    case 'viewer': return 'bg-gray-50 text-gray-500';
    default: return 'bg-gray-100 text-gray-700';
  }
}

function memberStatusClass(status) {
  switch (status) {
    case 'active': return 'bg-green-100 text-green-800';
    case 'invited': return 'bg-yellow-100 text-yellow-800';
    case 'suspended': return 'bg-red-100 text-red-800';
    default: return 'bg-gray-100 text-gray-600';
  }
}

function formatLabel(key) {
  return key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
}

function formatDate(dateStr) {
  if (!dateStr) return '—';
  return new Date(dateStr).toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  });
}

function showSuccess(msg) {
  success.value = msg;
  setTimeout(() => success.value = '', 3000);
}

async function loadTiers() {
  try {
    const { data } = await axios.get('/api/admin/tiers');
    tiers.value = data;
  } catch (e) {
    error.value = e.response?.data?.message || 'Failed to load tiers';
  }
}

async function loadOrganizations() {
  orgsLoading.value = true;
  try {
    const { data } = await axios.get('/api/admin/tiers/organizations');
    organizations.value = data;
  } catch (e) {
    error.value = e.response?.data?.message || 'Failed to load organizations';
  } finally {
    orgsLoading.value = false;
  }
}

async function openOrg(org) {
  selectedOrg.value = org;
  editingOrgTierId.value = org.tier_definition_id;
  membersLoading.value = true;
  members.value = [];
  editingOrgData.value = null;

  try {
    const [entResp, memberResp] = await Promise.all([
      axios.get(`/api/admin/tiers/org/${org.id}`),
      axios.get(`/api/admin/organizations/${org.id}/members`),
    ]);
    editingOrgData.value = entResp.data;
    members.value = memberResp.data;

    if (entResp.data.overrides) {
      overrideForm.config_overrides_json = JSON.stringify(entResp.data.overrides.config_overrides || {}, null, 2);
      overrideForm.reason = entResp.data.overrides.reason || '';
    } else {
      overrideForm.config_overrides_json = '{}';
      overrideForm.reason = '';
    }
  } catch (e) {
    error.value = e.response?.data?.message || 'Failed to load organization details';
    selectedOrg.value = null;
  } finally {
    membersLoading.value = false;
  }
}

function closeOrg() {
  selectedOrg.value = null;
  members.value = [];
  editingOrgData.value = null;
}

async function assignTierToOrg() {
  if (!selectedOrg.value || !editingOrgTierId.value) return;
  try {
    await axios.put(`/api/admin/tiers/org/${selectedOrg.value.id}`, {
      tier_definition_id: editingOrgTierId.value,
    });
    showSuccess('Tier assigned');
    await loadOrganizations();
    const updated = organizations.value.find(o => o.id === selectedOrg.value.id);
    if (updated) {
      selectedOrg.value = updated;
      const { data } = await axios.get(`/api/admin/tiers/org/${updated.id}`);
      editingOrgData.value = data;
    }
  } catch (e) {
    error.value = e.response?.data?.message || 'Failed to assign tier';
  }
}

async function saveOverrides() {
  if (!selectedOrg.value) return;
  let configOverrides;
  try {
    configOverrides = JSON.parse(overrideForm.config_overrides_json);
  } catch {
    error.value = 'Invalid JSON in config overrides';
    return;
  }
  try {
    const payload = {
      overrides: {
        config_overrides: configOverrides,
        reason: overrideForm.reason || null,
      },
    };
    await axios.put(`/api/admin/tiers/org/${selectedOrg.value.id}`, payload);
    showSuccess('Overrides saved');
    await loadOrganizations();
    const updated = organizations.value.find(o => o.id === selectedOrg.value.id);
    if (updated) {
      selectedOrg.value = updated;
      const { data } = await axios.get(`/api/admin/tiers/org/${updated.id}`);
      editingOrgData.value = data;
      if (data.overrides) {
        overrideForm.config_overrides_json = JSON.stringify(data.overrides.config_overrides || {}, null, 2);
        overrideForm.reason = data.overrides.reason || '';
      }
    }
  } catch (e) {
    error.value = e.response?.data?.message || 'Failed to save overrides';
  }
}

async function removeOverrides() {
  if (!selectedOrg.value) return;
  try {
    await axios.delete(`/api/admin/tiers/org/${selectedOrg.value.id}/overrides`);
    showSuccess('Overrides removed');
    overrideForm.config_overrides_json = '{}';
    overrideForm.reason = '';
    await loadOrganizations();
    const updated = organizations.value.find(o => o.id === selectedOrg.value.id);
    if (updated) {
      selectedOrg.value = updated;
      const { data } = await axios.get(`/api/admin/tiers/org/${updated.id}`);
      editingOrgData.value = data;
    }
  } catch (e) {
    error.value = e.response?.data?.message || 'Failed to remove overrides';
  }
}

onMounted(async () => {
  await loadTiers();
  await loadOrganizations();
});
</script>
