<template>
  <AppLayout :user="user" :current-project="null">
    <div class="max-w-[1400px] mx-auto px-8 py-6">
      <div class="mb-6">
        <h1 class="text-2xl font-semibold text-gray-900">Audit Log</h1>
        <p class="text-sm text-gray-500 mt-1">
          Platform-wide record of all write operations across UI, API, agents, and MCP
        </p>
      </div>

      <!-- Filters -->
      <div class="flex flex-wrap items-center gap-3 mb-4">
        <select v-model="filters.timeRange" @change="onFilterChange" class="filter-select">
          <option value="1h">Last 1 hour</option>
          <option value="6h">Last 6 hours</option>
          <option value="24h">Last 24 hours</option>
          <option value="7d">Last 7 days</option>
          <option value="30d">Last 30 days</option>
        </select>

        <select v-model="filters.resourceType" @change="onFilterChange" class="filter-select">
          <option value="">All Resources</option>
          <option v-for="rt in resourceTypes" :key="rt.value" :value="rt.value">{{ rt.label }}</option>
        </select>

        <select v-model="filters.callerType" @change="onFilterChange" class="filter-select">
          <option value="">All Actors</option>
          <option value="user">User</option>
          <option value="agent">Agent</option>
          <option value="system">System</option>
        </select>

        <select v-model="filters.userId" @change="onFilterChange" class="filter-select min-w-[200px]">
          <option value="">All users</option>
          <option v-for="m in members" :key="m.user_id" :value="m.user_id">{{ m.email }}</option>
        </select>

        <input
          v-model="filters.tokenPrefix"
          type="text"
          placeholder="Agent token (e.g. dh_... from Agents → Tokens)"
          class="filter-input w-[min(100%,280px)]"
          @keyup.enter="onFilterChange"
        />

        <input
          v-model="filters.callerKeyLabel"
          type="text"
          placeholder="Token label (optional)"
          class="filter-input w-[min(100%,180px)]"
          @keyup.enter="onFilterChange"
        />

        <select v-model="filters.service" @change="onFilterChange" class="filter-select">
          <option value="">All Services</option>
          <option value="website">Website</option>
          <option value="flow">Flow</option>
          <option value="watch">Watch</option>
        </select>
      </div>

      <!-- Table -->
      <BaseCard :padded="false">
        <div v-if="loading" class="text-center py-12 text-gray-500">Loading audit events...</div>
        <div v-else-if="events.length === 0" class="text-center py-12 text-gray-500">No events found.</div>
        <div v-else class="overflow-x-auto">
          <table class="min-w-full divide-y divide-gray-200">
            <thead class="bg-gray-50">
              <tr>
                <th class="th-cell">Timestamp</th>
                <th class="th-cell">Event</th>
                <th class="th-cell">Actor</th>
                <th class="th-cell">Resource</th>
                <th class="th-cell">Details</th>
                <th class="th-cell text-center">Status</th>
              </tr>
            </thead>
            <tbody class="bg-white divide-y divide-gray-200">
              <template v-for="event in events" :key="event.event_id">
                <tr class="hover:bg-gray-50 cursor-pointer" @click="toggleRow(event.event_id)">
                  <td class="td-cell text-gray-500 whitespace-nowrap text-xs">{{ formatTimestamp(event.timestamp) }}</td>
                  <td class="td-cell">
                    <span :class="eventTypeBadgeClass(event.event_type)">{{ formatEventType(event.event_type) }}</span>
                  </td>
                  <td class="td-cell">
                    <div class="flex items-center gap-1.5 flex-wrap">
                      <span :class="callerBadgeClass(event.caller_type)">{{ formatCallerTypeLabel(event.caller_type) }}</span>
                      <span
                        v-if="actorSecondaryText(event)"
                        class="text-xs text-gray-600 max-w-[260px] truncate"
                        :class="{ 'font-mono': actorSecondaryMonospace(event) }"
                        :title="actorSecondaryFullTitle(event)"
                      >{{ actorSecondaryText(event) }}</span>
                    </div>
                  </td>
                  <td class="td-cell">
                    <div class="flex items-center gap-1.5">
                      <span v-if="event.resource_type" class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-gray-100 text-gray-700">{{ event.resource_type }}</span>
                      <span v-if="event.resource_id" class="text-xs text-gray-400 font-mono truncate max-w-[120px]" :title="event.resource_id">{{ shortId(event.resource_id) }}</span>
                      <span v-if="!event.resource_type && !event.resource_id" class="text-xs text-gray-400">&mdash;</span>
                    </div>
                  </td>
                  <td class="td-cell text-xs text-gray-600 max-w-[250px] truncate" :title="event.details">
                    <span v-if="event.origin_type && event.origin_type !== 'user'" :class="originBadgeClass(event.origin_type)" class="mr-1.5">{{ formatOriginType(event.origin_type) }}</span>
                    <span>{{ detailsSummary(event) }}</span>
                  </td>
                  <td class="td-cell text-center">
                    <span v-if="event.success" class="inline-flex items-center justify-center w-5 h-5 rounded-full bg-green-100 text-green-600">
                      <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M5 13l4 4L19 7"/></svg>
                    </span>
                    <span v-else class="inline-flex items-center justify-center w-5 h-5 rounded-full bg-red-100 text-red-600">
                      <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M6 18L18 6M6 6l12 12"/></svg>
                    </span>
                  </td>
                </tr>
                <tr v-if="expandedRow === event.event_id">
                  <td colspan="6" class="p-0">
                    <div class="bg-gray-50 border-t border-gray-200 px-6 py-4">
                      <!-- Origin / Causation -->
                      <div v-if="event.origin_type && event.origin_type !== 'user'" class="mb-3 flex items-center gap-2 text-xs">
                        <span class="font-medium text-gray-500">Triggered by:</span>
                        <span :class="originBadgeClass(event.origin_type)">{{ formatOriginType(event.origin_type) }}</span>
                        <span v-if="event.origin_ref" class="text-gray-500 font-mono truncate max-w-[200px]" :title="event.origin_ref">{{ shortId(event.origin_ref) }}</span>
                        <span v-if="event.origin_reason" class="text-gray-500 italic">&mdash; {{ event.origin_reason }}</span>
                      </div>

                      <dl class="grid grid-cols-[160px_1fr] gap-x-4 gap-y-2">
                        <template v-for="(value, label) in detailFields(event)" :key="label">
                          <dt class="text-xs font-medium text-gray-500">{{ label }}</dt>
                          <dd class="text-xs text-gray-800 break-all font-mono">{{ value }}</dd>
                        </template>
                      </dl>

                      <!-- Before / After diff -->
                      <div v-if="hasBeforeAfter(event)" class="mt-3">
                        <div class="text-xs font-medium text-gray-500 mb-1">Changes</div>
                        <div class="grid grid-cols-2 gap-3">
                          <div>
                            <div class="text-xs font-medium text-red-600 mb-1">Before</div>
                            <pre class="text-xs bg-red-50 p-3 rounded border border-red-200 max-h-[200px] overflow-auto whitespace-pre-wrap">{{ formatJson(JSON.stringify(parseDetails(event.details).before)) }}</pre>
                          </div>
                          <div>
                            <div class="text-xs font-medium text-green-600 mb-1">After</div>
                            <pre class="text-xs bg-green-50 p-3 rounded border border-green-200 max-h-[200px] overflow-auto whitespace-pre-wrap">{{ formatJson(JSON.stringify(parseDetails(event.details).after)) }}</pre>
                          </div>
                        </div>
                      </div>

                      <!-- Created details -->
                      <div v-else-if="hasCreated(event)" class="mt-3">
                        <div class="text-xs font-medium text-green-600 mb-1">Created</div>
                        <pre class="text-xs bg-green-50 p-3 rounded border border-green-200 max-h-[200px] overflow-auto whitespace-pre-wrap">{{ formatJson(JSON.stringify(parseDetails(event.details).created)) }}</pre>
                      </div>

                      <!-- Deleted details -->
                      <div v-else-if="hasDeleted(event)" class="mt-3">
                        <div class="text-xs font-medium text-red-600 mb-1">Deleted</div>
                        <pre class="text-xs bg-red-50 p-3 rounded border border-red-200 max-h-[200px] overflow-auto whitespace-pre-wrap">{{ formatJson(JSON.stringify(parseDetails(event.details).deleted)) }}</pre>
                      </div>

                      <!-- Raw details fallback -->
                      <div v-else-if="event.details && event.details !== '{}'" class="mt-3">
                        <div class="text-xs font-medium text-gray-500 mb-1">Details</div>
                        <pre class="text-xs bg-white p-3 rounded border border-gray-200 max-h-[300px] overflow-auto whitespace-pre-wrap">{{ formatJson(event.details) }}</pre>
                      </div>

                      <div v-if="event.error_message" class="mt-3">
                        <div class="text-xs font-medium text-red-500 mb-1">Error</div>
                        <pre class="text-xs bg-red-50 p-3 rounded border border-red-200 whitespace-pre-wrap text-red-700">{{ event.error_message }}</pre>
                      </div>
                    </div>
                  </td>
                </tr>
              </template>
            </tbody>
          </table>
        </div>

        <!-- Pagination -->
        <template #footer>
          <div class="flex items-center justify-between">
            <div class="text-sm text-gray-500">
              Showing {{ total === 0 ? 0 : offset + 1 }}–{{ Math.min(offset + pageSize, total) }} of {{ total }}
            </div>
            <div class="flex gap-2">
              <BaseButton variant="secondary" size="sm" :disabled="offset === 0" @click="prevPage">Previous</BaseButton>
              <BaseButton variant="secondary" size="sm" :disabled="offset + pageSize >= total" @click="nextPage">Next</BaseButton>
            </div>
          </div>
        </template>
      </BaseCard>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, reactive, onMounted } from 'vue';
import axios from 'axios';
import AppLayout from '@/Layouts/AppLayout.vue';
import BaseCard from '@/components/BaseCard.vue';
import BaseButton from '@/components/BaseButton.vue';
import { useAuth } from '@/composables/useAuth';

const { user } = useAuth();

const loading = ref(false);
const events = ref([]);
const total = ref(0);
const offset = ref(0);
const pageSize = 50;
const expandedRow = ref(null);
const members = ref([]);

const resourceTypes = [
  { value: 'project', label: 'Project' },
  { value: 'api_key', label: 'API Key' },
  { value: 'dashboard', label: 'Dashboard' },
  { value: 'user', label: 'User' },
  { value: 'invitation', label: 'Invitation' },
  { value: 'member', label: 'Member' },
  { value: 'sso_config', label: 'SSO Config' },
  { value: 'mfa', label: 'MFA' },
  { value: 'session', label: 'Session' },
  { value: 'certificate', label: 'Certificate' },
  { value: 'payment_method', label: 'Payment Method' },
  { value: 'subscription', label: 'Subscription' },
  { value: 'budget', label: 'Budget' },
  { value: 'llm_integration', label: 'LLM Integration' },
  { value: 'secret_slot', label: 'Secret Slot' },
  { value: 'prompt_config', label: 'Prompt Config' },
  { value: 'rollout', label: 'Rollout' },
  { value: 'llm_settings', label: 'LLM Settings' },
  { value: 'session_profile', label: 'Session Profile' },
  { value: 'alert_rule', label: 'Alert Rule' },
  { value: 'notification_channel', label: 'Notification Channel' },
  { value: 'maintenance_window', label: 'Maintenance Window' },
  { value: 'health_check', label: 'Health Check' },
  { value: 'integration', label: 'Integration' },
  { value: 'scim', label: 'SCIM' },
  { value: 'provisioning_rule', label: 'Provisioning Rule' },
];

const filters = reactive({
  timeRange: '24h',
  resourceType: '',
  callerType: '',
  userId: '',
  tokenPrefix: '',
  callerKeyLabel: '',
  service: '',
});

const timeRangeToFrom = () => {
  const now = new Date();
  const map = {
    '1h': 60 * 60 * 1000,
    '6h': 6 * 60 * 60 * 1000,
    '24h': 24 * 60 * 60 * 1000,
    '7d': 7 * 24 * 60 * 60 * 1000,
    '30d': 30 * 24 * 60 * 60 * 1000,
  };
  return new Date(now.getTime() - (map[filters.timeRange] || map['24h'])).toISOString();
};

const fetchMembers = async () => {
  try {
    const res = await axios.get('/api/org/invitations/members');
    members.value = res.data || [];
  } catch {
    members.value = [];
  }
};

const onFilterChange = () => {
  offset.value = 0;
  fetchEvents();
};

const fetchEvents = async () => {
  loading.value = true;
  try {
    const params = {
      from: timeRangeToFrom(),
      limit: pageSize,
      offset: offset.value,
    };
    if (filters.resourceType) params.resource_type = filters.resourceType;
    if (filters.callerType) params.caller_type = filters.callerType;
    if (filters.service) params.service = filters.service;
    if (filters.userId) params.user_id = filters.userId;
    const tp = (filters.tokenPrefix || '').trim();
    if (tp) params.caller_key_prefix = tp;
    const kl = (filters.callerKeyLabel || '').trim();
    if (kl) params.caller_key_label = kl;

    const res = await axios.get('/api/audit/events', { params });
    events.value = res.data.events;
    total.value = res.data.total;
  } catch (err) {
    console.error('Failed to fetch audit events:', err);
    events.value = [];
    total.value = 0;
  } finally {
    loading.value = false;
  }
};

const prevPage = () => {
  offset.value = Math.max(0, offset.value - pageSize);
  fetchEvents();
};

const nextPage = () => {
  offset.value += pageSize;
  fetchEvents();
};

const toggleRow = (id) => {
  expandedRow.value = expandedRow.value === id ? null : id;
};

onMounted(async () => {
  await fetchMembers();
  await fetchEvents();
});

const formatTimestamp = (ts) => {
  if (!ts) return '';
  const d = new Date(ts.includes('T') ? ts : ts.replace(' ', 'T') + 'Z');
  return d.toLocaleString('en-US', {
    month: 'short', day: 'numeric',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false,
  });
};

const formatEventType = (et) => {
  if (!et) return '';
  return et.replace(/[._]/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
};

const shortId = (id) => {
  if (!id || id.length <= 12) return id;
  return id.slice(0, 8) + '...';
};

const formatJson = (s) => {
  try { return JSON.stringify(JSON.parse(s), null, 2); } catch { return s; }
};

const eventTypeBadgeClass = (et) => {
  const base = 'inline-flex items-center px-2 py-0.5 rounded text-xs font-medium';
  if (et?.includes('created')) return `${base} bg-green-100 text-green-800`;
  if (et?.includes('deleted') || et?.includes('removed') || et?.includes('revoked')) return `${base} bg-red-100 text-red-800`;
  if (et?.includes('failed')) return `${base} bg-red-100 text-red-800`;
  if (et?.includes('updated') || et?.includes('changed')) return `${base} bg-amber-100 text-amber-800`;
  return `${base} bg-indigo-100 text-indigo-800`;
};

const callerBadgeClass = (type) => {
  const base = 'inline-flex items-center px-2 py-0.5 rounded text-xs font-medium';
  if (type === 'agent') return `${base} bg-purple-100 text-purple-800`;
  if (type === 'system') return `${base} bg-gray-100 text-gray-600`;
  return `${base} bg-blue-100 text-blue-800`;
};

const formatCallerTypeLabel = (type) => {
  if (!type) return '';
  if (type === 'agent') return 'Agent';
  if (type === 'system') return 'System';
  if (type === 'user') return 'User';
  return type;
};

/** Matches Agents token list: `name (dh_...suffix)` */
const formatTokenDisplay = (label, prefix) => {
  const l = (label || '').trim();
  const p = (prefix || '').trim();
  const obfuscated = p ? `dh_...${p}` : '';
  if (l && obfuscated) return `${l} (${obfuscated})`;
  if (l) return l;
  return obfuscated;
};

const actorSecondaryText = (event) => {
  const tokenStr = formatTokenDisplay(event.caller_key_label, event.caller_key_prefix);
  if (event.caller_type === 'agent' && tokenStr) return tokenStr;
  if (event.caller_type === 'system' && tokenStr) return tokenStr;
  if (event.caller_type === 'user') {
    if (event.actor_email) return event.actor_email;
    if (event.actor_id) return shortId(event.actor_id);
  }
  if (event.actor_id) return shortId(event.actor_id);
  return '';
};

const actorSecondaryMonospace = (event) => {
  if (event.caller_type === 'user' && event.actor_email) return false;
  return true;
};

const actorSecondaryFullTitle = (event) => {
  const tokenStr = formatTokenDisplay(event.caller_key_label, event.caller_key_prefix);
  if (tokenStr) return tokenStr;
  if (event.actor_email) return event.actor_email;
  return event.actor_id || '';
};

const parseDetails = (details) => {
  try { return JSON.parse(details); } catch { return {}; }
};

const hasBeforeAfter = (event) => {
  const d = parseDetails(event.details);
  return d.before !== undefined && d.after !== undefined;
};

const hasCreated = (event) => {
  const d = parseDetails(event.details);
  return d.created !== undefined;
};

const hasDeleted = (event) => {
  const d = parseDetails(event.details);
  return d.deleted !== undefined;
};

const formatOriginType = (ot) => {
  const map = {
    'agent_chat': 'Agent Chat',
    'agent_investigation': 'Auto Investigation',
    'agent_task': 'Agent Task',
    'agent_token': 'Agent Token',
    'api_key': 'API Key',
    'system': 'System',
    'user': 'User',
  };
  return map[ot] || ot;
};

const originBadgeClass = (ot) => {
  const base = 'inline-flex items-center px-2 py-0.5 rounded text-xs font-medium';
  if (ot === 'agent_chat' || ot === 'agent_investigation' || ot === 'agent_task') return `${base} bg-purple-100 text-purple-800`;
  if (ot === 'agent_token' || ot === 'api_key') return `${base} bg-amber-100 text-amber-800`;
  if (ot === 'system') return `${base} bg-gray-100 text-gray-600`;
  return `${base} bg-blue-100 text-blue-800`;
};

const detailsSummary = (event) => {
  const d = parseDetails(event.details);
  if (d.before && d.after) {
    const keys = Object.keys(d.after);
    const changed = keys.filter(k => JSON.stringify(d.before[k]) !== JSON.stringify(d.after[k]));
    if (changed.length > 0) return `Changed: ${changed.join(', ')}`;
    return 'No visible changes';
  }
  if (d.created) {
    const name = d.created.name || d.created.label || d.created.key_type || '';
    return name ? `Created: ${name}` : 'Created';
  }
  if (d.deleted) {
    const name = d.deleted.name || d.deleted.label || '';
    return name ? `Deleted: ${name}` : 'Deleted';
  }
  return event.details && event.details !== '{}' ? event.details : '—';
};

const detailFields = (event) => {
  const fields = {};
  if (event.event_id) fields['Event ID'] = event.event_id;
  if (event.event_type) fields['Event Type'] = event.event_type;
  if (event.actor_email) fields['Actor'] = event.actor_email;
  if (event.actor_id) fields['Actor ID'] = event.actor_id;
  if (event.caller_user_email) fields['Token owner'] = event.caller_user_email;
  else if (event.caller_user_id) fields['Token owner ID'] = event.caller_user_id;
  const tok = formatTokenDisplay(event.caller_key_label, event.caller_key_prefix);
  if (tok) fields['Token'] = tok;
  if (event.organization_id) fields['Organization ID'] = event.organization_id;
  if (event.project_id) fields['Project ID'] = event.project_id;
  if (event.resource_type) fields['Resource Type'] = event.resource_type;
  if (event.resource_id) fields['Resource ID'] = event.resource_id;
  if (event.service) fields['Service'] = event.service;
  if (event.origin_type) fields['Origin'] = formatOriginType(event.origin_type);
  if (event.origin_ref) fields['Origin Ref'] = event.origin_ref;
  if (event.origin_reason) fields['Origin Reason'] = event.origin_reason;
  if (event.source_id) fields['Source ID'] = event.source_id;
  if (event.prompt_config_name) fields['Prompt Config'] = event.prompt_config_name;
  if (event.model_used) fields['Model'] = event.model_used;
  if (event.total_input_tokens) fields['Input Tokens'] = String(event.total_input_tokens);
  if (event.total_output_tokens) fields['Output Tokens'] = String(event.total_output_tokens);
  if (event.mcp_tool_name) fields['MCP Tool'] = event.mcp_tool_name;
  if (event.duration_ms) fields['Duration'] = `${event.duration_ms}ms`;
  return fields;
};
</script>

<style scoped>
.filter-select {
  @apply text-sm border-gray-300 rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 py-1.5 pr-8;
}

.filter-input {
  @apply text-sm border-gray-300 rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 py-1.5 px-2;
}

.th-cell {
  @apply px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider;
}

.td-cell {
  @apply px-4 py-2.5 text-sm;
}
</style>
