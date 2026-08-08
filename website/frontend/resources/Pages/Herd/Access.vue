<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-[1200px] mx-auto px-8 py-6">
      <div class="mb-6">
        <h1 class="text-2xl font-bold text-gray-900 dark:text-gray-100">Access Grants</h1>
        <p class="text-sm text-gray-500 dark:text-gray-400 mt-1">Manage agent-to-agent access for your A2A agents</p>
      </div>

      <!-- Tabs -->
      <div class="border-b border-gray-200 dark:border-gray-700 mb-6">
        <nav class="flex -mb-px space-x-8">
          <button v-for="tab in tabs" :key="tab.key" @click="activeTab = tab.key" class="py-3 px-1 border-b-2 text-sm font-medium transition-colors" :class="activeTab === tab.key ? 'border-blue-500 text-blue-600 dark:text-blue-400' : 'border-transparent text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-300'">
            {{ tab.label }}
            <span v-if="tab.count > 0" class="ml-2 inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300">{{ tab.count }}</span>
          </button>
        </nav>
      </div>

      <!-- Loading -->
      <div v-if="loading" class="flex items-center justify-center py-16">
        <svg class="spinner w-6 h-6 text-blue-500" fill="none" viewBox="0 0 24 24">
          <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
          <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"></path>
        </svg>
      </div>

      <!-- Incoming Requests Tab -->
      <div v-else-if="activeTab === 'incoming'">
        <BaseCard v-if="incomingGrants.length === 0" class="!p-12 text-center">
          <svg class="mx-auto h-12 w-12 text-gray-400 dark:text-gray-500 mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M20 13V6a2 2 0 00-2-2H6a2 2 0 00-2 2v7m16 0v5a2 2 0 01-2 2H6a2 2 0 01-2-2v-5m16 0h-2.586a1 1 0 00-.707.293l-2.414 2.414a1 1 0 01-.707.293h-3.172a1 1 0 01-.707-.293l-2.414-2.414A1 1 0 006.586 13H4" />
          </svg>
          <p class="text-sm text-gray-500 dark:text-gray-400">No pending access requests</p>
        </BaseCard>
        <div v-else class="space-y-3">
          <BaseCard v-for="grant in incomingGrants" :key="grant.id" class="!p-4">
            <div class="flex items-center justify-between">
              <div>
                <p class="text-sm font-medium text-gray-900 dark:text-gray-100">
                  <span class="font-semibold">{{ grant.grantedAgentName }}</span>
                  <span class="text-gray-500 dark:text-gray-400 mx-1">&rarr;</span>
                  <span class="font-semibold">{{ grant.targetAgentName }}</span>
                </p>
                <p class="text-xs text-gray-500 dark:text-gray-400 mt-1">
                  Requested by {{ grant.requestedByEmail || 'Unknown' }}
                  <span v-if="grant.grantedOrgDomain">({{ grant.grantedOrgDomain }})</span>
                  &middot; {{ formatTime(grant.requestedAt) }}
                </p>
              </div>
              <div class="flex items-center space-x-2">
                <button @click="resolveGrant(grant.id, 'approve')" class="px-3 py-1.5 text-xs font-medium text-white bg-green-600 hover:bg-green-700 rounded-lg transition-colors">Approve</button>
                <button @click="resolveGrant(grant.id, 'deny')" class="px-3 py-1.5 text-xs font-medium text-white bg-red-600 hover:bg-red-700 rounded-lg transition-colors">Deny</button>
              </div>
            </div>
          </BaseCard>
        </div>
      </div>

      <!-- Active Grants Tab -->
      <div v-else-if="activeTab === 'active'">
        <BaseCard v-if="activeGrants.length === 0" class="!p-12 text-center">
          <p class="text-sm text-gray-500 dark:text-gray-400">No active access grants</p>
        </BaseCard>
        <div v-else class="space-y-3">
          <BaseCard v-for="pair in activeGrants" :key="`${pair.agentAId}-${pair.agentBId}`" class="!p-4">
            <div class="flex items-center justify-between">
              <div>
                <p class="text-sm font-medium text-gray-900 dark:text-gray-100">
                  <span class="font-semibold">{{ pair.agentAName }}</span>
                  <span class="text-gray-500 dark:text-gray-400 mx-1">{{ pair.bidirectional ? '⇄' : '→' }}</span>
                  <span class="font-semibold">{{ pair.agentBName }}</span>
                </p>
                <p class="text-xs text-gray-500 dark:text-gray-400 mt-1">
                  <span v-if="pair.orgDomain">({{ pair.orgDomain }})</span>
                  Approved {{ formatTime(pair.approvedAt) }}
                </p>
              </div>
              <button @click="revokeGrantPair(pair)" class="px-3 py-1.5 text-xs font-medium text-red-600 dark:text-red-400 border border-red-300 dark:border-red-600 rounded-lg hover:bg-red-50 dark:hover:bg-red-900/20 transition-colors">Revoke</button>
            </div>
          </BaseCard>
        </div>
      </div>

      <!-- Outgoing Requests Tab -->
      <div v-else-if="activeTab === 'outgoing'">
        <BaseCard v-if="outgoingGrants.length === 0" class="!p-12 text-center">
          <svg class="mx-auto h-12 w-12 text-gray-400 dark:text-gray-500 mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M20 13V6a2 2 0 00-2-2H6a2 2 0 00-2 2v7m16 0v5a2 2 0 01-2 2H6a2 2 0 01-2-2v-5m16 0h-2.586a1 1 0 00-.707.293l-2.414 2.414a1 1 0 01-.707.293h-3.172a1 1 0 01-.707-.293l-2.414-2.414A1 1 0 006.586 13H4" />
          </svg>
          <p class="text-sm text-gray-500 dark:text-gray-400">No pending outgoing requests</p>
        </BaseCard>
        <div v-else class="space-y-3">
          <BaseCard v-for="grant in outgoingGrants" :key="grant.id" class="!p-4">
            <div class="flex items-center justify-between">
              <div>
                <p class="text-sm font-medium text-gray-900 dark:text-gray-100">
                  <span class="font-semibold">{{ grant.grantedAgentName }}</span>
                  <span class="text-gray-500 dark:text-gray-400 mx-1">&rarr;</span>
                  <span class="font-semibold">{{ grant.targetAgentName }}</span>
                </p>
                <p class="text-xs text-gray-500 dark:text-gray-400 mt-1">
                  Requested by {{ grant.requestedByEmail || 'Unknown' }}
                  <span v-if="grant.targetOrgDomain">({{ grant.targetOrgDomain }})</span>
                  &middot; {{ formatTime(grant.requestedAt) }}
                </p>
              </div>
              <span class="inline-flex items-center px-2.5 py-0.5 rounded text-xs font-medium bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-300">pending</span>
            </div>
          </BaseCard>
        </div>
      </div>
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
const { user, fetchUser } = useAuth();

const projectId = computed(() => route.params.id);
const project = computed(() => ({ id: projectId.value }));
const loading = ref(true);
const activeTab = ref('incoming');
const incomingGrants = ref([]);
const activeGrants = ref([]);
const outgoingGrants = ref([]);

const tabs = computed(() => [
  { key: 'incoming', label: 'Incoming Requests', count: incomingGrants.value.length },
  { key: 'active', label: 'Active Grants', count: activeGrants.value.length },
  { key: 'outgoing', label: 'Outgoing Requests', count: outgoingGrants.value.length },
]);

const formatTime = (timestamp) => {
  if (!timestamp) return '-';
  const date = new Date(timestamp);
  const now = new Date();
  const diff = now - date;
  if (diff < 60000) return 'just now';
  if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`;
  if (diff < 86400000) return `${Math.floor(diff / 3600000)}h ago`;
  return date.toLocaleDateString();
};

const statusClass = (status) => {
  const map = {
    approved: 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300',
    denied: 'bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-300',
    pending: 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-300',
    revoked: 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300',
  };
  return map[status] || 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300';
};

const fetchAccess = async () => {
  loading.value = true;
  try {
    const [incomingRes, grantsRes, outgoingRes] = await Promise.all([
      axios.get(`/api/projects/${projectId.value}/herd/access/incoming`).catch(() => ({ data: [] })),
      axios.get(`/api/projects/${projectId.value}/herd/access/grants`).catch(() => ({ data: [] })),
      axios.get(`/api/projects/${projectId.value}/herd/access/outgoing`).catch(() => ({ data: [] })),
    ]);
    incomingGrants.value = incomingRes.data || [];
    activeGrants.value = grantsRes.data || [];
    outgoingGrants.value = outgoingRes.data || [];
  } catch (error) {
    console.error('Failed to fetch access grants:', error);
  } finally {
    loading.value = false;
  }
};

const resolveGrant = async (grantId, action) => {
  try {
    await axios.post(`/api/projects/${projectId.value}/herd/access/${grantId}/${action}`);
    await fetchAccess();
  } catch (error) {
    console.error(`Failed to ${action} grant:`, error);
    alert(error.response?.data?.error || `Failed to ${action} grant`);
  }
};

const revokeGrant = async (grantId) => {
  try {
    await axios.post(`/api/projects/${projectId.value}/herd/access/${grantId}/revoke`);
  } catch (error) {
    console.error('Failed to revoke grant:', error);
    throw error;
  }
};

const revokeGrantPair = async (pair) => {
  if (!confirm('Revoke this access grant?')) return;
  try {
    const ids = [pair.aToBGrantId, pair.bToAGrantId].filter(Boolean);
    await Promise.all(ids.map(id => revokeGrant(id)));
    await fetchAccess();
  } catch (error) {
    alert(error.response?.data?.error || 'Failed to revoke grant');
  }
};

onMounted(async () => {
  await fetchUser();
  await fetchAccess();
});

watch(projectId, async () => {
  await fetchUser();
  await fetchAccess();
});
</script>

<style scoped>
.spinner { animation: spin 1s linear infinite; }
@keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
</style>
