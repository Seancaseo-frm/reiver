<template>
  <AppLayout :project="project">
    <div class="max-w-[1200px] mx-auto px-4 py-6 space-y-6">
      <div class="flex items-center justify-between">
        <div>
          <h1 class="text-2xl font-bold text-gray-900 dark:text-white">Agent Tokens</h1>
          <p class="mt-1 text-sm text-gray-500 dark:text-gray-400">Manage tokens used by AI agents and MCP integrations</p>
        </div>
        <button
          @click="showCreateModal = true"
          class="px-4 py-2 text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 rounded-lg transition-colors"
        >Create Agent Token</button>
      </div>

      <div v-if="loading" class="text-sm text-gray-500 dark:text-gray-400">Loading tokens…</div>

      <div v-else-if="tokens.length === 0" class="text-center py-12">
        <p class="text-gray-500 dark:text-gray-400">No agent tokens found. Create one to get started.</p>
      </div>

      <div v-else class="space-y-3">
        <BaseCard v-for="token in tokens" :key="token.id">
          <div class="p-4 flex items-start justify-between">
            <div class="space-y-1">
              <div class="flex items-center gap-2">
                <h3 class="text-sm font-semibold text-gray-900 dark:text-white">{{ token.label || '(unnamed)' }}</h3>
                <span class="text-xs font-mono text-gray-500 dark:text-gray-400">{{ token.key_prefix }}…</span>
              </div>
              <div class="flex items-center gap-4 text-xs text-gray-500 dark:text-gray-400">
                <span>Created {{ formatDate(token.created_at) }}</span>
                <span v-if="token.expires_at">Expires {{ formatDate(token.expires_at) }}</span>
                <span v-else>No expiration</span>
              </div>
              <div v-if="token.scopes && token.scopes.length" class="flex flex-wrap gap-1 mt-1">
                <span v-for="s in token.scopes" :key="s" class="text-xs px-2 py-0.5 rounded-full bg-gray-100 dark:bg-gray-800 text-gray-600 dark:text-gray-300">{{ s }}</span>
              </div>
              <div v-if="tokenStats[token.key_prefix]" class="mt-2 text-xs text-gray-500 dark:text-gray-400">
                {{ tokenStats[token.key_prefix].call_count }} calls in last 30d ·
                Tools: {{ tokenStats[token.key_prefix].tools_used.join(', ') || 'none' }}
              </div>
            </div>
            <button
              @click="revokeToken(token.id)"
              class="text-xs text-red-600 dark:text-red-400 hover:underline"
            >Revoke</button>
          </div>
        </BaseCard>
      </div>

      <!-- Create modal -->
      <div v-if="showCreateModal" class="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
        <div class="bg-white dark:bg-gray-800 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
          <h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Agent Token</h2>
          <div>
            <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Label</label>
            <input v-model="newLabel" type="text" placeholder="e.g. Cursor Dev" class="w-full text-sm px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100" />
          </div>
          <div v-if="createdKey" class="p-3 bg-green-50 dark:bg-green-900/20 rounded-lg">
            <p class="text-sm text-green-800 dark:text-green-200 font-medium">Token created — copy it now, it won't be shown again:</p>
            <code class="block mt-1 text-xs font-mono break-all text-green-900 dark:text-green-100">{{ createdKey }}</code>
          </div>
          <div class="flex justify-end gap-3">
            <button @click="showCreateModal = false; createdKey = ''" class="px-4 py-2 text-sm text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-white">Close</button>
            <button v-if="!createdKey" @click="createToken" :disabled="creating" class="px-4 py-2 text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 rounded-lg disabled:opacity-50">
              {{ creating ? 'Creating…' : 'Create' }}
            </button>
          </div>
        </div>
      </div>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue';
import { useRoute } from 'vue-router';
import axios from 'axios';
import AppLayout from '@/Layouts/AppLayout.vue';
import BaseCard from '@/components/BaseCard.vue';

const route = useRoute();
const projectId = computed(() => route.params.id);
const project = computed(() => ({ id: projectId.value }));

const tokens = ref([]);
const tokenStats = ref({});
const loading = ref(true);

const showCreateModal = ref(false);
const newLabel = ref('');
const createdKey = ref('');
const creating = ref(false);

function formatDate(ts) {
  if (!ts) return '';
  return new Date(ts).toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' });
}

async function fetchTokens() {
  try {
    const { data } = await axios.get(`/api/projects/${projectId.value}/keys`, { params: { key_type: 'agent' } });
    tokens.value = Array.isArray(data) ? data : (data.keys || []);
  } catch (e) {
    console.error('Failed to load tokens', e);
  } finally {
    loading.value = false;
  }
}

async function fetchTokenStats() {
  try {
    const { data } = await axios.get(`/api/projects/${projectId.value}/mcp/stats/by-token`, { params: { time_range: '30d' } });
    const map = {};
    for (const row of data) {
      if (row.key_prefix) map[row.key_prefix] = row;
    }
    tokenStats.value = map;
  } catch {
    // non-critical
  }
}

async function createToken() {
  creating.value = true;
  try {
    const { data } = await axios.post(`/api/projects/${projectId.value}/keys`, {
      key_type: 'agent',
      label: newLabel.value || undefined,
      scopes: ['project:read', 'llm:read', 'observability:read'],
    });
    createdKey.value = data.key || data.token || JSON.stringify(data);
    newLabel.value = '';
    await fetchTokens();
  } catch (e) {
    alert('Failed to create token: ' + (e.response?.data?.message || e.message));
  } finally {
    creating.value = false;
  }
}

async function revokeToken(id) {
  if (!confirm('Revoke this token? Any agents using it will lose access.')) return;
  try {
    await axios.delete(`/api/projects/${projectId.value}/keys/${id}`);
    await fetchTokens();
  } catch (e) {
    alert('Failed to revoke token: ' + (e.response?.data?.message || e.message));
  }
}

onMounted(() => {
  fetchTokens();
  fetchTokenStats();
});
</script>
