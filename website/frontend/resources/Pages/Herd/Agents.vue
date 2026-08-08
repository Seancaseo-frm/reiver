<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-[1200px] mx-auto px-8 py-6">
      <div class="flex items-center justify-between mb-6">
        <div>
          <h1 class="text-2xl font-bold text-gray-900 dark:text-gray-100">A2A Agents</h1>
          <p class="text-sm text-gray-500 dark:text-gray-400 mt-1">Register and manage your agents in the Herd registry</p>
        </div>
        <button @click="openRegisterModal" class="inline-flex items-center px-4 py-2 border border-transparent rounded-lg shadow-sm text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 dark:bg-blue-500 dark:hover:bg-blue-600 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 transition-colors">
          <svg class="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" /></svg>
          Register Agent
        </button>
      </div>

      <!-- Loading -->
      <div v-if="loading" class="flex items-center justify-center py-16">
        <svg class="spinner w-6 h-6 text-blue-500" fill="none" viewBox="0 0 24 24">
          <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
          <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"></path>
        </svg>
      </div>

      <!-- Empty State -->
      <BaseCard v-else-if="agents.length === 0" class="!p-12 text-center">
        <svg class="mx-auto h-16 w-16 text-gray-400 dark:text-gray-500 mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
        </svg>
        <h3 class="text-lg font-medium text-gray-900 dark:text-gray-100 mb-2">No agents registered</h3>
        <p class="text-sm text-gray-500 dark:text-gray-400 mb-4">Register your first A2A agent to start using the Herd protocol.</p>
        <button @click="openRegisterModal" class="inline-flex items-center px-4 py-2 border border-transparent rounded-lg shadow-sm text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 transition-colors">
          Register Agent
        </button>
      </BaseCard>

      <!-- Agent List -->
      <div v-else class="space-y-4">
        <BaseCard v-for="agent in agents" :key="agent.id" class="!p-0">
          <div class="p-5">
            <div class="flex items-start justify-between">
              <div class="flex items-start space-x-4">
                <div class="w-10 h-10 rounded-lg flex items-center justify-center" :class="agent.enabled ? 'bg-green-100 dark:bg-green-900/30' : 'bg-gray-100 dark:bg-gray-700'">
                  <svg class="w-5 h-5" :class="agent.enabled ? 'text-green-600 dark:text-green-400' : 'text-gray-400'" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
                  </svg>
                </div>
                <div>
                  <h3 class="text-base font-semibold text-gray-900 dark:text-gray-100">{{ agent.name }}</h3>
                  <p class="text-sm text-gray-500 dark:text-gray-400 mt-0.5">{{ agent.description || 'No description' }}</p>
                  <div class="flex items-center space-x-3 mt-2 flex-wrap gap-y-1">
                    <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium" :class="visibilityClass(agent.visibility)">{{ agent.visibility }}</span>
                    <span class="text-xs text-gray-400 dark:text-gray-500 font-mono truncate max-w-[300px]" :title="agent.endpointUrl">{{ agent.endpointUrl }}</span>
                    <span v-if="agent.keyId" class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-purple-100 text-purple-800 dark:bg-purple-900/30 dark:text-purple-300">{{ tokenLabel(agent.keyId) }}</span>
                    <span class="text-xs text-gray-400 dark:text-gray-500">Created {{ formatTime(agent.createdAt) }}</span>
                  </div>
                </div>
              </div>
              <div class="flex items-center space-x-2">
                <button @click="editAgent(agent)" class="p-2 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 transition-colors rounded-lg hover:bg-gray-100 dark:hover:bg-gray-700">
                  <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z" /></svg>
                </button>
                <button @click="deleteAgent(agent)" class="p-2 text-gray-400 hover:text-red-600 dark:hover:text-red-400 transition-colors rounded-lg hover:bg-gray-100 dark:hover:bg-gray-700">
                  <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" /></svg>
                </button>
              </div>
            </div>
          </div>
        </BaseCard>
      </div>

      <!-- Register/Edit Agent Modal -->
      <div v-if="showRegisterModal" class="fixed inset-0 z-50 overflow-y-auto" @click.self="showRegisterModal = false">
        <div class="flex items-center justify-center min-h-screen px-4">
          <div class="fixed inset-0 bg-black/50 transition-opacity" @click="showRegisterModal = false"></div>
          <div class="relative bg-white dark:bg-gray-800 rounded-xl shadow-2xl max-w-lg w-full p-6 z-10">
            <h3 class="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-4">{{ editingId ? 'Edit Agent' : 'Register A2A Agent' }}</h3>
            <form @submit.prevent="submitAgent">
              <div class="space-y-4">
                <div>
                  <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Name</label>
                  <input v-model="form.name" type="text" required :disabled="!!editingId" class="w-full rounded-lg border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 shadow-sm focus:ring-blue-500 focus:border-blue-500 text-sm disabled:opacity-50" placeholder="my-agent" />
                </div>
                <div>
                  <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Description</label>
                  <textarea v-model="form.description" rows="2" class="w-full rounded-lg border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 shadow-sm focus:ring-blue-500 focus:border-blue-500 text-sm" placeholder="What this agent does..."></textarea>
                </div>
                <div>
                  <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Endpoint URL</label>
                  <input v-model="form.endpointUrl" type="url" required class="w-full rounded-lg border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 shadow-sm focus:ring-blue-500 focus:border-blue-500 text-sm font-mono" placeholder="https://my-agent.example.com/a2a" />
                  <p class="text-xs text-gray-400 dark:text-gray-500 mt-1">Where Herd delivers A2A messages to your agent</p>
                </div>
                <div>
                  <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Agent Token</label>
                  <select v-model="form.keyId" class="w-full rounded-lg border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 shadow-sm focus:ring-blue-500 focus:border-blue-500 text-sm">
                    <option :value="null">None</option>
                    <option v-for="token in agentTokens" :key="token.id" :value="token.id">
                      {{ token.label || `...${token.keyPrefix}` }}
                    </option>
                  </select>
                  <p class="text-xs text-gray-400 dark:text-gray-500 mt-1">Link to an existing agent token from this project</p>
                </div>
                <div>
                  <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Visibility</label>
                  <select v-model="form.visibility" class="w-full rounded-lg border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 shadow-sm focus:ring-blue-500 focus:border-blue-500 text-sm">
                    <option value="private">Private (same project only)</option>
                    <option value="org">Organization (same org)</option>
                    <option value="public">Public (any org, requires access grant)</option>
                  </select>
                </div>
              </div>
              <div class="flex justify-end space-x-3 mt-6">
                <button type="button" @click="closeModal" class="px-4 py-2 text-sm font-medium text-gray-700 dark:text-gray-300 bg-gray-100 dark:bg-gray-700 rounded-lg hover:bg-gray-200 dark:hover:bg-gray-600 transition-colors">Cancel</button>
                <button type="submit" :disabled="submitting" class="px-4 py-2 text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 rounded-lg transition-colors disabled:opacity-50">
                  {{ submitting ? 'Saving...' : (editingId ? 'Save' : 'Register') }}
                </button>
              </div>
            </form>
          </div>
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
const agents = ref([]);
const agentTokens = ref([]);
const showRegisterModal = ref(false);
const submitting = ref(false);
const editingId = ref(null);

const defaultForm = () => ({ name: '', description: '', endpointUrl: '', keyId: null, visibility: 'org' });
const form = ref(defaultForm());

const visibilityClass = (v) => {
  const map = {
    private: 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300',
    org: 'bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300',
    public: 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300',
  };
  return map[v] || map.private;
};

const tokenLabel = (keyId) => {
  const token = agentTokens.value.find(t => t.id === keyId);
  if (!token) return 'token';
  return token.label || `...${token.keyPrefix}`;
};

const formatTime = (timestamp) => {
  const date = new Date(timestamp);
  const now = new Date();
  const diff = now - date;
  if (diff < 60000) return 'just now';
  if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`;
  if (diff < 86400000) return `${Math.floor(diff / 3600000)}h ago`;
  return date.toLocaleDateString();
};

const fetchAgents = async () => {
  loading.value = true;
  try {
    const res = await axios.get(`/api/projects/${projectId.value}/herd/agents`);
    agents.value = res.data || [];
  } catch (error) {
    console.error('Failed to fetch agents:', error);
  } finally {
    loading.value = false;
  }
};

const fetchAgentTokens = async () => {
  try {
    const res = await axios.get(`/api/projects/${projectId.value}/keys`, { params: { key_type: 'agent' } });
    agentTokens.value = (res.data || []).map(k => ({
      id: k.id,
      label: k.label,
      keyPrefix: k.key_prefix,
    }));
  } catch (error) {
    console.error('Failed to fetch agent tokens:', error);
  }
};

const openRegisterModal = () => {
  editingId.value = null;
  form.value = defaultForm();
  showRegisterModal.value = true;
};

const closeModal = () => {
  showRegisterModal.value = false;
  editingId.value = null;
  form.value = defaultForm();
};

const submitAgent = async () => {
  submitting.value = true;
  try {
    if (editingId.value) {
      await axios.put(`/api/projects/${projectId.value}/herd/agents/${editingId.value}`, {
        description: form.value.description,
        endpointUrl: form.value.endpointUrl,
        keyId: form.value.keyId,
        visibility: form.value.visibility,
      });
    } else {
      await axios.post(`/api/projects/${projectId.value}/herd/agents`, {
        name: form.value.name,
        description: form.value.description,
        endpointUrl: form.value.endpointUrl,
        keyId: form.value.keyId,
        visibility: form.value.visibility,
      });
    }
    closeModal();
    await fetchAgents();
  } catch (error) {
    console.error('Failed to save agent:', error);
    alert(error.response?.data || 'Failed to save agent');
  } finally {
    submitting.value = false;
  }
};

const editAgent = (agent) => {
  editingId.value = agent.id;
  form.value = {
    name: agent.name,
    description: agent.description || '',
    endpointUrl: agent.endpointUrl || '',
    keyId: agent.keyId || null,
    visibility: agent.visibility,
  };
  showRegisterModal.value = true;
};

const deleteAgent = async (agent) => {
  if (!confirm(`Delete agent "${agent.name}"? This cannot be undone.`)) return;
  try {
    await axios.delete(`/api/projects/${projectId.value}/herd/agents/${agent.id}`);
    await fetchAgents();
  } catch (error) {
    console.error('Failed to delete agent:', error);
    alert(error.response?.data || 'Failed to delete agent');
  }
};

onMounted(async () => {
  await fetchUser();
  await Promise.all([fetchAgents(), fetchAgentTokens()]);
});

watch(projectId, async () => {
  await fetchUser();
  await Promise.all([fetchAgents(), fetchAgentTokens()]);
});
</script>

<style scoped>
.spinner { animation: spin 1s linear infinite; }
@keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
</style>
