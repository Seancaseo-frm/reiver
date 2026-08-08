<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-[1200px] mx-auto px-8 py-6">
      <!-- Back link -->
      <router-link :to="`/p/${projectId}/herd/discovery`" class="inline-flex items-center text-sm text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-200 mb-4">
        <svg class="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7" /></svg>
        Back to Discovery
      </router-link>

      <!-- Loading -->
      <div v-if="loading" class="flex items-center justify-center py-16">
        <svg class="spinner w-6 h-6 text-blue-500" fill="none" viewBox="0 0 24 24">
          <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
          <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"></path>
        </svg>
      </div>

      <!-- Error -->
      <BaseCard v-else-if="error" class="!p-8 text-center">
        <p class="text-red-600 dark:text-red-400">{{ error }}</p>
      </BaseCard>

      <!-- Agent Detail -->
      <div v-else-if="agent">
        <!-- Header -->
        <BaseCard class="!p-6 mb-6">
          <div class="flex items-start justify-between">
            <div class="flex-1">
              <div class="flex items-center space-x-3">
                <h1 class="text-2xl font-bold text-gray-900 dark:text-gray-100">{{ agent.name }}</h1>
                <span class="inline-flex items-center px-2.5 py-0.5 rounded text-xs font-medium" :class="visibilityClass(agent.visibility)">{{ agent.visibility }}</span>
              </div>
              <p class="text-sm text-gray-500 dark:text-gray-400 mt-2">{{ agent.description || 'No description provided' }}</p>
              <p class="text-xs text-gray-400 dark:text-gray-500 mt-2">Organization: {{ agent.organizationName || agent.organization_id }}</p>
            </div>
            <button
              v-if="agent.needsAccess && !accessGranted"
              @click="openAccessModal"
              :disabled="accessRequested || requestingAccess"
              class="ml-4 shrink-0 inline-flex items-center px-4 py-2 border border-blue-600 dark:border-blue-400 rounded-lg text-sm font-medium text-blue-600 dark:text-blue-400 hover:bg-blue-50 dark:hover:bg-blue-900/20 transition-colors disabled:opacity-50"
            >
              {{ accessRequested ? 'Access Requested' : 'Request Access' }}
            </button>
            <span v-else-if="!agent.needsAccess" class="ml-4 shrink-0 inline-flex items-center px-3 py-1.5 rounded-lg text-xs font-medium bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300">
              Access Granted
            </span>
          </div>
        </BaseCard>

        <!-- Capabilities -->
        <BaseCard class="!p-6 mb-6">
          <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-4">Capabilities</h2>
          <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
            <div class="flex items-center space-x-2">
              <span class="w-2 h-2 rounded-full" :class="card.capabilities?.streaming ? 'bg-green-500' : 'bg-gray-300 dark:bg-gray-600'"></span>
              <span class="text-sm text-gray-700 dark:text-gray-300">Streaming</span>
            </div>
            <div class="flex items-center space-x-2">
              <span class="w-2 h-2 rounded-full" :class="card.capabilities?.push_notifications || card.capabilities?.pushNotifications ? 'bg-green-500' : 'bg-gray-300 dark:bg-gray-600'"></span>
              <span class="text-sm text-gray-700 dark:text-gray-300">Push Notifications</span>
            </div>
          </div>
          <div class="mt-4 flex flex-wrap gap-2" v-if="card.defaultInputModes?.length || card.default_input_modes?.length">
            <span class="text-xs text-gray-500 dark:text-gray-400 mr-2">Input:</span>
            <span v-for="mode in (card.defaultInputModes || card.default_input_modes || [])" :key="mode" class="inline-flex items-center px-2 py-0.5 rounded text-xs bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-300">{{ mode }}</span>
          </div>
          <div class="mt-2 flex flex-wrap gap-2" v-if="card.defaultOutputModes?.length || card.default_output_modes?.length">
            <span class="text-xs text-gray-500 dark:text-gray-400 mr-2">Output:</span>
            <span v-for="mode in (card.defaultOutputModes || card.default_output_modes || [])" :key="mode" class="inline-flex items-center px-2 py-0.5 rounded text-xs bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-300">{{ mode }}</span>
          </div>
        </BaseCard>

        <!-- Skills -->
        <BaseCard v-if="card.skills && card.skills.length > 0" class="!p-6 mb-6">
          <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-4">Skills</h2>
          <div class="space-y-4">
            <div v-for="skill in card.skills" :key="skill.id" class="border border-gray-200 dark:border-gray-700 rounded-lg p-4">
              <div class="flex items-start justify-between">
                <div>
                  <h3 class="text-sm font-medium text-gray-900 dark:text-gray-100">{{ skill.name }}</h3>
                  <p class="text-sm text-gray-500 dark:text-gray-400 mt-1">{{ skill.description }}</p>
                </div>
              </div>
              <div class="mt-2 flex flex-wrap gap-1" v-if="skill.tags && skill.tags.length">
                <span v-for="tag in skill.tags" :key="tag" class="inline-flex items-center px-2 py-0.5 rounded text-xs bg-blue-50 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300">{{ tag }}</span>
              </div>
              <div class="mt-2" v-if="skill.examples && skill.examples.length">
                <p class="text-xs text-gray-500 dark:text-gray-400 mb-1">Examples:</p>
                <ul class="list-disc list-inside text-xs text-gray-600 dark:text-gray-400 space-y-0.5">
                  <li v-for="ex in skill.examples" :key="ex">{{ ex }}</li>
                </ul>
              </div>
            </div>
          </div>
        </BaseCard>

        <!-- Interfaces -->
        <BaseCard v-if="card.supportedInterfaces?.length || card.supported_interfaces?.length" class="!p-6">
          <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-4">Interfaces</h2>
          <div class="space-y-3">
            <div v-for="iface in (card.supportedInterfaces || card.supported_interfaces || [])" :key="iface.url" class="flex items-center justify-between py-2 border-b border-gray-100 dark:border-gray-700 last:border-0">
              <div>
                <p class="text-sm text-gray-900 dark:text-gray-100 font-mono">{{ iface.url }}</p>
                <p class="text-xs text-gray-500 dark:text-gray-400 mt-0.5">{{ iface.protocol_binding || iface.protocolBinding }} v{{ iface.protocol_version || iface.protocolVersion }}</p>
              </div>
            </div>
          </div>
        </BaseCard>
      </div>
    </div>

    <!-- Agent Selection Modal -->
    <div v-if="showAccessModal" class="fixed inset-0 z-50 flex items-center justify-center">
      <div class="absolute inset-0 bg-black/50" @click="closeAccessModal"></div>
      <div class="relative bg-white dark:bg-gray-800 rounded-xl shadow-xl max-w-md w-full mx-4 p-6">
        <h3 class="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">Request Access</h3>
        <p class="text-sm text-gray-500 dark:text-gray-400 mb-4">
          Select which of your agents should be granted access to <span class="font-semibold">{{ agent?.name }}</span>.
        </p>

        <div v-if="projectAgents.length === 0" class="text-sm text-gray-500 dark:text-gray-400 py-4 text-center">
          No agents in your project. Register an agent first.
        </div>

        <div v-else>
          <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Source Agent</label>
          <select v-model="selectedSourceAgent" class="w-full rounded-lg border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 text-sm focus:ring-blue-500 focus:border-blue-500">
            <option value="" disabled>Choose an agent...</option>
            <option v-for="a in projectAgents" :key="a.id" :value="a.id">{{ a.name }}</option>
          </select>
        </div>

        <div class="flex justify-end space-x-3 mt-6">
          <button @click="closeAccessModal" class="px-4 py-2 text-sm font-medium text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-700 rounded-lg transition-colors">Cancel</button>
          <button @click="submitAccessRequest" :disabled="!selectedSourceAgent || requestingAccess" class="px-4 py-2 text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 rounded-lg transition-colors disabled:opacity-50">
            {{ requestingAccess ? 'Requesting...' : 'Request Access' }}
          </button>
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
import { useAuth } from '@/composables/useAuth';

const route = useRoute();
const { user, fetchUser } = useAuth();

const projectId = computed(() => route.params.id);
const agentId = computed(() => route.params.agentId);
const project = computed(() => ({ id: projectId.value }));

const loading = ref(true);
const error = ref(null);
const agent = ref(null);
const card = ref({});
const accessRequested = ref(false);
const accessGranted = ref(false);
const requestingAccess = ref(false);
const projectAgents = ref([]);
const showAccessModal = ref(false);
const selectedSourceAgent = ref('');

const visibilityClass = (v) => {
  const map = {
    org: 'bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300',
    public: 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300',
  };
  return map[v] || 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300';
};

const fetchAgent = async () => {
  loading.value = true;
  error.value = null;
  try {
    const res = await axios.get(`/api/projects/${projectId.value}/herd/discover/${agentId.value}/card`);
    card.value = res.data || {};
    agent.value = {
      id: agentId.value,
      name: card.value.name,
      description: card.value.description,
      visibility: 'public',
      needsAccess: false,
      organizationName: '',
    };

    // Also fetch from discover list to get metadata
    const discoverRes = await axios.get(`/api/projects/${projectId.value}/herd/discover`, { params: { q: card.value.name } });
    const match = (discoverRes.data || []).find(a => a.id === agentId.value);
    if (match) {
      agent.value = { ...match };
      accessRequested.value = match.accessPending || false;
      accessGranted.value = match.accessGranted || !match.needsAccess;
    }
  } catch (err) {
    if (err.response?.status === 404) {
      error.value = 'Agent not found or not accessible.';
    } else {
      error.value = 'Failed to load agent details.';
    }
  } finally {
    loading.value = false;
  }
};

const fetchProjectAgents = async () => {
  try {
    const res = await axios.get(`/api/projects/${projectId.value}/herd/agents`);
    projectAgents.value = res.data || [];
  } catch (err) {
    console.error('Failed to fetch project agents:', err);
  }
};

const openAccessModal = async () => {
  if (projectAgents.value.length === 0) await fetchProjectAgents();
  selectedSourceAgent.value = projectAgents.value.length === 1 ? projectAgents.value[0].id : '';
  showAccessModal.value = true;
};

const closeAccessModal = () => {
  showAccessModal.value = false;
  selectedSourceAgent.value = '';
};

const submitAccessRequest = async () => {
  if (!selectedSourceAgent.value) return;
  requestingAccess.value = true;
  try {
    const res = await axios.post(`/api/projects/${projectId.value}/herd/access/request`, {
      targetAgentId: agentId.value,
      grantedAgentId: selectedSourceAgent.value,
    });
    if (res.data?.status === 'approved') {
      accessGranted.value = true;
    } else {
      accessRequested.value = true;
    }
    closeAccessModal();
  } catch (err) {
    alert(err.response?.data || 'Failed to request access');
  } finally {
    requestingAccess.value = false;
  }
};

onMounted(async () => {
  await fetchUser();
  await fetchAgent();
});
</script>

<style scoped>
.spinner { animation: spin 1s linear infinite; }
@keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
</style>
