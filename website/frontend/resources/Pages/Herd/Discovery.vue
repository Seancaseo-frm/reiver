<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-[1200px] mx-auto px-8 py-6">
      <div class="mb-6">
        <h1 class="text-2xl font-bold text-gray-900 dark:text-gray-100">Discover Agents</h1>
        <p class="text-sm text-gray-500 dark:text-gray-400 mt-1">Browse agents available in the Herd registry</p>
      </div>

      <!-- Search -->
      <div class="mb-6">
        <div class="relative">
          <svg class="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
          </svg>
          <input v-model="searchQuery" type="text" placeholder="Search agents by name or description..." class="w-full pl-10 pr-4 py-2.5 rounded-lg border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 shadow-sm focus:ring-blue-500 focus:border-blue-500 text-sm" @input="debouncedSearch" />
        </div>
      </div>

      <!-- Loading -->
      <div v-if="loading" class="flex items-center justify-center py-16">
        <svg class="spinner w-6 h-6 text-blue-500" fill="none" viewBox="0 0 24 24">
          <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
          <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"></path>
        </svg>
      </div>

      <!-- Empty State -->
      <BaseCard v-else-if="discoveredAgents.length === 0" class="!p-12 text-center">
        <svg class="mx-auto h-16 w-16 text-gray-400 dark:text-gray-500 mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
        </svg>
        <h3 class="text-lg font-medium text-gray-900 dark:text-gray-100 mb-2">No agents found</h3>
        <p class="text-sm text-gray-500 dark:text-gray-400">{{ searchQuery ? 'Try a different search query.' : 'No discoverable agents are available yet.' }}</p>
      </BaseCard>

      <!-- Agent Cards Grid -->
      <div v-else class="grid grid-cols-1 md:grid-cols-2 gap-4">
        <router-link v-for="agent in discoveredAgents" :key="agent.id" :to="`/p/${projectId}/herd/discovery/${agent.id}`" class="block">
          <BaseCard class="!p-5 hover:shadow-md transition-shadow cursor-pointer">
            <div class="flex items-start justify-between">
              <div class="flex-1 min-w-0">
                <div class="flex items-center space-x-2">
                  <h3 class="text-base font-semibold text-gray-900 dark:text-gray-100 truncate">{{ agent.name }}</h3>
                  <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium" :class="visibilityClass(agent.visibility)">{{ agent.visibility }}</span>
                </div>
                <p class="text-sm text-gray-500 dark:text-gray-400 mt-1 line-clamp-2">{{ agent.description || 'No description' }}</p>
                <p class="text-xs text-gray-400 dark:text-gray-500 mt-2">{{ agent.organizationName || 'Unknown org' }}</p>
              </div>
              <div class="ml-4 shrink-0" @click.prevent.stop>
                <span v-if="agent.accessGranted" class="inline-flex items-center px-2 py-1 rounded text-xs font-medium bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300">Accessible</span>
                <button v-else-if="agent.needsAccess" @click="openAccessModal(agent)" :disabled="agent.accessPending" class="inline-flex items-center px-3 py-1.5 border border-blue-600 dark:border-blue-400 rounded-lg text-xs font-medium text-blue-600 dark:text-blue-400 hover:bg-blue-50 dark:hover:bg-blue-900/20 transition-colors disabled:opacity-50">
                  {{ agent.accessPending ? 'Requested' : 'Request Access' }}
                </button>
                <span v-else class="inline-flex items-center px-2 py-1 rounded text-xs font-medium bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300">Accessible</span>
              </div>
            </div>
          </BaseCard>
        </router-link>
      </div>
    </div>

    <!-- Agent Selection Modal -->
    <div v-if="showAccessModal" class="fixed inset-0 z-50 flex items-center justify-center">
      <div class="absolute inset-0 bg-black/50" @click="closeAccessModal"></div>
      <div class="relative bg-white dark:bg-gray-800 rounded-xl shadow-xl max-w-md w-full mx-4 p-6">
        <h3 class="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-2">Request Access</h3>
        <p class="text-sm text-gray-500 dark:text-gray-400 mb-4">
          Select which of your agents should be granted access to <span class="font-semibold">{{ accessModalTarget?.name }}</span>.
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
          <button @click="submitAccessRequest" :disabled="!selectedSourceAgent || submittingAccess" class="px-4 py-2 text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 rounded-lg transition-colors disabled:opacity-50">
            {{ submittingAccess ? 'Requesting...' : 'Request Access' }}
          </button>
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
const searchQuery = ref('');
const discoveredAgents = ref([]);
const projectAgents = ref([]);

const showAccessModal = ref(false);
const accessModalTarget = ref(null);
const selectedSourceAgent = ref('');
const submittingAccess = ref(false);

let debounceTimer = null;
const debouncedSearch = () => {
  clearTimeout(debounceTimer);
  debounceTimer = setTimeout(() => fetchDiscovery(), 300);
};

const visibilityClass = (v) => {
  const map = {
    org: 'bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300',
    public: 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300',
  };
  return map[v] || 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300';
};

const fetchDiscovery = async () => {
  loading.value = true;
  try {
    const params = {};
    if (searchQuery.value) params.q = searchQuery.value;
    const res = await axios.get(`/api/projects/${projectId.value}/herd/discover`, { params });
    discoveredAgents.value = res.data || [];
  } catch (error) {
    console.error('Failed to fetch discovery:', error);
  } finally {
    loading.value = false;
  }
};

const fetchProjectAgents = async () => {
  try {
    const res = await axios.get(`/api/projects/${projectId.value}/herd/agents`);
    projectAgents.value = res.data || [];
  } catch (error) {
    console.error('Failed to fetch project agents:', error);
  }
};

const openAccessModal = (agent) => {
  accessModalTarget.value = agent;
  selectedSourceAgent.value = projectAgents.value.length === 1 ? projectAgents.value[0].id : '';
  showAccessModal.value = true;
};

const closeAccessModal = () => {
  showAccessModal.value = false;
  accessModalTarget.value = null;
  selectedSourceAgent.value = '';
};

const submitAccessRequest = async () => {
  if (!selectedSourceAgent.value || !accessModalTarget.value) return;
  submittingAccess.value = true;
  try {
    await axios.post(`/api/projects/${projectId.value}/herd/access/request`, {
      targetAgentId: accessModalTarget.value.id,
      grantedAgentId: selectedSourceAgent.value,
    });
    accessModalTarget.value.accessPending = true;
    closeAccessModal();
  } catch (error) {
    console.error('Failed to request access:', error);
    alert(error.response?.data || 'Failed to request access');
  } finally {
    submittingAccess.value = false;
  }
};

onMounted(async () => {
  await fetchUser();
  await Promise.all([fetchDiscovery(), fetchProjectAgents()]);
});

watch(projectId, async () => {
  await fetchUser();
  await Promise.all([fetchDiscovery(), fetchProjectAgents()]);
});
</script>

<style scoped>
.spinner { animation: spin 1s linear infinite; }
@keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
.line-clamp-2 { display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }
</style>
