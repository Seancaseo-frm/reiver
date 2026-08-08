<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <!-- Header -->
      <div class="flex justify-between items-center mb-8">
        <div>
          <h1 class="text-3xl font-bold text-gray-900 dark:text-gray-100">Prompts</h1>
          <p class="mt-2 text-gray-600 dark:text-gray-400">Manage prompts and progressive rollouts for your LLM application</p>
        </div>
        <button
          @click="showCreateModal = true"
          class="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
        >
          <svg class="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" />
          </svg>
          New Prompt
        </button>
      </div>

      <!-- Loading state -->
      <div v-if="loading" class="flex justify-center py-12">
        <div class="animate-spin rounded-full h-8 w-8 border-b-2 border-primary-600"></div>
      </div>

      <!-- Empty state -->
      <div v-else-if="configs.length === 0" class="text-center py-12 bg-white dark:bg-gray-800 rounded-lg shadow">
        <svg class="mx-auto h-12 w-12 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
        </svg>
        <h3 class="mt-2 text-sm font-medium text-gray-900 dark:text-gray-100">No prompts</h3>
        <p class="mt-1 text-sm text-gray-500 dark:text-gray-400">Get started by creating a new prompt.</p>
        <div class="mt-6">
          <button
            @click="showCreateModal = true"
            class="inline-flex items-center px-4 py-2 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-primary-600 hover:bg-primary-700"
          >
            <svg class="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" />
            </svg>
            New Prompt
          </button>
        </div>
      </div>

      <!-- Configs table -->
      <div v-else class="bg-white dark:bg-gray-800 shadow overflow-hidden sm:rounded-lg">
        <table class="min-w-full table-fixed divide-y divide-gray-200 dark:divide-gray-700">
          <thead class="bg-gray-50 dark:bg-gray-700">
            <tr>
              <th scope="col" class="w-2/5 px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">Name</th>
              <th scope="col" class="w-28 px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">Active Version</th>
              <th scope="col" class="w-24 px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">Versions</th>
              <th scope="col" class="w-24 px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">Rollout</th>
              <th scope="col" class="w-28 px-6 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">Updated</th>
              <th scope="col" class="w-32 relative px-6 py-3"><span class="sr-only">Actions</span></th>
            </tr>
          </thead>
          <tbody class="bg-white dark:bg-gray-800 divide-y divide-gray-200 dark:divide-gray-700">
            <tr
              v-for="config in configs"
              :key="config.id"
              class="hover:bg-gray-50 dark:hover:bg-gray-700 cursor-pointer"
              @click="$router.push(`/p/${projectId}/llm/prompts/${config.id}`)"
            >
              <td class="px-6 py-4 max-w-md">
                <div class="flex items-center">
                  <div class="flex-shrink-0 h-10 w-10 bg-primary-100 dark:bg-primary-900/30 rounded-full flex items-center justify-center">
                    <svg class="w-5 h-5 text-primary-600 dark:text-primary-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                    </svg>
                  </div>
                  <div class="ml-4 min-w-0">
                    <div class="flex items-center gap-1.5">
                      <span class="text-sm font-medium text-gray-900 dark:text-gray-100 whitespace-nowrap">{{ config.name }}</span>
                      <button
                        @click.stop="copyName(config.name)"
                        class="flex-shrink-0 p-0.5 rounded text-gray-400 hover:text-gray-600 dark:hover:text-gray-300"
                        title="Copy prompt name"
                      >
                        <svg v-if="copiedName !== config.name" class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
                        </svg>
                        <svg v-else class="w-3.5 h-3.5 text-green-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
                        </svg>
                      </button>
                    </div>
                    <div class="text-sm text-gray-500 dark:text-gray-400 truncate">{{ config.description || 'No description' }}</div>
                  </div>
                </div>
              </td>
              <td class="px-6 py-4 whitespace-nowrap">
                <span v-if="config.active_version" class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400">
                  v{{ config.active_version }}
                </span>
                <span v-else class="text-gray-400 text-sm">None</span>
              </td>
              <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500 dark:text-gray-400">
                {{ config.version_count }} version{{ config.version_count !== 1 ? 's' : '' }}
              </td>
              <td class="px-6 py-4 whitespace-nowrap">
                <span v-if="config.has_active_rollout" class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-400">
                  <span class="w-2 h-2 mr-1 bg-yellow-400 rounded-full animate-pulse"></span>
                  Rolling out
                </span>
                <span v-else class="text-gray-400 text-sm">-</span>
              </td>
              <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500 dark:text-gray-400">
                {{ formatDate(config.updated_at) }}
              </td>
              <td class="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                <button @click.stop="editConfig(config)" class="text-primary-600 hover:text-primary-900 dark:text-primary-400 dark:hover:text-primary-300 mr-3">Edit</button>
                <button @click.stop="confirmDelete(config)" class="text-red-600 hover:text-red-900 dark:text-red-400 dark:hover:text-red-300">Delete</button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <!-- Create Modal -->
      <div v-if="showCreateModal" class="fixed inset-0 z-50 overflow-y-auto" aria-labelledby="modal-title" role="dialog" aria-modal="true">
        <div class="flex items-end justify-center min-h-screen pt-4 px-4 pb-20 text-center sm:block sm:p-0">
          <div class="fixed inset-0 bg-gray-500 dark:bg-gray-900 bg-opacity-75 dark:bg-opacity-75 transition-opacity" @click="showCreateModal = false"></div>
          <span class="hidden sm:inline-block sm:align-middle sm:h-screen">&#8203;</span>
          <div class="inline-block align-bottom bg-white dark:bg-gray-800 rounded-lg px-4 pt-5 pb-4 text-left overflow-hidden shadow-xl transform transition-all sm:my-8 sm:align-middle sm:max-w-lg sm:w-full sm:p-6">
            <div>
              <h3 class="text-lg leading-6 font-medium text-gray-900 dark:text-gray-100" id="modal-title">
                {{ editingConfig ? 'Edit Prompt' : 'Create Prompt' }}
              </h3>
              <div class="mt-4">
                <label for="name" class="block text-sm font-medium text-gray-700 dark:text-gray-300">Name</label>
                <input
                  v-model="form.name"
                  type="text"
                  id="name"
                  class="mt-1 block w-full border border-gray-300 dark:border-gray-600 rounded-md shadow-sm py-2 px-3 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
                  placeholder="e.g., customer-support"
                />
              </div>
              <div class="mt-4">
                <label for="description" class="block text-sm font-medium text-gray-700 dark:text-gray-300">Description</label>
                <textarea
                  v-model="form.description"
                  id="description"
                  rows="3"
                  class="mt-1 block w-full border border-gray-300 dark:border-gray-600 rounded-md shadow-sm py-2 px-3 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
                  placeholder="Optional description"
                ></textarea>
              </div>
            </div>
            <div class="mt-5 sm:mt-6 sm:grid sm:grid-cols-2 sm:gap-3 sm:grid-flow-row-dense">
              <button
                @click="saveConfig"
                :disabled="saving"
                class="w-full inline-flex justify-center rounded-md border border-transparent shadow-sm px-4 py-2 bg-primary-600 text-base font-medium text-white hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 sm:col-start-2 sm:text-sm disabled:opacity-50"
              >
                {{ saving ? 'Saving...' : (editingConfig ? 'Save' : 'Create') }}
              </button>
              <button
                @click="showCreateModal = false"
                class="mt-3 w-full inline-flex justify-center rounded-md border border-gray-300 dark:border-gray-600 shadow-sm px-4 py-2 bg-white dark:bg-gray-700 text-base font-medium text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-600 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 sm:mt-0 sm:col-start-1 sm:text-sm"
              >
                Cancel
              </button>
            </div>
          </div>
        </div>
      </div>

      <!-- Delete Confirmation Modal -->
      <div v-if="showDeleteModal" class="fixed inset-0 z-50 overflow-y-auto" aria-labelledby="modal-title" role="dialog" aria-modal="true">
        <div class="flex items-end justify-center min-h-screen pt-4 px-4 pb-20 text-center sm:block sm:p-0">
          <div class="fixed inset-0 bg-gray-500 dark:bg-gray-900 bg-opacity-75 dark:bg-opacity-75 transition-opacity" @click="showDeleteModal = false"></div>
          <span class="hidden sm:inline-block sm:align-middle sm:h-screen">&#8203;</span>
          <div class="inline-block align-bottom bg-white dark:bg-gray-800 rounded-lg px-4 pt-5 pb-4 text-left overflow-hidden shadow-xl transform transition-all sm:my-8 sm:align-middle sm:max-w-lg sm:w-full sm:p-6">
            <div class="sm:flex sm:items-start">
              <div class="mx-auto flex-shrink-0 flex items-center justify-center h-12 w-12 rounded-full bg-red-100 dark:bg-red-900/30 sm:mx-0 sm:h-10 sm:w-10">
                <svg class="h-6 w-6 text-red-600 dark:text-red-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                </svg>
              </div>
              <div class="mt-3 text-center sm:mt-0 sm:ml-4 sm:text-left">
                <h3 class="text-lg leading-6 font-medium text-gray-900 dark:text-gray-100">Delete Prompt</h3>
                <div class="mt-2">
                  <p class="text-sm text-gray-500 dark:text-gray-400">
                    Are you sure you want to delete "{{ deletingConfig?.name }}"? This will also delete all versions and rollout history. This action cannot be undone.
                  </p>
                </div>
              </div>
            </div>
            <div class="mt-5 sm:mt-4 sm:flex sm:flex-row-reverse">
              <button
                @click="deleteConfig"
                :disabled="deleting"
                class="w-full inline-flex justify-center rounded-md border border-transparent shadow-sm px-4 py-2 bg-red-600 text-base font-medium text-white hover:bg-red-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-red-500 sm:ml-3 sm:w-auto sm:text-sm disabled:opacity-50"
              >
                {{ deleting ? 'Deleting...' : 'Delete' }}
              </button>
              <button
                @click="showDeleteModal = false"
                class="mt-3 w-full inline-flex justify-center rounded-md border border-gray-300 dark:border-gray-600 shadow-sm px-4 py-2 bg-white dark:bg-gray-700 text-base font-medium text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-600 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 sm:mt-0 sm:w-auto sm:text-sm"
              >
                Cancel
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  </AppLayout>
</template>

<script>
import { ref, onMounted, onUnmounted, computed, watch } from 'vue';
import { useRoute } from 'vue-router';
import axios from 'axios';
import AppLayout from '../../../Layouts/AppLayout.vue';
import { useAuth } from '../../../composables/useAuth';
import { usePageContext } from '../../../composables/usePageContext';

export default {
  components: { AppLayout },
  setup() {
    const route = useRoute();
    const { user } = useAuth();
    const projectId = computed(() => route.params.id);
    const project = computed(() => (projectId.value ? { id: projectId.value } : null));

    const configs = ref([]);
    const loading = ref(true);
    const showCreateModal = ref(false);
    const showDeleteModal = ref(false);
    const editingConfig = ref(null);
    const deletingConfig = ref(null);
    const saving = ref(false);
    const deleting = ref(false);
    const copiedName = ref(null);

    const copyName = async (name) => {
      try {
        await navigator.clipboard.writeText(name);
      } catch {
        const ta = document.createElement('textarea');
        ta.value = name;
        ta.style.position = 'fixed';
        ta.style.opacity = '0';
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        document.body.removeChild(ta);
      }
      copiedName.value = name;
      setTimeout(() => { copiedName.value = null; }, 2000);
    };

    const form = ref({
      name: '',
      description: '',
    });

    const fetchConfigs = async () => {
      loading.value = true;
      try {
        const response = await axios.get(`/api/llm/prompts/configs?project_id=${projectId.value}`);
        configs.value = response.data;
      } catch (error) {
        console.error('Failed to fetch configs:', error);
      } finally {
        loading.value = false;
      }
    };

    const saveConfig = async () => {
      saving.value = true;
      try {
        if (editingConfig.value) {
          await axios.put(`/api/llm/prompts/configs/${editingConfig.value.id}`, {
            project_id: projectId.value,
            name: form.value.name,
            description: form.value.description,
          });
        } else {
          await axios.post('/api/llm/prompts/configs', {
            project_id: projectId.value,
            name: form.value.name,
            description: form.value.description,
          });
        }
        showCreateModal.value = false;
        editingConfig.value = null;
        form.value = { name: '', description: '' };
        await fetchConfigs();
      } catch (error) {
        console.error('Failed to save config:', error);
        alert(error.response?.data?.message || 'Failed to save config');
      } finally {
        saving.value = false;
      }
    };

    const editConfig = (config) => {
      editingConfig.value = config;
      form.value = {
        name: config.name,
        description: config.description || '',
      };
      showCreateModal.value = true;
    };

    const confirmDelete = (config) => {
      deletingConfig.value = config;
      showDeleteModal.value = true;
    };

    const deleteConfig = async () => {
      deleting.value = true;
      try {
        await axios.delete(`/api/llm/prompts/configs/${deletingConfig.value.id}?project_id=${projectId.value}`);
        showDeleteModal.value = false;
        deletingConfig.value = null;
        await fetchConfigs();
      } catch (error) {
        console.error('Failed to delete config:', error);
        alert(error.response?.data?.message || 'Failed to delete config');
      } finally {
        deleting.value = false;
      }
    };

    const formatDate = (dateString) => {
      const date = new Date(dateString);
      const now = new Date();
      const diffMs = now - date;
      const diffMins = Math.floor(diffMs / 60000);
      const diffHours = Math.floor(diffMs / 3600000);
      const diffDays = Math.floor(diffMs / 86400000);

      if (diffMins < 60) return `${diffMins}m ago`;
      if (diffHours < 24) return `${diffHours}h ago`;
      if (diffDays < 7) return `${diffDays}d ago`;
      return date.toLocaleDateString();
    };

    const { setPageSnapshot, clearPageSnapshot } = usePageContext();

    watch(configs, (list) => {
      setPageSnapshot({
        page: 'Prompt Configs List',
        total: list.length,
        configs: list.map(c => ({
          id: c.id,
          name: c.name,
          description: c.description || undefined,
          active_version: c.active_version || undefined,
          version_count: c.version_count,
          has_active_rollout: c.has_active_rollout || false,
        })),
      });
    }, { deep: true });

    onMounted(fetchConfigs);
    watch(projectId, fetchConfigs);
    onUnmounted(() => clearPageSnapshot());

    return {
      user,
      project,
      projectId,
      configs,
      loading,
      showCreateModal,
      showDeleteModal,
      editingConfig,
      deletingConfig,
      saving,
      deleting,
      copiedName,
      form,
      copyName,
      saveConfig,
      editConfig,
      confirmDelete,
      deleteConfig,
      formatDate,
    };
  },
};
</script>
