<template>
  <AppLayout :user="user" :current-project="null">
    <div class="max-w-[1200px] mx-auto px-8 py-6">
      <!-- Header -->
      <div class="mb-6">
        <div>
          <h1 class="text-2xl font-semibold text-gray-900">SSO Configuration</h1>
          <p class="text-sm text-gray-500 dark:text-gray-400 mt-1">
            Configure Single Sign-On for your organization
          </p>
        </div>
        <BaseButton variant="primary" @click="showCreateModal = true">
          <svg class="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" />
          </svg>
          Add SSO Provider
        </BaseButton>
      </div>

      <!-- SSO Status Card -->
      <BaseCard class="mb-6">
        <template #header>
          <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">SSO Status</h2>
        </template>
        <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
          <div class="text-center p-4 bg-gray-50 dark:bg-gray-800 rounded-lg">
            <div class="text-3xl font-bold text-primary-600">{{ configurations.length }}</div>
            <div class="text-sm text-gray-500 dark:text-gray-400">Configured Providers</div>
          </div>
          <div class="text-center p-4 bg-gray-50 dark:bg-gray-800 rounded-lg">
            <div class="text-3xl font-bold text-green-600">{{ enabledCount }}</div>
            <div class="text-sm text-gray-500 dark:text-gray-400">Active Providers</div>
          </div>
          <div class="text-center p-4 bg-gray-50 dark:bg-gray-800 rounded-lg">
            <div class="text-3xl font-bold text-blue-600">{{ domainCount }}</div>
            <div class="text-sm text-gray-500 dark:text-gray-400">Mapped Domains</div>
          </div>
        </div>
      </BaseCard>

      <!-- Configurations List -->
      <BaseCard>
        <template #header>
          <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">
            SSO Providers ({{ configurations.length }})
          </h2>
        </template>
        
        <div v-if="loading" class="text-center py-8 text-gray-500 dark:text-gray-400">
          <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full mx-auto mb-3"></div>
          <p>Loading configurations...</p>
        </div>
        
        <div v-else-if="configurations.length === 0" class="text-center py-12 text-gray-500 dark:text-gray-400">
          <svg class="w-12 h-12 mx-auto mb-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z" />
          </svg>
          <p class="text-lg font-medium mb-2">No SSO providers configured</p>
          <p class="text-sm mb-4">Add an identity provider to enable Single Sign-On</p>
          <BaseButton variant="primary" @click="showCreateModal = true">
            Add SSO Provider
          </BaseButton>
        </div>
        
        <div v-else class="space-y-4">
          <div
            v-for="config in configurations"
            :key="config.id"
            class="border border-gray-200 dark:border-gray-700 rounded-lg p-4 hover:border-primary-500 dark:hover:border-primary-500 transition-colors"
          >
            <div class="flex items-start justify-between">
              <div class="flex-1">
                <div class="flex items-center gap-3 mb-2">
                  <div class="w-10 h-10 rounded-lg bg-primary-100 dark:bg-primary-900 flex items-center justify-center">
                    <span class="text-primary-600 dark:text-primary-400 font-bold text-sm">
                      {{ getProviderInitials(config.provider) }}
                    </span>
                  </div>
                  <div>
                    <h3 class="text-base font-medium text-gray-900 dark:text-gray-100">
                      {{ config.name }}
                    </h3>
                    <p class="text-sm text-gray-500 dark:text-gray-400">
                      {{ getProviderLabel(config.provider) }} - {{ config.sso_type.toUpperCase() }}
                    </p>
                  </div>
                  <span
                    class="px-2 py-1 text-xs font-medium rounded"
                    :class="config.enabled ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200' : 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300'"
                  >
                    {{ config.enabled ? 'Active' : 'Disabled' }}
                  </span>
                </div>
                
                <div class="ml-13 text-sm text-gray-600 dark:text-gray-400 space-y-1">
                  <p v-if="config.domain_name">
                    <span class="font-medium">Domain:</span> {{ config.domain_name }}
                  </p>
                  <p v-if="config.issuer_url">
                    <span class="font-medium">Issuer:</span> {{ config.issuer_url }}
                  </p>
                  <p v-if="config.saml_entity_id">
                    <span class="font-medium">Entity ID:</span> {{ config.saml_entity_id }}
                  </p>
                  <p>
                    <span class="font-medium">Auto-create users:</span> 
                    {{ config.auto_create_users ? 'Yes' : 'No' }}
                  </p>
                </div>
              </div>
              
              <div class="flex items-center gap-2">
                <button
                  @click="testConfiguration(config)"
                  class="p-2 text-gray-400 hover:text-blue-600 dark:hover:text-blue-400 transition-colors"
                  title="Test Connection"
                >
                  <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                </button>
                <button
                  @click="editConfiguration(config)"
                  class="p-2 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 transition-colors"
                  title="Edit"
                >
                  <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z" />
                  </svg>
                </button>
                <button
                  @click="toggleConfiguration(config)"
                  class="p-2 text-gray-400 hover:text-yellow-600 dark:hover:text-yellow-400 transition-colors"
                  :title="config.enabled ? 'Disable' : 'Enable'"
                >
                  <svg v-if="config.enabled" class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M18.364 18.364A9 9 0 005.636 5.636m12.728 12.728A9 9 0 015.636 5.636m12.728 12.728L5.636 5.636" />
                  </svg>
                  <svg v-else class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                </button>
                <button
                  @click="deleteConfiguration(config)"
                  class="p-2 text-gray-400 hover:text-red-600 dark:hover:text-red-400 transition-colors"
                  title="Delete"
                >
                  <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
                  </svg>
                </button>
              </div>
            </div>
          </div>
        </div>
      </BaseCard>

      <!-- Create/Edit Modal -->
      <SsoConfigModal
        v-if="showCreateModal || selectedConfig"
        :config="selectedConfig"
        @close="closeModal"
        @save="saveConfiguration"
      />
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue';
import axios from 'axios';
import AppLayout from '@/Layouts/AppLayout.vue';
import BaseCard from '@/components/BaseCard.vue';
import BaseButton from '@/components/BaseButton.vue';
import SsoConfigModal from '@/components/sso/SsoConfigModal.vue';
import { useAuth } from '@/composables/useAuth';

const { user } = useAuth();

const loading = ref(false);
const configurations = ref([]);
const showCreateModal = ref(false);
const selectedConfig = ref(null);

const enabledCount = computed(() => configurations.value.filter(c => c.enabled).length);
const domainCount = computed(() => configurations.value.filter(c => c.domain_name).length);

const providerLabels = {
  okta: 'Okta',
  auth0: 'Auth0',
  entra_id: 'Microsoft Entra ID',
  onelogin: 'OneLogin',
  ping: 'Ping Identity',
  keycloak: 'Keycloak',
  google: 'Google Workspace',
  custom: 'Custom OIDC/SAML',
};

const getProviderLabel = (provider) => providerLabels[provider] || provider;
const getProviderInitials = (provider) => {
  const label = providerLabels[provider] || provider;
  return label.split(' ').map(w => w[0]).join('').slice(0, 2).toUpperCase();
};

const fetchConfigurations = async () => {
  loading.value = true;
  try {
    const response = await axios.get('/api/sso/configurations');
    configurations.value = response.data;
  } catch (error) {
    console.error('Failed to fetch SSO configurations:', error);
  } finally {
    loading.value = false;
  }
};

const editConfiguration = (config) => {
  selectedConfig.value = { ...config };
};

const closeModal = () => {
  showCreateModal.value = false;
  selectedConfig.value = null;
};

const saveConfiguration = async (configData) => {
  try {
    if (selectedConfig.value?.id) {
      await axios.put(`/api/sso/configurations/${selectedConfig.value.id}`, configData);
    } else {
      await axios.post('/api/sso/configurations', configData);
    }
    await fetchConfigurations();
    closeModal();
  } catch (error) {
    console.error('Failed to save SSO configuration:', error);
    alert('Failed to save: ' + (error.response?.data?.message || error.message));
  }
};

const toggleConfiguration = async (config) => {
  try {
    await axios.put(`/api/sso/configurations/${config.id}`, {
      enabled: !config.enabled,
    });
    await fetchConfigurations();
  } catch (error) {
    console.error('Failed to toggle SSO configuration:', error);
  }
};

const deleteConfiguration = async (config) => {
  if (!confirm(`Are you sure you want to delete the SSO configuration "${config.name}"? This cannot be undone.`)) {
    return;
  }
  try {
    await axios.delete(`/api/sso/configurations/${config.id}`);
    await fetchConfigurations();
  } catch (error) {
    console.error('Failed to delete SSO configuration:', error);
  }
};

const testConfiguration = async (config) => {
  try {
    // Open a new window to test SSO login
    const testUrl = config.sso_type === 'saml' 
      ? `/api/sso/login/saml/${config.id}`
      : `/api/sso/login/oidc/${config.id}`;
    window.open(testUrl, 'sso-test', 'width=500,height=600');
  } catch (error) {
    console.error('Failed to test SSO configuration:', error);
    alert('Test failed: ' + error.message);
  }
};

onMounted(() => {
  fetchConfigurations();
});
</script>

<style scoped>
.spinner {
  animation: spin 1s linear infinite;
}

@keyframes spin {
  from { transform: rotate(0deg); }
  to { transform: rotate(360deg); }
}

.ml-13 {
  margin-left: 52px;
}
</style>
