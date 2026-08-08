<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-[1200px] mx-auto px-8 py-6">
      <!-- Header -->
      <div class="mb-6">
        <div>
          <h1 class="text-2xl font-semibold text-gray-900">LLM Integrations</h1>
          <p class="text-sm text-gray-500 dark:text-gray-400 mt-1">Connect AI providers to power your Prompt Hub</p>
        </div>
      </div>

      <!-- Error Message -->
      <div v-if="errorMessage" class="mb-6 p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg flex items-center justify-between">
        <div class="flex items-center gap-3">
          <svg class="w-5 h-5 text-red-600 dark:text-red-400 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
          <span class="text-sm text-red-700 dark:text-red-300">{{ errorMessage }}</span>
        </div>
        <button @click="errorMessage = ''" class="text-red-600 dark:text-red-400 hover:text-red-800 dark:hover:text-red-300">
          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      </div>

      <!-- Search Bar -->
      <div class="mb-6">
        <div class="relative">
          <input
            v-model="searchQuery"
            type="text"
            placeholder="Search AI providers..."
            class="w-full px-4 py-3 pl-10 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent text-gray-900 dark:text-gray-100"
          />
          <svg
            class="absolute left-3 top-1/2 transform -translate-y-1/2 w-5 h-5 text-gray-400"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
          </svg>
        </div>
      </div>

      <!-- Available Providers -->
      <BaseCard class="mb-6">
        <template #header>
          <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">Available Providers</h2>
        </template>
        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          <div
            v-for="provider in filteredProviders"
            :key="provider.id"
            class="border border-gray-200 dark:border-gray-700 rounded-lg p-4 hover:border-primary-500 dark:hover:border-primary-500 transition-colors"
          >
            <div class="flex items-start justify-between mb-3">
              <div class="flex-1">
                <div class="flex items-center gap-2 mb-1">
                  <h3 class="text-base font-medium text-gray-900 dark:text-gray-100">
                    {{ provider.name }}
                  </h3>
                  <span v-if="provider.comingSoon" class="px-2 py-0.5 text-xs font-medium rounded bg-gray-100 text-gray-600 dark:bg-gray-700 dark:text-gray-400">
                    Coming Soon
                  </span>
                </div>
                <p class="text-sm text-gray-600 dark:text-gray-400">
                  {{ provider.description }}
                </p>
              </div>
              <span class="px-2 py-1 text-xs font-medium rounded bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200">
                ai-provider
              </span>
            </div>
            <div class="text-xs text-gray-500 dark:text-gray-500 mb-3">
              <template v-if="provider.models.length > 0">
                Models: {{ provider.models.slice(0, 3).join(', ') }}{{ provider.models.length > 3 ? '...' : '' }}
              </template>
              <template v-else>
                Models: User-configured
              </template>
            </div>
            <BaseButton
              v-if="!provider.comingSoon"
              variant="primary"
              size="sm"
              class="w-full"
              @click="openConfigModal(provider)"
              :disabled="isProviderConfigured(provider.id)"
            >
              {{ isProviderConfigured(provider.id) ? 'Configured' : 'Add Integration' }}
            </BaseButton>
            <BaseButton
              v-else
              variant="outline"
              size="sm"
              class="w-full"
              disabled
            >
              Coming Soon
            </BaseButton>
          </div>
        </div>
      </BaseCard>

      <!-- Configured Providers -->
      <BaseCard>
        <template #header>
          <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">
            Configured Providers ({{ configuredProviders.length }})
          </h2>
        </template>
        <div v-if="loading" class="text-center py-8 text-gray-500 dark:text-gray-400">
          <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full mx-auto mb-3"></div>
          <p>Loading integrations...</p>
        </div>
        <div v-else-if="configuredProviders.length === 0" class="text-center py-12 text-gray-500 dark:text-gray-400">
          <svg class="w-12 h-12 mx-auto mb-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1" />
          </svg>
          <p class="text-lg font-medium mb-2">No AI providers configured</p>
          <p class="text-sm">Add an integration above to start using the Prompt Hub</p>
        </div>
        <div v-else class="space-y-3">
          <div
            v-for="integration in configuredProviders"
            :key="integration.provider"
            class="border border-gray-200 dark:border-gray-700 rounded-lg p-4 hover:border-primary-500 dark:hover:border-primary-500 transition-colors"
          >
            <div class="flex items-start justify-between">
              <div class="flex-1">
                <div class="flex items-center gap-3 mb-2">
                  <h3 class="text-base font-medium text-gray-900 dark:text-gray-100">
                    {{ getProviderName(integration.provider) }}
                  </h3>
                  <span
                    class="px-2 py-1 text-xs font-medium rounded"
                    :class="integration.enabled ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200' : 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300'"
                  >
                    {{ integration.enabled ? 'Enabled' : 'Disabled' }}
                  </span>
                </div>
                <div class="text-sm text-gray-600 dark:text-gray-400 space-y-1">
                  <p>
                    <span class="font-medium">Status:</span>
                    <span :class="{
                      'text-green-600 dark:text-green-400': integration.last_test_status === 'success',
                      'text-red-600 dark:text-red-400': integration.last_test_status === 'failed',
                      'text-gray-500 dark:text-gray-500': integration.last_test_status === 'never'
                    }">
                      {{ formatTestStatus(integration) }}
                    </span>
                  </p>
                  <p><span class="font-medium">Models:</span> {{ getProviderModels(integration.provider) }}</p>
                </div>
              </div>
              <div class="flex items-center gap-2 ml-4">
                <button
                  @click="testConnection(integration)"
                  class="px-3 py-1.5 text-sm font-medium text-primary-600 hover:text-primary-700 dark:text-primary-400 dark:hover:text-primary-300 hover:bg-primary-50 dark:hover:bg-primary-900/20 rounded transition-colors"
                  :disabled="testing === integration.provider"
                >
                  {{ testing === integration.provider ? 'Testing...' : 'Test' }}
                </button>
                <button
                  @click="openConfigModal(availableProviders.find(p => p.id === integration.provider), integration)"
                  class="p-2 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 transition-colors"
                  title="Configure"
                >
                  <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
                  </svg>
                </button>
                <button
                  @click="toggleIntegration(integration)"
                  class="p-2 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 transition-colors"
                  :title="integration.enabled ? 'Disable' : 'Enable'"
                >
                  <svg v-if="integration.enabled" class="w-5 h-5" fill="currentColor" viewBox="0 0 20 20">
                    <path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clip-rule="evenodd" />
                  </svg>
                  <svg v-else class="w-5 h-5" fill="currentColor" viewBox="0 0 20 20">
                    <path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clip-rule="evenodd" />
                  </svg>
                </button>
                <button
                  @click="deleteIntegration(integration)"
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

      <!-- Configuration Modal -->
      <div v-if="showModal" class="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
        <div class="bg-white dark:bg-gray-800 rounded-lg shadow-xl max-w-md w-full mx-4">
          <div class="flex items-center justify-between p-4 border-b border-gray-200 dark:border-gray-700">
            <h3 class="text-lg font-semibold text-gray-900 dark:text-gray-100">
              Configure {{ selectedProvider?.name }}
            </h3>
            <button @click="closeModal" class="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300">
              <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>
          <div class="p-4 space-y-4">
            <!-- Standard API Key input (not shown for aws_credentials or theta_dedicated) -->
            <div v-if="selectedProvider?.authType !== 'aws_credentials' && selectedProvider?.authType !== 'theta_dedicated'">
              <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                API Key *
              </label>
              <input
                v-model="formData.apiKey"
                type="password"
                placeholder="Enter your API key"
                class="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-primary-500"
              />
              <p class="mt-1 text-xs text-gray-500 dark:text-gray-400">
                <a :href="selectedProvider?.docsUrl" target="_blank" class="text-primary-600 hover:text-primary-700 dark:text-primary-400">
                  Get your API key
                </a>
              </p>
            </div>

            <!-- Theta Dedicated fields -->
            <template v-if="selectedProvider?.authType === 'theta_dedicated'">
              <div>
                <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Deployment URL *
                </label>
                <input
                  v-model="formData.baseUrl"
                  type="text"
                  placeholder="https://your-deployment.tec-s1.onthetaedgecloud.com/v1"
                  class="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-primary-500"
                />
                <p class="mt-1 text-xs text-gray-500 dark:text-gray-400">
                  The OpenAI-compatible base URL of your vLLM deployment
                </p>
              </div>
              <div>
                <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  API Key (optional)
                </label>
                <input
                  v-model="formData.apiKey"
                  type="password"
                  placeholder="Enter API key if required"
                  class="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-primary-500"
                />
              </div>
              <p class="text-xs text-gray-500 dark:text-gray-400">
                <a :href="selectedProvider?.docsUrl" target="_blank" class="text-primary-600 hover:text-primary-700 dark:text-primary-400">
                  Theta EdgeCloud LLM Dashboard
                </a>
              </p>
            </template>

            <!-- AWS credential fields -->
            <template v-if="selectedProvider?.authType === 'aws_credentials'">
              <div>
                <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  AWS Access Key ID *
                </label>
                <input
                  v-model="formData.accessKeyId"
                  type="text"
                  placeholder="AKIA..."
                  class="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-primary-500"
                />
              </div>
              <div>
                <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  AWS Secret Access Key *
                </label>
                <input
                  v-model="formData.secretAccessKey"
                  type="password"
                  placeholder="Enter your secret access key"
                  class="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-primary-500"
                />
              </div>
              <div>
                <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Region *
                </label>
                <select
                  v-model="formData.region"
                  class="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-primary-500"
                >
                  <option value="us-east-1">us-east-1</option>
                  <option value="us-west-2">us-west-2</option>
                  <option value="eu-west-1">eu-west-1</option>
                  <option value="eu-central-1">eu-central-1</option>
                  <option value="ap-northeast-1">ap-northeast-1</option>
                  <option value="ap-southeast-1">ap-southeast-1</option>
                </select>
              </div>
              <p class="text-xs text-yellow-600 dark:text-yellow-400">
                Ensure your IAM user has bedrock:InvokeModel permission
              </p>
            </template>

            <div class="flex items-center">
              <input
                v-model="formData.enabled"
                type="checkbox"
                id="enabled"
                class="h-4 w-4 text-primary-600 focus:ring-primary-500 border-gray-300 rounded"
              />
              <label for="enabled" class="ml-2 text-sm text-gray-700 dark:text-gray-300">
                Enable this provider
              </label>
            </div>

            <!-- Test Connection -->
            <div class="border border-gray-200 dark:border-gray-700 rounded-lg p-3">
              <div class="flex items-center justify-between">
                <div>
                  <span class="text-sm font-medium text-gray-700 dark:text-gray-300">Test Connection</span>
                  <p v-if="testResult" class="text-xs mt-1" :class="{
                    'text-green-600 dark:text-green-400': testResult === 'success',
                    'text-red-600 dark:text-red-400': testResult === 'failed'
                  }">
                    {{ testResult === 'success' ? 'Connection successful' : 'Connection failed' }}
                  </p>
                </div>
                <BaseButton
                  variant="outline"
                  size="sm"
                  @click="testConnectionFromModal"
                  :disabled="testingModal"
                >
                  {{ testingModal ? 'Testing...' : 'Test' }}
                </BaseButton>
              </div>
            </div>
          </div>
          <div class="flex justify-end gap-3 p-4 border-t border-gray-200 dark:border-gray-700">
            <BaseButton variant="outline" @click="closeModal">Cancel</BaseButton>
            <BaseButton variant="primary" @click="saveIntegration" :loading="saving">
              Save & Encrypt Key
            </BaseButton>
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
import BaseButton from '@/components/BaseButton.vue';
import { useAuth } from '@/composables/useAuth';

const route = useRoute();
const { user, fetchUser } = useAuth();

const projectId = computed(() => route.params.id);
const project = computed(() => ({ id: projectId.value }));
const searchQuery = ref('');
const loading = ref(false);
const saving = ref(false);
const testing = ref(null);
const testingModal = ref(false);
const testResult = ref(null);
const configuredProviders = ref([]);
const showModal = ref(false);
const selectedProvider = ref(null);
const editingIntegration = ref(null);
const errorMessage = ref('');

// Helper to extract error message from response
const getErrorMessage = (error, fallback = 'An error occurred') => {
  if (error.response?.data?.error) return error.response.data.error;
  if (error.response?.data?.message) return error.response.data.message;
  if (error.message) return error.message;
  return fallback;
};

const formData = ref({
  apiKey: '',
  accessKeyId: '',
  secretAccessKey: '',
  region: 'us-east-1',
  baseUrl: '',
  enabled: true,
});

// Populated from the backend model catalog on mount.
const availableProviders = ref([]);

const filteredProviders = computed(() => {
  if (!searchQuery.value) {
    return availableProviders.value;
  }
  const query = searchQuery.value.toLowerCase();
  return availableProviders.value.filter(provider =>
    provider.name.toLowerCase().includes(query) ||
    provider.description.toLowerCase().includes(query) ||
    provider.models.some(m => m.toLowerCase().includes(query))
  );
});

const isProviderConfigured = (providerId) => {
  return configuredProviders.value.some(p => p.provider === providerId);
};

const getProviderName = (providerId) => {
  const provider = availableProviders.value.find(p => p.id === providerId);
  return provider?.name || providerId;
};

const getProviderModels = (providerId) => {
  const provider = availableProviders.value.find(p => p.id === providerId);
  return provider?.models.slice(0, 3).join(', ') || '';
};

const formatTestStatus = (integration) => {
  if (integration.last_test_status === 'success') {
    const date = integration.last_tested_at ? new Date(integration.last_tested_at).toLocaleString() : 'recently';
    return `Connected (tested ${date})`;
  } else if (integration.last_test_status === 'failed') {
    return 'Connection failed';
  }
  return 'Never tested';
};

const fetchIntegrations = async () => {
  loading.value = true;
  errorMessage.value = '';
  try {
    const response = await axios.get(`/api/projects/${projectId.value}/llm/integrations`);
    configuredProviders.value = response.data || [];
  } catch (error) {
    errorMessage.value = getErrorMessage(error, 'Failed to fetch integrations');
    configuredProviders.value = [];
  } finally {
    loading.value = false;
  }
};

const openConfigModal = (provider, existingIntegration = null) => {
  selectedProvider.value = provider;
  editingIntegration.value = existingIntegration;
  testResult.value = null;
  
  // Reset form
  formData.value = {
    apiKey: '',
    accessKeyId: '',
    secretAccessKey: '',
    region: 'us-east-1',
    baseUrl: '',
    enabled: existingIntegration?.enabled ?? true,
  };
  
  showModal.value = true;
};

const closeModal = () => {
  showModal.value = false;
  selectedProvider.value = null;
  editingIntegration.value = null;
  testResult.value = null;
};

const testConnectionFromModal = async () => {
  testingModal.value = true;
  testResult.value = null;
  
  try {
    let payload;
    if (selectedProvider.value.authType === 'aws_credentials') {
      payload = {
        provider: selectedProvider.value.id,
        access_key_id: formData.value.accessKeyId,
        secret_access_key: formData.value.secretAccessKey,
        region: formData.value.region,
      };
    } else if (selectedProvider.value.authType === 'theta_dedicated') {
      payload = {
        provider: selectedProvider.value.id,
        base_url: formData.value.baseUrl,
        api_key: formData.value.apiKey || undefined,
      };
    } else {
      payload = {
        provider: selectedProvider.value.id,
        api_key: formData.value.apiKey,
      };
    }
    
    await axios.post(`/api/projects/${projectId.value}/llm/integrations/${selectedProvider.value.id}/test`, payload);
    testResult.value = 'success';
  } catch (error) {
    testResult.value = 'failed';
  } finally {
    testingModal.value = false;
  }
};

const testConnection = async (integration) => {
  testing.value = integration.provider;
  errorMessage.value = '';
  try {
    await axios.post(`/api/projects/${projectId.value}/llm/integrations/${integration.provider}/test`, {});
    await fetchIntegrations();
  } catch (error) {
    errorMessage.value = getErrorMessage(error, 'Connection test failed');
  } finally {
    testing.value = null;
  }
};

const saveIntegration = async () => {
  saving.value = true;
  try {
    let payload;
    if (selectedProvider.value.authType === 'aws_credentials') {
      payload = {
        provider: selectedProvider.value.id,
        access_key_id: formData.value.accessKeyId,
        secret_access_key: formData.value.secretAccessKey,
        region: formData.value.region,
        enabled: formData.value.enabled,
      };
    } else if (selectedProvider.value.authType === 'theta_dedicated') {
      payload = {
        provider: selectedProvider.value.id,
        base_url: formData.value.baseUrl,
        api_key: formData.value.apiKey || undefined,
        enabled: formData.value.enabled,
      };
    } else {
      payload = {
        provider: selectedProvider.value.id,
        api_key: formData.value.apiKey,
        enabled: formData.value.enabled,
      };
    }
    
    if (editingIntegration.value) {
      await axios.put(`/api/projects/${projectId.value}/llm/integrations/${selectedProvider.value.id}`, payload);
    } else {
      await axios.post(`/api/projects/${projectId.value}/llm/integrations`, payload);
    }
    
    await fetchIntegrations();
    closeModal();
  } catch (error) {
    errorMessage.value = getErrorMessage(error, 'Failed to save integration');
  } finally {
    saving.value = false;
  }
};

const toggleIntegration = async (integration) => {
  errorMessage.value = '';
  try {
    await axios.put(`/api/projects/${projectId.value}/llm/integrations/${integration.provider}`, {
      enabled: !integration.enabled,
    });
    await fetchIntegrations();
  } catch (error) {
    errorMessage.value = getErrorMessage(error, 'Failed to toggle integration');
  }
};

const deleteIntegration = async (integration) => {
  if (!confirm(`Are you sure you want to delete the ${getProviderName(integration.provider)} integration?`)) {
    return;
  }
  errorMessage.value = '';
  try {
    await axios.delete(`/api/projects/${projectId.value}/llm/integrations/${integration.provider}`);
    await fetchIntegrations();
  } catch (error) {
    errorMessage.value = getErrorMessage(error, 'Failed to delete integration');
  }
};

const fetchModelCatalog = async () => {
  try {
    const { data } = await axios.get(`/api/projects/${projectId.value}/llm/models`);
    availableProviders.value = (data.providers || []).map(p => ({
      id: p.id,
      name: p.name,
      description: p.description,
      docsUrl: p.docs_url,
      authType: p.auth_type,
      supportsStreaming: p.supports_streaming,
      models: p.models.map(m => m.name),
    }));
  } catch (e) {
    console.warn('Failed to fetch model catalog', e);
  }
};

watch(projectId, () => {
  fetchIntegrations();
  fetchModelCatalog();
});

onMounted(async () => {
  await fetchUser();
  await Promise.all([fetchIntegrations(), fetchModelCatalog()]);
});
</script>

<style scoped>
.spinner {
  animation: spin 1s linear infinite;
}

@keyframes spin {
  from {
    transform: rotate(0deg);
  }
  to {
    transform: rotate(360deg);
  }
}
</style>
