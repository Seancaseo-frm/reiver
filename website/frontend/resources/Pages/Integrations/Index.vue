<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-[1200px] mx-auto px-8 py-6">
      <!-- Header -->
      <div class="mb-6">
        <div>
          <h1 class="text-2xl font-semibold text-gray-900">Integrations</h1>
          <p class="text-sm text-gray-500 dark:text-gray-400 mt-1">Connect and configure third-party services</p>
        </div>
      </div>

      <!-- Success banner (e.g. after GitHub install) -->
      <div v-if="successMessage" class="mb-6 bg-green-50 border border-green-200 rounded-lg p-4 flex items-center justify-between">
        <p class="text-sm text-green-700">{{ successMessage }}</p>
        <button @click="successMessage = ''" class="text-green-500 hover:text-green-700">
          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>
        </button>
      </div>

      <!-- Search Bar (Top) -->
      <div class="mb-6">
        <div class="relative">
          <input
            v-model="searchQuery"
            type="text"
            placeholder="Search integrations..."
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

      <!-- Available Integrations (Search Results) -->
      <BaseCard v-if="filteredAvailableIntegrations.length > 0" class="mb-6">
        <template #header>
          <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">Available Integrations</h2>
        </template>
        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          <div
            v-for="integration in filteredAvailableIntegrations"
            :key="integration.id"
            class="border border-gray-200 dark:border-gray-700 rounded-lg p-4 hover:border-primary-500 dark:hover:border-primary-500 transition-colors"
          >
            <div class="flex items-start justify-between mb-3">
              <div class="flex-1">
                <h3 class="text-base font-medium text-gray-900 dark:text-gray-100 mb-1">
                  {{ integration.name }}
                </h3>
                <p class="text-sm text-gray-600 dark:text-gray-400">
                  {{ integration.description }}
                </p>
              </div>
              <span
                class="px-2 py-1 text-xs font-medium rounded"
                :class="{
                  'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200': integration.category === 'cloud',
                  'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200': integration.category === 'database',
                  'bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200': integration.category === 'monitoring',
                  'bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200': integration.category === 'ci-cd',
                  'bg-pink-100 text-pink-800 dark:bg-pink-900 dark:text-pink-200': integration.category === 'identity',
                  'bg-cyan-100 text-cyan-800 dark:bg-cyan-900 dark:text-cyan-200': integration.category === 'synthetic',
                  'bg-teal-100 text-teal-800 dark:bg-teal-900 dark:text-teal-200': integration.category === 'collector',
                }"
              >
                {{ integration.category }}
              </span>
            </div>
            <BaseButton
              variant="primary"
              size="sm"
              class="w-full"
              @click="addIntegration(integration)"
              :disabled="isIntegrationAdded(integration.id)"
            >
              {{ isIntegrationAdded(integration.id) ? 'Added' : 'Add Integration' }}
            </BaseButton>
          </div>
        </div>
      </BaseCard>

      <!-- Current Project Integrations -->
      <BaseCard>
        <template #header>
          <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">
            Project Integrations ({{ projectIntegrations.length }})
          </h2>
        </template>
        <div v-if="loading" class="text-center py-8 text-gray-500 dark:text-gray-400">
          <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full mx-auto mb-3"></div>
          <p>Loading integrations...</p>
        </div>
        <div v-else-if="projectIntegrations.length === 0" class="text-center py-12 text-gray-500 dark:text-gray-400">
          <svg class="w-12 h-12 mx-auto mb-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 9l3 3-3 3m5 0h3M5 20h14a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
          </svg>
          <p class="text-lg font-medium mb-2">No integrations added yet</p>
          <p class="text-sm">Search and add integrations above to get started</p>
        </div>
        <div v-else class="space-y-3">
          <div
            v-for="integration in projectIntegrations"
            :key="integration.id"
            class="border border-gray-200 dark:border-gray-700 rounded-lg p-4 hover:border-primary-500 dark:hover:border-primary-500 transition-colors cursor-pointer"
            @click="openConfigModal(integration)"
          >
            <div class="flex items-start justify-between">
              <div class="flex-1">
                <div class="flex items-center gap-3 mb-2">
                  <h3 class="text-base font-medium text-gray-900 dark:text-gray-100">
                    {{ getIntegrationName(integration) }}
                  </h3>
                  <span
                    class="px-2 py-1 text-xs font-medium rounded"
                    :class="integration.enabled ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200' : 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300'"
                  >
                    {{ integration.enabled ? 'Enabled' : 'Disabled' }}
                  </span>
                </div>
                <div class="text-sm text-gray-600 dark:text-gray-400 space-y-1">
                  <p><span class="font-medium">Type:</span> {{ integration.integration_type || integration.check_type }}</p>
                  <!-- AWS info -->
                  <p v-if="integration.region"><span class="font-medium">Region:</span> {{ integration.region }}</p>
                  <p v-if="integration.role_arn"><span class="font-medium">Auth:</span> IAM Role</p>
                  <p v-else-if="integration.access_key_id"><span class="font-medium">Auth:</span> Access Keys</p>
                  <!-- Health Check info -->
                  <p v-if="integration.target_url"><span class="font-medium">URL:</span> {{ integration.target_url }}</p>
                  <p v-if="integration.target_host"><span class="font-medium">Host:</span> {{ integration.target_host }}:{{ integration.target_port }}</p>
                  <p v-if="integration.locations && integration.locations.length">
                    <span class="font-medium">Locations:</span> {{ integration.locations.join(', ') }}
                  </p>
                  <p v-if="integration.check_interval_seconds">
                    <span class="font-medium">Frequency:</span> {{ formatInterval(integration.check_interval_seconds) }}
                  </p>
                  <p v-if="integration.last_status">
                    <span class="font-medium">Status:</span>
                    <span :class="{
                      'text-green-600 dark:text-green-400': integration.last_status === 'healthy',
                      'text-red-600 dark:text-red-400': integration.last_status === 'unhealthy',
                      'text-yellow-600 dark:text-yellow-400': integration.last_status === 'unknown'
                    }">
                      {{ integration.last_status }}
                    </span>
                  </p>
                  <!-- Auth events info -->
                  <p v-if="integration.provider"><span class="font-medium">Provider:</span> {{ integration.provider }}</p>
                  <p v-if="integration.domain"><span class="font-medium">Domain:</span> {{ integration.domain }}</p>
                </div>
              </div>
              <div class="flex items-center gap-2 ml-4">
                <button
                  @click.stop="toggleIntegration(integration)"
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
                  @click.stop="deleteIntegration(integration)"
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

      <!-- GitHub Repository Linking -->
      <BaseCard v-if="githubInstallations.length > 0" class="mt-6">
        <template #header>
          <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">GitHub Repository</h2>
        </template>
        <div v-if="linkedRepo" class="flex items-center justify-between p-4">
          <div class="flex items-center gap-3">
            <svg class="w-5 h-5 text-gray-600" viewBox="0 0 16 16" fill="currentColor"><path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"/></svg>
            <div>
              <p class="text-sm font-medium text-gray-900 dark:text-gray-100">{{ linkedRepo }}</p>
              <p class="text-xs text-gray-500 dark:text-gray-400">Linked to this project</p>
            </div>
          </div>
          <button
            @click="unlinkRepo"
            class="px-3 py-1.5 text-sm text-red-600 hover:text-red-800 border border-red-200 hover:border-red-300 rounded-lg transition-colors"
            :disabled="repoLinking"
          >
            Unlink
          </button>
        </div>
        <div v-else class="p-4">
          <p class="text-sm text-gray-600 dark:text-gray-400 mb-3">
            Link a repository to this project to see commits and PRs on errors.
          </p>
          <div class="flex items-end gap-3">
            <div class="flex-1">
              <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Repository</label>
              <select
                v-model="selectedRepoUrl"
                class="w-full px-3 py-2 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-primary-500"
              >
                <option value="" disabled>Select a repository...</option>
                <optgroup v-for="inst in githubInstallations" :key="inst.installation_id" :label="inst.account_login">
                  <option v-for="repo in inst.repositories" :key="repo.full_name" :value="repo.html_url || `https://github.com/${repo.full_name}`">
                    {{ repo.full_name }}{{ repo.private ? ' (private)' : '' }}
                  </option>
                </optgroup>
              </select>
            </div>
            <button
              @click="linkRepo"
              :disabled="!selectedRepoUrl || repoLinking"
              class="px-4 py-2 text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 rounded-lg disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
            >
              {{ repoLinking ? 'Linking...' : 'Link' }}
            </button>
          </div>
        </div>
      </BaseCard>

      <!-- Configuration Modal -->
      <IntegrationConfigModal
        v-if="selectedIntegration"
        :integration="selectedIntegration"
        :integration-type="selectedIntegrationType"
        :project-api-key="projectApiKey"
        @close="closeConfigModal"
        @save="saveIntegration"
      />
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import axios from 'axios';
import { resolveApiUrl } from '@/composables/projectResolver';
import AppLayout from '@/Layouts/AppLayout.vue';
import BaseCard from '@/components/BaseCard.vue';
import BaseButton from '@/components/BaseButton.vue';
import IntegrationConfigModal from '@/components/IntegrationConfigModal.vue';
import { useAuth } from '@/composables/useAuth';

const route = useRoute();
const router = useRouter();
const { user } = useAuth();

const projectId = computed(() => route.params.id);
const project = computed(() => ({ id: projectId.value }));
const searchQuery = ref('');
const loading = ref(false);
const projectIntegrations = ref([]);
const selectedIntegration = ref(null);
const selectedIntegrationType = ref(null);
const projectApiKey = ref('');
const successMessage = ref('');
const linkedRepo = ref(null);
const selectedRepoUrl = ref('');
const repoLinking = ref(false);
const githubInstallations = ref([]);

// Available integrations catalog (this would ideally come from an API)
const availableIntegrations = ref([
  {
    id: 'github',
    name: 'GitHub',
    description: 'Link errors to commits and PRs for root cause analysis',
    category: 'ci-cd',
    type: 'github',
  },
  {
    id: 'slack',
    name: 'Slack',
    description: 'Send alerts and chat with Moodeng in Slack channels',
    category: 'monitoring',
    type: 'slack',
  },
  {
    id: 'discord',
    name: 'Discord',
    description: 'Send error alerts to Discord channels',
    category: 'monitoring',
    type: 'discord',
  },
  // {
  //   id: 'aws_ec2',
  //   name: 'AWS EC2',
  //   description: 'Monitor EC2 instances, metrics, and status checks',
  //   category: 'cloud',
  //   type: 'ec2',
  // },
  // {
  //   id: 'aws_rds',
  //   name: 'AWS RDS',
  //   description: 'Monitor RDS database instances and performance',
  //   category: 'cloud',
  //   type: 'rds',
  // },
  // {
  //   id: 'aws_lambda',
  //   name: 'AWS Lambda',
  //   description: 'Monitor Lambda function invocations and performance',
  //   category: 'cloud',
  //   type: 'lambda',
  // },
  // {
  //   id: 'aws_s3',
  //   name: 'AWS S3',
  //   description: 'Monitor S3 bucket metrics and usage',
  //   category: 'cloud',
  //   type: 's3',
  // },
  // {
  //   id: 'postgresql',
  //   name: 'PostgreSQL',
  //   description: 'Monitor PostgreSQL database performance',
  //   category: 'database',
  //   type: 'postgresql',
  // },
  // {
  //   id: 'mysql',
  //   name: 'MySQL',
  //   description: 'Monitor MySQL database performance',
  //   category: 'database',
  //   type: 'mysql',
  // },
  // {
  //   id: 'pagerduty',
  //   name: 'PagerDuty',
  //   description: 'Send alerts to PagerDuty for incident management',
  //   category: 'monitoring',
  //   type: 'pagerduty',
  // },
  // {
  //   id: 'servicenow',
  //   name: 'ServiceNow',
  //   description: 'Create incidents in ServiceNow for error tracking',
  //   category: 'monitoring',
  //   type: 'servicenow',
  // },
  // {
  //   id: 'teams',
  //   name: 'Microsoft Teams',
  //   description: 'Send error alerts to Microsoft Teams channels',
  //   category: 'monitoring',
  //   type: 'teams',
  // },
  // {
  //   id: 'okta-events',
  //   name: 'Okta Auth Events',
  //   description: 'Ingest Okta System Log events for trace/error correlation',
  //   category: 'identity',
  //   type: 'auth_events_okta',
  // },
  // {
  //   id: 'auth0-events',
  //   name: 'Auth0 Auth Events',
  //   description: 'Ingest Auth0 log events for trace/error correlation',
  //   category: 'identity',
  //   type: 'auth_events_auth0',
  // },
  // {
  //   id: 'entra-events',
  //   name: 'Microsoft Entra ID Events',
  //   description: 'Ingest Entra ID (Azure AD) sign-in logs for correlation',
  //   category: 'identity',
  //   type: 'auth_events_entra_id',
  // },
  // {
  //   id: 'onelogin-events',
  //   name: 'OneLogin Auth Events',
  //   description: 'Ingest OneLogin events for trace/error correlation',
  //   category: 'identity',
  //   type: 'auth_events_onelogin',
  // },
  // {
  //   id: 'ping-events',
  //   name: 'Ping Identity Auth Events',
  //   description: 'Ingest PingOne audit activities for correlation',
  //   category: 'identity',
  //   type: 'auth_events_ping_identity',
  // },
  // {
  //   id: 'keycloak-events',
  //   name: 'Keycloak Auth Events',
  //   description: 'Ingest Keycloak realm events for correlation',
  //   category: 'identity',
  //   type: 'auth_events_keycloak',
  // },
  // {
  //   id: 'health_check_http',
  //   name: 'HTTP/HTTPS Health Check',
  //   description: 'Monitor endpoint uptime with HTTP requests, status & body assertions',
  //   category: 'synthetic',
  //   type: 'health_check_http',
  // },
  // {
  //   id: 'health_check_tcp',
  //   name: 'TCP Health Check',
  //   description: 'Monitor TCP port connectivity and response patterns',
  //   category: 'synthetic',
  //   type: 'health_check_tcp',
  // },
  // {
  //   id: 'health_check_ssl',
  //   name: 'SSL Certificate Monitor',
  //   description: 'Monitor SSL/TLS certificate expiry and validity',
  //   category: 'synthetic',
  //   type: 'health_check_ssl',
  // },
  // {
  //   id: 'grafana_alloy',
  //   name: 'Grafana Alloy',
  //   description: 'Send logs from Grafana Alloy to Reiver',
  //   category: 'collector',
  //   type: 'collector_alloy',
  // },
]);

const filteredAvailableIntegrations = computed(() => {
  if (!searchQuery.value) {
    // Show all available integrations when no search query
    return availableIntegrations.value;
  }
  const query = searchQuery.value.toLowerCase();
  return availableIntegrations.value.filter(integration =>
    integration.name.toLowerCase().includes(query) ||
    integration.description.toLowerCase().includes(query) ||
    integration.category.toLowerCase().includes(query)
  );
});

const fetchProjectIntegrations = async () => {
  if (!projectId.value) return;
  
  loading.value = true;
  try {
    const [githubResponse, slackResponse, discordResponse] = await Promise.all([
      axios.get(`/api/github/installations`).catch(() => ({ data: { data: [] } })),
      axios.get(`/api/slack/integrations`, {
        params: { project_id: projectId.value }
      }).catch(() => ({ data: [] })),
      axios.get(`/api/discord/integrations`, {
        params: { project_id: projectId.value }
      }).catch(() => ({ data: [] })),
      // axios.get(`/api/aws/integrations`, { params: { project_id: projectId.value } }).catch(() => ({ data: [] })),
      // axios.get(`/api/pagerduty/integrations`, { params: { project_id: projectId.value } }).catch(() => ({ data: [] })),
      // axios.get(`/api/servicenow/integrations`, { params: { project_id: projectId.value } }).catch(() => ({ data: [] })),
      // axios.get(`/api/teams/integrations`, { params: { project_id: projectId.value } }).catch(() => ({ data: [] })),
      // axios.get(`/api/auth-events/integrations`, { params: { project_id: projectId.value } }).catch(() => ({ data: [] })),
      // axios.get(`/api/health-checks/checks`, { params: { project_id: projectId.value } }).catch(() => ({ data: [] })),
    ]);
    
    const githubInstallations = githubResponse.data?.data || githubResponse.data || [];
    
    projectIntegrations.value = [
      ...githubInstallations.map(i => ({ 
        ...i, 
        integration_type: 'github',
        name: `GitHub: ${i.account_login}`,
        enabled: true 
      })),
      ...(slackResponse.data || []).map(i => ({
        ...i,
        integration_type: 'slack',
        name: i.name || `Slack — ${i.team_name || 'Unknown'}`,
      })),
      ...(discordResponse.data || []),
      // ...(awsResponse.data || []),
      // ...(pagerdutyResponse.data || []),
      // ...(servicenowResponse.data || []),
      // ...(teamsResponse.data || []),
      // ...(authEventsResponse.data || []).map(i => ({ ...i, integration_type: `auth_events_${i.provider}` })),
      // ...(healthChecksResponse.data || []).map(i => ({ ...i, integration_type: `health_check_${i.check_type}` })),
    ];
  } catch (error) {
    console.error('Failed to fetch integrations:', error);
    // Handle error (show toast notification, etc.)
  } finally {
    loading.value = false;
  }
};

const isIntegrationAdded = (integrationId) => {
  // Check if integration type is already added
  const catalogItem = availableIntegrations.value.find(i => i.id === integrationId);
  if (!catalogItem) return false;
  
  const type = catalogItem.type;
  // For health checks, allow multiple (they're not exclusive)
  if (type.startsWith('health_check_')) {
    return false; // Always allow adding more health checks
  }
  return projectIntegrations.value.some(i => i.integration_type === type);
};

const getIntegrationName = (integration) => {
  const catalogItem = availableIntegrations.value.find(i => i.type === integration.integration_type);
  return catalogItem?.name || integration.name || integration.integration_type;
};

const addIntegration = (integration) => {
  // GitHub uses OAuth flow, not modal
  if (integration.type === 'github') {
    startGitHubInstall();
    return;
  }

  // Slack uses OAuth flow ("Add to Slack"), not modal
  if (integration.type === 'slack') {
    startSlackInstall();
    return;
  }
  
  // Open configuration modal for new integration
  selectedIntegrationType.value = integration.type;
  selectedIntegration.value = {
    integration_type: integration.type,
    name: integration.name,
    region: 'us-east-1',
    enabled: true,
  };
};

const openConfigModal = (integration) => {
  selectedIntegration.value = { ...integration };
  selectedIntegrationType.value = integration.integration_type;
};

const closeConfigModal = () => {
  selectedIntegration.value = null;
  selectedIntegrationType.value = null;
};

const getIntegrationApiPath = (integrationType) => {
  if (integrationType === 'github') {
    return '/api/github/installations';
  }
  if (integrationType === 'slack') {
    return '/api/slack/integrations';
  }
  if (integrationType === 'discord') {
    return '/api/discord/integrations';
  }
  // if (integrationType === 'pagerduty') {
  //   return '/api/pagerduty/integrations';
  // }
  // if (integrationType === 'servicenow') {
  //   return '/api/servicenow/integrations';
  // }
  // if (integrationType === 'teams') {
  //   return '/api/teams/integrations';
  // }
  // if (integrationType.startsWith('auth_events_')) {
  //   return '/api/auth-events/integrations';
  // }
  // if (integrationType.startsWith('health_check_')) {
  //   return '/api/health-checks/checks';
  // }
  // return '/api/aws/integrations';
  return '/api/discord/integrations';
};

// Handle GitHub App installation flow
const startGitHubInstall = () => {
  window.location.href = resolveApiUrl(`/api/github/install?project_id=${projectId.value}`);
};

// Handle Slack OAuth installation flow
const startSlackInstall = () => {
  window.location.href = resolveApiUrl(`/api/slack/oauth/install?project_id=${projectId.value}`);
};

const fetchLinkedRepo = async () => {
  try {
    const res = await axios.get(`/api/projects/${projectId.value}`);
    linkedRepo.value = res.data?.github_repo_url || null;
  } catch {
    linkedRepo.value = null;
  }
};

const fetchGitHubInstallations = async () => {
  try {
    const res = await axios.get('/api/github/installations');
    githubInstallations.value = res.data?.data || [];
  } catch {
    githubInstallations.value = [];
  }
};

const linkRepo = async () => {
  if (!selectedRepoUrl.value) return;
  repoLinking.value = true;
  try {
    await axios.post(`/api/projects/${projectId.value}/github`, {
      repository_url: selectedRepoUrl.value,
    });
    linkedRepo.value = selectedRepoUrl.value;
    selectedRepoUrl.value = '';
    successMessage.value = 'Repository linked successfully.';
  } catch (error) {
    alert('Failed to link repository: ' + (error.response?.data?.message || error.message));
  } finally {
    repoLinking.value = false;
  }
};

const unlinkRepo = async () => {
  repoLinking.value = true;
  try {
    await axios.delete(`/api/projects/${projectId.value}/github`);
    linkedRepo.value = null;
    successMessage.value = 'Repository unlinked.';
  } catch (error) {
    alert('Failed to unlink repository: ' + (error.response?.data?.message || error.message));
  } finally {
    repoLinking.value = false;
  }
};

const saveIntegration = async (integrationData) => {
  try {
    const apiPath = getIntegrationApiPath(selectedIntegrationType.value);
    
    if (selectedIntegration.value.id) {
      // Update existing
      await axios.put(`${apiPath}/${selectedIntegration.value.id}`, integrationData, {
        headers: { 'x-project-id': projectId.value }
      });
    } else {
      // Create new (project_id comes from header)
      await axios.post(apiPath, integrationData, {
        headers: { 'x-project-id': projectId.value }
      });
    }
    await fetchProjectIntegrations();
    closeConfigModal();
    // Show success notification
  } catch (error) {
    console.error('Failed to save integration:', error);
    alert('Failed to save integration: ' + (error.response?.data?.message || error.message));
  }
};

const toggleIntegration = async (integration) => {
  try {
    const apiPath = getIntegrationApiPath(integration.integration_type || 'aws');
    await axios.put(`${apiPath}/${integration.id}`, {
      enabled: !integration.enabled,
    });
    await fetchProjectIntegrations();
  } catch (error) {
    console.error('Failed to toggle integration:', error);
  }
};

const deleteIntegration = async (integration) => {
  if (!confirm('Are you sure you want to delete this integration?')) {
    return;
  }
  try {
    const apiPath = getIntegrationApiPath(integration.integration_type || 'aws');
    // GitHub integrations use installation_id (numeric), not id (UUID)
    const deleteId = integration.integration_type === 'github' 
      ? integration.installation_id 
      : integration.id;
    await axios.delete(`${apiPath}/${deleteId}`);
    await fetchProjectIntegrations();
  } catch (error) {
    console.error('Failed to delete integration:', error);
  }
};

const formatInterval = (seconds) => {
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  return `${Math.floor(seconds / 3600)}h`;
};

const fetchProjectApiKey = async () => {
  if (!projectId.value) return;
  try {
    const response = await axios.get(`/api/projects/${projectId.value}/keys`);
    const keys = response.data || [];
    if (keys.length > 0) {
      projectApiKey.value = keys[0].key;
    }
  } catch (error) {
    console.error('Failed to fetch project API key:', error);
  }
};

function loadIntegrationsPageData() {
  fetchProjectIntegrations();
  fetchProjectApiKey();
  fetchLinkedRepo();
  fetchGitHubInstallations();
}

onMounted(() => {
  if (route.query.github === 'success') {
    successMessage.value = 'GitHub App installed successfully.';
    router.replace({ ...route, query: { ...route.query, github: undefined } });
  }
  if (route.query.slack === 'installed') {
    successMessage.value = 'Slack app installed successfully. You can now receive alerts and chat with Moodeng in your Slack workspace.';
    router.replace({ ...route, query: { ...route.query, slack: undefined } });
  }
  if (route.query.slack === 'denied') {
    successMessage.value = '';
    router.replace({ ...route, query: { ...route.query, slack: undefined } });
  }
  loadIntegrationsPageData();
});

watch(projectId, loadIntegrationsPageData);
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

