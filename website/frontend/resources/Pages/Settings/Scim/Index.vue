<template>
  <AppLayout :user="user" :current-project="null">
    <div class="max-w-[1200px] mx-auto px-8 py-6">
      <!-- Header -->
      <div class="mb-6">
        <h1 class="text-2xl font-semibold text-gray-900 dark:text-gray-100">SCIM Provisioning</h1>
        <p class="text-sm text-gray-500 dark:text-gray-400 mt-1">
          Automate user and group provisioning from your Identity Provider using SCIM 2.0
        </p>
      </div>

      <!-- SCIM Endpoint URL -->
      <BaseCard class="mb-6">
        <template #header>
          <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">SCIM Endpoint URL</h2>
        </template>
        <p class="text-sm text-gray-500 dark:text-gray-400 mb-3">
          Configure this base URL in your Identity Provider's SCIM integration settings.
        </p>
        <div class="flex items-center gap-2">
          <input
            type="text"
            :value="scimBaseUrl"
            readonly
            class="flex-1 px-3 py-2 bg-gray-50 dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-lg text-sm text-gray-700 dark:text-gray-300 font-mono select-all"
          />
          <button
            @click="copyToClipboard(scimBaseUrl)"
            class="px-3 py-2 text-sm font-medium text-gray-600 dark:text-gray-300 bg-gray-100 dark:bg-gray-700 hover:bg-gray-200 dark:hover:bg-gray-600 border border-gray-300 dark:border-gray-600 rounded-lg transition-colors"
          >
            <svg v-if="!copiedUrl" class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
            </svg>
            <svg v-else class="w-4 h-4 text-green-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
            </svg>
          </button>
        </div>
      </BaseCard>

      <!-- SCIM Bearer Token -->
      <BaseCard class="mb-6">
        <template #header>
          <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">Bearer Token</h2>
        </template>
        <p class="text-sm text-gray-500 dark:text-gray-400 mb-4">
          Your Identity Provider will use this token to authenticate SCIM requests.
        </p>

        <div v-if="tokenLoading" class="text-center py-4 text-gray-500 dark:text-gray-400">
          <div class="spinner w-6 h-6 border-4 border-primary-600 border-t-transparent rounded-full mx-auto mb-2"></div>
          <p class="text-sm">Loading token status...</p>
        </div>

        <div v-else>
          <!-- Newly generated token display -->
          <div v-if="newlyGeneratedToken" class="mb-4">
            <div class="p-4 bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-lg">
              <div class="flex items-start gap-2 mb-2">
                <svg class="w-5 h-5 text-yellow-600 dark:text-yellow-400 flex-shrink-0 mt-0.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.082 16.5c-.77.833.192 2.5 1.732 2.5z" />
                </svg>
                <p class="text-sm font-medium text-yellow-800 dark:text-yellow-200">
                  Copy this token now — it will not be shown again.
                </p>
              </div>
              <div class="flex items-center gap-2">
                <input
                  type="text"
                  :value="newlyGeneratedToken"
                  readonly
                  class="flex-1 px-3 py-2 bg-white dark:bg-gray-800 border border-yellow-300 dark:border-yellow-700 rounded-lg text-sm text-gray-900 dark:text-gray-100 font-mono select-all"
                />
                <button
                  @click="copyToClipboard(newlyGeneratedToken)"
                  class="px-3 py-2 text-sm font-medium text-yellow-700 dark:text-yellow-300 bg-yellow-100 dark:bg-yellow-800 hover:bg-yellow-200 dark:hover:bg-yellow-700 border border-yellow-300 dark:border-yellow-600 rounded-lg transition-colors"
                >
                  <svg v-if="!copiedToken" class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
                  </svg>
                  <svg v-else class="w-4 h-4 text-green-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
                  </svg>
                </button>
              </div>
            </div>
          </div>

          <!-- Existing token status -->
          <div class="flex items-center justify-between">
            <div v-if="hasExistingToken" class="flex items-center gap-2">
              <span class="px-2 py-1 text-xs font-medium rounded bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200">Active</span>
              <span class="text-sm text-gray-500 dark:text-gray-400 font-mono">{{ maskedToken }}</span>
              <span v-if="tokenCreatedAt" class="text-xs text-gray-400 dark:text-gray-500">
                · created {{ tokenCreatedAt }}
              </span>
            </div>
            <div v-else class="text-sm text-gray-500 dark:text-gray-400">
              No token has been generated yet.
            </div>
            <BaseButton
              :variant="hasExistingToken ? 'secondary' : 'primary'"
              @click="generateToken"
              :disabled="tokenGenerating"
            >
              <svg v-if="tokenGenerating" class="w-4 h-4 mr-2 animate-spin" fill="none" viewBox="0 0 24 24">
                <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4" />
                <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
              </svg>
              {{ hasExistingToken ? 'Rotate Token' : 'Generate Token' }}
            </BaseButton>
          </div>
        </div>
      </BaseCard>

      <!-- Group-to-Role Mappings -->
      <BaseCard class="mb-6">
        <template #header>
          <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">Group-to-Role Mappings</h2>
        </template>
        <p class="text-sm text-gray-500 dark:text-gray-400 mb-4">
          Map IdP group names to Reiver roles. Users provisioned via SCIM will be assigned the role matching their group.
        </p>

        <div v-if="mappingsLoading" class="text-center py-4 text-gray-500 dark:text-gray-400">
          <div class="spinner w-6 h-6 border-4 border-primary-600 border-t-transparent rounded-full mx-auto mb-2"></div>
          <p class="text-sm">Loading mappings...</p>
        </div>

        <div v-else>
          <!-- Mappings table -->
          <div v-if="groupMappings.length > 0" class="border border-gray-200 dark:border-gray-700 rounded-lg overflow-hidden mb-4">
            <table class="w-full text-sm">
              <thead class="bg-gray-50 dark:bg-gray-800">
                <tr>
                  <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">IdP Group Name</th>
                  <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Mapped Role</th>
                  <th class="px-4 py-3 text-right text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Actions</th>
                </tr>
              </thead>
              <tbody class="divide-y divide-gray-200 dark:divide-gray-700">
                <tr
                  v-for="mapping in groupMappings"
                  :key="mapping.id"
                  class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors"
                >
                  <td class="px-4 py-3 text-gray-900 dark:text-gray-100 font-mono text-sm">{{ mapping.displayName }}</td>
                  <td class="px-4 py-3">
                    <span
                      class="px-2 py-1 text-xs font-medium rounded"
                      :class="roleBadgeClass(mapping.role)"
                    >
                      {{ mapping.role }}
                    </span>
                  </td>
                  <td class="px-4 py-3 text-right">
                    <button
                      @click="deleteMapping(mapping)"
                      class="p-1.5 text-gray-400 hover:text-red-600 dark:hover:text-red-400 transition-colors"
                      title="Delete mapping"
                    >
                      <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
                      </svg>
                    </button>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>

          <div v-else class="text-center py-6 text-gray-500 dark:text-gray-400 mb-4">
            <p class="text-sm">No group mappings configured. Add one below to get started.</p>
          </div>

          <!-- Add mapping form -->
          <div class="flex items-end gap-3 p-4 bg-gray-50 dark:bg-gray-800 rounded-lg">
            <div class="flex-1">
              <label class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Group Name</label>
              <input
                v-model="newMapping.groupName"
                type="text"
                placeholder="e.g. reiver-admins"
                class="w-full px-3 py-2 bg-white dark:bg-gray-900 border border-gray-300 dark:border-gray-600 rounded-lg text-sm text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-500 focus:ring-2 focus:ring-primary-500 focus:border-primary-500"
              />
            </div>
            <div class="w-40">
              <label class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Role</label>
              <select
                v-model="newMapping.role"
                class="w-full px-3 py-2 bg-white dark:bg-gray-900 border border-gray-300 dark:border-gray-600 rounded-lg text-sm text-gray-900 dark:text-gray-100 focus:ring-2 focus:ring-primary-500 focus:border-primary-500"
              >
                <option value="admin">Admin</option>
                <option value="member">Member</option>
                <option value="viewer">Viewer</option>
              </select>
            </div>
            <BaseButton
              variant="primary"
              @click="addMapping"
              :disabled="!newMapping.groupName.trim() || addingMapping"
            >
              <svg v-if="addingMapping" class="w-4 h-4 mr-2 animate-spin" fill="none" viewBox="0 0 24 24">
                <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4" />
                <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
              </svg>
              Add Mapping
            </BaseButton>
          </div>
        </div>
      </BaseCard>

      <!-- Provisioned Users -->
      <BaseCard>
        <template #header>
          <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">
            Provisioned Users
            <span v-if="provisionedUsers.length" class="text-sm font-normal text-gray-500 dark:text-gray-400">
              ({{ provisionedUsers.length }})
            </span>
          </h2>
        </template>

        <div v-if="usersLoading" class="text-center py-8 text-gray-500 dark:text-gray-400">
          <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full mx-auto mb-3"></div>
          <p>Loading provisioned users...</p>
        </div>

        <div v-else-if="provisionedUsers.length === 0" class="text-center py-12 text-gray-500 dark:text-gray-400">
          <svg class="w-12 h-12 mx-auto mb-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0z" />
          </svg>
          <p class="text-lg font-medium mb-2">No provisioned users</p>
          <p class="text-sm">Users will appear here once your IdP starts provisioning via SCIM.</p>
        </div>

        <div v-else class="border border-gray-200 dark:border-gray-700 rounded-lg overflow-hidden">
          <table class="w-full text-sm">
            <thead class="bg-gray-50 dark:bg-gray-800">
              <tr>
                <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Email</th>
                <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">External ID</th>
                <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Role</th>
                <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Status</th>
                <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Provisioned</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-gray-200 dark:divide-gray-700">
              <tr
                v-for="user in provisionedUsers"
                :key="user.id"
                class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors"
              >
                <td class="px-4 py-3 text-gray-900 dark:text-gray-100">{{ user.email }}</td>
                <td class="px-4 py-3 text-gray-500 dark:text-gray-400 font-mono text-xs">{{ user.external_id }}</td>
                <td class="px-4 py-3">
                  <span
                    class="px-2 py-1 text-xs font-medium rounded"
                    :class="roleBadgeClass(user.role)"
                  >
                    {{ user.role }}
                  </span>
                </td>
                <td class="px-4 py-3">
                  <span
                    class="px-2 py-1 text-xs font-medium rounded"
                    :class="user.active
                      ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200'
                      : 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300'"
                  >
                    {{ user.active ? 'Active' : 'Deactivated' }}
                  </span>
                </td>
                <td class="px-4 py-3 text-gray-500 dark:text-gray-400 text-xs">{{ formatDate(user.created_at) }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </BaseCard>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, onMounted } from 'vue';
import axios from 'axios';
import AppLayout from '@/Layouts/AppLayout.vue';
import BaseCard from '@/components/BaseCard.vue';
import BaseButton from '@/components/BaseButton.vue';
import { useAuth } from '@/composables/useAuth';

const { user } = useAuth();

const scimBaseUrl = `${window.location.origin}/api/scim/v2/`;

// Token state
const tokenLoading = ref(false);
const tokenGenerating = ref(false);
const hasExistingToken = ref(false);
const maskedToken = ref('');
const tokenCreatedAt = ref('');
const newlyGeneratedToken = ref('');
const copiedUrl = ref(false);
const copiedToken = ref(false);

// Group mappings state
const mappingsLoading = ref(false);
const addingMapping = ref(false);
const groupMappings = ref([]);
const newMapping = ref({ groupName: '', role: 'member' });

// Provisioned users state
const usersLoading = ref(false);
const provisionedUsers = ref([]);

const roleBadgeClass = (role) => {
  switch (role) {
    case 'admin': return 'bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200';
    case 'member': return 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200';
    case 'viewer': return 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300';
    default: return 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300';
  }
};

const copyToClipboard = async (text) => {
  try {
    await navigator.clipboard.writeText(text);
    if (text === scimBaseUrl) {
      copiedUrl.value = true;
      setTimeout(() => { copiedUrl.value = false; }, 2000);
    } else {
      copiedToken.value = true;
      setTimeout(() => { copiedToken.value = false; }, 2000);
    }
  } catch {
    const textarea = document.createElement('textarea');
    textarea.value = text;
    document.body.appendChild(textarea);
    textarea.select();
    document.execCommand('copy');
    document.body.removeChild(textarea);
  }
};

const formatDate = (dateStr) => {
  if (!dateStr) return '—';
  const d = new Date(dateStr);
  return d.toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' });
};

// --- Token API ---

const fetchTokenStatus = async () => {
  tokenLoading.value = true;
  try {
    const response = await axios.get('/api/settings/scim/token');
    hasExistingToken.value = response.data.exists;
    maskedToken.value = response.data.masked || '';
    tokenCreatedAt.value = response.data.created_at ? formatDate(response.data.created_at) : '';
  } catch (error) {
    console.error('Failed to fetch SCIM token status:', error);
  } finally {
    tokenLoading.value = false;
  }
};

const generateToken = async () => {
  if (hasExistingToken.value && !confirm('Rotating the token will invalidate the current one. Your IdP will need to be updated. Continue?')) {
    return;
  }
  tokenGenerating.value = true;
  try {
    const response = await axios.post('/api/settings/scim/token');
    newlyGeneratedToken.value = response.data.token;
    hasExistingToken.value = true;
    maskedToken.value = response.data.masked || '';
    tokenCreatedAt.value = 'just now';
  } catch (error) {
    console.error('Failed to generate SCIM token:', error);
    alert('Failed to generate token: ' + (error.response?.data?.message || error.message));
  } finally {
    tokenGenerating.value = false;
  }
};

// --- Group Mappings API ---

const ssoConfigId = ref(null);

const fetchSsoConfigId = async () => {
  try {
    const res = await axios.get('/api/settings/scim/token');
    // Token endpoint hitting the SSO config is enough to confirm one exists;
    // we fetch the ID separately so GroupMappings POST can include it.
    const configs = await axios.get('/api/scim/v2/GroupMappings');
    const mappings = configs.data || [];
    if (mappings.length > 0) {
      ssoConfigId.value = mappings[0].sso_config_id;
    }
  } catch {
    // If no config exists, we still proceed — the user can view but not add.
  }
};

const fetchGroupMappings = async () => {
  mappingsLoading.value = true;
  try {
    const response = await axios.get('/api/scim/v2/GroupMappings');
    const raw = response.data || [];
    groupMappings.value = raw.map(m => ({
      id: m.id,
      sso_config_id: m.sso_config_id,
      displayName: m.external_group_name,
      role: m.reiver_role,
    }));
    if (raw.length > 0 && !ssoConfigId.value) {
      ssoConfigId.value = raw[0].sso_config_id;
    }
  } catch (error) {
    console.error('Failed to fetch group mappings:', error);
  } finally {
    mappingsLoading.value = false;
  }
};

const addMapping = async () => {
  if (!newMapping.value.groupName.trim()) return;
  if (!ssoConfigId.value) {
    alert('No SCIM-enabled SSO configuration found. Please enable SCIM in your SSO settings first.');
    return;
  }
  addingMapping.value = true;
  try {
    const groupName = newMapping.value.groupName.trim();
    await axios.post('/api/scim/v2/GroupMappings', {
      sso_config_id: ssoConfigId.value,
      external_group_id: groupName,
      external_group_name: groupName,
      reiver_role: newMapping.value.role,
    });
    newMapping.value = { groupName: '', role: 'member' };
    await fetchGroupMappings();
  } catch (error) {
    console.error('Failed to add group mapping:', error);
    alert('Failed to add mapping: ' + (error.response?.data?.message || error.message));
  } finally {
    addingMapping.value = false;
  }
};

const deleteMapping = async (mapping) => {
  if (!confirm(`Remove the mapping for group "${mapping.displayName}"?`)) return;
  try {
    await axios.delete(`/api/scim/v2/GroupMappings/${mapping.id}`);
    await fetchGroupMappings();
  } catch (error) {
    console.error('Failed to delete group mapping:', error);
  }
};

// --- Provisioned Users API ---

const fetchProvisionedUsers = async () => {
  usersLoading.value = true;
  try {
    const response = await axios.get('/api/settings/scim/users');
    provisionedUsers.value = response.data.users || response.data || [];
  } catch (error) {
    console.error('Failed to fetch provisioned users:', error);
  } finally {
    usersLoading.value = false;
  }
};

onMounted(async () => {
  await fetchSsoConfigId();
  fetchTokenStatus();
  fetchGroupMappings();
  fetchProvisionedUsers();
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
</style>
