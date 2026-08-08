<template>
  <AppLayout :user="user" :current-project="null">
    <div class="max-w-[1200px] mx-auto px-8 py-6">
      <!-- Header -->
      <div class="mb-6 flex items-center justify-between">
        <div>
          <h1 class="text-2xl font-semibold text-gray-900">Members</h1>
          <p class="text-sm text-gray-500 mt-1">
            Manage your organization's members and invitations
          </p>
        </div>
        <div class="flex gap-3">
          <BaseButton variant="outline" @click="showGenerateLink = true">
            Generate Invite Link
          </BaseButton>
          <BaseButton variant="primary" @click="showInviteModal = true">
            Invite Member
          </BaseButton>
        </div>
      </div>

      <!-- Stats -->
      <BaseCard class="mb-6">
        <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
          <div class="text-center p-4 bg-gray-50 rounded-lg">
            <div class="text-3xl font-bold text-primary-600">{{ members.length }}</div>
            <div class="text-sm text-gray-500">Active Members</div>
          </div>
          <div class="text-center p-4 bg-gray-50 rounded-lg">
            <div class="text-3xl font-bold text-amber-600">{{ invitations.length }}</div>
            <div class="text-sm text-gray-500">Pending Invitations</div>
          </div>
          <div class="text-center p-4 bg-gray-50 rounded-lg">
            <div class="text-3xl font-bold text-blue-600">{{ orgDomain || 'N/A' }}</div>
            <div class="text-sm text-gray-500">Organization Domain</div>
          </div>
        </div>
      </BaseCard>

      <!-- Error / Success messages -->
      <div v-if="error" class="mb-4 bg-red-50 border border-red-200 rounded-lg p-4">
        <p class="text-sm text-red-700">{{ error }}</p>
      </div>
      <div v-if="success" class="mb-4 bg-green-50 border border-green-200 rounded-lg p-4">
        <p class="text-sm text-green-700">{{ success }}</p>
      </div>

      <!-- Members Table -->
      <BaseCard class="mb-6">
        <template #header>
          <h2 class="text-lg font-semibold text-gray-900">Members ({{ members.length }})</h2>
        </template>

        <div v-if="loadingMembers" class="text-center py-8 text-gray-500">
          Loading members...
        </div>

        <div v-else-if="members.length === 0" class="text-center py-8 text-gray-500">
          No members found.
        </div>

        <table v-else class="min-w-full divide-y divide-gray-200">
          <thead class="bg-gray-50">
            <tr>
              <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Email</th>
              <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Role</th>
              <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Joined</th>
              <th class="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
            </tr>
          </thead>
          <tbody class="bg-white divide-y divide-gray-200">
            <tr
              v-for="member in members"
              :id="'member-row-' + member.user_id"
              :key="member.user_id"
              :class="{ 'bg-amber-50 ring-1 ring-inset ring-amber-200': highlightUserId === String(member.user_id) }"
            >
              <td class="px-4 py-3 text-sm text-gray-900">
                {{ member.email }}
                <span v-if="member.user_id === user?.id" class="ml-1 text-xs text-gray-400">(you)</span>
              </td>
              <td class="px-4 py-3 text-sm">
                <select
                  v-if="member.user_id !== user?.id"
                  :value="member.role"
                  @change="updateRole(member.user_id, $event.target.value)"
                  class="text-sm border-gray-300 rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500"
                >
                  <option value="owner">Owner</option>
                  <option value="admin">Admin</option>
                  <option value="member">Member</option>
                  <option value="viewer">Viewer</option>
                </select>
                <span v-else class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium capitalize"
                  :class="{
                    'bg-purple-100 text-purple-800': member.role === 'owner',
                    'bg-blue-100 text-blue-800': member.role === 'admin',
                    'bg-gray-100 text-gray-800': member.role === 'member' || member.role === 'viewer'
                  }"
                >
                  {{ member.role }}
                </span>
              </td>
              <td class="px-4 py-3 text-sm text-gray-500">
                {{ formatDate(member.joined_at) }}
              </td>
              <td class="px-4 py-3 text-sm text-right">
                <button
                  v-if="member.user_id !== user?.id"
                  @click="removeMember(member.user_id, member.email)"
                  class="text-red-600 hover:text-red-800 text-sm font-medium"
                >
                  Remove
                </button>
              </td>
            </tr>
          </tbody>
        </table>
      </BaseCard>

      <!-- Pending Invitations Table -->
      <BaseCard>
        <template #header>
          <h2 class="text-lg font-semibold text-gray-900">Pending Invitations ({{ invitations.length }})</h2>
        </template>

        <div v-if="loadingInvitations" class="text-center py-8 text-gray-500">
          Loading invitations...
        </div>

        <div v-else-if="invitations.length === 0" class="text-center py-8 text-gray-500">
          No pending invitations.
        </div>

        <table v-else class="min-w-full divide-y divide-gray-200">
          <thead class="bg-gray-50">
            <tr>
              <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Type</th>
              <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Email / Link</th>
              <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Role</th>
              <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Expires</th>
              <th class="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
            </tr>
          </thead>
          <tbody class="bg-white divide-y divide-gray-200">
            <tr v-for="inv in invitations" :key="inv.id">
              <td class="px-4 py-3 text-sm">
                <span
                  class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium"
                  :class="inv.email ? 'bg-blue-100 text-blue-800' : 'bg-green-100 text-green-800'"
                >
                  {{ inv.email ? 'Email' : 'Link' }}
                </span>
              </td>
              <td class="px-4 py-3 text-sm text-gray-900">
                <template v-if="inv.email">{{ inv.email }}</template>
                <template v-else>
                  <div class="flex items-center gap-2">
                    <code class="text-xs bg-gray-100 px-2 py-1 rounded truncate max-w-[200px]">{{ getInviteUrl(inv.invite_token) }}</code>
                    <button @click="copyLink(inv.invite_token)" class="text-indigo-600 hover:text-indigo-800 text-xs font-medium whitespace-nowrap">
                      Copy
                    </button>
                  </div>
                </template>
              </td>
              <td class="px-4 py-3 text-sm text-gray-500 capitalize">{{ inv.role }}</td>
              <td class="px-4 py-3 text-sm text-gray-500">{{ formatDate(inv.expires_at) }}</td>
              <td class="px-4 py-3 text-sm text-right">
                <button @click="revokeInvitation(inv.id)" class="text-red-600 hover:text-red-800 text-sm font-medium">
                  Revoke
                </button>
              </td>
            </tr>
          </tbody>
        </table>
      </BaseCard>

      <!-- Invite Modal -->
      <div v-if="showInviteModal" class="fixed inset-0 bg-black/50 flex items-center justify-center z-50" @click.self="showInviteModal = false">
        <div class="bg-white rounded-xl shadow-xl w-full max-w-md p-6">
          <h3 class="text-lg font-semibold text-gray-900 mb-4">Invite a Member</h3>
          <div class="space-y-4">
            <div>
              <label class="block text-sm font-medium text-gray-700 mb-1">Email address</label>
              <input
                v-model="inviteEmail"
                type="email"
                placeholder="colleague@example.com"
                class="w-full border-gray-300 rounded-lg shadow-sm focus:ring-indigo-500 focus:border-indigo-500"
              />
            </div>
            <div>
              <label class="block text-sm font-medium text-gray-700 mb-1">Role</label>
              <select v-model="inviteRole" class="w-full border-gray-300 rounded-lg shadow-sm focus:ring-indigo-500 focus:border-indigo-500">
                <option value="member">Member</option>
                <option value="admin">Admin</option>
                <option value="viewer">Viewer</option>
              </select>
            </div>
          </div>
          <div v-if="inviteError" class="mt-3 text-sm text-red-600">{{ inviteError }}</div>
          <div class="mt-6 flex justify-end gap-3">
            <BaseButton variant="outline" @click="showInviteModal = false">Cancel</BaseButton>
            <BaseButton variant="primary" @click="sendEmailInvite" :disabled="!inviteEmail || inviteSending">
              {{ inviteSending ? 'Sending...' : 'Send Invite' }}
            </BaseButton>
          </div>
        </div>
      </div>

      <!-- Generate Link Modal -->
      <div v-if="showGenerateLink" class="fixed inset-0 bg-black/50 flex items-center justify-center z-50" @click.self="showGenerateLink = false">
        <div class="bg-white rounded-xl shadow-xl w-full max-w-md p-6">
          <h3 class="text-lg font-semibold text-gray-900 mb-4">Generate Invite Link</h3>
          <div v-if="!generatedLink">
            <div class="mb-4">
              <label class="block text-sm font-medium text-gray-700 mb-1">Role for invited user</label>
              <select v-model="linkRole" class="w-full border-gray-300 rounded-lg shadow-sm focus:ring-indigo-500 focus:border-indigo-500">
                <option value="member">Member</option>
                <option value="admin">Admin</option>
                <option value="viewer">Viewer</option>
              </select>
            </div>
            <div class="flex justify-end gap-3">
              <BaseButton variant="outline" @click="showGenerateLink = false">Cancel</BaseButton>
              <BaseButton variant="primary" @click="generateLink" :disabled="linkGenerating">
                {{ linkGenerating ? 'Generating...' : 'Generate Link' }}
              </BaseButton>
            </div>
          </div>
          <div v-else>
            <p class="text-sm text-gray-600 mb-3">Share this link with the person you want to invite. It expires in 7 days.</p>
            <div class="flex items-center gap-2 bg-gray-50 rounded-lg p-3">
              <code class="text-sm text-gray-800 break-all flex-1">{{ generatedLink }}</code>
              <button @click="copyGeneratedLink" class="text-indigo-600 hover:text-indigo-800 text-sm font-medium whitespace-nowrap">
                {{ linkCopied ? 'Copied!' : 'Copy' }}
              </button>
            </div>
            <div class="mt-4 flex justify-end">
              <BaseButton variant="outline" @click="closeGenerateLink">Done</BaseButton>
            </div>
          </div>
        </div>
      </div>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, onMounted, watch, nextTick } from 'vue';
import { useRoute } from 'vue-router';
import axios from 'axios';
import AppLayout from '@/Layouts/AppLayout.vue';
import BaseCard from '@/components/BaseCard.vue';
import BaseButton from '@/components/BaseButton.vue';
import { useAuth } from '@/composables/useAuth';

const { user } = useAuth();
const route = useRoute();

const members = ref([]);
const highlightUserId = ref(null);
const invitations = ref([]);
const loadingMembers = ref(true);
const loadingInvitations = ref(true);
const error = ref('');
const success = ref('');
const orgDomain = ref('');

const showInviteModal = ref(false);
const inviteEmail = ref('');
const inviteRole = ref('member');
const inviteError = ref('');
const inviteSending = ref(false);

const showGenerateLink = ref(false);
const linkRole = ref('member');
const linkGenerating = ref(false);
const generatedLink = ref('');
const linkCopied = ref(false);

const formatDate = (dateStr) => {
  if (!dateStr) return '';
  return new Date(dateStr).toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
};

const getInviteUrl = (token) => {
  return `${window.location.origin}/api/invite/${token}`;
};

const fetchMembers = async () => {
  loadingMembers.value = true;
  try {
    const res = await axios.get('/api/org/invitations/members');
    members.value = res.data;
  } catch (err) {
    error.value = 'Failed to load members';
  } finally {
    loadingMembers.value = false;
  }
};

const fetchInvitations = async () => {
  loadingInvitations.value = true;
  try {
    const res = await axios.get('/api/org/invitations');
    invitations.value = res.data;
  } catch (err) {
    // May fail if not admin, which is fine
  } finally {
    loadingInvitations.value = false;
  }
};

const sendEmailInvite = async () => {
  inviteError.value = '';
  inviteSending.value = true;
  try {
    await axios.post('/api/org/invitations', {
      email: inviteEmail.value,
      role: inviteRole.value,
    });
    success.value = `Invitation sent to ${inviteEmail.value}`;
    showInviteModal.value = false;
    inviteEmail.value = '';
    inviteRole.value = 'member';
    fetchInvitations();
  } catch (err) {
    inviteError.value = err.response?.data?.error || err.response?.data?.message || 'Failed to create invitation';
  } finally {
    inviteSending.value = false;
  }
};

const generateLink = async () => {
  linkGenerating.value = true;
  try {
    const res = await axios.post('/api/org/invitations', {
      role: linkRole.value,
    });
    generatedLink.value = getInviteUrl(res.data.invite_token);
    fetchInvitations();
  } catch (err) {
    error.value = err.response?.data?.error || 'Failed to generate invite link';
    showGenerateLink.value = false;
  } finally {
    linkGenerating.value = false;
  }
};

const copyLink = async (token) => {
  try {
    await navigator.clipboard.writeText(getInviteUrl(token));
    success.value = 'Invite link copied to clipboard';
    setTimeout(() => { success.value = ''; }, 3000);
  } catch {
    // Fallback
  }
};

const copyGeneratedLink = async () => {
  try {
    await navigator.clipboard.writeText(generatedLink.value);
    linkCopied.value = true;
    setTimeout(() => { linkCopied.value = false; }, 2000);
  } catch {
    // Fallback
  }
};

const closeGenerateLink = () => {
  showGenerateLink.value = false;
  generatedLink.value = '';
  linkCopied.value = false;
  linkRole.value = 'member';
};

const revokeInvitation = async (id) => {
  if (!confirm('Are you sure you want to revoke this invitation?')) return;
  try {
    await axios.delete(`/api/org/invitations/${id}`);
    success.value = 'Invitation revoked';
    fetchInvitations();
  } catch (err) {
    error.value = err.response?.data?.error || 'Failed to revoke invitation';
  }
};

const updateRole = async (userId, newRole) => {
  try {
    await axios.put(`/api/org/invitations/members/${userId}`, { role: newRole });
    success.value = 'Role updated';
    fetchMembers();
  } catch (err) {
    error.value = err.response?.data?.error || 'Failed to update role';
    fetchMembers();
  }
};

const removeMember = async (userId, email) => {
  if (!confirm(`Remove ${email} from the organization?`)) return;
  try {
    await axios.delete(`/api/org/invitations/members/${userId}`);
    success.value = `${email} has been removed`;
    fetchMembers();
  } catch (err) {
    error.value = err.response?.data?.error || 'Failed to remove member';
  }
};

const fetchOrgInfo = async () => {
  try {
    const res = await axios.get('/api/org/invitations/info');
    orgDomain.value = res.data.domain || '';
  } catch {
    // Not critical
  }
};

const scrollToUserFromQuery = async () => {
  const uid = route.query.user;
  if (!uid || typeof uid !== 'string') return;
  await nextTick();
  const el = document.getElementById(`member-row-${uid}`);
  if (el) {
    el.scrollIntoView({ behavior: 'smooth', block: 'center' });
    highlightUserId.value = uid;
    window.setTimeout(() => {
      highlightUserId.value = null;
    }, 4000);
  }
};

onMounted(async () => {
  await fetchMembers();
  fetchInvitations();
  fetchOrgInfo();
  await nextTick();
  await scrollToUserFromQuery();
});

watch(
  () => route.query.user,
  async () => {
    await nextTick();
    await scrollToUserFromQuery();
  },
);
</script>
