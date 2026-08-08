<template>
  <div class="space-y-6">
    <BaseCard class="p-5">
      <h3 class="text-sm font-semibold text-gray-900 mb-2">New account registration</h3>
      <p class="text-sm text-gray-600 mb-4">
        Applies when someone creates a new account via email/password or OAuth (the flow that later creates their organization and first project when they get started).
        When enabled, you must approve those accounts here before they can use the platform. This does <strong>not</strong> apply to users provisioned through SCIM or SSO for an existing organization—those are approved for login separately.
        Invite-based sign-ups are always approved.
      </p>
      <div v-if="loadingPolicy" class="text-sm text-gray-400">Loading policy…</div>
      <label v-else class="flex items-center gap-3 cursor-pointer">
        <input
          type="checkbox"
          v-model="requireSignupApproval"
          @change="saveSignupPolicy"
          :disabled="savingPolicy"
          class="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
        />
        <span class="text-sm text-gray-800">Require manual approval for new self-serve accounts</span>
      </label>
      <p v-if="policyError" class="mt-2 text-sm text-red-600">{{ policyError }}</p>
    </BaseCard>

    <div>
      <h2 class="text-lg font-semibold text-gray-900 mb-1">Users</h2>
      <p class="text-sm text-gray-500 mb-4">Approve or disable user accounts for platform access.</p>
      <div v-if="loadingUsers" class="text-gray-400 text-sm py-8">Loading users...</div>
      <div v-else-if="usersError" class="text-red-600 text-sm py-4">{{ usersError }}</div>
      <div v-else class="bg-white border border-gray-200 rounded-lg overflow-hidden">
        <table class="min-w-full divide-y divide-gray-200">
          <thead class="bg-gray-50">
            <tr>
              <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Email</th>
              <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Status</th>
              <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Role</th>
              <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Registered</th>
              <th class="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Actions</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-gray-200">
            <tr v-for="u in users" :key="u.id">
              <td class="px-4 py-3 text-sm font-medium text-gray-900">{{ u.email }}</td>
              <td class="px-4 py-3">
                <span
                  class="inline-flex px-2 py-0.5 rounded-full text-xs font-semibold"
                  :class="u.is_approved ? 'bg-green-100 text-green-800' : 'bg-amber-100 text-amber-800'"
                >
                  {{ u.is_approved ? 'Approved' : 'Pending' }}
                </span>
              </td>
              <td class="px-4 py-3 text-sm">
                <span v-if="u.is_platform_admin" class="text-indigo-700 font-medium">Admin</span>
                <span v-else class="text-gray-400">User</span>
              </td>
              <td class="px-4 py-3 text-sm text-gray-500">{{ formatUserDate(u.created_at) }}</td>
              <td class="px-4 py-3 text-right text-sm">
                <button
                  v-if="!u.is_approved"
                  @click="approveUser(u)"
                  :disabled="u._loading"
                  class="px-3 py-1 rounded-md bg-green-600 text-white text-xs font-medium hover:bg-green-700 disabled:opacity-50"
                >
                  {{ u._loading ? '…' : 'Approve' }}
                </button>
                <button
                  v-else-if="!u.is_platform_admin"
                  @click="disableUser(u)"
                  :disabled="u._loading"
                  class="px-3 py-1 rounded-md border border-red-200 text-red-700 text-xs font-medium hover:bg-red-50 disabled:opacity-50"
                >
                  {{ u._loading ? '…' : 'Disable' }}
                </button>
                <span v-else class="text-gray-300">—</span>
              </td>
            </tr>
          </tbody>
        </table>
        <div v-if="users.length === 0" class="px-4 py-8 text-center text-gray-500 text-sm">No users found.</div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue';
import axios from 'axios';
import BaseCard from '@/components/BaseCard.vue';

const users = ref([]);
const loadingUsers = ref(true);
const usersError = ref('');
const requireSignupApproval = ref(true);
const loadingPolicy = ref(true);
const savingPolicy = ref(false);
const policyError = ref('');

async function fetchSignupPolicy() {
  loadingPolicy.value = true;
  policyError.value = '';
  try {
    const { data } = await axios.get('/api/admin/signup-policy');
    requireSignupApproval.value = !!data.require_signup_approval;
  } catch (e) {
    policyError.value = e.response?.data?.error || 'Failed to load signup policy';
  } finally {
    loadingPolicy.value = false;
  }
}

async function saveSignupPolicy() {
  savingPolicy.value = true;
  policyError.value = '';
  try {
    await axios.put('/api/admin/signup-policy', {
      require_signup_approval: requireSignupApproval.value,
    });
  } catch (e) {
    policyError.value = e.response?.data?.error || 'Failed to save';
    await fetchSignupPolicy();
  } finally {
    savingPolicy.value = false;
  }
}

async function fetchUsers() {
  loadingUsers.value = true;
  usersError.value = '';
  try {
    const res = await axios.get('/api/admin/users');
    users.value = (res.data || []).map((u) => ({ ...u, _loading: false }));
  } catch (e) {
    usersError.value = e.response?.data?.error || e.response?.data?.message || 'Failed to load users';
  } finally {
    loadingUsers.value = false;
  }
}

function formatUserDate(dateStr) {
  if (!dateStr) return '—';
  return new Date(dateStr).toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  });
}

async function approveUser(u) {
  u._loading = true;
  try {
    await axios.post(`/api/admin/users/${u.id}/approve`);
    u.is_approved = true;
  } catch (e) {
    alert(e.response?.data?.error || 'Failed to approve user');
  } finally {
    u._loading = false;
  }
}

async function disableUser(u) {
  if (!confirm(`Disable access for ${u.email}?`)) return;
  u._loading = true;
  try {
    await axios.post(`/api/admin/users/${u.id}/disable`);
    u.is_approved = false;
  } catch (e) {
    alert(e.response?.data?.error || 'Failed to disable user');
  } finally {
    u._loading = false;
  }
}

onMounted(() => {
  fetchUsers();
  fetchSignupPolicy();
});
</script>
