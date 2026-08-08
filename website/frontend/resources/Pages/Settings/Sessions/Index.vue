<template>
  <AppLayout :user="user" :current-project="null">
    <div class="max-w-[1200px] mx-auto px-8 py-6">
      <!-- Header -->
      <div class="mb-6">
        <div>
          <h1 class="text-2xl font-semibold text-gray-900">Active Sessions</h1>
          <p class="text-sm text-gray-500 dark:text-gray-400 mt-1">
            Manage your active login sessions across devices
          </p>
        </div>
        <BaseButton variant="danger" @click="revokeAllSessions" :disabled="sessions.length === 0">
          <svg class="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1" />
          </svg>
          Sign Out All Devices
        </BaseButton>
      </div>

      <!-- Current Session -->
      <BaseCard class="mb-6">
        <template #header>
          <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">Current Session</h2>
        </template>
        <div v-if="currentSession" class="flex items-start justify-between p-4 bg-green-50 dark:bg-green-900/20 rounded-lg border border-green-200 dark:border-green-800">
          <div class="flex items-start gap-4">
            <div class="w-10 h-10 rounded-lg bg-green-100 dark:bg-green-900 flex items-center justify-center">
              <svg class="w-5 h-5 text-green-600 dark:text-green-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z" />
              </svg>
            </div>
            <div>
              <h3 class="font-medium text-gray-900 dark:text-gray-100">This device</h3>
              <p class="text-sm text-gray-600 dark:text-gray-400 mt-1">
                {{ currentSession.user_agent || 'Unknown browser' }}
              </p>
              <div class="text-xs text-gray-500 dark:text-gray-500 mt-2 space-y-1">
                <p>IP: {{ currentSession.ip_address || 'Unknown' }}</p>
                <p>Last active: {{ formatDate(currentSession.last_activity_at) }}</p>
                <p>Started: {{ formatDate(currentSession.created_at) }}</p>
              </div>
            </div>
          </div>
          <span class="px-2 py-1 text-xs font-medium rounded bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200">
            Active
          </span>
        </div>
        <div v-else class="text-center py-8 text-gray-500 dark:text-gray-400">
          <p>Current session information not available</p>
        </div>
      </BaseCard>

      <!-- Other Sessions -->
      <BaseCard>
        <template #header>
          <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">
            Other Sessions ({{ otherSessions.length }})
          </h2>
        </template>
        
        <div v-if="loading" class="text-center py-8 text-gray-500 dark:text-gray-400">
          <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full mx-auto mb-3"></div>
          <p>Loading sessions...</p>
        </div>
        
        <div v-else-if="otherSessions.length === 0" class="text-center py-12 text-gray-500 dark:text-gray-400">
          <svg class="w-12 h-12 mx-auto mb-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z" />
          </svg>
          <p class="text-lg font-medium mb-2">No other active sessions</p>
          <p class="text-sm">You're only signed in on this device</p>
        </div>
        
        <div v-else class="space-y-4">
          <div
            v-for="session in otherSessions"
            :key="session.id"
            class="flex items-start justify-between p-4 border border-gray-200 dark:border-gray-700 rounded-lg hover:border-gray-300 dark:hover:border-gray-600 transition-colors"
          >
            <div class="flex items-start gap-4">
              <div class="w-10 h-10 rounded-lg bg-gray-100 dark:bg-gray-800 flex items-center justify-center">
                <svg class="w-5 h-5 text-gray-500 dark:text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
                </svg>
              </div>
              <div>
                <h3 class="font-medium text-gray-900 dark:text-gray-100">
                  {{ parseUserAgent(session.user_agent) }}
                </h3>
                <div class="text-xs text-gray-500 dark:text-gray-500 mt-2 space-y-1">
                  <p>IP: {{ session.ip_address || 'Unknown' }}</p>
                  <p>Last active: {{ formatDate(session.last_activity_at) }}</p>
                  <p>Started: {{ formatDate(session.created_at) }}</p>
                </div>
              </div>
            </div>
            <button
              @click="revokeSession(session)"
              class="px-3 py-1.5 text-sm font-medium text-red-600 dark:text-red-400 hover:bg-red-50 dark:hover:bg-red-900/20 rounded-md transition-colors"
            >
              Sign out
            </button>
          </div>
        </div>
      </BaseCard>

      <!-- MFA Section -->
      <BaseCard class="mt-6">
        <template #header>
          <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">Two-Factor Authentication</h2>
        </template>
        
        <div class="p-4">
          <div v-if="mfaStatus.enabled" class="flex items-start justify-between">
            <div class="flex items-start gap-4">
              <div class="w-10 h-10 rounded-lg bg-green-100 dark:bg-green-900 flex items-center justify-center">
                <svg class="w-5 h-5 text-green-600 dark:text-green-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z" />
                </svg>
              </div>
              <div>
                <h3 class="font-medium text-gray-900 dark:text-gray-100">2FA is enabled</h3>
                <p class="text-sm text-gray-600 dark:text-gray-400 mt-1">
                  {{ mfaStatus.methods.length }} method(s) configured
                </p>
                <p class="text-xs text-gray-500 dark:text-gray-500 mt-2">
                  Recovery codes remaining: {{ mfaStatus.recovery_codes_remaining }}
                </p>
              </div>
            </div>
            <BaseButton variant="secondary" size="sm" @click="manageMfa">
              Manage
            </BaseButton>
          </div>
          
          <div v-else class="flex items-start justify-between">
            <div class="flex items-start gap-4">
              <div class="w-10 h-10 rounded-lg bg-yellow-100 dark:bg-yellow-900 flex items-center justify-center">
                <svg class="w-5 h-5 text-yellow-600 dark:text-yellow-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                </svg>
              </div>
              <div>
                <h3 class="font-medium text-gray-900 dark:text-gray-100">2FA is not enabled</h3>
                <p class="text-sm text-gray-600 dark:text-gray-400 mt-1">
                  Add an extra layer of security to your account
                </p>
              </div>
            </div>
            <BaseButton variant="primary" size="sm" @click="setupMfa">
              Enable 2FA
            </BaseButton>
          </div>
        </div>
      </BaseCard>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue';
import axios from 'axios';
import AppLayout from '@/Layouts/AppLayout.vue';
import BaseCard from '@/components/BaseCard.vue';
import BaseButton from '@/components/BaseButton.vue';
import { useAuth } from '@/composables/useAuth';

const { user } = useAuth();

const loading = ref(false);
const sessions = ref([]);
const currentSessionId = ref(null);
const mfaStatus = ref({ enabled: false, methods: [], recovery_codes_remaining: 0 });

const currentSession = computed(() => 
  sessions.value.find(s => s.id === currentSessionId.value)
);

const otherSessions = computed(() => 
  sessions.value.filter(s => s.id !== currentSessionId.value)
);

const fetchSessions = async () => {
  loading.value = true;
  try {
    const response = await axios.get('/api/auth/sessions');
    sessions.value = response.data.sessions || [];
    currentSessionId.value = response.data.current_session_id;
  } catch (error) {
    console.error('Failed to fetch sessions:', error);
  } finally {
    loading.value = false;
  }
};

const fetchMfaStatus = async () => {
  try {
    const response = await axios.get('/api/auth/mfa/status');
    mfaStatus.value = response.data;
  } catch (error) {
    console.error('Failed to fetch MFA status:', error);
  }
};

const revokeSession = async (session) => {
  if (!confirm('Are you sure you want to sign out this session?')) return;
  
  try {
    await axios.delete(`/api/auth/sessions/${session.id}`);
    await fetchSessions();
  } catch (error) {
    console.error('Failed to revoke session:', error);
    alert('Failed to sign out session');
  }
};

const revokeAllSessions = async () => {
  if (!confirm('Are you sure you want to sign out all other devices? You will remain signed in on this device.')) return;
  
  try {
    await axios.delete('/api/auth/sessions', { params: { except_current: true } });
    await fetchSessions();
  } catch (error) {
    console.error('Failed to revoke sessions:', error);
    alert('Failed to sign out other devices');
  }
};

const setupMfa = () => {
  window.location.href = '/settings/security/mfa/setup';
};

const manageMfa = () => {
  window.location.href = '/settings/security/mfa';
};

const formatDate = (dateString) => {
  if (!dateString) return 'Unknown';
  const date = new Date(dateString);
  const now = new Date();
  const diff = now - date;
  
  if (diff < 60000) return 'Just now';
  if (diff < 3600000) return `${Math.floor(diff / 60000)} minutes ago`;
  if (diff < 86400000) return `${Math.floor(diff / 3600000)} hours ago`;
  if (diff < 604800000) return `${Math.floor(diff / 86400000)} days ago`;
  
  return date.toLocaleDateString();
};

const parseUserAgent = (ua) => {
  if (!ua) return 'Unknown device';
  
  // Simple parsing - in production, use a proper UA parser library
  if (ua.includes('Chrome')) return 'Chrome Browser';
  if (ua.includes('Firefox')) return 'Firefox Browser';
  if (ua.includes('Safari')) return 'Safari Browser';
  if (ua.includes('Edge')) return 'Microsoft Edge';
  if (ua.includes('Mobile')) return 'Mobile Device';
  
  return ua.substring(0, 50) + (ua.length > 50 ? '...' : '');
};

onMounted(() => {
  fetchSessions();
  fetchMfaStatus();
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
