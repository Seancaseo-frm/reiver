<template>
  <AppLayout :user="user" :current-project="null">
    <div class="max-w-[1200px] mx-auto px-8 py-6">
      <!-- Header -->
      <div class="mb-6">
        <div>
          <h1 class="text-2xl font-semibold text-gray-900">Active Sessions</h1>
          <p class="text-sm text-gray-500 dark:text-gray-400 mt-1">
            Manage your active sessions and security settings
          </p>
        </div>
        <BaseButton variant="danger" @click="revokeAllSessions" :disabled="sessions.length === 0">
          <svg class="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1" />
          </svg>
          Sign Out All Devices
        </BaseButton>
      </div>

      <!-- MFA Status Card -->
      <BaseCard class="mb-6">
        <template #header>
          <div class="flex items-center justify-between">
            <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">Multi-Factor Authentication</h2>
            <span 
              class="px-3 py-1 text-sm font-medium rounded-full"
              :class="mfaStatus.enabled 
                ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200' 
                : 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200'"
            >
              {{ mfaStatus.enabled ? 'Enabled' : 'Not Enabled' }}
            </span>
          </div>
        </template>
        
        <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
          <!-- TOTP -->
          <div class="p-4 bg-gray-50 dark:bg-gray-800 rounded-lg">
            <div class="flex items-center justify-between mb-3">
              <div class="flex items-center">
                <svg class="w-6 h-6 text-primary-600 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 18h.01M8 21h8a2 2 0 002-2V5a2 2 0 00-2-2H8a2 2 0 00-2 2v14a2 2 0 002 2z" />
                </svg>
                <span class="font-medium">Authenticator App</span>
              </div>
              <span 
                class="px-2 py-1 text-xs rounded"
                :class="hasTOTP ? 'bg-green-100 text-green-700 dark:bg-green-900 dark:text-green-300' : 'bg-gray-200 text-gray-600 dark:bg-gray-700 dark:text-gray-400'"
              >
                {{ hasTOTP ? 'Active' : 'Not Set' }}
              </span>
            </div>
            <p class="text-sm text-gray-500 dark:text-gray-400 mb-3">
              Use an app like Google Authenticator or Authy
            </p>
            <BaseButton 
              v-if="!hasTOTP" 
              variant="primary" 
              size="sm" 
              @click="setupTOTP"
              class="w-full"
            >
              Set Up
            </BaseButton>
            <BaseButton 
              v-else 
              variant="secondary" 
              size="sm" 
              @click="disableTOTP"
              class="w-full"
            >
              Disable
            </BaseButton>
          </div>

          <!-- WebAuthn -->
          <div class="p-4 bg-gray-50 dark:bg-gray-800 rounded-lg">
            <div class="flex items-center justify-between mb-3">
              <div class="flex items-center">
                <svg class="w-6 h-6 text-primary-600 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 7a2 2 0 012 2m4 0a6 6 0 01-7.743 5.743L11 17H9v2H7v2H4a1 1 0 01-1-1v-2.586a1 1 0 01.293-.707l5.964-5.964A6 6 0 1121 9z" />
                </svg>
                <span class="font-medium">Security Keys</span>
              </div>
              <span 
                class="px-2 py-1 text-xs rounded"
                :class="webauthnCount > 0 ? 'bg-green-100 text-green-700 dark:bg-green-900 dark:text-green-300' : 'bg-gray-200 text-gray-600 dark:bg-gray-700 dark:text-gray-400'"
              >
                {{ webauthnCount > 0 ? `${webauthnCount} Key(s)` : 'None' }}
              </span>
            </div>
            <p class="text-sm text-gray-500 dark:text-gray-400 mb-3">
              Use a hardware security key or biometrics
            </p>
            <BaseButton 
              variant="primary" 
              size="sm" 
              @click="addSecurityKey"
              class="w-full"
            >
              {{ webauthnCount > 0 ? 'Add Another' : 'Add Key' }}
            </BaseButton>
          </div>

          <!-- Recovery Codes -->
          <div class="p-4 bg-gray-50 dark:bg-gray-800 rounded-lg">
            <div class="flex items-center justify-between mb-3">
              <div class="flex items-center">
                <svg class="w-6 h-6 text-primary-600 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                </svg>
                <span class="font-medium">Recovery Codes</span>
              </div>
              <span 
                class="px-2 py-1 text-xs rounded"
                :class="mfaStatus.recovery_codes_count > 0 ? 'bg-green-100 text-green-700 dark:bg-green-900 dark:text-green-300' : 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900 dark:text-yellow-300'"
              >
                {{ mfaStatus.recovery_codes_count }} remaining
              </span>
            </div>
            <p class="text-sm text-gray-500 dark:text-gray-400 mb-3">
              Backup codes for account recovery
            </p>
            <BaseButton 
              variant="secondary" 
              size="sm" 
              @click="regenerateRecoveryCodes"
              :disabled="!mfaStatus.enabled"
              class="w-full"
            >
              Regenerate
            </BaseButton>
          </div>
        </div>
      </BaseCard>

      <!-- Active Sessions -->
      <BaseCard>
        <template #header>
          <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">
            Active Sessions ({{ sessions.length }})
          </h2>
        </template>
        
        <div v-if="loading" class="text-center py-8 text-gray-500 dark:text-gray-400">
          <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full mx-auto mb-3"></div>
          <p>Loading sessions...</p>
        </div>
        
        <div v-else-if="sessions.length === 0" class="text-center py-12 text-gray-500 dark:text-gray-400">
          <svg class="w-12 h-12 mx-auto mb-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z" />
          </svg>
          <p class="text-lg font-medium mb-2">No active sessions</p>
          <p class="text-sm">You don't have any active sessions at the moment.</p>
        </div>
        
        <div v-else class="space-y-4">
          <div
            v-for="session in sessions"
            :key="session.id"
            class="border border-gray-200 dark:border-gray-700 rounded-lg p-4"
            :class="session.is_current ? 'border-primary-500 bg-primary-50 dark:bg-primary-900/20' : ''"
          >
            <div class="flex items-start justify-between">
              <div class="flex-1">
                <div class="flex items-center gap-3 mb-2">
                  <div class="w-10 h-10 rounded-lg bg-gray-100 dark:bg-gray-800 flex items-center justify-center">
                    <svg class="w-5 h-5 text-gray-600 dark:text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
                    </svg>
                  </div>
                  <div>
                    <div class="flex items-center gap-2">
                      <span class="font-medium text-gray-900 dark:text-gray-100">
                        {{ parseUserAgent(session.user_agent) }}
                      </span>
                      <span 
                        v-if="session.is_current" 
                        class="px-2 py-0.5 text-xs font-medium bg-primary-100 text-primary-800 dark:bg-primary-900 dark:text-primary-200 rounded"
                      >
                        Current
                      </span>
                      <span 
                        v-if="!session.is_active" 
                        class="px-2 py-0.5 text-xs font-medium bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200 rounded"
                      >
                        Expired
                      </span>
                    </div>
                    <p class="text-sm text-gray-500 dark:text-gray-400">
                      {{ session.sso_config_name || 'Direct Login' }}
                    </p>
                  </div>
                </div>
                
                <div class="ml-13 text-sm text-gray-600 dark:text-gray-400 space-y-1">
                  <p v-if="session.ip_address">
                    <span class="font-medium">IP:</span> {{ session.ip_address }}
                  </p>
                  <p>
                    <span class="font-medium">Signed in:</span> {{ formatDate(session.created_at) }}
                  </p>
                  <p>
                    <span class="font-medium">Last activity:</span> {{ formatRelative(session.last_activity_at) }}
                  </p>
                  <p>
                    <span class="font-medium">Expires:</span> {{ formatDate(session.expires_at) }}
                  </p>
                </div>
              </div>
              
              <button
                v-if="session.is_active && !session.is_current"
                @click="revokeSession(session)"
                class="p-2 text-gray-400 hover:text-red-600 dark:hover:text-red-400 transition-colors"
                title="Revoke Session"
              >
                <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>
          </div>
        </div>
      </BaseCard>

      <!-- TOTP Setup Modal -->
      <div v-if="showTotpModal" class="fixed inset-0 z-50 overflow-y-auto">
        <div class="flex items-center justify-center min-h-screen px-4">
          <div class="fixed inset-0 bg-black opacity-50" @click="closeTotpModal"></div>
          <div class="relative bg-white dark:bg-gray-800 rounded-lg shadow-xl max-w-md w-full p-6">
            <h3 class="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-4">
              Set Up Authenticator App
            </h3>
            
            <div v-if="totpSetup">
              <p class="text-sm text-gray-600 dark:text-gray-400 mb-4">
                Scan this QR code with your authenticator app:
              </p>
              
              <div class="flex justify-center mb-4">
                <img :src="totpQrCode" alt="TOTP QR Code" class="w-48 h-48" />
              </div>
              
              <p class="text-xs text-gray-500 dark:text-gray-400 mb-4 text-center">
                Or enter this key manually: <code class="bg-gray-100 dark:bg-gray-700 px-2 py-1 rounded">{{ totpSetup.secret }}</code>
              </p>
              
              <div class="mb-4">
                <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                  Enter the 6-digit code from your app:
                </label>
                <input
                  v-model="totpCode"
                  type="text"
                  maxlength="6"
                  pattern="[0-9]*"
                  class="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md dark:bg-gray-700 dark:text-gray-100 focus:ring-primary-500 focus:border-primary-500"
                  placeholder="000000"
                />
              </div>
              
              <div class="flex gap-3">
                <BaseButton variant="secondary" @click="closeTotpModal" class="flex-1">
                  Cancel
                </BaseButton>
                <BaseButton variant="primary" @click="confirmTotpSetup" :disabled="totpCode.length !== 6" class="flex-1">
                  Verify & Enable
                </BaseButton>
              </div>
            </div>
          </div>
        </div>
      </div>

      <!-- Recovery Codes Modal -->
      <div v-if="showRecoveryCodesModal" class="fixed inset-0 z-50 overflow-y-auto">
        <div class="flex items-center justify-center min-h-screen px-4">
          <div class="fixed inset-0 bg-black opacity-50" @click="closeRecoveryCodesModal"></div>
          <div class="relative bg-white dark:bg-gray-800 rounded-lg shadow-xl max-w-md w-full p-6">
            <h3 class="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-4">
              Recovery Codes
            </h3>
            
            <div class="bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-700 rounded-lg p-4 mb-4">
              <p class="text-sm text-yellow-800 dark:text-yellow-200">
                <strong>Important:</strong> Save these codes in a secure location. Each code can only be used once.
              </p>
            </div>
            
            <div class="grid grid-cols-2 gap-2 mb-4">
              <code 
                v-for="code in recoveryCodes" 
                :key="code"
                class="bg-gray-100 dark:bg-gray-700 px-3 py-2 rounded text-center font-mono text-sm"
              >
                {{ code }}
              </code>
            </div>
            
            <div class="flex gap-3">
              <BaseButton variant="secondary" @click="copyRecoveryCodes" class="flex-1">
                <svg class="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
                </svg>
                Copy
              </BaseButton>
              <BaseButton variant="primary" @click="closeRecoveryCodesModal" class="flex-1">
                Done
              </BaseButton>
            </div>
          </div>
        </div>
      </div>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue';
import axios from 'axios';
import QRCode from 'qrcode';
import AppLayout from '@/Layouts/AppLayout.vue';
import BaseCard from '@/components/BaseCard.vue';
import BaseButton from '@/components/BaseButton.vue';
import { useAuth } from '@/composables/useAuth';

const { user } = useAuth();

const loading = ref(false);
const sessions = ref([]);
const mfaStatus = ref({
  enabled: false,
  methods: [],
  has_recovery_codes: false,
  recovery_codes_count: 0,
});
const webauthnCount = ref(0);

// TOTP setup
const showTotpModal = ref(false);
const totpSetup = ref(null);
const totpQrCode = ref('');
const totpCode = ref('');

// Recovery codes
const showRecoveryCodesModal = ref(false);
const recoveryCodes = ref([]);

const hasTOTP = computed(() => mfaStatus.value.methods.includes('totp'));

const fetchSessions = async () => {
  loading.value = true;
  try {
    const response = await axios.get('/api/sso/sessions/my');
    sessions.value = response.data.sessions || [];
  } catch (error) {
    console.error('Failed to fetch sessions:', error);
    sessions.value = [];
  } finally {
    loading.value = false;
  }
};

const fetchMfaStatus = async () => {
  try {
    const response = await axios.get('/api/mfa/status');
    mfaStatus.value = response.data;
  } catch (error) {
    console.error('Failed to fetch MFA status:', error);
  }
};

const fetchWebAuthnCredentials = async () => {
  try {
    const response = await axios.get('/api/webauthn/credentials');
    webauthnCount.value = response.data.length;
  } catch (error) {
    console.error('Failed to fetch WebAuthn credentials:', error);
    webauthnCount.value = 0;
  }
};

const revokeSession = async (session) => {
  if (!confirm('Are you sure you want to revoke this session?')) return;
  
  try {
    await axios.post(`/api/sso/sessions/${session.id}/revoke`, { reason: 'user_revoke' });
    await fetchSessions();
  } catch (error) {
    console.error('Failed to revoke session:', error);
    alert('Failed to revoke session');
  }
};

const revokeAllSessions = async () => {
  if (!confirm('Are you sure you want to sign out from all devices? You will need to sign in again.')) return;
  
  try {
    await axios.post('/api/sso/sessions/revoke-all', { 
      user_id: user.value.id,
      reason: 'user_revoke_all' 
    });
    await fetchSessions();
    alert('All sessions have been revoked. Please sign in again.');
  } catch (error) {
    console.error('Failed to revoke all sessions:', error);
    alert('Failed to revoke sessions');
  }
};

const setupTOTP = async () => {
  try {
    const response = await axios.post('/api/mfa/totp/setup');
    totpSetup.value = response.data;
    totpQrCode.value = await QRCode.toDataURL(response.data.qr_code_url);
    showTotpModal.value = true;
    totpCode.value = '';
  } catch (error) {
    console.error('Failed to start TOTP setup:', error);
    alert('Failed to start TOTP setup');
  }
};

const confirmTotpSetup = async () => {
  try {
    const response = await axios.post('/api/mfa/totp/confirm', { 
      code: totpCode.value 
    });
    
    if (response.data.recovery_codes) {
      recoveryCodes.value = response.data.recovery_codes;
      showRecoveryCodesModal.value = true;
    }
    
    closeTotpModal();
    await fetchMfaStatus();
    alert('TOTP has been enabled successfully!');
  } catch (error) {
    console.error('Failed to confirm TOTP:', error);
    alert(error.response?.data?.message || 'Failed to verify code');
  }
};

const closeTotpModal = () => {
  showTotpModal.value = false;
  totpSetup.value = null;
  totpQrCode.value = '';
  totpCode.value = '';
};

const disableTOTP = async () => {
  if (!confirm('Are you sure you want to disable TOTP? This will make your account less secure.')) return;
  
  try {
    await axios.delete('/api/mfa/totp');
    await fetchMfaStatus();
  } catch (error) {
    console.error('Failed to disable TOTP:', error);
    alert('Failed to disable TOTP');
  }
};

const addSecurityKey = async () => {
  try {
    // Start registration
    const startResponse = await axios.post('/api/webauthn/register/start', { 
      name: prompt('Enter a name for this security key:', 'Security Key') 
    });
    
    // Use Web Authentication API
    const credential = await navigator.credentials.create({
      publicKey: startResponse.data
    });
    
    // Finish registration
    await axios.post('/api/webauthn/register/finish', {
      response: {
        id: credential.id,
        rawId: arrayBufferToBase64(credential.rawId),
        response: {
          clientDataJSON: arrayBufferToBase64(credential.response.clientDataJSON),
          attestationObject: arrayBufferToBase64(credential.response.attestationObject),
        },
        type: credential.type,
      }
    });
    
    await fetchWebAuthnCredentials();
    await fetchMfaStatus();
    alert('Security key added successfully!');
  } catch (error) {
    console.error('Failed to add security key:', error);
    if (error.name === 'NotAllowedError') {
      alert('Security key registration was cancelled');
    } else {
      alert('Failed to add security key');
    }
  }
};

const regenerateRecoveryCodes = async () => {
  if (!confirm('This will invalidate all existing recovery codes. Continue?')) return;
  
  try {
    const response = await axios.post('/api/mfa/recovery-codes');
    recoveryCodes.value = response.data.codes;
    showRecoveryCodesModal.value = true;
    await fetchMfaStatus();
  } catch (error) {
    console.error('Failed to regenerate recovery codes:', error);
    alert('Failed to regenerate recovery codes');
  }
};

const closeRecoveryCodesModal = () => {
  showRecoveryCodesModal.value = false;
  recoveryCodes.value = [];
};

const copyRecoveryCodes = () => {
  navigator.clipboard.writeText(recoveryCodes.value.join('\n'));
  alert('Recovery codes copied to clipboard');
};

const parseUserAgent = (ua) => {
  if (!ua) return 'Unknown Device';
  
  // Simple user agent parsing
  if (ua.includes('Chrome')) return 'Chrome Browser';
  if (ua.includes('Firefox')) return 'Firefox Browser';
  if (ua.includes('Safari')) return 'Safari Browser';
  if (ua.includes('Edge')) return 'Edge Browser';
  if (ua.includes('Mobile')) return 'Mobile Device';
  
  return 'Unknown Browser';
};

const formatDate = (date) => {
  return new Date(date).toLocaleString();
};

const formatRelative = (date) => {
  const now = new Date();
  const then = new Date(date);
  const diff = now - then;
  
  if (diff < 60000) return 'Just now';
  if (diff < 3600000) return `${Math.floor(diff / 60000)} minutes ago`;
  if (diff < 86400000) return `${Math.floor(diff / 3600000)} hours ago`;
  return formatDate(date);
};

const arrayBufferToBase64 = (buffer) => {
  const bytes = new Uint8Array(buffer);
  let binary = '';
  for (let i = 0; i < bytes.byteLength; i++) {
    binary += String.fromCharCode(bytes[i]);
  }
  return btoa(binary);
};

onMounted(() => {
  fetchSessions();
  fetchMfaStatus();
  fetchWebAuthnCredentials();
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
