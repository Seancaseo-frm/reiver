<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-[1200px] mx-auto px-8 py-6">
      <div class="mb-6">
        <h1 class="text-2xl font-bold text-gray-900 dark:text-gray-100">Herd Settings</h1>
        <p class="text-sm text-gray-500 dark:text-gray-400 mt-1">Configure pipeline controls, verification webhooks, and agent defaults</p>
      </div>

      <!-- Pipeline Controls -->
      <BaseCard class="mb-6">
        <div class="p-6">
          <h3 class="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-4">Pipeline Controls</h3>
          <p class="text-sm text-gray-500 dark:text-gray-400 mb-6">Enterprise controls applied to all A2A messages before delivery</p>

          <div class="space-y-6">
            <!-- PII Scrubbing -->
            <div class="flex items-center justify-between">
              <div>
                <h4 class="text-sm font-medium text-gray-900 dark:text-gray-100">PII Scrubbing</h4>
                <p class="text-xs text-gray-500 dark:text-gray-400 mt-1">Automatically redact personally identifiable information (emails, phone numbers, SSNs, credit cards) from messages</p>
              </div>
              <button @click="settings.pii_enabled = !settings.pii_enabled" class="relative inline-flex h-6 w-11 flex-shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors duration-200 ease-in-out focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2" :class="settings.pii_enabled ? 'bg-blue-600' : 'bg-gray-200 dark:bg-gray-600'">
                <span class="pointer-events-none inline-block h-5 w-5 transform rounded-full bg-white shadow ring-0 transition duration-200 ease-in-out" :class="settings.pii_enabled ? 'translate-x-5' : 'translate-x-0'"></span>
              </button>
            </div>

            <!-- Injection Detection -->
            <div class="flex items-center justify-between">
              <div>
                <h4 class="text-sm font-medium text-gray-900 dark:text-gray-100">Prompt Injection Detection</h4>
                <p class="text-xs text-gray-500 dark:text-gray-400 mt-1">Scan messages for prompt injection patterns and block suspicious content</p>
              </div>
              <div class="flex items-center space-x-3">
                <select v-model="settings.injection_mode" class="rounded-lg border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 text-sm shadow-sm focus:ring-blue-500 focus:border-blue-500">
                  <option value="off">Off</option>
                  <option value="warn">Warn (log only)</option>
                  <option value="block">Block</option>
                </select>
              </div>
            </div>
          </div>
        </div>
      </BaseCard>

      <!-- Verification Webhook -->
      <BaseCard class="mb-6">
        <div class="p-6">
          <h3 class="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-1">Verification Webhook</h3>
          <p class="text-sm text-gray-500 dark:text-gray-400 mb-6">Automatically approve or deny cross-organization agent access. When an agent from another organization wants to communicate with your agents, Herd will POST their organization owner's email to this URL. Return 200 to approve, 403 to deny.</p>

          <div v-if="webhookLoading" class="flex items-center justify-center py-8">
            <svg class="spinner w-5 h-5 text-blue-500" fill="none" viewBox="0 0 24 24">
              <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
              <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"></path>
            </svg>
          </div>

          <div v-else class="space-y-4">
            <div>
              <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Verification URL</label>
              <input v-model="webhook.verificationUrl" type="url" class="w-full rounded-lg border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 shadow-sm focus:ring-blue-500 focus:border-blue-500 text-sm font-mono" placeholder="https://your-server.com/verify-customer" />
            </div>

            <div>
              <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Webhook Secret</label>
              <p class="text-xs text-gray-500 dark:text-gray-400 mb-2">Used to sign webhook payloads with HMAC-SHA256. Verify the <code class="text-xs">X-Herd-Signature</code> header on your server.</p>

              <!-- Secret just generated: show it once -->
              <div v-if="revealedSecret" class="space-y-2">
                <div class="flex items-center space-x-2">
                  <input :value="revealedSecret" readonly class="flex-1 rounded-lg border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 shadow-sm text-sm font-mono bg-gray-50 dark:bg-gray-800" />
                  <button @click="copySecret" class="px-3 py-2 text-sm font-medium text-gray-700 dark:text-gray-300 bg-gray-100 dark:bg-gray-700 rounded-lg hover:bg-gray-200 dark:hover:bg-gray-600 transition-colors">
                    {{ copied ? 'Copied' : 'Copy' }}
                  </button>
                </div>
                <p class="text-xs text-amber-600 dark:text-amber-400">This secret will only be shown once. Copy it now.</p>
              </div>

              <!-- Secret exists but not revealed -->
              <div v-else-if="webhook.hasWebhookSecret" class="flex items-center space-x-3">
                <span class="text-sm text-gray-500 dark:text-gray-400 font-mono">••••••••••••••••</span>
                <button @click="regenerateSecret" :disabled="webhookSaving" class="px-3 py-1.5 text-xs font-medium text-amber-700 dark:text-amber-300 bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-800 rounded-lg hover:bg-amber-100 dark:hover:bg-amber-900/40 transition-colors disabled:opacity-50">
                  Regenerate
                </button>
              </div>

              <!-- No secret -->
              <div v-else>
                <button @click="regenerateSecret" :disabled="webhookSaving" class="px-3 py-1.5 text-sm font-medium text-blue-700 dark:text-blue-300 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg hover:bg-blue-100 dark:hover:bg-blue-900/40 transition-colors disabled:opacity-50">
                  Generate Secret
                </button>
              </div>
            </div>

            <div class="flex items-center justify-between pt-2">
              <button v-if="webhook.verificationUrl || webhook.hasWebhookSecret" @click="removeWebhook" :disabled="webhookSaving" class="px-3 py-1.5 text-sm font-medium text-red-600 dark:text-red-400 hover:text-red-700 dark:hover:text-red-300 transition-colors disabled:opacity-50">
                Remove Webhook
              </button>
              <div v-else></div>
              <button @click="saveWebhook" :disabled="webhookSaving || !webhook.verificationUrl" class="px-4 py-2 text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 rounded-lg transition-colors disabled:opacity-50 shadow-sm">
                {{ webhookSaving ? 'Saving...' : 'Save Webhook' }}
              </button>
            </div>
          </div>
        </div>
      </BaseCard>

      <!-- Default Agent Settings -->
      <BaseCard class="mb-6">
        <div class="p-6">
          <h3 class="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-4">Default Agent Settings</h3>
          <p class="text-sm text-gray-500 dark:text-gray-400 mb-6">Defaults applied when registering new agents</p>

          <div class="space-y-4">
            <div>
              <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Default Visibility</label>
              <select v-model="settings.default_visibility" class="w-full max-w-xs rounded-lg border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-gray-100 text-sm shadow-sm focus:ring-blue-500 focus:border-blue-500">
                <option value="private">Private (same project)</option>
                <option value="org">Organization</option>
                <option value="public">Public</option>
              </select>
            </div>
          </div>
        </div>
      </BaseCard>

      <!-- Push Notification Defaults -->
      <BaseCard>
        <div class="p-6">
          <h3 class="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-4">Push Notifications</h3>
          <p class="text-sm text-gray-500 dark:text-gray-400 mb-6">Webhook delivery settings for asynchronous task updates</p>

          <div class="space-y-4">
            <div class="flex items-center justify-between">
              <div>
                <h4 class="text-sm font-medium text-gray-900 dark:text-gray-100">HMAC Signatures</h4>
                <p class="text-xs text-gray-500 dark:text-gray-400 mt-1">All webhook deliveries include HMAC-SHA256 signature in X-A2A-Signature header</p>
              </div>
              <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300">Always On</span>
            </div>
            <div class="flex items-center justify-between">
              <div>
                <h4 class="text-sm font-medium text-gray-900 dark:text-gray-100">Retry Policy</h4>
                <p class="text-xs text-gray-500 dark:text-gray-400 mt-1">Exponential backoff (5s, 10s, 20s... max 1h), 7-day TTL</p>
              </div>
              <span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300">Automatic</span>
            </div>
          </div>
        </div>
      </BaseCard>

      <!-- Save Button (for pipeline + defaults) -->
      <div class="mt-6 flex justify-end">
        <button @click="saveSettings" :disabled="saving" class="px-6 py-2.5 text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 rounded-lg transition-colors disabled:opacity-50 shadow-sm">
          {{ saving ? 'Saving...' : 'Save Settings' }}
        </button>
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
const saving = ref(false);

const settings = ref({
  pii_enabled: true,
  injection_mode: 'block',
  default_visibility: 'org',
});

// --- Verification Webhook ---
const webhookLoading = ref(true);
const webhookSaving = ref(false);
const revealedSecret = ref(null);
const copied = ref(false);

const webhook = ref({
  verificationUrl: '',
  hasWebhookSecret: false,
});

const fetchWebhookSettings = async () => {
  webhookLoading.value = true;
  try {
    const res = await axios.get(`/api/projects/${projectId.value}/herd/settings/verification`);
    webhook.value = {
      verificationUrl: res.data.verificationUrl || '',
      hasWebhookSecret: res.data.hasWebhookSecret || false,
    };
  } catch (error) {
    if (error.response?.status !== 404) {
      console.error('Failed to fetch webhook settings:', error);
    }
  } finally {
    webhookLoading.value = false;
  }
};

const saveWebhook = async () => {
  webhookSaving.value = true;
  try {
    const res = await axios.put(`/api/projects/${projectId.value}/herd/settings/verification`, {
      verificationUrl: webhook.value.verificationUrl || null,
    });
    webhook.value.hasWebhookSecret = res.data.hasWebhookSecret;
  } catch (error) {
    console.error('Failed to save webhook:', error);
    alert(error.response?.data || 'Failed to save webhook settings');
  } finally {
    webhookSaving.value = false;
  }
};

const regenerateSecret = async () => {
  webhookSaving.value = true;
  revealedSecret.value = null;
  try {
    const res = await axios.post(`/api/projects/${projectId.value}/herd/settings/verification/regenerate-secret`);
    revealedSecret.value = res.data.webhookSecret;
    webhook.value.hasWebhookSecret = true;
    copied.value = false;
  } catch (error) {
    console.error('Failed to regenerate secret:', error);
    alert(error.response?.data || 'Failed to regenerate secret');
  } finally {
    webhookSaving.value = false;
  }
};

const removeWebhook = async () => {
  if (!confirm('Remove the verification webhook? Cross-org access will require manual approval.')) return;
  webhookSaving.value = true;
  try {
    await axios.delete(`/api/projects/${projectId.value}/herd/settings/verification`);
    webhook.value = { verificationUrl: '', hasWebhookSecret: false };
    revealedSecret.value = null;
  } catch (error) {
    console.error('Failed to remove webhook:', error);
    alert(error.response?.data || 'Failed to remove webhook');
  } finally {
    webhookSaving.value = false;
  }
};

const copySecret = async () => {
  if (revealedSecret.value) {
    await navigator.clipboard.writeText(revealedSecret.value);
    copied.value = true;
    setTimeout(() => { copied.value = false; }, 2000);
  }
};

// --- Pipeline settings (localStorage for now) ---
const saveSettings = async () => {
  saving.value = true;
  try {
    localStorage.setItem(`herd_settings_${projectId.value}`, JSON.stringify(settings.value));
    alert('Settings saved');
  } finally {
    saving.value = false;
  }
};

const loadSettings = () => {
  try {
    const stored = localStorage.getItem(`herd_settings_${projectId.value}`);
    if (stored) {
      settings.value = { ...settings.value, ...JSON.parse(stored) };
    }
  } catch { /* ignore */ }
};

onMounted(async () => {
  await fetchUser();
  loadSettings();
  await fetchWebhookSettings();
});

watch(projectId, async () => {
  loadSettings();
  await fetchWebhookSettings();
});
</script>

<style scoped>
.spinner { animation: spin 1s linear infinite; }
@keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
</style>
