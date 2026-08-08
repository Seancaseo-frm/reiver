<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-[1200px] mx-auto px-8 py-6">
      <!-- Header -->
      <div class="mb-6">
        <div class="flex items-center gap-3">
          <h1 class="text-2xl font-semibold text-gray-900 dark:text-gray-100">Guardrails</h1>
          <transition name="fade">
            <span v-if="saveStatus === 'saving'" class="inline-flex items-center gap-1.5 text-xs text-gray-400 dark:text-gray-500">
              <svg class="w-3.5 h-3.5 animate-spin" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4" /><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" /></svg>
              Saving…
            </span>
            <span v-else-if="saveStatus === 'saved'" class="inline-flex items-center gap-1 text-xs text-green-600 dark:text-green-400">
              <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" /></svg>
              Saved
            </span>
            <span v-else-if="saveStatus === 'error'" class="inline-flex items-center gap-1 text-xs text-red-600 dark:text-red-400">
              <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" /></svg>
              Save failed
            </span>
          </transition>
        </div>
        <p class="text-sm text-gray-500 dark:text-gray-400 mt-1">Content safety controls applied by the gateway before and after LLM calls.</p>
      </div>

      <!-- Error Message -->
      <div v-if="errorMessage" class="mb-6 p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg flex items-center justify-between max-w-3xl">
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

      <div v-if="loading" class="text-center py-12">
        <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full mx-auto mb-3"></div>
        <p class="text-gray-500 dark:text-gray-400">Loading settings...</p>
      </div>

      <div v-else class="max-w-3xl">
        <!-- Tab bar -->
        <div class="border-b border-gray-200 dark:border-gray-700 mb-6">
          <nav class="-mb-px flex gap-6" aria-label="Guardrails tabs">
            <button
              v-for="tab in tabs"
              :key="tab.id"
              type="button"
              class="whitespace-nowrap border-b-2 pb-2 text-sm font-medium transition-colors"
              :class="activeTab === tab.id
                ? 'border-primary-600 text-primary-600 dark:text-primary-400 dark:border-primary-400'
                : 'border-transparent text-gray-500 hover:border-gray-300 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200 dark:hover:border-gray-500'"
              @click="activeTab = tab.id"
            >
              {{ tab.label }}
            </button>
          </nav>
        </div>

        <!-- ========== Trust & Injection ========== -->
        <div v-if="activeTab === 'trust'" class="space-y-6">
          <BaseCard>
            <template #header>
              <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">Trust & Injection Protection</h2>
            </template>
            <div class="space-y-6">
              <p class="text-sm text-gray-600 dark:text-gray-400">
                Control which message roles are treated as untrusted and enable injection defences.
              </p>

              <!-- Trust Mode -->
              <div>
                <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">Trust Mode</label>
                <p class="text-xs text-gray-500 dark:text-gray-400 mb-2">
                  Controls which message roles are treated as untrusted for injection detection and spotlighting.
                </p>
                <div class="flex flex-col gap-2">
                  <label class="flex items-start gap-3 p-3 rounded-lg border border-gray-200 dark:border-gray-600 cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-700/50">
                    <input v-model="guardrailTrustMode" type="radio" value="" class="mt-1 form-radio text-primary-600" />
                    <div>
                      <span class="font-medium text-gray-900 dark:text-gray-100">Off</span>
                      <p class="text-xs text-gray-500 dark:text-gray-400 mt-0.5">No role-based scanning. Injection detection and spotlighting are disabled.</p>
                    </div>
                  </label>
                  <label class="flex items-start gap-3 p-3 rounded-lg border border-gray-200 dark:border-gray-600 cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-700/50">
                    <input v-model="guardrailTrustMode" type="radio" value="agent" class="mt-1 form-radio text-primary-600" />
                    <div>
                      <span class="font-medium text-gray-900 dark:text-gray-100">Agent Mode</span>
                      <p class="text-xs text-gray-500 dark:text-gray-400 mt-0.5">Your app owns the agent. External data arrives via tool results. Untrusted: <code class="text-xs bg-gray-100 dark:bg-gray-700 px-1 rounded">tool</code> messages.</p>
                    </div>
                  </label>
                  <label class="flex items-start gap-3 p-3 rounded-lg border border-gray-200 dark:border-gray-600 cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-700/50">
                    <input v-model="guardrailTrustMode" type="radio" value="chatbot" class="mt-1 form-radio text-primary-600" />
                    <div>
                      <span class="font-medium text-gray-900 dark:text-gray-100">Chatbot Mode</span>
                      <p class="text-xs text-gray-500 dark:text-gray-400 mt-0.5">Users are external and untrusted. Untrusted: <code class="text-xs bg-gray-100 dark:bg-gray-700 px-1 rounded">user</code> + <code class="text-xs bg-gray-100 dark:bg-gray-700 px-1 rounded">tool</code> messages.</p>
                    </div>
                  </label>
                </div>
              </div>

              <!-- Prompt Injection Detection -->
              <div class="flex items-center justify-between">
                <div>
                  <p class="text-sm font-medium text-gray-700 dark:text-gray-300">Prompt Injection Detection</p>
                  <p class="text-xs text-gray-500 dark:text-gray-400">Scan untrusted messages for known injection patterns, obfuscation, and encoded payloads. Requires a trust mode to be set.</p>
                </div>
                <label class="relative inline-flex items-center cursor-pointer" :class="{ 'opacity-50 cursor-not-allowed': !guardrailTrustMode }">
                  <input v-model="settings.guardrails.prompt_injection_detection" type="checkbox" class="sr-only peer" :disabled="!guardrailTrustMode" />
                  <div class="w-11 h-6 bg-gray-200 dark:bg-gray-700 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-primary-500 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-primary-600"></div>
                </label>
              </div>

              <!-- Spotlighting -->
              <div class="flex items-center justify-between">
                <div>
                  <p class="text-sm font-medium text-gray-700 dark:text-gray-300">Input Spotlighting</p>
                  <p class="text-xs text-gray-500 dark:text-gray-400">Wrap untrusted messages in delimiters and inject a canary system instruction. Requires a trust mode to be set.</p>
                </div>
                <label class="relative inline-flex items-center cursor-pointer" :class="{ 'opacity-50 cursor-not-allowed': !guardrailTrustMode }">
                  <input v-model="settings.guardrails.spotlighting_enabled" type="checkbox" class="sr-only peer" :disabled="!guardrailTrustMode" />
                  <div class="w-11 h-6 bg-gray-200 dark:bg-gray-700 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-primary-500 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-primary-600"></div>
                </label>
              </div>
            </div>
          </BaseCard>
        </div>

        <!-- ========== Input ========== -->
        <div v-if="activeTab === 'input'" class="space-y-6">
          <BaseCard>
            <template #header>
              <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">Input Guardrails</h2>
            </template>
            <div class="space-y-6">
              <p class="text-sm text-gray-600 dark:text-gray-400">
                Controls applied to incoming prompts before they reach the LLM.
              </p>

              <div>
                <label class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Blocked Input Topics (comma-separated)</label>
                <input
                  v-model="blockedInputTopicsInput"
                  type="text"
                  class="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 text-sm"
                  placeholder="competitor, internal-only"
                />
              </div>

              <div class="flex items-center gap-4">
                <label class="text-sm text-gray-700 dark:text-gray-300">Max Prompt Tokens:</label>
                <input
                  v-model.number="settings.guardrails.max_prompt_tokens"
                  type="number"
                  min="0"
                  step="1000"
                  placeholder="No limit"
                  class="w-32 px-3 py-1.5 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 text-sm"
                />
              </div>

              <div class="flex items-center justify-between">
                <div>
                  <p class="text-sm font-medium text-gray-700 dark:text-gray-300">Block on PII Detection</p>
                  <p class="text-xs text-gray-500 dark:text-gray-400">Reject requests with PII instead of redacting</p>
                </div>
                <label class="relative inline-flex items-center cursor-pointer">
                  <input v-model="settings.guardrails.pii_block_on_detect" type="checkbox" class="sr-only peer" />
                  <div class="w-11 h-6 bg-gray-200 dark:bg-gray-700 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-primary-500 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-primary-600"></div>
                </label>
              </div>
            </div>
          </BaseCard>
        </div>

        <!-- ========== Output ========== -->
        <div v-if="activeTab === 'output'" class="space-y-6">
          <BaseCard>
            <template #header>
              <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">Output Guardrails</h2>
            </template>
            <div class="space-y-6">
              <p class="text-sm text-gray-600 dark:text-gray-400">
                Controls applied to LLM responses before they reach the client.
              </p>

              <!-- Exfiltration Scanning -->
              <div class="flex items-center justify-between">
                <div>
                  <p class="text-sm font-medium text-gray-700 dark:text-gray-300">Output Exfiltration Scanning</p>
                  <p class="text-xs text-gray-500 dark:text-gray-400">Block responses containing markdown/HTML images with external URLs that could exfiltrate data.</p>
                </div>
                <label class="relative inline-flex items-center cursor-pointer">
                  <input v-model="settings.guardrails.block_exfiltration_urls" type="checkbox" class="sr-only peer" />
                  <div class="w-11 h-6 bg-gray-200 dark:bg-gray-700 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-primary-500 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-primary-600"></div>
                </label>
              </div>

              <!-- Mask Output PII -->
              <div class="flex items-center justify-between">
                <div>
                  <p class="text-sm font-medium text-gray-700 dark:text-gray-300">Mask Output PII</p>
                  <p class="text-xs text-gray-500 dark:text-gray-400">Redact PII from response content before returning to client</p>
                </div>
                <label class="relative inline-flex items-center cursor-pointer">
                  <input v-model="settings.guardrails.mask_output_pii" type="checkbox" class="sr-only peer" />
                  <div class="w-11 h-6 bg-gray-200 dark:bg-gray-700 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-primary-500 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-primary-600"></div>
                </label>
              </div>

              <div>
                <label class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Blocked Output Topics (comma-separated)</label>
                <input
                  v-model="blockedOutputTopicsInput"
                  type="text"
                  class="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 text-sm"
                  placeholder="confidential, internal-only"
                />
              </div>

              <div class="flex items-center gap-4">
                <label class="text-sm text-gray-700 dark:text-gray-300">Min Quality Score:</label>
                <input
                  v-model.number="settings.guardrails.min_quality_score"
                  type="number"
                  min="0"
                  max="1"
                  step="0.05"
                  placeholder="Disabled"
                  class="w-32 px-3 py-1.5 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 text-sm"
                />
                <span class="text-xs text-gray-500 dark:text-gray-400">0.0 – 1.0 (rollout quality gate threshold)</span>
              </div>
            </div>
          </BaseCard>
        </div>

        <!-- ========== Tools ========== -->
        <div v-if="activeTab === 'tools'" class="space-y-6">
          <BaseCard>
            <template #header>
              <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">Tool Restrictions</h2>
            </template>
            <div class="space-y-6">
              <p class="text-sm text-gray-600 dark:text-gray-400">
                Block specific tools from being called, regardless of prompt configuration.
              </p>

              <div>
                <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Blocked Tools (project-wide)</label>
                <p class="text-xs text-gray-500 dark:text-gray-400 mb-2">
                  Comma-separated tool names to always block. If the LLM returns a tool call matching these names, the response is rejected.
                </p>
                <input
                  v-model="blockedToolsInput"
                  type="text"
                  class="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 text-sm"
                  placeholder="send_email, delete_file, execute_code"
                />
              </div>
            </div>
          </BaseCard>
        </div>

        <!-- ========== Judge ========== -->
        <div v-if="activeTab === 'judge'" class="space-y-6">
          <BaseCard>
            <template #header>
              <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">LLM-as-Judge</h2>
            </template>
            <div class="space-y-6">
              <p class="text-sm text-gray-600 dark:text-gray-400">
                Evaluate a sample of prompt-config requests with an LLM judge. Used for prompt version quality comparison and rollout quality gates.
              </p>

              <div class="flex items-center gap-4">
                <label class="text-sm text-gray-700 dark:text-gray-300">Judge Sample Rate:</label>
                <div class="flex items-center gap-2">
                  <input
                    v-model.number="judgeSamplePercent"
                    type="number"
                    min="0"
                    max="100"
                    step="1"
                    placeholder="0"
                    class="w-20 px-3 py-1.5 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 text-sm"
                  />
                  <span class="text-sm text-gray-500 dark:text-gray-400">%</span>
                </div>
                <span class="text-xs text-gray-500 dark:text-gray-400">0 = disabled, 1-5% recommended</span>
              </div>
            </div>
          </BaseCard>
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
import { useAuth } from '@/composables/useAuth';

const route = useRoute();
const { user, fetchUser } = useAuth();

const projectId = computed(() => route.params.id);
const project = computed(() => ({ id: projectId.value }));
const loading = ref(true);
const errorMessage = ref('');
const saveStatus = ref('idle');
const activeTab = ref('trust');

const tabs = [
  { id: 'trust', label: 'Trust & Injection' },
  { id: 'input', label: 'Input' },
  { id: 'output', label: 'Output' },
  { id: 'tools', label: 'Tools' },
  { id: 'judge', label: 'Judge' },
];
let saveStatusTimer = null;

const getErrorMessage = (error, fallback = 'An error occurred') => {
  if (error.response?.data?.error) return error.response.data.error;
  if (error.response?.data?.message) return error.response.data.message;
  if (error.message) return error.message;
  return fallback;
};

const defaultGuardrails = {
  trust_mode: null,
  blocked_input_topics: [],
  max_prompt_tokens: null,
  pii_block_on_detect: false,
  prompt_injection_detection: false,
  spotlighting_enabled: false,
  mask_output_pii: false,
  blocked_output_topics: [],
  min_quality_score: null,
  blocked_tools: [],
  block_exfiltration_urls: false,
};

const settings = ref({
  guardrails: { ...defaultGuardrails },
  judge_sample_rate: null,
});
const originalSettings = ref(JSON.parse(JSON.stringify(settings.value)));

const guardrailTrustMode = computed({
  get: () => settings.value.guardrails?.trust_mode || '',
  set: (val) => {
    if (!settings.value.guardrails) settings.value.guardrails = { ...defaultGuardrails };
    settings.value.guardrails.trust_mode = val || null;
    if (!val) {
      settings.value.guardrails.prompt_injection_detection = false;
      settings.value.guardrails.spotlighting_enabled = false;
    }
  },
});

const blockedToolsInput = computed({
  get: () => (settings.value.guardrails?.blocked_tools || []).join(', '),
  set: (val) => {
    if (!settings.value.guardrails) settings.value.guardrails = { ...defaultGuardrails };
    settings.value.guardrails.blocked_tools = val
      ? val.split(',').map(t => t.trim()).filter(Boolean)
      : [];
  },
});

const blockedInputTopicsInput = computed({
  get: () => (settings.value.guardrails?.blocked_input_topics || []).join(', '),
  set: (val) => {
    if (!settings.value.guardrails) settings.value.guardrails = { ...defaultGuardrails };
    settings.value.guardrails.blocked_input_topics = val
      ? val.split(',').map(t => t.trim()).filter(Boolean)
      : [];
  },
});

const blockedOutputTopicsInput = computed({
  get: () => (settings.value.guardrails?.blocked_output_topics || []).join(', '),
  set: (val) => {
    if (!settings.value.guardrails) settings.value.guardrails = { ...defaultGuardrails };
    settings.value.guardrails.blocked_output_topics = val
      ? val.split(',').map(t => t.trim()).filter(Boolean)
      : [];
  },
});

const judgeSamplePercent = computed({
  get: () => {
    const rate = settings.value.judge_sample_rate;
    return rate != null ? Math.round(rate * 100) : null;
  },
  set: (val) => {
    if (val == null || val === '' || val === 0) {
      settings.value.judge_sample_rate = null;
    } else {
      settings.value.judge_sample_rate = Math.min(100, Math.max(0, val)) / 100;
    }
  },
});

const fetchSettings = async () => {
  loading.value = true;
  errorMessage.value = '';
  try {
    const response = await axios.get(`/api/projects/${projectId.value}/llm/settings`);
    const data = response.data || {};
    const guardrails = { ...defaultGuardrails, ...(data.guardrails || {}) };
    settings.value = {
      guardrails,
      judge_sample_rate: data.judge_sample_rate ?? null,
    };
    originalSettings.value = JSON.parse(JSON.stringify(settings.value));
  } catch (error) {
    errorMessage.value = getErrorMessage(error, 'Failed to fetch guardrail settings');
    settings.value = { guardrails: { ...defaultGuardrails }, judge_sample_rate: null };
    originalSettings.value = JSON.parse(JSON.stringify(settings.value));
  } finally {
    loading.value = false;
  }
};

const saveSettings = async () => {
  clearTimeout(saveStatusTimer);
  saveStatus.value = 'saving';
  errorMessage.value = '';
  try {
    await axios.put(`/api/projects/${projectId.value}/llm/settings`, settings.value);
    originalSettings.value = JSON.parse(JSON.stringify(settings.value));
    saveStatus.value = 'saved';
    saveStatusTimer = setTimeout(() => { saveStatus.value = 'idle'; }, 2000);
  } catch (error) {
    errorMessage.value = getErrorMessage(error, 'Failed to save guardrail settings');
    saveStatus.value = 'error';
    saveStatusTimer = setTimeout(() => { saveStatus.value = 'idle'; }, 4000);
  }
};

let debounceTimer = null;
watch(settings, () => {
  if (loading.value) return;
  if (JSON.stringify(settings.value) === JSON.stringify(originalSettings.value)) return;
  clearTimeout(debounceTimer);
  debounceTimer = setTimeout(saveSettings, 800);
}, { deep: true });

watch(projectId, () => { fetchSettings(); });

onMounted(async () => {
  await fetchUser();
  fetchSettings();
});
</script>

<style scoped>
.spinner { animation: spin 1s linear infinite; }
@keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
.fade-enter-active, .fade-leave-active { transition: opacity 0.3s ease; }
.fade-enter-from, .fade-leave-to { opacity: 0; }
</style>
