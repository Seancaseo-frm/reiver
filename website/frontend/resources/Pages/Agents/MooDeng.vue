<template>
  <AppLayout :project="project">
    <div class="max-w-[800px] mx-auto px-4 py-6 space-y-6">
      <div class="flex items-center justify-between">
        <div>
          <h1 class="text-2xl font-bold text-gray-900 dark:text-white">MooDeng</h1>
          <p class="mt-1 text-sm text-gray-500 dark:text-gray-400">Configure the in-app AI agent</p>
        </div>
        <label class="relative inline-flex items-center cursor-pointer">
          <input v-model="settings.agent_enabled" type="checkbox" class="sr-only peer" />
          <div class="w-11 h-6 bg-gray-200 dark:bg-gray-700 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-primary-500 rounded-full peer peer-checked:after:translate-x-full rtl:peer-checked:after:-translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-primary-600"></div>
        </label>
      </div>

      <transition name="fade">
        <span v-if="saveStatus === 'saving'" class="inline-flex items-center gap-1.5 text-xs text-gray-400">
          <svg class="w-3.5 h-3.5 animate-spin" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4" /><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" /></svg>
          Saving…
        </span>
        <span v-else-if="saveStatus === 'saved'" class="inline-flex items-center gap-1 text-xs text-green-600 dark:text-green-400">
          <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" /></svg>
          Saved
        </span>
        <span v-else-if="saveStatus === 'error'" class="inline-flex items-center gap-1 text-xs text-red-600 dark:text-red-400">
          Save failed
        </span>
      </transition>

      <div v-if="loading" class="text-sm text-gray-500 dark:text-gray-400">Loading…</div>

      <template v-else-if="settingsLoaded">
        <BaseCard>
          <div class="p-4 space-y-4">
            <p class="text-sm text-gray-600 dark:text-gray-400">
              The in-app AI agent can help users navigate the platform, query data, and perform actions.
            </p>

            <div v-if="!hasIntegrations" class="bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-lg p-3">
              <p class="text-sm text-yellow-800 dark:text-yellow-200">
                Add at least one AI provider integration to use the agent.
                <router-link :to="`/p/${projectId}/llm/integrations`" class="font-medium underline hover:text-yellow-900 dark:hover:text-yellow-100">
                  Go to Integrations
                </router-link>
              </p>
            </div>

            <div v-if="settings.agent_enabled" class="space-y-5">
              <div>
                <h3 class="text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Agent Permissions</h3>
                <p class="text-sm text-gray-600 dark:text-gray-400 mb-3">Control which actions the AI agent can perform</p>
                <ScopeSelector v-model="settings.agent_scopes" :max-scopes="AGENT_SCOPES_MAX" />
              </div>

              <div class="border-t border-gray-200 dark:border-gray-700 pt-4">
                <div class="flex items-center justify-between">
                  <div>
                    <h3 class="text-sm font-medium text-gray-900 dark:text-gray-100">Auto-Investigate Alerts &amp; Exceptions</h3>
                    <p class="text-sm text-gray-500 dark:text-gray-400 mt-0.5">
                      When enabled, MooDeng automatically investigates alert firings and new exceptions,
                      then posts findings to your notification channels.
                    </p>
                  </div>
                  <label class="relative inline-flex items-center cursor-pointer flex-shrink-0 ml-4">
                    <input v-model="settings.auto_investigate" type="checkbox" class="sr-only peer" />
                    <div class="w-11 h-6 bg-gray-200 dark:bg-gray-700 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-primary-500 rounded-full peer peer-checked:after:translate-x-full rtl:peer-checked:after:-translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-primary-600"></div>
                  </label>
                </div>
              </div>
            </div>
          </div>
        </BaseCard>
      </template>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue';
import { useRoute } from 'vue-router';
import axios from 'axios';
import AppLayout from '@/Layouts/AppLayout.vue';
import BaseCard from '@/components/BaseCard.vue';
import ScopeSelector from '@/components/ScopeSelector.vue';

const route = useRoute();
const projectId = computed(() => route.params.id);
const project = computed(() => ({ id: projectId.value }));

const loading = ref(true);
const saveStatus = ref('idle');
let saveStatusTimer = null;

const AGENT_SCOPES_MAX = [
  'project:read', 'project:write',
  'llm:read', 'llm:write',
  'observability:read', 'observability:write',
  'herd:read', 'herd:write',
];

const DEFAULT_AGENT_SCOPES = [
  'project:read', 'llm:read', 'observability:read', 'herd:read',
];

const settings = ref({
  agent_enabled: true,
  agent_scopes: [...DEFAULT_AGENT_SCOPES],
  auto_investigate: false,
});
const settingsLoaded = ref(false);

const hasIntegrations = ref(true);
let fullSettings = null;

async function fetchSettings() {
  loading.value = true;
  settingsLoaded.value = false;
  try {
    const { data } = await axios.get(`/api/projects/${projectId.value}/llm/settings`);
    fullSettings = data;
    settings.value.agent_enabled = data.agent_enabled ?? true;
    settings.value.agent_scopes = Array.isArray(data.agent_scopes) ? data.agent_scopes : [...DEFAULT_AGENT_SCOPES];
    settings.value.auto_investigate = data.auto_investigate ?? false;
    settingsLoaded.value = true;
  } catch {
    fullSettings = {};
  } finally {
    loading.value = false;
  }
}

async function fetchIntegrations() {
  try {
    const { data } = await axios.get(`/api/projects/${projectId.value}/llm/integrations`);
    hasIntegrations.value = Array.isArray(data) && data.length > 0;
  } catch {
    hasIntegrations.value = false;
  }
}

async function save() {
  if (!settingsLoaded.value) return;
  clearTimeout(saveStatusTimer);
  saveStatus.value = 'saving';
  try {
    const payload = { ...settings.value };
    await axios.put(`/api/projects/${projectId.value}/llm/settings`, payload);
    fullSettings = payload;
    saveStatus.value = 'saved';
    saveStatusTimer = setTimeout(() => { saveStatus.value = 'idle'; }, 2000);
  } catch {
    saveStatus.value = 'error';
    saveStatusTimer = setTimeout(() => { saveStatus.value = 'idle'; }, 4000);
  }
}

let debounceTimer = null;
watch(settings, () => {
  if (loading.value || !settingsLoaded.value) return;
  clearTimeout(debounceTimer);
  debounceTimer = setTimeout(save, 800);
}, { deep: true });

onMounted(() => {
  fetchSettings();
  fetchIntegrations();
});
</script>
