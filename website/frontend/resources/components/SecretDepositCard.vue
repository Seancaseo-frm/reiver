<template>
  <div class="max-w-[85%] rounded-lg border border-gray-200 dark:border-gray-700 overflow-hidden text-sm">
    <!-- Header -->
    <div class="flex items-center gap-2 px-3 py-2 bg-gray-50 dark:bg-gray-800">
      <svg class="w-4 h-4 flex-shrink-0" :class="statusIcon.color" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" :d="statusIcon.path" />
      </svg>
      <span class="font-medium text-gray-700 dark:text-gray-300 text-xs">{{ headerText }}</span>
    </div>

    <!-- Deposit form (only when slot is pending) -->
    <div v-if="cardState === 'pending'" class="px-3 py-3 border-t border-gray-200 dark:border-gray-700">
      <p class="text-xs text-gray-500 dark:text-gray-400 mb-2">
        Enter your {{ providerLabel }} securely. This value will NOT be visible to the AI agent.
      </p>
      <form @submit.prevent="submitSecret" class="flex gap-2">
        <input
          ref="inputEl"
          type="password"
          :placeholder="placeholder"
          autocomplete="off"
          data-lpignore="true"
          class="flex-1 px-2 py-1.5 text-xs rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-900 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-1 focus:ring-primary-500"
          :disabled="submitting"
        />
        <button
          type="submit"
          :disabled="submitting"
          class="px-3 py-1.5 text-xs font-medium rounded bg-primary-600 hover:bg-primary-700 text-white disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
        >
          {{ submitting ? 'Saving...' : 'Save' }}
        </button>
      </form>
      <p v-if="errorMsg" class="text-xs text-red-500 mt-1.5">{{ errorMsg }}</p>
    </div>

    <!-- Success state -->
    <div v-else-if="cardState === 'saved'" class="px-3 py-2 border-t border-gray-200 dark:border-gray-700">
      <p class="text-xs text-green-600 dark:text-green-400">Secret saved securely.</p>
    </div>

    <!-- Expired / already used -->
    <div v-else class="px-3 py-2 border-t border-gray-200 dark:border-gray-700">
      <p class="text-xs text-gray-400">This deposit link has expired or was already used.</p>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue';
import axios from 'axios';

const props = defineProps({
  slotId: { type: String, required: true },
  projectId: { type: String, required: true },
  purpose: { type: String, default: '' },
  provider: { type: String, default: null },
  providerName: { type: String, default: null },
  depositUrl: { type: String, default: '' },
  expiresAt: { type: String, default: '' },
  initialState: { type: String, default: 'pending' },
});

const emit = defineEmits(['deposited']);

const cardState = ref(props.initialState);
const submitting = ref(false);
const errorMsg = ref('');
const inputEl = ref(null);

const providerLabel = computed(() => {
  if (props.provider) {
    const name = props.providerName
      || props.provider.charAt(0).toUpperCase() + props.provider.slice(1);
    return name + ' API key';
  }
  return props.purpose || 'secret';
});

const placeholder = computed(() => `Paste your ${providerLabel.value}...`);

const headerText = computed(() => {
  if (cardState.value === 'saved') return 'Secret deposited';
  if (cardState.value === 'expired') return 'Deposit link expired';
  return `Secure deposit: ${providerLabel.value}`;
});

const statusIcon = computed(() => {
  if (cardState.value === 'saved') {
    return { color: 'text-green-500', path: 'M9 12.75L11.25 15 15 9.75M21 12a9 9 0 11-18 0 9 9 0 0118 0z' };
  }
  if (cardState.value === 'expired') {
    return { color: 'text-gray-400', path: 'M12 9v3.75m9-.75a9 9 0 11-18 0 9 9 0 0118 0zm-9 3.75h.008v.008H12v-.008z' };
  }
  return { color: 'text-primary-500', path: 'M16.5 10.5V6.75a4.5 4.5 0 10-9 0v3.75m-.75 11.25h10.5a2.25 2.25 0 002.25-2.25v-6.75a2.25 2.25 0 00-2.25-2.25H6.75a2.25 2.25 0 00-2.25 2.25v6.75a2.25 2.25 0 002.25 2.25z' };
});

onMounted(() => {
  if (props.expiresAt && cardState.value === 'pending') {
    const expiryMs = new Date(props.expiresAt).getTime() - Date.now();
    if (expiryMs <= 0) {
      cardState.value = 'expired';
    } else {
      setTimeout(() => {
        if (cardState.value === 'pending') cardState.value = 'expired';
      }, expiryMs);
    }
  }
});

async function submitSecret() {
  const value = inputEl.value?.value?.trim();
  if (!value) {
    errorMsg.value = 'Please enter a value.';
    return;
  }

  submitting.value = true;
  errorMsg.value = '';

  try {
    const resp = await axios.post(
      `/api/projects/${props.projectId}/secrets/deposit/${props.slotId}`,
      { value },
    );

    if (inputEl.value) inputEl.value.value = '';
    cardState.value = 'saved';
    emit('deposited', props.slotId);
  } catch (err) {
    if (inputEl.value) inputEl.value.value = '';
    if (err.response?.status === 410) {
      cardState.value = 'expired';
    } else {
      const msg = err.response?.data || err.message || `HTTP ${err.response?.status}`;
      errorMsg.value = `Failed to save: ${msg}`;
    }
  } finally {
    submitting.value = false;
  }
}
</script>
