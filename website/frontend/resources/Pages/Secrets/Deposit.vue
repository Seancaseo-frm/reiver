<template>
  <div class="min-h-screen flex items-center justify-center bg-gray-50 dark:bg-gray-950 px-4">
    <div class="w-full max-w-md">
      <div class="text-center mb-6">
        <h1 class="text-lg font-semibold text-gray-900 dark:text-gray-100">Secure Secret Deposit</h1>
        <p class="text-sm text-gray-500 dark:text-gray-400 mt-1">
          This value will be encrypted and stored securely. It will not be visible to the AI agent.
        </p>
      </div>
      <SecretDepositCard
        :slot-id="slotId"
        :project-id="projectId"
        :purpose="''"
        :provider="null"
        :deposit-url="''"
        :expires-at="''"
        @deposited="onDeposited"
      />
      <p v-if="deposited" class="text-center text-sm text-green-600 dark:text-green-400 mt-4">
        You can close this tab and return to the chat.
      </p>
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue';
import { useRoute } from 'vue-router';
import SecretDepositCard from '@/components/SecretDepositCard.vue';

const route = useRoute();
const projectId = route.params.projectId;
const slotId = route.params.slotId;
const deposited = ref(false);

function onDeposited() {
  deposited.value = true;
}
</script>
