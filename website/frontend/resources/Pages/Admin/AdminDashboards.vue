<template>
  <div class="space-y-4">
    <div class="flex items-center justify-between">
      <h2 class="text-lg font-semibold text-gray-900">Dashboard Management</h2>
      <button
        @click="confirmReconvert"
        :disabled="reconverting"
        class="px-4 py-2 rounded-lg bg-blue-600 text-white text-sm font-medium hover:bg-blue-700 disabled:opacity-50"
      >
        {{ reconverting ? 'Reconverting...' : 'Reconvert All Imported Dashboards' }}
      </button>
    </div>

    <div v-if="error" class="rounded-lg bg-red-50 border border-red-200 px-4 py-3 text-sm text-red-700 flex items-center justify-between">
      <span>{{ error }}</span>
      <button @click="error = ''" class="ml-4 text-red-500 hover:text-red-700">&times;</button>
    </div>

    <div v-if="result" class="rounded-lg border px-4 py-3 text-sm" :class="result.failed > 0 ? 'bg-yellow-50 border-yellow-200 text-yellow-800' : 'bg-green-50 border-green-200 text-green-800'">
      <p class="font-medium">
        Reconverted {{ result.reconverted }} dashboard{{ result.reconverted !== 1 ? 's' : '' }} successfully.
        <span v-if="result.failed > 0" class="text-red-600">{{ result.failed }} failed.</span>
      </p>
      <ul v-if="result.errors.length > 0" class="mt-2 list-disc list-inside text-xs text-red-600 space-y-1">
        <li v-for="(err, i) in result.errors" :key="i">{{ err }}</li>
      </ul>
    </div>

    <div v-if="showConfirm" class="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div class="bg-white rounded-lg shadow-xl max-w-md w-full p-6">
        <h3 class="text-lg font-semibold text-gray-900 mb-2">Confirm Reconversion</h3>
        <p class="text-sm text-gray-600 mb-4">
          This will reconvert all imported dashboards using the latest converter logic.
          Existing tabs and widgets will be replaced. Continue?
        </p>
        <div class="flex justify-end gap-3">
          <button @click="showConfirm = false" class="px-4 py-2 text-sm font-medium text-gray-700 bg-gray-100 rounded-lg hover:bg-gray-200">
            Cancel
          </button>
          <button @click="doReconvert" class="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700">
            Reconvert
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue';
import axios from 'axios';

const reconverting = ref(false);
const error = ref('');
const result = ref(null);
const showConfirm = ref(false);

function confirmReconvert() {
  result.value = null;
  error.value = '';
  showConfirm.value = true;
}

async function doReconvert() {
  showConfirm.value = false;
  reconverting.value = true;
  error.value = '';
  result.value = null;
  try {
    const { data } = await axios.post('/api/admin/dashboards/reconvert');
    result.value = data;
  } catch (e) {
    error.value = 'Failed to reconvert dashboards. Please try again.';
    console.error('Failed to reconvert dashboards', e);
  } finally {
    reconverting.value = false;
  }
}
</script>
