<template>
  <div class="space-y-6 max-w-xl">
    <p class="text-sm text-gray-600">
      Sends OTLP JSON to Watch (<code class="text-xs bg-gray-100 px-1 rounded">/api/v1/traces</code>,
      <code class="text-xs bg-gray-100 px-1 rounded">logs</code>,
      <code class="text-xs bg-gray-100 px-1 rounded">metrics</code>) at random per request, at the chosen rate, until you stop the test.
    </p>
    <div>
      <label class="block text-sm font-medium text-gray-700 mb-1">Project</label>
      <select v-model="stressProjectId" class="w-full border-gray-300 rounded-md shadow-sm text-sm">
        <option value="">Select project…</option>
        <option v-for="p in projects" :key="p.id" :value="p.id">{{ p.name || p.slug || p.id }}</option>
      </select>
    </div>
    <div>
      <label class="block text-sm font-medium text-gray-700 mb-1">Requests per second (1–200)</label>
      <input
        v-model.number="stressRps"
        type="range"
        min="1"
        max="200"
        class="w-full accent-blue-600"
      />
      <p class="text-sm text-gray-500 mt-1">{{ stressRps }} req/s</p>
    </div>
    <div class="flex items-center gap-3">
      <button
        type="button"
        @click="stressRunning ? stopStress() : runStress()"
        :disabled="!stressRunning && !canRunStress"
        :class="[
          'px-4 py-2 rounded-lg text-white text-sm font-medium disabled:opacity-50',
          stressRunning
            ? 'bg-red-600 hover:bg-red-700'
            : 'bg-blue-600 hover:bg-blue-700',
        ]"
      >
        {{ stressRunning ? 'Stop load test' : 'Start load test' }}
      </button>
    </div>
    <p v-if="stressError" class="text-sm text-red-600">{{ stressError }}</p>
    <p v-else-if="stressStoppedEarly" class="text-sm text-gray-600">Load test stopped.</p>
    <div v-if="stressResult" class="text-sm text-gray-800 bg-gray-50 p-4 rounded-lg border border-gray-200">
      <p>Sent: <strong>{{ stressResult.sent }}</strong></p>
      <p>Errors: <strong>{{ stressResult.errors }}</strong></p>
      <p>Duration: <strong>{{ stressResult.duration_ms }} ms</strong></p>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue';
import axios from 'axios';

const projects = ref([]);
const stressProjectId = ref('');
const stressRps = ref(20);
const stressRunning = ref(false);
let stressAbortController = null;
const stressError = ref('');
const stressResult = ref(null);
const stressStoppedEarly = ref(false);

const canRunStress = computed(() => {
  return stressProjectId.value && stressRps.value >= 1;
});

function stopStress() {
  stressAbortController?.abort();
}

function isStressRequestCanceled(e) {
  return axios.isCancel(e) || e?.code === 'ERR_CANCELED' || e?.name === 'CanceledError';
}

async function runStress() {
  stressError.value = '';
  stressResult.value = null;
  stressStoppedEarly.value = false;
  stressAbortController = new AbortController();
  stressRunning.value = true;
  try {
    const { data } = await axios.post(
      '/api/admin/ingestion-stress',
      {
        project_id: stressProjectId.value,
        rps: stressRps.value,
      },
      { signal: stressAbortController.signal },
    );
    stressResult.value = data;
  } catch (e) {
    if (isStressRequestCanceled(e)) {
      stressError.value = '';
      stressStoppedEarly.value = true;
    } else {
      stressError.value = e.response?.data?.error || e.message || 'Failed';
    }
  } finally {
    stressRunning.value = false;
    stressAbortController = null;
  }
}

onMounted(async () => {
  try {
    const { data } = await axios.get('/api/projects');
    if (data?.length > 0) {
      projects.value = data;
      stressProjectId.value = data[0].id;
    }
  } catch (_) {}
});
</script>
