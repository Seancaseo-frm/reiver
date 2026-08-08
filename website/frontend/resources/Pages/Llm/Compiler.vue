<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-[1000px] mx-auto px-8 py-6">
      <!-- Header -->
      <div class="mb-6">
        <h1 class="text-2xl font-semibold text-gray-900">Prompt Compiler</h1>
        <p class="text-sm text-gray-500 mt-0.5">
          Generate an optimized prompt candidate, evaluate it against saved sessions, and commit the result as a new version.
        </p>
      </div>

      <!-- Prompt Selector + Compile -->
      <div class="bg-white border border-gray-200 rounded-xl p-5 mb-6">
        <div class="flex items-end gap-4">
          <div class="flex-1">
            <label class="block text-sm font-medium text-gray-700 mb-1.5">Prompt Configuration</label>
            <select
              v-model="selectedConfigId"
              class="w-full px-3 py-2.5 border border-gray-300 rounded-lg bg-white text-gray-900 text-sm focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
              :disabled="compiling"
            >
              <option value="">Select a prompt config...</option>
              <option v-for="c in configs" :key="c.id" :value="c.id">
                {{ c.name }}
                <template v-if="c.active_version"> (v{{ c.active_version }})</template>
              </option>
            </select>
          </div>
          <div class="flex-shrink-0">
            <label class="block text-sm font-medium text-gray-700 mb-1.5">Hint (optional)</label>
            <input
              v-model="hint"
              type="text"
              placeholder="e.g. Reduce cost"
              class="w-48 px-3 py-2.5 border border-gray-300 rounded-lg bg-white text-gray-900 text-sm focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
              :disabled="compiling"
            />
          </div>
          <button
            @click="runCompilation"
            :disabled="!selectedConfigId || compiling"
            class="px-5 py-2.5 bg-indigo-600 text-white text-sm font-medium rounded-lg hover:bg-indigo-700 disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2 transition-colors"
          >
            <svg v-if="compiling" class="animate-spin h-4 w-4" fill="none" viewBox="0 0 24 24">
              <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4" />
              <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
            </svg>
            <svg v-else class="h-4 w-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
              <path stroke-linecap="round" stroke-linejoin="round" d="M13 10V3L4 14h7v7l9-11h-7z" />
            </svg>
            {{ compiling ? 'Compiling...' : 'Compile' }}
          </button>
        </div>
        <div v-if="compiling" class="mt-3 space-y-2">
          <progress
            class="w-full h-2 rounded overflow-hidden accent-indigo-600"
            max="1"
            :value="compileProgressPct == null ? undefined : compileProgressPct"
          />
          <p class="text-sm text-gray-500">
            {{ compileProgressMessage || 'Generating candidates, replaying sessions, and evaluating…' }}
          </p>
        </div>
      </div>

      <!-- Compiler notice (e.g. cancelled) -->
      <div v-if="compilerNotice" class="bg-amber-50 border border-amber-200 rounded-xl p-4 mb-6">
        <p class="text-sm text-amber-800">{{ compilerNotice }}</p>
      </div>

      <!-- Error -->
      <div v-if="error" class="bg-red-50 border border-red-200 rounded-xl p-4 mb-6">
        <div class="flex gap-3">
          <svg class="h-5 w-5 text-red-500 flex-shrink-0 mt-0.5" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
            <path stroke-linecap="round" stroke-linejoin="round" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
          </svg>
          <p class="text-sm text-red-700">{{ error }}</p>
        </div>
      </div>

      <!-- Commit Success -->
      <div v-if="commitSuccess" class="bg-green-50 border border-green-200 rounded-xl p-4 mb-6">
        <div class="flex gap-3">
          <svg class="h-5 w-5 text-green-500 flex-shrink-0 mt-0.5" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
            <path stroke-linecap="round" stroke-linejoin="round" d="M5 13l4 4L19 7" />
          </svg>
          <div class="text-sm text-green-700 space-y-2">
            <p>{{ commitSuccess }}</p>
            <a
              v-if="commitRolloutLink"
              :href="commitRolloutLink"
              class="inline-flex text-indigo-600 font-medium hover:text-indigo-800"
            >Open rollout</a>
          </div>
        </div>
      </div>

      <!-- Report -->
      <div v-if="report" class="space-y-5">
        <!-- Reasoning -->
        <div class="bg-white border border-gray-200 rounded-xl p-5">
          <h3 class="text-sm font-medium text-gray-700 mb-2">Reasoning</h3>
          <p class="text-sm text-gray-600">{{ report.reasoning }}</p>
        </div>

        <!-- Prompt Diff -->
        <div class="bg-white border border-gray-200 rounded-xl p-5">
          <div class="flex items-center justify-between mb-2">
            <h3 class="text-sm font-medium text-gray-700">Prompt Changes</h3>
            <button
              @click="promptExpanded = !promptExpanded"
              class="text-xs text-indigo-600 hover:text-indigo-800"
            >
              {{ promptExpanded ? 'Collapse' : 'Expand' }}
            </button>
          </div>
          <div
            class="diff-view rounded-lg bg-gray-50 p-4 text-sm font-mono leading-relaxed overflow-auto"
            :class="{ 'max-h-64': !promptExpanded }"
          >
            <div
              v-for="(line, lIdx) in promptDiff"
              :key="lIdx"
              class="whitespace-pre-wrap break-words"
              :class="{
                'bg-red-100 text-red-800': line.type === 'removed',
                'bg-green-100 text-green-800': line.type === 'added',
                'text-gray-600': line.type === 'same',
              }"
            ><span class="select-none inline-block w-5 text-right mr-2 text-xs opacity-50">{{ line.type === 'removed' ? '−' : line.type === 'added' ? '+' : ' ' }}</span>{{ line.text }}</div>
          </div>
        </div>

        <!-- Per-Session Breakdown -->
        <div class="bg-white border border-gray-200 rounded-xl p-5">
          <button
            @click="sessionsExpanded = !sessionsExpanded"
            class="flex items-center justify-between w-full text-left"
          >
            <h3 class="text-sm font-medium text-gray-700">Per-Session Breakdown</h3>
            <svg
              class="h-4 w-4 text-gray-400 transition-transform"
              :class="{ 'rotate-180': sessionsExpanded }"
              fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2"
            >
              <path stroke-linecap="round" stroke-linejoin="round" d="M19 9l-7 7-7-7" />
            </svg>
          </button>
          <div v-if="sessionsExpanded" class="mt-3">
            <table class="w-full text-sm">
              <thead>
                <tr class="border-b border-gray-200">
                  <th class="text-left py-2 text-xs font-medium text-gray-500" rowspan="2">Session</th>
                  <th class="text-center py-1 text-xs font-medium text-gray-500 border-b border-gray-100" colspan="2">Judge Score</th>
                  <th class="text-center py-1 text-xs font-medium text-gray-500 border-b border-gray-100" colspan="2">Cost</th>
                  <th class="text-center py-1 text-xs font-medium text-gray-500 border-b border-gray-100" colspan="2">Latency</th>
                  <th class="text-center py-1 text-xs font-medium text-gray-500 border-b border-gray-100" colspan="2">Errors</th>
                </tr>
                <tr class="border-b border-gray-200">
                  <th class="text-right py-1 text-xs text-gray-400">Baseline</th>
                  <th class="text-right py-1 text-xs text-gray-400">Candidate</th>
                  <th class="text-right py-1 text-xs text-gray-400">Baseline</th>
                  <th class="text-right py-1 text-xs text-gray-400">Candidate</th>
                  <th class="text-right py-1 text-xs text-gray-400">Baseline</th>
                  <th class="text-right py-1 text-xs text-gray-400">Candidate</th>
                  <th class="text-right py-1 text-xs text-gray-400">Baseline</th>
                  <th class="text-right py-1 text-xs text-gray-400">Candidate</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="(session, idx) in report.candidate.per_session" :key="session.session_id" class="border-b border-gray-100">
                  <td class="py-2 text-gray-700 font-mono text-xs">{{ session.session_id.substring(0, 12) }}...</td>
                  <td class="py-2 text-right text-gray-600">
                    {{ report.baseline.per_session[idx]?.judge_score?.toFixed(2) ?? '—' }}
                  </td>
                  <td class="py-2 text-right font-medium"
                    :class="session.judge_score >= (report.baseline.per_session[idx]?.judge_score || 0) ? 'text-green-600' : 'text-red-600'">
                    {{ session.judge_score.toFixed(2) }}
                  </td>
                  <td class="py-2 text-right text-gray-600">
                    ${{ report.baseline.per_session[idx]?.cost_usd?.toFixed(4) ?? '—' }}
                  </td>
                  <td class="py-2 text-right font-medium"
                    :class="session.cost_usd <= (report.baseline.per_session[idx]?.cost_usd || 0) ? 'text-green-600' : 'text-red-600'">
                    ${{ session.cost_usd.toFixed(4) }}
                  </td>
                  <td class="py-2 text-right text-gray-600">
                    {{ report.baseline.per_session[idx]?.latency_ms?.toFixed(0) ?? '—' }}ms
                  </td>
                  <td class="py-2 text-right font-medium"
                    :class="session.latency_ms <= (report.baseline.per_session[idx]?.latency_ms || 0) ? 'text-green-600' : 'text-red-600'">
                    {{ session.latency_ms.toFixed(0) }}ms
                  </td>
                  <td class="py-2 text-right text-gray-600">
                    {{ report.baseline.per_session[idx]?.error_count ?? '—' }}
                  </td>
                  <td class="py-2 text-right font-medium"
                    :class="session.error_count <= (report.baseline.per_session[idx]?.error_count || 0) ? 'text-green-600' : 'text-red-600'">
                    {{ session.error_count }}
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>

        <!-- Commit button -->
        <div class="flex justify-end">
          <button
            @click="commitVersion"
            :disabled="committing"
            class="px-6 py-2.5 bg-green-600 text-white text-sm font-medium rounded-lg hover:bg-green-700 disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2 transition-colors"
          >
            <svg v-if="committing" class="animate-spin h-4 w-4" fill="none" viewBox="0 0 24 24">
              <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4" />
              <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
            </svg>
            <svg v-else class="h-4 w-4" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
              <path stroke-linecap="round" stroke-linejoin="round" d="M5 13l4 4L19 7" />
            </svg>
            {{ commitButtonLabel }}
          </button>
        </div>
      </div>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import axios from 'axios';
import AppLayout from '@/Layouts/AppLayout.vue';
import { useAuth } from '@/composables/useAuth';

const route = useRoute();
const router = useRouter();
const { user, fetchUser } = useAuth();

const projectId = computed(() => route.params.id);
const project = computed(() => ({ id: projectId.value }));

const configs = ref([]);
const selectedConfigId = ref('');
const hint = ref('');
const compiling = ref(false);
const committing = ref(false);
const startingRollout = ref(false);
const compileTaskId = ref(null);
const compileProgressPct = ref(null);
const compileProgressMessage = ref('');
const compilerNotice = ref('');
const error = ref('');
const commitSuccess = ref('');
const commitRolloutLink = ref('');
const report = ref(null);
const promptExpanded = ref(false);
const sessionsExpanded = ref(false);

function computeDiff(original, modified) {
  const oldLines = original.split('\n');
  const newLines = modified.split('\n');
  const result = [];
  let oi = 0, ni = 0;
  while (oi < oldLines.length || ni < newLines.length) {
    if (oi < oldLines.length && ni < newLines.length && oldLines[oi] === newLines[ni]) {
      result.push({ type: 'same', text: oldLines[oi] });
      oi++; ni++;
    } else {
      let syncFound = false;
      const window = 6;
      for (let ahead = 1; ahead <= window && !syncFound; ahead++) {
        if (ni + ahead < newLines.length && oi < oldLines.length && oldLines[oi] === newLines[ni + ahead]) {
          for (let j = 0; j < ahead; j++) result.push({ type: 'added', text: newLines[ni + j] });
          ni += ahead;
          syncFound = true;
        }
        if (oi + ahead < oldLines.length && ni < newLines.length && oldLines[oi + ahead] === newLines[ni]) {
          for (let j = 0; j < ahead; j++) result.push({ type: 'removed', text: oldLines[oi + j] });
          oi += ahead;
          syncFound = true;
        }
      }
      if (!syncFound) {
        if (oi < oldLines.length) { result.push({ type: 'removed', text: oldLines[oi] }); oi++; }
        if (ni < newLines.length) { result.push({ type: 'added', text: newLines[ni] }); ni++; }
      }
    }
  }
  return result;
}

const promptDiff = computed(() => {
  if (!report.value) return [];
  return computeDiff(report.value.original_prompt || '', report.value.compiled_prompt || '');
});

const commitButtonLabel = computed(() => {
  if (startingRollout.value) return 'Starting rollout…';
  if (committing.value) return 'Committing…';
  return 'Commit and rollout';
});

async function fetchConfigs() {
  try {
    const res = await axios.get(`/api/llm/prompts/configs`, {
      params: { project_id: projectId.value },
    });
    configs.value = res.data || [];
  } catch (e) {
    console.error('Failed to load prompt configs:', e);
  }
}

let pollTimer = null;

function clearPollTimer() {
  if (pollTimer) {
    clearInterval(pollTimer);
    pollTimer = null;
  }
}

/** Best-effort cancel when tab closes (cookies sent for same-origin). */
function beaconCancelCompile(projectIdVal, taskId) {
  if (!taskId || !projectIdVal) return;
  const url = `${window.location.origin}/api/projects/${projectIdVal}/llm/compiler/cancel/${taskId}`;
  const body = JSON.stringify({ project_id: projectIdVal });
  const blob = new Blob([body], { type: 'application/json' });
  if (navigator.sendBeacon(url, blob)) return;
  fetch(url, {
    method: 'POST',
    body,
    headers: { 'Content-Type': 'application/json' },
    keepalive: true,
    credentials: 'same-origin',
  }).catch(() => {});
}

function fireAndForgetCancelCompile(projectIdVal, taskId) {
  if (!taskId || !projectIdVal) return;
  axios
    .post(`/api/projects/${projectIdVal}/llm/compiler/cancel/${taskId}`, {
      project_id: projectIdVal,
    })
    .catch(() => {});
}

function onBeforeUnload() {
  if (compiling.value && compileTaskId.value) {
    beaconCancelCompile(projectId.value, compileTaskId.value);
  }
}

async function runCompilation() {
  error.value = '';
  compilerNotice.value = '';
  commitSuccess.value = '';
  commitRolloutLink.value = '';
  report.value = null;
  compiling.value = true;
  compileProgressPct.value = null;
  compileProgressMessage.value = '';
  compileTaskId.value = null;

  try {
    const res = await axios.post(
      `/api/projects/${projectId.value}/llm/compiler/compile-report`,
      {
        project_id: projectId.value,
        config_id: selectedConfigId.value,
        hint: hint.value || undefined,
      }
    );
    const taskId = res.data.task_id;
    if (!taskId) {
      error.value = 'Compilation failed: no task_id returned';
      compiling.value = false;
      return;
    }
    compileTaskId.value = taskId;
    pollCompilationStatus(taskId);
  } catch (e) {
    const msg = e.response?.data?.error || e.response?.data?.message || e.message;
    error.value = `Compilation failed: ${msg}`;
    compiling.value = false;
  }
}

function pollCompilationStatus(taskId) {
  clearPollTimer();
  pollTimer = setInterval(async () => {
    try {
      const res = await axios.get(
        `/api/projects/${projectId.value}/llm/compiler/status/${taskId}`
      );
      const data = res.data;
      if (data.status === 'running') {
        compileProgressPct.value =
          data.progress_pct != null && data.progress_pct !== undefined
            ? Number(data.progress_pct)
            : null;
        compileProgressMessage.value = data.progress_message || '';
        return;
      }
      if (data.status === 'completed') {
        clearPollTimer();
        report.value = data.report;
        promptExpanded.value = false;
        sessionsExpanded.value = false;
        compiling.value = false;
        compileTaskId.value = null;
        compileProgressPct.value = null;
        compileProgressMessage.value = '';
      } else if (data.status === 'failed') {
        clearPollTimer();
        error.value = `Compilation failed: ${data.error || 'Unknown error'}`;
        compiling.value = false;
        compileTaskId.value = null;
        compileProgressPct.value = null;
        compileProgressMessage.value = '';
      } else if (data.status === 'cancelled') {
        clearPollTimer();
        compilerNotice.value = 'Compilation was cancelled.';
        compiling.value = false;
        compileTaskId.value = null;
        compileProgressPct.value = null;
        compileProgressMessage.value = '';
      }
    } catch (e) {
      clearPollTimer();
      error.value = `Failed to check compilation status: ${e.message}`;
      compiling.value = false;
      compileTaskId.value = null;
      compileProgressPct.value = null;
      compileProgressMessage.value = '';
    }
  }, 4000);
}

async function commitVersion() {
  if (!report.value) return;
  error.value = '';
  compilerNotice.value = '';
  commitSuccess.value = '';
  commitRolloutLink.value = '';
  committing.value = true;
  startingRollout.value = false;

  try {
    const res = await axios.post(
      `/api/projects/${projectId.value}/llm/compiler/commit`,
      {
        project_id: projectId.value,
        config_id: report.value.config_id,
        system_prompt: report.value.compiled_prompt,
        reasoning: report.value.reasoning,
      }
    );
    const version = res.data.version;
    const rolloutId = res.data.rollout_id;
    const rolloutPath = `/p/${projectId.value}/llm/rollouts/${rolloutId}`;
    startingRollout.value = true;
    try {
      await axios.post(`/api/llm/prompts/rollouts/${rolloutId}/start`, {
        project_id: projectId.value,
      });
      report.value = null;
      await router.push(rolloutPath);
    } catch (startErr) {
      const startMsg =
        startErr.response?.data?.message ||
        startErr.response?.data?.error ||
        startErr.message ||
        'Unknown error';
      commitSuccess.value = `Created version ${version} and a pending rollout. The rollout could not be started automatically (${startMsg}). You can start it from the rollout page.`;
      commitRolloutLink.value = rolloutPath;
      report.value = null;
    }
  } catch (e) {
    const msg = e.response?.data?.error || e.response?.data?.message || e.message;
    error.value = `Commit failed: ${msg}`;
  } finally {
    committing.value = false;
    startingRollout.value = false;
  }
}

onMounted(async () => {
  window.addEventListener('beforeunload', onBeforeUnload);
  await fetchUser();
  await fetchConfigs();
  if (route.query.config) {
    selectedConfigId.value = route.query.config;
  }
});

onUnmounted(() => {
  window.removeEventListener('beforeunload', onBeforeUnload);
  clearPollTimer();
  const tid = compileTaskId.value;
  const pid = projectId.value;
  compileTaskId.value = null;
  if (tid && pid) {
    fireAndForgetCancelCompile(pid, tid);
  }
});

watch(projectId, (_newPid, oldPid) => {
  clearPollTimer();
  const tid = compileTaskId.value;
  if (tid && oldPid) {
    fireAndForgetCancelCompile(oldPid, tid);
  }
  compileTaskId.value = null;
  compiling.value = false;
  compileProgressPct.value = null;
  compileProgressMessage.value = '';
  compilerNotice.value = '';
  fetchUser();
  fetchConfigs();
  report.value = null;
  error.value = '';
  commitSuccess.value = '';
  commitRolloutLink.value = '';
});
</script>

<style scoped>
.diff-view {
  max-height: 600px;
  overflow-y: auto;
}
</style>
