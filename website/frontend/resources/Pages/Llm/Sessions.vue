<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-[1200px] mx-auto px-8 py-6">
      <!-- Header -->
      <div class="mb-6">
        <h1 class="text-2xl font-semibold text-gray-900 dark:text-gray-100">Sessions</h1>
        <p class="text-sm text-gray-500 dark:text-gray-400 mt-1">View and analyze AI conversation sessions</p>
      </div>

      <!-- Tab bar -->
      <div class="border-b border-gray-200 dark:border-gray-700 mb-6">
        <nav class="-mb-px flex gap-6" aria-label="Sessions tabs">
          <button
            v-for="tab in tabs"
            :key="tab.id"
            @click="setTab(tab.id)"
            class="pb-3 text-sm font-medium transition-colors border-b-2"
            :class="activeTab === tab.id
              ? 'border-primary-600 text-primary-600 dark:text-primary-400'
              : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'"
          >{{ tab.label }}</button>
        </nav>
      </div>

      <!-- Sessions tab -->
      <div v-if="activeTab === 'sessions'">
      <!-- Delay note -->
      <div class="flex items-start gap-2 px-3 py-2 mb-4 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-700/50 rounded-lg text-blue-800 dark:text-blue-300 text-xs">
        <svg class="w-4 h-4 mt-0.5 flex-shrink-0" fill="currentColor" viewBox="0 0 20 20"><path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd"/></svg>
        <span>Sessions appear here approximately 30 minutes after the session ends, due to ingestion and processing pipelines.</span>
      </div>

      <!-- Filters -->
      <div class="mb-6 flex flex-wrap gap-4 items-end">
        <div class="flex-1 min-w-[200px]">
          <label class="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">Search</label>
          <input
            v-model="searchQuery"
            type="text"
            placeholder="Filter by session name or ID..."
            class="w-full px-3 py-2 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-primary-500"
            @input="debouncedFetch"
          />
        </div>
        <div v-if="availableProfiles.length" class="min-w-[160px]">
          <label class="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">Profile</label>
          <select
            v-model="profileFilter"
            class="w-full px-3 py-2 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-primary-500"
            @change="currentOffset = 0; fetchSessions()"
          >
            <option value="">All sessions</option>
            <option v-for="p in availableProfiles" :key="p.id" :value="p.id">{{ p.name }}</option>
          </select>
        </div>
      </div>

      <!-- Sessions Table -->
      <BaseCard>
        <div v-if="loading" class="text-center py-12">
          <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full mx-auto mb-3"></div>
          <p class="text-gray-500 dark:text-gray-400">Loading sessions...</p>
        </div>
        <div v-else-if="sessions.length === 0" class="text-center py-12 text-gray-500 dark:text-gray-400">
          <svg class="w-12 h-12 mx-auto mb-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z" />
          </svg>
          <p class="text-lg font-medium mb-2">No sessions found</p>
          <p class="text-sm">Sessions appear when requests include an <code class="bg-gray-100 dark:bg-gray-700 px-1 rounded">x-reiver-session-id</code> header.</p>
        </div>
        <div v-else class="overflow-x-auto">
          <table class="w-full">
            <thead>
              <tr class="border-b border-gray-200 dark:border-gray-700">
                <th class="text-left py-3 px-4 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Session</th>
                <th class="text-right py-3 px-4 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Requests</th>
                <th class="text-right py-3 px-4 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Tokens</th>
                <th class="text-right py-3 px-4 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Cost</th>
                <th class="text-right py-3 px-4 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Errors</th>
                <th class="text-left py-3 px-4 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Profiles</th>
                <th class="text-left py-3 px-4 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Labels</th>
                <th class="text-left py-3 px-4 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Feedback</th>
                <th class="text-left py-3 px-4 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Last Active</th>
              </tr>
            </thead>
            <tbody>
              <tr
                v-for="session in sessions"
                :key="session.session_id"
                class="border-b border-gray-100 dark:border-gray-800 hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors cursor-pointer"
                @click="viewSession(session)"
              >
                <td class="py-3 px-4">
                  <div class="flex flex-col">
                    <span v-if="session.session_name" class="text-sm font-medium text-gray-900 dark:text-gray-100">{{ session.session_name }}</span>
                    <span class="text-xs font-mono text-gray-500 dark:text-gray-400">{{ session.session_id.slice(0, 12) }}…</span>
                  </div>
                </td>
                <td class="py-3 px-4 text-right">
                  <span class="text-sm tabular-nums text-gray-900 dark:text-gray-100">{{ session.request_count }}</span>
                </td>
                <td class="py-3 px-4 text-right">
                  <span class="text-sm tabular-nums text-gray-600 dark:text-gray-400">{{ formatNumber(session.total_tokens) }}</span>
                </td>
                <td class="py-3 px-4 text-right">
                  <span class="text-sm tabular-nums text-gray-600 dark:text-gray-400">${{ formatCost(session.total_cost_usd) }}</span>
                </td>
                <td class="py-3 px-4 text-right">
                  <span
                    v-if="session.error_count > 0"
                    class="inline-flex items-center px-2 py-0.5 text-xs font-medium rounded-full bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-300"
                  >{{ session.error_count }}</span>
                  <span v-else class="text-sm text-gray-400">0</span>
                </td>
                <td class="py-3 px-4">
                  <div class="flex flex-wrap gap-1">
                    <span
                      v-for="mp in (session.matched_profiles || [])"
                      :key="mp.profile_id"
                      class="inline-flex items-center px-2 py-0.5 text-xs font-medium rounded-full bg-primary-100 text-primary-700 dark:bg-primary-900/40 dark:text-primary-300"
                    >{{ mp.profile_name }}</span>
                    <span v-if="!session.matched_profiles?.length" class="text-gray-400 text-sm">—</span>
                  </div>
                </td>
                <td class="py-3 px-4">
                  <div class="flex flex-wrap gap-1">
                    <span
                      v-for="label in (session.labels || [])"
                      :key="label"
                      class="inline-flex items-center px-2 py-0.5 text-xs font-medium rounded-full bg-brand-100 text-brand-700 dark:bg-brand-900/40 dark:text-brand-300"
                    >{{ label }}</span>
                    <span v-if="!session.labels?.length" class="text-gray-400 text-sm">—</span>
                  </div>
                </td>
                <td class="py-3 px-4">
                  <span v-if="session.feedback_score === 1" class="text-green-600 dark:text-green-400" title="Positive">👍</span>
                  <span v-else-if="session.feedback_score === -1" class="text-red-600 dark:text-red-400" title="Negative">👎</span>
                  <span v-else class="text-gray-400">—</span>
                </td>
                <td class="py-3 px-4">
                  <span class="text-sm text-gray-600 dark:text-gray-400">{{ formatTime(session.last_request_time) }}</span>
                </td>
              </tr>
            </tbody>
          </table>

          <!-- Pagination -->
          <div class="flex items-center justify-between px-4 py-3 border-t border-gray-200 dark:border-gray-700">
            <p class="text-sm text-gray-500 dark:text-gray-400">
              Showing {{ sessions.length }} of {{ totalCount }} sessions
            </p>
            <div class="flex gap-2">
              <button
                :disabled="currentOffset === 0"
                class="px-3 py-1.5 text-sm border border-gray-300 dark:border-gray-600 rounded-lg disabled:opacity-40 disabled:cursor-not-allowed hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors"
                @click="prevPage"
              >Previous</button>
              <button
                :disabled="sessions.length < pageSize"
                class="px-3 py-1.5 text-sm border border-gray-300 dark:border-gray-600 rounded-lg disabled:opacity-40 disabled:cursor-not-allowed hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors"
                @click="nextPage"
              >Next</button>
            </div>
          </div>
        </div>
      </BaseCard>
      </div><!-- /sessions tab -->

      <!-- Session Profiles tab -->
      <div v-if="activeTab === 'profiles'" class="space-y-6">
        <BaseCard>
          <template #header>
            <div class="flex items-center justify-between">
              <div>
                <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">Session Profiles</h2>
                <p class="text-sm text-gray-500 dark:text-gray-400 mt-0.5">Define criteria for which sessions get their content preserved for replay. Matched sessions appear on the Sessions tab approximately 30 minutes after the session ends.</p>
              </div>
              <button
                type="button"
                class="px-3 py-1.5 text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 rounded-lg transition-colors"
                @click="addSessionProfile"
              >+ Add Profile</button>
            </div>
          </template>

          <div v-if="!sessionProfiles.length" class="text-center py-8 text-gray-500 dark:text-gray-400">
            <p class="text-sm mb-1">No session profiles configured</p>
            <p class="text-xs">When profiles exist, session content (request/response bodies) is logged and matched sessions are preserved for replay.</p>
          </div>

          <div v-else class="space-y-4">
            <div class="flex items-start gap-2 px-3 py-2 bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-700/50 rounded-lg text-amber-800 dark:text-amber-300 text-xs">
              <svg class="w-4 h-4 mt-0.5 flex-shrink-0" fill="currentColor" viewBox="0 0 20 20"><path fill-rule="evenodd" d="M8.485 2.495c.673-1.167 2.357-1.167 3.03 0l6.28 10.875c.673 1.167-.168 2.625-1.516 2.625H3.72c-1.347 0-2.189-1.458-1.515-2.625L8.485 2.495zM10 6a.75.75 0 01.75.75v3.5a.75.75 0 01-1.5 0v-3.5A.75.75 0 0110 6zm0 9a1 1 0 100-2 1 1 0 000 2z" clip-rule="evenodd"/></svg>
              <span>If two profiles have overlapping conditions (e.g. one is a subset of the other), a session may match both. Make sure your profiles target distinct scenarios to avoid misleading results.</span>
            </div>
            <div
              v-for="(profile, pIdx) in sessionProfiles"
              :key="profile.id"
              class="border border-gray-200 dark:border-gray-600 rounded-lg"
            >
              <div class="flex items-center justify-between px-4 py-3 bg-gray-50 dark:bg-gray-800 rounded-t-lg">
                <div class="flex items-center gap-3 flex-1">
                  <input
                    v-model="profile.name"
                    type="text"
                    placeholder="Profile name"
                    class="text-sm font-medium bg-transparent border-0 border-b border-transparent focus:border-primary-500 focus:ring-0 text-gray-900 dark:text-gray-100 px-0 py-0.5 flex-1"
                  />
                  <select
                    v-model="profile.logic"
                    class="text-xs px-2 py-1 border border-gray-300 dark:border-gray-600 rounded bg-white dark:bg-gray-700 text-gray-700 dark:text-gray-300"
                  >
                    <option value="AND">Match ALL filters</option>
                    <option value="OR">Match ANY filter</option>
                  </select>
                </div>
                <button
                  type="button"
                  @click="removeSessionProfile(pIdx)"
                  class="p-1.5 rounded text-red-600 hover:bg-red-50 dark:hover:bg-red-900/20 ml-2"
                  title="Delete profile"
                >
                  <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" /></svg>
                </button>
              </div>

              <div class="px-4 py-3 space-y-2">
                <div
                  v-for="(filter, fIdx) in profile.filters"
                  :key="fIdx"
                  class="flex items-center gap-2"
                >
                  <select
                    v-model="filter.field"
                    class="text-sm px-2 py-1.5 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 min-w-[160px]"
                    @change="onFilterFieldChange(filter)"
                  >
                    <optgroup v-for="(fields, ns) in filterFieldsByNs" :key="ns" :label="ns">
                      <option v-for="fd in fields" :key="fd.field" :value="fd.field">{{ fd.label }}{{ fd.unit ? ` (${fd.unit})` : '' }}</option>
                    </optgroup>
                  </select>

                  <template v-if="isNumericFilter(filter.field)">
                    <select
                      v-model="filter.op"
                      class="text-sm px-2 py-1.5 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 w-16"
                    >
                      <option value="gt">&gt;</option>
                      <option value="gte">&ge;</option>
                      <option value="lt">&lt;</option>
                      <option value="lte">&le;</option>
                    </select>
                    <input
                      :value="filter.value"
                      @input="filter.value = parseFloat($event.target.value) || 0"
                      type="number"
                      step="any"
                      class="text-sm px-2 py-1.5 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 w-28"
                      :placeholder="getFilterPlaceholder(filter.field)"
                    />
                    <span class="text-xs text-gray-500 dark:text-gray-400">{{ getFilterUnit(filter.field) }}</span>
                  </template>

                  <template v-else-if="isSetFilter(filter.field)">
                    <span class="text-sm text-gray-500 dark:text-gray-400">contains</span>
                    <input
                      v-model="filter.value"
                      type="text"
                      class="text-sm px-2 py-1.5 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 flex-1"
                      :placeholder="getFilterPlaceholder(filter.field)"
                    />
                  </template>

                  <button
                    type="button"
                    @click="profile.filters.splice(fIdx, 1)"
                    class="p-1 rounded text-gray-400 hover:text-red-600 hover:bg-red-50 dark:hover:bg-red-900/20"
                  >
                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" /></svg>
                  </button>
                </div>

                <button
                  type="button"
                  @click="addFilterToProfile(pIdx)"
                  class="text-sm text-primary-600 hover:text-primary-700 dark:text-primary-400 font-medium"
                >+ Add filter</button>
              </div>
            </div>
          </div>
        </BaseCard>

        <div class="flex items-center gap-3">
          <button
            type="button"
            class="px-4 py-2 text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 rounded-lg transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            :disabled="profileSaveStatus === 'saving' || !settingsLoaded"
            @click="saveProfiles"
          >Save Profiles</button>
          <span v-if="profileSaveStatus === 'saved'" class="text-xs text-green-600 dark:text-green-400">Saved</span>
          <span v-else-if="profileSaveStatus === 'error'" class="text-xs text-red-600 dark:text-red-400">Save failed</span>
        </div>
      </div><!-- /profiles tab -->

      <!-- Session Labels tab -->
      <div v-if="activeTab === 'labels'" class="space-y-6">
        <BaseCard>
          <template #header>
            <div class="flex items-center justify-between">
              <div>
                <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">Session Labels</h2>
                <p class="text-sm text-gray-500 dark:text-gray-400 mt-0.5">Define labels and their criteria. MooDeng will automatically classify sessions based on these definitions.</p>
              </div>
              <button
                type="button"
                class="px-3 py-1.5 text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 rounded-lg transition-colors"
                @click="addSessionLabel"
              >+ Add Label</button>
            </div>
          </template>

          <div v-if="!sessionLabels.length" class="text-center py-8 text-gray-500 dark:text-gray-400">
            <p class="text-sm mb-1">No session labels configured</p>
            <p class="text-xs">Add labels to automatically classify your sessions.</p>
          </div>

          <div v-else class="space-y-3">
            <div
              v-for="(label, idx) in sessionLabels"
              :key="idx"
              class="flex items-start gap-3 border border-gray-200 dark:border-gray-600 rounded-lg px-4 py-3"
            >
              <div class="flex-1 space-y-2">
                <input
                  v-model="label.name"
                  type="text"
                  placeholder="Label name"
                  class="w-full text-sm font-medium bg-transparent border-0 border-b border-transparent focus:border-primary-500 focus:ring-0 text-gray-900 dark:text-gray-100 px-0 py-0.5"
                />
                <input
                  v-model="label.definition"
                  type="text"
                  placeholder="Optional: describe what qualifies a message for this label"
                  class="w-full text-sm bg-transparent border-0 border-b border-transparent focus:border-gray-400 focus:ring-0 text-gray-600 dark:text-gray-400 px-0 py-0.5"
                />
              </div>
              <button
                type="button"
                @click="removeSessionLabel(idx)"
                class="p-1.5 rounded text-red-600 hover:bg-red-50 dark:hover:bg-red-900/20 mt-1"
                title="Delete label"
              >
                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" /></svg>
              </button>
            </div>
          </div>
        </BaseCard>

        <div class="flex items-center gap-3">
          <button
            type="button"
            class="px-4 py-2 text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 rounded-lg transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            :disabled="labelSaveStatus === 'saving' || !settingsLoaded"
            @click="saveLabels"
          >Save Labels</button>
          <span v-if="labelSaveStatus === 'saved'" class="text-xs text-green-600 dark:text-green-400">Saved</span>
          <span v-else-if="labelSaveStatus === 'error'" class="text-xs text-red-600 dark:text-red-400">Save failed</span>
        </div>
      </div><!-- /labels tab -->

    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import axios from 'axios';
import AppLayout from '@/Layouts/AppLayout.vue';
import BaseCard from '@/components/BaseCard.vue';
import { useAuth } from '@/composables/useAuth';

const route = useRoute();
const router = useRouter();
const { user, fetchUser } = useAuth();

const projectId = computed(() => route.params.id);
const project = computed(() => ({ id: projectId.value }));

// ─── Tabs ───
const tabs = [
  { id: 'sessions', label: 'Sessions' },
  { id: 'profiles', label: 'Session Profiles' },
  { id: 'labels', label: 'Session Labels' },
];
const activeTab = ref(route.query.tab || 'sessions');

function setTab(tab) {
  activeTab.value = tab;
  router.replace({ query: { ...route.query, tab } });
}

// ─── Sessions ───
const loading = ref(true);
const sessions = ref([]);
const totalCount = ref(0);
const searchQuery = ref('');
const profileFilter = ref('');
const availableProfiles = ref([]);
const pageSize = 25;
const currentOffset = ref(0);

let debounceTimer = null;
const debouncedFetch = () => {
  clearTimeout(debounceTimer);
  debounceTimer = setTimeout(() => {
    currentOffset.value = 0;
    fetchSessions();
  }, 300);
};

const formatNumber = (num) => {
  if (num == null) return '0';
  return Number(num).toLocaleString();
};

const formatCost = (cost) => {
  return parseFloat(cost || 0).toFixed(4);
};

const formatTime = (timestamp) => {
  if (!timestamp) return '—';
  const d = new Date(timestamp);
  const now = new Date();
  const diffMs = now - d;
  const diffMin = Math.floor(diffMs / 60000);
  if (diffMin < 1) return 'just now';
  if (diffMin < 60) return `${diffMin}m ago`;
  const diffHrs = Math.floor(diffMin / 60);
  if (diffHrs < 24) return `${diffHrs}h ago`;
  return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
};

const viewSession = (session) => {
  router.push(`/p/${projectId.value}/llm/sessions/${session.session_id}`);
};

const prevPage = () => {
  currentOffset.value = Math.max(0, currentOffset.value - pageSize);
  fetchSessions();
};

const nextPage = () => {
  currentOffset.value += pageSize;
  fetchSessions();
};

const fetchSessions = async () => {
  loading.value = true;
  try {
    const params = {
      limit: pageSize,
      offset: currentOffset.value,
    };
    if (searchQuery.value.trim()) params.name_pattern = searchQuery.value.trim();
    if (profileFilter.value) params.profile_id = profileFilter.value;

    const response = await axios.get(`/api/projects/${projectId.value}/llm/sessions`, { params });
    sessions.value = response.data?.sessions || [];
    totalCount.value = response.data?.total || 0;
  } catch (error) {
    console.error('Failed to fetch sessions:', error);
    sessions.value = [];
  } finally {
    loading.value = false;
  }
};

const fetchProfiles = async () => {
  settingsLoaded.value = false;
  try {
    const response = await axios.get(`/api/projects/${projectId.value}/llm/settings`);
    const data = response.data || {};
    availableProfiles.value = data.session_profiles || [];
    fullSettings = data;
    sessionProfiles.value = Array.isArray(data.session_profiles) ? JSON.parse(JSON.stringify(data.session_profiles)) : [];
    sessionLabels.value = Array.isArray(data.session_labels) ? JSON.parse(JSON.stringify(data.session_labels)) : [];
    settingsLoaded.value = true;
  } catch {
    availableProfiles.value = [];
    fullSettings = {};
    sessionProfiles.value = [];
    sessionLabels.value = [];
  }
};

// ─── Session Profiles ───
const sessionProfiles = ref([]);
const settingsLoaded = ref(false);
const profileSaveStatus = ref('idle');
let fullSettings = {};
let profileSaveTimer = null;

const filterFields = ref([]);
const filterFieldsByNs = computed(() => {
  const groups = {};
  for (const f of filterFields.value) {
    if (!groups[f.namespace]) groups[f.namespace] = [];
    groups[f.namespace].push(f);
  }
  return groups;
});

async function fetchFilterFields() {
  try {
    const { data } = await axios.get(`/api/projects/${projectId.value}/llm/settings/filter-fields`);
    filterFields.value = data;
  } catch {
    filterFields.value = [];
  }
}

function fieldDef(field) {
  return filterFields.value.find(f => f.field === field);
}
function isNumericFilter(field) {
  return fieldDef(field)?.kind === 'numeric';
}
function isSetFilter(field) {
  return fieldDef(field)?.kind === 'set';
}
function getFilterPlaceholder(field) {
  const map = {
    'latency.avg_ms': '5000', 'latency.max_ms': '10000',
    'cost.avg_per_call': '0.05', 'cost.total': '1.00',
    'model.names': 'gpt-4o', 'provider.names': 'openai',
    'prompt.ids': 'prompt-config-id', 'errors.count': '1',
    'tools.count': '1', 'tools.names': 'search',
    'fallback.count': '1', 'guardrail.count': '1',
  };
  return map[field] || '';
}
function getFilterUnit(field) {
  return fieldDef(field)?.unit || '';
}
function onFilterFieldChange(filter) {
  const def = fieldDef(filter.field);
  if (!def) return;
  if (def.kind === 'numeric') {
    filter.op = filter.op || 'gte';
    filter.value = filter.value ?? 0;
  } else {
    filter.op = undefined;
    filter.value = filter.value || '';
  }
}

function addSessionProfile() {
  const crypto = window.crypto || window.msCrypto;
  const id = ([1e7]+-1e3+-4e3+-8e3+-1e11).replace(/[018]/g, c =>
    (c ^ crypto.getRandomValues(new Uint8Array(1))[0] & 15 >> c / 4).toString(16)
  );
  sessionProfiles.value = [
    ...sessionProfiles.value,
    { id, name: '', logic: 'AND', filters: [{ field: 'errors.count', op: 'gte', value: 1 }] },
  ];
}

function removeSessionProfile(index) {
  sessionProfiles.value = sessionProfiles.value.filter((_, i) => i !== index);
}

function addFilterToProfile(profileIdx) {
  sessionProfiles.value[profileIdx].filters.push({ field: 'errors.count', op: 'gte', value: 1 });
}

async function saveProfiles() {
  if (!settingsLoaded.value) return;
  profileSaveStatus.value = 'saving';
  try {
    const payload = { session_profiles: sessionProfiles.value };
    await axios.put(`/api/projects/${projectId.value}/llm/settings`, payload);
    fullSettings = payload;
    availableProfiles.value = JSON.parse(JSON.stringify(sessionProfiles.value));
    profileSaveStatus.value = 'saved';
    clearTimeout(profileSaveTimer);
    profileSaveTimer = setTimeout(() => { profileSaveStatus.value = 'idle'; }, 2000);
  } catch {
    profileSaveStatus.value = 'error';
    clearTimeout(profileSaveTimer);
    profileSaveTimer = setTimeout(() => { profileSaveStatus.value = 'idle'; }, 4000);
  }
}

// ─── Session Labels ───
const sessionLabels = ref([]);
const labelSaveStatus = ref('idle');
let labelSaveTimer = null;

function addSessionLabel() {
  sessionLabels.value = [...sessionLabels.value, { name: '', definition: '' }];
}

function removeSessionLabel(index) {
  sessionLabels.value = sessionLabels.value.filter((_, i) => i !== index);
}

async function saveLabels() {
  if (!settingsLoaded.value) return;
  labelSaveStatus.value = 'saving';
  try {
    const payload = { session_labels: sessionLabels.value };
    await axios.put(`/api/projects/${projectId.value}/llm/settings`, payload);
    fullSettings = payload;
    labelSaveStatus.value = 'saved';
    clearTimeout(labelSaveTimer);
    labelSaveTimer = setTimeout(() => { labelSaveStatus.value = 'idle'; }, 2000);
  } catch {
    labelSaveStatus.value = 'error';
    clearTimeout(labelSaveTimer);
    labelSaveTimer = setTimeout(() => { labelSaveStatus.value = 'idle'; }, 4000);
  }
}

// ─── Init ───
onMounted(async () => {
  await fetchUser();
  await Promise.all([fetchSessions(), fetchProfiles(), fetchFilterFields()]);
});

watch(projectId, () => {
  currentOffset.value = 0;
  fetchSessions();
  fetchProfiles();
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
