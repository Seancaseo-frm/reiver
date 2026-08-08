<template>
  <AppLayout :project="project">
    <div class="max-w-[1200px] mx-auto px-4 py-6 space-y-6">
      <div class="flex items-center justify-between">
        <div>
          <h1 class="text-2xl font-bold text-gray-900 dark:text-white">Agent Analytics</h1>
          <p class="mt-1 text-sm text-gray-500 dark:text-gray-400">MCP tool usage across all agent tokens</p>
        </div>
        <select v-model="timeRange" class="text-sm px-3 py-1.5 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100">
          <option value="1h">Last hour</option>
          <option value="24h">Last 24 hours</option>
          <option value="7d">Last 7 days</option>
          <option value="30d">Last 30 days</option>
        </select>
      </div>

      <!-- Stat cards -->
      <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
        <BaseCard v-for="stat in statCards" :key="stat.label">
          <div class="p-4 text-center">
            <div class="text-2xl font-bold text-gray-900 dark:text-white">{{ stat.value }}</div>
            <div class="text-xs text-gray-500 dark:text-gray-400 mt-1">{{ stat.label }}</div>
          </div>
        </BaseCard>
      </div>

      <!-- Per-tool breakdown -->
      <BaseCard>
        <div class="p-4">
          <h2 class="text-lg font-semibold text-gray-900 dark:text-white mb-3">Usage by Tool</h2>
          <div v-if="byTool.length === 0" class="text-sm text-gray-500 dark:text-gray-400">No tool calls in this time range.</div>
          <table v-else class="w-full text-sm">
            <thead>
              <tr class="text-left text-gray-500 dark:text-gray-400 border-b border-gray-200 dark:border-gray-700">
                <th class="pb-2 font-medium">Tool</th>
                <th class="pb-2 font-medium text-right">Calls</th>
                <th class="pb-2 font-medium text-right">Avg (ms)</th>
                <th class="pb-2 font-medium text-right">P95 (ms)</th>
                <th class="pb-2 font-medium text-right">Errors</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="t in byTool" :key="t.tool_name" class="border-b border-gray-100 dark:border-gray-800">
                <td class="py-2 font-mono text-gray-900 dark:text-white">{{ t.tool_name }}</td>
                <td class="py-2 text-right text-gray-700 dark:text-gray-300">{{ t.call_count }}</td>
                <td class="py-2 text-right text-gray-700 dark:text-gray-300">{{ t.avg_duration_ms.toFixed(0) }}</td>
                <td class="py-2 text-right text-gray-700 dark:text-gray-300">{{ t.p95_duration_ms.toFixed(0) }}</td>
                <td class="py-2 text-right">
                  <span :class="t.error_count > 0 ? 'text-red-600 dark:text-red-400' : 'text-gray-500 dark:text-gray-400'">{{ t.error_count }}</span>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </BaseCard>

      <!-- Per-token breakdown -->
      <BaseCard>
        <div class="p-4">
          <h2 class="text-lg font-semibold text-gray-900 dark:text-white mb-3">Usage by Agent Token</h2>
          <div v-if="byToken.length === 0" class="text-sm text-gray-500 dark:text-gray-400">No token-attributed calls in this time range.</div>
          <table v-else class="w-full text-sm">
            <thead>
              <tr class="text-left text-gray-500 dark:text-gray-400 border-b border-gray-200 dark:border-gray-700">
                <th class="pb-2 font-medium">Token</th>
                <th class="pb-2 font-medium">Prefix</th>
                <th class="pb-2 font-medium text-right">Calls</th>
                <th class="pb-2 font-medium">Last Used</th>
                <th class="pb-2 font-medium">Tools Used</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="t in byToken" :key="t.key_prefix" class="border-b border-gray-100 dark:border-gray-800">
                <td class="py-2 text-gray-900 dark:text-white">{{ t.key_label || '(unnamed)' }}</td>
                <td class="py-2 font-mono text-gray-500 dark:text-gray-400">{{ t.key_prefix }}</td>
                <td class="py-2 text-right text-gray-700 dark:text-gray-300">{{ t.call_count }}</td>
                <td class="py-2 text-gray-500 dark:text-gray-400">{{ formatTime(t.last_used) }}</td>
                <td class="py-2">
                  <span v-for="tool in t.tools_used" :key="tool" class="inline-block text-xs px-2 py-0.5 mr-1 mb-1 rounded-full bg-gray-100 dark:bg-gray-800 text-gray-700 dark:text-gray-300">{{ tool }}</span>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </BaseCard>

      <!-- Timeline -->
      <BaseCard>
        <div class="p-4">
          <h2 class="text-lg font-semibold text-gray-900 dark:text-white mb-3">Usage Over Time</h2>
          <div v-if="timeline.length === 0" class="text-sm text-gray-500 dark:text-gray-400">No data for this time range.</div>
          <div v-else class="overflow-x-auto">
            <div class="flex items-end gap-1 h-32">
              <div
                v-for="(point, idx) in timeline"
                :key="idx"
                class="flex-1 min-w-[4px] rounded-t transition-all"
                :class="point.error_count > 0 ? 'bg-red-400 dark:bg-red-500' : 'bg-primary-400 dark:bg-primary-500'"
                :style="{ height: barHeight(point.call_count) }"
                :title="`${point.call_count} calls, ${point.error_count} errors at ${formatTime(point.timestamp)}`"
              ></div>
            </div>
          </div>
        </div>
      </BaseCard>

      <!-- Recent calls -->
      <BaseCard>
        <div class="p-4">
          <h2 class="text-lg font-semibold text-gray-900 dark:text-white mb-3">Recent Tool Calls</h2>
          <div v-if="recentCalls.length === 0" class="text-sm text-gray-500 dark:text-gray-400">No recent calls.</div>
          <table v-else class="w-full text-sm">
            <thead>
              <tr class="text-left text-gray-500 dark:text-gray-400 border-b border-gray-200 dark:border-gray-700">
                <th class="pb-2 font-medium">Time</th>
                <th class="pb-2 font-medium">Tool</th>
                <th class="pb-2 font-medium">Token</th>
                <th class="pb-2 font-medium text-right">Duration</th>
                <th class="pb-2 font-medium">Status</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="c in recentCalls" :key="c.trace_id" class="border-b border-gray-100 dark:border-gray-800 cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-800/50" @click="goToTrace(c.trace_id)">
                <td class="py-2 text-gray-500 dark:text-gray-400">{{ formatTime(c.timestamp) }}</td>
                <td class="py-2 font-mono text-gray-900 dark:text-white">{{ c.tool_name }}</td>
                <td class="py-2 text-gray-500 dark:text-gray-400">{{ c.key_label || c.key_prefix }}</td>
                <td class="py-2 text-right text-gray-700 dark:text-gray-300">{{ c.duration_ms.toFixed(0) }}ms</td>
                <td class="py-2">
                  <span :class="c.status === 'ok' ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'" class="text-xs font-medium px-2 py-0.5 rounded-full" :style="c.status === 'ok' ? 'background:rgb(220 252 231/.3)' : 'background:rgb(254 226 226/.3)'">{{ c.status }}</span>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </BaseCard>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, watch, onMounted } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import axios from 'axios';
import AppLayout from '@/Layouts/AppLayout.vue';
import BaseCard from '@/components/BaseCard.vue';

const route = useRoute();
const router = useRouter();
const projectId = computed(() => route.params.id);
const project = computed(() => ({ id: projectId.value }));

const timeRange = ref('24h');
const stats = ref(null);
const byTool = ref([]);
const byToken = ref([]);
const timeline = ref([]);
const recentCalls = ref([]);

const statCards = computed(() => {
  const s = stats.value || {};
  return [
    { label: 'Total Calls', value: s.total_calls ?? '—' },
    { label: 'Unique Tools', value: s.unique_tools ?? '—' },
    { label: 'Avg Latency', value: s.avg_duration_ms ? `${s.avg_duration_ms.toFixed(0)}ms` : '—' },
    { label: 'Error Rate', value: s.total_calls ? `${(s.error_rate * 100).toFixed(1)}%` : '—' },
  ];
});

function formatTime(ts) {
  if (!ts) return '';
  const d = new Date(ts);
  return d.toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
}

function barHeight(count) {
  const maxCount = Math.max(...timeline.value.map(p => p.call_count), 1);
  return `${Math.max((count / maxCount) * 100, 2)}%`;
}

function goToTrace(traceId) {
  router.push(`/p/${projectId.value}/traces/${traceId}`);
}

async function fetchAll() {
  const tr = timeRange.value;
  const pid = projectId.value;
  const [s, bt, bk, tl, rc] = await Promise.allSettled([
    axios.get(`/api/projects/${pid}/mcp/stats`, { params: { time_range: tr } }),
    axios.get(`/api/projects/${pid}/mcp/stats/by-tool`, { params: { time_range: tr } }),
    axios.get(`/api/projects/${pid}/mcp/stats/by-token`, { params: { time_range: tr } }),
    axios.get(`/api/projects/${pid}/mcp/stats/timeline`, { params: { time_range: tr } }),
    axios.get(`/api/projects/${pid}/mcp/calls`, { params: { limit: 25 } }),
  ]);
  if (s.status === 'fulfilled') stats.value = s.value.data;
  if (bt.status === 'fulfilled') byTool.value = bt.value.data;
  if (bk.status === 'fulfilled') byToken.value = bk.value.data;
  if (tl.status === 'fulfilled') timeline.value = tl.value.data;
  if (rc.status === 'fulfilled') recentCalls.value = rc.value.data;
}

watch(timeRange, fetchAll);
onMounted(fetchAll);
</script>
