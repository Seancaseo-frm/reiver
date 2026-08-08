<template>
  <AppLayout :project="project">
    <div class="max-w-[1200px] mx-auto px-4 py-6 space-y-6">
      <div class="flex items-center justify-between">
        <div>
          <h1 class="text-2xl font-bold text-gray-900 dark:text-white">Tool Catalog</h1>
          <p class="mt-1 text-sm text-gray-500 dark:text-gray-400">{{ tools.length }} tool{{ tools.length !== 1 ? 's' : '' }} observed in the last 30 days</p>
        </div>
      </div>

      <div v-if="loading" class="text-sm text-gray-500 dark:text-gray-400">Loading tools…</div>

      <div v-else-if="tools.length === 0" class="text-center py-12 text-gray-500 dark:text-gray-400">
        <p class="text-sm">No tools observed yet.</p>
        <p class="text-xs mt-1">Tools appear here automatically when your agents or LLM requests use function calling.</p>
      </div>

      <div v-else class="grid gap-4 md:grid-cols-2">
        <BaseCard v-for="tool in tools" :key="tool.name">
          <div class="p-4 space-y-3">
            <div class="flex items-center gap-2">
              <h3 class="text-base font-semibold text-gray-900 dark:text-white font-mono">{{ tool.name }}</h3>
              <span v-if="tool.blocked_project_wide" class="text-xs px-2 py-0.5 rounded-full bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300 font-medium">Blocked</span>
            </div>
            <div class="flex items-center gap-4 text-xs text-gray-500 dark:text-gray-400">
              <span>{{ formatNumber(tool.total_calls) }} calls</span>
              <span>{{ formatNumber(tool.request_count) }} requests</span>
              <span v-if="tool.last_used">Last used {{ formatRelative(tool.last_used) }}</span>
            </div>
            <details v-if="tool.blocked_by_prompts && tool.blocked_by_prompts.length" class="text-xs">
              <summary class="cursor-pointer text-amber-600 dark:text-amber-400 hover:underline">Blocked by {{ tool.blocked_by_prompts.length }} prompt{{ tool.blocked_by_prompts.length > 1 ? 's' : '' }}</summary>
              <ul class="mt-2 space-y-1 pl-4 list-disc text-gray-600 dark:text-gray-400">
                <li v-for="p in tool.blocked_by_prompts" :key="p.prompt_id">
                  <router-link :to="`/p/${projectId}/llm/prompts/${p.prompt_id}`" class="text-primary-600 dark:text-primary-400 hover:underline">{{ p.prompt_name }}</router-link>
                </li>
              </ul>
            </details>
          </div>
        </BaseCard>
      </div>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue';
import { useRoute } from 'vue-router';
import axios from 'axios';
import AppLayout from '@/Layouts/AppLayout.vue';
import BaseCard from '@/components/BaseCard.vue';

const route = useRoute();
const projectId = computed(() => route.params.id);
const project = computed(() => ({ id: projectId.value }));

const tools = ref([]);
const loading = ref(true);

function formatNumber(num) {
  if (num == null) return '0';
  return Number(num).toLocaleString();
}

function formatRelative(ts) {
  if (!ts) return '';
  const d = new Date(ts);
  const now = new Date();
  const diffMin = Math.floor((now - d) / 60000);
  if (diffMin < 1) return 'just now';
  if (diffMin < 60) return `${diffMin}m ago`;
  const diffHrs = Math.floor(diffMin / 60);
  if (diffHrs < 24) return `${diffHrs}h ago`;
  const diffDays = Math.floor(diffHrs / 24);
  if (diffDays < 30) return `${diffDays}d ago`;
  return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
}

onMounted(async () => {
  try {
    const { data } = await axios.get(`/api/projects/${projectId.value}/mcp/tools`);
    tools.value = data;
  } catch (e) {
    console.error('Failed to load tools', e);
  } finally {
    loading.value = false;
  }
});
</script>
