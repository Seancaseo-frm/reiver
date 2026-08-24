<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-[1400px] mx-auto px-8 py-6">
      <div class="mb-6"><h1 class="text-2xl font-semibold text-gray-900 dark:text-gray-100">Users</h1><p class="text-sm text-gray-500 mt-1">Opaque customer-supplied identities, scoped to this project.</p></div>
      <BaseCard>
        <div v-if="loading" class="py-12 text-center text-gray-500">Loading users…</div>
        <div v-else-if="!users.length" class="py-12 text-center text-gray-500">No attributed users found.</div>
        <div v-else class="overflow-x-auto"><table class="w-full text-sm">
          <thead><tr class="border-b dark:border-gray-700 text-xs uppercase text-gray-500">
            <th v-for="h in headers" :key="h" class="text-left px-3 py-3">{{ h }}</th>
          </tr></thead><tbody><tr v-for="u in users" :key="u.user_id" class="border-b dark:border-gray-800 hover:bg-gray-50 dark:hover:bg-gray-800/50">
            <td class="px-3 py-3"><router-link class="font-mono text-primary-600 hover:underline break-all" :to="userPath(u.user_id)">{{ u.user_id }}</router-link></td>
            <td class="px-3 py-3 whitespace-nowrap">{{ formatDate(u.first_seen) }}</td><td class="px-3 py-3 whitespace-nowrap">{{ formatDate(u.last_seen) }}</td>
            <td class="px-3 py-3 tabular-nums">{{ u.session_count }}</td><td class="px-3 py-3 tabular-nums">{{ u.request_count }}</td>
            <td class="px-3 py-3 tabular-nums">${{ formatCost(u.total_cost) }}</td><td class="px-3 py-3 tabular-nums">{{ u.error_count }} ({{ percent(u.error_rate) }})</td>
            <td class="px-3 py-3">{{ u.models.join(', ') || '—' }}</td><td class="px-3 py-3"><span v-for="p in u.matched_profiles" :key="p.profile_id" class="mr-1 inline-flex rounded bg-purple-100 dark:bg-purple-900/40 px-2 py-0.5 text-xs text-purple-700 dark:text-purple-300">{{ p.profile_name || p.profile_id }}</span><span v-if="!u.matched_profiles.length">—</span></td>
          </tr></tbody></table></div>
      </BaseCard>
    </div>
  </AppLayout>
</template>
<script setup>
import { onMounted, ref } from 'vue'; import { useRoute } from 'vue-router'; import axios from 'axios';
import AppLayout from '@/Layouts/AppLayout.vue'; import BaseCard from '@/components/BaseCard.vue'; import { useAuth } from '@/composables/useAuth';
const route=useRoute(), projectId=route.params.id; const {user}=useAuth(); const project={id:projectId}; const users=ref([]), loading=ref(true);
const headers=['User ID','First Seen','Last Seen','Sessions','Requests','Cost','Errors','Models','Matched Session Profiles'];
const userPath=id=>`/p/${projectId}/llm/users/${encodeURIComponent(id)}`; const formatDate=v=>v?new Date(v).toLocaleString():'—'; const formatCost=v=>Number(v||0).toFixed(4); const percent=v=>`${(Number(v||0)*100).toFixed(1)}%`;
onMounted(async()=>{try{users.value=(await axios.get(`/api/projects/${projectId}/llm/users`)).data?.users||[]}finally{loading.value=false}});
</script>
