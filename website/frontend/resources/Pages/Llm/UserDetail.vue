<template>
  <AppLayout :user="user" :current-project="project"><div class="max-w-[1200px] mx-auto px-8 py-6">
    <div class="mb-6"><router-link :to="`/p/${projectId}/llm/users`" class="text-sm text-primary-600 hover:underline">← Users</router-link><h1 class="mt-2 text-2xl font-semibold dark:text-gray-100">User detail</h1><p class="font-mono text-sm text-gray-500 break-all">{{ userId }}</p></div>
    <div v-if="loading" class="py-12 text-center text-gray-500">Loading user…</div><template v-else-if="detail">
      <div class="grid grid-cols-2 md:grid-cols-6 gap-3 mb-6"><BaseCard v-for="m in metrics" :key="m.label"><div class="text-xs text-gray-500">{{ m.label }}</div><div class="mt-1 font-semibold dark:text-gray-100">{{ m.value }}</div></BaseCard></div>
      <div class="rounded-lg border border-amber-200 bg-amber-50 dark:bg-amber-900/20 dark:border-amber-800 px-4 py-3 text-sm text-amber-800 dark:text-amber-200 mb-6">{{ detail.retention_notice }}</div>
      <BaseCard><template #header><h2 class="text-lg font-semibold dark:text-gray-100">Session timeline</h2></template>
        <div v-for="s in detail.sessions" :key="s.session_id" class="py-4 border-b last:border-0 dark:border-gray-700">
          <div class="flex justify-between gap-4"><div><router-link v-if="s.has_saved_content" :to="`/p/${projectId}${s.saved_session_path}`" class="font-mono text-primary-600 hover:underline">{{ s.session_name || s.session_id }}</router-link><span v-else class="font-mono dark:text-gray-100">{{ s.session_name || s.session_id }}</span><div class="text-xs text-gray-500 mt-1">{{ formatDate(s.first_session_timestamp) }} → {{ formatDate(s.last_session_timestamp) }}</div></div><span class="text-xs" :class="s.has_saved_content?'text-green-600':'text-gray-400'">{{ s.has_saved_content ? 'Saved content' : 'Aggregate only' }}</span></div>
          <div class="mt-2 text-sm text-gray-600 dark:text-gray-400">{{ s.request_count }} requests · ${{ cost(s.cost) }} · {{ s.error_count }} errors · {{ s.models.join(', ') || 'No model' }}</div>
          <div class="mt-2 flex flex-wrap gap-1"><span v-for="l in s.labels" :key="l" class="rounded bg-blue-100 dark:bg-blue-900/40 px-2 py-0.5 text-xs text-blue-700 dark:text-blue-300">{{ l }}</span><span v-for="p in s.matched_profiles" :key="p.profile_id" class="rounded bg-purple-100 dark:bg-purple-900/40 px-2 py-0.5 text-xs text-purple-700 dark:text-purple-300">{{ p.profile_name || p.profile_id }}</span></div>
        </div></BaseCard>
    </template><div v-else class="py-12 text-center text-gray-500">User not found.</div>
  </div></AppLayout>
</template>
<script setup>
import { computed,onMounted,ref } from 'vue'; import {useRoute} from 'vue-router'; import axios from 'axios'; import AppLayout from '@/Layouts/AppLayout.vue'; import BaseCard from '@/components/BaseCard.vue'; import {useAuth} from '@/composables/useAuth';
const route=useRoute(),projectId=route.params.id,userId=route.params.userId; const {user}=useAuth();const project={id:projectId};const detail=ref(null),loading=ref(true);const cost=v=>Number(v||0).toFixed(4),formatDate=v=>v?new Date(v).toLocaleString():'—';
const metrics=computed(()=>detail.value?[{label:'First seen',value:formatDate(detail.value.first_seen)},{label:'Last seen',value:formatDate(detail.value.last_seen)},{label:'Sessions',value:detail.value.session_count},{label:'Requests',value:detail.value.request_count},{label:'Cost',value:`$${cost(detail.value.total_cost)}`},{label:'Errors',value:`${detail.value.error_count} (${(detail.value.error_rate*100).toFixed(1)}%)`}]:[]);
onMounted(async()=>{try{detail.value=(await axios.get(`/api/projects/${projectId}/llm/users/detail`,{params:{user_id:userId}})).data}catch{}finally{loading.value=false}});
</script>
