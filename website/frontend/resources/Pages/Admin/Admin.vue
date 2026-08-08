<template>
  <AppLayout :user="user" :current-project="currentProject">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <h1 class="text-2xl font-bold text-gray-900 mb-6">Admin</h1>

      <div class="border-b border-gray-200 mb-6">
        <nav class="-mb-px flex flex-wrap gap-x-6 gap-y-1" aria-label="Tabs">
          <button
            v-for="tab in tabs"
            :key="tab.id"
            type="button"
            @click="setTab(tab.id)"
            :class="[
              'whitespace-nowrap py-3 px-1 border-b-2 text-sm font-medium transition-colors',
              activeTab === tab.id
                ? 'border-blue-500 text-blue-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            ]"
          >
            {{ tab.label }}
          </button>
        </nav>
      </div>

      <AdminOrganizations v-if="activeTab === 'organizations'" :schema="schema" />
      <AdminUsers v-if="activeTab === 'users'" />
      <AdminTierDefinitions v-if="activeTab === 'tiers'" :schema="schema" />
      <AdminBilling v-if="activeTab === 'billing'" />
      <AdminModelCatalog v-if="activeTab === 'models'" />
      <AdminDashboards v-if="activeTab === 'dashboards'" />
      <AdminKnowledgeBase v-if="activeTab === 'knowledge-base'" />
      <AdminTemplates v-if="activeTab === 'templates'" />
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, onMounted, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import axios from 'axios';
import AppLayout from '@/Layouts/AppLayout.vue';
import { useAuth } from '@/composables/useAuth';
import AdminOrganizations from './AdminOrganizations.vue';
import AdminUsers from './AdminUsers.vue';
import AdminTierDefinitions from './AdminTierDefinitions.vue';
import AdminBilling from './AdminBilling.vue';
import AdminModelCatalog from './AdminModelCatalog.vue';
import AdminDashboards from './AdminDashboards.vue';
import AdminKnowledgeBase from './AdminKnowledgeBase.vue';
import AdminTemplates from './AdminTemplates.vue';

const { user } = useAuth();
const route = useRoute();
const router = useRouter();
const currentProject = ref(null);

const tabs = [
  { id: 'organizations', label: 'Organizations' },
  { id: 'users', label: 'Users' },
  { id: 'tiers', label: 'Tiers' },
  { id: 'billing', label: 'Billing' },
  { id: 'models', label: 'Model Catalog' },
  { id: 'dashboards', label: 'Dashboards' },
  { id: 'knowledge-base', label: 'Knowledge Base' },
  { id: 'templates', label: 'Templates' },
];

const tabIds = new Set(tabs.map(t => t.id));
const activeTab = ref('organizations');

const schema = ref({});

function setTab(id) {
  activeTab.value = id;
  router.replace({ query: { ...route.query, tab: id } });
}

function syncTabFromRoute() {
  const t = route.query.tab;
  if (typeof t === 'string' && tabIds.has(t)) {
    activeTab.value = t;
  }
}

watch(() => route.query.tab, () => syncTabFromRoute());

onMounted(async () => {
  syncTabFromRoute();
  try {
    const { data } = await axios.get('/api/projects');
    if (data?.length > 0) currentProject.value = data[0];
  } catch (_) {}
  try {
    const { data } = await axios.get('/api/admin/tiers/schema');
    schema.value = data;
  } catch (_) {}
});
</script>
