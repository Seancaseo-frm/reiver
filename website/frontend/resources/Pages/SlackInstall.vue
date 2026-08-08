<template>
  <div class="min-h-screen flex items-center justify-center bg-gray-50 py-12 px-4">
    <div class="max-w-md w-full space-y-6">
      <div class="text-center">
        <h1 class="text-2xl font-bold text-gray-900">Add Reiver to Slack</h1>
        <p class="mt-2 text-sm text-gray-600">
          {{ pendingKey ? 'Choose which project to connect with your Slack workspace.' : 'Choose a project to start the Slack integration.' }}
        </p>
      </div>

      <div v-if="error" class="bg-red-50 border border-red-200 rounded-lg p-4">
        <p class="text-sm text-red-700">{{ error }}</p>
      </div>

      <div v-if="success" class="bg-green-50 border border-green-200 rounded-lg p-4 text-center">
        <p class="text-sm text-green-700 font-medium">Slack integration installed successfully.</p>
        <a
          :href="`/p/${successProjectId}/integrations`"
          class="mt-2 inline-block text-sm text-brand-600 hover:text-brand-700 underline"
        >
          Go to Integrations
        </a>
      </div>

      <div v-if="loading && !success" class="text-center py-8 text-gray-500">
        <div class="spinner w-8 h-8 border-4 border-brand-600 border-t-transparent rounded-full mx-auto mb-3"></div>
        <p>{{ finalizing ? 'Setting up integration...' : 'Loading projects...' }}</p>
      </div>

      <div v-else-if="!success && projects.length === 0" class="text-center py-8">
        <p class="text-gray-500 mb-4">You don't have any projects yet.</p>
        <a
          href="/projects/create"
          class="inline-flex items-center px-4 py-2 text-sm font-medium text-white bg-brand-600 hover:bg-brand-700 rounded-lg transition-colors"
        >
          Create a Project
        </a>
      </div>

      <div v-else-if="!success" class="space-y-3">
        <button
          v-for="project in projects"
          :key="project.id"
          @click="selectProject(project.id)"
          :disabled="finalizing"
          class="w-full flex items-center justify-between p-4 bg-white border border-gray-200 rounded-lg hover:border-brand-500 hover:shadow-sm transition-all text-left disabled:opacity-50"
        >
          <div>
            <p class="text-sm font-medium text-gray-900">{{ project.name }}</p>
            <p class="text-xs text-gray-500 mt-0.5">{{ project.id }}</p>
          </div>
          <svg class="w-5 h-5 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
          </svg>
        </button>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue';
import { useRoute } from 'vue-router';
import axios from 'axios';

const route = useRoute();
const loading = ref(true);
const finalizing = ref(false);
const projects = ref([]);
const error = ref('');
const success = ref(false);
const successProjectId = ref('');

const pendingKey = route.query.pending || '';

const selectProject = async (projectId) => {
  if (pendingKey) {
    await finalizePendingInstall(projectId);
  } else {
    window.location.href = `/api/slack/oauth/install?project_id=${projectId}`;
  }
};

const finalizePendingInstall = async (projectId) => {
  finalizing.value = true;
  loading.value = true;
  error.value = '';
  try {
    await axios.post('/api/slack/oauth/finalize', {
      pending_key: pendingKey,
      project_id: projectId,
    });
    success.value = true;
    successProjectId.value = projectId;
  } catch (err) {
    error.value = err.response?.data?.message || 'Failed to finalize Slack installation. The link may have expired — please try again.';
  } finally {
    loading.value = false;
    finalizing.value = false;
  }
};

onMounted(async () => {
  if (route.query.slack === 'denied') {
    error.value = 'Slack authorization was denied.';
  }

  try {
    const res = await axios.get('/api/projects');
    projects.value = res.data || [];

    if (projects.value.length === 1 && !pendingKey) {
      selectProject(projects.value[0].id);
      return;
    }
  } catch (err) {
    console.error('Failed to load projects:', err);
    error.value = 'Failed to load projects.';
  } finally {
    loading.value = false;
  }
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
