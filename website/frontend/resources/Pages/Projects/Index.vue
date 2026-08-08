<template>
  <AppLayout :user="user">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <div class="flex justify-between items-center mb-8">
        <div>
          <h1 class="text-3xl font-bold text-gray-900">Projects</h1>
          <p class="mt-2 text-gray-600">Manage your error monitoring projects</p>
        </div>
        <router-link
          to="/projects/create"
          class="bg-indigo-600 text-white px-4 py-2 rounded-md text-sm font-medium hover:bg-indigo-700"
        >
          New Project
        </router-link>
      </div>

      <div v-if="projects.length === 0" class="bg-white rounded-lg shadow p-12 text-center">
        <svg class="mx-auto h-12 w-12 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10" />
        </svg>
        <h3 class="mt-2 text-sm font-medium text-gray-900">No projects</h3>
        <p class="mt-1 text-sm text-gray-500">Get started by creating a new project.</p>
        <div class="mt-6">
          <router-link
            to="/projects/create"
            class="inline-flex items-center px-4 py-2 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-indigo-600 hover:bg-indigo-700"
          >
            New Project
          </router-link>
        </div>
      </div>

      <div v-else class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        <div
          v-for="project in projects"
          :key="project.id"
          class="bg-white rounded-lg shadow hover:shadow-lg transition-shadow cursor-pointer"
          @click="$router.push(`/p/${project.id}`)"
        >
          <div class="p-6">
            <div class="flex items-center justify-between mb-4">
              <h3 class="text-lg font-semibold text-gray-900">{{ project.name }}</h3>
              <svg class="w-5 h-5 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
              </svg>
            </div>
            <p class="text-sm text-gray-500">
              Created {{ formatDate(project.created_at) }}
            </p>
          </div>
        </div>
      </div>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, onMounted } from 'vue';
import { useRouter } from 'vue-router';
import { format } from 'date-fns';
import axios from 'axios';
import AppLayout from '@/Layouts/AppLayout.vue';
import { useAuth } from '@/composables/useAuth';

const router = useRouter();
const { user, fetchUser } = useAuth();
const projects = ref([]);

onMounted(async () => {
  try {
    // Fetch user if not already cached (should be cached from router guard)
    await fetchUser();
    // Fetch projects
    const projectsResponse = await axios.get('/api/projects');
    projects.value = projectsResponse.data || [];
  } catch (err) {
    console.error('Failed to load projects:', err);
    router.push('/login');
  }
});

const formatDate = (dateString) => {
  return format(new Date(dateString), 'MMM d, yyyy');
};
</script>
