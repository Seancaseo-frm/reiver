<template>
  <AppLayout :user="user">
    <div class="max-w-2xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <div class="mb-8">
        <h1 class="text-3xl font-bold text-gray-900">Create New Project</h1>
        <p class="mt-2 text-gray-600">Set up a new project to start tracking errors</p>
      </div>

      <div class="bg-white rounded-lg shadow p-6 border border-gray-200">
        <form @submit.prevent="submit">
          <div v-if="organizations.length > 1" class="mb-4">
            <label for="organization" class="block text-sm font-medium text-gray-700">
              Organization
            </label>
            <select
              id="organization"
              v-model="selectedOrgId"
              class="mt-1 block w-full rounded-md border-gray-300 bg-white text-gray-900 shadow-sm focus:border-primary-500 focus:ring-primary-500 sm:text-sm"
            >
              <option v-for="org in organizations" :key="org.id" :value="org.id">
                {{ org.name }}
              </option>
            </select>
          </div>

          <div>
            <label for="name" class="block text-sm font-medium text-gray-700">
              Project Name
            </label>
            <input
              id="name"
              v-model="name"
              type="text"
              required
              class="mt-1 block w-full rounded-md border-gray-300 bg-white text-gray-900 shadow-sm focus:border-primary-500 focus:ring-primary-500 sm:text-sm placeholder-gray-400"
              placeholder="My Awesome Project"
            />
            <div v-if="error" class="mt-2 text-sm text-red-600 bg-red-50 p-3 rounded-md">
              {{ error }}
            </div>
          </div>

          <div class="mt-6 flex justify-end space-x-4">
            <router-link
              to="/dashboard"
              class="px-4 py-2 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 bg-white hover:bg-gray-50"
            >
              Cancel
            </router-link>
            <button
              type="submit"
              :disabled="processing"
              class="px-4 py-2 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 disabled:opacity-50"
            >
              {{ processing ? 'Creating...' : 'Create Project' }}
            </button>
          </div>
        </form>
      </div>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, onMounted } from 'vue';
import { useRouter } from 'vue-router';
import axios from 'axios';
import AppLayout from '@/Layouts/AppLayout.vue';
import { useAuth } from '@/composables/useAuth';

const router = useRouter();
const { user, fetchUser } = useAuth();
const name = ref('');
const error = ref('');
const processing = ref(false);
const organizations = ref([]);
const selectedOrgId = ref(null);

onMounted(async () => {
  try {
    await fetchUser();
  } catch (err) {
    router.push('/login');
    return;
  }

  try {
    const orgsRes = await axios.get('/api/organizations');
    organizations.value = orgsRes.data || [];
    if (organizations.value.length > 0) {
      selectedOrgId.value = organizations.value[0].id;
    }
  } catch {
    // Organizations not available; project will use default org
  }
});

const submit = async () => {
  if (!name.value.trim()) {
    error.value = 'Project name is required';
    return;
  }

  processing.value = true;
  error.value = '';
  
  try {
    console.log('Creating project:', name.value);
    
    const payload = { name: name.value.trim() };
    if (selectedOrgId.value) {
      payload.organization_id = selectedOrgId.value;
    }

    const response = await axios.post('/api/projects', payload, {
      timeout: 10000,
    });
    
    console.log('Project created:', response.data);
    
    // Check if response has an id
    if (response.data && response.data.id) {
      // Small delay to ensure user sees success
      await new Promise(resolve => setTimeout(resolve, 100));
      await router.push(`/p/${response.data.id}`);
    } else {
      throw new Error('Invalid response from server: missing project id');
    }
  } catch (err) {
    console.error('Failed to create project:', err);
    console.error('Error response:', err.response);
    
    // Try different error response formats
    let errorMessage = 'Failed to create project. Please try again.';
    
    if (err.response) {
      // Handle different error formats
      if (err.response.data) {
        errorMessage = err.response.data.error 
          || err.response.data.message 
          || `Server error: ${err.response.status}`;
      } else {
        errorMessage = `Server error: ${err.response.status} ${err.response.statusText}`;
      }
    } else if (err.request) {
      errorMessage = 'Network error: Could not reach server. Please check your connection.';
    } else {
      errorMessage = err.message || errorMessage;
    }
    
    error.value = errorMessage;
  } finally {
    // Always reset processing state
    processing.value = false;
  }
};
</script>
