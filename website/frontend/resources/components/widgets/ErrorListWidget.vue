<template>
  <div class="error-list-widget">
    <div v-if="loading" class="flex items-center justify-center h-full">
      <div class="spinner w-6 h-6 border-2 border-primary-600 border-t-transparent rounded-full"></div>
    </div>
    <div v-else-if="error" class="text-center text-danger-400 text-sm p-4">
      {{ error }}
    </div>
    <div v-else-if="errors.length === 0" class="text-center text-gray-400 text-sm py-8">
      No errors
    </div>
    <div v-else class="error-list">
      <div
        v-for="errorItem in errors"
        :key="errorItem.id"
        class="error-item p-3 border-b border-gray-700 hover:bg-gray-700/50 cursor-pointer transition-colors"
        @click="$router.push(`/p/${projectId}/errors/${errorItem.id}`)"
      >
        <div class="flex items-start justify-between">
          <div class="flex-1 min-w-0">
            <div class="text-sm font-medium text-gray-100 truncate">{{ errorItem.message }}</div>
            <div class="flex items-center gap-2 mt-1">
              <span
                :class="[
                  errorItem.level === 'error' ? 'bg-danger-900/30 text-danger-300' : 'bg-warning-900/30 text-warning-300',
                  'px-2 py-0.5 text-xs font-semibold rounded'
                ]"
              >
                {{ errorItem.level }}
              </span>
              <span class="text-xs text-gray-400">×{{ errorItem.count }}</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, watch } from 'vue'
import { useRouter } from 'vue-router'
import axios from 'axios'

const props = defineProps({
  config: {
    type: Object,
    required: true,
  },
  projectId: {
    type: String,
    required: true,
  },
  dashboardId: {
    type: String,
    required: true,
  },
})

const router = useRouter()
const errors = ref([])
const loading = ref(true)
const error = ref(null)

const fetchErrors = async () => {
  loading.value = true
  error.value = null

  try {
    const limit = props.config.limit || 10
    const response = await axios.get(`/api/projects/${props.projectId}/exceptions`, {
      params: { limit },
    })
    errors.value = response.data || []
  } catch (err) {
    console.error('Failed to fetch errors:', err)
    error.value = err.response?.data?.message || err.message || 'Failed to load errors'
  } finally {
    loading.value = false
  }
}

onMounted(() => {
  fetchErrors()
})
</script>

<style scoped>
.error-list-widget {
  @apply h-full flex flex-col;
}

.error-list {
  @apply flex-1 overflow-y-auto;
}

.error-item {
  @apply transition-colors;
}

.spinner {
  animation: spin 0.6s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}
</style>


