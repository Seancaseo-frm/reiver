<template>
  <AppLayout :user="user" :current-project="currentProject">
    <div class="system-overview max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <div class="mb-6 flex items-center justify-between">
        <div>
          <h1 class="text-2xl font-bold text-gray-900">System Overview</h1>
          <p class="text-sm text-gray-500 mt-1">
            Cross-stack correlation — hover to compare, drag to investigate
          </p>
        </div>
        <select v-model="timeRange" @change="refreshStack" class="rounded-md border-gray-300 text-sm shadow-sm focus:border-primary-500 focus:ring-primary-500">
          <option value="15m">Last 15 minutes</option>
          <option value="1h">Last 1 hour</option>
          <option value="6h">Last 6 hours</option>
          <option value="24h">Last 24 hours</option>
          <option value="7d">Last 7 days</option>
        </select>
      </div>

      <!-- Loading -->
      <div v-if="loading" class="flex items-center justify-center py-20">
        <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full animate-spin"></div>
      </div>

      <!-- Empty State -->
      <div v-else-if="!stack.length" class="text-center py-20">
        <svg class="w-16 h-16 mx-auto text-gray-300 mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M9 17v-2m3 2v-4m3 4v-6m2 10H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
        </svg>
        <h3 class="text-lg font-medium text-gray-900 mb-1">No stack detected</h3>
        <p class="text-gray-500 text-sm max-w-md mx-auto">
          We couldn't detect any technologies in this project. Make sure your services are instrumented with OpenTelemetry and sending metrics.
        </p>
        <a href="/integrations" class="mt-4 inline-block text-primary-600 hover:text-primary-700 text-sm font-medium">
          Set up integrations &rarr;
        </a>
      </div>

      <!-- Stack Lanes -->
      <div v-else class="space-y-6">
        <StackLane
          v-for="tier in stack"
          :key="tier.technology"
          :technology="tier.technology"
          :tier="tier.tier"
          :golden-signals="tier.golden_signals"
          :project-id="projectId"
          :time-range="timeRange"
          :cursor-time="cursorTime"
          @update:cursor-time="cursorTime = $event"
          @select-range="handleRangeSelect"
        />
      </div>

      <!-- Correlation Drawer -->
      <CorrelationDrawer
        v-if="selectedRange"
        :project-id="projectId"
        :start-ms="selectedRange.start_ms"
        :end-ms="selectedRange.end_ms"
        @close="selectedRange = null"
      />
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRoute } from 'vue-router'
import { useAuth } from '@/composables/useAuth'
import axios from 'axios'
import AppLayout from '@/Layouts/AppLayout.vue'
import StackLane from '@/components/StackLane.vue'
import CorrelationDrawer from '@/components/CorrelationDrawer.vue'

const route = useRoute()
const { user } = useAuth()

const projectId = computed(() => route.params.id)
const currentProject = ref(null)
const loading = ref(true)
const stack = ref([])
const timeRange = ref('1h')
const cursorTime = ref(null)
const selectedRange = ref(null)

async function fetchProject() {
  try {
    const response = await axios.get(`/api/projects/${projectId.value}`)
    currentProject.value = response.data
  } catch (err) {
    console.error('Failed to fetch project:', err)
  }
}

async function refreshStack() {
  loading.value = true
  try {
    const response = await axios.get(`/api/system-overview/${projectId.value}/stack`)
    stack.value = response.data.tiers || []
  } catch (err) {
    console.error('Failed to load stack:', err)
    stack.value = []
  } finally {
    loading.value = false
  }
}

function handleRangeSelect(range) {
  selectedRange.value = range
}

onMounted(() => {
  fetchProject()
  refreshStack()
})
</script>

<style scoped>
.system-overview {
  min-height: calc(100vh - 4rem);
}
</style>
