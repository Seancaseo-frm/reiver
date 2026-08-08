import { ref, onMounted, onUnmounted, watch } from 'vue'
import { useRoute } from 'vue-router'

export function useErrorsStream(projectId) {
  const errors = ref([])
  const stats = ref(null)
  const error = ref(null)
  const eventSource = ref(null)

  const connect = () => {
    if (!projectId.value) return

    // Close existing connection if any
    if (eventSource.value) {
      eventSource.value.close()
    }

    const url = `/api/projects/${projectId.value}/stats/stream`
    eventSource.value = new EventSource(url)

    eventSource.value.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data)
        
        // Update stats
        if (data.stats) {
          stats.value = data.stats
        }
        
        // Update errors if included
        if (data.exception_groups) {
          errors.value = data.exception_groups
        }
        
        error.value = null
      } catch (e) {
        console.error('Failed to parse SSE message:', e)
        error.value = 'Failed to parse server message'
      }
    }

    eventSource.value.onerror = (err) => {
      console.error('SSE connection error:', err)
      error.value = 'Connection error'
      
      // Reconnect after 3 seconds
      setTimeout(() => {
        if (eventSource.value?.readyState === EventSource.CLOSED) {
          connect()
        }
      }, 3000)
    }
  }

  const disconnect = () => {
    if (eventSource.value) {
      eventSource.value.close()
      eventSource.value = null
    }
  }

  // Watch for route changes and disconnect when leaving
  const route = useRoute()
  watch(() => route.path, (newPath) => {
    if (!newPath.includes(`/p/${projectId.value}/exceptions`)) {
      disconnect()
    }
  })

  onMounted(() => {
    connect()
  })

  onUnmounted(() => {
    disconnect()
  })

  return {
    errors,
    stats,
    error,
    reconnect: connect,
  }
}


