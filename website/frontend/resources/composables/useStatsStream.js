import { ref, onUnmounted } from 'vue';

/**
 * Composable for connecting to Server-Sent Events (SSE) stream for real-time stats updates
 * @param {string} projectId - The project ID to listen for updates
 * @param {Function} onUpdate - Callback function called when stats update event is received
 */
export function useStatsStream(projectId, onUpdate) {
  const eventSource = ref(null);
  const isConnected = ref(false);
  const error = ref(null);

  function connect() {
    if (eventSource.value) {
      disconnect();
    }

    // Don't connect if no project ID
    if (!projectId) {
      return;
    }

    try {
      const url = `/api/projects/${projectId}/stats/stream`;
      eventSource.value = new EventSource(url, {
        withCredentials: true,
      });

      eventSource.value.onopen = () => {
        isConnected.value = true;
        error.value = null;
        // Only log in development
        if (process.env.NODE_ENV === 'development') {
          console.log('Stats stream connected');
        }
      };

      eventSource.value.onmessage = (event) => {
        try {
          const statsData = JSON.parse(event.data);
          onUpdate(statsData);
        } catch (err) {
          console.error('Failed to parse stats data from SSE:', err);
        }
      };

      eventSource.value.onerror = (err) => {
        const es = eventSource.value;
        if (!es) return;
        
        // EventSource automatically reconnects on error
        // Only log errors if connection is actually closed (not just reconnecting)
        if (es.readyState === EventSource.CLOSED) {
          // Connection closed - will auto-reconnect, but log only in dev
          if (process.env.NODE_ENV === 'development') {
            console.warn('Stats stream closed');
          }
          isConnected.value = false;
        } else {
          // CONNECTING or OPEN - suppress console noise
          isConnected.value = es.readyState === EventSource.OPEN;
        }
      };
    } catch (err) {
      console.error('Failed to create EventSource:', err);
      error.value = err.message;
    }
  }

  function disconnect() {
    if (eventSource.value) {
      eventSource.value.close();
      eventSource.value = null;
      isConnected.value = false;
    }
  }

  // Cleanup on unmount
  onUnmounted(() => {
    disconnect();
  });

  return {
    connect,
    disconnect,
    isConnected,
    error,
  };
}
