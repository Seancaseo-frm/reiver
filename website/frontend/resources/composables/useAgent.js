import { ref, computed, watch } from 'vue';
import { useRoute } from 'vue-router';
import axios from 'axios';
import { usePageContext } from '@/composables/usePageContext';
import { useCurrentProject } from '@/composables/useCurrentProject';
import { resolveApiUrl, resolveSlug } from '@/composables/projectResolver';

const STORAGE_OPEN = 'agentPanelOpen';
const STORAGE_CONV_PREFIX = 'agentConversationId:';

const isOpen = ref(localStorage.getItem(STORAGE_OPEN) === '1');
const position = ref(localStorage.getItem('agentPanelPosition') || 'right');
const panelWidth = ref(parseInt(localStorage.getItem('agentPanelWidth')) || 400);
const panelHeight = ref(parseInt(localStorage.getItem('agentPanelHeight')) || 350);

const conversations = ref([]);
const currentConversationId = ref(null);
const messages = ref([]);
const isStreaming = ref(false);
const pendingAttachments = ref([]);
const isUploading = ref(false);
let restoredOnce = false;
let watchersRegistered = false;
let lastProjectId = null;

let currentAbortController = null;

export function useAgent() {
  const route = useRoute();
  const { currentProject, restore: restoreProject } = useCurrentProject();

  const projectId = computed(() => {
    const raw = route.params?.id || currentProject.value?.id;
    return raw ? resolveSlug(raw) : undefined;
  });

  if (!watchersRegistered) {
    watchersRegistered = true;

    // Persist layout preferences on change
    watch(position, (v) => localStorage.setItem('agentPanelPosition', v));
    watch(panelWidth, (v) => localStorage.setItem('agentPanelWidth', String(v)));
    watch(panelHeight, (v) => localStorage.setItem('agentPanelHeight', String(v)));

    // Persist panel open state and active conversation across refreshes
    watch(isOpen, (v) => localStorage.setItem(STORAGE_OPEN, v ? '1' : '0'));
    watch(currentConversationId, (v) => {
      const pid = lastProjectId;
      if (!pid) return;
      if (v) {
        localStorage.setItem(STORAGE_CONV_PREFIX + pid, v);
      } else {
        localStorage.removeItem(STORAGE_CONV_PREFIX + pid);
      }
    });

    // Clear stale state when switching projects
    watch(projectId, (newId, oldId) => {
      if (newId !== oldId) {
        lastProjectId = newId;
        conversations.value = [];
        currentConversationId.value = null;
        messages.value = [];
        restoredOnce = false;
        if (isOpen.value && newId) {
          restoreOrLoad();
        }
      }
    });
  }

  async function restoreOrLoad() {
    if (restoredOnce) return;
    if (!projectId.value) {
      await restoreProject();
      if (!projectId.value) return;
    }
    restoredOnce = true;
    lastProjectId = projectId.value;
    await loadConversations();
    const savedId = localStorage.getItem(STORAGE_CONV_PREFIX + projectId.value);
    if (savedId && conversations.value.some((c) => c.id === savedId)) {
      await loadMessages(savedId);
    } else if (conversations.value.length > 0) {
      await loadMessages(conversations.value[0].id);
    }
  }

  function toggle() {
    isOpen.value = !isOpen.value;
  }

  function togglePosition() {
    position.value = position.value === 'right' ? 'bottom' : 'right';
  }

  function collapse() {
    isOpen.value = false;
  }

  function abortStream() {
    if (currentAbortController) {
      currentAbortController.abort();
      currentAbortController = null;
    }
  }

  async function loadConversations() {
    if (!projectId.value) return;
    try {
      const resp = await axios.get(`/api/projects/${projectId.value}/agent/conversations`);
      conversations.value = resp.data;
    } catch {
      conversations.value = [];
    }
  }

  async function createConversation(title) {
    if (!projectId.value) return null;
    try {
      const resp = await axios.post(`/api/projects/${projectId.value}/agent/conversations`, {
        title: title || null,
      });
      const conv = resp.data;
      conversations.value.unshift(conv);
      currentConversationId.value = conv.id;
      messages.value = [];
      return conv;
    } catch (err) {
      messages.value.push({
        id: crypto.randomUUID(),
        role: 'assistant',
        content: `Error creating conversation: ${err.response?.data?.error || err.message}`,
        created_at: new Date().toISOString(),
      });
      return null;
    }
  }

  async function deleteConversation(id) {
    if (!projectId.value) return;
    try {
      await axios.delete(`/api/projects/${projectId.value}/agent/conversations/${id}`);
      conversations.value = conversations.value.filter((c) => c.id !== id);
      if (currentConversationId.value === id) {
        currentConversationId.value = null;
        messages.value = [];
      }
    } catch (err) {
      messages.value.push({
        id: crypto.randomUUID(),
        role: 'assistant',
        content: `Error deleting conversation: ${err.response?.data?.error || err.message}`,
        created_at: new Date().toISOString(),
      });
    }
  }

  async function loadMessages(conversationId) {
    if (!projectId.value || !conversationId) return;
    currentConversationId.value = conversationId;
    try {
      const resp = await axios.get(
        `/api/projects/${projectId.value}/agent/conversations/${conversationId}/messages`
      );
      messages.value = resp.data;
    } catch {
      messages.value = [];
    }
  }

  function selectConversation(id) {
    loadMessages(id);
  }

  function buildPageContext() {
    const { pageSnapshot } = usePageContext();
    return {
      route: route.fullPath || null,
      entity_type: route.meta?.entityType || null,
      entity_id: route.params?.entityId || route.params?.id || null,
      snapshot: pageSnapshot.value || null,
    };
  }

  function processSSELine(line, state) {
    if (!line.startsWith('data: ')) return;
    const data = line.slice(6).trim();
    if (!data || data === '[DONE]') return;

    let event;
    try {
      event = JSON.parse(data);
    } catch {
      return;
    }

    switch (event.type) {
      case 'conversation_created':
        if (event.conversation_id) {
          currentConversationId.value = event.conversation_id;
        }
        break;

      case 'text_delta':
        if (!state.assistantMsg) {
          const raw = {
            id: crypto.randomUUID(),
            role: 'assistant',
            content: '',
            created_at: new Date().toISOString(),
          };
          messages.value.push(raw);
          // Re-read from array to grab the reactive proxy
          state.assistantMsg = messages.value[messages.value.length - 1];
        }
        state.assistantMsg.content += event.content || '';
        break;

      case 'tool_start': {
        const raw = {
          id: crypto.randomUUID(),
          role: 'tool_call',
          tool_name: event.name,
          tool_input: event.input,
          tool_output: null,
          tool_status: 'running',
          call_id: event.call_id,
          created_at: new Date().toISOString(),
        };
        messages.value.push(raw);
        // Re-read from array to grab the reactive proxy
        state.pendingToolCalls[event.call_id] = messages.value[messages.value.length - 1];
        break;
      }

      case 'tool_result':
        if (state.pendingToolCalls[event.call_id]) {
          state.pendingToolCalls[event.call_id].tool_output = event.output;
          state.pendingToolCalls[event.call_id].tool_status = 'done';
        }
        state.assistantMsg = null;
        break;

      case 'done':
        if (event.conversation_id) {
          currentConversationId.value = event.conversation_id;
        }
        break;

      case 'error':
        messages.value.push({
          id: crypto.randomUUID(),
          role: 'assistant',
          content: `Error: ${event.error}`,
          created_at: new Date().toISOString(),
        });
        break;
    }
  }

  async function uploadAttachment(file) {
    if (!projectId.value) return null;
    isUploading.value = true;
    try {
      const formData = new FormData();
      formData.append('file', file);
      const resp = await axios.post(
        `/api/projects/${projectId.value}/agent/attachments`,
        formData,
        { headers: { 'Content-Type': 'multipart/form-data' } }
      );
      const attachment = resp.data;
      pendingAttachments.value.push(attachment);
      return attachment;
    } catch (err) {
      messages.value.push({
        id: crypto.randomUUID(),
        role: 'assistant',
        content: `Failed to upload file: ${err.response?.data?.error || err.message}`,
        created_at: new Date().toISOString(),
      });
      return null;
    } finally {
      isUploading.value = false;
    }
  }

  function removePendingAttachment(id) {
    pendingAttachments.value = pendingAttachments.value.filter((a) => a.id !== id);
  }

  async function sendMessage(text) {
    if (!projectId.value || !text.trim() || isStreaming.value) return;

    const attachments = [...pendingAttachments.value];
    pendingAttachments.value = [];

    messages.value.push({
      id: crypto.randomUUID(),
      role: 'user',
      content: text,
      attachments: attachments.length > 0 ? attachments : undefined,
      created_at: new Date().toISOString(),
    });

    isStreaming.value = true;
    currentAbortController = new AbortController();

    try {
      const body = {
        conversation_id: currentConversationId.value || undefined,
        message: text,
        page_context: buildPageContext(),
        attachment_ids: attachments.length > 0 ? attachments.map((a) => a.id) : undefined,
      };

      const response = await fetch(resolveApiUrl(`/api/projects/${projectId.value}/agent/chat`), {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...getAuthHeaders(),
        },
        body: JSON.stringify(body),
        signal: currentAbortController.signal,
      });

      if (!response.ok) {
        let err;
        try { err = await response.text(); } catch { err = `HTTP ${response.status}`; }
        messages.value.push({
          id: crypto.randomUUID(),
          role: 'assistant',
          content: `Error: ${err}`,
          created_at: new Date().toISOString(),
        });
        return;
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      const state = { assistantMsg: null, pendingToolCalls: {} };

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        for (const line of lines) {
          processSSELine(line, state);
        }
      }

      // Flush remaining decoder buffer and process any trailing data
      buffer += decoder.decode();
      if (buffer.trim()) {
        for (const line of buffer.split('\n')) {
          processSSELine(line, state);
        }
      }
    } catch (err) {
      if (err.name === 'AbortError') return;
      messages.value.push({
        id: crypto.randomUUID(),
        role: 'assistant',
        content: `Connection error: ${err.message}`,
        created_at: new Date().toISOString(),
      });
    } finally {
      isStreaming.value = false;
      currentAbortController = null;
    }
  }

  function getAuthHeaders() {
    const cookies = document.cookie.split('; ');
    const tokenCookie = cookies.find((row) => row.trim().startsWith('token='));
    if (tokenCookie) {
      const tokenValue = tokenCookie.split('=').slice(1).join('=');
      const token = decodeURIComponent(tokenValue);
      if (token && token.trim()) {
        return { Authorization: `Bearer ${token}` };
      }
    }
    return {};
  }

  function startNewConversation() {
    currentConversationId.value = null;
    messages.value = [];
  }

  return {
    isOpen,
    position,
    panelWidth,
    panelHeight,
    conversations,
    currentConversationId,
    messages,
    isStreaming,
    isUploading,
    pendingAttachments,
    projectId,
    toggle,
    togglePosition,
    collapse,
    abortStream,
    loadConversations,
    createConversation,
    deleteConversation,
    loadMessages,
    selectConversation,
    uploadAttachment,
    removePendingAttachment,
    sendMessage,
    startNewConversation,
    restoreOrLoad,
  };
}
