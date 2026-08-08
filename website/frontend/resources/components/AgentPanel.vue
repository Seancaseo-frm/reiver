<template>
  <!-- Collapsed: floating button -->
  <button
    v-if="!isOpen"
    @click="toggle"
    class="fixed bottom-4 right-4 z-50 w-12 h-12 rounded-full bg-primary-600 text-white shadow-lg hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-primary-500 focus:ring-offset-2 transition-all flex items-center justify-center"
    aria-label="Open MooDeng"
  >
    <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.813 15.904L9 18.75l-.813-2.846a4.5 4.5 0 00-3.09-3.09L2.25 12l2.846-.813a4.5 4.5 0 003.09-3.09L9 5.25l.813 2.846a4.5 4.5 0 003.09 3.09L15.75 12l-2.846.813a4.5 4.5 0 00-3.09 3.09zM18.259 8.715L18 9.75l-.259-1.035a3.375 3.375 0 00-2.455-2.456L14.25 6l1.036-.259a3.375 3.375 0 002.455-2.456L18 2.25l.259 1.035a3.375 3.375 0 002.455 2.456L21.75 6l-1.036.259a3.375 3.375 0 00-2.455 2.456z" />
    </svg>
  </button>

  <!-- Expanded panel -->
  <Transition :name="position === 'right' ? 'slide-right' : 'slide-bottom'">
    <div
      v-if="isOpen"
      :class="panelClasses"
      :style="panelStyle"
      class="bg-white dark:bg-gray-900 border-gray-200 dark:border-gray-700 shadow-xl flex flex-col z-40"
      role="complementary"
      aria-label="MooDeng"
      @keydown.escape="collapse"
      tabindex="-1"
    >
      <!-- Resize handle -->
      <div
        :class="resizeHandleClasses"
        @mousedown="startResize"
      ></div>

      <!-- Header -->
      <div class="flex items-center justify-between px-4 py-3 border-b border-gray-200 dark:border-gray-700 flex-shrink-0">
        <div class="flex items-center gap-2">
          <svg class="w-5 h-5 text-primary-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.813 15.904L9 18.75l-.813-2.846a4.5 4.5 0 00-3.09-3.09L2.25 12l2.846-.813a4.5 4.5 0 003.09-3.09L9 5.25l.813 2.846a4.5 4.5 0 003.09 3.09L15.75 12l-2.846.813a4.5 4.5 0 00-3.09 3.09z" />
          </svg>
          <h3 class="text-sm font-semibold text-gray-900 dark:text-gray-100">MooDeng</h3>
        </div>
        <div class="flex items-center gap-1">
          <!-- Conversation history -->
          <button
            @click="showConversationList = !showConversationList"
            class="p-1.5 rounded text-gray-500 hover:bg-gray-100 dark:hover:bg-gray-800"
            :class="{ 'bg-gray-100 dark:bg-gray-800': showConversationList }"
            title="Conversation history"
          >
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M20.25 8.511c.884.284 1.5 1.128 1.5 2.097v4.286c0 1.136-.847 2.1-1.98 2.193-.34.027-.68.052-1.02.072v3.091l-3-3c-1.354 0-2.694-.055-4.02-.163a2.115 2.115 0 01-.825-.242m9.345-8.334a2.126 2.126 0 00-.476-.095 48.64 48.64 0 00-8.048 0c-1.131.094-1.976 1.057-1.976 2.192v4.286c0 .837.46 1.58 1.155 1.951m9.345-8.334V6.637c0-1.621-1.152-3.026-2.76-3.235A48.455 48.455 0 0011.25 3c-2.115 0-4.198.137-6.24.402-1.608.209-2.76 1.614-2.76 3.235v6.226c0 1.621 1.152 3.026 2.76 3.235.577.075 1.157.14 1.74.194V21l4.155-4.155" />
            </svg>
          </button>
          <!-- New conversation -->
          <button
            @click="startNewConversation"
            class="p-1.5 rounded text-gray-500 hover:bg-gray-100 dark:hover:bg-gray-800"
            title="New conversation"
          >
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4.5v15m7.5-7.5h-15" />
            </svg>
          </button>
          <!-- Toggle position -->
          <button
            @click="togglePosition"
            class="p-1.5 rounded text-gray-500 hover:bg-gray-100 dark:hover:bg-gray-800"
            :title="position === 'right' ? 'Move to bottom' : 'Move to right'"
          >
            <svg v-if="position === 'right'" class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 16.5v2.25A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75V16.5M3 16.5V5.25A2.25 2.25 0 015.25 3h13.5A2.25 2.25 0 0121 5.25V16.5" />
            </svg>
            <svg v-else class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15.75 3v17.25M3 5.25A2.25 2.25 0 015.25 3h13.5A2.25 2.25 0 0121 5.25v13.5A2.25 2.25 0 0118.75 21H5.25A2.25 2.25 0 013 18.75V5.25z" />
            </svg>
          </button>
          <!-- Minimize -->
          <button
            @click="collapse"
            class="p-1.5 rounded text-gray-500 hover:bg-gray-100 dark:hover:bg-gray-800"
            title="Minimize"
          >
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19.5 12h-15" />
            </svg>
          </button>
        </div>
      </div>

      <!-- Conversation list (collapsible) -->
      <div v-if="showConversationList" class="border-b border-gray-200 dark:border-gray-700 max-h-40 overflow-y-auto flex-shrink-0">
        <div
          v-for="conv in conversations"
          :key="conv.id"
          role="button"
          tabindex="0"
          @click="selectConversation(conv.id); showConversationList = false"
          @keydown.enter="selectConversation(conv.id); showConversationList = false"
          class="w-full text-left px-4 py-2 text-sm hover:bg-gray-50 dark:hover:bg-gray-800 flex items-center justify-between group cursor-pointer"
          :class="{ 'bg-primary-50 dark:bg-primary-900/20': conv.id === currentConversationId }"
        >
          <span class="truncate text-gray-700 dark:text-gray-300">{{ conv.title || 'New conversation' }}</span>
          <button
            @click.stop="deleteConversation(conv.id)"
            class="opacity-0 group-hover:opacity-100 p-1 rounded text-gray-400 hover:text-red-500"
            aria-label="Delete conversation"
          >
            <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>
      </div>

      <!-- Messages -->
      <div
        ref="messagesContainer"
        class="flex-1 overflow-y-auto p-4 space-y-3"
        aria-live="polite"
      >
        <div v-if="messages.length === 0" class="flex items-center justify-center h-full">
          <p class="text-sm text-gray-400 dark:text-gray-500">Ask MooDeng anything about your project.</p>
        </div>
        <template v-for="entry in groupedMessages" :key="entry.type === 'tool_group' ? entry.id : entry.message.id">
          <AgentToolGroup
            v-if="entry.type === 'tool_group'"
            :items="entry.items"
            :project-id="projectId"
          />
          <AgentMessage
            v-else
            :message="entry.message"
            :project-id="projectId"
          />
        </template>
        <div v-if="isStreaming" class="flex items-center gap-2 text-sm text-gray-400">
          <div class="agent-spinner"></div>
          <span>MooDeng is thinking...</span>
        </div>
      </div>

      <!-- Input area -->
      <div
        class="border-t border-gray-200 dark:border-gray-700 p-3 flex-shrink-0"
        @drop.prevent="handleDrop"
        @dragover.prevent="handleDragOver"
        @dragleave="handleDragLeave"
        :class="{ 'ring-2 ring-primary-400 ring-inset bg-primary-50 dark:bg-primary-900/20': isDragOver }"
      >
        <!-- Pending attachments -->
        <div v-if="pendingAttachments.length" class="flex flex-wrap gap-1.5 mb-2">
          <div
            v-for="att in pendingAttachments"
            :key="att.id"
            class="flex items-center gap-1 px-2 py-1 bg-gray-100 dark:bg-gray-800 rounded text-xs text-gray-700 dark:text-gray-300"
          >
            <svg v-if="att.content_type.startsWith('image/')" class="w-3 h-3 text-blue-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M2.25 15.75l5.159-5.159a2.25 2.25 0 013.182 0l5.159 5.159m-1.5-1.5l1.409-1.409a2.25 2.25 0 013.182 0l2.909 2.909M3.75 21h16.5A2.25 2.25 0 0022.5 18.75V5.25A2.25 2.25 0 0020.25 3H3.75A2.25 2.25 0 001.5 5.25v13.5A2.25 2.25 0 003.75 21z" />
            </svg>
            <svg v-else class="w-3 h-3 text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19.5 14.25v-2.625a3.375 3.375 0 00-3.375-3.375h-1.5A1.125 1.125 0 0113.5 7.125v-1.5a3.375 3.375 0 00-3.375-3.375H8.25m2.25 0H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 00-9-9z" />
            </svg>
            <span class="truncate max-w-[120px]">{{ att.filename }}</span>
            <button @click="removePendingAttachment(att.id)" class="text-gray-400 hover:text-red-500">
              <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>
        </div>
        <div class="flex gap-2">
          <!-- File attach button -->
          <button
            @click="fileInputEl?.click()"
            :disabled="isStreaming || isUploading"
            class="p-2 rounded text-gray-500 hover:bg-gray-100 dark:hover:bg-gray-800 disabled:opacity-50 flex-shrink-0"
            title="Attach file"
          >
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M18.375 12.739l-7.693 7.693a4.5 4.5 0 01-6.364-6.364l10.94-10.94A3 3 0 1119.5 7.372L8.552 18.32m.009-.01l-.01.01m5.699-9.941l-7.81 7.81a1.5 1.5 0 002.112 2.13" />
            </svg>
          </button>
          <input
            ref="fileInputEl"
            type="file"
            multiple
            class="hidden"
            @change="handleFileInput"
          />
          <textarea
            ref="inputEl"
            v-model="inputText"
            @keydown.enter.exact.prevent="handleSend"
            @input="autoResizeTextarea"
            :disabled="isStreaming"
            rows="1"
            class="flex-1 resize-none px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 text-sm placeholder-gray-400 focus:outline-none focus:ring-1 focus:ring-primary-500 focus:border-primary-500 disabled:opacity-50"
            :placeholder="isDragOver ? 'Drop files here...' : 'Message MooDeng...'"
          ></textarea>
          <button
            @click="handleSend"
            :disabled="isStreaming || !inputText.trim()"
            class="px-3 py-2 bg-primary-600 text-white rounded-lg hover:bg-primary-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors flex-shrink-0"
            aria-label="Send message"
          >
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 12L3.269 3.126A59.768 59.768 0 0121.485 12 59.77 59.77 0 013.27 20.876L5.999 12zm0 0h7.5" />
            </svg>
          </button>
        </div>
      </div>
    </div>
  </Transition>
</template>

<script setup>
import { ref, computed, onMounted, nextTick, watch } from 'vue';
import { useAgent } from '@/composables/useAgent';
import AgentMessage from '@/components/AgentMessage.vue';
import AgentToolGroup from '@/components/AgentToolGroup.vue';

const {
  isOpen,
  position,
  panelWidth,
  panelHeight,
  conversations,
  currentConversationId,
  messages,
  isStreaming,
  projectId,
  toggle,
  togglePosition,
  collapse,
  deleteConversation,
  selectConversation,
  uploadAttachment,
  removePendingAttachment,
  pendingAttachments,
  isUploading,
  sendMessage,
  startNewConversation,
  restoreOrLoad,
} = useAgent();

const inputText = ref('');
const messagesContainer = ref(null);
const inputEl = ref(null);
const fileInputEl = ref(null);
const showConversationList = ref(false);
const isResizing = ref(false);
const isDragOver = ref(false);

function handleFiles(files) {
  for (const file of files) {
    uploadAttachment(file);
  }
}

function handleDrop(e) {
  isDragOver.value = false;
  if (e.dataTransfer?.files?.length) {
    handleFiles(e.dataTransfer.files);
  }
}

function handleDragOver(e) {
  e.preventDefault();
  isDragOver.value = true;
}

function handleDragLeave() {
  isDragOver.value = false;
}

function handleFileInput(e) {
  if (e.target.files?.length) {
    handleFiles(e.target.files);
    e.target.value = '';
  }
}

function isSecretSlot(msg) {
  return (msg.role === 'tool_call' || msg.role === 'tool') && msg.tool_name === 'create_secret_slot';
}

function isToolRelated(msg) {
  if ((msg.role === 'tool_call' || msg.role === 'tool') && !isSecretSlot(msg)) return true;
  if (msg.role === 'assistant' && msg.tool_calls) return true;
  return false;
}

function hasToolAhead(msgs, fromIndex) {
  for (let j = fromIndex; j < msgs.length; j++) {
    if (msgs[j].role === 'user') return false;
    if (isToolRelated(msgs[j])) return true;
  }
  return false;
}

const groupedMessages = computed(() => {
  const msgs = messages.value;
  const result = [];
  let i = 0;

  while (i < msgs.length) {
    const msg = msgs[i];

    if (msg.role === 'user' || isSecretSlot(msg)) {
      result.push({ type: 'message', message: msg });
      i++;
      continue;
    }

    const canStartGroup = isToolRelated(msg)
      || (msg.role === 'assistant' && hasToolAhead(msgs, i + 1));

    if (canStartGroup) {
      const items = [];
      while (i < msgs.length) {
        const m = msgs[i];
        if (m.role === 'user' || isSecretSlot(m)) break;

        if (isToolRelated(m)) {
          items.push(m);
          i++;
        } else if (m.role === 'assistant') {
          if (hasToolAhead(msgs, i + 1)) {
            items.push(m);
            i++;
          } else {
            break;
          }
        } else {
          break;
        }
      }

      if (items.some(m => m.role === 'tool_call' || m.role === 'tool')) {
        result.push({ type: 'tool_group', id: 'grp-' + items[0].id, items });
      } else {
        for (const m of items) {
          result.push({ type: 'message', message: m });
        }
      }
      continue;
    }

    result.push({ type: 'message', message: msg });
    i++;
  }

  return result;
});

const panelClasses = computed(() => {
  if (position.value === 'right') {
    return 'fixed top-[48px] right-0 bottom-0 border-l';
  }
  return 'fixed left-60 max-lg:left-0 right-0 bottom-0 border-t';
});

const panelStyle = computed(() => {
  if (position.value === 'right') {
    return { width: `${panelWidth.value}px` };
  }
  return { height: `${panelHeight.value}px` };
});

const resizeHandleClasses = computed(() => {
  if (position.value === 'right') {
    return 'absolute left-0 top-0 bottom-0 w-1 cursor-col-resize hover:bg-primary-400 active:bg-primary-500 transition-colors';
  }
  return 'absolute left-0 right-0 top-0 h-1 cursor-row-resize hover:bg-primary-400 active:bg-primary-500 transition-colors';
});

function startResize(e) {
  e.preventDefault();
  isResizing.value = true;
  const startX = e.clientX;
  const startY = e.clientY;
  const startWidth = panelWidth.value;
  const startHeight = panelHeight.value;

  document.body.style.userSelect = 'none';

  function onMouseMove(ev) {
    if (position.value === 'right') {
      const delta = startX - ev.clientX;
      const maxWidth = Math.min(window.innerWidth * 0.5, window.innerWidth - 500);
      const newWidth = Math.min(Math.max(startWidth + delta, 300), maxWidth);
      panelWidth.value = newWidth;
    } else {
      const delta = startY - ev.clientY;
      const newHeight = Math.min(Math.max(startHeight + delta, 200), window.innerHeight * 0.6);
      panelHeight.value = newHeight;
    }
  }

  function onMouseUp() {
    isResizing.value = false;
    document.body.style.userSelect = '';
    document.removeEventListener('mousemove', onMouseMove);
    document.removeEventListener('mouseup', onMouseUp);
  }

  document.addEventListener('mousemove', onMouseMove);
  document.addEventListener('mouseup', onMouseUp);
}

function autoResizeTextarea() {
  const el = inputEl.value;
  if (!el) return;
  el.style.height = 'auto';
  el.style.height = Math.min(el.scrollHeight, 120) + 'px';
}

function handleSend() {
  const text = inputText.value.trim();
  if (!text || isStreaming.value) return;
  inputText.value = '';
  if (inputEl.value) {
    inputEl.value.style.height = 'auto';
  }
  sendMessage(text);
}

function scrollToBottom() {
  nextTick(() => {
    if (messagesContainer.value) {
      messagesContainer.value.scrollTop = messagesContainer.value.scrollHeight;
    }
  });
}

watch(messages, scrollToBottom, { deep: true });

watch(isOpen, (open) => {
  if (open) {
    restoreOrLoad();
    nextTick(() => inputEl.value?.focus());
  }
});

onMounted(() => {
  if (isOpen.value) {
    restoreOrLoad();
  }
});
</script>

<style scoped>
.slide-right-enter-active,
.slide-right-leave-active {
  transition: transform 0.2s ease;
}
.slide-right-enter-from,
.slide-right-leave-to {
  transform: translateX(100%);
}

.slide-bottom-enter-active,
.slide-bottom-leave-active {
  transition: transform 0.2s ease;
}
.slide-bottom-enter-from,
.slide-bottom-leave-to {
  transform: translateY(100%);
}

.agent-spinner {
  width: 16px;
  height: 16px;
  border: 2px solid #d1d5db;
  border-top-color: #6366f1;
  border-radius: 50%;
  animation: agent-spin 0.6s linear infinite;
}

@keyframes agent-spin {
  to { transform: rotate(360deg); }
}
</style>
