<template>
  <!-- User message -->
  <div v-if="message.role === 'user'" class="flex justify-end">
    <div class="max-w-[85%]">
      <!-- Attachments -->
      <div v-if="message.attachments?.length" class="flex flex-wrap gap-1.5 mb-1.5 justify-end">
        <div
          v-for="att in message.attachments"
          :key="att.id"
          class="flex items-center gap-1 px-2 py-1 bg-primary-100 dark:bg-primary-800/40 rounded text-xs text-gray-600 dark:text-gray-300"
        >
          <svg v-if="att.content_type?.startsWith('image/')" class="w-3 h-3 text-blue-500 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M2.25 15.75l5.159-5.159a2.25 2.25 0 013.182 0l5.159 5.159m-1.5-1.5l1.409-1.409a2.25 2.25 0 013.182 0l2.909 2.909M3.75 21h16.5A2.25 2.25 0 0022.5 18.75V5.25A2.25 2.25 0 0020.25 3H3.75A2.25 2.25 0 001.5 5.25v13.5A2.25 2.25 0 003.75 21z" />
          </svg>
          <svg v-else class="w-3 h-3 text-gray-500 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19.5 14.25v-2.625a3.375 3.375 0 00-3.375-3.375h-1.5A1.125 1.125 0 0113.5 7.125v-1.5a3.375 3.375 0 00-3.375-3.375H8.25m2.25 0H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 00-9-9z" />
          </svg>
          <span class="truncate max-w-[120px]">{{ att.filename }}</span>
        </div>
      </div>
      <div class="px-3 py-2 rounded-lg bg-primary-50 dark:bg-primary-900/30 text-sm text-gray-900 dark:text-gray-100 whitespace-pre-wrap">
        {{ message.content }}
      </div>
    </div>
  </div>

  <!-- Secret deposit card (from live stream) -->
  <div v-else-if="message.role === 'tool_call' && message.tool_name === 'create_secret_slot' && message.tool_output" class="flex justify-start">
    <SecretDepositCard
      :slot-id="message.tool_output.slot_id"
      :project-id="projectId"
      :purpose="message.tool_output.purpose || message.tool_input?.purpose || ''"
      :provider="message.tool_output.provider || message.tool_input?.provider || null"
      :expires-at="message.tool_output.expires_at || ''"
    />
  </div>

  <!-- Tool call -->
  <div v-else-if="message.role === 'tool_call'" class="flex justify-start">
    <AgentToolCall
      :name="message.tool_name"
      :input="message.tool_input"
      :output="message.tool_output"
      :status="message.tool_status"
    />
  </div>

  <!-- Assistant message -->
  <div v-else-if="message.role === 'assistant'" class="flex justify-start">
    <div
      class="max-w-[85%] px-3 py-2 rounded-lg bg-gray-50 dark:bg-gray-800 text-sm text-gray-900 dark:text-gray-100 agent-markdown"
      v-html="renderedContent"
    ></div>
  </div>

  <!-- Secret deposit card (from history reload) -->
  <div v-else-if="message.role === 'tool' && message.tool_name === 'create_secret_slot'" class="flex justify-start">
    <SecretDepositCard
      :slot-id="parsedToolOutput(message.content)?.slot_id || ''"
      :project-id="projectId"
      :purpose="parsedToolOutput(message.content)?.purpose || ''"
      :provider="parsedToolOutput(message.content)?.provider || null"
      :deposit-url="''"
      :expires-at="''"
      initial-state="saved"
    />
  </div>

  <!-- Tool result (from history reload) -->
  <div v-else-if="message.role === 'tool'" class="flex justify-start">
    <AgentToolCall
      :name="message.tool_name"
      :input="null"
      :output="parseToolOutput(message.content)"
      status="done"
    />
  </div>
</template>

<script setup>
import { computed } from 'vue';
import { marked } from 'marked';
import DOMPurify from 'dompurify';
import AgentToolCall from '@/components/AgentToolCall.vue';
import SecretDepositCard from '@/components/SecretDepositCard.vue';

const props = defineProps({
  message: {
    type: Object,
    required: true,
  },
  projectId: {
    type: String,
    default: '',
  },
});

marked.setOptions({
  breaks: true,
  gfm: true,
});

const renderedContent = computed(() => {
  const content = props.message.content;
  if (!content) return '';
  try {
    const html = marked.parse(content);
    return DOMPurify.sanitize(html);
  } catch {
    return DOMPurify.sanitize(content);
  }
});

function parseToolOutput(content) {
  if (!content) return null;
  try {
    return JSON.parse(content);
  } catch {
    return content;
  }
}

function parsedToolOutput(content) {
  if (!content) return {};
  try {
    return JSON.parse(content);
  } catch {
    return {};
  }
}
</script>

<style>
.agent-markdown p { margin-bottom: 0.5em; }
.agent-markdown p:last-child { margin-bottom: 0; }
.agent-markdown pre { background: #1f2937; color: #e5e7eb; padding: 0.75em; border-radius: 0.375rem; overflow-x: auto; margin: 0.5em 0; font-size: 0.8125rem; }
.agent-markdown code { font-size: 0.8125rem; }
.agent-markdown :not(pre) > code { background: #e5e7eb; padding: 0.125em 0.25em; border-radius: 0.25rem; }
.dark .agent-markdown :not(pre) > code { background: #374151; }
.agent-markdown ul, .agent-markdown ol { padding-left: 1.5em; margin: 0.25em 0; }
.agent-markdown li { margin-bottom: 0.125em; }
.agent-markdown h1, .agent-markdown h2, .agent-markdown h3 { font-weight: 600; margin: 0.5em 0 0.25em; }
.agent-markdown a { color: #4f46e5; text-decoration: underline; }
.agent-markdown blockquote { border-left: 3px solid #d1d5db; padding-left: 0.75em; margin: 0.5em 0; color: #6b7280; }
</style>
