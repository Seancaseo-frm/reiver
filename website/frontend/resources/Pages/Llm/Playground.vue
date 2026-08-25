<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-[1200px] mx-auto px-8 py-6 flex flex-col h-[calc(100vh-4rem)] min-h-0">
      <!-- Header + compact settings bar -->
      <div class="shrink-0 border-b border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900/50">
        <div class="mb-6 flex flex-wrap items-center justify-between gap-3">
          <div>
            <h1 class="text-2xl font-semibold text-gray-900">LLM Playground</h1>
            <p class="text-sm text-gray-500 dark:text-gray-400 mt-0.5">Test prompts and compare responses across models</p>
          </div>
          <button
            type="button"
            @click="showStatsDrawer = true"
            class="inline-flex items-center gap-2 px-3 py-2 text-sm font-medium text-gray-700 dark:text-gray-300 bg-gray-100 dark:bg-gray-800 rounded-lg hover:bg-gray-200 dark:hover:bg-gray-700"
          >
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
            </svg>
            Stats
          </button>
        </div>
        <!-- Compact settings bar -->
        <div class="px-4 pb-3 flex flex-wrap items-center gap-3 sm:gap-4">
          <div class="min-w-[180px] max-w-[220px]">
            <select
              v-model="selectedModel"
              class="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 text-sm"
            >
              <optgroup label="Smart Routing">
                <option value="auto">Auto (Fallback Chain)</option>
              </optgroup>
              <optgroup v-for="provider in modelCatalog" :key="provider.id" :label="provider.name">
                <option v-for="m in provider.models" :key="m.id" :value="m.id">{{ m.name }}</option>
              </optgroup>
            </select>
            <p v-if="isAutoMode" class="mt-1 text-xs text-indigo-600 dark:text-indigo-400">Uses fallback chain</p>
          </div>
          <div v-if="isAutoMode" class="flex items-center gap-2" title="Select an explicit model to control introspection">
            <span class="text-xs text-gray-500 dark:text-gray-400">Thinking</span>
            <span class="text-xs font-medium text-gray-600 dark:text-gray-300">model default</span>
          </div>
          <div v-else-if="hasDefaultAdaptiveThinking" class="flex items-center gap-2" title="This Claude model manages adaptive thinking itself">
            <span class="text-xs text-gray-500 dark:text-gray-400">Thinking</span>
            <span class="text-xs font-medium text-indigo-600 dark:text-indigo-400">adaptive</span>
          </div>
          <div v-else class="flex items-center gap-2">
            <span class="text-xs text-gray-500 dark:text-gray-400">Introspection</span>
            <label class="relative inline-flex items-center cursor-pointer">
              <input v-model="introspectionEnabled" type="checkbox" class="sr-only peer" />
              <div class="w-8 h-4 bg-gray-200 dark:bg-gray-700 rounded-full peer peer-checked:bg-primary-600 after:content-[''] after:absolute after:top-0.5 after:start-0.5 after:bg-white after:rounded-full after:h-3 after:w-3 after:transition-all peer-checked:after:translate-x-4"></div>
            </label>
          </div>
          <div class="flex items-center gap-2" :title="!isAutoMode ? 'Evaluation requires Auto mode' : ''">
            <span class="text-xs text-gray-500 dark:text-gray-400" :class="{ 'opacity-50': !isAutoMode }">Evaluate</span>
            <label class="relative inline-flex items-center cursor-pointer" :class="{ 'opacity-50 pointer-events-none': !isAutoMode }">
              <input v-model="autoEvaluate" type="checkbox" class="sr-only peer" :disabled="!isAutoMode" />
              <div class="w-8 h-4 bg-gray-200 dark:bg-gray-700 rounded-full peer peer-checked:bg-brand-600 after:content-[''] after:absolute after:top-0.5 after:start-0.5 after:bg-white after:rounded-full after:h-3 after:w-3 after:transition-all peer-checked:after:translate-x-4"></div>
            </label>
          </div>
          <div v-if="hasProviderManagedSampling" class="flex items-center gap-2" title="Anthropic rejects non-default temperature and top-p values for this model">
            <span class="text-xs text-gray-500 dark:text-gray-400">Sampling</span>
            <span class="text-xs font-medium text-indigo-600 dark:text-indigo-400">provider default</span>
          </div>
          <div v-else class="flex items-center gap-2">
            <label class="text-xs text-gray-500 dark:text-gray-400">Temp</label>
            <input
              v-model.number="temperature"
              type="number"
              min="0"
              max="1"
              step="0.1"
              class="w-14 px-2 py-1.5 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 text-sm"
            />
          </div>
          <div class="flex items-center gap-2">
            <label class="text-xs text-gray-500 dark:text-gray-400">Max tokens</label>
            <input
              v-model.number="maxTokens"
              type="number"
              min="1"
              max="100000"
              class="w-20 px-2 py-1.5 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 text-sm"
            />
          </div>
          <button
            type="button"
            @click="showSystemPrompt = !showSystemPrompt"
            class="text-sm text-primary-600 dark:text-primary-400 hover:underline"
          >
            {{ showSystemPrompt ? 'Hide' : 'Show' }} system prompt
          </button>
        </div>
        <!-- Collapsible system prompt -->
        <div v-show="showSystemPrompt" class="px-4 pb-3 pt-0 border-t border-gray-100 dark:border-gray-800">
          <div class="flex items-center justify-between mb-2">
            <span class="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase">System prompt</span>
            <div class="flex items-center gap-2">
              <button
                v-if="isManagedPromptMode"
                type="button"
                @click="clearManagedPrompt"
                class="inline-flex items-center gap-1 px-2 py-1 text-xs font-medium rounded text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-900/20 hover:bg-red-100 dark:hover:bg-red-900/40"
              >
                <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
                </svg>
                Clear prompt
              </button>
              <button
                type="button"
                @click="openLoadPromptModal"
                class="inline-flex items-center gap-1.5 px-2 py-1 text-xs font-medium rounded text-primary-600 dark:text-primary-400 bg-primary-50 dark:bg-primary-900/30 hover:bg-primary-100 dark:hover:bg-primary-900/50"
              >
                <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4-4m0 0L8 8m4-4v12" />
                </svg>
                Load from Prompt Hub
              </button>
            </div>
          </div>
          <!-- Managed prompt badge -->
          <div v-if="isManagedPromptMode" class="mb-2 flex items-center gap-2">
            <span class="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium bg-indigo-100 dark:bg-indigo-900/30 text-indigo-700 dark:text-indigo-300">
              <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101" />
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10.172 13.828a4 4 0 015.656 0l4-4a4 4 0 00-5.656-5.656l-1.102 1.101" />
              </svg>
              {{ activePromptConfigLabel }}
            </span>
            <span class="text-xs text-gray-500 dark:text-gray-400">Server-side resolution enabled</span>
          </div>
          <textarea
            v-model="systemPrompt"
            placeholder="You are a helpful assistant..."
            rows="2"
            :readonly="isManagedPromptMode"
            :class="[
              'w-full px-3 py-2 border rounded-lg text-sm resize-none',
              isManagedPromptMode
                ? 'border-indigo-200 dark:border-indigo-800 bg-indigo-50/50 dark:bg-indigo-900/10 text-gray-600 dark:text-gray-400 cursor-default'
                : 'border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100'
            ]"
          ></textarea>
          <p v-if="isManagedPromptMode" class="mt-1 text-xs text-gray-500 dark:text-gray-400">
            Template shown for reference. Variables are resolved server-side.
          </p>
        </div>
        <!-- Prompt variable inputs (managed mode only) -->
        <div v-if="isManagedPromptMode && promptVariableDefinitions.length > 0" class="px-4 pb-3 border-t border-gray-100 dark:border-gray-800">
          <p class="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase mt-2 mb-2">Template variables</p>
          <div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
            <div v-for="def in promptVariableDefinitions" :key="def.name" class="flex flex-col gap-1">
              <label class="text-xs font-medium text-gray-700 dark:text-gray-300">
                {{ def.name }}
                <span v-if="def.required" class="text-red-500">*</span>
                <span v-if="def.description" class="font-normal text-gray-400 dark:text-gray-500 ml-1">{{ def.description }}</span>
              </label>
              <select
                v-if="def.type === 'enum' || def.var_type === 'enum'"
                v-model="promptVariableValues[def.name]"
                class="px-2.5 py-1.5 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 text-sm"
              >
                <option value="">-- select --</option>
                <option v-for="val in (def.values || [])" :key="val" :value="val">{{ val }}</option>
              </select>
              <input
                v-else-if="def.type === 'number' || def.var_type === 'number'"
                v-model="promptVariableValues[def.name]"
                type="number"
                :min="def.min"
                :max="def.max"
                :placeholder="def.default != null ? String(def.default) : def.name"
                class="px-2.5 py-1.5 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 text-sm"
              />
              <select
                v-else-if="def.type === 'boolean' || def.var_type === 'boolean'"
                v-model="promptVariableValues[def.name]"
                class="px-2.5 py-1.5 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 text-sm"
              >
                <option value="">-- select --</option>
                <option value="true">true</option>
                <option value="false">false</option>
              </select>
              <input
                v-else
                v-model="promptVariableValues[def.name]"
                type="text"
                :maxlength="def.max_chars || undefined"
                :placeholder="def.default != null ? String(def.default) : def.name"
                class="px-2.5 py-1.5 border border-gray-300 dark:border-gray-600 rounded-md bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 text-sm"
              />
            </div>
          </div>
        </div>
      </div>

      <!-- Messages: scrollable, full-bleed, bubbles -->
      <div class="flex-1 min-h-0 overflow-y-auto px-4 py-4">
        <div class="max-w-3xl mx-auto space-y-6">
          <!-- Empty state -->
          <div v-if="messages.length === 0 && !streaming" class="flex flex-col items-center justify-center py-16 text-center">
            <p class="text-gray-500 dark:text-gray-400">Send a message to get started.</p>
            <p class="text-sm text-gray-400 dark:text-gray-500 mt-1">Press Enter to send</p>
          </div>

          <!-- Message history -->
          <div
            v-for="(message, index) in messages"
            :key="index"
            :class="['flex gap-3', message.role === 'user' ? 'flex-row-reverse' : '']"
          >
            <div
              class="w-8 h-8 rounded-full flex items-center justify-center flex-shrink-0"
              :class="message.role === 'user' ? 'bg-primary-600' : 'bg-gray-200 dark:bg-gray-600'"
            >
              <svg v-if="message.role === 'user'" class="w-4 h-4 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z" />
              </svg>
              <svg v-else class="w-4 h-4 text-gray-600 dark:text-gray-300" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
              </svg>
            </div>
            <div :class="['flex-1 min-w-0 max-w-[85%]', message.role === 'user' ? 'flex flex-col items-end' : '']">
              <div
                :class="[
                  'rounded-2xl px-4 py-3',
                  message.role === 'user'
                    ? 'bg-primary-600 text-white'
                    : 'bg-gray-100 dark:bg-gray-800 text-gray-900 dark:text-gray-100'
                ]"
              >
                <div v-if="message.role === 'assistant' && message.routedVia" class="flex items-center gap-1.5 mb-2">
                  <span class="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-indigo-100 dark:bg-indigo-900/40 text-indigo-700 dark:text-indigo-300">
                    {{ message.routedVia }}
                    <span v-if="message.fallbackUsed" class="text-orange-500 dark:text-orange-400">(fallback)</span>
                  </span>
                </div>
                <div v-if="message.thinking" class="mb-2 rounded-lg p-2 bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800">
                  <p class="text-xs font-medium text-yellow-700 dark:text-yellow-300 mb-1">Thinking</p>
                  <pre class="text-xs text-yellow-900 dark:text-yellow-200 whitespace-pre-wrap">{{ message.thinking }}</pre>
                </div>
                <pre :class="['text-sm whitespace-pre-wrap', message.role === 'user' ? 'text-white' : '']">{{ message.content }}</pre>
              </div>
              <!-- Evaluation scores -->
              <div
                v-if="message.evaluation"
                class="mt-2 rounded-lg border border-brand-200 dark:border-brand-800 bg-brand-50 dark:bg-brand-900/20 px-3 py-2"
              >
                <p class="text-xs font-medium text-brand-700 dark:text-brand-300 mb-1.5">Quality Evaluation</p>
                <div class="flex flex-wrap gap-3 text-xs">
                  <div v-for="dim in ['relevance', 'coherence', 'helpfulness']" :key="dim" class="flex items-center gap-1.5">
                    <span class="text-gray-500 dark:text-gray-400 capitalize">{{ dim }}</span>
                    <span class="font-mono font-semibold" :class="scoreColor(message.evaluation[dim])">{{ (message.evaluation[dim] * 100).toFixed(0) }}%</span>
                  </div>
                </div>
                <p v-if="message.evaluation.summary" class="mt-1 text-xs text-gray-600 dark:text-gray-400 italic">{{ message.evaluation.summary }}</p>
              </div>
            </div>
          </div>

          <!-- Streaming response -->
          <div v-if="streaming" class="flex gap-3">
            <div class="w-8 h-8 rounded-full bg-gray-200 dark:bg-gray-600 flex items-center justify-center flex-shrink-0">
              <svg class="w-4 h-4 text-gray-600 dark:text-gray-300 animate-pulse" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
              </svg>
            </div>
            <div class="flex-1 min-w-0 max-w-[85%]">
              <div class="rounded-2xl px-4 py-3 bg-gray-100 dark:bg-gray-800 text-gray-900 dark:text-gray-100">
                <div v-if="streamingThinking" class="mb-2 rounded-lg p-2 bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800">
                  <p class="text-xs font-medium text-yellow-700 dark:text-yellow-300 mb-1">Thinking...</p>
                  <pre class="text-xs text-yellow-900 dark:text-yellow-200 whitespace-pre-wrap">{{ streamingThinking }}</pre>
                </div>
                <pre class="text-sm whitespace-pre-wrap">{{ streamingContent }}<span class="animate-pulse">|</span></pre>
              </div>
            </div>
          </div>
        </div>
      </div>

      <!-- Sticky input bar -->
      <div class="shrink-0 px-4 py-3 bg-white dark:bg-gray-900/50 border-t border-gray-200 dark:border-gray-700">
        <div class="max-w-3xl mx-auto">
          <div class="flex items-end gap-2">
            <div class="flex-1 relative">
              <textarea
                v-model="userInput"
                @keydown="onInputKeydown"
                placeholder="Type your message... (Enter to send, Shift+Enter for new line)"
                rows="1"
                class="w-full px-4 py-3 pr-12 border border-gray-300 dark:border-gray-600 rounded-2xl bg-gray-50 dark:bg-gray-800 text-gray-900 dark:text-gray-100 resize-none focus:ring-2 focus:ring-primary-500 focus:border-transparent min-h-[48px] max-h-32"
                :disabled="streaming"
              />
              <BaseButton
                variant="primary"
                class="absolute right-2 bottom-2 rounded-xl px-3 py-1.5 text-sm"
                @click="sendMessage"
                :loading="streaming"
                :disabled="!userInput.trim() || streaming"
              >
                Send
              </BaseButton>
            </div>
          </div>
          <div class="flex items-center justify-between mt-2 px-1">
            <div class="flex items-center gap-4 text-xs text-gray-500 dark:text-gray-400">
              <button type="button" @click="clearConversation" class="hover:text-gray-700 dark:hover:text-gray-300">
                Clear
              </button>
              <button
                type="button"
                @click="saveAsTemplate"
                class="hover:text-gray-700 dark:hover:text-gray-300 disabled:opacity-50"
                :disabled="messages.length === 0"
              >
                Save as template
              </button>
            </div>
          </div>
        </div>
      </div>

      <!-- Load from Prompt Hub modal -->
      <div v-if="showLoadPromptModal" class="fixed inset-0 z-50 overflow-y-auto" role="dialog" aria-modal="true" aria-labelledby="load-prompt-title">
        <div class="flex items-center justify-center min-h-screen pt-4 px-4 pb-20 text-center sm:block sm:p-0">
          <div class="fixed inset-0 bg-gray-500 dark:bg-gray-900 bg-opacity-75 dark:bg-opacity-75 transition-opacity" @click="showLoadPromptModal = false"></div>
          <span class="hidden sm:inline-block sm:align-middle sm:h-screen">&#8203;</span>
          <div class="inline-block align-bottom bg-white dark:bg-gray-800 rounded-lg text-left overflow-hidden shadow-xl transform transition-all sm:my-8 sm:align-middle sm:max-w-lg sm:w-full sm:p-6">
            <h3 id="load-prompt-title" class="text-lg font-medium text-gray-900 dark:text-gray-100 mb-4">Load from Prompt Hub</h3>
            <input
              v-model="loadPromptSearch"
              type="text"
              placeholder="Search prompts by name or description..."
              class="w-full px-3 py-2 mb-4 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 placeholder-gray-500 dark:placeholder-gray-400"
              autofocus
            />
            <div v-if="loadPromptLoading" class="flex justify-center py-8">
              <div class="animate-spin rounded-full h-8 w-8 border-b-2 border-primary-600"></div>
            </div>
            <div v-else-if="filteredPromptConfigs.length === 0" class="py-6 text-center text-sm text-gray-500 dark:text-gray-400">
              {{ loadPromptConfigs.length === 0 ? 'No prompts in this project. Create one on the Prompts page.' : 'No prompts match your search.' }}
            </div>
            <ul v-else class="max-h-72 overflow-y-auto divide-y divide-gray-200 dark:divide-gray-700 rounded-lg border border-gray-200 dark:border-gray-600">
              <li
                v-for="config in filteredPromptConfigs"
                :key="config.id"
                class="flex items-center justify-between px-4 py-3 hover:bg-gray-50 dark:hover:bg-gray-700 cursor-pointer"
                :class="{ 'bg-primary-50 dark:bg-primary-900/20': loadPromptSelectingId === config.id }"
                @click="selectPromptConfig(config)"
              >
                <div class="flex-1 min-w-0">
                  <p class="text-sm font-medium text-gray-900 dark:text-gray-100 truncate">{{ config.name }}</p>
                  <p class="text-xs text-gray-500 dark:text-gray-400 truncate">{{ config.description || 'No description' }}</p>
                </div>
                <div v-if="loadPromptSelectingId === config.id" class="ml-2 flex-shrink-0">
                  <div class="animate-spin rounded-full h-4 w-4 border-2 border-primary-600 border-t-transparent"></div>
                </div>
                <svg v-else class="ml-2 w-5 h-5 text-gray-400 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
                </svg>
              </li>
            </ul>
            <div class="mt-4 flex justify-end">
              <button
                type="button"
                @click="showLoadPromptModal = false"
                class="px-4 py-2 text-sm font-medium text-gray-700 dark:text-gray-300 bg-white dark:bg-gray-700 border border-gray-300 dark:border-gray-600 rounded-md hover:bg-gray-50 dark:hover:bg-gray-600"
              >
                Cancel
              </button>
            </div>
          </div>
        </div>
      </div>

      <!-- Stats drawer (slide-over) -->
      <div
        v-if="showStatsDrawer"
        class="fixed inset-0 z-40 overflow-hidden"
        aria-modal="true"
      >
        <div class="absolute inset-0 bg-gray-500 dark:bg-gray-900 bg-opacity-75 dark:bg-opacity-75 transition-opacity" @click="showStatsDrawer = false" />
        <div class="fixed inset-y-0 right-0 flex max-w-full pl-10">
          <div class="w-80 max-w-full flex flex-col bg-white dark:bg-gray-800 shadow-xl">
            <div class="flex items-center justify-between px-4 py-3 border-b border-gray-200 dark:border-gray-700">
              <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">Stats &amp; actions</h2>
              <button type="button" @click="showStatsDrawer = false" class="p-2 text-gray-500 hover:text-gray-700 dark:hover:text-gray-300 rounded-lg">
                <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>
            <div class="flex-1 overflow-y-auto p-4 space-y-4">
              <BaseCard>
                <template #header>
                  <h3 class="text-sm font-medium text-gray-900 dark:text-gray-100">Token usage</h3>
                </template>
                <div class="space-y-2 text-sm">
                  <div class="flex justify-between">
                    <span class="text-gray-500 dark:text-gray-400">Input</span>
                    <span class="text-gray-900 dark:text-gray-100">{{ formatNumber(tokenUsage.input) }}</span>
                  </div>
                  <div class="flex justify-between">
                    <span class="text-gray-500 dark:text-gray-400">Output</span>
                    <span class="text-gray-900 dark:text-gray-100">{{ formatNumber(tokenUsage.output) }}</span>
                  </div>
                  <div v-if="tokenUsage.thinking > 0" class="flex justify-between">
                    <span class="text-gray-500 dark:text-gray-400">Thinking</span>
                    <span class="text-gray-900 dark:text-gray-100">{{ formatNumber(tokenUsage.thinking) }}</span>
                  </div>
                  <div class="flex justify-between pt-2 border-t border-gray-200 dark:border-gray-700 font-medium">
                    <span class="text-gray-500 dark:text-gray-400">Total</span>
                    <span class="text-gray-900 dark:text-gray-100">{{ formatNumber(tokenUsage.total) }}</span>
                  </div>
                </div>
              </BaseCard>
              <BaseCard>
                <template #header>
                  <h3 class="text-sm font-medium text-gray-900 dark:text-gray-100">Estimated cost</h3>
                </template>
                <p class="text-2xl font-bold text-gray-900 dark:text-gray-100">${{ formatCost(estimatedCost) }}</p>
                <p class="text-xs text-gray-500 dark:text-gray-400 mt-1">Based on token count</p>
              </BaseCard>
              <BaseCard v-if="lastLatency">
                <template #header>
                  <h3 class="text-sm font-medium text-gray-900 dark:text-gray-100">Latency</h3>
                </template>
                <p class="text-2xl font-bold text-gray-900 dark:text-gray-100">{{ lastLatency }}ms</p>
                <p class="text-xs text-gray-500 dark:text-gray-400 mt-1">Last request</p>
              </BaseCard>
              <BaseCard>
                <template #header>
                  <h3 class="text-sm font-medium text-gray-900 dark:text-gray-100">Quick actions</h3>
                </template>
                <div class="space-y-1">
                  <router-link
                    :to="`/p/${projectId}/llm/sessions`"
                    class="flex items-center gap-2 p-2 text-sm text-primary-600 dark:text-primary-400 hover:bg-gray-50 dark:hover:bg-gray-700 rounded-lg"
                    @click="showStatsDrawer = false"
                  >
                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z" />
                    </svg>
                    Sessions
                  </router-link>
                  <router-link
                    :to="`/p/${projectId}/llm/prompts`"
                    class="flex items-center gap-2 p-2 text-sm text-primary-600 dark:text-primary-400 hover:bg-gray-50 dark:hover:bg-gray-700 rounded-lg"
                    @click="showStatsDrawer = false"
                  >
                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                    </svg>
                    Prompts
                  </router-link>
                  <router-link
                    :to="`/p/${projectId}/llm/settings`"
                    class="flex items-center gap-2 p-2 text-sm text-primary-600 dark:text-primary-400 hover:bg-gray-50 dark:hover:bg-gray-700 rounded-lg"
                    @click="showStatsDrawer = false"
                  >
                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
                    </svg>
                    Settings
                  </router-link>
                </div>
              </BaseCard>
            </div>
          </div>
        </div>
      </div>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue';
import { useRoute } from 'vue-router';
import axios from 'axios';
import { resolveApiUrl } from '@/composables/projectResolver';
import AppLayout from '@/Layouts/AppLayout.vue';
import BaseCard from '@/components/BaseCard.vue';
import BaseButton from '@/components/BaseButton.vue';
import { useAuth } from '@/composables/useAuth';

const route = useRoute();
const { user, fetchUser } = useAuth();

const projectId = computed(() => route.params.id);
const project = computed(() => ({ id: projectId.value }));

// UI state
const showSystemPrompt = ref(false);
const showStatsDrawer = ref(false);

// Model catalog (fetched from API)
const modelCatalog = ref([]);

// Model configuration
const selectedModel = ref('auto');
const temperature = ref(0.7);
const maxTokens = ref(4096);
const introspectionEnabled = ref(false);
const autoEvaluate = ref(false);
const isAutoMode = computed(() => selectedModel.value === 'auto');
const normalizedSelectedModel = computed(() => selectedModel.value.replaceAll('.', '-'));
const modelIsInFamily = (model, family) =>
  model === family || model.startsWith(`${family}-`) || model.startsWith(`${family}:`);
const hasProviderManagedSampling = computed(() => [
  'claude-opus-4-7',
  'claude-opus-4-8',
  'claude-opus-5',
  'claude-sonnet-5',
  'claude-fable-5',
  'claude-mythos-5',
].some((family) => modelIsInFamily(normalizedSelectedModel.value, family)));
const hasDefaultAdaptiveThinking = computed(() => [
  'claude-opus-5',
  'claude-sonnet-5',
  'claude-fable-5',
  'claude-mythos-5',
].some((family) => modelIsInFamily(normalizedSelectedModel.value, family)));

// Conversation state
const systemPrompt = ref('You are a helpful assistant.');
const messages = ref([]);
const userInput = ref('');
const streaming = ref(false);
const streamingContent = ref('');
const streamingThinking = ref('');

// Load from Prompt Hub modal
const showLoadPromptModal = ref(false);
const loadPromptConfigs = ref([]);
const loadPromptSearch = ref('');
const loadPromptLoading = ref(false);
const loadPromptSelectingId = ref(null);
const filteredPromptConfigs = computed(() => {
  const q = loadPromptSearch.value.trim().toLowerCase();
  if (!q) return loadPromptConfigs.value;
  return loadPromptConfigs.value.filter(
    (c) =>
      (c.name || '').toLowerCase().includes(q) ||
      (c.description || '').toLowerCase().includes(q)
  );
});

// Managed prompt config mode — when set, prompt_config + prompt_variables are
// sent to the backend for server-side resolution instead of raw system prompt.
const activePromptConfigName = ref(null);
const activePromptConfigLabel = ref('');
const promptVariableDefinitions = ref([]);
const promptVariableValues = ref({});
const isManagedPromptMode = computed(() => !!activePromptConfigName.value);

const clearManagedPrompt = () => {
  activePromptConfigName.value = null;
  activePromptConfigLabel.value = '';
  promptVariableDefinitions.value = [];
  promptVariableValues.value = {};
};

// Stats
const tokenUsage = ref({
  input: 0,
  output: 0,
  thinking: 0,
  total: 0,
});
const estimatedCost = ref(0);
const lastLatency = ref(null);

const formatNumber = (num) => {
  return (num || 0).toLocaleString();
};

const formatCost = (cost) => {
  return parseFloat(cost || 0).toFixed(4);
};

const scoreColor = (score) => {
  if (score >= 0.8) return 'text-brand-600 dark:text-brand-400';
  if (score >= 0.5) return 'text-yellow-600 dark:text-yellow-400';
  return 'text-red-600 dark:text-red-400';
};

const buildRequestMessages = () => {
  const msgs = [];
  // In managed prompt mode, don't send the raw system prompt — the backend resolves it
  if (!isManagedPromptMode.value && systemPrompt.value.trim()) {
    msgs.push({ role: 'system', content: systemPrompt.value.trim() });
  }
  messages.value.forEach(m => {
    msgs.push({ role: m.role, content: m.content });
  });
  return msgs;
};

/** Send via the dedicated playground endpoint (used for auto/fallback-chain mode). */
const sendMessageAuto = async (userMessage) => {
  const requestMessages = buildRequestMessages();

  const body = {
    project_id: projectId.value,
    model: 'auto',
    messages: requestMessages,
    max_tokens: maxTokens.value,
    use_fallback_chain: true,
    enable_introspection: introspectionEnabled.value,
    auto_evaluate: autoEvaluate.value,
  };

  // Auto may select a provider that accepts sampling controls. The Anthropic
  // adapter strips unsupported values after the actual model is resolved.
  body.temperature = temperature.value;

  // In managed prompt mode, send prompt_config + variables for server-side resolution
  if (isManagedPromptMode.value) {
    body.prompt_config = activePromptConfigName.value;
    const vars = {};
    for (const def of promptVariableDefinitions.value) {
      const val = promptVariableValues.value[def.name];
      if (val !== '' && val != null) {
        // Try to parse as JSON for typed values, fall back to string
        try {
          vars[def.name] = JSON.parse(val);
        } catch {
          vars[def.name] = val;
        }
      }
    }
    if (Object.keys(vars).length > 0) {
      body.prompt_variables = vars;
    }
  }

  const response = await axios.post('/api/llm/playground', body);

  const data = response.data;
  const content = data.response || '';
  const modelUsed = data.model || '';
  const providerUsed = data.provider || '';
  const fallbackUsed = data.fallback_used || false;

  messages.value.push({
    role: 'assistant',
    content,
    routedVia: modelUsed ? `${modelUsed} · ${providerUsed}` : null,
    fallbackUsed,
    evaluation: data.evaluation || null,
  });

  const inputTokens = requestMessages.reduce((acc, m) => acc + (m.content?.length || 0) / 4, 0);
  const outputTokens = content.length / 4;
  tokenUsage.value.input += Math.round(inputTokens);
  tokenUsage.value.output += Math.round(outputTokens);
  tokenUsage.value.total = tokenUsage.value.input + tokenUsage.value.output + tokenUsage.value.thinking;

  if (data.cost_usd) {
    estimatedCost.value += parseFloat(data.cost_usd);
  } else {
    estimatedCost.value += (tokenUsage.value.input * 0.000003) + (tokenUsage.value.output * 0.000015);
  }
};

/** Send via the main gateway with streaming (used for explicit model selection). */
const sendMessageStreaming = async (requestMessages) => {
  const requestBody = {
    model: selectedModel.value,
    messages: requestMessages,
    max_tokens: maxTokens.value,
    stream: true,
  };

  if (!hasProviderManagedSampling.value) {
    requestBody.temperature = temperature.value;
  }

  if (introspectionEnabled.value && !hasDefaultAdaptiveThinking.value) {
    if (selectedModel.value.includes('claude')) {
      requestBody.thinking = { type: 'enabled', budget_tokens: 10000 };
    } else if (selectedModel.value.startsWith('o')) {
      requestBody.reasoning_effort = 'medium';
    }
  }

  // In managed prompt mode, let the gateway resolve the prompt config
  if (isManagedPromptMode.value) {
    requestBody.prompt_config = activePromptConfigName.value;
    const vars = {};
    for (const def of promptVariableDefinitions.value) {
      const val = promptVariableValues.value[def.name];
      if (val !== '' && val != null) {
        try {
          vars[def.name] = JSON.parse(val);
        } catch {
          vars[def.name] = val;
        }
      }
    }
    if (Object.keys(vars).length > 0) {
      requestBody.prompt_variables = vars;
    }
  }

  const response = await fetch(resolveApiUrl(`/api/projects/${projectId.value}/gateway/v1/chat/completions`), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(requestBody),
  });

  if (!response.ok) {
    let detail = `HTTP error: ${response.status}`;
    try {
      const body = await response.json();
      detail = body?.error?.message || body?.message || body?.error || detail;
    } catch (_) {}
    throw new Error(detail);
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let fullContent = '';
  let fullThinking = '';

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    const chunk = decoder.decode(value, { stream: true });
    for (const line of chunk.split('\n')) {
      if (!line.startsWith('data: ')) continue;
      const data = line.slice(6);
      if (data === '[DONE]') continue;
      try {
        const parsed = JSON.parse(data);
        const delta = parsed.choices?.[0]?.delta;
        if (delta?.content) {
          fullContent += delta.content;
          streamingContent.value = fullContent;
        }
        if (delta?.thinking) {
          fullThinking += delta.thinking;
          streamingThinking.value = fullThinking;
        }
      } catch (e) {
        // partial chunk — ignore
      }
    }
  }

  messages.value.push({
    role: 'assistant',
    content: fullContent,
    thinking: fullThinking || null,
  });

  const inputTokens = requestMessages.reduce((acc, m) => acc + (m.content?.length || 0) / 4, 0);
  const outputTokens = fullContent.length / 4;
  const thinkingTokens = fullThinking.length / 4;
  tokenUsage.value.input += Math.round(inputTokens);
  tokenUsage.value.output += Math.round(outputTokens);
  tokenUsage.value.thinking += Math.round(thinkingTokens);
  tokenUsage.value.total = tokenUsage.value.input + tokenUsage.value.output + tokenUsage.value.thinking;
  estimatedCost.value = (tokenUsage.value.input * 0.000003) +
                        (tokenUsage.value.output * 0.000015) +
                        (tokenUsage.value.thinking * 0.000015);
};

function onInputKeydown(e) {
  if (e.key !== 'Enter') return;
  if (e.shiftKey) return; // Shift+Enter: new line
  e.preventDefault();
  sendMessage();
}

const sendMessage = async () => {
  if (!userInput.value.trim() || streaming.value) return;

  const userMessage = userInput.value.trim();
  userInput.value = '';

  messages.value.push({ role: 'user', content: userMessage });

  streaming.value = true;
  streamingContent.value = '';
  streamingThinking.value = '';
  const startTime = Date.now();

  try {
    if (isAutoMode.value) {
      await sendMessageAuto(userMessage);
    } else {
      await sendMessageStreaming(buildRequestMessages());
    }
    lastLatency.value = Date.now() - startTime;
  } catch (error) {
    console.error('Failed to send message:', error);
    const detail = error.response?.data?.message
      || error.response?.data?.error?.message
      || error.response?.data?.error
      || error.message;
    messages.value.push({
      role: 'assistant',
      content: `Error: ${detail}`,
    });
  } finally {
    streaming.value = false;
    streamingContent.value = '';
    streamingThinking.value = '';
  }
};

const clearConversation = () => {
  messages.value = [];
  tokenUsage.value = { input: 0, output: 0, thinking: 0, total: 0 };
  estimatedCost.value = 0;
  lastLatency.value = null;
};

const saveAsTemplate = async () => {
  alert('Save as template feature coming soon!');
};

const openLoadPromptModal = async () => {
  showLoadPromptModal.value = true;
  loadPromptSearch.value = '';
  loadPromptSelectingId.value = null;
  loadPromptLoading.value = true;
  try {
    const res = await axios.get(`/api/llm/prompts/configs?project_id=${projectId.value}`);
    loadPromptConfigs.value = res.data || [];
  } catch (err) {
    console.error('Failed to fetch prompt configs:', err);
    loadPromptConfigs.value = [];
  } finally {
    loadPromptLoading.value = false;
  }
};

const selectPromptConfig = async (config) => {
  loadPromptSelectingId.value = config.id;
  try {
    const [configRes, versionsRes] = await Promise.all([
      axios.get(`/api/llm/prompts/configs/${config.id}?project_id=${projectId.value}`),
      axios.get(`/api/llm/prompts/configs/${config.id}/versions?project_id=${projectId.value}`),
    ]);
    const cfg = configRes.data;
    const versions = versionsRes.data || [];
    const activeVersion = cfg?.active_version_id
      ? versions.find((v) => v.id === cfg.active_version_id)
      : versions[0];

    // Enter managed prompt mode: store config name for server-side resolution
    activePromptConfigName.value = cfg.name;
    activePromptConfigLabel.value = cfg.name + (activeVersion ? ` (v${activeVersion.version})` : '');

    // Parse variable definitions from the active version
    const vars = activeVersion?.variables || [];
    const defs = Array.isArray(vars) ? vars : (typeof vars === 'string' ? JSON.parse(vars) : []);
    promptVariableDefinitions.value = defs;

    // Initialize variable values with defaults
    const vals = {};
    for (const def of defs) {
      if (def.default != null) {
        vals[def.name] = typeof def.default === 'string' ? def.default : JSON.stringify(def.default);
      } else {
        vals[def.name] = '';
      }
    }
    promptVariableValues.value = vals;

    // Show the raw template in the system prompt area for reference (read-only in managed mode)
    if (activeVersion?.system_prompt != null) {
      systemPrompt.value = activeVersion.system_prompt;
    } else {
      systemPrompt.value = '';
    }
    showSystemPrompt.value = true;

    // Apply model settings for display (actual resolution happens server-side)
    if (activeVersion?.model != null && activeVersion.model !== '') {
      selectedModel.value = activeVersion.model;
    }
    if (activeVersion?.temperature != null) {
      temperature.value = Number(activeVersion.temperature);
    }
    if (activeVersion?.max_tokens != null) {
      maxTokens.value = Number(activeVersion.max_tokens);
    }
    showLoadPromptModal.value = false;
  } catch (err) {
    console.error('Failed to load prompt:', err);
    alert(err.response?.data?.message || 'Failed to load prompt');
  } finally {
    loadPromptSelectingId.value = null;
  }
};

const fetchModelCatalog = async () => {
  try {
    const response = await axios.get(`/api/projects/${projectId.value}/llm/settings/models`);
    modelCatalog.value = response.data.providers || [];
  } catch (e) {
    console.warn('Failed to fetch model catalog', e);
  }
};

onMounted(async () => {
  await fetchUser();
  await fetchModelCatalog();
});

watch(projectId, () => {
  fetchUser();
  fetchModelCatalog();
});

watch([isAutoMode, hasDefaultAdaptiveThinking], ([auto, adaptive]) => {
  if (auto || adaptive) {
    introspectionEnabled.value = false;
  }
});
</script>
