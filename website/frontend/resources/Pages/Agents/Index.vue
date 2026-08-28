<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-[1200px] mx-auto px-4 py-6 space-y-6">
      <div>
        <h1 class="text-2xl font-bold text-gray-900 dark:text-white">Agents</h1>
        <p class="mt-1 text-sm text-gray-500 dark:text-gray-400">Manage agent tools, tokens, and MooDeng configuration</p>
      </div>

      <!-- Tab bar -->
      <div class="flex border-b border-gray-200 dark:border-gray-700">
        <button
          v-for="tab in tabs"
          :key="tab.id"
          @click="setTab(tab.id)"
          class="px-4 py-2.5 text-sm font-medium transition-colors border-b-2 -mb-px"
          :class="activeTab === tab.id
            ? 'border-primary-600 text-primary-600 dark:text-primary-400'
            : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'"
        >{{ tab.label }}</button>
      </div>

      <!-- Tools tab -->
      <div v-if="activeTab === 'tools'">
        <div v-if="toolsLoading" class="text-sm text-gray-500 dark:text-gray-400">Loading tools...</div>
        <div v-else-if="tools.length === 0" class="text-center py-12 text-gray-500 dark:text-gray-400">
          <p class="text-sm">No tools observed yet.</p>
          <p class="text-xs mt-1">Tools appear here automatically when your agents or LLM requests use function calling.</p>
        </div>
        <div v-else class="grid gap-4 md:grid-cols-2">
          <BaseCard v-for="tool in tools" :key="tool.name">
            <div class="p-4 space-y-3">
              <div class="flex items-center gap-2">
                <h3 class="text-base font-semibold text-gray-900 dark:text-white font-mono">{{ tool.name }}</h3>
                <span v-if="tool.blocked_project_wide" class="text-xs px-2 py-0.5 rounded-full bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300 font-medium">Blocked</span>
              </div>
              <div class="flex items-center gap-4 text-xs text-gray-500 dark:text-gray-400">
                <span>{{ formatNumber(tool.total_calls) }} calls</span>
                <span v-if="tool.last_used">Last used {{ formatRelative(tool.last_used) }}</span>
              </div>
              <details v-if="tool.blocked_by_prompts && tool.blocked_by_prompts.length" class="text-xs">
                <summary class="cursor-pointer text-amber-600 dark:text-amber-400 hover:underline">Blocked by {{ tool.blocked_by_prompts.length }} prompt{{ tool.blocked_by_prompts.length > 1 ? 's' : '' }}</summary>
                <ul class="mt-2 space-y-1 pl-4 list-disc text-gray-600 dark:text-gray-400">
                  <li v-for="p in tool.blocked_by_prompts" :key="p.prompt_id">
                    <router-link :to="`/p/${projectId}/llm/prompts/${p.prompt_id}`" class="text-primary-600 dark:text-primary-400 hover:underline">{{ p.prompt_name }}</router-link>
                  </li>
                </ul>
              </details>
            </div>
          </BaseCard>
        </div>
      </div>

      <!-- Tokens tab -->
      <div v-if="activeTab === 'tokens'">
        <div class="flex items-center justify-between mb-6">
          <p class="text-sm text-gray-500 dark:text-gray-400">Manage tokens used by AI agents and MCP integrations</p>
          <button
            @click="showCreateModal = true"
            class="px-4 py-2 text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 rounded-lg transition-colors"
          >Create Agent Token</button>
        </div>

        <div v-if="tokensLoading" class="text-sm text-gray-500 dark:text-gray-400">Loading tokens...</div>
        <div v-else-if="agentTokens.length === 0" class="text-center py-12">
          <p class="text-gray-500 dark:text-gray-400">No agent tokens found. Create one to get started.</p>
        </div>
        <div v-else class="space-y-3">
          <BaseCard v-for="token in agentTokens" :key="token.id">
            <div class="p-4 flex items-start justify-between">
              <div class="space-y-1">
                <div class="flex items-center gap-2">
                  <h3 class="text-sm font-semibold text-gray-900 dark:text-white">{{ token.label || '(unnamed)' }}</h3>
                  <code class="text-xs font-mono text-gray-500 dark:text-gray-400 bg-gray-100 dark:bg-gray-800 px-2 py-0.5 rounded">dh_...{{ token.key_prefix }}</code>
                </div>
                <div class="flex items-center gap-4 text-xs text-gray-500 dark:text-gray-400">
                  <span>Created {{ formatDate(token.created_at) }}</span>
                  <span v-if="token.expires_at">Expires {{ formatDate(token.expires_at) }}</span>
                  <span v-else>No expiration</span>
                </div>
                <div v-if="token.scopes && token.scopes.length" class="flex flex-wrap gap-1.5 mt-1">
                  <span
                    v-for="s in token.scopes"
                    :key="s"
                    class="inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium"
                    :class="scopeBadgeClass(s)"
                  >{{ s }}</span>
                </div>
              </div>
              <button
                @click="revokeToken(token.id)"
                class="text-xs text-red-600 dark:text-red-400 hover:underline"
              >Revoke</button>
            </div>
          </BaseCard>
        </div>

        <!-- Create token modal -->
        <Teleport to="body">
          <div v-if="showCreateModal" class="fixed inset-0 z-50 flex items-center justify-center">
            <div class="fixed inset-0 bg-black/40" @click="showCreateModal = false"></div>
            <div class="relative z-10 w-full max-w-lg rounded-xl bg-white dark:bg-gray-800 p-6 shadow-xl mx-4">
              <h3 class="text-lg font-semibold text-gray-900 dark:text-white mb-4">Create Agent Token</h3>
              <form @submit.prevent="createAgentToken" class="space-y-5">
                <div>
                  <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Label</label>
                  <input
                    v-model="newKeyLabel"
                    type="text"
                    placeholder="e.g. Cursor Dev, CI Pipeline"
                    class="w-full rounded-lg border border-gray-300 dark:border-gray-600 px-3 py-2 text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:border-primary-500 focus:outline-none focus:ring-1 focus:ring-primary-500"
                  />
                </div>
                <div>
                  <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Scopes</label>
                  <ScopeSelector v-model="newKeyScopes" :max-scopes="allScopes" />
                </div>
                <div>
                  <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Expiry date <span class="text-gray-400 font-normal">(optional)</span>
                  </label>
                  <input
                    v-model="newKeyExpiresAt"
                    type="date"
                    :min="todayISO"
                    class="w-full rounded-lg border border-gray-300 dark:border-gray-600 px-3 py-2 text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:border-primary-500 focus:outline-none focus:ring-1 focus:ring-primary-500"
                  />
                </div>
                <div class="flex items-center justify-end gap-3 pt-2">
                  <button type="button" @click="showCreateModal = false" class="px-4 py-2 text-sm text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-white">Cancel</button>
                  <button
                    type="submit"
                    :disabled="creatingKey || newKeyScopes.length === 0"
                    class="px-4 py-2 text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 rounded-lg disabled:opacity-50"
                  >{{ creatingKey ? 'Creating...' : 'Create' }}</button>
                </div>
              </form>
            </div>
          </div>
        </Teleport>

        <!-- Key reveal dialog -->
        <Teleport to="body">
          <div v-if="showKeyReveal" class="fixed inset-0 z-50 flex items-center justify-center">
            <div class="fixed inset-0 bg-black/40"></div>
            <div class="relative z-10 w-full max-w-lg rounded-xl bg-white dark:bg-gray-800 p-6 shadow-xl mx-4">
              <div class="flex items-start gap-3 mb-4">
                <div class="flex-shrink-0 rounded-full bg-amber-100 dark:bg-amber-900/30 p-2">
                  <svg class="w-5 h-5 text-amber-600 dark:text-amber-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                </div>
                <div>
                  <h3 class="text-lg font-semibold text-gray-900 dark:text-white">Your new API key</h3>
                  <p class="mt-1 text-sm text-amber-700 dark:text-amber-300">This key will not be shown again. Copy it now and store it securely.</p>
                </div>
              </div>
              <div class="flex items-center gap-2 mb-6">
                <code class="flex-1 text-sm font-mono bg-gray-100 dark:bg-gray-900 px-3 py-2 rounded-lg break-all select-all border border-gray-200 dark:border-gray-700 text-gray-900 dark:text-gray-100">{{ revealedKey }}</code>
                <button
                  @click="copyKey"
                  class="flex-shrink-0 p-2 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-700 rounded-lg transition-colors"
                  :class="copiedReveal ? 'text-green-600 dark:text-green-400' : ''"
                >
                  <svg v-if="!copiedReveal" class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" /></svg>
                  <svg v-else class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" /></svg>
                </button>
              </div>
              <div class="flex justify-end">
                <button @click="showKeyReveal = false; revealedKey = ''" class="px-4 py-2 text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 rounded-lg">Done</button>
              </div>
            </div>
          </div>
        </Teleport>
      </div>

      <!-- MooDeng tab -->
      <div v-if="activeTab === 'moodeng'">
        <div class="max-w-[800px] space-y-6">
          <div class="flex items-center justify-between">
            <p class="text-sm text-gray-500 dark:text-gray-400">Configure the in-app AI agent</p>
            <label class="relative inline-flex items-center cursor-pointer">
              <input v-model="moodengSettings.agent_enabled" type="checkbox" class="sr-only peer" />
              <div class="w-11 h-6 bg-gray-200 dark:bg-gray-700 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-primary-500 rounded-full peer peer-checked:after:translate-x-full rtl:peer-checked:after:-translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-primary-600"></div>
            </label>
          </div>

          <transition name="fade">
            <span v-if="moodengSaveStatus === 'saving'" class="inline-flex items-center gap-1.5 text-xs text-gray-400">
              <svg class="w-3.5 h-3.5 animate-spin" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4" /><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" /></svg>
              Saving...
            </span>
            <span v-else-if="moodengSaveStatus === 'saved'" class="inline-flex items-center gap-1 text-xs text-green-600 dark:text-green-400">
              <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" /></svg>
              Saved
            </span>
            <span v-else-if="moodengSaveStatus === 'error'" class="inline-flex items-center gap-1 text-xs text-red-600 dark:text-red-400">Save failed</span>
          </transition>

          <div v-if="moodengLoading" class="text-sm text-gray-500 dark:text-gray-400">Loading...</div>

          <template v-else>
            <BaseCard>
              <div class="p-4 space-y-4">
                <p class="text-sm text-gray-600 dark:text-gray-400">
                  The in-app AI agent can help users navigate the platform, query data, and perform actions.
                </p>

                <div v-if="!hasIntegrations" class="bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-lg p-3">
                  <p class="text-sm text-yellow-800 dark:text-yellow-200">
                    Add at least one AI provider integration to use the agent.
                    <router-link :to="`/p/${projectId}/llm/integrations`" class="font-medium underline hover:text-yellow-900 dark:hover:text-yellow-100">Go to Integrations</router-link>
                  </p>
                </div>

                <div v-if="moodengSettings.agent_enabled" class="space-y-5">
                  <div>
                    <h3 class="text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Agent Permissions</h3>
                    <p class="text-sm text-gray-600 dark:text-gray-400 mb-3">Control which actions the AI agent can perform</p>
                    <ScopeSelector v-model="moodengSettings.agent_scopes" :max-scopes="AGENT_SCOPES_MAX" />
                  </div>

                  <div class="border-t border-gray-200 dark:border-gray-700 pt-4">
                    <div class="flex items-center justify-between">
                      <div>
                        <h3 class="text-sm font-medium text-gray-900 dark:text-gray-100">Auto-Investigate Alerts &amp; Exceptions</h3>
                        <p class="text-sm text-gray-500 dark:text-gray-400 mt-0.5">
                          When enabled, MooDeng automatically investigates alert firings and new exceptions,
                          then posts findings to your notification channels.
                        </p>
                      </div>
                      <label class="relative inline-flex items-center cursor-pointer flex-shrink-0 ml-4">
                        <input v-model="moodengSettings.auto_investigate" type="checkbox" class="sr-only peer" />
                        <div class="w-11 h-6 bg-gray-200 dark:bg-gray-700 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-primary-500 rounded-full peer peer-checked:after:translate-x-full rtl:peer-checked:after:-translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-primary-600"></div>
                      </label>
                    </div>
                  </div>

                  <div class="border-t border-gray-200 dark:border-gray-700 pt-4">
                    <div class="flex items-center justify-between">
                      <div>
                        <h3 class="text-sm font-medium text-gray-900 dark:text-gray-100">Agent Soul</h3>
                        <p class="text-sm text-gray-500 dark:text-gray-400 mt-0.5">
                          Personalize MooDeng with your project's context, custom instructions, key services, and behavioral rules.
                        </p>
                      </div>
                      <button
                        @click="openSoulOverlay"
                        class="px-4 py-2 text-sm font-medium text-primary-600 dark:text-primary-400 border border-primary-300 dark:border-primary-600 rounded-lg hover:bg-primary-50 dark:hover:bg-primary-900/20 transition-colors flex-shrink-0 ml-4"
                      >Configure Soul</button>
                    </div>
                    <div v-if="soulSummaryCount > 0" class="mt-2 text-xs text-gray-500 dark:text-gray-400">
                      {{ soulSummaryCount }} field{{ soulSummaryCount > 1 ? 's' : '' }} configured
                    </div>
                  </div>
                </div>
              </div>
            </BaseCard>
          </template>
        </div>
      </div>
    </div>

    <!-- Soul overlay -->
    <Teleport to="body">
      <div v-if="showSoulOverlay" class="fixed inset-0 z-50 flex flex-col bg-white dark:bg-gray-900">
        <!-- Header -->
        <div class="flex items-center justify-between px-6 py-4 border-b border-gray-200 dark:border-gray-700 flex-shrink-0">
          <div>
            <h2 class="text-lg font-semibold text-gray-900 dark:text-white">Agent Soul</h2>
            <p class="text-sm text-gray-500 dark:text-gray-400 mt-0.5">Personalize MooDeng for this project</p>
          </div>
          <div class="flex items-center gap-3">
            <span v-if="soulSaving" class="text-xs text-gray-400">Saving...</span>
            <button
              @click="saveSoul"
              :disabled="soulSaving"
              class="px-4 py-2 text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 rounded-lg disabled:opacity-50 transition-colors"
            >Save</button>
            <button @click="showSoulOverlay = false" class="p-2 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-800">
              <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" /></svg>
            </button>
          </div>
        </div>

        <!-- Scrollable body -->
        <div class="flex-1 overflow-y-auto px-6 py-6">
          <div class="max-w-[800px] mx-auto space-y-8">

            <!-- Project Description -->
            <div>
              <label class="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Project Description</label>
              <p class="text-xs text-gray-500 dark:text-gray-400 mb-2">What does this project do? MooDeng uses this to contextualize every answer.</p>
              <textarea
                v-model="soulDraft.project_description"
                rows="3"
                placeholder="B2B SaaS for real-time logistics tracking with 50k daily active users."
                class="w-full rounded-lg border border-gray-300 dark:border-gray-600 px-3 py-2 text-sm bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:border-primary-500 focus:outline-none focus:ring-1 focus:ring-primary-500 placeholder-gray-400 dark:placeholder-gray-500"
              ></textarea>
            </div>

            <!-- Tech Context -->
            <div>
              <label class="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Tech Context</label>
              <p class="text-xs text-gray-500 dark:text-gray-400 mb-2">Your tech stack, infrastructure, and deployment details.</p>
              <textarea
                v-model="soulDraft.tech_context"
                rows="3"
                placeholder="Rust backend, Vue.js frontend, ClickHouse analytics, PostgreSQL, k8s on GCP"
                class="w-full rounded-lg border border-gray-300 dark:border-gray-600 px-3 py-2 text-sm bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:border-primary-500 focus:outline-none focus:ring-1 focus:ring-primary-500 placeholder-gray-400 dark:placeholder-gray-500"
              ></textarea>
            </div>

            <!-- Custom Instructions -->
            <div>
              <label class="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Custom Instructions</label>
              <p class="text-xs text-gray-500 dark:text-gray-400 mb-2">Freeform instructions injected directly into MooDeng's system prompt. Tell it how you want it to behave.</p>
              <textarea
                v-model="soulDraft.custom_instructions"
                rows="6"
                placeholder="Always check the GenAI dashboard before investigating alerts. Prefer traces over logs for latency debugging."
                class="w-full rounded-lg border border-gray-300 dark:border-gray-600 px-3 py-2 text-sm bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:border-primary-500 focus:outline-none focus:ring-1 focus:ring-primary-500 placeholder-gray-400 dark:placeholder-gray-500"
              ></textarea>
            </div>

            <!-- Tone -->
            <div>
              <label class="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Tone</label>
              <p class="text-xs text-gray-500 dark:text-gray-400 mb-3">How should MooDeng communicate?</p>
              <div class="flex flex-wrap gap-3">
                <label
                  v-for="t in toneOptions"
                  :key="t.value"
                  class="relative flex items-center gap-2 px-4 py-2 rounded-lg border cursor-pointer text-sm transition-colors"
                  :class="soulDraft.tone === t.value
                    ? 'border-primary-500 bg-primary-50 dark:bg-primary-900/20 text-primary-700 dark:text-primary-300'
                    : 'border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-300 hover:border-gray-400 dark:hover:border-gray-500'"
                >
                  <input
                    type="radio"
                    name="soul-tone"
                    :value="t.value"
                    v-model="soulDraft.tone"
                    class="sr-only"
                  />
                  {{ t.label }}
                </label>
                <button
                  v-if="soulDraft.tone"
                  @click="soulDraft.tone = null"
                  class="text-xs text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 underline"
                >Clear</button>
              </div>
            </div>

            <!-- Key Services -->
            <div>
              <label class="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Key Services</label>
              <p class="text-xs text-gray-500 dark:text-gray-400 mb-3">Services MooDeng should know about. It will reference these when investigating issues or answering questions.</p>
              <div class="space-y-2">
                <div v-for="(svc, i) in soulDraft.key_services" :key="i" class="flex items-start gap-2">
                  <input
                    v-model="svc.name"
                    placeholder="Service name"
                    class="flex-1 rounded-lg border border-gray-300 dark:border-gray-600 px-3 py-2 text-sm bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:border-primary-500 focus:outline-none focus:ring-1 focus:ring-primary-500 placeholder-gray-400 dark:placeholder-gray-500"
                  />
                  <input
                    v-model="svc.description"
                    placeholder="Description"
                    class="flex-1 rounded-lg border border-gray-300 dark:border-gray-600 px-3 py-2 text-sm bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:border-primary-500 focus:outline-none focus:ring-1 focus:ring-primary-500 placeholder-gray-400 dark:placeholder-gray-500"
                  />
                  <input
                    v-model="svc.owner"
                    placeholder="Owner (optional)"
                    class="w-36 rounded-lg border border-gray-300 dark:border-gray-600 px-3 py-2 text-sm bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:border-primary-500 focus:outline-none focus:ring-1 focus:ring-primary-500 placeholder-gray-400 dark:placeholder-gray-500"
                  />
                  <button @click="soulDraft.key_services.splice(i, 1)" class="p-2 text-gray-400 hover:text-red-500 flex-shrink-0">
                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" /></svg>
                  </button>
                </div>
              </div>
              <button @click="soulDraft.key_services.push({ name: '', description: '', owner: '' })" class="mt-2 text-xs text-primary-600 dark:text-primary-400 hover:underline">+ Add service</button>
            </div>

            <!-- Important Thresholds -->
            <div>
              <label class="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Important Thresholds</label>
              <p class="text-xs text-gray-500 dark:text-gray-400 mb-3">SLOs and thresholds MooDeng should reference when evaluating health.</p>
              <div class="space-y-2">
                <div v-for="(_, i) in soulDraft.important_thresholds" :key="i" class="flex items-center gap-2">
                  <input
                    v-model="soulDraft.important_thresholds[i]"
                    placeholder="p99 latency on routing-engine < 200ms"
                    class="flex-1 rounded-lg border border-gray-300 dark:border-gray-600 px-3 py-2 text-sm bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:border-primary-500 focus:outline-none focus:ring-1 focus:ring-primary-500 placeholder-gray-400 dark:placeholder-gray-500"
                  />
                  <button @click="soulDraft.important_thresholds.splice(i, 1)" class="p-2 text-gray-400 hover:text-red-500 flex-shrink-0">
                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" /></svg>
                  </button>
                </div>
              </div>
              <button @click="soulDraft.important_thresholds.push('')" class="mt-2 text-xs text-primary-600 dark:text-primary-400 hover:underline">+ Add threshold</button>
            </div>

            <!-- Known Issues -->
            <div>
              <label class="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Known Issues</label>
              <p class="text-xs text-gray-500 dark:text-gray-400 mb-3">Quirks and known problems MooDeng should factor in before escalating.</p>
              <div class="space-y-2">
                <div v-for="(_, i) in soulDraft.known_issues" :key="i" class="flex items-center gap-2">
                  <input
                    v-model="soulDraft.known_issues[i]"
                    placeholder="GCP us-east1 has periodic network blips on Tuesdays"
                    class="flex-1 rounded-lg border border-gray-300 dark:border-gray-600 px-3 py-2 text-sm bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:border-primary-500 focus:outline-none focus:ring-1 focus:ring-primary-500 placeholder-gray-400 dark:placeholder-gray-500"
                  />
                  <button @click="soulDraft.known_issues.splice(i, 1)" class="p-2 text-gray-400 hover:text-red-500 flex-shrink-0">
                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" /></svg>
                  </button>
                </div>
              </div>
              <button @click="soulDraft.known_issues.push('')" class="mt-2 text-xs text-primary-600 dark:text-primary-400 hover:underline">+ Add known issue</button>
            </div>

            <!-- Playbooks -->
            <div>
              <label class="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Playbooks</label>
              <p class="text-xs text-gray-500 dark:text-gray-400 mb-3">Step-by-step workflows for specific situations. MooDeng follows these when the trigger matches.</p>
              <div class="space-y-3">
                <div v-for="(pb, i) in soulDraft.playbooks" :key="i" class="border border-gray-200 dark:border-gray-700 rounded-lg p-3 space-y-2">
                  <div class="flex items-center gap-2">
                    <input
                      v-model="pb.trigger"
                      placeholder="Trigger (e.g. on_alert, on_latency_spike)"
                      class="flex-1 rounded-lg border border-gray-300 dark:border-gray-600 px-3 py-2 text-sm bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:border-primary-500 focus:outline-none focus:ring-1 focus:ring-primary-500 placeholder-gray-400 dark:placeholder-gray-500"
                    />
                    <button @click="soulDraft.playbooks.splice(i, 1)" class="p-2 text-gray-400 hover:text-red-500 flex-shrink-0">
                      <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" /></svg>
                    </button>
                  </div>
                  <textarea
                    v-model="pb.instructions"
                    rows="3"
                    placeholder="Step-by-step instructions for MooDeng to follow when this trigger fires..."
                    class="w-full rounded-lg border border-gray-300 dark:border-gray-600 px-3 py-2 text-sm bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:border-primary-500 focus:outline-none focus:ring-1 focus:ring-primary-500 placeholder-gray-400 dark:placeholder-gray-500"
                  ></textarea>
                </div>
              </div>
              <button @click="soulDraft.playbooks.push({ trigger: '', instructions: '' })" class="mt-2 text-xs text-primary-600 dark:text-primary-400 hover:underline">+ Add playbook</button>
            </div>

            <!-- Always Do -->
            <div>
              <label class="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Always Do</label>
              <p class="text-xs text-gray-500 dark:text-gray-400 mb-3">Rules MooDeng must always follow.</p>
              <div class="space-y-2">
                <div v-for="(_, i) in soulDraft.always_do" :key="i" class="flex items-center gap-2">
                  <input
                    v-model="soulDraft.always_do[i]"
                    placeholder="Always mention the dashboard link when sharing metrics"
                    class="flex-1 rounded-lg border border-gray-300 dark:border-gray-600 px-3 py-2 text-sm bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:border-primary-500 focus:outline-none focus:ring-1 focus:ring-primary-500 placeholder-gray-400 dark:placeholder-gray-500"
                  />
                  <button @click="soulDraft.always_do.splice(i, 1)" class="p-2 text-gray-400 hover:text-red-500 flex-shrink-0">
                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" /></svg>
                  </button>
                </div>
              </div>
              <button @click="soulDraft.always_do.push('')" class="mt-2 text-xs text-primary-600 dark:text-primary-400 hover:underline">+ Add rule</button>
            </div>

            <!-- Never Do -->
            <div>
              <label class="block text-sm font-medium text-gray-900 dark:text-gray-100 mb-1">Never Do</label>
              <p class="text-xs text-gray-500 dark:text-gray-400 mb-3">Hard constraints MooDeng must never violate.</p>
              <div class="space-y-2">
                <div v-for="(_, i) in soulDraft.never_do" :key="i" class="flex items-center gap-2">
                  <input
                    v-model="soulDraft.never_do[i]"
                    placeholder="Never delete production data without explicit confirmation"
                    class="flex-1 rounded-lg border border-gray-300 dark:border-gray-600 px-3 py-2 text-sm bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:border-primary-500 focus:outline-none focus:ring-1 focus:ring-primary-500 placeholder-gray-400 dark:placeholder-gray-500"
                  />
                  <button @click="soulDraft.never_do.splice(i, 1)" class="p-2 text-gray-400 hover:text-red-500 flex-shrink-0">
                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" /></svg>
                  </button>
                </div>
              </div>
              <button @click="soulDraft.never_do.push('')" class="mt-2 text-xs text-primary-600 dark:text-primary-400 hover:underline">+ Add constraint</button>
            </div>

          </div>
        </div>
      </div>
    </Teleport>
  </AppLayout>
</template>

<script setup>
import { ref, computed, watch, nextTick } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import axios from 'axios';
import AppLayout from '@/Layouts/AppLayout.vue';
import BaseCard from '@/components/BaseCard.vue';
import ScopeSelector from '@/components/ScopeSelector.vue';
import { useAuth } from '@/composables/useAuth';

const route = useRoute();
const router = useRouter();
const { user } = useAuth();
const projectId = computed(() => route.params.id);
const project = computed(() => ({ id: projectId.value }));

const tabs = [
  { id: 'tools', label: 'Tools' },
  { id: 'tokens', label: 'Tokens' },
  { id: 'moodeng', label: 'MooDeng' },
];
const activeTab = ref(route.query.tab || 'tools');

function setTab(tab) {
  activeTab.value = tab;
  router.replace({ query: { ...route.query, tab } });
}

// ─── Tools ───
const tools = ref([]);
const toolsLoading = ref(false);

async function fetchTools() {
  toolsLoading.value = true;
  try {
    const { data } = await axios.get(`/api/projects/${projectId.value}/mcp/tools`);
    tools.value = data;
  } catch (e) {
    console.error('Failed to load tools', e);
  } finally {
    toolsLoading.value = false;
  }
}

// ─── Tokens ───
const agentTokens = ref([]);
const tokensLoading = ref(false);
const showCreateModal = ref(false);
const showKeyReveal = ref(false);
const revealedKey = ref('');
const copiedReveal = ref(false);
const creatingKey = ref(false);

const newKeyLabel = ref('');
const newKeyScopes = ref([]);
const newKeyExpiresAt = ref('');

const allScopes = [
  'project:read',
  'project:write',
  'llm:read',
  'llm:write',
  'observability:read',
  'observability:write',
  'herd:read',
  'herd:write',
];

const todayISO = computed(() => new Date().toISOString().slice(0, 10));

const SCOPE_COLORS = {
  project: 'bg-blue-50 text-blue-700 ring-1 ring-inset ring-blue-600/20 dark:bg-blue-900/20 dark:text-blue-300 dark:ring-blue-500/30',
  llm: 'bg-purple-50 text-purple-700 ring-1 ring-inset ring-purple-600/20 dark:bg-purple-900/20 dark:text-purple-300 dark:ring-purple-500/30',
  observability: 'bg-green-50 text-green-700 ring-1 ring-inset ring-green-600/20 dark:bg-green-900/20 dark:text-green-300 dark:ring-green-500/30',
  herd: 'bg-amber-50 text-amber-700 ring-1 ring-inset ring-amber-600/20 dark:bg-amber-900/20 dark:text-amber-300 dark:ring-amber-500/30',
};

function scopeBadgeClass(scope) {
  const area = scope.split(':')[0];
  return SCOPE_COLORS[area] || 'bg-gray-50 text-gray-700 ring-1 ring-inset ring-gray-600/20 dark:bg-gray-800 dark:text-gray-300 dark:ring-gray-600/30';
}

async function fetchAgentTokens() {
  tokensLoading.value = true;
  try {
    const { data } = await axios.get(`/api/projects/${projectId.value}/keys`, { params: { key_type: 'agent' } });
    agentTokens.value = Array.isArray(data) ? data : (data.keys || []);
  } catch (e) {
    console.error('Failed to load tokens', e);
  } finally {
    tokensLoading.value = false;
  }
}

async function createAgentToken() {
  creatingKey.value = true;
  try {
    const body = {
      label: newKeyLabel.value || null,
      scopes: newKeyScopes.value,
      key_type: 'agent',
      expires_at: newKeyExpiresAt.value || null,
    };
    const { data } = await axios.post(`/api/projects/${projectId.value}/keys`, body);
    revealedKey.value = data.key || data.token || '';
    showCreateModal.value = false;
    showKeyReveal.value = true;
    copiedReveal.value = false;
    newKeyLabel.value = '';
    newKeyScopes.value = [];
    newKeyExpiresAt.value = '';
    await fetchAgentTokens();
  } catch (e) {
    alert('Failed to create token: ' + (e.response?.data?.message || e.message));
  } finally {
    creatingKey.value = false;
  }
}

async function copyKey() {
  try {
    await navigator.clipboard.writeText(revealedKey.value);
    copiedReveal.value = true;
    setTimeout(() => { copiedReveal.value = false; }, 2000);
  } catch (err) {
    console.error('Failed to copy:', err);
  }
}

async function revokeToken(id) {
  if (!confirm('Revoke this token? Any agents using it will lose access.')) return;
  try {
    await axios.delete(`/api/projects/${projectId.value}/keys/${id}`);
    await fetchAgentTokens();
  } catch (e) {
    alert('Failed to revoke token: ' + (e.response?.data?.message || e.message));
  }
}

// ─── MooDeng ───
const moodengLoading = ref(true);
const moodengSaveStatus = ref('idle');
let moodengSaveTimer = null;
let moodengSnapshot = '';

const AGENT_SCOPES_MAX = [
  'project:read', 'project:write',
  'llm:read', 'llm:write',
  'observability:read', 'observability:write',
  'herd:read', 'herd:write',
];

const DEFAULT_AGENT_SCOPES = [
  'project:read', 'llm:read', 'observability:read', 'herd:read',
];

const moodengSettings = ref({
  agent_enabled: true,
  agent_scopes: [...DEFAULT_AGENT_SCOPES],
  auto_investigate: false,
  agent_soul: emptySoul(),
});
const moodengSettingsLoaded = ref(false);

function emptySoul() {
  return {
    project_description: '',
    tech_context: '',
    custom_instructions: '',
    tone: null,
    key_services: [],
    important_thresholds: [],
    known_issues: [],
    playbooks: [],
    never_do: [],
    always_do: [],
  };
}

const showSoulOverlay = ref(false);
const soulDraft = ref(emptySoul());
const soulSaving = ref(false);

const toneOptions = [
  { value: 'concise', label: 'Concise' },
  { value: 'detailed', label: 'Detailed' },
  { value: 'casual', label: 'Casual' },
  { value: 'formal', label: 'Formal' },
];

const soulSummaryCount = computed(() => {
  const s = moodengSettings.value.agent_soul;
  if (!s) return 0;
  let c = 0;
  if (s.project_description) c++;
  if (s.tech_context) c++;
  if (s.custom_instructions) c++;
  if (s.tone) c++;
  if (s.key_services?.length) c++;
  if (s.important_thresholds?.length) c++;
  if (s.known_issues?.length) c++;
  if (s.playbooks?.length) c++;
  if (s.never_do?.length) c++;
  if (s.always_do?.length) c++;
  return c;
});

function openSoulOverlay() {
  const src = moodengSettings.value.agent_soul || emptySoul();
  soulDraft.value = JSON.parse(JSON.stringify(src));
  showSoulOverlay.value = true;
}

async function saveSoul() {
  if (!moodengSettingsLoaded.value) return;
  soulSaving.value = true;
  try {
    const cleaned = { ...soulDraft.value };
    cleaned.important_thresholds = cleaned.important_thresholds.filter(v => v.trim());
    cleaned.known_issues = cleaned.known_issues.filter(v => v.trim());
    cleaned.always_do = cleaned.always_do.filter(v => v.trim());
    cleaned.never_do = cleaned.never_do.filter(v => v.trim());
    cleaned.key_services = cleaned.key_services.filter(s => s.name.trim());
    cleaned.playbooks = cleaned.playbooks.filter(p => p.trigger.trim() || p.instructions.trim());

    moodengSettings.value.agent_soul = cleaned;

    const payload = { agent_soul: cleaned };
    await axios.put(`/api/projects/${projectId.value}/llm/settings`, payload);
    fullMoodengSettings = payload;
    moodengSnapshot = JSON.stringify(moodengSettings.value);
    showSoulOverlay.value = false;
  } catch (e) {
    alert('Failed to save Agent Soul: ' + (e.response?.data?.message || e.message));
  } finally {
    soulSaving.value = false;
  }
}

const hasIntegrations = ref(true);
let fullMoodengSettings = null;

async function fetchMoodengSettings() {
  moodengLoading.value = true;
  moodengSettingsLoaded.value = false;
  try {
    const { data } = await axios.get(`/api/projects/${projectId.value}/llm/settings`);
    fullMoodengSettings = data;
    moodengSettings.value.agent_enabled = data.agent_enabled ?? true;
    moodengSettings.value.agent_scopes = Array.isArray(data.agent_scopes) ? data.agent_scopes : [...DEFAULT_AGENT_SCOPES];
    moodengSettings.value.auto_investigate = data.auto_investigate ?? false;
    moodengSettings.value.agent_soul = data.agent_soul && typeof data.agent_soul === 'object' ? data.agent_soul : emptySoul();
    moodengSettingsLoaded.value = true;
  } catch {
    fullMoodengSettings = {};
  } finally {
    moodengLoading.value = false;
    nextTick(() => { moodengSnapshot = JSON.stringify(moodengSettings.value); });
  }
}

async function fetchIntegrations() {
  try {
    const { data } = await axios.get(`/api/projects/${projectId.value}/llm/integrations`);
    hasIntegrations.value = Array.isArray(data) && data.length > 0;
  } catch {
    hasIntegrations.value = false;
  }
}

async function saveMoodeng() {
  if (!moodengSettingsLoaded.value) return;
  clearTimeout(moodengSaveTimer);
  moodengSaveStatus.value = 'saving';
  try {
    const payload = {
      agent_enabled: moodengSettings.value.agent_enabled,
      agent_scopes: moodengSettings.value.agent_scopes,
      auto_investigate: moodengSettings.value.auto_investigate,
    };
    await axios.put(`/api/projects/${projectId.value}/llm/settings`, payload);
    fullMoodengSettings = payload;
    moodengSaveStatus.value = 'saved';
    moodengSaveTimer = setTimeout(() => { moodengSaveStatus.value = 'idle'; }, 2000);
  } catch {
    moodengSaveStatus.value = 'error';
    moodengSaveTimer = setTimeout(() => { moodengSaveStatus.value = 'idle'; }, 4000);
  }
}

let moodengDebounce = null;
watch(moodengSettings, () => {
  if (!moodengSnapshot || !moodengSettingsLoaded.value) return;
  const current = JSON.stringify(moodengSettings.value);
  if (current === moodengSnapshot) return;
  moodengSnapshot = current;
  clearTimeout(moodengDebounce);
  moodengDebounce = setTimeout(saveMoodeng, 800);
}, { deep: true });

// ─── Shared ───
function formatTime(ts) {
  if (!ts) return '';
  const d = new Date(ts);
  return d.toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
}

function formatDate(ts) {
  if (!ts) return '';
  return new Date(ts).toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' });
}

function formatNumber(num) {
  if (num == null) return '0';
  return Number(num).toLocaleString();
}

function formatRelative(ts) {
  if (!ts) return '';
  const d = new Date(ts);
  const now = new Date();
  const diffMin = Math.floor((now - d) / 60000);
  if (diffMin < 1) return 'just now';
  if (diffMin < 60) return `${diffMin}m ago`;
  const diffHrs = Math.floor(diffMin / 60);
  if (diffHrs < 24) return `${diffHrs}h ago`;
  const diffDays = Math.floor(diffHrs / 24);
  if (diffDays < 30) return `${diffDays}d ago`;
  return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
}

// ─── Init ───
watch(activeTab, (tab) => {
  if (tab === 'tools' && tools.value.length === 0) fetchTools();
  if (tab === 'tokens' && agentTokens.value.length === 0) fetchAgentTokens();
  if (tab === 'moodeng' && moodengLoading.value) {
    fetchMoodengSettings();
    fetchIntegrations();
  }
}, { immediate: true });
</script>
