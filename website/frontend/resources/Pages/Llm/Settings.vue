<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-[1200px] mx-auto px-8 py-6">
      <!-- Header -->
      <div class="mb-6">
        <div class="flex items-center gap-3">
          <h1 class="text-2xl font-semibold text-gray-900">Prompt Hub Settings</h1>
          <transition name="fade">
            <span v-if="saveStatus === 'saving'" class="inline-flex items-center gap-1.5 text-xs text-gray-400 dark:text-gray-500">
              <svg class="w-3.5 h-3.5 animate-spin" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4" /><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" /></svg>
              Saving…
            </span>
            <span v-else-if="saveStatus === 'saved'" class="inline-flex items-center gap-1 text-xs text-green-600 dark:text-green-400">
              <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" /></svg>
              Saved
            </span>
            <span v-else-if="saveStatus === 'error'" class="inline-flex items-center gap-1 text-xs text-red-600 dark:text-red-400">
              <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" /></svg>
              Save failed
            </span>
          </transition>
        </div>
        <p class="text-sm text-gray-500 dark:text-gray-400 mt-1">Configure Prompt Hub behavior and limits</p>
      </div>

      <!-- Error Message -->
      <div v-if="errorMessage" class="mb-6 p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg flex items-center justify-between max-w-3xl">
        <div class="flex items-center gap-3">
          <svg class="w-5 h-5 text-red-600 dark:text-red-400 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
          <span class="text-sm text-red-700 dark:text-red-300">{{ errorMessage }}</span>
        </div>
        <button @click="errorMessage = ''" class="text-red-600 dark:text-red-400 hover:text-red-800 dark:hover:text-red-300">
          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      </div>

      <div v-if="loading" class="text-center py-12">
        <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full mx-auto mb-3"></div>
        <p class="text-gray-500 dark:text-gray-400">Loading settings...</p>
      </div>

      <div v-else class="max-w-3xl">
        <!-- Tab bar -->
        <div class="border-b border-gray-200 dark:border-gray-700 mb-6">
          <nav class="-mb-px flex gap-6" aria-label="Settings tabs">
            <button
              v-for="tab in tabs"
              :key="tab.id"
              type="button"
              class="whitespace-nowrap border-b-2 pb-2 text-sm font-medium transition-colors"
              :class="activeTab === tab.id
                ? 'border-primary-600 text-primary-600 dark:text-primary-400 dark:border-primary-400'
                : 'border-transparent text-gray-500 hover:border-gray-300 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200 dark:hover:border-gray-500'"
              @click="activeTab = tab.id"
            >
              {{ tab.label }}
            </button>
          </nav>
        </div>

        <!-- ========== General ========== -->
        <div v-if="activeTab === 'general'" class="space-y-6">

        <!-- Link to Integrations -->
        <BaseCard>
          <div class="flex items-center gap-3 text-sm text-gray-600 dark:text-gray-400">
            <svg class="w-5 h-5 text-primary-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
            <span>
              Manage AI provider API keys on the
              <router-link :to="`/p/${projectId}/llm/integrations`" class="text-primary-600 hover:text-primary-700 dark:text-primary-400 font-medium">
                Integrations page
              </router-link>
            </span>
          </div>
        </BaseCard>

        <!-- Introspection -->
        <BaseCard>
          <template #header>
            <div class="flex items-center justify-between">
              <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">Introspection</h2>
              <label class="relative inline-flex items-center cursor-pointer">
                <input
                  v-model="settings.introspection_enabled"
                  type="checkbox"
                  class="sr-only peer"
                />
                <div class="w-11 h-6 bg-gray-200 dark:bg-gray-700 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-primary-500 rounded-full peer peer-checked:after:translate-x-full rtl:peer-checked:after:-translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-primary-600"></div>
              </label>
            </div>
          </template>
          <div class="space-y-3">
            <p class="text-sm text-gray-600 dark:text-gray-400">
              Capture the AI model's reasoning process (thinking tokens). When enabled, you can see how the AI reached its conclusions.
            </p>
            <div class="bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-lg p-3">
              <p class="text-sm text-yellow-800 dark:text-yellow-200 font-medium mb-1">Additional costs apply:</p>
              <ul class="text-sm text-yellow-700 dark:text-yellow-300 list-disc list-inside space-y-1">
                <li>Anthropic Claude: Thinking tokens billed at output rate</li>
                <li>OpenAI o-series: Reasoning tokens add to total token count</li>
              </ul>
            </div>
            <div v-if="modelCatalog.length" class="text-xs text-gray-500 dark:text-gray-500">
              <p class="font-medium mb-1">Supported models:</p>
              <p>
                <span v-for="(provider, idx) in modelCatalog" :key="provider.id">
                  <span v-if="idx > 0"> | </span>
                  {{ provider.name }}: {{ provider.models.map(m => m.name).join(', ') }}
                </span>
              </p>
            </div>
          </div>
        </BaseCard>

        <!-- Thinking Budget -->
        <BaseCard v-if="settings.introspection_enabled">
          <template #header>
            <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">Thinking Budget</h2>
          </template>
          <div class="space-y-4">
            <p class="text-sm text-gray-600 dark:text-gray-400">
              Maximum tokens for AI thinking per request
            </p>
            <div class="flex items-center gap-4">
              <div class="flex gap-2">
                <label class="flex items-center">
                  <input
                    v-model="settings.thinking_budget_tokens"
                    type="radio"
                    :value="5000"
                    class="form-radio text-primary-600"
                  />
                  <span class="ml-2 text-sm text-gray-700 dark:text-gray-300">Low (5K)</span>
                </label>
                <label class="flex items-center">
                  <input
                    v-model="settings.thinking_budget_tokens"
                    type="radio"
                    :value="10000"
                    class="form-radio text-primary-600"
                  />
                  <span class="ml-2 text-sm text-gray-700 dark:text-gray-300">Medium (10K)</span>
                </label>
                <label class="flex items-center">
                  <input
                    v-model="settings.thinking_budget_tokens"
                    type="radio"
                    :value="50000"
                    class="form-radio text-primary-600"
                  />
                  <span class="ml-2 text-sm text-gray-700 dark:text-gray-300">High (50K)</span>
                </label>
              </div>
            </div>
            <div class="flex items-center gap-2">
              <label class="text-sm text-gray-700 dark:text-gray-300">Custom:</label>
              <input
                v-model.number="settings.thinking_budget_tokens"
                type="number"
                min="1000"
                max="100000"
                class="w-32 px-3 py-1.5 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 text-sm"
              />
              <span class="text-sm text-gray-500">tokens</span>
            </div>
          </div>
        </BaseCard>

        </div><!-- /general -->

        <!-- ========== Routing ========== -->
        <div v-if="activeTab === 'routing'" class="space-y-6">

        <!-- Fallback Behavior -->
        <BaseCard>
          <template #header>
            <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">Fallback Behavior</h2>
          </template>
          <div class="space-y-4">
            <div class="flex items-center justify-between">
              <div>
                <p class="text-sm font-medium text-gray-700 dark:text-gray-300">Enable automatic failover</p>
                <p class="text-xs text-gray-500 dark:text-gray-400">Switch to backup providers on failure</p>
              </div>
              <label class="relative inline-flex items-center cursor-pointer">
                <input v-model="settings.fallback_enabled" type="checkbox" class="sr-only peer" />
                <div class="w-11 h-6 bg-gray-200 dark:bg-gray-700 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-primary-500 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-primary-600"></div>
              </label>
            </div>
            <div class="flex items-center justify-between">
              <div>
                <p class="text-sm font-medium text-gray-700 dark:text-gray-300">Retry failed requests</p>
                <p class="text-xs text-gray-500 dark:text-gray-400">Automatically retry on transient errors</p>
              </div>
              <div class="flex items-center gap-2">
                <label class="relative inline-flex items-center cursor-pointer">
                  <input v-model="settings.retry_enabled" type="checkbox" class="sr-only peer" />
                  <div class="w-11 h-6 bg-gray-200 dark:bg-gray-700 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-primary-500 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-primary-600"></div>
                </label>
              </div>
            </div>
            <div v-if="settings.retry_enabled" class="flex items-center gap-2">
              <label class="text-sm text-gray-700 dark:text-gray-300">Max attempts:</label>
              <input
                v-model.number="settings.retry_max_attempts"
                type="number"
                min="1"
                max="5"
                class="w-20 px-3 py-1.5 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 text-sm"
              />
            </div>
          </div>
        </BaseCard>

        <!-- Default Fallback Models -->
        <BaseCard>
          <template #header>
            <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">Default Fallback Models</h2>
          </template>
          <div class="space-y-3">
            <p class="text-sm text-gray-600 dark:text-gray-400">
              Default fallback chain applied when a request doesn't include its own <code class="text-xs bg-gray-100 dark:bg-gray-700 px-1 rounded">models</code> array.
              Order defines priority. SDK per-request overrides take precedence.
            </p>
            <ul class="space-y-2">
              <li
                v-for="(modelId, index) in settings.default_fallback_models"
                :key="'fb-' + modelId + '-' + index"
                class="flex items-center gap-2 py-2 px-3 rounded-lg bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-600"
              >
                <span class="flex-1 text-sm font-medium text-gray-900 dark:text-gray-100">{{ modelLabelsMap[modelId] || modelId }}</span>
                <div class="flex items-center gap-1">
                  <button type="button" :disabled="index === 0" @click="moveFallbackModel(index, -1)" class="p-1.5 rounded text-gray-500 hover:bg-gray-200 dark:hover:bg-gray-600 disabled:opacity-40 disabled:cursor-not-allowed" title="Move up">
                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 15l7-7 7 7" /></svg>
                  </button>
                  <button type="button" :disabled="index === settings.default_fallback_models.length - 1" @click="moveFallbackModel(index, 1)" class="p-1.5 rounded text-gray-500 hover:bg-gray-200 dark:hover:bg-gray-600 disabled:opacity-40 disabled:cursor-not-allowed" title="Move down">
                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" /></svg>
                  </button>
                  <button type="button" @click="removeFallbackModel(index)" class="p-1.5 rounded text-red-600 hover:bg-red-50 dark:hover:bg-red-900/20" title="Remove">
                    <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" /></svg>
                  </button>
                </div>
              </li>
            </ul>
            <div class="flex gap-2 items-center">
              <select
                v-model="addFallbackModelId"
                class="flex-1 px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 text-sm"
                @change="addFallbackModel"
              >
                <option value="">Add fallback model...</option>
                <optgroup v-for="provider in modelCatalog" :key="provider.id" :label="provider.name">
                  <option v-for="m in provider.models" :key="m.id" :value="m.id">{{ m.name }}</option>
                </optgroup>
              </select>
            </div>
            <p class="text-xs text-gray-500 dark:text-gray-500">
              Max 5 models. These are tried in order if the primary model fails with 5xx, rate limit, or timeout.
            </p>
          </div>
        </BaseCard>

        <!-- Provider Preferences -->
        <BaseCard>
          <template #header>
            <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">Provider Preferences</h2>
          </template>
          <div class="space-y-5">
            <p class="text-sm text-gray-600 dark:text-gray-400">
              Default provider routing preferences. When a model can be served by multiple providers (e.g., Claude via Anthropic or AWS Bedrock), these preferences determine routing. SDK per-request overrides take precedence.
            </p>

            <!-- Sort Strategy -->
            <div>
              <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">Sort Strategy</label>
              <div class="flex gap-3">
                <label class="flex items-center gap-2 p-3 rounded-lg border border-gray-200 dark:border-gray-600 cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-700/50">
                  <input v-model="sortStrategy" type="radio" value="" class="form-radio text-primary-600" />
                  <div>
                    <span class="text-sm font-medium text-gray-900 dark:text-gray-100">Default</span>
                    <p class="text-xs text-gray-500 dark:text-gray-400">Platform ordering</p>
                  </div>
                </label>
                <label class="flex items-center gap-2 p-3 rounded-lg border border-gray-200 dark:border-gray-600 cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-700/50">
                  <input v-model="sortStrategy" type="radio" value="latency" class="form-radio text-primary-600" />
                  <div>
                    <span class="text-sm font-medium text-gray-900 dark:text-gray-100">Latency</span>
                    <p class="text-xs text-gray-500 dark:text-gray-400">Sort by P95 response time</p>
                  </div>
                </label>
              </div>
            </div>

            <!-- Provider Order -->
            <div>
              <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">Provider Order</label>
              <p class="text-xs text-gray-500 dark:text-gray-400 mb-2">
                Preferred order when multiple providers can serve the same model. Providers not listed are tried last.
              </p>
              <ul class="space-y-2 mb-2">
                <li
                  v-for="(providerId, index) in providerOrder"
                  :key="'po-' + providerId"
                  class="flex items-center gap-2 py-2 px-3 rounded-lg bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-600"
                >
                  <span class="flex-1 text-sm font-medium text-gray-900 dark:text-gray-100">{{ KNOWN_PROVIDERS.find(p => p.id === providerId)?.name || providerId }}</span>
                  <div class="flex items-center gap-1">
                    <button type="button" :disabled="index === 0" @click="moveProviderOrder(index, -1)" class="p-1.5 rounded text-gray-500 hover:bg-gray-200 dark:hover:bg-gray-600 disabled:opacity-40 disabled:cursor-not-allowed" title="Move up">
                      <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 15l7-7 7 7" /></svg>
                    </button>
                    <button type="button" :disabled="index === providerOrder.length - 1" @click="moveProviderOrder(index, 1)" class="p-1.5 rounded text-gray-500 hover:bg-gray-200 dark:hover:bg-gray-600 disabled:opacity-40 disabled:cursor-not-allowed" title="Move down">
                      <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" /></svg>
                    </button>
                    <button type="button" @click="removeFromProviderOrder(index)" class="p-1.5 rounded text-red-600 hover:bg-red-50 dark:hover:bg-red-900/20" title="Remove">
                      <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" /></svg>
                    </button>
                  </div>
                </li>
              </ul>
              <select
                class="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 text-sm"
                @change="(e) => { addToProviderOrder(e.target.value); e.target.value = ''; }"
              >
                <option value="">Add provider...</option>
                <option v-for="p in KNOWN_PROVIDERS.filter(p => !providerOrder.includes(p.id))" :key="p.id" :value="p.id">{{ p.name }}</option>
              </select>
            </div>

            <!-- Ignored Providers -->
            <div>
              <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">Ignored Providers</label>
              <p class="text-xs text-gray-500 dark:text-gray-400 mb-2">
                Skip these providers entirely. They will not be used for routing or fallback.
              </p>
              <div class="flex flex-wrap gap-2">
                <label
                  v-for="p in KNOWN_PROVIDERS"
                  :key="'ign-' + p.id"
                  class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-700/50"
                  :class="{ 'bg-red-50 dark:bg-red-900/20 border-red-300 dark:border-red-700': ignoredProviders.includes(p.id) }"
                >
                  <input
                    type="checkbox"
                    :checked="ignoredProviders.includes(p.id)"
                    @change="toggleIgnoredProvider(p.id)"
                    class="form-checkbox text-red-600"
                  />
                  <span class="text-sm text-gray-900 dark:text-gray-100">{{ p.name }}</span>
                </label>
              </div>
            </div>
          </div>
        </BaseCard>

        </div><!-- /routing -->

        <!-- ========== Cost & Limits ========== -->
        <div v-if="activeTab === 'costs'" class="space-y-6">

        <!-- Cost Controls -->
        <BaseCard>
          <template #header>
            <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">Cost Controls</h2>
          </template>
          <div class="space-y-4">
            <div>
              <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                Monthly Budget Limit
              </label>
              <div class="flex items-center gap-3">
                <div class="flex items-center">
                  <span class="text-gray-500 dark:text-gray-400 mr-1">$</span>
                  <input
                    v-model.number="settings.monthly_budget_usd"
                    type="number"
                    min="0"
                    step="10"
                    placeholder="No limit"
                    class="w-32 px-3 py-1.5 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 text-sm"
                  />
                </div>
                <label class="flex items-center text-sm">
                  <input v-model="settings.budget_alert_enabled" type="checkbox" class="form-checkbox text-primary-600 mr-2" />
                  <span class="text-gray-700 dark:text-gray-300">Alert at 80%</span>
                </label>
                <label class="flex items-center text-sm">
                  <input v-model="settings.budget_hard_stop" type="checkbox" class="form-checkbox text-primary-600 mr-2" />
                  <span class="text-gray-700 dark:text-gray-300">Hard stop at 100%</span>
                </label>
              </div>
            </div>
            <div>
              <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                Per-Request Limit
              </label>
              <div class="flex items-center">
                <span class="text-gray-500 dark:text-gray-400 mr-1">$</span>
                <input
                  v-model.number="settings.per_request_limit_usd"
                  type="number"
                  min="0"
                  step="0.5"
                  placeholder="No limit"
                  class="w-32 px-3 py-1.5 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 text-sm"
                />
                <span class="ml-2 text-sm text-gray-500 dark:text-gray-400">maximum cost per request</span>
              </div>
            </div>
          </div>
        </BaseCard>

        <!-- Rate Limiting -->
        <BaseCard>
          <template #header>
            <div class="flex items-center justify-between">
              <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">Rate Limiting</h2>
              <label class="relative inline-flex items-center cursor-pointer">
                <input v-model="settings.rate_limit_enabled" type="checkbox" class="sr-only peer" />
                <div class="w-11 h-6 bg-gray-200 dark:bg-gray-700 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-primary-500 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-primary-600"></div>
              </label>
            </div>
          </template>
          <div v-if="settings.rate_limit_enabled" class="space-y-4">
            <div class="flex items-center gap-4">
              <label class="text-sm text-gray-700 dark:text-gray-300 w-40">Requests per minute:</label>
              <input
                v-model.number="settings.rate_limit_rpm"
                type="number"
                min="1"
                class="w-32 px-3 py-1.5 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 text-sm"
              />
            </div>
          </div>
          <div v-else class="text-sm text-gray-500 dark:text-gray-400">
            Enable rate limiting to control request usage
          </div>
        </BaseCard>

        </div><!-- /costs -->

      </div>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue';
import { useRoute } from 'vue-router';
import axios from 'axios';
import AppLayout from '@/Layouts/AppLayout.vue';
import BaseCard from '@/components/BaseCard.vue';
import { useAuth } from '@/composables/useAuth';

const route = useRoute();
const { user, fetchUser } = useAuth();

const projectId = computed(() => route.params.id);
const project = computed(() => ({ id: projectId.value }));
const loading = ref(true);
const errorMessage = ref('');
const saveStatus = ref('idle'); // 'idle' | 'saving' | 'saved' | 'error'
const activeTab = ref('general');

const tabs = [
  { id: 'general', label: 'General' },
  { id: 'routing', label: 'Routing' },
  { id: 'costs', label: 'Cost & Limits' },
];
let saveStatusTimer = null;

// Helper to extract error message from response
const getErrorMessage = (error, fallback = 'An error occurred') => {
  if (error.response?.data?.error) return error.response.data.error;
  if (error.response?.data?.message) return error.response.data.message;
  if (error.message) return error.message;
  return fallback;
};

const defaultGuardrails = {
  trust_mode: null,
  blocked_input_topics: [],
  max_prompt_tokens: null,
  pii_block_on_detect: false,
  prompt_injection_detection: false,
  spotlighting_enabled: false,
  mask_output_pii: false,
  blocked_output_topics: [],
  min_quality_score: null,
  blocked_tools: [],
  block_exfiltration_urls: false,
};

const defaultSettings = {
  introspection_enabled: false,
  thinking_budget_tokens: 10000,
  fallback_enabled: true,
  retry_enabled: true,
  retry_max_attempts: 3,
  monthly_budget_usd: null,
  budget_alert_enabled: true,
  budget_hard_stop: false,
  per_request_limit_usd: null,
  rate_limit_enabled: false,
  rate_limit_rpm: 60,
  agent_enabled: true,
  agent_scopes: ['project:read', 'llm:read', 'observability:read', 'herd:read'],
  auto_investigate: false,
  judge_sample_rate: null,
  guardrails: { ...defaultGuardrails },
  default_fallback_models: [],
  provider_preferences: null,
  session_profiles: [],
};

const modelCatalog = ref([]);
const modelLabelsMap = computed(() => {
  const map = {};
  for (const provider of modelCatalog.value) {
    for (const m of provider.models) {
      map[m.id] = m.name;
    }
  }
  return map;
});

const settings = ref(JSON.parse(JSON.stringify(defaultSettings)));
const originalSettings = ref(JSON.parse(JSON.stringify(defaultSettings)));
const addFallbackModelId = ref('');

const KNOWN_PROVIDERS = computed(() =>
  modelCatalog.value.map(p => ({ id: p.id, name: p.name }))
);




// --- Default fallback models ---
function addFallbackModel() {
  if (!addFallbackModelId.value) return;
  if (!Array.isArray(settings.value.default_fallback_models)) settings.value.default_fallback_models = [];
  if (settings.value.default_fallback_models.includes(addFallbackModelId.value)) return;
  settings.value.default_fallback_models = [...settings.value.default_fallback_models, addFallbackModelId.value];
  addFallbackModelId.value = '';
}

function removeFallbackModel(index) {
  if (!Array.isArray(settings.value.default_fallback_models)) return;
  settings.value.default_fallback_models = settings.value.default_fallback_models.filter((_, i) => i !== index);
}

function moveFallbackModel(index, delta) {
  const list = settings.value.default_fallback_models;
  if (!Array.isArray(list)) return;
  const next = index + delta;
  if (next < 0 || next >= list.length) return;
  const copy = [...list];
  [copy[index], copy[next]] = [copy[next], copy[index]];
  settings.value.default_fallback_models = copy;
}

// --- Provider preferences ---
function ensureProviderPrefs() {
  if (!settings.value.provider_preferences) {
    settings.value.provider_preferences = { order: null, only: null, ignore: null, allow_fallbacks: null, sort: null };
  }
}

const providerOrder = computed({
  get: () => settings.value.provider_preferences?.order || [],
  set: (val) => {
    ensureProviderPrefs();
    settings.value.provider_preferences.order = val.length ? val : null;
  },
});

const ignoredProviders = computed({
  get: () => settings.value.provider_preferences?.ignore || [],
  set: (val) => {
    ensureProviderPrefs();
    settings.value.provider_preferences.ignore = val.length ? val : null;
  },
});

const sortStrategy = computed({
  get: () => settings.value.provider_preferences?.sort || '',
  set: (val) => {
    ensureProviderPrefs();
    settings.value.provider_preferences.sort = val || null;
  },
});

function moveProviderOrder(index, delta) {
  const list = [...providerOrder.value];
  const next = index + delta;
  if (next < 0 || next >= list.length) return;
  [list[index], list[next]] = [list[next], list[index]];
  providerOrder.value = list;
}

function removeFromProviderOrder(index) {
  const list = providerOrder.value.filter((_, i) => i !== index);
  providerOrder.value = list;
}

function addToProviderOrder(providerId) {
  if (!providerId || providerOrder.value.includes(providerId)) return;
  providerOrder.value = [...providerOrder.value, providerId];
}

function toggleIgnoredProvider(providerId) {
  const current = [...ignoredProviders.value];
  const idx = current.indexOf(providerId);
  if (idx >= 0) {
    current.splice(idx, 1);
  } else {
    current.push(providerId);
  }
  ignoredProviders.value = current;
}

const fetchSettings = async () => {
  loading.value = true;
  errorMessage.value = '';
  try {
    const response = await axios.get(`/api/projects/${projectId.value}/llm/settings`);
    const data = { ...defaultSettings, ...response.data };
    if (!Array.isArray(data.default_fallback_models)) data.default_fallback_models = [];
    if (!Array.isArray(data.agent_scopes)) data.agent_scopes = ['project:read', 'llm:read', 'observability:read', 'herd:read'];
    if (!Array.isArray(data.session_profiles)) data.session_profiles = [];
    data.guardrails = { ...defaultGuardrails, ...(data.guardrails || {}) };
    settings.value = data;
    originalSettings.value = JSON.parse(JSON.stringify(settings.value));
  } catch (error) {
    errorMessage.value = getErrorMessage(error, 'Failed to fetch settings');
    // Use defaults if fetch fails
    settings.value = JSON.parse(JSON.stringify(defaultSettings));
    originalSettings.value = JSON.parse(JSON.stringify(defaultSettings));
  } finally {
    loading.value = false;
  }
};

const saveSettings = async () => {
  clearTimeout(saveStatusTimer);
  saveStatus.value = 'saving';
  errorMessage.value = '';
  try {
    await axios.put(`/api/projects/${projectId.value}/llm/settings`, settings.value);
    originalSettings.value = JSON.parse(JSON.stringify(settings.value));
    saveStatus.value = 'saved';
    saveStatusTimer = setTimeout(() => { saveStatus.value = 'idle'; }, 2000);
  } catch (error) {
    errorMessage.value = getErrorMessage(error, 'Failed to save settings');
    saveStatus.value = 'error';
    saveStatusTimer = setTimeout(() => { saveStatus.value = 'idle'; }, 4000);
  }
};

let debounceTimer = null;
watch(settings, () => {
  if (loading.value) return;
  if (JSON.stringify(settings.value) === JSON.stringify(originalSettings.value)) return;
  clearTimeout(debounceTimer);
  debounceTimer = setTimeout(saveSettings, 800);
}, { deep: true });

const fetchModelCatalog = async () => {
  try {
    const response = await axios.get(`/api/projects/${projectId.value}/llm/settings/models`);
    modelCatalog.value = response.data.providers || [];
  } catch (e) {
    console.warn('Failed to fetch model catalog', e);
  }
};

watch(projectId, () => {
  fetchSettings();
  fetchModelCatalog();
});

onMounted(async () => {
  await fetchUser();
  await Promise.all([fetchSettings(), fetchModelCatalog()]);
});
</script>

<style scoped>
.spinner {
  animation: spin 1s linear infinite;
}

@keyframes spin {
  from { transform: rotate(0deg); }
  to { transform: rotate(360deg); }
}

.fade-enter-active, .fade-leave-active {
  transition: opacity 0.2s ease;
}
.fade-enter-from, .fade-leave-to {
  opacity: 0;
}
</style>
