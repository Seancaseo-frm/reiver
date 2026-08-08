<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <!-- Loading state -->
      <div v-if="loading" class="flex justify-center py-12">
        <div class="animate-spin rounded-full h-8 w-8 border-b-2 border-primary-600"></div>
      </div>

      <div v-else-if="config">
        <!-- Header -->
        <div class="mb-8">
          <nav class="flex mb-4" aria-label="Breadcrumb">
            <ol class="flex items-center space-x-4">
              <li>
                <router-link :to="`/p/${projectId}/llm/prompts`" class="text-gray-400 hover:text-gray-500 dark:hover:text-gray-300">
                  Prompts
                </router-link>
              </li>
              <li>
                <div class="flex items-center">
                  <svg class="flex-shrink-0 h-5 w-5 text-gray-300 dark:text-gray-600" fill="currentColor" viewBox="0 0 20 20">
                    <path fill-rule="evenodd" d="M7.293 14.707a1 1 0 010-1.414L10.586 10 7.293 6.707a1 1 0 011.414-1.414l4 4a1 1 0 010 1.414l-4 4a1 1 0 01-1.414 0z" clip-rule="evenodd" />
                  </svg>
                  <span class="ml-4 text-sm font-medium text-gray-900 dark:text-gray-100">{{ config.name }}</span>
                </div>
              </li>
            </ol>
          </nav>
          <div class="flex justify-between items-start">
            <div>
              <h1 class="text-3xl font-bold text-gray-900 dark:text-gray-100">{{ config.name }}</h1>
              <p class="mt-2 text-gray-600 dark:text-gray-400">{{ config.description || 'No description' }}</p>
            </div>
            <div class="flex items-center space-x-3">
              <button
                @click="triggerCompile"
                class="inline-flex items-center px-4 py-2 border border-gray-300 dark:border-gray-600 text-sm font-medium rounded-md shadow-sm text-gray-700 dark:text-gray-200 bg-white dark:bg-gray-700 hover:bg-gray-50 dark:hover:bg-gray-600 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
              >
                <svg class="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z" />
                </svg>
                Compile
              </button>
              <button
                @click="showVersionEditor = true"
                class="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
              >
                <svg class="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" />
                </svg>
                New Version
              </button>
            </div>
          </div>
        </div>

        <!-- Usage / Integration -->
        <div class="bg-white dark:bg-gray-800 shadow rounded-lg mb-8">
          <div class="px-6 py-4 border-b border-gray-200 dark:border-gray-700">
            <h2 class="text-lg font-medium text-gray-900 dark:text-gray-100">Usage</h2>
          </div>
          <div class="px-6 py-4 space-y-4">
            <p class="text-sm text-gray-600 dark:text-gray-400">
              Reference this prompt in your LLM gateway requests using the prompt name:
            </p>
            <div class="space-y-3">
              <!-- Prompt name with copy -->
              <div>
                <label class="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">Prompt Name</label>
                <div class="flex items-center gap-2">
                  <code class="flex-1 px-3 py-2 bg-gray-50 dark:bg-gray-700 border border-gray-200 dark:border-gray-600 rounded-md text-sm font-mono text-gray-900 dark:text-gray-100">{{ config.name }}</code>
                  <button
                    @click="copyToClipboard(config.name, 'name')"
                    class="inline-flex items-center px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md text-sm font-medium text-gray-700 dark:text-gray-300 bg-white dark:bg-gray-700 hover:bg-gray-50 dark:hover:bg-gray-600"
                  >
                    <svg v-if="copiedField !== 'name'" class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
                    </svg>
                    <svg v-else class="w-4 h-4 text-green-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
                    </svg>
                  </button>
                </div>
              </div>
              <!-- Header example -->
              <div>
                <label class="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">Via HTTP Header</label>
                <div class="flex items-center gap-2">
                  <code class="flex-1 px-3 py-2 bg-gray-50 dark:bg-gray-700 border border-gray-200 dark:border-gray-600 rounded-md text-sm font-mono text-gray-900 dark:text-gray-100">X-Reiver-Prompt-Config: {{ config.name }}</code>
                  <button
                    @click="copyToClipboard(`X-Reiver-Prompt-Config: ${config.name}`, 'header')"
                    class="inline-flex items-center px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md text-sm font-medium text-gray-700 dark:text-gray-300 bg-white dark:bg-gray-700 hover:bg-gray-50 dark:hover:bg-gray-600"
                  >
                    <svg v-if="copiedField !== 'header'" class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
                    </svg>
                    <svg v-else class="w-4 h-4 text-green-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
                    </svg>
                  </button>
                </div>
              </div>
              <!-- Body example -->
              <div>
                <label class="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">Via Request Body (OpenAI SDK)</label>
                <div class="flex items-center gap-2">
                  <code class="flex-1 px-3 py-2 bg-gray-50 dark:bg-gray-700 border border-gray-200 dark:border-gray-600 rounded-md text-sm font-mono text-gray-900 dark:text-gray-100">{{ bodyExample }}</code>
                  <button
                    @click="copyToClipboard(bodyExample, 'body')"
                    class="inline-flex items-center px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md text-sm font-medium text-gray-700 dark:text-gray-300 bg-white dark:bg-gray-700 hover:bg-gray-50 dark:hover:bg-gray-600"
                  >
                    <svg v-if="copiedField !== 'body'" class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
                    </svg>
                    <svg v-else class="w-4 h-4 text-green-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
                    </svg>
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>

        <!-- Active Rollout Banner -->
        <div v-if="activeRollout" class="mb-6 bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-lg p-4">
          <div class="flex items-center justify-between">
            <div class="flex items-center">
              <span class="w-3 h-3 bg-yellow-400 rounded-full animate-pulse mr-3"></span>
              <div>
                <p class="text-sm font-medium text-yellow-800 dark:text-yellow-200">
                  Active Rollout: v{{ activeRollout.baseline_version || '?' }} → v{{ activeRollout.target_version }}
                </p>
                <p class="text-sm text-yellow-700 dark:text-yellow-300">
                  Stage {{ activeRollout.current_stage + 1 }} ({{ activeRollout.current_weight }}% traffic to new version)
                </p>
              </div>
            </div>
            <router-link
              :to="`/p/${projectId}/llm/rollouts/${activeRollout.id}`"
              class="text-sm font-medium text-yellow-800 dark:text-yellow-200 hover:underline"
            >
              View Details →
            </router-link>
          </div>
        </div>

        <!-- Compiled Proposal Card -->
        <div v-if="proposal" class="mb-6 bg-gradient-to-r from-purple-50 to-indigo-50 dark:from-purple-900/20 dark:to-indigo-900/20 border border-purple-200 dark:border-purple-800 rounded-lg overflow-hidden">
          <div class="px-6 py-4 border-b border-purple-200 dark:border-purple-700">
            <div class="flex items-center justify-between">
              <div class="flex items-center">
                <svg class="w-5 h-5 text-purple-600 dark:text-purple-400 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z" />
                </svg>
                <h3 class="text-lg font-medium text-purple-900 dark:text-purple-100">Compiled Proposal</h3>
              </div>
              <span class="text-sm text-purple-600 dark:text-purple-400">{{ formatDate(proposal.created_at) }}</span>
            </div>
          </div>
          <div class="px-6 py-4 space-y-4">
            <div>
              <label class="block text-xs font-medium text-purple-700 dark:text-purple-300 mb-1">Reasoning</label>
              <p class="text-sm text-gray-900 dark:text-gray-100">{{ proposal.reasoning }}</p>
            </div>

            <div v-if="proposal.comparison && Object.keys(proposal.comparison).length" class="grid grid-cols-2 gap-4">
              <div v-if="proposal.comparison.baseline_scores" class="bg-white dark:bg-gray-800 rounded-md p-3 border border-gray-200 dark:border-gray-600">
                <label class="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">Baseline Scores</label>
                <pre class="text-xs text-gray-900 dark:text-gray-100 whitespace-pre-wrap font-mono">{{ formatJsonPretty(proposal.comparison.baseline_scores) }}</pre>
              </div>
              <div v-if="proposal.comparison.candidate_scores" class="bg-white dark:bg-gray-800 rounded-md p-3 border border-gray-200 dark:border-gray-600">
                <label class="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">Compiled Scores</label>
                <pre class="text-xs text-gray-900 dark:text-gray-100 whitespace-pre-wrap font-mono">{{ formatJsonPretty(proposal.comparison.candidate_scores) }}</pre>
              </div>
            </div>

            <div v-if="proposal.system_prompt">
              <button
                @click="showProposalDiff = !showProposalDiff"
                class="flex items-center text-sm font-medium text-purple-700 dark:text-purple-300 hover:text-purple-900 dark:hover:text-purple-100"
              >
                <svg class="w-4 h-4 mr-1 transition-transform" :class="{ 'rotate-90': showProposalDiff }" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
                </svg>
                {{ showProposalDiff ? 'Hide' : 'Show' }} Prompt Diff
              </button>
              <div v-show="showProposalDiff" class="mt-2 grid grid-cols-2 gap-4">
                <div class="bg-white dark:bg-gray-800 rounded-md p-3 border border-gray-200 dark:border-gray-600">
                  <label class="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">Current Prompt</label>
                  <pre class="text-xs text-gray-900 dark:text-gray-100 whitespace-pre-wrap font-mono max-h-64 overflow-y-auto">{{ activeVersion?.system_prompt || '(empty)' }}</pre>
                </div>
                <div class="bg-white dark:bg-gray-800 rounded-md p-3 border border-purple-200 dark:border-purple-600">
                  <label class="block text-xs font-medium text-purple-600 dark:text-purple-400 mb-1">Compiled Prompt</label>
                  <pre class="text-xs text-gray-900 dark:text-gray-100 whitespace-pre-wrap font-mono max-h-64 overflow-y-auto">{{ proposal.system_prompt }}</pre>
                </div>
              </div>
            </div>

            <div class="flex items-center space-x-3 pt-2">
              <button
                @click="acceptProposal"
                :disabled="acceptingProposal"
                class="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-green-600 hover:bg-green-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-green-500 disabled:opacity-50"
              >
                <svg class="w-4 h-4 mr-1.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
                </svg>
                {{ acceptingProposal ? 'Accepting...' : 'Accept' }}
              </button>
              <button
                @click="dismissProposal"
                :disabled="dismissingProposal"
                class="inline-flex items-center px-4 py-2 border border-gray-300 dark:border-gray-600 text-sm font-medium rounded-md shadow-sm text-gray-700 dark:text-gray-200 bg-white dark:bg-gray-700 hover:bg-gray-50 dark:hover:bg-gray-600 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-gray-500 disabled:opacity-50"
              >
                {{ dismissingProposal ? 'Dismissing...' : 'Dismiss' }}
              </button>
              <span class="text-xs text-gray-500 dark:text-gray-400">
                Accepting will create a new version and start a rollout.
              </span>
            </div>
          </div>
        </div>

        <!-- Active Version Card -->
        <div class="bg-white dark:bg-gray-800 shadow rounded-lg mb-8">
          <div class="px-6 py-4 border-b border-gray-200 dark:border-gray-700">
            <h2 class="text-lg font-medium text-gray-900 dark:text-gray-100">Active Version</h2>
          </div>
          <div class="px-6 py-4">
            <div v-if="activeVersion" class="space-y-4">
              <div class="flex items-center justify-between">
                <span class="inline-flex items-center px-3 py-1 rounded-full text-sm font-medium bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400">
                  v{{ activeVersion.version }}
                </span>
                <span class="text-sm text-gray-500 dark:text-gray-400">{{ formatDate(activeVersion.created_at) }}</span>
              </div>
              <div v-if="activeVersion.system_prompt" class="bg-gray-50 dark:bg-gray-700 rounded-md p-4">
                <label class="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-2">System Prompt</label>
                <pre class="text-sm text-gray-900 dark:text-gray-100 whitespace-pre-wrap font-mono">{{ activeVersion.system_prompt }}</pre>
              </div>
              <div class="grid grid-cols-3 gap-4 text-sm">
                <div>
                  <span class="text-gray-500 dark:text-gray-400">Model:</span>
                  <span class="ml-2 text-gray-900 dark:text-gray-100">{{ activeVersion.model || 'Default' }}</span>
                </div>
                <div>
                  <span class="text-gray-500 dark:text-gray-400">Temperature:</span>
                  <span class="ml-2 text-gray-900 dark:text-gray-100">{{ activeVersion.temperature ?? 'Default' }}</span>
                </div>
                <div>
                  <span class="text-gray-500 dark:text-gray-400">Max Tokens:</span>
                  <span class="ml-2 text-gray-900 dark:text-gray-100">{{ activeVersion.max_tokens || 'Default' }}</span>
                </div>
              </div>
            </div>
            <div v-else class="text-center py-6 text-gray-500 dark:text-gray-400">
              No active version. Create one to get started.
            </div>
          </div>
        </div>

        <!-- Version History -->
        <div class="bg-white dark:bg-gray-800 shadow rounded-lg">
          <div class="px-6 py-4 border-b border-gray-200 dark:border-gray-700">
            <h2 class="text-lg font-medium text-gray-900 dark:text-gray-100">Version History</h2>
          </div>
          <div class="divide-y divide-gray-200 dark:divide-gray-700">
            <div
              v-for="version in versions"
              :key="version.id"
            >
              <div class="px-6 py-4 hover:bg-gray-50 dark:hover:bg-gray-800/40 flex items-center justify-between gap-2">
                <button
                  type="button"
                  class="flex items-center space-x-3 min-w-0 flex-1 text-left rounded-md -m-1 p-1 hover:bg-gray-100/80 dark:hover:bg-gray-700/50 focus:outline-none focus:ring-2 focus:ring-primary-500/40"
                  :aria-expanded="isVersionExpanded(version)"
                  :aria-controls="`version-detail-${version.id}`"
                  :id="`version-summary-${version.id}`"
                  @click="toggleVersionDetail(version)"
                >
                  <span
                    class="flex-shrink-0 text-gray-400 dark:text-gray-500 transition-transform"
                    :class="{ 'rotate-90': isVersionExpanded(version) }"
                    aria-hidden="true"
                  >
                    <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
                    </svg>
                  </span>
                  <div class="flex items-center space-x-4 min-w-0 flex-wrap">
                    <span :class="[
                      'inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium flex-shrink-0',
                      version.id === config.active_version_id
                        ? 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400'
                        : 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300'
                    ]">
                      v{{ version.version }}
                    </span>
                    <span class="text-sm text-gray-900 dark:text-gray-100">{{ version.commit_message || 'No commit message' }}</span>
                    <span
                      v-if="allowedToolsAsList(version)?.length"
                      class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200"
                    >
                      {{ allowedToolsAsList(version).length }} tool{{ allowedToolsAsList(version).length === 1 ? '' : 's' }} allowed
                    </span>
                  </div>
                </button>
                <div class="flex items-center space-x-4 flex-shrink-0">
                  <span class="text-sm text-gray-500 dark:text-gray-400">{{ formatDate(version.created_at) }}</span>
                  <button
                    v-if="!activeRollout && version.id !== config.active_version_id"
                    type="button"
                    @click="startRollout(version)"
                    class="text-sm text-primary-600 hover:text-primary-900 dark:text-primary-400 dark:hover:text-primary-300"
                  >
                    Deploy
                  </button>
                </div>
              </div>
              <div
                v-show="isVersionExpanded(version)"
                :id="`version-detail-${version.id}`"
                role="region"
                :aria-labelledby="`version-summary-${version.id}`"
                class="px-6 pb-4 pt-0 -mt-1 border-t border-gray-100 dark:border-gray-700/80 bg-gray-50/80 dark:bg-gray-800/50"
              >
                <div class="pl-8 pr-0 pt-3 space-y-4">
                  <div v-if="version.system_prompt" class="bg-white dark:bg-gray-700/80 rounded-md p-4 border border-gray-100 dark:border-gray-600">
                    <label class="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-2">System Prompt</label>
                    <pre class="text-sm text-gray-900 dark:text-gray-100 whitespace-pre-wrap font-mono max-h-96 overflow-y-auto">{{ version.system_prompt }}</pre>
                  </div>
                  <p v-else class="text-sm text-gray-500 dark:text-gray-400">No system prompt for this version.</p>
                  <div class="grid grid-cols-1 sm:grid-cols-3 gap-4 text-sm">
                    <div>
                      <span class="text-gray-500 dark:text-gray-400">Model:</span>
                      <span class="ml-2 text-gray-900 dark:text-gray-100">{{ version.model || 'Default' }}</span>
                    </div>
                    <div>
                      <span class="text-gray-500 dark:text-gray-400">Temperature:</span>
                      <span class="ml-2 text-gray-900 dark:text-gray-100">{{ version.temperature ?? 'Default' }}</span>
                    </div>
                    <div>
                      <span class="text-gray-500 dark:text-gray-400">Max Tokens:</span>
                      <span class="ml-2 text-gray-900 dark:text-gray-100">{{ version.max_tokens != null && version.max_tokens !== '' ? version.max_tokens : 'Default' }}</span>
                    </div>
                  </div>
                  <div v-if="formatVersionVariables(version)">
                    <label class="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-2">Variables</label>
                    <pre class="text-xs text-gray-900 dark:text-gray-100 whitespace-pre-wrap font-mono bg-white dark:bg-gray-700/80 rounded-md p-3 border border-gray-100 dark:border-gray-600 max-h-48 overflow-y-auto">{{ formatVersionVariables(version) }}</pre>
                  </div>
                  <div v-if="allowedToolsAsList(version)?.length">
                    <span class="text-xs font-medium text-gray-500 dark:text-gray-400">Allowed tools</span>
                    <p class="mt-1 text-sm text-gray-900 dark:text-gray-100">{{ allowedToolsAsList(version).join(', ') }}</p>
                  </div>
                  <div v-if="version.response_format && typeof version.response_format === 'object'">
                    <label class="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-2">Response format (JSON Schema)</label>
                    <pre class="text-xs text-gray-900 dark:text-gray-100 whitespace-pre-wrap font-mono bg-white dark:bg-gray-700/80 rounded-md p-3 border border-gray-100 dark:border-gray-600 max-h-48 overflow-y-auto">{{ formatJsonPretty(version.response_format) }}</pre>
                  </div>
                  <div v-if="outputFailureAction(version)">
                    <span class="text-xs font-medium text-gray-500 dark:text-gray-400">On output schema violation</span>
                    <p class="mt-1 text-sm text-gray-900 dark:text-gray-100 font-mono">{{ outputFailureAction(version) }}</p>
                  </div>
                </div>
              </div>
            </div>
            <div v-if="versions.length === 0" class="px-6 py-8 text-center text-gray-500 dark:text-gray-400">
              No versions yet. Create one to get started.
            </div>
          </div>
        </div>
      </div>

      <!-- Version Editor Modal -->
      <div v-if="showVersionEditor" class="fixed inset-0 z-50 overflow-y-auto" aria-labelledby="modal-title" role="dialog" aria-modal="true">
        <div class="flex items-end justify-center min-h-screen pt-4 px-4 pb-20 text-center sm:block sm:p-0">
          <div class="fixed inset-0 bg-gray-500 dark:bg-gray-900 bg-opacity-75 dark:bg-opacity-75 transition-opacity" @click="showVersionEditor = false"></div>
          <span class="hidden sm:inline-block sm:align-middle sm:h-screen">&#8203;</span>
          <div class="inline-block align-bottom bg-white dark:bg-gray-800 rounded-lg px-4 pt-5 pb-4 text-left overflow-hidden shadow-xl transform transition-all sm:my-8 sm:align-middle sm:max-w-2xl sm:w-full sm:p-6">
            <div>
              <h3 class="text-lg leading-6 font-medium text-gray-900 dark:text-gray-100">Create New Version</h3>
              <div class="mt-4 space-y-4">
                <div>
                  <label class="flex items-center gap-1.5 text-sm font-medium text-gray-700">
                    System Prompt
                    <span class="relative group">
                      <svg class="w-4 h-4 text-gray-400 cursor-help" fill="currentColor" viewBox="0 0 20 20"><path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-8-3a1 1 0 00-.867.5 1 1 0 11-1.731-1A3 3 0 0113 8a3.001 3.001 0 01-2 2.83V11a1 1 0 11-2 0v-1a1 1 0 011-1 1 1 0 100-2zm0 8a1 1 0 100-2 1 1 0 000 2z" clip-rule="evenodd" /></svg>
                      <span class="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 w-64 px-3 py-2 text-xs text-gray-700 bg-white border border-gray-200 rounded-lg shadow-lg opacity-0 invisible group-hover:opacity-100 group-hover:visible transition-all z-50">The instructions given to the model before the user's message. Defines the model's behavior, personality, and constraints.</span>
                    </span>
                  </label>
                  <textarea
                    v-model="versionForm.system_prompt"
                    rows="8"
                    class="mt-1 block w-full border border-gray-300 rounded-md shadow-sm py-2 px-3 bg-white text-gray-900 focus:outline-none focus:ring-primary-500 focus:border-primary-500 sm:text-sm font-mono"
                    placeholder="You are a helpful assistant..."
                  ></textarea>
                  <p v-if="systemPromptError" class="mt-1 text-xs text-red-600">{{ systemPromptError }}</p>
                </div>
                <div class="grid grid-cols-3 gap-4">
                  <div>
                    <label class="flex items-center gap-1.5 text-sm font-medium text-gray-700">
                      Model Override
                      <span class="relative group">
                        <svg class="w-4 h-4 text-gray-400 cursor-help" fill="currentColor" viewBox="0 0 20 20"><path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-8-3a1 1 0 00-.867.5 1 1 0 11-1.731-1A3 3 0 0113 8a3.001 3.001 0 01-2 2.83V11a1 1 0 11-2 0v-1a1 1 0 011-1 1 1 0 100-2zm0 8a1 1 0 100-2 1 1 0 000 2z" clip-rule="evenodd" /></svg>
                        <span class="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 w-64 px-3 py-2 text-xs text-gray-700 bg-white border border-gray-200 rounded-lg shadow-lg opacity-0 invisible group-hover:opacity-100 group-hover:visible transition-all z-50">Override the model used for this prompt version. If left empty, the project's default model is used.</span>
                      </span>
                    </label>
                    <select
                      v-model="versionForm.model"
                      class="mt-1 block w-full border border-gray-300 rounded-md shadow-sm py-2 px-3 bg-white text-gray-900 focus:outline-none focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
                    >
                      <option value="">Default (no override)</option>
                      <optgroup v-for="provider in modelCatalog" :key="provider.id" :label="provider.name">
                        <option v-for="m in provider.models" :key="m.id" :value="m.id">{{ m.name }}</option>
                      </optgroup>
                    </select>
                  </div>
                  <div>
                    <label class="flex items-center gap-1.5 text-sm font-medium text-gray-700">
                      Temperature
                      <span class="relative group">
                        <svg class="w-4 h-4 text-gray-400 cursor-help" fill="currentColor" viewBox="0 0 20 20"><path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-8-3a1 1 0 00-.867.5 1 1 0 11-1.731-1A3 3 0 0113 8a3.001 3.001 0 01-2 2.83V11a1 1 0 11-2 0v-1a1 1 0 011-1 1 1 0 100-2zm0 8a1 1 0 100-2 1 1 0 000 2z" clip-rule="evenodd" /></svg>
                        <span class="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 w-64 px-3 py-2 text-xs text-gray-700 bg-white border border-gray-200 rounded-lg shadow-lg opacity-0 invisible group-hover:opacity-100 group-hover:visible transition-all z-50">Controls randomness. Lower values (0.0) make output more deterministic, higher values (up to 1.0) make it more creative.</span>
                      </span>
                    </label>
                    <input
                      v-model.number="versionForm.temperature"
                      type="number"
                      step="0.1"
                      min="0"
                      max="1"
                      class="mt-1 block w-full border border-gray-300 rounded-md shadow-sm py-2 px-3 bg-white text-gray-900 focus:outline-none focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
                      placeholder="0.5"
                    />
                    <p v-if="temperatureError" class="mt-1 text-xs text-red-600">{{ temperatureError }}</p>
                  </div>
                  <div>
                    <label class="flex items-center gap-1.5 text-sm font-medium text-gray-700">
                      Max Tokens
                      <span class="relative group">
                        <svg class="w-4 h-4 text-gray-400 cursor-help" fill="currentColor" viewBox="0 0 20 20"><path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-8-3a1 1 0 00-.867.5 1 1 0 11-1.731-1A3 3 0 0113 8a3.001 3.001 0 01-2 2.83V11a1 1 0 11-2 0v-1a1 1 0 011-1 1 1 0 100-2zm0 8a1 1 0 100-2 1 1 0 000 2z" clip-rule="evenodd" /></svg>
                        <span class="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 w-64 px-3 py-2 text-xs text-gray-700 bg-white border border-gray-200 rounded-lg shadow-lg opacity-0 invisible group-hover:opacity-100 group-hover:visible transition-all z-50">Maximum number of tokens the model can generate in its response. Leave empty for the model's default limit.</span>
                      </span>
                    </label>
                    <input
                      v-model.number="versionForm.max_tokens"
                      type="number"
                      min="1"
                      max="1000000"
                      class="mt-1 block w-full border border-gray-300 rounded-md shadow-sm py-2 px-3 bg-white text-gray-900 focus:outline-none focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
                      placeholder="4096"
                    />
                    <p v-if="maxTokensError" class="mt-1 text-xs text-red-600">{{ maxTokensError }}</p>
                  </div>
                </div>
                <div>
                  <label class="flex items-center gap-1.5 text-sm font-medium text-gray-700">
                    Variables (optional)
                    <span class="relative group">
                      <svg class="w-4 h-4 text-gray-400 cursor-help" fill="currentColor" viewBox="0 0 20 20"><path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-8-3a1 1 0 00-.867.5 1 1 0 11-1.731-1A3 3 0 0113 8a3.001 3.001 0 01-2 2.83V11a1 1 0 11-2 0v-1a1 1 0 011-1 1 1 0 100-2zm0 8a1 1 0 100-2 1 1 0 000 2z" clip-rule="evenodd" /></svg>
                      <span class="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 w-64 px-3 py-2 text-xs text-gray-700 bg-white border border-gray-200 rounded-lg shadow-lg opacity-0 invisible group-hover:opacity-100 group-hover:visible transition-all z-50">Template variables that can be injected into the system prompt at runtime using {{variable_name}} syntax.</span>
                    </span>
                  </label>
                  <p class="text-xs text-gray-500 mb-1">JSON array of variable definitions for template placeholders like <span v-pre>{{user_name}}</span>. Use <code class="text-xs bg-gray-100 px-1 rounded dark:bg-gray-600">name</code>, <code class="text-xs bg-gray-100 px-1 rounded dark:bg-gray-600">type</code> (alias <code class="text-xs bg-gray-100 px-1 rounded dark:bg-gray-600">var_type</code>; string|number|boolean|enum|json), and optionally <code class="text-xs bg-gray-100 px-1 rounded dark:bg-gray-600">required</code>, <code class="text-xs bg-gray-100 px-1 rounded dark:bg-gray-600">default</code>, <code class="text-xs bg-gray-100 px-1 rounded dark:bg-gray-600">values</code> (enum), <code class="text-xs bg-gray-100 px-1 rounded dark:bg-gray-600">max_chars</code> (string), <code class="text-xs bg-gray-100 px-1 rounded dark:bg-gray-600">min</code>/<code class="text-xs bg-gray-100 px-1 rounded dark:bg-gray-600">max</code> (number). Definitions are validated in the Flow gateway (see product docs).</p>
                  <textarea
                    v-model="versionForm.variables_json"
                    rows="5"
                    class="mt-1 block w-full border border-gray-300 rounded-md shadow-sm py-2 px-3 bg-white text-gray-900 focus:outline-none focus:ring-primary-500 focus:border-primary-500 sm:text-sm font-mono"
                    placeholder='[{"name":"user_name","type":"string"},{"name":"current_date","type":"string"}]'
                  ></textarea>
                  <p v-if="versionFormVariablesError" class="mt-1 text-xs text-red-600">{{ versionFormVariablesError }}</p>
                </div>
                <div>
                  <label class="flex items-center gap-1.5 text-sm font-medium text-gray-700">
                    Allowed Tools (optional)
                    <span class="relative group">
                      <svg class="w-4 h-4 text-gray-400 cursor-help" fill="currentColor" viewBox="0 0 20 20"><path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-8-3a1 1 0 00-.867.5 1 1 0 11-1.731-1A3 3 0 0113 8a3.001 3.001 0 01-2 2.83V11a1 1 0 11-2 0v-1a1 1 0 011-1 1 1 0 100-2zm0 8a1 1 0 100-2 1 1 0 000 2z" clip-rule="evenodd" /></svg>
                      <span class="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 w-72 px-3 py-2 text-xs text-gray-700 bg-white border border-gray-200 rounded-lg shadow-lg opacity-0 invisible group-hover:opacity-100 group-hover:visible transition-all z-50">Comma-separated tool names that this prompt version is allowed to call. Leave empty for no restriction. If set, the gateway will strip any tools not in this list from the request and block unauthorized tool calls in the response.</span>
                    </span>
                  </label>
                  <input
                    v-model="versionForm.allowed_tools"
                    type="text"
                    class="mt-1 block w-full border border-gray-300 rounded-md shadow-sm py-2 px-3 bg-white text-gray-900 focus:outline-none focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
                    placeholder="read_emails, search_emails, get_calendar"
                  />
                </div>
                <div>
                  <label class="flex items-center gap-1.5 text-sm font-medium text-gray-700">
                    Response format (JSON Schema, optional)
                    <span class="relative group">
                      <svg class="w-4 h-4 text-gray-400 cursor-help" fill="currentColor" viewBox="0 0 20 20"><path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-8-3a1 1 0 00-.867.5 1 1 0 11-1.731-1A3 3 0 0113 8a3.001 3.001 0 01-2 2.83V11a1 1 0 11-2 0v-1a1 1 0 011-1 1 1 0 100-2zm0 8a1 1 0 100-2 1 1 0 000 2z" clip-rule="evenodd" /></svg>
                      <span class="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 w-72 px-3 py-2 text-xs text-gray-700 dark:text-gray-200 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-600 rounded-lg shadow-lg opacity-0 invisible group-hover:opacity-100 group-hover:visible transition-all z-50">If set, the gateway validates the assistant message content as JSON against this schema (non-streaming). Leave empty to allow free-form text. See Flow README &quot;Output Contract Enforcement&quot;.</span>
                    </span>
                  </label>
                  <textarea
                    v-model="versionForm.response_format_json"
                    rows="4"
                    class="mt-1 block w-full border border-gray-300 dark:border-gray-600 rounded-md shadow-sm py-2 px-3 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-primary-500 focus:border-primary-500 sm:text-sm font-mono"
                    placeholder='{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"]}'
                  ></textarea>
                  <p v-if="versionFormResponseFormatError" class="mt-1 text-xs text-red-600">{{ versionFormResponseFormatError }}</p>
                </div>
                <div>
                  <label class="flex items-center gap-1.5 text-sm font-medium text-gray-700">On output schema violation (optional)</label>
                  <select
                    v-model="versionForm.output_failure_action"
                    class="mt-1 block w-full border border-gray-300 dark:border-gray-600 rounded-md shadow-sm py-2 px-3 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 sm:text-sm"
                  >
                    <option value="">Default (error — return 422)</option>
                    <option value="error">error</option>
                    <option value="retry">retry (one re-execution)</option>
                    <option value="retry_then_passthrough">retry then passthrough</option>
                    <option value="log_only">log only</option>
                  </select>
                  <p class="mt-1 text-xs text-gray-500">Only applies when a response format schema is set. Empty uses gateway default (<code class="text-xs">error</code>).</p>
                </div>
                <div>
                  <label class="flex items-center gap-1.5 text-sm font-medium text-gray-700">
                    Commit Message <span class="text-red-500">*</span>
                    <span class="relative group">
                      <svg class="w-4 h-4 text-gray-400 cursor-help" fill="currentColor" viewBox="0 0 20 20"><path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-8-3a1 1 0 00-.867.5 1 1 0 11-1.731-1A3 3 0 0113 8a3.001 3.001 0 01-2 2.83V11a1 1 0 11-2 0v-1a1 1 0 011-1 1 1 0 100-2zm0 8a1 1 0 100-2 1 1 0 000 2z" clip-rule="evenodd" /></svg>
                      <span class="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 w-64 px-3 py-2 text-xs text-gray-700 bg-white border border-gray-200 rounded-lg shadow-lg opacity-0 invisible group-hover:opacity-100 group-hover:visible transition-all z-50">A short note describing what changed in this version, for version history tracking.</span>
                    </span>
                  </label>
                  <input
                    v-model="versionForm.commit_message"
                    type="text"
                    class="mt-1 block w-full border border-gray-300 rounded-md shadow-sm py-2 px-3 bg-white text-gray-900 focus:outline-none focus:ring-primary-500 focus:border-primary-500 sm:text-sm"
                    placeholder="What changed in this version?"
                  />
                  <p v-if="commitMessageError" class="mt-1 text-xs text-red-600">{{ commitMessageError }}</p>
                </div>
              </div>
            </div>
            <div class="mt-5 sm:mt-6 sm:grid sm:grid-cols-2 sm:gap-3 sm:grid-flow-row-dense">
              <button
                @click="createVersion"
                :disabled="savingVersion"
                class="w-full inline-flex justify-center rounded-md border border-transparent shadow-sm px-4 py-2 bg-primary-600 text-base font-medium text-white hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 sm:col-start-2 sm:text-sm disabled:opacity-50"
              >
                {{ savingVersion ? 'Saving...' : 'Save Version' }}
              </button>
              <button
                @click="showVersionEditor = false"
                class="mt-3 w-full inline-flex justify-center rounded-md border border-gray-300 dark:border-gray-600 shadow-sm px-4 py-2 bg-white dark:bg-gray-700 text-base font-medium text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-600 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 sm:mt-0 sm:col-start-1 sm:text-sm"
              >
                Cancel
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  </AppLayout>
</template>

<script>
import { ref, onMounted, onUnmounted, computed, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import axios from 'axios';
import AppLayout from '../../../Layouts/AppLayout.vue';
import { useAuth } from '../../../composables/useAuth';
import { usePageContext } from '../../../composables/usePageContext';

export default {
  components: { AppLayout },
  setup() {
    const route = useRoute();
    const router = useRouter();
    const { user } = useAuth();
    const projectId = computed(() => route.params.id);
    const configId = computed(() => route.params.config_id);
    const project = computed(() => (projectId.value ? { id: projectId.value } : null));

    const config = ref(null);
    const versions = ref([]);
    const activeRollout = ref(null);
    const loading = ref(true);
    const showVersionEditor = ref(false);
    const savingVersion = ref(false);
    const selectedVersion = ref(null);
    const copiedField = ref(null);
    const modelCatalog = ref([]);

    const proposal = ref(null);
    const compiling = ref(false);
    const showProposalDiff = ref(false);
    const acceptingProposal = ref(false);
    const dismissingProposal = ref(false);

    const copyToClipboard = async (text, field) => {
      try {
        await navigator.clipboard.writeText(text);
        copiedField.value = field;
        setTimeout(() => { copiedField.value = null; }, 2000);
      } catch {
        // Fallback for insecure contexts
        const ta = document.createElement('textarea');
        ta.value = text;
        ta.style.position = 'fixed';
        ta.style.opacity = '0';
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        document.body.removeChild(ta);
        copiedField.value = field;
        setTimeout(() => { copiedField.value = null; }, 2000);
      }
    };

    const versionForm = ref({
      system_prompt: '',
      model: '',
      temperature: 0.5,
      max_tokens: null,
      variables_json: '',
      response_format_json: '',
      output_failure_action: '',
      commit_message: '',
      allowed_tools: '',
    });
    const versionFormVariablesError = ref('');
    const versionFormResponseFormatError = ref('');
    const systemPromptError = ref('');
    const temperatureError = ref('');
    const maxTokensError = ref('');
    const commitMessageError = ref('');

    const validateVersionForm = () => {
      systemPromptError.value = '';
      temperatureError.value = '';
      maxTokensError.value = '';
      commitMessageError.value = '';
      let valid = true;

      if (!versionForm.value.system_prompt || !versionForm.value.system_prompt.trim()) {
        systemPromptError.value = 'System prompt is required.';
        valid = false;
      }

      const temp = versionForm.value.temperature;
      if (temp == null || temp === '') {
        temperatureError.value = 'Temperature is required.';
        valid = false;
      } else if (typeof temp !== 'number' || isNaN(temp) || temp < 0 || temp > 1) {
        temperatureError.value = 'Temperature must be a number between 0 and 1.';
        valid = false;
      }

      const mt = versionForm.value.max_tokens;
      if (mt != null && mt !== '') {
        if (typeof mt !== 'number' || !Number.isInteger(mt) || mt < 1 || mt > 1_000_000) {
          maxTokensError.value = 'Max tokens must be an integer between 1 and 1,000,000.';
          valid = false;
        }
      }

      if (!versionForm.value.commit_message || !versionForm.value.commit_message.trim()) {
        commitMessageError.value = 'Commit message is required.';
        valid = false;
      }

      return valid;
    };

    const activeVersion = computed(() => {
      if (!config.value?.active_version_id) return null;
      return versions.value.find(v => v.id === config.value.active_version_id);
    });

    const bodyExample = computed(() => {
      if (!config.value) return '';
      return `extra_body={"prompt_config": "${config.value.name}"}`;
    });

    const fetchData = async () => {
      loading.value = true;
      try {
        const [configRes, versionsRes, rolloutsRes] = await Promise.all([
          axios.get(`/api/llm/prompts/configs/${configId.value}?project_id=${projectId.value}`),
          axios.get(`/api/llm/prompts/configs/${configId.value}/versions?project_id=${projectId.value}`),
          axios.get(`/api/llm/prompts/rollouts?project_id=${projectId.value}&config_id=${configId.value}&status=running`),
        ]);
        config.value = configRes.data;
        versions.value = versionsRes.data;
        activeRollout.value = rolloutsRes.data.find(r => r.status === 'running') || null;

        await fetchProposals();

        // Pre-fill version editor with active version content
        if (activeVersion.value) {
          const av = activeVersion.value;
          versionForm.value.system_prompt = av.system_prompt || '';
          versionForm.value.model = av.model || '';
          versionForm.value.temperature = parseFloat(Number(av.temperature ?? 0.5).toFixed(1));
          versionForm.value.max_tokens = av.max_tokens;
          versionForm.value.variables_json = av.variables && typeof av.variables !== 'string'
            ? JSON.stringify(av.variables, null, 2)
            : (av.variables || '');
          versionForm.value.response_format_json = av.response_format
            ? JSON.stringify(av.response_format, null, 2)
            : '';
          const action = av.parameters && typeof av.parameters === 'object' && av.parameters !== null
            ? av.parameters.output_failure_action
            : null;
          versionForm.value.output_failure_action = typeof action === 'string' ? action : '';
          if (av.allowed_tools && Array.isArray(av.allowed_tools)) {
            versionForm.value.allowed_tools = av.allowed_tools.join(', ');
          }
        }
      } catch (error) {
        console.error('Failed to fetch data:', error);
      } finally {
        loading.value = false;
      }
    };

    const fetchModelCatalog = async () => {
      try {
        const { data } = await axios.get(`/api/projects/${projectId.value}/llm/settings/models`);
        modelCatalog.value = data.providers || [];
      } catch (e) {
        console.warn('Failed to fetch model catalog', e);
      }
    };

    const createVersion = async () => {
      if (!validateVersionForm()) return;

      versionFormVariablesError.value = '';
      versionFormResponseFormatError.value = '';
      let variables = null;
      const raw = (versionForm.value.variables_json || '').trim();
      if (raw) {
        try {
          variables = JSON.parse(raw);
          if (!Array.isArray(variables)) {
            versionFormVariablesError.value = 'Variables must be a JSON array.';
            return;
          }
        } catch (e) {
          versionFormVariablesError.value = 'Invalid JSON: ' + (e.message || 'parse error');
          return;
        }
      }
      let responseFormat = null;
      const rawRf = (versionForm.value.response_format_json || '').trim();
      if (rawRf) {
        try {
          responseFormat = JSON.parse(rawRf);
          if (responseFormat === null || typeof responseFormat !== 'object' || Array.isArray(responseFormat)) {
            versionFormResponseFormatError.value = 'Response format must be a JSON object (schema).';
            return;
          }
        } catch (e) {
          versionFormResponseFormatError.value = 'Invalid response format JSON: ' + (e.message || 'parse error');
          return;
        }
      }
      const action = (versionForm.value.output_failure_action || '').trim();
      const parameters = action ? { output_failure_action: action } : null;
      savingVersion.value = true;
      try {
        const allowedToolsRaw = versionForm.value.allowed_tools?.trim();
        const allowedTools = allowedToolsRaw
          ? allowedToolsRaw.split(',').map(t => t.trim()).filter(Boolean)
          : null;

        const payload = {
          project_id: projectId.value,
          system_prompt: versionForm.value.system_prompt || null,
          model: versionForm.value.model || null,
          temperature: versionForm.value.temperature,
          max_tokens: versionForm.value.max_tokens,
          variables: variables,
          response_format: responseFormat,
          parameters: parameters,
          commit_message: versionForm.value.commit_message,
          allowed_tools: allowedTools,
        };

        await axios.post(`/api/llm/prompts/configs/${configId.value}/versions`, payload);

        showVersionEditor.value = false;
        versionForm.value = { system_prompt: '', model: '', temperature: 0.5, max_tokens: null, variables_json: '', response_format_json: '', output_failure_action: '', commit_message: '', allowed_tools: '' };
        await fetchData();
      } catch (error) {
        console.error('Failed to create version:', error);
        alert(error.response?.data?.message || 'Failed to create version');
      } finally {
        savingVersion.value = false;
      }
    };

    const toggleVersionDetail = (version) => {
      selectedVersion.value = selectedVersion.value?.id === version.id ? null : version;
    };

    const isVersionExpanded = (version) => selectedVersion.value?.id === version.id;

    const formatVersionVariables = (v) => {
      const raw = v?.variables;
      if (raw == null) return null;
      if (Array.isArray(raw) && raw.length === 0) return null;
      if (typeof raw === 'object' && !Array.isArray(raw) && Object.keys(raw).length === 0) {
        return null;
      }
      try {
        return JSON.stringify(typeof raw === 'string' ? JSON.parse(raw) : raw, null, 2);
      } catch {
        return String(raw);
      }
    };

    const formatJsonPretty = (v) => {
      try {
        return JSON.stringify(typeof v === 'string' ? JSON.parse(v) : v, null, 2);
      } catch {
        return String(v);
      }
    };

    const outputFailureAction = (version) => {
      const p = version?.parameters;
      if (p == null || typeof p !== 'object') return null;
      const a = p.output_failure_action;
      return typeof a === 'string' && a ? a : null;
    };

    const allowedToolsAsList = (version) => {
      const t = version?.allowed_tools;
      if (t == null) return null;
      if (Array.isArray(t)) return t.length ? t : null;
      if (typeof t === 'string') {
        const s = t.trim();
        if (!s) return null;
        try {
          const parsed = JSON.parse(s);
          return Array.isArray(parsed) && parsed.length ? parsed : null;
        } catch {
          return null;
        }
      }
      return null;
    };

    const startRollout = async (version) => {
      try {
        const rolloutRes = await axios.post('/api/llm/prompts/rollouts', {
          project_id: projectId.value,
          config_id: configId.value,
          target_version_id: version.id,
          mode: 'auto',
        });
        // API returns RolloutWithStages with flattened rollout fields, so id is at data.id
        const rolloutId = rolloutRes.data.id ?? rolloutRes.data.rollout?.id;
        if (!rolloutId) {
          throw new Error('Invalid rollout response: missing id');
        }
        await axios.post(`/api/llm/prompts/rollouts/${rolloutId}/start`, {
          project_id: projectId.value,
        });
        router.push(`/p/${projectId.value}/llm/rollouts/${rolloutId}`);
      } catch (error) {
        console.error('Failed to start rollout:', error);
        alert(error.response?.data?.message || error.message || 'Failed to start rollout');
      }
    };

    const fetchProposals = async () => {
      try {
        const { data } = await axios.get(
          `/api/llm/prompts/configs/${configId.value}/proposals?project_id=${projectId.value}`
        );
        proposal.value = data && data.length > 0 ? data[0] : null;
      } catch {
        proposal.value = null;
      }
    };

    const triggerCompile = () => {
      router.push({
        path: `/p/${projectId.value}/llm/compiler`,
        query: { config: configId.value },
      });
    };

    const acceptProposal = async () => {
      if (!proposal.value) return;
      if (!confirm('This will create a new prompt version and start a rollout. Continue?')) return;
      acceptingProposal.value = true;
      try {
        await axios.post(`/api/llm/prompts/proposals/${proposal.value.id}/accept`, {
          project_id: projectId.value,
        });
        proposal.value = null;
        await fetchData();
      } catch (error) {
        console.error('Failed to accept proposal:', error);
        alert(error.response?.data?.message || 'Failed to accept proposal');
      } finally {
        acceptingProposal.value = false;
      }
    };

    const dismissProposal = async () => {
      if (!proposal.value) return;
      dismissingProposal.value = true;
      try {
        await axios.post(`/api/llm/prompts/proposals/${proposal.value.id}/dismiss`, {
          project_id: projectId.value,
        });
        proposal.value = null;
      } catch (error) {
        console.error('Failed to dismiss proposal:', error);
        alert(error.response?.data?.message || 'Failed to dismiss proposal');
      } finally {
        dismissingProposal.value = false;
      }
    };

    const formatDate = (dateString) => {
      const date = new Date(dateString);
      const now = new Date();
      const diffMs = now - date;
      const diffMins = Math.floor(diffMs / 60000);
      const diffHours = Math.floor(diffMs / 3600000);
      const diffDays = Math.floor(diffMs / 86400000);

      if (diffMins < 60) return `${diffMins}m ago`;
      if (diffHours < 24) return `${diffHours}h ago`;
      if (diffDays < 7) return `${diffDays}d ago`;
      return date.toLocaleDateString();
    };

    const { setPageSnapshot, clearPageSnapshot } = usePageContext();

    watch([config, activeVersion, activeRollout], () => {
      if (!config.value) return;
      const av = activeVersion.value;
      setPageSnapshot({
        page: 'Prompt Config Detail',
        prompt_name: config.value.name,
        description: config.value.description || undefined,
        usage: {
          header: `X-Reiver-Prompt-Config: ${config.value.name}`,
          body_field: `"prompt_config": "${config.value.name}"`,
          openai_sdk: `extra_body={"prompt_config": "${config.value.name}"}`,
        },
        active_version: av ? {
          version: av.version,
          model: av.model,
          temperature: av.temperature,
          max_tokens: av.max_tokens,
        } : undefined,
        rollout: activeRollout.value ? {
          status: activeRollout.value.rollout?.status,
          baseline_version: activeRollout.value.baseline_version,
          target_version: activeRollout.value.target_version,
          current_weight: activeRollout.value.rollout?.current_weight,
        } : undefined,
        total_versions: versions.value.length,
      });
    }, { deep: true });

    onMounted(() => { fetchData(); fetchModelCatalog(); });
    watch([projectId, configId], () => { fetchData(); fetchModelCatalog(); });
    onUnmounted(() => clearPageSnapshot());

    return {
      user,
      project,
      projectId,
      config,
      versions,
      activeVersion,
      activeRollout,
      bodyExample,
      loading,
      showVersionEditor,
      savingVersion,
      selectedVersion,
      versionForm,
      versionFormVariablesError,
      versionFormResponseFormatError,
      systemPromptError,
      temperatureError,
      maxTokensError,
      copiedField,
      modelCatalog,
      proposal,
      compiling,
      showProposalDiff,
      acceptingProposal,
      dismissingProposal,
      copyToClipboard,
      createVersion,
      startRollout,
      triggerCompile,
      acceptProposal,
      dismissProposal,
      formatDate,
      toggleVersionDetail,
      isVersionExpanded,
      formatVersionVariables,
      formatJsonPretty,
      outputFailureAction,
      allowedToolsAsList,
    };
  },
};
</script>
