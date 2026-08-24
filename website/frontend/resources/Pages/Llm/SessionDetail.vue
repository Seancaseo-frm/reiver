<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-[1200px] mx-auto px-8 py-6">
      <!-- Header -->
      <div class="mb-6">
        <div class="flex items-center justify-between">
          <div class="flex items-center gap-3">
            <router-link
              :to="`/p/${projectId}/llm/sessions`"
              class="p-2 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-800 transition-colors"
            >
              <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7" />
              </svg>
            </router-link>
            <div>
              <h1 class="text-2xl font-semibold text-gray-900 dark:text-gray-100">
                {{ session.session_name || 'Session Detail' }}
              </h1>
              <p class="text-sm text-gray-500 dark:text-gray-400 mt-1 font-mono">{{ sessionId }}</p>
            </div>
          </div>
          <button
            v-if="hasContent && !loading"
            @click="openReplay"
            class="inline-flex items-center gap-2 px-4 py-2 text-sm font-medium rounded-lg transition-colors bg-primary-600 text-white hover:bg-primary-700"
          >
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z" />
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
            Replay
          </button>
        </div>
      </div>

      <div v-if="loading" class="text-center py-12">
        <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full mx-auto mb-3"></div>
        <p class="text-gray-500 dark:text-gray-400">Loading session...</p>
      </div>

      <div v-else-if="!session.session_id" class="text-center py-12">
        <p class="text-gray-500 dark:text-gray-400">Session not found.</p>
      </div>

      <div v-else class="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <!-- Main: Requests table + tabs -->
        <div class="lg:col-span-2 space-y-6">
          <!-- Requests -->
          <BaseCard>
            <template #header>
              <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">
                Requests ({{ session.request_count }})
              </h2>
            </template>
            <div v-if="requestsLoading" class="text-center py-8">
              <div class="spinner w-6 h-6 border-3 border-primary-600 border-t-transparent rounded-full mx-auto mb-2"></div>
              <p class="text-sm text-gray-500 dark:text-gray-400">Loading requests...</p>
            </div>
            <div v-else-if="requests.length === 0" class="text-center py-8 text-gray-500 dark:text-gray-400 text-sm">
              No individual requests found.
            </div>
            <div v-else class="overflow-x-auto">
              <table class="w-full">
                <thead>
                  <tr class="border-b border-gray-200 dark:border-gray-700">
                    <th class="text-left py-2 px-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Model</th>
                    <th class="text-right py-2 px-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Tokens</th>
                    <th class="text-right py-2 px-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Cost</th>
                    <th class="text-right py-2 px-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Latency</th>
                    <th v-if="creditsEnabled" class="text-center py-2 px-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Key</th>
                    <th class="text-center py-2 px-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Status</th>
                    <th class="text-left py-2 px-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Time</th>
                  </tr>
                </thead>
                <tbody>
                  <tr
                    v-for="req in requests"
                    :key="req.request_id"
                    class="border-b border-gray-100 dark:border-gray-800 hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors"
                  >
                    <td class="py-2.5 px-3">
                      <div class="flex flex-col">
                        <span class="text-sm text-gray-900 dark:text-gray-100">{{ req.gen_ai_request_model }}</span>
                        <span class="text-xs text-gray-400">{{ req.gen_ai_system }}</span>
                      </div>
                    </td>
                    <td class="py-2.5 px-3 text-right">
                      <span class="text-sm tabular-nums text-gray-600 dark:text-gray-400">
                        {{ formatNumber(req.input_tokens + req.output_tokens) }}
                      </span>
                    </td>
                    <td class="py-2.5 px-3 text-right">
                      <span class="text-sm tabular-nums text-gray-600 dark:text-gray-400">${{ formatCost(req.cost_usd) }}</span>
                    </td>
                    <td class="py-2.5 px-3 text-right">
                      <span class="text-sm tabular-nums text-gray-600 dark:text-gray-400">{{ req.duration_ms }}ms</span>
                    </td>
                    <td v-if="creditsEnabled" class="py-2.5 px-3 text-center">
                      <span
                        class="inline-flex items-center px-1.5 py-0.5 text-[10px] font-medium rounded"
                        :class="req.is_platform_key
                          ? 'bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300'
                          : 'bg-gray-100 text-gray-600 dark:bg-gray-700 dark:text-gray-300'"
                      >{{ req.is_platform_key ? 'Platform' : 'BYOK' }}</span>
                    </td>
                    <td class="py-2.5 px-3">
                      <div class="flex items-center justify-center gap-1.5 flex-wrap">
                        <span
                          class="inline-block w-2 h-2 rounded-full flex-shrink-0"
                          :class="req.status_code === 'error' ? 'bg-red-500' : 'bg-green-500'"
                          :title="req.status_code"
                        ></span>
                        <span
                          v-for="rule in (req.guardrail_violations || [])"
                          :key="rule"
                          class="inline-flex items-center gap-1 px-1.5 py-0.5 text-[10px] font-medium rounded bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-300"
                          :title="rule"
                        >
                          <svg class="w-2.5 h-2.5 flex-shrink-0" fill="currentColor" viewBox="0 0 20 20"><path fill-rule="evenodd" d="M10 1.944A11.954 11.954 0 012.166 5C2.056 5.649 2 6.319 2 7c0 5.225 3.34 9.67 8 11.317C14.66 16.67 18 12.225 18 7c0-.682-.057-1.35-.166-2A11.954 11.954 0 0110 1.944zM11 14a1 1 0 11-2 0 1 1 0 012 0zm0-7a1 1 0 10-2 0v3a1 1 0 102 0V7z" clip-rule="evenodd"/></svg>
                          {{ formatRuleName(rule) }}
                        </span>
                      </div>
                    </td>
                    <td class="py-2.5 px-3">
                      <span class="text-sm text-gray-600 dark:text-gray-400">{{ formatTime(req.timestamp) }}</span>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </BaseCard>
        </div>

        <!-- Sidebar -->
        <div class="space-y-6">
          <BaseCard>
            <template #header>
              <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">Overview</h2>
            </template>
            <div class="space-y-3 text-sm">
              <div v-if="session.user_id" class="flex justify-between">
                <span class="text-gray-500 dark:text-gray-400">User</span>
                <router-link :to="`/p/${projectId}/llm/users/${encodeURIComponent(session.user_id)}`" class="text-primary-600 hover:underline font-mono text-xs break-all text-right ml-4">{{ session.user_id }}</router-link>
              </div>
              <div class="flex justify-between">
                <span class="text-gray-500 dark:text-gray-400">Models</span>
                <span class="text-gray-900 dark:text-gray-100">{{ (session.models || []).join(', ') || '—' }}</span>
              </div>
              <div class="flex justify-between">
                <span class="text-gray-500 dark:text-gray-400">Requests</span>
                <span class="text-gray-900 dark:text-gray-100 tabular-nums">{{ session.request_count }}</span>
              </div>
              <div class="flex justify-between">
                <span class="text-gray-500 dark:text-gray-400">Errors</span>
                <span :class="session.error_count > 0 ? 'text-red-600 dark:text-red-400' : 'text-gray-900 dark:text-gray-100'" class="tabular-nums">{{ session.error_count }}</span>
              </div>
              <div v-if="session.guardrail_count > 0" class="flex justify-between">
                <span class="text-gray-500 dark:text-gray-400 flex items-center gap-1">
                  <svg class="w-3.5 h-3.5" fill="currentColor" viewBox="0 0 20 20"><path fill-rule="evenodd" d="M10 1.944A11.954 11.954 0 012.166 5C2.056 5.649 2 6.319 2 7c0 5.225 3.34 9.67 8 11.317C14.66 16.67 18 12.225 18 7c0-.682-.057-1.35-.166-2A11.954 11.954 0 0110 1.944zM11 14a1 1 0 11-2 0 1 1 0 012 0zm0-7a1 1 0 10-2 0v3a1 1 0 102 0V7z" clip-rule="evenodd"/></svg>
                  Guardrail Blocks
                </span>
                <span class="text-red-600 dark:text-red-400 tabular-nums">{{ session.guardrail_count }}</span>
              </div>
              <div class="flex justify-between">
                <span class="text-gray-500 dark:text-gray-400">Avg Latency</span>
                <span class="text-gray-900 dark:text-gray-100 tabular-nums">{{ Math.round(session.avg_latency_ms || 0) }}ms</span>
              </div>
              <div class="flex justify-between">
                <span class="text-gray-500 dark:text-gray-400">First Request</span>
                <span class="text-gray-900 dark:text-gray-100">{{ formatTimeFull(session.first_request_time) }}</span>
              </div>
              <div class="flex justify-between">
                <span class="text-gray-500 dark:text-gray-400">Last Request</span>
                <span class="text-gray-900 dark:text-gray-100">{{ formatTimeFull(session.last_request_time) }}</span>
              </div>
            </div>
          </BaseCard>

          <BaseCard>
            <template #header>
              <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">Token Usage</h2>
            </template>
            <div class="space-y-3 text-sm">
              <div class="flex justify-between">
                <span class="text-gray-500 dark:text-gray-400">Input Tokens</span>
                <span class="text-gray-900 dark:text-gray-100 tabular-nums">{{ formatNumber(session.total_input_tokens) }}</span>
              </div>
              <div class="flex justify-between">
                <span class="text-gray-500 dark:text-gray-400">Output Tokens</span>
                <span class="text-gray-900 dark:text-gray-100 tabular-nums">{{ formatNumber(session.total_output_tokens) }}</span>
              </div>
              <div class="flex justify-between pt-2 border-t border-gray-200 dark:border-gray-700">
                <span class="text-gray-500 dark:text-gray-400 font-medium">Total</span>
                <span class="text-gray-900 dark:text-gray-100 font-medium tabular-nums">{{ formatNumber((session.total_input_tokens || 0) + (session.total_output_tokens || 0)) }}</span>
              </div>
            </div>
          </BaseCard>

          <BaseCard>
            <template #header>
              <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">Cost</h2>
            </template>
            <div class="text-center py-2">
              <span class="text-2xl font-semibold text-gray-900 dark:text-gray-100 tabular-nums">${{ formatCost(session.total_cost_usd) }}</span>
            </div>
          </BaseCard>

          <BaseCard>
            <template #header>
              <h2 class="text-lg font-semibold text-gray-900 dark:text-gray-100">Feedback</h2>
            </template>
            <div class="space-y-3">
              <div class="flex items-center gap-3">
                <button
                  class="flex-1 py-2 rounded-lg border text-sm font-medium transition-colors"
                  :class="session.feedback_score === 1
                    ? 'bg-green-100 border-green-300 text-green-800 dark:bg-green-900/40 dark:border-green-700 dark:text-green-300'
                    : 'border-gray-300 dark:border-gray-600 text-gray-600 dark:text-gray-400 hover:bg-gray-50 dark:hover:bg-gray-700'"
                  @click="submitFeedback(1)"
                >Good</button>
                <button
                  class="flex-1 py-2 rounded-lg border text-sm font-medium transition-colors"
                  :class="session.feedback_score === -1
                    ? 'bg-red-100 border-red-300 text-red-800 dark:bg-red-900/40 dark:border-red-700 dark:text-red-300'
                    : 'border-gray-300 dark:border-gray-600 text-gray-600 dark:text-gray-400 hover:bg-gray-50 dark:hover:bg-gray-700'"
                  @click="submitFeedback(-1)"
                >Bad</button>
              </div>
              <div v-if="session.feedback_text" class="text-sm text-gray-600 dark:text-gray-400 bg-gray-50 dark:bg-gray-800 rounded-lg p-3">
                {{ session.feedback_text }}
              </div>
            </div>
          </BaseCard>
        </div>
      </div>
    </div>

    <!-- Replay Overlay -->
    <Teleport to="body">
      <Transition name="replay-overlay">
        <div
          v-if="replayOpen"
          class="fixed inset-0 z-50 flex flex-col bg-gray-950/80 backdrop-blur-sm"
          @keydown.escape="closeReplay"
          @keydown.left="prevStep"
          @keydown.right="nextStep"
          tabindex="0"
          ref="overlayEl"
        >
          <!-- Top bar -->
          <div class="flex-none flex items-center justify-between px-6 py-4 border-b border-white/10">
            <div class="flex items-center gap-4">
              <h2 class="text-white font-semibold text-lg">Session Replay</h2>
              <span class="text-sm text-gray-400 tabular-nums">
                Step {{ currentStep + 1 }} of {{ replayRequests.length }}
              </span>
            </div>

            <!-- Step metadata pills -->
            <div v-if="currentReq" class="flex items-center gap-3">
              <span class="replay-pill">{{ currentReq.gen_ai_request_model }}</span>
              <span class="replay-pill tabular-nums">{{ currentReq.duration_ms }}ms</span>
              <span class="replay-pill tabular-nums">{{ formatNumber(currentReq.input_tokens) }} in / {{ formatNumber(currentReq.output_tokens) }} out</span>
              <span class="replay-pill tabular-nums">${{ formatCost(currentReq.cost_usd) }}</span>
              <span
                class="inline-block w-2 h-2 rounded-full"
                :class="currentReq.status_code === 'error' ? 'bg-red-500' : 'bg-brand-500'"
              ></span>
            </div>

            <button
              @click="closeReplay"
              class="p-2 text-gray-400 hover:text-white rounded-lg hover:bg-white/10 transition-colors"
            >
              <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>

          <!-- Main content area with side arrows -->
          <div class="flex-1 flex items-stretch overflow-hidden relative">
            <!-- Left arrow -->
            <button
              @click="prevStep"
              :disabled="currentStep === 0"
              class="replay-nav-arrow left-0"
              :class="currentStep === 0 ? 'opacity-20 cursor-default' : 'opacity-60 hover:opacity-100'"
            >
              <svg class="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7" />
              </svg>
            </button>

            <!-- Conversation card -->
            <div ref="replayScrollEl" class="flex-1 flex items-start justify-center overflow-y-auto py-8 px-20">
              <div v-if="currentReq" class="w-full max-w-3xl space-y-4">
                <!-- Messages for this turn -->
                <template v-for="(msg, mIdx) in currentMessages" :key="mIdx">
                  <!-- User message -->
                  <div v-if="msg.role === 'user'" class="flex justify-end">
                    <div class="max-w-[85%] rounded-2xl rounded-tr-md px-5 py-3 bg-primary-600 text-white">
                      <div class="text-sm whitespace-pre-wrap break-words leading-relaxed">{{ getMessageContent(msg) }}</div>
                    </div>
                  </div>

                  <!-- System message -->
                  <div v-else-if="msg.role === 'system'" class="flex justify-center">
                    <div class="max-w-[90%] rounded-xl px-4 py-2.5 bg-white/5 border border-white/10">
                      <div class="text-[11px] font-medium text-blue-400 uppercase tracking-wider mb-1">system</div>
                      <div class="text-sm text-gray-300 whitespace-pre-wrap break-words leading-relaxed">{{ getMessageContent(msg) }}</div>
                    </div>
                  </div>

                  <!-- Previous assistant context (from earlier turns) -->
                  <div v-else-if="msg.role === 'assistant'" class="flex justify-start">
                    <div class="max-w-[85%] rounded-2xl rounded-tl-md px-5 py-3 bg-white/5 border border-white/10">
                      <div class="text-sm text-gray-400 whitespace-pre-wrap break-words leading-relaxed">{{ getMessageContent(msg) }}</div>
                    </div>
                  </div>

                  <!-- Tool call / result -->
                  <div v-else-if="msg.role === 'tool'" class="flex justify-start">
                    <div class="max-w-[85%] rounded-xl px-4 py-2.5 bg-amber-500/10 border border-amber-500/20">
                      <div class="text-[11px] font-medium text-amber-400 uppercase tracking-wider mb-1">tool</div>
                      <div class="text-sm text-gray-300 font-mono whitespace-pre-wrap break-words leading-relaxed">{{ getMessageContent(msg) }}</div>
                    </div>
                  </div>
                </template>

                <!-- This turn's response -->
                <div class="flex justify-start">
                  <div class="max-w-[85%] rounded-2xl rounded-tl-md px-5 py-3 bg-white/[0.08] border border-white/10">
                    <div v-if="currentReq.response_content" class="text-sm text-gray-100 whitespace-pre-wrap break-words leading-relaxed">{{ currentReq.response_content }}</div>
                    <div v-else class="text-sm text-gray-500 italic">No response content</div>
                  </div>
                </div>

                <!-- Diff view when a fork result exists -->
                <div v-if="stepResults[currentStep]" class="mt-6">
                  <div class="rounded-xl border border-purple-500/30 overflow-hidden">
                    <div class="bg-purple-500/10 px-5 py-2.5 flex items-center justify-between">
                      <span class="text-xs font-semibold text-purple-300">
                        Replayed with {{ stepResults[currentStep].model }}
                        <span v-if="stepResults[currentStep].duration_ms" class="font-normal ml-2 tabular-nums text-purple-400">
                          {{ stepResults[currentStep].duration_ms }}ms · {{ formatNumber(stepResults[currentStep].input_tokens + stepResults[currentStep].output_tokens) }} tok
                        </span>
                      </span>
                      <div class="flex items-center gap-2">
                        <span v-if="stepResults[currentStep].error" class="text-xs text-red-400">Error</span>
                        <button
                          class="text-xs text-gray-500 hover:text-gray-300 transition-colors"
                          @click="clearStepResult(currentStep)"
                          title="Clear"
                        >
                          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" /></svg>
                        </button>
                      </div>
                    </div>
                    <div v-if="stepResults[currentStep].error" class="px-5 py-3 text-sm text-red-400">
                      {{ stepResults[currentStep].error }}
                    </div>
                    <div v-else class="diff-view px-5 py-3 text-sm font-mono leading-relaxed overflow-x-auto">
                      <div
                        v-for="(line, lIdx) in computeDiff(currentReq.response_content || '', stepResults[currentStep].response_content || '')"
                        :key="lIdx"
                        class="whitespace-pre-wrap break-words"
                        :class="{
                          'bg-red-500/10 text-red-300': line.type === 'removed',
                          'bg-brand-500/10 text-brand-300': line.type === 'added',
                          'text-gray-500': line.type === 'same',
                        }"
                      ><span class="select-none inline-block w-5 text-right mr-2 text-xs opacity-50">{{ line.type === 'removed' ? '−' : line.type === 'added' ? '+' : ' ' }}</span>{{ line.text }}</div>
                    </div>
                  </div>
                </div>
              </div>
            </div>

            <!-- Right arrow -->
            <button
              @click="nextStep"
              :disabled="currentStep >= replayRequests.length - 1"
              class="replay-nav-arrow right-0"
              :class="currentStep >= replayRequests.length - 1 ? 'opacity-20 cursor-default' : 'opacity-60 hover:opacity-100'"
            >
              <svg class="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" />
              </svg>
            </button>
          </div>

          <!-- Bottom bar: fork controls + step dots -->
          <div class="flex-none border-t border-white/10 px-6 py-3 flex items-center justify-between">
            <!-- Fork controls -->
            <div class="flex items-center gap-3">
              <div class="flex items-center gap-1.5">
                <label class="text-xs text-gray-500">Model</label>
                <select
                  :value="stepOverrides[currentStep]?.model ?? currentReq?.gen_ai_request_model"
                  @change="setStepOverride(currentStep, 'model', $event.target.value)"
                  :disabled="stepRunning[currentStep]"
                  class="w-48 px-2.5 py-1.5 text-xs bg-white/5 border border-white/10 rounded-lg text-gray-300 disabled:opacity-40"
                >
                  <option v-if="currentReq" :value="currentReq.gen_ai_request_model">{{ currentReq.gen_ai_request_model }} (original)</option>
                  <template v-for="provider in modelCatalog" :key="provider.id">
                    <optgroup :label="provider.name">
                      <option
                        v-for="m in provider.models"
                        :key="m.id"
                        :value="m.id"
                        :disabled="currentReq && m.id === currentReq.gen_ai_request_model"
                      >{{ m.name }}</option>
                    </optgroup>
                  </template>
                </select>
              </div>
              <div class="flex items-center gap-1.5">
                <label class="text-xs text-gray-500">Temp</label>
                <input
                  type="number"
                  min="0" max="1" step="0.1"
                  :value="stepOverrides[currentStep]?.temperature ?? currentReq?.temperature ?? 0.5"
                  @input="setStepOverride(currentStep, 'temperature', $event.target.value === '' ? null : parseFloat($event.target.value))"
                  :disabled="stepRunning[currentStep]"
                  class="w-16 px-2.5 py-1.5 text-xs bg-white/5 border border-white/10 rounded-lg text-gray-300 tabular-nums disabled:opacity-40"
                />
              </div>
              <button
                v-if="isStepModified(currentStep)"
                :disabled="stepRunning[currentStep]"
                class="px-3 py-1.5 text-xs font-medium rounded-lg transition-colors"
                :class="stepRunning[currentStep]
                  ? 'bg-white/5 text-gray-500 cursor-wait'
                  : 'bg-purple-600 text-white hover:bg-purple-700'"
                @click="replayStep(currentStep)"
              >
                <span v-if="stepRunning[currentStep]" class="inline-flex items-center gap-1">
                  <span class="spinner-sm w-3 h-3 border-2 border-white/40 border-t-white rounded-full"></span>
                </span>
                <span v-else>Replay this step</span>
              </button>
            </div>

            <!-- Step dots -->
            <div class="flex items-center gap-1.5">
              <button
                v-for="(_, idx) in replayRequests"
                :key="idx"
                @click="currentStep = idx"
                class="w-2 h-2 rounded-full transition-all"
                :class="idx === currentStep ? 'bg-primary-500 scale-125' : 'bg-white/20 hover:bg-white/40'"
              ></button>
            </div>
          </div>
        </div>
      </Transition>
    </Teleport>
  </AppLayout>
</template>

<script setup>
import { ref, reactive, computed, onMounted, onUnmounted, watch, nextTick } from 'vue';
import { useRoute } from 'vue-router';
import axios from 'axios';
import AppLayout from '@/Layouts/AppLayout.vue';
import BaseCard from '@/components/BaseCard.vue';
import { useAuth } from '@/composables/useAuth';
import { resolveApiUrl } from '@/composables/projectResolver';

const route = useRoute();
const { user, fetchUser } = useAuth();

const projectId = computed(() => route.params.id);
const sessionId = computed(() => route.params.sessionId);
const project = computed(() => ({ id: projectId.value }));

const loading = ref(true);
const requestsLoading = ref(true);
const creditsEnabled = ref(false);
const session = ref({});
const requests = ref([]);

// Replay overlay state
const replayOpen = ref(false);
const currentStep = ref(0);
const overlayEl = ref(null);
const replayScrollEl = ref(null);

// Model catalog for dropdowns
const modelCatalog = ref([]);

// Per-step fork overrides and results
const stepOverrides = reactive({});
const stepResults = reactive({});
const stepRunning = reactive({});

const hasContent = computed(() =>
  requests.value.some(r => r.request_messages || r.response_content)
);

const replayRequests = computed(() =>
  requests.value.filter(r => r.request_messages || r.response_content)
);

const currentReq = computed(() => replayRequests.value[currentStep.value] ?? null);

const currentMessages = computed(() => {
  if (!currentReq.value) return [];
  const allMsgs = parseMessages(currentReq.value.request_messages);
  if (currentStep.value === 0) return allMsgs;
  const prevReq = replayRequests.value[currentStep.value - 1];
  if (!prevReq) return allMsgs;
  const prevMsgCount = parseMessages(prevReq.request_messages).length;
  // Skip messages from previous steps + 1 for the assistant response that was appended
  return allMsgs.slice(prevMsgCount + 1);
});

const openReplay = () => {
  currentStep.value = 0;
  replayOpen.value = true;
  nextTick(() => overlayEl.value?.focus());
};

const closeReplay = () => {
  replayOpen.value = false;
};

const prevStep = () => {
  if (currentStep.value > 0) currentStep.value--;
};

const nextStep = () => {
  if (currentStep.value < replayRequests.value.length - 1) currentStep.value++;
};

watch(currentStep, () => {
  nextTick(() => replayScrollEl.value?.scrollTo({ top: 0, behavior: 'smooth' }));
});

const onKeydown = (e) => {
  if (!replayOpen.value) return;
  if (e.key === 'Escape') closeReplay();
  if (e.key === 'ArrowLeft') prevStep();
  if (e.key === 'ArrowRight') nextStep();
};

const parseMessages = (json) => {
  if (!json) return [];
  try { return JSON.parse(json); } catch { return []; }
};

const getMessageContent = (msg) => {
  if (typeof msg.content === 'string') return msg.content;
  if (Array.isArray(msg.content)) {
    return msg.content
      .map(p => {
        if (typeof p === 'string') return p;
        if (p.type === 'text') return p.text;
        return JSON.stringify(p);
      })
      .join('\n');
  }
  return JSON.stringify(msg.content);
};

const setStepOverride = (idx, field, value) => {
  if (!stepOverrides[idx]) stepOverrides[idx] = {};
  stepOverrides[idx][field] = value;
};

const isStepModified = (idx) => {
  const ov = stepOverrides[idx];
  if (!ov) return false;
  const req = replayRequests.value[idx];
  if (ov.model && ov.model !== req.gen_ai_request_model) return true;
  if (ov.temperature != null && ov.temperature !== (req.temperature ?? 0.5)) return true;
  return false;
};

const clearStepResult = (idx) => {
  delete stepResults[idx];
  delete stepOverrides[idx];
};

const replayStep = async (idx) => {
  const ov = stepOverrides[idx] || {};
  const req = replayRequests.value[idx];
  const model = ov.model || req.gen_ai_request_model;
  if (!model) return;

  stepRunning[idx] = true;
  try {
    const body = { fork_from_index: idx, model };
    if (ov.temperature != null) body.temperature = ov.temperature;

    const response = await fetch(
      resolveApiUrl(`/api/projects/${projectId.value}/llm/sessions/${sessionId.value}/replay`),
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      }
    );

    if (!response.ok) {
      const errText = await response.text();
      stepResults[idx] = { model, response_content: '', input_tokens: 0, output_tokens: 0, duration_ms: 0, status: 'error', error: `HTTP ${response.status}: ${errText}` };
      return;
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop() || '';
      for (const line of lines) {
        if (line.startsWith('data:')) {
          const data = line.slice(5).trim();
          if (!data || data === '{}') continue;
          try {
            const parsed = JSON.parse(data);
            if (parsed.index === idx) {
              stepResults[idx] = parsed;
            }
          } catch { /* skip */ }
        }
      }
    }
  } catch (e) {
    stepResults[idx] = { model, response_content: '', input_tokens: 0, output_tokens: 0, duration_ms: 0, status: 'error', error: e.message };
  } finally {
    stepRunning[idx] = false;
  }
};

const computeDiff = (original, modified) => {
  const oldLines = original.split('\n');
  const newLines = modified.split('\n');
  const result = [];
  let oi = 0, ni = 0;
  while (oi < oldLines.length || ni < newLines.length) {
    if (oi < oldLines.length && ni < newLines.length && oldLines[oi] === newLines[ni]) {
      result.push({ type: 'same', text: oldLines[oi] });
      oi++; ni++;
    } else {
      let syncFound = false;
      const window = 6;
      for (let ahead = 1; ahead <= window && !syncFound; ahead++) {
        if (ni + ahead < newLines.length && oi < oldLines.length && oldLines[oi] === newLines[ni + ahead]) {
          for (let j = 0; j < ahead; j++) result.push({ type: 'added', text: newLines[ni + j] });
          ni += ahead;
          syncFound = true;
        }
        if (oi + ahead < oldLines.length && ni < newLines.length && oldLines[oi + ahead] === newLines[ni]) {
          for (let j = 0; j < ahead; j++) result.push({ type: 'removed', text: oldLines[oi + j] });
          oi += ahead;
          syncFound = true;
        }
      }
      if (!syncFound) {
        if (oi < oldLines.length) { result.push({ type: 'removed', text: oldLines[oi] }); oi++; }
        if (ni < newLines.length) { result.push({ type: 'added', text: newLines[ni] }); ni++; }
      }
    }
  }
  return result;
};

const formatRuleName = (rule) => {
  return (rule || '')
    .replace(/_/g, ' ')
    .replace(/\b\w/g, c => c.toUpperCase())
    .replace(/\bPii\b/, 'PII');
};

const formatNumber = (num) => {
  if (num == null) return '0';
  return Number(num).toLocaleString();
};

const formatCost = (cost) => {
  return parseFloat(cost || 0).toFixed(4);
};

const formatTime = (timestamp) => {
  if (!timestamp) return '—';
  const d = new Date(timestamp);
  return d.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit' });
};

const formatTimeFull = (timestamp) => {
  if (!timestamp) return '—';
  return new Date(timestamp).toLocaleString(undefined, {
    month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit',
  });
};

const submitFeedback = async (score) => {
  const newScore = session.value.feedback_score === score ? null : score;
  try {
    await axios.post(`/api/projects/${projectId.value}/llm/sessions/${sessionId.value}/feedback`, {
      score: newScore,
    });
    session.value.feedback_score = newScore;
  } catch (e) {
    console.error('Failed to submit feedback:', e);
  }
};

const fetchSession = async () => {
  loading.value = true;
  try {
    const response = await axios.get(`/api/projects/${projectId.value}/llm/sessions/${sessionId.value}`);
    session.value = response.data || {};
  } catch (error) {
    console.error('Failed to fetch session:', error);
  } finally {
    loading.value = false;
  }
};

const fetchRequests = async () => {
  requestsLoading.value = true;
  try {
    const response = await axios.get(`/api/projects/${projectId.value}/llm/sessions/${sessionId.value}/requests`, {
      params: { limit: 100 },
    });
    requests.value = Array.isArray(response.data) ? response.data : [];
  } catch (error) {
    console.error('Failed to fetch session requests:', error);
  } finally {
    requestsLoading.value = false;
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
  fetchModelCatalog();
  fetchSession();
  fetchRequests();
  axios.get(`/api/projects/${projectId.value}/llm/metrics/overview`)
    .then(res => { creditsEnabled.value = res.data?.credits_enabled === true; })
    .catch(() => {});
  window.addEventListener('keydown', onKeydown);
});

onUnmounted(() => {
  window.removeEventListener('keydown', onKeydown);
});

watch(projectId, () => {
  fetchModelCatalog();
  fetchSession();
  fetchRequests();
});
</script>

<style scoped>
.spinner {
  animation: spin 1s linear infinite;
}
.spinner-sm {
  animation: spin 0.7s linear infinite;
}

@keyframes spin {
  from { transform: rotate(0deg); }
  to { transform: rotate(360deg); }
}

.diff-view {
  max-height: 400px;
  overflow-y: auto;
}

.replay-pill {
  @apply px-2.5 py-1 text-xs rounded-full bg-white/10 text-gray-300;
}

.replay-nav-arrow {
  @apply absolute top-0 bottom-0 w-16 flex items-center justify-center text-white transition-opacity z-10;
}

.replay-overlay-enter-active {
  transition: opacity 0.2s ease-out;
}
.replay-overlay-leave-active {
  transition: opacity 0.15s ease-in;
}
.replay-overlay-enter-from,
.replay-overlay-leave-to {
  opacity: 0;
}

select option,
select optgroup {
  background: #1a1a2e;
  color: #e2e8f0;
}
</style>
