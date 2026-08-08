<template>
  <div class="min-h-screen bg-gradient-to-b from-slate-900 via-slate-800 to-slate-900">
    <MarketingNav />

    <!-- Page Header -->
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-16 lg:py-24">
      <div class="text-center">
        <h1 class="text-4xl sm:text-5xl font-extrabold text-white leading-tight">
          Our Products
        </h1>
        <p class="mt-6 text-xl text-slate-300 max-w-3xl mx-auto">
          Full-stack observability and AI infrastructure — use them together or separately.
        </p>
      </div>
    </div>

    <!-- Reiver APM -->
    <div id="apm" class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 pb-24">
      <div class="bg-slate-800 rounded-2xl p-8 lg:p-12 border border-slate-700">
        <div class="flex items-center space-x-4 mb-8">
          <div class="bg-blue-500/20 rounded-lg p-3">
            <svg class="w-10 h-10 text-blue-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
            </svg>
          </div>
          <div>
            <h2 class="text-3xl font-bold text-white">Reiver APM</h2>
            <p class="text-slate-400 text-lg">Full-Stack Application Monitoring</p>
          </div>
        </div>

        <p class="text-slate-300 text-lg mb-10 max-w-3xl">
          Monitor your entire stack from a single platform. Reiver APM correlates traces, logs, errors, metrics, and profiles so you can pinpoint issues in seconds — not hours.
        </p>

        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          <div class="bg-slate-700/50 rounded-xl p-6">
            <h3 class="text-lg font-semibold text-white mb-3">Distributed Tracing</h3>
            <p class="text-slate-400">End-to-end request visualization with full OpenTelemetry (OTLP) support. See every hop across services, databases, and external APIs.</p>
          </div>
          <div class="bg-slate-700/50 rounded-xl p-6">
            <h3 class="text-lg font-semibold text-white mb-3">Error Tracking</h3>
            <p class="text-slate-400">Automatic exception grouping with smart fingerprinting. Get alerted on new error patterns and track regressions across deployments.</p>
          </div>
          <div class="bg-slate-700/50 rounded-xl p-6">
            <h3 class="text-lg font-semibold text-white mb-3">Log Aggregation</h3>
            <p class="text-slate-400">Structured logging with full-text search. Correlate logs with traces and errors for complete request context.</p>
          </div>
          <div class="bg-slate-700/50 rounded-xl p-6">
            <h3 class="text-lg font-semibold text-white mb-3">Database Monitoring</h3>
            <p class="text-slate-400">Query explain plans, slow query detection, and connection pool monitoring. Spot N+1 queries and missing indexes automatically.</p>
          </div>
          <div class="bg-slate-700/50 rounded-xl p-6">
            <h3 class="text-lg font-semibold text-white mb-3">Continuous Profiling</h3>
            <p class="text-slate-400">CPU and memory flamegraphs linked directly to traces. Identify hot paths and memory leaks in production without overhead.</p>
          </div>
          <div class="bg-slate-700/50 rounded-xl p-6">
            <h3 class="text-lg font-semibold text-white mb-3">Alerts &amp; Incidents</h3>
            <p class="text-slate-400">Threshold and anomaly-based alerts with Slack, PagerDuty, Discord, and Teams. Unified incident timelines with correlated telemetry.</p>
          </div>
        </div>
      </div>
    </div>

    <!-- Reiver Prompt Hub -->
    <div id="prompt-hub" class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 pb-24">
      <div class="bg-slate-800 rounded-2xl p-8 lg:p-12 border border-slate-700">
        <div class="flex items-center space-x-4 mb-8">
          <div class="bg-purple-500/20 rounded-lg p-3">
            <svg class="w-10 h-10 text-purple-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z" />
            </svg>
          </div>
          <div>
            <h2 class="text-3xl font-bold text-white">Reiver Prompt Hub</h2>
            <p class="text-slate-400 text-lg">LLM Prompt Management and Gateway</p>
          </div>
        </div>

        <p class="text-slate-300 text-lg mb-10 max-w-3xl">
          Version, test, and roll out prompts centrally. Route requests to any LLM provider through one OpenAI-compatible API with automatic failover, caching, and cost tracking.
        </p>

        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          <div class="bg-slate-700/50 rounded-xl p-6">
            <h3 class="text-lg font-semibold text-white mb-3">One API, All Providers</h3>
            <p class="text-slate-400">Route to OpenAI, Anthropic, Google Gemini, and AWS Bedrock through a single OpenAI-compatible endpoint. Switch providers without changing code.</p>
          </div>
          <div class="bg-slate-700/50 rounded-xl p-6">
            <h3 class="text-lg font-semibold text-white mb-3">Prompt Versioning</h3>
            <p class="text-slate-400">Version control for your prompts with diff views and rollback. Track which prompt version produced which outputs.</p>
          </div>
          <div class="bg-slate-700/50 rounded-xl p-6">
            <h3 class="text-lg font-semibold text-white mb-3">A/B Testing &amp; Rollouts</h3>
            <p class="text-slate-400">Canary and percentage-based rollouts for prompt changes. Compare output quality across versions before going to 100%.</p>
          </div>
          <div class="bg-slate-700/50 rounded-xl p-6">
            <h3 class="text-lg font-semibold text-white mb-3">Real-Time Streaming</h3>
            <p class="text-slate-400">SSE streaming support for all providers. First-token latency tracking and streaming-aware observability built in.</p>
          </div>
          <div class="bg-slate-700/50 rounded-xl p-6">
            <h3 class="text-lg font-semibold text-white mb-3">Automatic Failover</h3>
            <p class="text-slate-400">Retry logic with cross-provider fallback. If one provider is down or rate-limited, requests automatically route to your backup.</p>
          </div>
          <div class="bg-slate-700/50 rounded-xl p-6">
            <h3 class="text-lg font-semibold text-white mb-3">Cost &amp; Usage Tracking</h3>
            <p class="text-slate-400">Per-request token counting and cost attribution. Track spend by model, prompt, and user with real-time dashboards.</p>
          </div>
        </div>
      </div>
    </div>

    <!-- CTA -->
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 pb-24">
      <div class="text-center">
        <h2 class="text-3xl font-bold text-white mb-4">Ready to get started?</h2>
        <p class="text-xl text-slate-400 mb-8">Try Reiver free — no credit card required.</p>
        <div class="flex flex-col sm:flex-row justify-center gap-4">
          <router-link to="/signup" class="bg-brand-600 text-white px-8 py-4 rounded-lg text-lg font-medium hover:bg-brand-700 transition-colors">
            Start Free Trial
          </router-link>
          <a :href="docsHome" target="_blank" rel="noopener" class="bg-slate-700 text-white border border-slate-600 px-8 py-4 rounded-lg text-lg font-medium hover:bg-slate-600 transition-colors">
            Read the Docs
          </a>
        </div>
      </div>
    </div>

    <!-- Footer -->
    <footer class="bg-slate-900 border-t border-slate-800">
      <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
        <div class="grid grid-cols-2 md:grid-cols-3 gap-8 mb-8">
          <div>
            <h3 class="text-sm font-semibold text-white uppercase tracking-wider mb-4">Products</h3>
            <ul class="space-y-2">
              <li><router-link to="/products#apm" class="text-slate-400 hover:text-white">APM</router-link></li>
              <li><router-link to="/products#prompt-hub" class="text-slate-400 hover:text-white">Prompt Hub</router-link></li>
              <li><router-link to="/model-catalog" class="text-slate-400 hover:text-white">Model Catalog</router-link></li>
            </ul>
          </div>
          <div>
            <h3 class="text-sm font-semibold text-white uppercase tracking-wider mb-4">Resources</h3>
            <ul class="space-y-2">
              <li><a :href="docsHome" target="_blank" rel="noopener" class="text-slate-400 hover:text-white">Documentation</a></li>
              <li><a href="/blog" class="text-slate-400 hover:text-white">Blog</a></li>
              <li><a href="/status" class="text-slate-400 hover:text-white">Status</a></li>
            </ul>
          </div>
          <div>
            <h3 class="text-sm font-semibold text-white uppercase tracking-wider mb-4">Company</h3>
            <ul class="space-y-2">
              <li><a href="/about" class="text-slate-400 hover:text-white">About</a></li>
              <li><a href="/contact" class="text-slate-400 hover:text-white">Contact</a></li>
              <li><a href="/careers" class="text-slate-400 hover:text-white">Careers</a></li>
            </ul>
          </div>
        </div>
        <div class="border-t border-slate-800 pt-8">
          <div class="flex flex-col md:flex-row justify-between items-center">
            <div class="flex items-center space-x-2 mb-4 md:mb-0">
              <ReiverLogo class="w-6 h-6 text-brand-500" />
              <span class="text-lg font-bold text-white">Reiver</span>
            </div>
            <p class="text-slate-500 text-sm">Managed prompts and a unified LLM gateway</p>
          </div>
        </div>
      </div>
    </footer>
  </div>
</template>

<script setup>
import { docsUrl } from '@/composables/useDocsUrl';
import MarketingNav from '@/Pages/Home/MarketingNav.vue';
import ReiverLogo from '@/components/ReiverLogo.vue';

const docsHome = docsUrl('/');
</script>
