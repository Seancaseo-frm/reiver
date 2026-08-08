<template>
  <div class="min-h-screen bg-gradient-to-b from-slate-900 via-slate-800 to-slate-900">
    <MarketingNav />

    <!-- Hero -->
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-16 lg:py-24">
      <div class="text-center">
        <h1 class="text-4xl sm:text-5xl font-extrabold text-white leading-tight">
          Professional Services
        </h1>
        <p class="mt-6 text-xl text-slate-300 max-w-3xl mx-auto">
          Get to production faster with hands-on guidance from the engineers who built the platform.
          Fixed-scope, outcome-driven engagements — no open-ended retainers.
        </p>
      </div>
    </div>

    <!-- Quick Start Package -->
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 pb-24">
      <div class="bg-slate-800 rounded-2xl p-8 lg:p-12 border border-slate-700">
        <div class="flex items-center space-x-4 mb-8">
          <div class="bg-brand-500/20 rounded-lg p-3">
            <svg class="w-10 h-10 text-brand-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z" />
            </svg>
          </div>
          <div>
            <h2 class="text-3xl font-bold text-white">Quick Start</h2>
            <p class="text-slate-400 text-lg">90-day activation package</p>
          </div>
        </div>

        <p class="text-slate-300 text-lg mb-10 max-w-3xl">
          A fixed-scope engagement designed to take your team from zero to production-ready on Reiver
          in 90 days. We handle the setup, configuration, and training so your engineers can focus on
          shipping product — not wrestling with infrastructure.
        </p>

        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-10">
          <div class="bg-slate-700/50 rounded-xl p-6">
            <div class="bg-blue-500/20 rounded-lg p-2 w-10 h-10 flex items-center justify-center mb-4">
              <svg class="w-5 h-5 text-blue-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
              </svg>
            </div>
            <h3 class="text-white font-semibold mb-2">APM Instrumentation</h3>
            <p class="text-slate-400 text-sm">Full-stack tracing, logs, metrics, and error tracking configured for your services.</p>
          </div>

          <div class="bg-slate-700/50 rounded-xl p-6">
            <div class="bg-purple-500/20 rounded-lg p-2 w-10 h-10 flex items-center justify-center mb-4">
              <svg class="w-5 h-5 text-purple-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 9l3 3-3 3m5 0h3M5 20h14a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
              </svg>
            </div>
            <h3 class="text-white font-semibold mb-2">LLM Gateway Setup</h3>
            <p class="text-slate-400 text-sm">Provider keys, model routing, fallback chains, and cost tracking configured end-to-end.</p>
          </div>

          <div class="bg-slate-700/50 rounded-xl p-6">
            <div class="bg-amber-500/20 rounded-lg p-2 w-10 h-10 flex items-center justify-center mb-4">
              <svg class="w-5 h-5 text-amber-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
              </svg>
            </div>
            <h3 class="text-white font-semibold mb-2">Prompt Hub Configuration</h3>
            <p class="text-slate-400 text-sm">Version your first prompts, set up guardrails, and configure rollout strategies.</p>
          </div>

          <div class="bg-slate-700/50 rounded-xl p-6">
            <div class="bg-brand-500/20 rounded-lg p-2 w-10 h-10 flex items-center justify-center mb-4">
              <svg class="w-5 h-5 text-brand-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 5a1 1 0 011-1h14a1 1 0 011 1v2a1 1 0 01-1 1H5a1 1 0 01-1-1V5zM4 13a1 1 0 011-1h6a1 1 0 011 1v6a1 1 0 01-1 1H5a1 1 0 01-1-1v-6zM16 13a1 1 0 011-1h2a1 1 0 011 1v6a1 1 0 01-1 1h-2a1 1 0 01-1-1v-6z" />
              </svg>
            </div>
            <h3 class="text-white font-semibold mb-2">Dashboards & Alerts</h3>
            <p class="text-slate-400 text-sm">Custom dashboards and alert rules tailored to your SLOs and operational needs.</p>
          </div>

          <div class="bg-slate-700/50 rounded-xl p-6">
            <div class="bg-rose-500/20 rounded-lg p-2 w-10 h-10 flex items-center justify-center mb-4">
              <svg class="w-5 h-5 text-rose-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 6.253v13m0-13C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.253m0-13C13.168 5.477 14.754 5 16.5 5c1.747 0 3.332.477 4.5 1.253v13C19.832 18.477 18.247 18 16.5 18c-1.746 0-3.332.477-4.5 1.253" />
              </svg>
            </div>
            <h3 class="text-white font-semibold mb-2">Team Training</h3>
            <p class="text-slate-400 text-sm">Hands-on session covering the platform, best practices, and your specific setup.</p>
          </div>

          <div class="bg-slate-700/50 rounded-xl p-6">
            <div class="bg-cyan-500/20 rounded-lg p-2 w-10 h-10 flex items-center justify-center mb-4">
              <svg class="w-5 h-5 text-cyan-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 17v-2m3 2v-4m3 4v-6m2 10H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
              </svg>
            </div>
            <h3 class="text-white font-semibold mb-2">Architecture Review</h3>
            <p class="text-slate-400 text-sm">Detailed document covering your setup, recommendations, and next steps.</p>
          </div>
        </div>

        <div class="flex flex-wrap gap-4 text-sm text-slate-400">
          <span class="inline-flex items-center gap-1.5">
            <span class="text-brand-500">&#10003;</span> 90-day engagement
          </span>
          <span class="inline-flex items-center gap-1.5">
            <span class="text-brand-500">&#10003;</span> Remote delivery (on-site available)
          </span>
          <span class="inline-flex items-center gap-1.5">
            <span class="text-brand-500">&#10003;</span> For technical teams with existing infrastructure
          </span>
        </div>
      </div>
    </div>

    <!-- Process -->
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 pb-24">
      <h2 class="text-2xl sm:text-3xl font-bold text-white text-center mb-12">How it works</h2>
      <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-8">
        <div class="text-center">
          <div class="mx-auto w-12 h-12 rounded-full bg-brand-600/20 border border-brand-600/40 flex items-center justify-center mb-4">
            <span class="text-brand-400 font-bold">1</span>
          </div>
          <h3 class="text-white font-semibold mb-2">Discover & Plan</h3>
          <p class="text-slate-400 text-sm">We map your stack, define objectives, and agree on success criteria.</p>
        </div>
        <div class="text-center">
          <div class="mx-auto w-12 h-12 rounded-full bg-brand-600/20 border border-brand-600/40 flex items-center justify-center mb-4">
            <span class="text-brand-400 font-bold">2</span>
          </div>
          <h3 class="text-white font-semibold mb-2">Configure & Build</h3>
          <p class="text-slate-400 text-sm">We instrument your services, configure the gateway, and build your dashboards.</p>
        </div>
        <div class="text-center">
          <div class="mx-auto w-12 h-12 rounded-full bg-brand-600/20 border border-brand-600/40 flex items-center justify-center mb-4">
            <span class="text-brand-400 font-bold">3</span>
          </div>
          <h3 class="text-white font-semibold mb-2">Train & Launch</h3>
          <p class="text-slate-400 text-sm">Your team gets hands-on training, then we go live together.</p>
        </div>
        <div class="text-center">
          <div class="mx-auto w-12 h-12 rounded-full bg-brand-600/20 border border-brand-600/40 flex items-center justify-center mb-4">
            <span class="text-brand-400 font-bold">4</span>
          </div>
          <h3 class="text-white font-semibold mb-2">Ongoing Support</h3>
          <p class="text-slate-400 text-sm">Post-launch check-ins to optimize, tune alerts, and answer questions.</p>
        </div>
      </div>
    </div>

    <!-- Who it's for -->
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 pb-24">
      <h2 class="text-2xl sm:text-3xl font-bold text-white text-center mb-12">Who it's for</h2>
      <div class="grid grid-cols-1 md:grid-cols-3 gap-8 max-w-4xl mx-auto">
        <div class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
          <h3 class="text-white font-semibold mb-2">Migrating off legacy APM</h3>
          <p class="text-slate-400 text-sm">Moving from Datadog, New Relic, or Splunk and want a clean, fast cutover with zero data loss.</p>
        </div>
        <div class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
          <h3 class="text-white font-semibold mb-2">Adopting LLM infrastructure</h3>
          <p class="text-slate-400 text-sm">Shipping AI features and need a production-grade gateway with guardrails, cost controls, and prompt versioning from day one.</p>
        </div>
        <div class="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50">
          <h3 class="text-white font-semibold mb-2">Want production-ready fast</h3>
          <p class="text-slate-400 text-sm">Technical team with existing infra that doesn't want to spend months self-teaching a new platform.</p>
        </div>
      </div>
    </div>

    <!-- CTA -->
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 pb-24">
      <div class="rounded-3xl border border-slate-700/80 bg-gradient-to-br from-slate-900/90 via-slate-900/50 to-brand-950/20 px-8 py-12 sm:px-12 sm:py-16 text-center">
        <h2 class="text-2xl sm:text-3xl font-bold text-white tracking-tight">
          Let's scope your Quick Start
        </h2>
        <p class="mt-4 text-slate-400 max-w-2xl mx-auto leading-relaxed">
          Every engagement is tailored to your stack and goals. Book a call and we'll put together
          a proposal within 48 hours.
        </p>
        <div class="mt-8 flex flex-col sm:flex-row gap-3 justify-center">
          <a
            v-bind="demoLink"
            class="inline-flex justify-center items-center rounded-xl bg-brand-600 px-8 py-3.5 text-base font-semibold text-white hover:bg-brand-500 transition-colors"
          >
            Talk to us
          </a>
          <router-link
            to="/signup"
            class="inline-flex justify-center items-center rounded-xl border border-slate-600 bg-slate-900/50 px-8 py-3.5 text-base font-semibold text-white hover:bg-slate-800 transition-colors"
          >
            Or start a free trial
          </router-link>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { getDemoLinkAttrs } from '@/composables/useDemoBooking';
import MarketingNav from '@/Pages/Home/MarketingNav.vue';

const demoLink = getDemoLinkAttrs();
</script>
