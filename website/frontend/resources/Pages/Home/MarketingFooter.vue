<template>
  <footer class="border-t border-line bg-white py-12">
    <div class="max-w-wrap mx-auto px-6 flex justify-between items-start gap-10 flex-wrap">
      <div class="max-w-[30ch]">
        <router-link to="/" class="flex items-center" aria-label="Reiver home">
          <ReiverLogo class="h-6 w-6 text-accent" />
          <span class="ml-2 text-base font-semibold font-display tracking-tight text-ink">Reiver</span>
        </router-link>
        <p class="text-muted text-sm mt-3">Production AI control plane for teams shipping AI without a platform team.</p>
        <p class="text-sm text-muted mt-2">Private Early Access. No SLA. Production use is at your own risk.</p>
      </div>

      <div class="flex gap-16 flex-wrap">
        <div>
          <h5 class="font-mono text-[0.7rem] tracking-[0.1em] uppercase text-faint mb-3 font-medium">Product</h5>
          <a href="#product" class="block text-sm text-muted mb-2 hover:text-ink transition-colors">Flow</a>
          <a href="#product" class="block text-sm text-muted mb-2 hover:text-ink transition-colors">Watch</a>
          <a href="#product" class="block text-sm text-muted mb-2 hover:text-ink transition-colors">Herd</a>
          <a href="#product" class="block text-sm text-muted mb-2 hover:text-ink transition-colors">Agent</a>
          <router-link to="/model-catalog" class="block text-sm text-muted mb-2 hover:text-ink transition-colors">Model Catalog</router-link>
          <a href="#pricing" class="block text-sm text-muted mb-2 hover:text-ink transition-colors">Pricing</a>
          <a href="/compare/datadog" class="block text-sm text-muted mb-2 hover:text-ink transition-colors">Reiver vs Datadog</a>
        </div>
        <div>
          <h5 class="font-mono text-[0.7rem] tracking-[0.1em] uppercase text-faint mb-3 font-medium">Developers</h5>
          <a href="https://docs.reiver.ai/" class="block text-sm text-muted mb-2 hover:text-ink transition-colors">Docs</a>
          <router-link to="/quickstart" class="block text-sm text-muted mb-2 hover:text-ink transition-colors">Quickstart</router-link>
          <a href="https://docs.reiver.ai/sdks/" class="block text-sm text-muted mb-2 hover:text-ink transition-colors">SDKs</a>
        </div>
        <div>
          <h5 class="font-mono text-[0.7rem] tracking-[0.1em] uppercase text-faint mb-3 font-medium">Legal</h5>
          <router-link to="/security" class="block text-sm text-muted mb-2 hover:text-ink transition-colors">Security</router-link>
          <a href="/legal/privacy" class="block text-sm text-muted mb-2 hover:text-ink transition-colors">Privacy Policy</a>
          <a href="/legal/early-access-terms" class="block text-sm text-muted mb-2 hover:text-ink transition-colors">Early Access Terms</a>
          <a href="/legal/dpa" class="block text-sm text-muted mb-2 hover:text-ink transition-colors">Data Processing Agreement</a>
          <a href="/legal/aup" class="block text-sm text-muted mb-2 hover:text-ink transition-colors">Acceptable Use Policy</a>
          <a href="/legal/cookies" class="block text-sm text-muted mb-2 hover:text-ink transition-colors">Cookie Policy</a>
          <router-link to="/subprocessors" class="block text-sm text-muted mb-2 hover:text-ink transition-colors">Sub-processors</router-link>
        </div>
        <div>
          <h5 class="font-mono text-[0.7rem] tracking-[0.1em] uppercase text-faint mb-3 font-medium">Company</h5>
          <router-link to="/login" class="block text-sm text-muted mb-2 hover:text-ink transition-colors">Login</router-link>
          <a href="mailto:hello@reiver.ai" class="block text-sm text-muted mb-2 hover:text-ink transition-colors">Contact</a>
        </div>
      </div>
    </div>

    <div class="max-w-wrap mx-auto mt-8 px-6 pt-4 border-t border-line text-sm text-faint flex justify-between flex-wrap gap-2">
      <span>&copy; {{ year }} Reiver. All rights reserved.</span>
      <span>Built on OpenTelemetry and MCP.</span>
    </div>
  </footer>
</template>

<script setup>
import ReiverLogo from '@/components/ReiverLogo.vue';

const year = new Date().getFullYear();
</script>
