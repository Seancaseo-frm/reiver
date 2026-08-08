<template>
  <header class="sticky top-0 z-50 bg-paper/[.78] backdrop-saturate-[160%] backdrop-blur-[12px] border-b border-line">
    <div class="max-w-wrap mx-auto px-6">
      <div class="flex items-center justify-between h-16">
        <router-link to="/" class="flex items-center" aria-label="Reiver home">
          <ReiverLogo class="h-7 w-7 text-accent" />
          <span class="ml-2 text-lg font-semibold font-display tracking-tight text-ink">Reiver</span>
        </router-link>

        <nav class="hidden md:flex items-center gap-8" id="navlinks">
          <a href="#how" class="text-sm text-muted hover:text-ink transition-colors">How it works</a>
          <a href="#product" class="text-sm text-muted hover:text-ink transition-colors">Product</a>
          <a href="#pricing" class="text-sm text-muted hover:text-ink transition-colors">Pricing</a>
          <router-link to="/model-catalog" class="text-sm text-muted hover:text-ink transition-colors">Model Catalog</router-link>
          <router-link to="/quickstart" class="text-sm text-muted hover:text-ink transition-colors">Quickstart</router-link>
          <a href="https://docs.reiver.ai/" class="text-sm text-muted hover:text-ink transition-colors">Docs</a>
        </nav>

        <div class="flex items-center gap-3">
          <router-link to="/login"
             class="hidden md:inline-flex text-sm font-medium text-muted hover:text-ink transition-colors">
            Login
          </router-link>
          <router-link to="/signup"
             class="hidden md:inline-flex items-center gap-2 font-medium text-sm px-4 py-2 rounded-[10px] bg-accent text-white border border-accent hover:bg-accent-dark hover:-translate-y-px transition-all">
            Signup
          </router-link>
          <button
            class="md:hidden flex items-center justify-center w-10 h-10 border border-line rounded-lg"
            @click="mobileOpen = !mobileOpen"
            :aria-expanded="mobileOpen"
            aria-label="Menu">
            <span class="flex flex-col gap-[3.5px]">
              <span class="block w-[18px] h-[1.5px] bg-ink"></span>
              <span class="block w-[18px] h-[1.5px] bg-ink"></span>
              <span class="block w-[18px] h-[1.5px] bg-ink"></span>
            </span>
          </button>
        </div>
      </div>
    </div>

    <nav v-if="mobileOpen" class="md:hidden absolute top-16 left-0 right-0 bg-white border-b border-line px-6">
      <a href="#how" class="block py-3 border-b border-line-2 text-sm text-muted" @click="mobileOpen = false">How it works</a>
      <a href="#product" class="block py-3 border-b border-line-2 text-sm text-muted" @click="mobileOpen = false">Product</a>
      <a href="#pricing" class="block py-3 border-b border-line-2 text-sm text-muted" @click="mobileOpen = false">Pricing</a>
      <router-link to="/model-catalog" class="block py-3 border-b border-line-2 text-sm text-muted" @click="mobileOpen = false">Model Catalog</router-link>
      <router-link to="/quickstart" class="block py-3 border-b border-line-2 text-sm text-muted" @click="mobileOpen = false">Quickstart</router-link>
      <a href="https://docs.reiver.ai/" class="block py-3 border-b border-line-2 text-sm text-muted" @click="mobileOpen = false">Docs</a>
      <router-link to="/login" class="block py-3 border-b border-line-2 text-sm text-muted" @click="mobileOpen = false">Login</router-link>
      <router-link to="/signup" class="block py-3 text-sm font-medium text-accent" @click="mobileOpen = false">Signup</router-link>
    </nav>
  </header>
</template>

<script setup>
import { ref } from 'vue';
import ReiverLogo from '@/components/ReiverLogo.vue';

const mobileOpen = ref(false);
</script>
