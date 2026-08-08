<template>
  <section class="text-center py-24 lg:py-28" id="access">
    <div class="max-w-wrap mx-auto px-6">
      <h2 class="font-display font-semibold text-[clamp(2.1rem,3.6vw,3.1rem)] leading-[1.04] tracking-tight">
        Point your agent at the docs.
      </h2>
      <p class="text-muted text-lg mt-4 mx-auto max-w-[46ch]">
        Give your coding agent our docs and a key, and watch it wire Reiver in.
      </p>
      <div class="flex gap-3 justify-center mt-7 flex-wrap">
        <a href="https://docs.reiver.ai/" class="inline-flex items-center gap-2 font-medium text-sm px-4 py-2.5 rounded-[10px] bg-accent text-white border border-accent hover:bg-accent-dark hover:-translate-y-px transition-all">
          Read the docs
        </a>
        <router-link to="/signup" class="inline-flex items-center gap-2 font-medium text-sm px-4 py-2.5 rounded-[10px] bg-ink text-white border border-ink hover:bg-black hover:-translate-y-px transition-all">
          Signup
        </router-link>
      </div>
    </div>
  </section>
</template>
