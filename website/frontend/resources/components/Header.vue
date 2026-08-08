<template>
  <div class="flex justify-between items-center h-12 px-5 bg-white border-b border-gray-200 z-10 relative">
    <div class="flex items-center gap-4">
      <slot name="left">
        <router-link to="/dashboard" class="text-gray-900 no-underline font-semibold text-base transition-colors hover:text-gray-500 flex items-center">
          <span class="font-semibold tracking-tight">Reiver</span>
        </router-link>
      </slot>
    </div>

    <div v-if="$slots.right" class="flex items-center gap-3">
      <slot name="right" />
    </div>
  </div>
</template>

<script setup>
</script>
