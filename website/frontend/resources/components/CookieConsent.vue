<template>
  <Transition name="consent-slide">
    <div v-if="visible"
         class="fixed bottom-0 left-0 right-0 z-[9999] bg-white border-t border-line px-6 py-4 shadow-[0_-4px_24px_rgba(0,0,0,0.08)]">
      <div class="max-w-wrap mx-auto flex flex-col sm:flex-row items-start sm:items-center gap-4">
        <p class="text-sm text-muted flex-1">
          We use cookies to understand how visitors use our site.
          <a href="/legal/cookies" class="text-accent hover:underline">Cookie Policy</a>.
        </p>
        <div class="flex gap-2 shrink-0">
          <button @click="decline"
                  class="text-sm font-medium px-4 py-2 rounded-[10px] border border-line text-ink hover:border-ink transition-all">
            Decline
          </button>
          <button @click="accept"
                  class="text-sm font-medium px-4 py-2 rounded-[10px] bg-accent text-white border border-accent hover:bg-accent-dark transition-all">
            Accept
          </button>
        </div>
      </div>
    </div>
  </Transition>
</template>

<script setup>
import { ref, onMounted } from 'vue';

const STORAGE_KEY = 'cookie_consent';
const visible = ref(false);

onMounted(() => {
  const stored = localStorage.getItem(STORAGE_KEY);
  if (stored === 'granted') {
    grantConsent();
  } else if (stored === 'denied') {
    // Already declined, stay hidden
  } else {
    visible.value = true;
  }
});

function grantConsent() {
  if (typeof window.gtag === 'function') {
    window.gtag('consent', 'update', {
      analytics_storage: 'granted',
    });
  }
}

function accept() {
  localStorage.setItem(STORAGE_KEY, 'granted');
  grantConsent();
  visible.value = false;
}

function decline() {
  localStorage.setItem(STORAGE_KEY, 'denied');
  visible.value = false;
}
</script>

<style scoped>
.consent-slide-enter-active,
.consent-slide-leave-active {
  transition: transform 0.3s ease;
}
.consent-slide-enter-from,
.consent-slide-leave-to {
  transform: translateY(100%);
}
</style>
