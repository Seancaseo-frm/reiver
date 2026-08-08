<template>
  <div class="flex flex-col h-screen w-screen overflow-hidden bg-white text-gray-900 font-sans">
    <Header>
      <template #left>
        <button
          v-if="isMobile"
          @click="sidebarOpen = !sidebarOpen"
          class="mobile-menu-btn"
        >
          <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 12h16M4 18h16" />
          </svg>
        </button>
      </template>
      <template #right>
        <HeaderRightSection />
      </template>
    </Header>

    <div class="flex flex-1 overflow-hidden">
      <Sidebar
        :user="user"
        :current-project="sidebarProject"
        :is-open="sidebarOpen"
        :is-mobile="isMobile"
        @close="sidebarOpen = false"
        @logout="handleLogout"
      />

      <div
        class="flex-1 flex flex-col overflow-hidden ml-60 max-lg:ml-0 transition-all duration-200"
        :style="mainContentStyle"
      >
        <main class="main-scroll">
          <slot />
        </main>
      </div>
    </div>

    <AgentPanel />
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted, watch } from 'vue';
import { useRouter, useRoute } from 'vue-router';
import Sidebar from '@/components/Sidebar.vue';
import Header from '@/components/Header.vue';
import HeaderRightSection from '@/components/HeaderRightSection.vue';
import AgentPanel from '@/components/AgentPanel.vue';
import { useAgent } from '@/composables/useAgent';
import { useAuth } from '@/composables/useAuth';
import { useCurrentProject } from '@/composables/useCurrentProject';

const { isOpen: agentOpen, position: agentPosition, panelWidth: agentWidth, panelHeight: agentHeight } = useAgent();
const { clearUser } = useAuth();
const { currentProject: sidebarProject, fetchAndSet, restore } = useCurrentProject();

const props = defineProps({
  user: Object,
  currentProject: Object,
});

const router = useRouter();
const route = useRoute();

const sidebarOpen = ref(false);
const isMobile = ref(false);

const handleLogout = () => {
  document.cookie = 'token=; expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/;';
  clearUser();
  router.push('/login');
};

watch(() => route.params.id, (newId) => {
  if (newId) {
    fetchAndSet(newId);
  }
}, { immediate: true });

const mainContentStyle = computed(() => {
  if (!agentOpen.value) return {};
  if (agentPosition.value === 'right') {
    return { marginRight: `${agentWidth.value}px`, minWidth: '400px' };
  }
  if (agentPosition.value === 'bottom') {
    return { marginBottom: `${agentHeight.value}px` };
  }
  return {};
});

const checkMobile = () => {
  isMobile.value = window.innerWidth < 1024;
};

onMounted(async () => {
  checkMobile();
  window.addEventListener('resize', checkMobile);

  if (!isMobile.value) {
    sidebarOpen.value = true;
  }

  if (!route.params.id) {
    await restore();
  }
});

onUnmounted(() => {
  window.removeEventListener('resize', checkMobile);
});

watch(() => route.path, () => {
  if (isMobile.value) {
    sidebarOpen.value = false;
  }
});
</script>

<style scoped>
.mobile-menu-btn {
  color: #9ca3af;
  background: none;
  border: none;
  cursor: pointer;
  padding: 8px;
  border-radius: 4px;
  transition: all 0.2s;
  margin-right: 16px;
}

.mobile-menu-btn:hover {
  color: #374151;
  background-color: #f3f4f6;
}

.main-scroll {
  flex: 1;
  overflow-y: auto;
  overflow-x: hidden;
  background-color: #ffffff;
  padding: 0;
}

.main-scroll::-webkit-scrollbar {
  width: 8px;
}

.main-scroll::-webkit-scrollbar-track {
  background: transparent;
}

.main-scroll::-webkit-scrollbar-thumb {
  background: #cbd5e1;
  border-radius: 4px;
}

.main-scroll::-webkit-scrollbar-thumb:hover {
  background: #94a3b8;
}
</style>
