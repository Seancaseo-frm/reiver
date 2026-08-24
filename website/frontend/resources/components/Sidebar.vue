<template>
  <div 
    class="app-sidebar" 
    :class="{ 'hidden lg:flex': !isOpen, 'flex': isOpen }"
  >
    <!-- Logo Section -->
    <div class="sidebar-header">
      <router-link :to="currentProject ? `/p/${currentProject.slug || currentProject.id}/dashboards` : '/'" class="sidebar-logo">
        <div class="logo-icon">
          <ReiverLogo class="w-full h-full" />
        </div>
        <span class="logo-text">Reiver</span>
      </router-link>
      <button
        v-if="isMobile"
        @click="$emit('close')"
        class="sidebar-close-btn"
      >
        <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
        </svg>
      </button>
    </div>

    <!-- Project Switcher -->
    <div class="project-switcher" v-if="currentProject">
      <button class="project-switcher-btn" @click="toggleProjectDropdown">
        <div class="project-switcher-info">
          <span class="project-switcher-name">{{ currentProject.name }}</span>
          <span class="project-switcher-org">{{ currentProject.organization_name || 'Organization' }}</span>
        </div>
        <svg 
          class="project-switcher-chevron" 
          :class="{ 'project-switcher-chevron-open': projectDropdownOpen }"
          fill="none" stroke="currentColor" viewBox="0 0 24 24"
        >
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 9l4-4 4 4m0 6l-4 4-4-4" />
        </svg>
      </button>

      <div v-if="projectDropdownOpen" class="project-dropdown">
        <div class="project-dropdown-scroll">
          <template v-for="org in groupedProjects" :key="org.id">
            <div class="project-dropdown-org-label">{{ org.name }}</div>
            <button
              v-for="project in org.projects"
              :key="project.id"
              class="project-dropdown-item"
              :class="{ 'project-dropdown-item-active': project.id === currentProject?.id }"
              @click="switchProject(project)"
            >
              <span class="project-dropdown-item-name">{{ project.name }}</span>
              <svg v-if="project.id === currentProject?.id" class="project-dropdown-check" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
              </svg>
            </button>
          </template>
        </div>
        <router-link 
          to="/projects/create" 
          class="project-dropdown-create"
          @click="projectDropdownOpen = false"
        >
          <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2" class="project-dropdown-create-icon">
            <path stroke-linecap="round" stroke-linejoin="round" d="M12 4v16m8-8H4" />
          </svg>
          <span>New Project</span>
        </router-link>
      </div>
    </div>

    <!-- Navigation -->
    <nav class="sidebar-nav">
      <!-- Main Navigation -->
      <template v-if="currentProject">
        <!-- Observability Section -->
        <div v-if="hasProduct('watch')" class="sidebar-section">
          <button 
            class="section-header"
            :class="{ 'section-header-active': expandedSection === 'observability' }"
            @click="toggleSection('observability')"
          >
            <component :is="ObservabilitySectionIcon" class="section-icon" />
            <span class="section-label">Observability</span>
            <svg 
              class="section-chevron" 
              :class="{ 'section-chevron-expanded': expandedSection === 'observability' }"
              fill="none" 
              stroke="currentColor" 
              viewBox="0 0 24 24"
            >
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
            </svg>
          </button>
          <div class="section-items" :class="{ 'section-items-expanded': expandedSection === 'observability' }">
            <router-link
              v-for="item in observabilityItems"
              :key="item.name"
              :to="item.href"
              :class="[
                'nav-item',
                isActive(item.href) ? 'nav-item-active' : ''
              ]"
            >
              <component :is="item.icon" class="nav-icon" />
              <span class="nav-text">{{ item.name }}</span>
            </router-link>
          </div>
        </div>

        <!-- Prompt Hub Section -->
        <div v-if="hasProduct('prompt_hub')" class="sidebar-section">
          <button 
            class="section-header"
            :class="{ 'section-header-active': expandedSection === 'llm' }"
            @click="toggleSection('llm')"
          >
            <component :is="LlmSectionIcon" class="section-icon" />
            <span class="section-label">Prompt Hub</span>
            <svg 
              class="section-chevron" 
              :class="{ 'section-chevron-expanded': expandedSection === 'llm' }"
              fill="none" 
              stroke="currentColor" 
              viewBox="0 0 24 24"
            >
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
            </svg>
          </button>
          <div class="section-items" :class="{ 'section-items-expanded': expandedSection === 'llm' }">
            <router-link
              v-for="item in promptHubItems"
              :key="item.name"
              :to="item.href"
              :class="[
                'nav-item',
                isActive(item.href) ? 'nav-item-active' : ''
              ]"
            >
              <component :is="item.icon" class="nav-icon" />
              <span class="nav-text">{{ item.name }}</span>
            </router-link>
          </div>
        </div>

        <!-- Herd Section (A2A Agent Registry) -->
        <div v-if="hasProduct('herd')" class="sidebar-section">
          <button 
            class="section-header"
            :class="{ 'section-header-active': expandedSection === 'herd' }"
            @click="toggleSection('herd')"
          >
            <component :is="HerdSectionIcon" class="section-icon" />
            <span class="section-label">Herd</span>
            <svg 
              class="section-chevron" 
              :class="{ 'section-chevron-expanded': expandedSection === 'herd' }"
              fill="none" 
              stroke="currentColor" 
              viewBox="0 0 24 24"
            >
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
            </svg>
          </button>
          <div class="section-items" :class="{ 'section-items-expanded': expandedSection === 'herd' }">
            <router-link
              v-for="item in herdItems"
              :key="item.name"
              :to="item.href"
              :class="[
                'nav-item',
                isActive(item.href) ? 'nav-item-active' : ''
              ]"
            >
              <component :is="item.icon" class="nav-icon" />
              <span class="nav-text">{{ item.name }}</span>
            </router-link>
          </div>
        </div>

        <!-- Warehouse Section (disabled — re-enable when Pond launches)
        <div class="sidebar-section">
          <button 
            class="section-header"
            :class="{ 'section-header-active': expandedSection === 'warehouse' }"
            @click="toggleSection('warehouse')"
          >
            <component :is="WarehouseSectionIcon" class="section-icon" />
            <span class="section-label">Warehouse</span>
            <svg 
              class="section-chevron" 
              :class="{ 'section-chevron-expanded': expandedSection === 'warehouse' }"
              fill="none" 
              stroke="currentColor" 
              viewBox="0 0 24 24"
            >
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
            </svg>
          </button>
          <div class="section-items" :class="{ 'section-items-expanded': expandedSection === 'warehouse' }">
            <router-link
              v-for="item in warehouseItems"
              :key="item.name"
              :to="item.href"
              :class="[
                'nav-item',
                isActive(item.href) ? 'nav-item-active' : ''
              ]"
            >
              <component :is="item.icon" class="nav-icon" />
              <span class="nav-text">{{ item.name }}</span>
            </router-link>
          </div>
        </div>
        -->

        <!-- Settings Section -->
        <div class="sidebar-section">
          <button 
            class="section-header"
            :class="{ 'section-header-active': expandedSection === 'settings' }"
            @click="toggleSection('settings')"
          >
            <component :is="SettingsSectionIcon" class="section-icon" />
            <span class="section-label">Settings</span>
            <svg 
              class="section-chevron" 
              :class="{ 'section-chevron-expanded': expandedSection === 'settings' }"
              fill="none" 
              stroke="currentColor" 
              viewBox="0 0 24 24"
            >
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
            </svg>
          </button>
          <div class="section-items" :class="{ 'section-items-expanded': expandedSection === 'settings' }">
            <router-link
              v-for="item in settingsItems"
              :key="item.name"
              :to="item.href"
              :class="[
                'nav-item',
                isActive(item.href) ? 'nav-item-active' : ''
              ]"
            >
              <component :is="item.icon" class="nav-icon" />
              <span class="nav-text">{{ item.name }}</span>
            </router-link>
          </div>
        </div>
      </template>

      <!-- Empty State -->
      <div v-else class="sidebar-empty-state">
        <p class="empty-text">Loading project...</p>
      </div>
    </nav>

    <!-- Footer Links -->
    <div v-if="user?.is_platform_admin || isOrgAdmin" class="sidebar-footer-links">
      <router-link
        v-if="user?.is_platform_admin"
        to="/admin"
        :class="['footer-nav-item', isActive('/admin') ? 'footer-nav-item-active' : '']"
      >
        <svg class="footer-nav-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
          <path stroke-linecap="round" stroke-linejoin="round" d="M10.5 6h9.75M10.5 6a1.5 1.5 0 11-3 0m3 0a1.5 1.5 0 10-3 0M3.75 6H7.5m3 12h9.75m-9.75 0a1.5 1.5 0 01-3 0m3 0a1.5 1.5 0 00-3 0m-3.75 0H7.5m9-6h3.75m-3.75 0a1.5 1.5 0 01-3 0m3 0a1.5 1.5 0 00-3 0m-9.75 0h9.75" />
        </svg>
        <span class="footer-nav-text">Admin</span>
      </router-link>
      <router-link
        v-if="isOrgAdmin"
        to="/settings/billing"
        :class="['footer-nav-item', isActive('/settings/billing') ? 'footer-nav-item-active' : '']"
      >
        <svg class="footer-nav-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
          <path stroke-linecap="round" stroke-linejoin="round" d="M2.25 8.25h19.5M2.25 9h19.5m-16.5 5.25h6m-6 2.25h3m-3.75 3h15a2.25 2.25 0 002.25-2.25V6.75A2.25 2.25 0 0019.5 4.5h-15a2.25 2.25 0 00-2.25 2.25v10.5A2.25 2.25 0 004.5 19.5z" />
        </svg>
        <span class="footer-nav-text">Billing & Usage</span>
      </router-link>
    </div>

    <!-- User Section -->
    <div class="sidebar-footer">
      <div class="user-info">
        <div class="user-avatar">
          {{ user?.email?.charAt(0).toUpperCase() || 'U' }}
        </div>
        <div class="user-details">
          <div class="user-email">{{ user?.email }}</div>
        </div>
        <button
          @click="$emit('logout')"
          class="logout-btn"
          title="Logout"
        >
          <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1" />
          </svg>
        </button>
      </div>
    </div>
  </div>

  <!-- Mobile overlay -->
  <div
    v-if="isOpen && isMobile"
    class="sidebar-overlay"
    @click="$emit('close')"
  ></div>
</template>

<script setup>
import { computed, ref, watch, onMounted, onUnmounted } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import axios from 'axios';
import { registerProjects } from '../composables/projectResolver';
import { useEntitlements } from '@/composables/useEntitlements';
import ReiverLogo from '@/components/ReiverLogo.vue';

const props = defineProps({
  user: Object,
  currentProject: Object,
  isOpen: {
    type: Boolean,
    default: false
  },
  isMobile: {
    type: Boolean,
    default: false
  }
});

defineEmits(['close', 'logout']);

const route = useRoute();
const router = useRouter();

const { fetchEntitlements, hasProduct, hasFeature } = useEntitlements();
watch(() => props.currentProject?.id, (id) => {
  if (id) fetchEntitlements(props.currentProject.slug || id);
}, { immediate: true });

// Project switcher state
const projectDropdownOpen = ref(false);
const allProjects = ref([]);

const toggleProjectDropdown = () => {
  projectDropdownOpen.value = !projectDropdownOpen.value;
  if (projectDropdownOpen.value) {
    fetchProjects();
  }
};

const fetchProjects = async () => {
  try {
    const response = await axios.get('/api/projects');
    allProjects.value = response.data || [];
    registerProjects(allProjects.value);
  } catch {
    // Silently fail
  }
};

const groupedProjects = computed(() => {
  const orgMap = new Map();
  for (const project of allProjects.value) {
    const orgId = project.organization_id;
    if (!orgMap.has(orgId)) {
      orgMap.set(orgId, {
        id: orgId,
        name: project.organization_name || 'Organization',
        projects: [],
      });
    }
    orgMap.get(orgId).projects.push(project);
  }
  return Array.from(orgMap.values());
});

const switchProject = (project) => {
  projectDropdownOpen.value = false;
  localStorage.setItem('dh_current_project_id', project.id);
  const slug = project.slug || project.id;
  const target = `/p/${slug}/dashboards`;
  if (route.params.id === slug || route.params.id === project.id) return;
  window.location.href = target;
};

const handleClickOutside = (e) => {
  if (projectDropdownOpen.value && !e.target.closest('.project-switcher')) {
    projectDropdownOpen.value = false;
  }
};

onMounted(() => {
  document.addEventListener('click', handleClickOutside);
});

onUnmounted(() => {
  document.removeEventListener('click', handleClickOutside);
});

// Accordion state - only one section expanded at a time
const expandedSection = ref('observability');

// Toggle section (accordion behavior)
const toggleSection = (section) => {
  if (expandedSection.value === section) {
    // Allow collapsing current section
    expandedSection.value = null;
  } else {
    expandedSection.value = section;
  }
};

// Auto-expand section based on current route
const detectActiveSection = () => {
  const path = route.path;
  if (path.includes('/llm/')) {
    return 'llm';
  } else if (path.includes('/herd/')) {
    return 'herd';
  // Warehouse disabled — re-enable when Pond launches
  // } else if (path.includes('/warehouse/')) {
  //   return 'warehouse';
  } else if (/\/p\/[^/]+\/settings/.test(path)) {
    return 'settings';
  } else if (path.startsWith('/settings/sso') || path.startsWith('/settings/scim') || path.startsWith('/settings/members') || path.startsWith('/settings/audit')) {
    return 'settings';
  } else if (
    path.includes('/dashboards') || path.includes('/services') ||
    path.includes('/system-overview') ||
    path.includes('/traces') ||
    path.includes('/logs') || path.includes('/metrics') ||
    path.includes('/exceptions') || path.includes('/api-monitoring') ||
    path.includes('/infrastructure') || path.includes('/profiles') ||
    path.includes('/alerts') || path.includes('/integrations')
  ) {
    return 'observability';
  }
  if (hasProduct('prompt_hub')) return 'llm';
  if (hasProduct('herd')) return 'herd';
  return 'settings';
};

// Watch route changes to auto-expand the correct section
watch(() => route.path, () => {
  expandedSection.value = detectActiveSection();
});

onMounted(() => {
  expandedSection.value = detectActiveSection();
});

// Section Icons
const ObservabilitySectionIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
    </svg>
  `
};

const LlmSectionIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
    </svg>
  `
};

const WarehouseSectionIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4" />
    </svg>
  `
};

const SettingsSectionIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
      <path stroke-linecap="round" stroke-linejoin="round" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
    </svg>
  `
};

const AgentsSectionIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M9.75 3.104v5.714a2.25 2.25 0 01-.659 1.591L5 14.5M9.75 3.104c-.251.023-.501.05-.75.082m.75-.082a24.301 24.301 0 014.5 0m0 0v5.714a2.25 2.25 0 00.659 1.591L19 14.5m-4.75-11.396c.251.023.501.05.75.082M19 14.5l-2.47 2.47a2.25 2.25 0 01-1.59.659H9.06a2.25 2.25 0 01-1.591-.659L5 14.5m14 0V17a2 2 0 01-2 2H7a2 2 0 01-2-2v-2.5" />
    </svg>
  `
};

const AgentToolsIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M11.42 15.17l-5.384 5.383a1.5 1.5 0 01-2.12-2.122l5.383-5.383m2.121 2.122l5.383-5.383a1.5 1.5 0 00-2.121-2.122l-5.383 5.383m2.121 2.122l-2.121-2.122" />
    </svg>
  `
};

const AgentAnalyticsIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 013 19.875v-6.75zM9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V8.625zM16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V4.125z" />
    </svg>
  `
};

const AgentTokensIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M15.75 5.25a3 3 0 013 3m3 0a6 6 0 01-7.029 5.912c-.563-.097-1.159.026-1.563.43L10.5 17.25H8.25v2.25H6v2.25H2.25v-2.818c0-.597.237-1.17.659-1.591l6.499-6.499c.404-.404.527-1 .43-1.563A6 6 0 1121.75 8.25z" />
    </svg>
  `
};

// Icon components
const DashboardIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-6 0a1 1 0 001-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 001 1m-6 0h6" />
    </svg>
  `
};

const ErrorIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
    </svg>
  `
};

const TracingIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
    </svg>
  `
};

const LogsIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M4 6h16M4 10h16M4 14h16M4 18h16" />
    </svg>
  `
};

const IntegrationsIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M8 9l3 3-3 3m5 0h3M5 20h14a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
    </svg>
  `
};

const SettingsIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
      <path stroke-linecap="round" stroke-linejoin="round" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
    </svg>
  `
};

const MembersIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M18 9v3m0 0v3m0-3h3m-3 0h-3m-2-5a4 4 0 11-8 0 4 4 0 018 0zM3 20a6 6 0 0112 0v1H3v-1z" />
    </svg>
  `
};

const SsoIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M15 7a2 2 0 012 2m4 0a6 6 0 01-7.743 5.743L11 17H9v2H7v2H4a1 1 0 01-1-1v-2.586a1 1 0 01.293-.707l5.964-5.964A6 6 0 1121 9z" />
    </svg>
  `
};

const ScimIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z" />
    </svg>
  `
};

const AuditLogIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-3 7h3m-3 4h3m-6-4h.01M9 16h.01" />
    </svg>
  `
};

const AlertIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M15 17h5l-1.405-1.405A2.032 2.032 0 0118 14.158V11a6.002 6.002 0 00-4-5.659V5a2 2 0 10-4 0v.341C7.67 6.165 6 8.388 6 11v3.159c0 .538-.214 1.055-.595 1.436L4 17h5m6 0v1a3 3 0 11-6 0v-1m6 0H9" />
    </svg>
  `
};

const MaintenanceIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
      <path stroke-linecap="round" stroke-linejoin="round" d="M9 14l2 2 4-4" />
    </svg>
  `
};

const ServicesIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1" />
    </svg>
  `
};

const SystemOverviewIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M3.75 6A2.25 2.25 0 016 3.75h2.25A2.25 2.25 0 0110.5 6v2.25a2.25 2.25 0 01-2.25 2.25H6a2.25 2.25 0 01-2.25-2.25V6zM3.75 15.75A2.25 2.25 0 016 13.5h2.25a2.25 2.25 0 012.25 2.25V18a2.25 2.25 0 01-2.25 2.25H6A2.25 2.25 0 013.75 18v-2.25zM13.5 6a2.25 2.25 0 012.25-2.25H18A2.25 2.25 0 0120.25 6v2.25A2.25 2.25 0 0118 10.5h-2.25a2.25 2.25 0 01-2.25-2.25V6zM13.5 15.75a2.25 2.25 0 012.25-2.25H18a2.25 2.25 0 012.25 2.25V18A2.25 2.25 0 0118 20.25h-2.25A2.25 2.25 0 0113.5 18v-2.25z" />
    </svg>
  `
};

const MetricsIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
    </svg>
  `
};

const ApiMonitoringIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M8 9l3 3-3 3m5 0h3M5 20h14a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
    </svg>
  `
};

const InfrastructureIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 00-2 2v4a2 2 0 002 2h14a2 2 0 002-2v-4a2 2 0 00-2-2m-2-4h.01M17 16h.01" />
    </svg>
  `
};

const ProfilesIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M17.657 18.657A8 8 0 016.343 7.343S7 9 9 10c0-2 .5-5 2.986-7C14 5 16.09 5.777 17.656 7.343A7.975 7.975 0 0120 13a7.975 7.975 0 01-2.343 5.657z" />
      <path stroke-linecap="round" stroke-linejoin="round" d="M9.879 16.121A3 3 0 1012.015 11L11 14H9l1.015 2.121z" />
    </svg>
  `
};

// Prompt Hub Icons
const LlmOverviewIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
    </svg>
  `
};

const LlmSessionsIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z" />
    </svg>
  `
};

const LlmPromptsIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
    </svg>
  `
};

const LlmRolloutsIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
    </svg>
  `
};

const LlmPlaygroundIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z" />
      <path stroke-linecap="round" stroke-linejoin="round" d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
    </svg>
  `
};

const LlmIntegrationsIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1" />
    </svg>
  `
};

const LlmSettingsIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M12 6V4m0 2a2 2 0 100 4m0-4a2 2 0 110 4m-6 8a2 2 0 100-4m0 4a2 2 0 110-4m0 4v2m0-6V4m6 6v10m6-2a2 2 0 100-4m0 4a2 2 0 110-4m0 4v2m0-6V4" />
    </svg>
  `
};

const GuardrailsIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M9 12.75L11.25 15 15 9.75m-3-7.036A11.959 11.959 0 013.598 6 11.99 11.99 0 003 9.749c0 5.592 3.824 10.29 9 11.623 5.176-1.332 9-6.03 9-11.622 0-1.31-.21-2.571-.598-3.751h-.152c-3.196 0-6.1-1.248-8.25-3.285z" />
    </svg>
  `
};

const LlmMooDengIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M9.813 15.904L9 18.75l-.813-2.846a4.5 4.5 0 00-3.09-3.09L2.25 12l2.846-.813a4.5 4.5 0 003.09-3.09L9 5.25l.813 2.846a4.5 4.5 0 003.09 3.09L15.75 12l-2.846.813a4.5 4.5 0 00-3.09 3.09zM18.259 8.715L18 9.75l-.259-1.035a3.375 3.375 0 00-2.455-2.456L14.25 6l1.036-.259a3.375 3.375 0 002.455-2.456L18 2.25l.259 1.035a3.375 3.375 0 002.455 2.456L21.75 6l-1.036.259a3.375 3.375 0 00-2.455 2.456z" />
    </svg>
  `
};

const LlmCompilerIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M13 10V3L4 14h7v7l9-11h-7z" />
    </svg>
  `
};

// Warehouse Icons
const WarehouseOverviewIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4" />
    </svg>
  `
};

const WarehouseQueriesIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
    </svg>
  `
};

const WarehouseTablesIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M3 10h18M3 14h18m-9-4v8m-7 0h14a2 2 0 002-2V8a2 2 0 00-2-2H5a2 2 0 00-2 2v8a2 2 0 002 2z" />
    </svg>
  `
};

const WarehouseIntegrationsIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1" />
    </svg>
  `
};

const WarehousePipelinesIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M3 8h4m0 0a2 2 0 104 0m-4 0a2 2 0 114 0m0 0h4m0 0a2 2 0 104 0m-4 0a2 2 0 114 0m0 0h3M3 16h4m0 0a2 2 0 104 0m-4 0a2 2 0 114 0m0 0h10" />
    </svg>
  `
};

const WarehouseSettingsIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M12 6V4m0 2a2 2 0 100 4m0-4a2 2 0 110 4m-6 8a2 2 0 100-4m0 4a2 2 0 110-4m0 4v2m0-6V4m6 6v10m6-2a2 2 0 100-4m0 4a2 2 0 110-4m0 4v2m0-6V4" />
    </svg>
  `
};

// Herd section icon (hippo herd / connected agents)
const HerdSectionIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M13 10V3L4 14h7v7l9-11h-7z" />
    </svg>
  `
};

const HerdOverviewIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-4 0a1 1 0 01-1-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 01-1 1h-2z" />
    </svg>
  `
};

const HerdAgentsIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
    </svg>
  `
};

const HerdDiscoveryIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
    </svg>
  `
};

const HerdAccessIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M15 7a2 2 0 012 2m4 0a6 6 0 01-7.743 5.743L11 17H9v2H7v2H4a1 1 0 01-1-1v-2.586a1 1 0 01.293-.707l5.964-5.964A6 6 0 1121 9z" />
    </svg>
  `
};

const HerdSettingsIcon = {
  template: `
    <svg fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2">
      <path stroke-linecap="round" stroke-linejoin="round" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.066 2.573c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.573 1.066c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.066-2.573c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
      <path stroke-linecap="round" stroke-linejoin="round" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
    </svg>
  `
};

const pslug = computed(() => props.currentProject?.slug || props.currentProject?.id);

// Observability items
const observabilityItems = computed(() => {
  return [
    {
      name: 'Dashboards',
      href: props.currentProject ? `/p/${pslug.value}/dashboards` : '/dashboards',
      icon: DashboardIcon,
    },
    {
      name: 'Services',
      href: props.currentProject ? `/p/${pslug.value}/services` : '/projects',
      icon: ServicesIcon,
    },
    {
      name: 'System Overview',
      href: props.currentProject ? `/p/${pslug.value}/system-overview` : '/projects',
      icon: SystemOverviewIcon,
    },
    {
      name: 'Traces',
      href: props.currentProject ? `/p/${pslug.value}/traces` : '/projects',
      icon: TracingIcon,
    },
    {
      name: 'Logs',
      href: props.currentProject ? `/p/${pslug.value}/logs` : '/projects',
      icon: LogsIcon,
    },
    {
      name: 'Metrics',
      href: props.currentProject ? `/p/${pslug.value}/metrics` : '/projects',
      icon: MetricsIcon,
    },
    {
      name: 'Exceptions',
      href: props.currentProject ? `/p/${pslug.value}/exceptions` : '/projects',
      icon: ErrorIcon,
    },
    {
      name: 'API Monitoring',
      href: props.currentProject ? `/p/${pslug.value}/api-monitoring` : '/projects',
      icon: ApiMonitoringIcon,
    },
    {
      name: 'Infrastructure',
      href: props.currentProject ? `/p/${pslug.value}/infrastructure` : '/projects',
      icon: InfrastructureIcon,
    },
    {
      name: 'Profiles',
      href: props.currentProject ? `/p/${pslug.value}/profiles` : '/projects',
      icon: ProfilesIcon,
    },
    {
      name: 'Alerts',
      href: props.currentProject ? `/p/${pslug.value}/alerts` : '/alerts',
      icon: AlertIcon,
    },
    {
      name: 'Integrations',
      href: props.currentProject ? `/p/${pslug.value}/integrations` : '/projects',
      icon: IntegrationsIcon,
    },
  ];
});

// Settings section items
const isOrgAdmin = computed(() => {
  return props.user?.is_platform_admin || ['owner', 'admin'].includes(props.user?.org_role);
});

const settingsItems = computed(() => {
  const items = [
    {
      name: 'General',
      href: props.currentProject ? `/p/${pslug.value}/settings` : '/projects',
      icon: SettingsIcon,
    },
  ];
  if (isOrgAdmin.value) {
    items.push({ name: 'Members', href: '/settings/members', icon: MembersIcon });
    if (hasFeature('sso')) {
      items.push(
        { name: 'SSO', href: '/settings/sso', icon: SsoIcon },
        { name: 'SCIM', href: '/settings/scim', icon: ScimIcon },
      );
    }
    if (hasFeature('audit_log')) {
      items.push({ name: 'Audit Log', href: '/settings/audit', icon: AuditLogIcon });
    }
  }
  return items;
});

// Prompt Hub items
const promptHubItems = computed(() => {
  return [
    {
      name: 'Overview',
      href: props.currentProject ? `/p/${pslug.value}/llm/overview` : '/projects',
      icon: LlmOverviewIcon,
    },
    {
      name: 'Sessions',
      href: props.currentProject ? `/p/${pslug.value}/llm/sessions` : '/projects',
      icon: LlmSessionsIcon,
    },
    {
      name: 'Users',
      href: props.currentProject ? `/p/${pslug.value}/llm/users` : '/projects',
      icon: LlmSessionsIcon,
    },
    {
      name: 'Prompts',
      href: props.currentProject ? `/p/${pslug.value}/llm/prompts` : '/projects',
      icon: LlmPromptsIcon,
    },
    {
      name: 'Compiler',
      href: props.currentProject ? `/p/${pslug.value}/llm/compiler` : '/projects',
      icon: LlmCompilerIcon,
    },
    {
      name: 'Rollouts',
      href: props.currentProject ? `/p/${pslug.value}/llm/rollouts` : '/projects',
      icon: LlmRolloutsIcon,
    },
    {
      name: 'Playground',
      href: props.currentProject ? `/p/${pslug.value}/llm/playground` : '/projects',
      icon: LlmPlaygroundIcon,
    },
    {
      name: 'Integrations',
      href: props.currentProject ? `/p/${pslug.value}/llm/integrations` : '/projects',
      icon: LlmIntegrationsIcon,
    },
    {
      name: 'Agents',
      href: props.currentProject ? `/p/${pslug.value}/llm/agents` : '/projects',
      icon: AgentToolsIcon,
    },
    {
      name: 'Guardrails',
      href: props.currentProject ? `/p/${pslug.value}/llm/guardrails` : '/projects',
      icon: GuardrailsIcon,
    },
    {
      name: 'Settings',
      href: props.currentProject ? `/p/${pslug.value}/llm/settings` : '/projects',
      icon: LlmSettingsIcon,
    },
  ];
});

// Herd items (A2A Agent Registry)
const herdItems = computed(() => {
  return [
    {
      name: 'Overview',
      href: props.currentProject ? `/p/${pslug.value}/herd/overview` : '/projects',
      icon: HerdOverviewIcon,
    },
    {
      name: 'Agents',
      href: props.currentProject ? `/p/${pslug.value}/herd/agents` : '/projects',
      icon: HerdAgentsIcon,
    },
    {
      name: 'Discovery',
      href: props.currentProject ? `/p/${pslug.value}/herd/discovery` : '/projects',
      icon: HerdDiscoveryIcon,
    },
    {
      name: 'Access',
      href: props.currentProject ? `/p/${pslug.value}/herd/access` : '/projects',
      icon: HerdAccessIcon,
    },
    {
      name: 'Settings',
      href: props.currentProject ? `/p/${pslug.value}/herd/settings` : '/projects',
      icon: HerdSettingsIcon,
    },
  ];
});

// Warehouse items (disabled — re-enable when Pond launches)
// const warehouseItems = computed(() => {
//   return [
//     {
//       name: 'Sources',
//       href: props.currentProject ? `/p/${props.currentProject.id}/warehouse/sources` : '/projects',
//       icon: WarehouseIntegrationsIcon,
//     },
//     {
//       name: 'Pipelines',
//       href: props.currentProject ? `/p/${props.currentProject.id}/warehouse/pipelines` : '/projects',
//       icon: WarehousePipelinesIcon,
//     },
//     {
//       name: 'UDFs',
//       href: props.currentProject ? `/p/${props.currentProject.id}/warehouse/udfs` : '/projects',
//       icon: WarehousePipelinesIcon,
//     },
//     {
//       name: 'Queries',
//       href: props.currentProject ? `/p/${props.currentProject.id}/warehouse/queries` : '/projects',
//       icon: WarehouseQueriesIcon,
//     },
//   ];
// });


const isActive = (href) => {
  if (href === '/projects') {
    return route.path === href;
  }
  return route.path.startsWith(href);
};
</script>

<style scoped>
.app-sidebar {
  position: fixed;
  left: 0;
  top: 0;
  bottom: 0;
  width: 240px;
  background-color: #ffffff;
  border-right: 1px solid #e5e7eb;
  display: flex;
  flex-direction: column;
  z-index: 50;
  font-family: 'Inter', sans-serif;
}

.sidebar-header {
  height: 64px;
  padding: 0 16px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  border-bottom: 1px solid #e5e7eb;
}

.sidebar-logo {
  display: flex;
  align-items: center;
  gap: 8px;
  text-decoration: none;
  color: #111827;
  font-weight: 600;
  font-size: 16px;
}

.logo-icon {
  width: 24px;
  height: 24px;
  color: #4f46e5;
}

.logo-text {
  font-weight: 600;
  letter-spacing: -0.02em;
}

.sidebar-close-btn {
  color: #9ca3af;
  background: none;
  border: none;
  cursor: pointer;
  padding: 4px;
  border-radius: 4px;
  transition: all 0.2s;
}

.sidebar-close-btn:hover {
  color: #374151;
  background-color: #f3f4f6;
}

.sidebar-nav {
  flex: 1;
  overflow-y: auto;
  padding: 16px 8px;
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.sidebar-section {
  display: flex;
  flex-direction: column;
}

.section-header {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 10px 12px;
  margin: 0 4px;
  background: none;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  transition: all 0.2s;
  width: calc(100% - 8px);
  text-align: left;
}

.section-header:hover {
  background-color: #f3f4f6;
}

.section-header-active {
  background-color: #eef2ff;
}

.section-icon {
  width: 20px;
  height: 20px;
  color: #9ca3af;
  flex-shrink: 0;
}

.section-header:hover .section-icon,
.section-header-active .section-icon {
  color: #374151;
}

.section-label {
  flex: 1;
  font-size: 13px;
  font-weight: 500;
  color: #6b7280;
  letter-spacing: 0.01em;
}

.section-header:hover .section-label,
.section-header-active .section-label {
  color: #111827;
}

.section-chevron {
  width: 16px;
  height: 16px;
  color: #9ca3af;
  transition: transform 0.2s ease;
  flex-shrink: 0;
}

.section-chevron-expanded {
  transform: rotate(180deg);
}

.section-items {
  max-height: 0;
  overflow: hidden;
  transition: max-height 0.25s ease-out;
}

.section-items-expanded {
  max-height: 500px;
  transition: max-height 0.3s ease-in;
}

.nav-item {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 8px 16px 8px 42px;
  color: #6b7280;
  text-decoration: none;
  border-radius: 6px;
  margin: 2px 4px;
  transition: all 0.2s;
  font-size: 13px;
  font-weight: 400;
}

.nav-item:hover {
  background-color: #f3f4f6;
  color: #111827;
}

.nav-item-active {
  background-color: #eef2ff;
  color: #4f46e5;
  border-left: 2px solid #4f46e5;
  padding-left: 40px;
}

.nav-icon {
  width: 16px;
  height: 16px;
  flex-shrink: 0;
}

.nav-text {
  flex: 1;
}

.sidebar-empty-state {
  padding: 24px 16px;
  text-align: center;
}

.empty-text {
  font-size: 14px;
  color: #9ca3af;
}

.sidebar-footer-links {
  border-top: 1px solid #e5e7eb;
  padding: 8px 8px 0;
}

.footer-nav-item {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 9px 12px;
  margin: 0 4px;
  color: #6b7280;
  text-decoration: none;
  border-radius: 6px;
  transition: all 0.2s;
  font-size: 13px;
  font-weight: 400;
}

.footer-nav-item:hover {
  background-color: #f3f4f6;
  color: #111827;
}

.footer-nav-item-active {
  background-color: #eef2ff;
  color: #4f46e5;
}

.footer-nav-icon {
  width: 18px;
  height: 18px;
  flex-shrink: 0;
}

.footer-nav-text {
  flex: 1;
}

.sidebar-footer {
  border-top: 1px solid #e5e7eb;
  padding: 12px 16px;
}

.user-info {
  display: flex;
  align-items: center;
  gap: 12px;
}

.user-avatar {
  width: 32px;
  height: 32px;
  border-radius: 50%;
  background: linear-gradient(135deg, #4f46e5 0%, #6366f1 100%);
  color: #ffffff;
  display: flex;
  align-items: center;
  justify-content: center;
  font-weight: 600;
  font-size: 14px;
  flex-shrink: 0;
}

.user-details {
  flex: 1;
  min-width: 0;
}

.user-email {
  font-size: 13px;
  color: #111827;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.logout-btn {
  color: #9ca3af;
  background: none;
  border: none;
  cursor: pointer;
  padding: 4px;
  border-radius: 4px;
  transition: all 0.2s;
  flex-shrink: 0;
}

.logout-btn:hover {
  color: #374151;
  background-color: #f3f4f6;
}

.sidebar-overlay {
  position: fixed;
  inset: 0;
  background-color: rgba(0, 0, 0, 0.3);
  z-index: 40;
}

.sidebar-nav::-webkit-scrollbar {
  width: 6px;
}

.sidebar-nav::-webkit-scrollbar-track {
  background: transparent;
}

.sidebar-nav::-webkit-scrollbar-thumb {
  background: #cbd5e1;
  border-radius: 3px;
}

.sidebar-nav::-webkit-scrollbar-thumb:hover {
  background: #94a3b8;
}

/* Project Switcher */
.project-switcher {
  position: relative;
  padding: 8px;
  border-bottom: 1px solid #e5e7eb;
}

.project-switcher-btn {
  display: flex;
  align-items: center;
  justify-content: space-between;
  width: 100%;
  padding: 8px 12px;
  background: none;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  cursor: pointer;
  transition: all 0.15s;
}

.project-switcher-btn:hover {
  background-color: #f9fafb;
  border-color: #d1d5db;
}

.project-switcher-info {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  min-width: 0;
}

.project-switcher-name {
  font-size: 13px;
  font-weight: 600;
  color: #111827;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  max-width: 170px;
}

.project-switcher-org {
  font-size: 11px;
  color: #9ca3af;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  max-width: 170px;
}

.project-switcher-chevron {
  width: 16px;
  height: 16px;
  color: #9ca3af;
  flex-shrink: 0;
  transition: transform 0.15s;
}

.project-switcher-chevron-open {
  transform: rotate(180deg);
}

.project-dropdown {
  position: absolute;
  top: calc(100% + 4px);
  left: 8px;
  right: 8px;
  background: #ffffff;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -2px rgba(0, 0, 0, 0.1);
  z-index: 60;
  overflow: hidden;
}

.project-dropdown-scroll {
  max-height: 280px;
  overflow-y: auto;
  padding: 4px 0;
}

.project-dropdown-org-label {
  padding: 6px 12px 4px;
  font-size: 11px;
  font-weight: 600;
  color: #9ca3af;
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.project-dropdown-item {
  display: flex;
  align-items: center;
  justify-content: space-between;
  width: 100%;
  padding: 7px 12px;
  background: none;
  border: none;
  cursor: pointer;
  transition: background-color 0.1s;
  text-align: left;
}

.project-dropdown-item:hover {
  background-color: #f3f4f6;
}

.project-dropdown-item-active {
  background-color: #eef2ff;
}

.project-dropdown-item-name {
  font-size: 13px;
  color: #374151;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.project-dropdown-item-active .project-dropdown-item-name {
  color: #4f46e5;
  font-weight: 500;
}

.project-dropdown-check {
  width: 16px;
  height: 16px;
  color: #4f46e5;
  flex-shrink: 0;
}

.project-dropdown-create {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  border-top: 1px solid #e5e7eb;
  color: #6b7280;
  font-size: 13px;
  text-decoration: none;
  transition: all 0.1s;
}

.project-dropdown-create:hover {
  background-color: #f3f4f6;
  color: #111827;
}

.project-dropdown-create-icon {
  width: 16px;
  height: 16px;
}
</style>
