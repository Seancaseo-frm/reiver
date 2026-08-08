import { createApp } from 'vue';
import { createRouter, createWebHistory } from 'vue-router';
import axios from 'axios';
import './css/app.css';

import App from './App.vue';
import Home from './Pages/Home.vue';
import Login from './Pages/Auth/Login.vue';
import ProjectsCreate from './Pages/Projects/Create.vue';
import ErrorsIndex from './Pages/Errors/Index.vue';
import ErrorsShow from './Pages/Errors/Show.vue';
import TracesIndex from './Pages/Traces/Index.vue';
import LogsIndex from './Pages/Logs/Index.vue';
import IncidentsIndex from './Pages/Incidents/Index.vue';
import IncidentsErrorDetail from './Pages/Incidents/ErrorDetail.vue';
import IncidentsDetail from './Pages/Incidents/Detail.vue';
import TracingShow from './Pages/Tracing/Show.vue';
import LogsShow from './Pages/Logs/Show.vue';
import ProjectSettings from './Pages/Projects/Settings.vue';
import IntegrationsIndex from './Pages/Integrations/Index.vue';
import DashboardsIndex from './Pages/Dashboards/Index.vue';
import DashboardsShow from './Pages/Dashboards/Show.vue';
import DashboardsEdit from './Pages/Dashboards/Edit.vue';
import DashboardsNew from './Pages/Dashboards/New.vue';
import AlertsIndex from './Pages/Alerts/Index.vue';
import AlertsNew from './Pages/Alerts/New.vue';
import { useAuth } from './composables/useAuth';
import { useCurrentProject } from './composables/useCurrentProject';
import { useEntitlements } from './composables/useEntitlements';
import { getProjectRedirectPath } from './composables/useProjectRedirect';
import { registerProject, isUuid, isKnownSlug, resolveApiUrl, resolveSlug } from './composables/projectResolver';

// Configure axios - use relative URLs so Vite proxy works
axios.defaults.withCredentials = true;

// Add token to requests from cookie
axios.interceptors.request.use((config) => {
  // Read token from cookie
  const cookies = document.cookie.split('; ');
  const tokenCookie = cookies.find(row => row.trim().startsWith('token='));
  if (tokenCookie) {
    const tokenValue = tokenCookie.split('=').slice(1).join('=');
    const token = decodeURIComponent(tokenValue);
    if (token && token.trim()) {
      config.headers.Authorization = `Bearer ${token}`;
    }
  }
  // Transparently replace project slugs with UUIDs in API URLs, params, body, and headers
  if (config.url) {
    config.url = resolveApiUrl(config.url);
  }
  if (config.params?.project_id) {
    config.params.project_id = resolveSlug(config.params.project_id);
  }
  if (config.data?.project_id) {
    config.data.project_id = resolveSlug(config.data.project_id);
  }
  if (config.headers?.['x-project-id']) {
    config.headers['x-project-id'] = resolveSlug(config.headers['x-project-id']);
  }
  return config;
});

axios.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401 && !window.location.pathname.startsWith('/login')) {
      document.cookie = 'token=; expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/;';
      const redirect = window.location.pathname !== '/' ? `?redirect=${encodeURIComponent(window.location.pathname)}` : '';
      window.location.href = `/login${redirect}`;
    }
    return Promise.reject(error);
  }
);

// Router setup
const routes = [
  {
    path: '/',
    component: Home,
    beforeEnter: async (to, from, next) => {
      const cookies = document.cookie.split('; ');
      const hasToken = cookies.some(row => row.trim().startsWith('token='));
      if (!hasToken) return next();
      try {
        const { fetchUser } = useAuth();
        const userData = await fetchUser();
        if (userData && userData.id && userData.is_approved) {
          const path = await getProjectRedirectPath();
          return next(path);
        }
        next();
      } catch {
        next();
      }
    }
  },
  { path: '/products', component: () => import('./Pages/Products.vue') },
  { path: '/services', component: () => import('./Pages/Services.vue') },
  { path: '/model-catalog', component: () => import('./Pages/Pricing.vue') },
  { path: '/quickstart', component: () => import('./Pages/Quickstart/Index.vue') },
  { path: '/security', component: () => import('./Pages/Security/Index.vue') },
  { path: '/compare/datadog', component: () => import('./Pages/Compare/Datadog.vue') },
  { path: '/subprocessors', component: () => import('./Pages/Subprocessors/Index.vue') },
  { path: '/login', component: Login },
  { path: '/signup', redirect: '/login' },
  { path: '/dashboard', redirect: '/projects' },
  { 
    // Redirect /projects to user's project dashboards  
    path: '/projects', 
    meta: { requiresAuth: true, redirectToProject: true }
  },
  { 
    path: '/projects/create', 
    component: ProjectsCreate,
    meta: { requiresAuth: true }
  },
  // Backwards-compatible redirect: /projects/:id/... -> /p/:id/...
  {
    path: '/projects/:id/:rest(.*)',
    redirect: to => `/p/${to.params.id}/${to.params.rest}`,
  },
  {
    path: '/projects/:id',
    redirect: to => `/p/${to.params.id}`,
  },
  { 
    path: '/p/:id', 
    redirect: to => `/p/${to.params.id}/dashboards`,
    meta: { requiresAuth: true }
  },
  {
    path: '/p/:id/exceptions',
    component: ErrorsIndex,
    meta: { requiresAuth: true, requiresProduct: 'watch' }
  },
  {
    path: '/p/:id/exceptions/:group_id',
    component: ErrorsShow,
    meta: { requiresAuth: true, requiresProduct: 'watch' }
  },
  {
    path: '/p/:id/incidents',
    component: IncidentsIndex,
    meta: { requiresAuth: true, requiresProduct: 'watch' }
  },
  {
    path: '/p/:id/incidents/errors',
    component: IncidentsErrorDetail,
    meta: { requiresAuth: true, requiresProduct: 'watch' }
  },
  {
    path: '/p/:id/incidents/:type/:incident_id',
    component: IncidentsDetail,
    meta: { requiresAuth: true, requiresProduct: 'watch' }
  },
  {
    path: '/p/:id/traces',
    component: TracesIndex,
    meta: { requiresAuth: true, requiresProduct: 'watch' }
  },
  {
    path: '/p/:id/traces/:trace_id',
    component: TracingShow,
    meta: { requiresAuth: true, requiresProduct: 'watch' }
  },
  {
    path: '/p/:id/profiles',
    component: () => import('./Pages/Profiles/Index.vue'),
    meta: { requiresAuth: true, requiresProduct: 'watch' }
  },
  {
    path: '/p/:id/profiles/compare',
    component: () => import('./Pages/Profiles/Compare.vue'),
    meta: { requiresAuth: true, requiresProduct: 'watch' }
  },
  {
    path: '/p/:id/profiles/:profile_id',
    component: () => import('./Pages/Profiles/Show.vue'),
    meta: { requiresAuth: true, requiresProduct: 'watch' }
  },
  {
    path: '/p/:id/logs',
    component: LogsIndex,
    meta: { requiresAuth: true, requiresProduct: 'watch' }
  },
  {
    path: '/p/:id/logs/:log_id',
    component: LogsShow,
    meta: { requiresAuth: true, requiresProduct: 'watch' }
  },
  {
    path: '/p/:id/settings', 
    component: ProjectSettings,
    meta: { requiresAuth: true }
  },
  { 
    path: '/p/:id/integrations', 
    component: IntegrationsIndex,
    meta: { requiresAuth: true, requiresProduct: 'watch' }
  },
  { 
    path: '/p/:id/dashboards', 
    component: DashboardsIndex,
    meta: { requiresAuth: true, requiresProduct: 'watch' }
  },
  { 
    path: '/p/:id/dashboards/new', 
    component: DashboardsNew,
    meta: { requiresAuth: true, requiresProduct: 'watch' }
  },
  { 
    path: '/p/:id/dashboards/:dashboard_id', 
    component: DashboardsShow,
    meta: { requiresAuth: true, requiresProduct: 'watch' }
  },
  { 
    path: '/p/:id/dashboards/:dashboard_id/edit', 
    component: DashboardsEdit,
    meta: { requiresAuth: true, requiresProduct: 'watch' }
  },
  { 
    path: '/p/:id/alerts', 
    component: AlertsIndex,
    meta: { requiresAuth: true, requiresProduct: 'watch' }
  },
  { 
    path: '/p/:id/alerts/new', 
    component: AlertsNew,
    meta: { requiresAuth: true, requiresProduct: 'watch' }
  },
  { 
    path: '/p/:id/alerts/:ruleId/edit', 
    component: () => import('./Pages/Alerts/Edit.vue'),
    meta: { requiresAuth: true, requiresProduct: 'watch' }
  },
  {
    path: '/p/:id/alerts/history',
    component: () => import('./Pages/Alerts/History.vue'),
    meta: { requiresAuth: true, requiresProduct: 'watch' }
  },
  {
    path: '/p/:id/maintenance',
    component: () => import('./Pages/Maintenance/Index.vue'),
    meta: { requiresAuth: true, requiresProduct: 'watch' }
  },
  {
    path: '/p/:id/maintenance/new',
    component: () => import('./Pages/Maintenance/New.vue'),
    meta: { requiresAuth: true, requiresProduct: 'watch' }
  },
  {
    path: '/p/:id/maintenance/:windowId/edit',
    component: () => import('./Pages/Maintenance/Edit.vue'),
    meta: { requiresAuth: true, requiresProduct: 'watch' }
  },
  // Warehouse - Federated Data Sources (disabled — re-enable when Pond launches)
  // {
  //   path: '/p/:id/warehouse/sources',
  //   component: () => import('./Pages/Warehouse/Sources.vue'),
  //   meta: { requiresAuth: true }
  // },
  // {
  //   path: '/p/:id/warehouse/sources/add',
  //   component: () => import('./Pages/Warehouse/AddSource.vue'),
  //   meta: { requiresAuth: true }
  // },
  // {
  //   path: '/p/:id/warehouse/sources/:source_name/tables/:table_name',
  //   component: () => import('./Pages/Warehouse/TableDetail.vue'),
  //   meta: { requiresAuth: true }
  // },
  // {
  //   path: '/p/:id/warehouse/queries',
  //   component: () => import('./Pages/Warehouse/Queries.vue'),
  //   meta: { requiresAuth: true }
  // },
  // {
  //   path: '/p/:id/warehouse/compliance',
  //   component: () => import('./Pages/Warehouse/Compliance.vue'),
  //   meta: { requiresAuth: true }
  // },
  // {
  //   path: '/p/:id/warehouse/pipelines',
  //   component: () => import('./Pages/Warehouse/Pipelines.vue'),
  //   meta: { requiresAuth: true }
  // },
  // {
  //   path: '/p/:id/warehouse/pipelines/new',
  //   component: () => import('./Pages/Warehouse/PipelineEditor.vue'),
  //   meta: { requiresAuth: true }
  // },
  // {
  //   path: '/p/:id/warehouse/pipelines/:pipeline_id/edit',
  //   component: () => import('./Pages/Warehouse/PipelineEditor.vue'),
  //   meta: { requiresAuth: true }
  // },
  // {
  //   path: '/p/:id/warehouse/udfs',
  //   component: () => import('./Pages/Warehouse/Udfs.vue'),
  //   meta: { requiresAuth: true }
  // },
  // LLM Prompt Management
  {
    path: '/p/:id/llm/prompts',
    component: () => import('./Pages/Llm/Prompts/Index.vue'),
    meta: { requiresAuth: true, requiresProduct: 'prompt_hub' }
  },
  {
    path: '/p/:id/llm/prompts/:config_id',
    component: () => import('./Pages/Llm/Prompts/Show.vue'),
    meta: { requiresAuth: true, requiresProduct: 'prompt_hub' }
  },
  {
    path: '/p/:id/llm/compiler',
    component: () => import('./Pages/Llm/Compiler.vue'),
    meta: { requiresAuth: true, requiresProduct: 'prompt_hub' }
  },
  {
    path: '/p/:id/llm/rollouts',
    component: () => import('./Pages/Llm/Rollouts/Index.vue'),
    meta: { requiresAuth: true, requiresProduct: 'prompt_hub' }
  },
  {
    path: '/p/:id/llm/rollouts/:rollout_id',
    component: () => import('./Pages/Llm/Rollouts/Show.vue'),
    meta: { requiresAuth: true, requiresProduct: 'prompt_hub' }
  },
  // Prompt Hub - New pages
  {
    path: '/p/:id/llm/overview',
    component: () => import('./Pages/Llm/Overview.vue'),
    meta: { requiresAuth: true, requiresProduct: 'prompt_hub' }
  },
  {
    path: '/p/:id/llm/sessions',
    component: () => import('./Pages/Llm/Sessions.vue'),
    meta: { requiresAuth: true, requiresProduct: 'prompt_hub' }
  },
  {
    path: '/p/:id/llm/sessions/:sessionId',
    component: () => import('./Pages/Llm/SessionDetail.vue'),
    meta: { requiresAuth: true, requiresProduct: 'prompt_hub' }
  },
  {
    path: '/p/:id/llm/integrations',
    component: () => import('./Pages/Llm/Integrations.vue'),
    meta: { requiresAuth: true, requiresProduct: 'prompt_hub' }
  },
  {
    path: '/p/:id/llm/settings',
    component: () => import('./Pages/Llm/Settings.vue'),
    meta: { requiresAuth: true, requiresProduct: 'prompt_hub' }
  },
  {
    path: '/p/:id/llm/guardrails',
    component: () => import('./Pages/Llm/Guardrails.vue'),
    meta: { requiresAuth: true, requiresProduct: 'prompt_hub' }
  },
  {
    path: '/p/:id/llm/playground',
    component: () => import('./Pages/Llm/Playground.vue'),
    meta: { requiresAuth: true, requiresProduct: 'prompt_hub' }
  },
  // Agents hub (single tabbed page)
  {
    path: '/p/:id/llm/agents',
    component: () => import('./Pages/Agents/Index.vue'),
    meta: { requiresAuth: true, requiresProduct: 'prompt_hub' }
  },
  // Redirects: old individual routes -> agents hub with tab query param
  { path: '/p/:id/llm/tools', redirect: to => `/p/${to.params.id}/llm/agents?tab=tools` },
  { path: '/p/:id/llm/analytics', redirect: to => `/p/${to.params.id}/dashboards` },
  { path: '/p/:id/llm/tokens', redirect: to => `/p/${to.params.id}/llm/agents?tab=tokens` },
  { path: '/p/:id/llm/moodeng', redirect: to => `/p/${to.params.id}/llm/agents?tab=moodeng` },
  { path: '/p/:id/agents/tools', redirect: to => `/p/${to.params.id}/llm/agents?tab=tools` },
  { path: '/p/:id/agents/analytics', redirect: to => `/p/${to.params.id}/dashboards` },
  { path: '/p/:id/agents/tokens', redirect: to => `/p/${to.params.id}/llm/agents?tab=tokens` },
  // Herd (A2A Agent Registry)
  {
    path: '/p/:id/herd/overview',
    component: () => import('./Pages/Herd/Overview.vue'),
    meta: { requiresAuth: true, requiresProduct: 'herd' }
  },
  {
    path: '/p/:id/herd/agents',
    component: () => import('./Pages/Herd/Agents.vue'),
    meta: { requiresAuth: true, requiresProduct: 'herd' }
  },
  {
    path: '/p/:id/herd/discovery',
    component: () => import('./Pages/Herd/Discovery.vue'),
    meta: { requiresAuth: true, requiresProduct: 'herd' }
  },
  {
    path: '/p/:id/herd/discovery/:agentId',
    component: () => import('./Pages/Herd/AgentDetail.vue'),
    meta: { requiresAuth: true, requiresProduct: 'herd' }
  },
  {
    path: '/p/:id/herd/access',
    component: () => import('./Pages/Herd/Access.vue'),
    meta: { requiresAuth: true, requiresProduct: 'herd' }
  },
  {
    path: '/p/:id/herd/settings',
    component: () => import('./Pages/Herd/Settings.vue'),
    meta: { requiresAuth: true, requiresProduct: 'herd' }
  },
  // Services / Service Map
  {
    path: '/p/:id/services',
    component: () => import('./Pages/Services/Index.vue'),
    meta: { requiresAuth: true, requiresProduct: 'watch' }
  },
  {
    path: '/p/:id/services/:service_name',
    component: () => import('./Pages/Services/Show.vue'),
    meta: { requiresAuth: true, requiresProduct: 'watch' }
  },
  // System Overview (cross-stack correlation)
  {
    path: '/p/:id/system-overview',
    component: () => import('./Pages/SystemOverview/Index.vue'),
    meta: { requiresAuth: true, requiresProduct: 'watch' }
  },
  // Metrics Explorer
  {
    path: '/p/:id/metrics',
    component: () => import('./Pages/Metrics/Index.vue'),
    meta: { requiresAuth: true, requiresProduct: 'watch' }
  },
  // API Monitoring
  {
    path: '/p/:id/api-monitoring',
    component: () => import('./Pages/ApiMonitoring/Index.vue'),
    meta: { requiresAuth: true, requiresProduct: 'watch' }
  },
  // Infrastructure Monitoring
  {
    path: '/p/:id/infrastructure',
    component: () => import('./Pages/Infrastructure/Index.vue'),
    meta: { requiresAuth: true, requiresProduct: 'watch' }
  },
  {
    path: '/p/:id/infrastructure/pods/:namespace/:pod',
    component: () => import('./Pages/Infrastructure/PodDetail.vue'),
    meta: { requiresAuth: true, requiresProduct: 'watch' }
  },
  {
    path: '/p/:id/infrastructure/nodes/:node',
    component: () => import('./Pages/Infrastructure/NodeDetail.vue'),
    meta: { requiresAuth: true, requiresProduct: 'watch' }
  },
  // Organization-level settings
  {
    path: '/settings/billing',
    component: () => import('./Pages/Settings/Billing/Index.vue'),
    meta: { requiresAuth: true, requiresOrgAdmin: true }
  },
  {
    path: '/settings/sso',
    component: () => import('./Pages/Settings/Sso/Index.vue'),
    meta: { requiresAuth: true, requiresOrgAdmin: true, requiresFeature: 'sso' }
  },
  {
    path: '/settings/scim',
    component: () => import('./Pages/Settings/Scim/Index.vue'),
    meta: { requiresAuth: true, requiresOrgAdmin: true, requiresFeature: 'sso' }
  },
  {
    path: '/settings/members',
    component: () => import('./Pages/Settings/Members.vue'),
    meta: { requiresAuth: true, requiresOrgAdmin: true }
  },
  {
    path: '/settings/audit',
    component: () => import('./Pages/Settings/AuditLog.vue'),
    meta: { requiresAuth: true, requiresOrgAdmin: true, requiresFeature: 'audit_log' }
  },
  // Platform admin
  {
    path: '/admin',
    component: () => import('./Pages/Admin/Admin.vue'),
    meta: { requiresAuth: true, requiresAdmin: true }
  },
  { path: '/admin/sync', redirect: '/admin' },
  { path: '/admin/billing', redirect: { path: '/admin', query: { tab: 'billing' } } },
  { path: '/admin/tiers', redirect: { path: '/admin', query: { tab: 'tiers' } } },
  { path: '/admin/users', redirect: { path: '/admin', query: { tab: 'users' } } },
  // Secret deposit (standalone page for plushie / MCP CLI / shared links)
  {
    path: '/p/:projectId/secrets/deposit/:slotId',
    component: () => import('./Pages/Secrets/Deposit.vue'),
    meta: { requiresAuth: true }
  },
  // Slack install (entry point from Slack marketplace)
  {
    path: '/slack/install',
    component: () => import('./Pages/SlackInstall.vue'),
    meta: { requiresAuth: true }
  },
];

const router = createRouter({
  history: createWebHistory(),
  routes,
});

// Resolve project slug → UUID on first encounter so the axios interceptor can rewrite API calls
router.beforeEach(async (to, from, next) => {
  const routeId = to.params.id || to.params.projectId;
  if (routeId && !isUuid(routeId) && !isKnownSlug(routeId)) {
    try {
      const res = await axios.get(`/api/projects/${routeId}`);
      if (res.data?.id && res.data?.slug) {
        registerProject(res.data.slug, res.data.id);
      }
    } catch {
      // slug not found -- let the navigation continue; component will show error
    }
  }
  next();
});

// Auth guard - check if route requires authentication
router.beforeEach(async (to, from, next) => {
  if (to.meta.requiresAuth) {
    try {
      const { fetchUser } = useAuth();
      const userData = await fetchUser();
      if (userData && userData.id) {
        if (!userData.is_approved && !userData.is_platform_admin) {
          next('/login?pending=1');
          return;
        }
        if (to.meta.requiresOrgAdmin) {
          const isOrgAdmin = ['owner', 'admin'].includes(userData.org_role);
          if (!isOrgAdmin && !userData.is_platform_admin) {
            next('/');
            return;
          }
        }
        if (to.meta.requiresAdmin && !userData.is_platform_admin) {
          next('/');
          return;
        }
        if (to.meta.redirectToProject) {
          const path = await getProjectRedirectPath();
          next(path);
          return;
        }
        next();
      } else {
        const redirect = to.fullPath !== '/' ? `?redirect=${encodeURIComponent(to.fullPath)}` : '';
        next(`/login${redirect}`);
      }
    } catch (err) {
      if (err.response?.status === 401) {
        const redirect = to.fullPath !== '/' ? `?redirect=${encodeURIComponent(to.fullPath)}` : '';
        next(`/login${redirect}`);
      } else {
        next();
      }
    }
  } else {
    next();
  }
});

// Entitlement guard - check product/feature access for gated routes
router.beforeEach(async (to, from, next) => {
  const { requiresProduct, requiresFeature } = to.meta;
  if (!requiresProduct && !requiresFeature) return next();

  let projectId = to.params.id || to.params.projectId;

  // For settings routes without a project param, use the stored current project
  if (!projectId) {
    const { currentProject, restore } = useCurrentProject();
    await restore();
    const cp = currentProject.value;
    projectId = cp?.slug || cp?.id;
  }

  if (!projectId) return next();

  const { fetchEntitlements, hasProduct, hasFeature } = useEntitlements();
  await fetchEntitlements(projectId);

  if (requiresProduct && !hasProduct(requiresProduct)) {
    return next(`/p/${projectId}/settings`);
  }
  if (requiresFeature && !hasFeature(requiresFeature)) {
    return next('/settings/billing');
  }

  next();
});

router.afterEach((to) => {
  if (typeof window.gtag === 'function') {
    window.gtag('event', 'page_view', {
      page_path: to.fullPath,
      page_title: document.title,
    });
  }
});

const app = createApp(App);
app.use(router);
app.mount('#app');
