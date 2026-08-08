<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-[1200px] mx-auto px-8 py-6">
      <!-- Stats Cards -->
      <div class="grid grid-cols-1 md:grid-cols-5 gap-4 mb-6">
        <BaseCard class="!p-4">
          <div class="flex items-center justify-between">
            <div>
              <p class="text-sm text-gray-500 dark:text-gray-400">Project Agents</p>
              <p class="text-2xl font-bold text-gray-900 dark:text-gray-100">{{ formatNumber(stats.project_agents) }}</p>
            </div>
            <div class="w-10 h-10 rounded-lg bg-blue-100 dark:bg-blue-900/30 flex items-center justify-center">
              <svg class="w-5 h-5 text-blue-600 dark:text-blue-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
              </svg>
            </div>
          </div>
          <p class="text-xs text-gray-500 dark:text-gray-400 mt-2">In this project</p>
        </BaseCard>

        <BaseCard class="!p-4">
          <div class="flex items-center justify-between">
            <div>
              <p class="text-sm text-gray-500 dark:text-gray-400">Org Agents</p>
              <p class="text-2xl font-bold text-gray-900 dark:text-gray-100">{{ formatNumber(stats.org_agents) }}</p>
            </div>
            <div class="w-10 h-10 rounded-lg bg-amber-100 dark:bg-amber-900/30 flex items-center justify-center">
              <svg class="w-5 h-5 text-amber-600 dark:text-amber-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 21V5a2 2 0 00-2-2H7a2 2 0 00-2 2v16m14 0h2m-2 0h-5m-9 0H3m2 0h5M9 7h1m-1 4h1m4-4h1m-1 4h1m-5 10v-5a1 1 0 011-1h2a1 1 0 011 1v5m-4 0h4" />
              </svg>
            </div>
          </div>
          <p class="text-xs text-gray-500 dark:text-gray-400 mt-2">Other projects in org</p>
        </BaseCard>

        <BaseCard class="!p-4">
          <div class="flex items-center justify-between">
            <div>
              <p class="text-sm text-gray-500 dark:text-gray-400">External Agents</p>
              <p class="text-2xl font-bold text-gray-900 dark:text-gray-100">{{ formatNumber(stats.external_agents) }}</p>
            </div>
            <div class="w-10 h-10 rounded-lg bg-green-100 dark:bg-green-900/30 flex items-center justify-center">
              <svg class="w-5 h-5 text-green-600 dark:text-green-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1" />
              </svg>
            </div>
          </div>
          <p class="text-xs text-gray-500 dark:text-gray-400 mt-2">Other organizations</p>
        </BaseCard>

        <BaseCard class="!p-4">
          <div class="flex items-center justify-between">
            <div>
              <p class="text-sm text-gray-500 dark:text-gray-400">Active Today</p>
              <p class="text-2xl font-bold text-gray-900 dark:text-gray-100">{{ formatNumber(stats.active_edges) }}</p>
            </div>
            <div class="w-10 h-10 rounded-lg bg-violet-100 dark:bg-violet-900/30 flex items-center justify-center">
              <svg class="w-5 h-5 text-violet-600 dark:text-violet-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z" />
              </svg>
            </div>
          </div>
          <p class="text-xs text-gray-500 dark:text-gray-400 mt-2">Connections with traffic</p>
        </BaseCard>

        <BaseCard class="!p-4">
          <div class="flex items-center justify-between">
            <div>
              <p class="text-sm text-gray-500 dark:text-gray-400">Security Flags</p>
              <p class="text-2xl font-bold" :class="(stats.pii_redacted + stats.injection_flagged) > 0 ? 'text-red-600 dark:text-red-400' : 'text-gray-900 dark:text-gray-100'">{{ formatNumber(stats.pii_redacted + stats.injection_flagged) }}</p>
            </div>
            <div class="w-10 h-10 rounded-lg bg-red-100 dark:bg-red-900/30 flex items-center justify-center">
              <svg class="w-5 h-5 text-red-600 dark:text-red-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
              </svg>
            </div>
          </div>
          <p class="text-xs text-gray-500 dark:text-gray-400 mt-2">PII + injection detected</p>
        </BaseCard>
      </div>

      <!-- Topology Graph -->
      <BaseCard class="!p-0 overflow-hidden">
        <div class="flex items-center justify-between px-5 py-3 border-b border-gray-200 dark:border-gray-700">
          <h2 class="text-sm font-semibold text-gray-900 dark:text-gray-100">Agent Communication Topology</h2>
          <div class="flex items-center gap-3">
            <input
              type="date"
              v-model="selectedDate"
              class="text-xs bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-600 rounded-md px-2.5 py-1.5 text-gray-900 dark:text-gray-200"
              @change="fetchTopology"
            />
            <button
              @click="fitGraph"
              class="text-xs text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-200 transition-colors"
              title="Fit to view"
            >
              <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 8V4m0 0h4M4 4l5 5m11-1V4m0 0h-4m4 0l-5 5M4 16v4m0 0h4m-4 0l5-5m11 5l-5-5m5 5v-4m0 4h-4" />
              </svg>
            </button>
          </div>
        </div>

        <div v-if="topologyLoading" class="flex items-center justify-center h-[560px]">
          <div class="spinner w-6 h-6 border-2 border-blue-500 border-t-transparent rounded-full"></div>
        </div>

        <div v-else-if="stats.project_agents === 0" class="flex flex-col items-center justify-center h-[560px] text-gray-500 dark:text-gray-400">
          <svg class="w-12 h-12 mb-3 opacity-40" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
          </svg>
          <p class="text-sm">No agents registered yet</p>
          <p class="text-xs mt-1">Register agents on the Agents page to see the topology</p>
        </div>

        <div v-else ref="cyContainer" class="h-[560px]"></div>

        <!-- Legend -->
        <div v-if="stats.project_agents > 0" class="flex flex-wrap items-center gap-4 px-5 py-3 border-t border-gray-200 dark:border-gray-700 text-xs text-gray-500 dark:text-gray-400">
          <span class="flex items-center gap-1.5">
            <span class="w-3 h-3 rounded border-2 border-blue-500"></span>
            Project (same project)
          </span>
          <span class="flex items-center gap-1.5">
            <span class="w-3 h-3 rounded border-2 border-amber-500"></span>
            Org (other project)
          </span>
          <span class="flex items-center gap-1.5">
            <span class="w-3 h-3 rounded border-2 border-green-500"></span>
            External (other org)
          </span>
          <span class="ml-auto flex items-center gap-4">
            <span class="flex items-center gap-1.5">
              <span class="w-6 h-0.5 bg-brand-500 rounded"></span>
              Healthy
            </span>
            <span class="flex items-center gap-1.5">
              <span class="w-6 h-0.5 bg-orange-500 rounded"></span>
              Errors 5-20%
            </span>
            <span class="flex items-center gap-1.5">
              <span class="w-6 h-0.5 bg-red-600 rounded"></span>
              Errors &gt;20%
            </span>
            <span class="flex items-center gap-1.5">
              <span class="w-6 h-0.5 bg-gray-400 rounded opacity-30" style="border-top: 2px dashed"></span>
              Grant only
            </span>
            <span class="flex items-center gap-1.5">
              <span class="w-6 h-0.5 bg-violet-500 rounded"></span>
              PII redacted
            </span>
          </span>
        </div>
      </BaseCard>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted, onBeforeUnmount, watch, nextTick } from 'vue';
import { useRoute } from 'vue-router';
import cytoscape from 'cytoscape';
import fcose from 'cytoscape-fcose';
import axios from 'axios';
import AppLayout from '@/Layouts/AppLayout.vue';
import BaseCard from '@/components/BaseCard.vue';
import { useAuth } from '@/composables/useAuth';

cytoscape.use(fcose);

const route = useRoute();
const { user, fetchUser } = useAuth();

const projectId = computed(() => route.params.id);
const project = computed(() => ({ id: projectId.value }));
const loading = ref(true);
const topologyLoading = ref(false);

const today = new Date().toISOString().slice(0, 10);
const selectedDate = ref(today);

const stats = ref({
  project_agents: 0,
  org_agents: 0,
  external_agents: 0,
  active_edges: 0,
  pii_redacted: 0,
  injection_flagged: 0,
});

const topologyData = ref({ nodes: [], edges: [] });
const cyContainer = ref(null);
let cy = null;

const formatNumber = (num) => {
  if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
  if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
  return (num || 0).toLocaleString();
};

const fitGraph = () => {
  if (cy) cy.fit(undefined, 30);
};

const getCyStyle = () => [
  {
    selector: 'node',
    style: {
      'label': 'data(label)',
      'text-valign': 'center',
      'text-halign': 'center',
      'font-size': '10px',
      'font-weight': 600,
      'color': '#374151',
      'text-max-width': '80px',
      'text-wrap': 'ellipsis',
      'width': 100,
      'height': 40,
      'shape': 'round-rectangle',
      'background-color': '#ffffff',
      'border-width': 2.5,
      'border-color': '#6b7280',
    },
  },
  {
    selector: 'node.project',
    style: {
      'border-color': '#3b82f6',
      'background-color': '#eff6ff',
    },
  },
  {
    selector: 'node.org',
    style: {
      'border-color': '#f59e0b',
      'background-color': '#fffbeb',
    },
  },
  {
    selector: 'node.external',
    style: {
      'border-color': '#22c55e',
      'background-color': '#f0fdf4',
    },
  },
  {
    selector: 'node.active',
    style: {
      'border-width': 4,
    },
  },
  {
    selector: 'node.dormant',
    style: { 'opacity': 0.5 },
  },
  {
    selector: 'node.pending',
    style: { 'border-style': 'dashed' },
  },
  {
    selector: 'node.denied',
    style: { 'border-style': 'dashed', 'border-color': '#ef4444', 'opacity': 0.5 },
  },
  {
    selector: 'node.revoked',
    style: { 'border-style': 'dashed', 'border-color': '#6b7280', 'opacity': 0.5 },
  },
  {
    selector: 'edge',
    style: {
      'width': 1.5,
      'line-color': '#9ca3af',
      'target-arrow-color': '#9ca3af',
      'target-arrow-shape': 'triangle',
      'arrow-scale': 0.7,
      'curve-style': 'bezier',
      'opacity': 0.3,
      'line-style': 'dashed',
      'line-dash-pattern': [6, 4],
    },
  },
  {
    selector: 'edge.pending',
    style: {
      'line-color': '#f59e0b',
      'target-arrow-color': '#f59e0b',
      'opacity': 0.4,
      'label': 'pending',
      'font-size': '9px',
      'color': '#f59e0b',
      'text-rotation': 'autorotate',
      'text-margin-y': -8,
    },
  },
  {
    selector: 'edge.denied',
    style: {
      'line-color': '#ef4444',
      'target-arrow-color': '#ef4444',
      'opacity': 0.2,
      'line-dash-pattern': [3, 3],
    },
  },
  {
    selector: 'edge.traffic-healthy',
    style: {
      'line-color': '#10b981',
      'target-arrow-color': '#10b981',
      'opacity': 1,
      'line-style': 'solid',
    },
  },
  {
    selector: 'edge.traffic-moderate-errors',
    style: {
      'line-color': '#f97316',
      'target-arrow-color': '#f97316',
      'opacity': 1,
      'line-style': 'solid',
    },
  },
  {
    selector: 'edge.traffic-high-errors',
    style: {
      'line-color': '#dc2626',
      'target-arrow-color': '#dc2626',
      'opacity': 1,
      'line-style': 'solid',
      'width': 3,
    },
  },
  {
    selector: 'edge.traffic-pii',
    style: {
      'line-color': '#8b5cf6',
      'target-arrow-color': '#8b5cf6',
      'opacity': 1,
      'line-style': 'solid',
    },
  },
  {
    selector: 'edge.traffic-injection',
    style: {
      'line-color': '#dc2626',
      'target-arrow-color': '#dc2626',
      'opacity': 1,
      'line-style': 'solid',
    },
  },
  {
    selector: 'edge[label]',
    style: {
      'label': 'data(label)',
      'font-size': '9px',
      'text-rotation': 'autorotate',
      'text-margin-y': -8,
      'text-background-color': '#ffffff',
      'text-background-opacity': 0.85,
      'text-background-padding': '2px',
    },
  },
];

const buildGraph = () => {
  const data = topologyData.value;
  if (!data.nodes.length) {
    if (cy) { cy.destroy(); cy = null; }
    return;
  }

  const activeNodeIds = new Set();
  for (const edge of data.edges) {
    if (edge.traffic) {
      activeNodeIds.add(edge.source);
      activeNodeIds.add(edge.target);
    }
  }

  const projectNodes = data.nodes.filter(n => n.kind === 'project');
  const orgNodes = data.nodes.filter(n => n.kind === 'org');
  const extNodes = data.nodes.filter(n => n.kind === 'external');

  const elements = [];

  for (const n of data.nodes) {
    const classes = [n.kind];
    if (activeNodeIds.has(n.id)) {
      classes.push('active');
    } else {
      classes.push('dormant');
    }
    const grantStatus = n.grantStatus || 'none';
    if (grantStatus === 'pending') classes.push('pending');
    else if (grantStatus === 'denied') classes.push('denied');
    else if (grantStatus === 'revoked') classes.push('revoked');

    elements.push({
      group: 'nodes',
      data: { id: n.id, label: n.name },
      classes: classes.join(' '),
    });
  }

  for (const e of data.edges) {
    const hasTraffic = !!e.traffic;
    const errorRate = hasTraffic && e.traffic.messageCount > 0
      ? e.traffic.errorCount / e.traffic.messageCount
      : 0;
    const hasPii = hasTraffic && e.traffic.piiRedactedCount > 0;
    const hasInjection = hasTraffic && e.traffic.injectionFlaggedCount > 0;

    const classes = [];
    let label = '';
    let width = 1.5;

    if (hasTraffic) {
      width = Math.max(2, Math.min(6, Math.log2(e.traffic.messageCount + 1)));
      const parts = [`${e.traffic.messageCount} msgs`, `${Math.round(e.traffic.avgLatencyMs)}ms`];

      if (errorRate > 0.2) {
        classes.push('traffic-high-errors');
        width = Math.max(width, 3);
        parts.push(`${Math.round(errorRate * 100)}% err`);
      } else if (errorRate > 0.05) {
        classes.push('traffic-moderate-errors');
        parts.push(`${Math.round(errorRate * 100)}% err`);
      } else if (hasInjection) {
        classes.push('traffic-injection');
      } else if (hasPii) {
        classes.push('traffic-pii');
        parts.push(`${e.traffic.piiRedactedCount} PII`);
      } else {
        classes.push('traffic-healthy');
      }

      if (hasPii && !classes.includes('traffic-pii')) parts.push(`${e.traffic.piiRedactedCount} PII`);
      if (hasInjection) parts.push(`${e.traffic.injectionFlaggedCount} inj`);
      label = parts.join(' / ');
    } else {
      if (e.grantStatus === 'pending') {
        classes.push('pending');
      } else if (e.grantStatus === 'denied' || e.grantStatus === 'revoked') {
        classes.push('denied');
      }
    }

    elements.push({
      group: 'edges',
      data: {
        id: `e-${e.source}-${e.target}`,
        source: e.source,
        target: e.target,
        label: label || undefined,
        width,
      },
      classes: classes.join(' '),
    });
  }

  if (cy) { cy.destroy(); cy = null; }

  cy = cytoscape({
    container: cyContainer.value,
    elements,
    style: getCyStyle(),
    layout: {
      name: 'fcose',
      quality: 'default',
      randomize: true,
      animate: false,
      fit: true,
      padding: 30,
      nodeRepulsion: 4500,
      idealEdgeLength: 140,
      edgeElasticity: 0.45,
      nestingFactor: 0.1,
      gravity: 0.25,
      gravityRange: 3.8,
      numIter: 2500,
    },
    minZoom: 0.3,
    maxZoom: 3,
  });

  // Apply dynamic edge widths after layout
  cy.edges().forEach(edge => {
    const w = edge.data('width');
    if (w) edge.style('width', w);
  });
};

const fetchTopology = async () => {
  topologyLoading.value = true;
  try {
    const res = await axios.get(
      `/api/projects/${projectId.value}/herd/topology`,
      { params: { date: selectedDate.value } }
    );
    topologyData.value = res.data;
  } catch (error) {
    console.error('Failed to fetch topology:', error);
    topologyData.value = { nodes: [], edges: [] };
  }
  // Compute stats early so the container div renders via v-else
  const data = topologyData.value;
  stats.value.project_agents = data.nodes.filter(n => n.kind === 'project').length;
  stats.value.org_agents = data.nodes.filter(n => n.kind === 'org').length;
  stats.value.external_agents = data.nodes.filter(n => n.kind === 'external').length;
  stats.value.active_edges = data.edges.filter(e => e.traffic).length;
  stats.value.pii_redacted = data.edges.reduce((sum, e) => sum + (e.traffic?.piiRedactedCount || 0), 0);
  stats.value.injection_flagged = data.edges.reduce((sum, e) => sum + (e.traffic?.injectionFlaggedCount || 0), 0);
  topologyLoading.value = false;
  await nextTick();
  if (data.nodes.length) buildGraph();
};

const fetchData = async () => {
  loading.value = true;
  try {
    await fetchTopology();
  } catch (error) {
    console.error('Failed to fetch Herd overview data:', error);
  } finally {
    loading.value = false;
  }
};

onMounted(async () => {
  await fetchUser();
  await fetchData();
});

onBeforeUnmount(() => {
  if (cy) { cy.destroy(); cy = null; }
});

watch(projectId, async () => {
  await fetchUser();
  await fetchData();
});
</script>

<style scoped>
.spinner { animation: spin 1s linear infinite; }
@keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
</style>
