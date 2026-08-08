<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-[1200px] mx-auto px-8 py-6 flex flex-col h-[calc(100vh-64px)]">
      <!-- Header -->
      <div class="mb-6 flex items-center justify-between shrink-0">
        <div class="flex items-center gap-4">
          <button @click="goBack" class="text-gray-400 hover:text-gray-200 transition-colors">
            <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7" />
            </svg>
          </button>
          <div>
            <input
              v-model="pipelineName"
              class="text-xl font-semibold bg-transparent border-none outline-none text-gray-100 placeholder-gray-500"
              placeholder="Untitled Pipeline"
            />
            <input
              v-model="pipelineDescription"
              class="text-sm bg-transparent border-none outline-none text-gray-400 placeholder-gray-600 w-full"
              placeholder="Add a description..."
            />
          </div>
        </div>
        <div class="flex items-center gap-3">
          <span :class="computedMode === 'streaming' ? 'mode-badge-streaming' : 'mode-badge-batch'">
            {{ computedMode === 'streaming' ? 'Streaming' : 'Batch' }}
          </span>
          <div class="flex items-center gap-2">
            <input
              v-model="pipelineSchedule"
              class="text-sm bg-gray-50 border border-gray-200 rounded-md px-3 py-1.5 text-gray-900 placeholder-gray-400 w-40"
              placeholder="Cron (optional)"
            />
          </div>
          <button @click="savePipeline" :disabled="saving" class="px-4 py-2 text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 rounded-lg transition-colors disabled:opacity-50">
            {{ saving ? 'Saving...' : 'Save' }}
          </button>
          <button v-if="pipelineId" @click="runPipeline" :disabled="running" class="px-4 py-2 text-sm font-medium text-white bg-green-600 hover:bg-green-700 rounded-lg transition-colors disabled:opacity-50">
            {{ running ? 'Running...' : 'Run' }}
          </button>
          <span v-if="runFeedback" class="text-xs text-blue-400">{{ runFeedback }}</span>
        </div>
      </div>

      <!-- Toolbar -->
      <div class="flex items-center gap-2 px-6 py-2 border-b border-gray-800 shrink-0">
        <button @click="addSourceNode" class="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium bg-brand-900/50 text-brand-400 border border-brand-800 rounded-md hover:bg-brand-900 transition-colors">
          <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" /></svg>
          Source
        </button>
        <button @click="addTransformNode" class="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium bg-blue-900/50 text-blue-400 border border-blue-800 rounded-md hover:bg-blue-900 transition-colors">
          <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" /></svg>
          Transform
        </button>
        <button @click="addSinkNode" class="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium bg-purple-900/50 text-purple-400 border border-purple-800 rounded-md hover:bg-purple-900 transition-colors">
          <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" /></svg>
          Sink
        </button>
        <div class="flex-1" />
        <span v-if="saveError" class="text-xs text-red-400">{{ saveError }}</span>
        <span v-if="saveSuccess" class="text-xs text-green-400">Saved</span>
      </div>

      <!-- Graph Canvas + Config Panel -->
      <div class="flex flex-1 overflow-hidden min-h-0">
        <div class="flex flex-col flex-1 min-h-0">
          <div class="flex-1 relative min-h-0">
            <VueFlow
              v-model:nodes="nodes"
              v-model:edges="edges"
              :default-viewport="{ x: 0, y: 0, zoom: 1 }"
              :snap-to-grid="true"
              :snap-grid="[20, 20]"
              fit-view-on-init
              @node-click="onNodeClick"
              @connect="onConnect"
            >
              <Background />
              <Controls />
              <template #node-source="nodeProps">
                <div class="pipeline-node source-node" :class="{ selected: selectedNodeId === nodeProps.id }">
                  <div class="node-badge bg-brand-500">SRC</div>
                  <div class="node-label">{{ nodeProps.data.label }}</div>
                  <Handle type="source" :position="Position.Right" />
                </div>
              </template>
              <template #node-transform="nodeProps">
                <div class="pipeline-node transform-node" :class="{ selected: selectedNodeId === nodeProps.id }">
                  <Handle type="target" :position="Position.Left" />
                  <div class="node-badge bg-blue-500">UDF</div>
                  <div class="node-label">{{ nodeProps.data.label }}</div>
                  <Handle type="source" :position="Position.Right" />
                </div>
              </template>
              <template #node-sink="nodeProps">
                <div class="pipeline-node sink-node" :class="{ selected: selectedNodeId === nodeProps.id }">
                  <Handle type="target" :position="Position.Left" />
                  <div class="node-badge bg-purple-500">SINK</div>
                  <div class="node-label">{{ nodeProps.data.label }}</div>
                </div>
              </template>
            </VueFlow>
          </div>

          <!-- Bottom Panel: Subscriptions + Run History -->
          <div v-if="pipelineId && bottomPanel" class="border-t border-gray-800 bg-gray-950 shrink-0" style="height: 260px;">
            <div class="flex items-center border-b border-gray-800 px-4">
              <button
                v-for="tab in ['subscriptions', 'runs']" :key="tab"
                @click="bottomTab = tab"
                class="px-3 py-2 text-xs font-medium transition-colors border-b-2 -mb-px"
                :class="bottomTab === tab ? 'text-primary-400 border-primary-500' : 'text-gray-500 border-transparent hover:text-gray-300'"
              >
                {{ tab === 'subscriptions' ? 'Event Subscriptions' : 'Run History' }}
              </button>
              <div class="flex-1" />
              <button @click="bottomPanel = false" class="text-gray-500 hover:text-gray-300 p-1">
                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" /></svg>
              </button>
            </div>
            <div class="overflow-y-auto p-4" style="height: 220px;">

              <!-- Subscriptions Tab -->
              <div v-if="bottomTab === 'subscriptions'">
                <div class="flex gap-2 mb-3">
                  <select v-model="newSubEventType" class="config-input flex-1">
                    <option value="">Select event type...</option>
                    <option value="cron">Cron</option>
                    <option value="manual">Manual</option>
                    <option value="data.insert">Data Insert</option>
                    <option value="data.change">Data Change</option>
                    <option value="pipeline.completed">Pipeline Completed</option>
                  </select>
                  <input v-model="newSubFilter" class="config-input flex-1" placeholder='Filter JSON (optional, e.g. {"table":"orders"})' />
                  <button @click="createSubscription" :disabled="!newSubEventType || creatingSub" class="px-3 py-1.5 text-xs font-medium text-white bg-primary-600 hover:bg-primary-700 rounded-md disabled:opacity-50">
                    Add
                  </button>
                </div>
                <table v-if="subscriptions.length > 0" class="w-full text-xs">
                  <thead>
                    <tr class="text-gray-500 border-b border-gray-800">
                      <th class="text-left py-1.5 px-2 font-medium">Event Type</th>
                      <th class="text-left py-1.5 px-2 font-medium">Filter</th>
                      <th class="text-left py-1.5 px-2 font-medium">Created</th>
                      <th class="text-right py-1.5 px-2 font-medium"></th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr v-for="sub in subscriptions" :key="sub.id" class="border-b border-gray-800/50">
                      <td class="py-1.5 px-2">
                        <span class="px-1.5 py-0.5 rounded bg-gray-800 text-gray-300 font-mono">{{ sub.event_type }}</span>
                      </td>
                      <td class="py-1.5 px-2 text-gray-400 font-mono truncate max-w-[200px]">{{ formatFilter(sub.event_filter) }}</td>
                      <td class="py-1.5 px-2 text-gray-500">{{ timeAgo(sub.created_at) }}</td>
                      <td class="py-1.5 px-2 text-right">
                        <button @click="deleteSubscription(sub.id)" class="text-red-400 hover:text-red-300">Remove</button>
                      </td>
                    </tr>
                  </tbody>
                </table>
                <p v-else class="text-gray-500 text-xs">No subscriptions. Add one to trigger this pipeline on events.</p>
              </div>

              <!-- Runs Tab -->
              <div v-if="bottomTab === 'runs'">
                <div class="flex items-center justify-between mb-2">
                  <span class="text-xs text-gray-500">{{ runs.length }} recent runs</span>
                  <button @click="loadRuns" class="text-xs text-primary-400 hover:text-primary-300">Refresh</button>
                </div>
                <table v-if="runs.length > 0" class="w-full text-xs">
                  <thead>
                    <tr class="text-gray-500 border-b border-gray-800">
                      <th class="text-left py-1.5 px-2 font-medium">Status</th>
                      <th class="text-left py-1.5 px-2 font-medium">Trigger</th>
                      <th class="text-left py-1.5 px-2 font-medium">Started</th>
                      <th class="text-left py-1.5 px-2 font-medium">Duration</th>
                      <th class="text-left py-1.5 px-2 font-medium">Error</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr v-for="run in runs" :key="run.id" class="border-b border-gray-800/50">
                      <td class="py-1.5 px-2">
                        <span :class="runStatusClass(run.status)">{{ run.status }}</span>
                      </td>
                      <td class="py-1.5 px-2 text-gray-400">{{ run.trigger }}</td>
                      <td class="py-1.5 px-2 text-gray-400">{{ run.started_at ? timeAgo(run.started_at) : 'Pending' }}</td>
                      <td class="py-1.5 px-2 text-gray-400">{{ runDuration(run) }}</td>
                      <td class="py-1.5 px-2 text-red-400 truncate max-w-[240px]" :title="run.error_message">{{ run.error_message || '' }}</td>
                    </tr>
                  </tbody>
                </table>
                <p v-else class="text-gray-500 text-xs">No runs yet.</p>
              </div>
            </div>
          </div>

          <!-- Bottom panel toggle -->
          <div v-if="pipelineId && !bottomPanel" class="border-t border-gray-800 bg-gray-950 shrink-0">
            <button @click="bottomPanel = true" class="w-full px-4 py-1.5 text-xs text-gray-500 hover:text-gray-300 text-left flex items-center gap-1">
              <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 15l7-7 7 7" /></svg>
              Subscriptions &amp; Run History
            </button>
          </div>
        </div>

        <!-- Config Side Panel -->
        <div v-if="selectedNode" class="w-80 border-l border-gray-200 bg-gray-50 overflow-y-auto p-4">
          <div class="flex items-center justify-between mb-4">
            <h3 class="text-sm font-semibold text-gray-200">Node Configuration</h3>
            <button @click="deleteSelectedNode" class="text-xs text-red-400 hover:text-red-300">Delete</button>
          </div>

          <div class="space-y-3">
            <div>
              <label class="block text-xs font-medium text-gray-400 mb-1">Label</label>
              <input v-model="selectedNode.data.label" @input="syncNodeLabel" class="config-input" />
            </div>

            <!-- Source Config -->
            <template v-if="selectedNode.type === 'source'">
              <div>
                <label class="block text-xs font-medium text-gray-400 mb-1">Connector</label>
                <select v-model="selectedNode.data.config.connector_name" class="config-input">
                  <option v-for="c in connectors" :key="c" :value="c">{{ c }}</option>
                </select>
              </div>
              <div>
                <label class="block text-xs font-medium text-gray-400 mb-1">Read Strategy</label>
                <select v-model="selectedNode.data.config.read_strategy.strategy" @change="onStrategyChange" class="config-input">
                  <option value="full_sync">Full Sync</option>
                  <option value="incremental">Incremental Cursor</option>
                  <option value="query">Custom Query</option>
                  <option value="batch_fetch">Batch Fetch</option>
                  <option value="cdc_stream">CDC Stream</option>
                  <option value="filter">Filter</option>
                </select>
              </div>
              <div v-if="selectedNode.data.config.read_strategy.strategy !== 'query'">
                <label class="block text-xs font-medium text-gray-400 mb-1">Table</label>
                <input v-model="selectedNode.data.config.read_strategy.table" class="config-input" placeholder="schema.table" />
              </div>
              <div v-if="selectedNode.data.config.read_strategy.strategy === 'query'">
                <label class="block text-xs font-medium text-gray-400 mb-1">SQL Query</label>
                <textarea v-model="selectedNode.data.config.read_strategy.sql" class="config-input h-24 resize-none" placeholder="SELECT ..." />
              </div>
              <div v-if="selectedNode.data.config.read_strategy.strategy === 'incremental'">
                <label class="block text-xs font-medium text-gray-400 mb-1">Cursor Key</label>
                <input v-model="selectedNode.data.config.read_strategy.cursor_key" class="config-input" placeholder="updated_at" />
              </div>
              <div v-if="selectedNode.data.config.read_strategy.strategy === 'filter'">
                <label class="block text-xs font-medium text-gray-400 mb-1">WHERE Clause</label>
                <input v-model="selectedNode.data.config.read_strategy.filter" class="config-input" placeholder="status = 'active'" />
              </div>
              <div v-if="selectedNode.data.config.read_strategy.strategy === 'batch_fetch'">
                <label class="block text-xs font-medium text-gray-400 mb-1">Batch Size</label>
                <input v-model.number="selectedNode.data.config.read_strategy.batch_size" type="number" class="config-input" placeholder="1000" />
              </div>
              <div v-if="selectedNode.data.config.read_strategy.strategy === 'batch_fetch'">
                <label class="block text-xs font-medium text-gray-400 mb-1">Max Rows (optional)</label>
                <input v-model.number="selectedNode.data.config.read_strategy.max_rows" type="number" class="config-input" placeholder="No limit" />
              </div>
            </template>

            <!-- Transform Config -->
            <template v-if="selectedNode.type === 'transform'">
              <div>
                <label class="block text-xs font-medium text-gray-400 mb-1">UDF</label>
                <select v-model="selectedNode.data.config.udf_name" class="config-input">
                  <option v-for="u in udfs" :key="u.name" :value="u.name">{{ u.name }}</option>
                </select>
              </div>
              <div>
                <label class="block text-xs font-medium text-gray-400 mb-1">Config Params</label>
                <div v-for="(val, key) in selectedNode.data.config.params" :key="key" class="flex gap-1 mb-1">
                  <input :value="key" readonly class="config-input flex-1 bg-gray-100" />
                  <input v-model="selectedNode.data.config.params[key]" class="config-input flex-1" />
                  <button @click="removeParam(key)" class="text-red-400 hover:text-red-300 px-1">x</button>
                </div>
                <div class="flex gap-1">
                  <input v-model="newParamKey" class="config-input flex-1" placeholder="Key" />
                  <input v-model="newParamVal" class="config-input flex-1" placeholder="Value" />
                  <button @click="addParam" class="text-xs text-blue-400 hover:text-blue-300 px-2">+</button>
                </div>
              </div>
            </template>

            <!-- Sink Config -->
            <template v-if="selectedNode.type === 'sink'">
              <div>
                <label class="block text-xs font-medium text-gray-400 mb-1">Connector</label>
                <select v-model="selectedNode.data.config.connector_name" class="config-input">
                  <option v-for="c in connectors" :key="c" :value="c">{{ c }}</option>
                </select>
              </div>
              <div>
                <label class="block text-xs font-medium text-gray-400 mb-1">Table</label>
                <input v-model="selectedNode.data.config.table" class="config-input" placeholder="schema.table" />
              </div>
            </template>
          </div>
        </div>
      </div>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { VueFlow, Handle, Position } from '@vue-flow/core'
import { Background } from '@vue-flow/background'
import { Controls } from '@vue-flow/controls'
import '@vue-flow/core/dist/style.css'
import '@vue-flow/core/dist/theme-default.css'
import axios from 'axios'
import AppLayout from '@/Layouts/AppLayout.vue'
import { useAuth } from '@/composables/useAuth'

const route = useRoute()
const router = useRouter()
const { user } = useAuth()

const projectId = computed(() => route.params.id)
const pipelineId = computed(() => route.params.pipeline_id || null)
const project = computed(() => ({ id: projectId.value }))

const pipelineName = ref('')
const pipelineDescription = ref('')
const pipelineSchedule = ref('')

const nodes = ref([])
const edges = ref([])
const selectedNodeId = ref(null)
const saving = ref(false)
const running = ref(false)
const saveError = ref(null)
const saveSuccess = ref(false)
const runFeedback = ref(null)

const connectors = ref([])
const udfs = ref([])
const newParamKey = ref('')
const newParamVal = ref('')

const bottomPanel = ref(false)
const bottomTab = ref('subscriptions')

const subscriptions = ref([])
const newSubEventType = ref('')
const newSubFilter = ref('')
const creatingSub = ref(false)

const runs = ref([])

let nodeCounter = 0

const selectedNode = computed(() => {
  if (!selectedNodeId.value) return null
  return nodes.value.find(n => n.id === selectedNodeId.value) || null
})

const computedMode = computed(() => {
  const hasCdc = nodes.value.some(n =>
    n.type === 'source' && n.data?.config?.read_strategy?.strategy === 'cdc_stream'
  )
  return hasCdc ? 'streaming' : 'batch'
})

function generateNodeId() {
  return crypto.randomUUID ? crypto.randomUUID() : `node-${Date.now()}-${++nodeCounter}`
}

function addSourceNode() {
  const id = generateNodeId()
  nodes.value.push({
    id,
    type: 'source',
    position: { x: 100, y: 100 + nodes.value.length * 120 },
    data: {
      label: 'New Source',
      config: { type: 'source', connector_name: '', read_strategy: { strategy: 'full_sync', table: '' } }
    }
  })
  selectedNodeId.value = id
}

function addTransformNode() {
  const id = generateNodeId()
  nodes.value.push({
    id,
    type: 'transform',
    position: { x: 400, y: 100 + nodes.value.length * 120 },
    data: {
      label: 'New Transform',
      config: { type: 'transform', udf_name: '', params: {} }
    }
  })
  selectedNodeId.value = id
}

function addSinkNode() {
  const id = generateNodeId()
  nodes.value.push({
    id,
    type: 'sink',
    position: { x: 700, y: 100 + nodes.value.length * 120 },
    data: {
      label: 'New Sink',
      config: { type: 'sink', connector_name: '', table: '' }
    }
  })
  selectedNodeId.value = id
}

function onNodeClick({ node }) {
  selectedNodeId.value = node.id
}

function onConnect(params) {
  edges.value.push({
    id: `e-${params.source}-${params.target}`,
    source: params.source,
    target: params.target,
    animated: true,
    style: { stroke: '#6366f1' }
  })
}

function deleteSelectedNode() {
  if (!selectedNodeId.value) return
  const id = selectedNodeId.value
  nodes.value = nodes.value.filter(n => n.id !== id)
  edges.value = edges.value.filter(e => e.source !== id && e.target !== id)
  selectedNodeId.value = null
}

function syncNodeLabel() {
  // Vue reactivity handles this automatically
}

function addParam() {
  if (!newParamKey.value.trim() || !selectedNode.value) return
  if (!selectedNode.value.data.config.params) {
    selectedNode.value.data.config.params = {}
  }
  selectedNode.value.data.config.params[newParamKey.value.trim()] = newParamVal.value
  newParamKey.value = ''
  newParamVal.value = ''
}

function removeParam(key) {
  if (!selectedNode.value) return
  delete selectedNode.value.data.config.params[key]
}

const strategyDefaults = {
  full_sync:    { strategy: 'full_sync', table: '' },
  incremental:  { strategy: 'incremental', table: '', cursor_key: '' },
  query:        { strategy: 'query', sql: '' },
  batch_fetch:  { strategy: 'batch_fetch', table: '', batch_size: 1000, max_rows: null },
  cdc_stream:   { strategy: 'cdc_stream', table: '' },
  filter:       { strategy: 'filter', table: '', filter: '' },
}

function onStrategyChange() {
  if (!selectedNode.value) return
  const strat = selectedNode.value.data.config.read_strategy.strategy
  selectedNode.value.data.config.read_strategy = { ...(strategyDefaults[strat] || strategyDefaults.full_sync) }
}

function buildPayload() {
  const nodeTypeMap = { source: 'source', transform: 'transform', sink: 'sink' }
  return {
    name: pipelineName.value || 'Untitled Pipeline',
    description: pipelineDescription.value || null,
    schedule: pipelineSchedule.value || null,
    enabled: true,
    nodes: nodes.value.map(n => ({
      id: n.id,
      node_type: nodeTypeMap[n.type],
      label: n.data.label,
      config: n.data.config,
      position_x: n.position.x,
      position_y: n.position.y,
    })),
    edges: edges.value.map(e => ({
      from_node_id: e.source,
      to_node_id: e.target,
    }))
  }
}

async function savePipeline() {
  saving.value = true
  saveError.value = null
  saveSuccess.value = false
  try {
    const payload = buildPayload()
    if (pipelineId.value) {
      await axios.put(`/api/projects/${projectId.value}/warehouse/pipelines/${pipelineId.value}`, payload)
    } else {
      const res = await axios.post(`/api/projects/${projectId.value}/warehouse/pipelines`, payload)
      router.replace(`/p/${projectId.value}/warehouse/pipelines/${res.data.id}/edit`)
    }
    saveSuccess.value = true
    setTimeout(() => { saveSuccess.value = false }, 2000)
  } catch (err) {
    saveError.value = err.response?.data?.message || err.message || 'Save failed'
  } finally {
    saving.value = false
  }
}

async function runPipeline() {
  if (!pipelineId.value) return
  running.value = true
  runFeedback.value = null
  try {
    const res = await axios.post(`/api/projects/${projectId.value}/warehouse/pipelines/${pipelineId.value}/run`)
    runFeedback.value = `Triggered (${res.data.status})`
    setTimeout(() => { runFeedback.value = null }, 4000)
    loadRuns()
  } catch (err) {
    saveError.value = err.response?.data?.message || 'Run failed'
  } finally {
    running.value = false
  }
}

function goBack() {
  router.push(`/p/${projectId.value}/warehouse/pipelines`)
}

async function loadExistingPipeline() {
  if (!pipelineId.value) return
  try {
    const res = await axios.get(`/api/projects/${projectId.value}/warehouse/pipelines/${pipelineId.value}`)
    const p = res.data
    pipelineName.value = p.name
    pipelineDescription.value = p.description || ''
    pipelineSchedule.value = p.schedule || ''
    nodes.value = p.nodes.map(n => ({
      id: n.id,
      type: n.node_type,
      position: { x: n.position_x, y: n.position_y },
      data: { label: n.label, config: n.config }
    }))
    edges.value = p.edges.map(e => ({
      id: `e-${e.from_node_id}-${e.to_node_id}`,
      source: e.from_node_id,
      target: e.to_node_id,
      animated: true,
      style: { stroke: '#6366f1' }
    }))
  } catch (err) {
    saveError.value = 'Failed to load pipeline'
  }
}

async function loadConnectors() {
  try {
    const res = await axios.get(`/api/projects/${projectId.value}/warehouse/sources`)
    connectors.value = (res.data.sources || []).map(s => s.name)
  } catch { /* ignore */ }
}

async function loadUdfs() {
  try {
    const res = await axios.get(`/api/projects/${projectId.value}/warehouse/udfs`)
    udfs.value = res.data.udfs || []
  } catch { /* ignore */ }
}

async function loadSubscriptions() {
  if (!pipelineId.value) return
  try {
    const res = await axios.get(`/api/projects/${projectId.value}/warehouse/pipelines/${pipelineId.value}/subscriptions`)
    subscriptions.value = res.data.subscriptions || []
  } catch { /* ignore */ }
}

async function createSubscription() {
  if (!pipelineId.value || !newSubEventType.value) return
  creatingSub.value = true
  try {
    let filter = {}
    if (newSubFilter.value.trim()) {
      try { filter = JSON.parse(newSubFilter.value) } catch { filter = {} }
    }
    await axios.post(`/api/projects/${projectId.value}/warehouse/pipelines/${pipelineId.value}/subscriptions`, {
      event_type: newSubEventType.value,
      event_filter: filter,
    })
    newSubEventType.value = ''
    newSubFilter.value = ''
    await loadSubscriptions()
  } catch (err) {
    saveError.value = err.response?.data?.message || 'Failed to create subscription'
  } finally {
    creatingSub.value = false
  }
}

async function deleteSubscription(subId) {
  try {
    await axios.delete(`/api/projects/${projectId.value}/warehouse/subscriptions/${subId}`)
    subscriptions.value = subscriptions.value.filter(s => s.id !== subId)
  } catch { /* ignore */ }
}

async function loadRuns() {
  if (!pipelineId.value) return
  try {
    const res = await axios.get(`/api/projects/${projectId.value}/warehouse/pipelines/${pipelineId.value}/runs`)
    runs.value = res.data.runs || []
  } catch { /* ignore */ }
}

function timeAgo(iso) {
  if (!iso) return ''
  const diff = Math.floor((Date.now() - new Date(iso).getTime()) / 1000)
  if (diff < 60) return `${diff}s ago`
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`
  return `${Math.floor(diff / 86400)}d ago`
}

function runDuration(run) {
  if (!run.started_at || !run.finished_at) return run.status === 'running' ? 'Running...' : '-'
  const ms = new Date(run.finished_at) - new Date(run.started_at)
  if (ms < 1000) return `${ms}ms`
  return `${(ms / 1000).toFixed(1)}s`
}

function runStatusClass(status) {
  const map = {
    succeeded: 'text-green-400 font-medium',
    running: 'text-blue-400 font-medium',
    pending: 'text-yellow-400 font-medium',
    failed: 'text-red-400 font-medium',
    crashed: 'text-red-300 font-medium',
  }
  return map[status] || 'text-gray-400'
}

function formatFilter(f) {
  if (!f || (typeof f === 'object' && Object.keys(f).length === 0)) return '-'
  return JSON.stringify(f)
}

async function loadPipelineEditorData() {
  await Promise.all([loadConnectors(), loadUdfs(), loadExistingPipeline()])
  if (pipelineId.value) {
    loadSubscriptions()
    loadRuns()
  }
}

onMounted(loadPipelineEditorData)
watch(projectId, loadPipelineEditorData)
</script>

<style scoped>
.pipeline-node {
  @apply flex items-center gap-2 px-3 py-2 rounded-lg border text-sm min-w-[140px];
  @apply bg-white border-gray-200 text-gray-900;
}
.pipeline-node.selected {
  @apply ring-2 ring-primary-500;
}
.source-node { @apply border-brand-700 bg-brand-950/50; }
.transform-node { @apply border-blue-700 bg-blue-950/50; }
.sink-node { @apply border-purple-700 bg-purple-950/50; }
.node-badge {
  @apply text-[10px] font-bold text-white px-1.5 py-0.5 rounded;
}
.node-label {
  @apply truncate;
}
.config-input {
  @apply w-full text-sm bg-gray-50 border border-gray-200 rounded-md px-2.5 py-1.5 text-gray-900 placeholder-gray-400 focus:border-primary-500 focus:outline-none;
}
.mode-badge-batch {
  @apply text-xs font-medium px-2 py-0.5 rounded-full bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300;
}
.mode-badge-streaming {
  @apply text-xs font-medium px-2 py-0.5 rounded-full bg-brand-100 text-brand-700 dark:bg-brand-900/40 dark:text-brand-300;
}
</style>
