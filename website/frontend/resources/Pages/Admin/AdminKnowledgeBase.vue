<template>
  <div class="space-y-4">
    <div class="flex items-center justify-between">
      <h2 class="text-lg font-semibold text-gray-900">Knowledge Base</h2>
      <div class="flex gap-2">
        <button
          @click="seedEntries"
          :disabled="seeding || loading"
          class="px-4 py-2 rounded-lg bg-gray-100 text-gray-700 text-sm font-medium hover:bg-gray-200 disabled:opacity-50"
        >
          {{ seeding ? 'Seeding...' : 'Seed Starter Entries' }}
        </button>
        <button
          @click="openForm('upload')"
          class="px-4 py-2 rounded-lg bg-gray-600 text-white text-sm font-medium hover:bg-gray-700"
        >
          Upload File
        </button>
        <button
          @click="openForm('manual')"
          class="px-4 py-2 rounded-lg bg-blue-600 text-white text-sm font-medium hover:bg-blue-700"
        >
          Add Entry
        </button>
      </div>
    </div>

    <div v-if="error" class="rounded-lg bg-red-50 border border-red-200 px-4 py-3 text-sm text-red-700 flex items-center justify-between">
      <span>{{ error }}</span>
      <button @click="error = ''" class="ml-4 text-red-500 hover:text-red-700">&times;</button>
    </div>

    <div v-if="success" class="rounded-lg bg-green-50 border border-green-200 px-4 py-3 text-sm text-green-700 flex items-center justify-between">
      <span>{{ success }}</span>
      <button @click="success = ''" class="ml-4 text-green-500 hover:text-green-700">&times;</button>
    </div>

    <div v-if="loading" class="text-center py-8 text-gray-500 text-sm">Loading...</div>

    <div v-else-if="documents.length === 0" class="text-center py-8 text-gray-500 text-sm">
      No knowledge base documents yet. Click "Seed Starter Entries" or "Add Entry" to get started.
    </div>

    <table v-else class="min-w-full divide-y divide-gray-200">
      <thead class="bg-gray-50">
        <tr>
          <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Title</th>
          <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Category</th>
          <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Source</th>
          <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Severity</th>
          <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Status</th>
          <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Chunks</th>
          <th class="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Enabled</th>
          <th class="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
        </tr>
      </thead>
      <tbody class="bg-white divide-y divide-gray-200">
        <tr v-for="doc in documents" :key="doc.id" class="hover:bg-gray-50">
          <td class="px-4 py-3 text-sm text-gray-900 max-w-xs truncate">
            {{ doc.title }}
            <span v-if="doc.original_filename" class="block text-xs text-gray-400 truncate">{{ doc.original_filename }}</span>
          </td>
          <td class="px-4 py-3 text-sm text-gray-600">
            <span class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-gray-100 text-gray-700">
              {{ doc.category }}
            </span>
          </td>
          <td class="px-4 py-3 text-sm text-gray-500">
            <span :class="sourceClass(doc.source_type)" class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium">
              {{ doc.source_type }}
            </span>
          </td>
          <td class="px-4 py-3 text-sm">
            <span :class="severityClass(doc.severity)" class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium">
              {{ doc.severity }}
            </span>
          </td>
          <td class="px-4 py-3 text-sm">
            <span :class="statusClass(doc.embedding_status)" class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium" :title="doc.embedding_error || ''">
              <span v-if="doc.embedding_status === 'processing' || doc.embedding_status === 'pending'" class="mr-1 inline-block h-2 w-2 rounded-full bg-current animate-pulse" />
              {{ statusLabel(doc.embedding_status) }}
            </span>
          </td>
          <td class="px-4 py-3 text-sm text-gray-600 font-mono">
            {{ doc.chunk_count }}
          </td>
          <td class="px-4 py-3 text-sm">
            <button
              @click="toggleEnabled(doc)"
              :class="doc.enabled ? 'bg-blue-600' : 'bg-gray-300'"
              class="relative inline-flex h-5 w-9 items-center rounded-full transition-colors"
            >
              <span
                :class="doc.enabled ? 'translate-x-5' : 'translate-x-1'"
                class="inline-block h-3 w-3 transform rounded-full bg-white transition-transform"
              />
            </button>
          </td>
          <td class="px-4 py-3 text-sm text-right space-x-2">
            <button @click="reembed(doc)" :disabled="doc.embedding_status === 'processing' || doc.embedding_status === 'pending'" class="text-purple-600 hover:text-purple-800 text-xs font-medium disabled:opacity-50">
              Re-embed
            </button>
            <button v-if="doc.source_type === 'manual'" @click="openEditForm(doc)" class="text-blue-600 hover:text-blue-800 text-xs font-medium">Edit</button>
            <button @click="confirmDelete(doc)" class="text-red-600 hover:text-red-800 text-xs font-medium">Delete</button>
          </td>
        </tr>
      </tbody>
    </table>

    <!-- Add/Edit Modal -->
    <div v-if="showForm" class="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div class="bg-white rounded-lg shadow-xl max-w-lg w-full p-6 max-h-[80vh] overflow-y-auto">
        <h3 class="text-lg font-semibold text-gray-900 mb-4">
          {{ editingDoc ? 'Edit Document' : formMode === 'upload' ? 'Upload File' : 'Add Manual Entry' }}
        </h3>

        <div class="space-y-4">
          <div>
            <label class="block text-sm font-medium text-gray-700 mb-1">Title</label>
            <input v-model="form.title" type="text" class="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:ring-blue-500 focus:border-blue-500" placeholder="Descriptive title for this document" />
          </div>

          <div>
            <label class="block text-sm font-medium text-gray-700 mb-1">Category</label>
            <select v-model="form.category" class="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:ring-blue-500 focus:border-blue-500">
              <option value="metric_patterns">Metric Patterns</option>
              <option value="common_issues">Common Issues</option>
              <option value="platform_quirks">Platform Quirks</option>
              <option value="data_collection">Data Collection</option>
              <option value="best_practices">Best Practices</option>
              <option value="reference">Reference</option>
            </select>
          </div>

          <!-- Manual entry: text area -->
          <div v-if="formMode === 'manual' || editingDoc">
            <label class="block text-sm font-medium text-gray-700 mb-1">Content</label>
            <textarea v-model="form.content" rows="8" class="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:ring-blue-500 focus:border-blue-500" placeholder="Content to be embedded and searchable by the AI agent..." />
          </div>

          <!-- Upload mode: file picker -->
          <div v-if="formMode === 'upload' && !editingDoc">
            <label class="block text-sm font-medium text-gray-700 mb-1">File</label>
            <input
              type="file"
              accept=".pdf,.md,.txt,.markdown"
              @change="onFileSelect"
              class="w-full text-sm text-gray-500 file:mr-4 file:py-2 file:px-4 file:rounded-lg file:border-0 file:text-sm file:font-medium file:bg-blue-50 file:text-blue-700 hover:file:bg-blue-100"
            />
            <p class="mt-1 text-xs text-gray-400">Accepts PDF, Markdown, and plain text files</p>
          </div>

          <div>
            <label class="block text-sm font-medium text-gray-700 mb-1">Severity</label>
            <select v-model="form.severity" class="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:ring-blue-500 focus:border-blue-500">
              <option value="info">Info</option>
              <option value="warning">Warning</option>
              <option value="critical">Critical</option>
            </select>
          </div>
        </div>

        <div class="flex justify-end gap-3 mt-6">
          <button @click="showForm = false" class="px-4 py-2 text-sm font-medium text-gray-700 bg-gray-100 rounded-lg hover:bg-gray-200">
            Cancel
          </button>
          <button @click="saveDocument" :disabled="saving" class="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700 disabled:opacity-50">
            {{ saving ? 'Saving...' : (editingDoc ? 'Update' : (formMode === 'upload' ? 'Upload' : 'Create')) }}
          </button>
        </div>
      </div>
    </div>

    <!-- Delete Confirmation -->
    <div v-if="showDeleteConfirm" class="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div class="bg-white rounded-lg shadow-xl max-w-md w-full p-6">
        <h3 class="text-lg font-semibold text-gray-900 mb-2">Delete Document</h3>
        <p class="text-sm text-gray-600 mb-4">
          Are you sure you want to delete "{{ deletingDoc?.title }}"? This will remove the document and all its embedded chunks. This cannot be undone.
        </p>
        <div class="flex justify-end gap-3">
          <button @click="showDeleteConfirm = false" class="px-4 py-2 text-sm font-medium text-gray-700 bg-gray-100 rounded-lg hover:bg-gray-200">
            Cancel
          </button>
          <button @click="doDelete" :disabled="deleting" class="px-4 py-2 text-sm font-medium text-white bg-red-600 rounded-lg hover:bg-red-700 disabled:opacity-50">
            {{ deleting ? 'Deleting...' : 'Delete' }}
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted, computed } from 'vue';
import axios from 'axios';

const documents = ref([]);
const loading = ref(true);
const error = ref('');
const success = ref('');
const saving = ref(false);
const seeding = ref(false);
const deleting = ref(false);

const showForm = ref(false);
const formMode = ref('manual');
const editingDoc = ref(null);
const form = ref(emptyForm());
const selectedFile = ref(null);

const showDeleteConfirm = ref(false);
const deletingDoc = ref(null);

let pollTimer = null;

const hasProcessing = computed(() =>
  documents.value.some(d => d.embedding_status === 'pending' || d.embedding_status === 'processing')
);

function emptyForm() {
  return { title: '', category: 'common_issues', content: '', severity: 'info' };
}

function severityClass(sev) {
  switch (sev) {
    case 'critical': return 'bg-red-100 text-red-700';
    case 'warning': return 'bg-yellow-100 text-yellow-700';
    default: return 'bg-blue-100 text-blue-700';
  }
}

function sourceClass(type) {
  switch (type) {
    case 'pdf': return 'bg-orange-100 text-orange-700';
    case 'markdown': return 'bg-green-100 text-green-700';
    default: return 'bg-gray-100 text-gray-600';
  }
}

function statusClass(status) {
  switch (status) {
    case 'ready': return 'bg-green-100 text-green-700';
    case 'processing': return 'bg-blue-100 text-blue-700';
    case 'pending': return 'bg-yellow-100 text-yellow-700';
    case 'failed': return 'bg-red-100 text-red-700';
    default: return 'bg-gray-100 text-gray-600';
  }
}

function statusLabel(status) {
  switch (status) {
    case 'ready': return 'Ready';
    case 'processing': return 'Embedding...';
    case 'pending': return 'Pending';
    case 'failed': return 'Failed';
    default: return status;
  }
}

function openForm(mode) {
  formMode.value = mode;
  editingDoc.value = null;
  form.value = emptyForm();
  selectedFile.value = null;
  showForm.value = true;
}

function openEditForm(doc) {
  formMode.value = 'manual';
  editingDoc.value = doc;
  form.value = {
    title: doc.title,
    category: doc.category,
    content: doc.original_content || '',
    severity: doc.severity,
  };
  selectedFile.value = null;
  showForm.value = true;
}

function onFileSelect(e) {
  selectedFile.value = e.target.files[0] || null;
}

function confirmDelete(doc) {
  deletingDoc.value = doc;
  showDeleteConfirm.value = true;
}

function startPolling() {
  stopPolling();
  pollTimer = setInterval(async () => {
    if (!hasProcessing.value) {
      stopPolling();
      return;
    }
    try {
      const { data } = await axios.get('/api/admin/knowledge-base');
      documents.value = data;
      if (!documents.value.some(d => d.embedding_status === 'pending' || d.embedding_status === 'processing')) {
        stopPolling();
      }
    } catch (_) {
      // ignore poll errors
    }
  }, 3000);
}

function stopPolling() {
  if (pollTimer) {
    clearInterval(pollTimer);
    pollTimer = null;
  }
}

async function loadDocuments() {
  loading.value = true;
  try {
    const { data } = await axios.get('/api/admin/knowledge-base');
    documents.value = data;
    if (hasProcessing.value) startPolling();
  } catch (e) {
    error.value = 'Failed to load knowledge base documents.';
  } finally {
    loading.value = false;
  }
}

async function saveDocument() {
  saving.value = true;
  error.value = '';
  try {
    if (editingDoc.value) {
      const payload = {
        title: form.value.title,
        category: form.value.category,
        severity: form.value.severity,
      };
      if (form.value.content.trim()) {
        payload.content = form.value.content;
      }
      await axios.put(`/api/admin/knowledge-base/${editingDoc.value.id}`, payload);
      success.value = 'Document updated.';
    } else if (formMode.value === 'upload') {
      if (!selectedFile.value) {
        error.value = 'Please select a file.';
        return;
      }
      const fd = new FormData();
      fd.append('title', form.value.title);
      fd.append('category', form.value.category);
      fd.append('severity', form.value.severity);
      fd.append('file', selectedFile.value);
      await axios.post('/api/admin/knowledge-base/upload', fd, {
        headers: { 'Content-Type': 'multipart/form-data' },
      });
      success.value = 'File uploaded — embedding in progress.';
    } else {
      await axios.post('/api/admin/knowledge-base', {
        title: form.value.title,
        category: form.value.category,
        content: form.value.content,
        severity: form.value.severity,
      });
      success.value = 'Document created — embedding in progress.';
    }

    showForm.value = false;
    await loadDocuments();
  } catch (e) {
    error.value = `Failed to save: ${e.response?.data?.message || e.message}`;
  } finally {
    saving.value = false;
  }
}

async function toggleEnabled(doc) {
  try {
    await axios.put(`/api/admin/knowledge-base/${doc.id}`, { enabled: !doc.enabled });
    doc.enabled = !doc.enabled;
  } catch (e) {
    error.value = 'Failed to toggle document.';
  }
}

async function reembed(doc) {
  error.value = '';
  try {
    await axios.post(`/api/admin/knowledge-base/${doc.id}/reembed`);
    success.value = `Re-embedding "${doc.title}" — processing in background.`;
    await loadDocuments();
  } catch (e) {
    error.value = `Failed to re-embed: ${e.response?.data?.message || e.message}`;
  }
}

async function doDelete() {
  deleting.value = true;
  error.value = '';
  try {
    await axios.delete(`/api/admin/knowledge-base/${deletingDoc.value.id}`);
    showDeleteConfirm.value = false;
    success.value = 'Document deleted.';
    await loadDocuments();
  } catch (e) {
    error.value = 'Failed to delete document.';
  } finally {
    deleting.value = false;
  }
}

const SEED_ENTRIES = [
  {
    category: 'platform_quirks',
    title: 'Materialized view INSERT queries appear as failed SELECTs in ClickHouse',
    content: 'When ClickHouse refreshes a materialized view, it internally runs an INSERT...SELECT. The SELECT part is logged in system.query_log with type=\'SELECT\' and may show a non-zero exception_code. This is normal internal ClickHouse behavior — not actual query failures. These entries should be excluded or annotated when analyzing query error rates.',
    severity: 'info',
  },
  {
    category: 'metric_patterns',
    title: 'Prometheus metric names are stored under OpenTelemetry names',
    content: 'Metrics collected via Prometheus (e.g., kube_pod_info, node_cpu_seconds_total) are stored internally under OpenTelemetry semantic conventions (e.g., k8s.pod.phase, system.cpu.time). If a dashboard widget shows "No data", check whether the Prometheus metric name has a corresponding OTel mapping. The platform automatically translates most common names, but some older or custom metrics may need manual mapping.',
    severity: 'info',
  },
  {
    category: 'metric_patterns',
    title: 'Gauge vs counter: understanding metric types',
    content: 'Gauges represent current values that can go up and down (e.g., memory usage, active connections). Counters are cumulative totals that only increase (e.g., total requests, total bytes sent). A counter resetting to 0 indicates a process restart, not data loss. When analyzing charts, rate() should be applied to counters to see per-second change, while gauges should be viewed as raw values.',
    severity: 'info',
  },
  {
    category: 'common_issues',
    title: 'Timestamps displayed as 1970s/1971 dates',
    content: 'If chart timestamps show dates from 1970-1972, the issue is likely a unit mismatch. Metrics stored in milliseconds (unix_milli) need to be divided by 1000 to convert to epoch seconds for charting libraries. The platform handles this automatically for known timestamp columns, but custom queries may need explicit conversion.',
    severity: 'warning',
  },
  {
    category: 'common_issues',
    title: 'Stat widgets showing 0 instead of "No data"',
    content: 'When a stat or gauge widget query returns no rows, the widget may display "0" instead of "No data available". This typically means the metric is not being collected for the selected time range or service. Check that the metric exists, the time range is appropriate, and the service selector matches a running service.',
    severity: 'info',
  },
  {
    category: 'platform_quirks',
    title: 'Legend template variables showing as {{label}} literals',
    content: 'If a chart legend shows literal template syntax like {{instance}} instead of actual values, it means the label referenced in the legend template could not be resolved. This usually happens because Prometheus-style label names (e.g., "instance") need to be mapped to their OpenTelemetry equivalents (e.g., "service.instance.id"). The platform handles most common mappings automatically.',
    severity: 'info',
  },
  {
    category: 'data_collection',
    title: 'Spiky query latencies are normal in ClickHouse',
    content: 'ClickHouse query execution times often show a spiky distribution rather than smooth curves. This is expected behavior due to the columnar engine\'s batch processing nature. Occasional high-latency spikes (2-5x the median) are normal and usually correspond to queries touching larger data ranges or running during merge operations. Only sustained elevated latencies or order-of-magnitude increases warrant investigation.',
    severity: 'info',
  },
];

async function seedEntries() {
  seeding.value = true;
  error.value = '';
  let created = 0;
  try {
    for (const entry of SEED_ENTRIES) {
      try {
        await axios.post('/api/admin/knowledge-base', entry);
        created++;
      } catch (e) {
        // skip individual errors
      }
    }
    success.value = `Seeded ${created} entries — embedding in progress.`;
    await loadDocuments();
  } catch (e) {
    error.value = 'Failed to seed entries.';
  } finally {
    seeding.value = false;
  }
}

onMounted(loadDocuments);
onUnmounted(stopPolling);
</script>
