<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <div v-if="loading" class="flex items-center justify-center py-12">
        <div class="spinner w-8 h-8 border-4 border-primary-600 border-t-transparent rounded-full"></div>
        <span class="ml-3 text-gray-600">Loading settings...</span>
      </div>

      <div v-else-if="project" class="space-y-6">
        <div>
          <h1 class="text-2xl font-semibold text-gray-900">Project settings</h1>
          <p class="mt-1 text-sm text-gray-600">
            Settings that apply to all services (API access, data, and project).
          </p>
        </div>

        <!-- SDK Keys Section -->
        <BaseCard>
          <template #header>
            <div class="flex items-center justify-between">
              <h2 class="text-lg font-semibold text-gray-900">SDK Keys</h2>
              <BaseButton variant="primary" size="sm" @click="openCreateModal">
                Create Key
              </BaseButton>
            </div>
          </template>
          <div class="space-y-4">
            <p class="text-sm text-gray-600">
              SDK keys authenticate your application with the gateway, watch, and SDKs. Keep them secure.
            </p>

            <div v-if="filteredKeys.length === 0" class="text-center py-8 text-gray-500">
              <p class="mb-4">
                No SDK keys found. Create one to get started.
              </p>
              <BaseButton variant="primary" @click="openCreateModal">
                Create SDK Key
              </BaseButton>
            </div>

            <div v-else class="space-y-3">
              <div
                v-for="key in filteredKeys"
                :key="key.id"
                class="border border-gray-200 rounded-lg p-4"
              >
                <div class="flex items-start justify-between gap-4">
                  <div class="min-w-0 flex-1 space-y-2">
                    <div class="flex items-center gap-3">
                      <span v-if="key.label" class="text-sm font-medium text-gray-900 truncate">
                        {{ key.label }}
                      </span>
                      <code class="text-xs font-mono text-gray-500 bg-gray-100 px-2 py-0.5 rounded">
                        dh_...{{ key.key_prefix }}
                      </code>
                    </div>

                    <div v-if="key.scopes?.length" class="flex flex-wrap gap-1.5">
                      <span
                        v-for="scope in key.scopes"
                        :key="scope"
                        class="inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium"
                        :class="scopeBadgeClass(scope)"
                      >
                        {{ scope }}
                      </span>
                    </div>

                    <div class="flex items-center gap-4 text-xs text-gray-500">
                      <span>Created {{ formatDate(key.created_at) }}</span>
                      <span v-if="key.expires_at">
                        Expires {{ formatDate(key.expires_at) }}
                      </span>
                      <span v-if="key.created_by" class="truncate">
                        by {{ key.created_by }}
                      </span>
                    </div>
                  </div>

                  <BaseButton
                    variant="danger"
                    size="sm"
                    @click="confirmDeleteKey(key)"
                    :loading="deletingKeyId === key.id"
                    :disabled="!!deletingKeyId"
                    title="Revoke this key"
                  >
                    Revoke
                  </BaseButton>
                </div>
              </div>

            </div>
          </div>
        </BaseCard>

        <!-- Data & Privacy -->
        <BaseCard>
          <template #header>
            <h2 class="text-lg font-semibold text-gray-900">Data & Privacy</h2>
          </template>
          <div class="space-y-4">
            <div class="flex items-start justify-between">
              <div class="flex-1">
                <label class="block text-sm font-medium text-gray-700 mb-1">
                  Mask PII in logs
                </label>
                <p class="text-sm text-gray-500">
                  When enabled, sensitive data (emails, SSNs, credit cards, IPs, AWS keys) in log messages is replaced with [REDACTED] before storage.
                </p>
              </div>
              <label class="relative inline-flex items-center cursor-pointer ml-4">
                <input
                  v-model="piiMaskingEnabled"
                  type="checkbox"
                  class="sr-only peer"
                />
                <div class="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-primary-500 rounded-full peer peer-checked:after:translate-x-full rtl:peer-checked:after:-translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-primary-600"></div>
                <span class="ms-3 text-sm font-medium text-gray-700">{{ piiMaskingEnabled ? 'On' : 'Off' }}</span>
              </label>
            </div>
            <div class="flex justify-end">
              <BaseButton
                variant="primary"
                @click="updatePiiMasking"
                :disabled="piiMaskingEnabled === (project.settings?.pii_masking_enabled ?? true) || savingPii"
                :loading="savingPii"
              >
                Save
              </BaseButton>
            </div>
          </div>
        </BaseCard>

        <!-- Span Metrics -->
        <BaseCard>
          <template #header>
            <h2 class="text-lg font-semibold text-gray-900">Span Metrics</h2>
          </template>
          <div class="space-y-4">
            <div class="flex items-start justify-between">
              <div class="flex-1">
                <label class="block text-sm font-medium text-gray-700 mb-1">
                  Generate RED metrics from spans
                </label>
                <p class="text-sm text-gray-500">
                  Automatically derive rate, error, and duration (RED) metrics from ingested traces.
                  Creates <code class="text-xs bg-gray-100 px-1 rounded">http.server.request.duration</code> metric series
                  from server spans, enabling PromQL dashboards and alerts without separate metrics instrumentation.
                </p>
              </div>
              <label class="relative inline-flex items-center cursor-pointer ml-4">
                <input
                  v-model="spanMetricsEnabled"
                  type="checkbox"
                  class="sr-only peer"
                />
                <div class="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-primary-500 rounded-full peer peer-checked:after:translate-x-full rtl:peer-checked:after:-translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:start-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-primary-600"></div>
                <span class="ms-3 text-sm font-medium text-gray-700">{{ spanMetricsEnabled ? 'On' : 'Off' }}</span>
              </label>
            </div>
            <div class="flex justify-end">
              <BaseButton
                variant="primary"
                @click="updateSpanMetrics"
                :disabled="spanMetricsEnabled === (project.settings?.span_metrics_enabled ?? false) || savingSpanMetrics"
                :loading="savingSpanMetrics"
              >
                Save
              </BaseButton>
            </div>
          </div>
        </BaseCard>

        <!-- Danger Zone -->
        <BaseCard>
          <template #header>
            <h2 class="text-lg font-semibold text-red-600">Danger Zone</h2>
          </template>
          <div class="space-y-4">
            <div>
              <h3 class="text-sm font-medium text-gray-900 mb-1">
                Delete Project
              </h3>
              <p class="text-sm text-gray-600 mb-4">
                Once you delete a project, there is no going back. Please be certain.
              </p>
              <BaseButton
                variant="danger"
                size="sm"
                @click="confirmDelete"
                :disabled="deleting"
                :loading="deleting"
              >
                Delete Project
              </BaseButton>
            </div>
          </div>
        </BaseCard>
      </div>
    </div>

    <!-- Create Key Modal -->
    <Teleport to="body">
      <div
        v-if="showCreateModal"
        class="fixed inset-0 z-50 flex items-center justify-center"
      >
        <div class="fixed inset-0 bg-black/40" @click="showCreateModal = false"></div>
        <div class="relative z-10 w-full max-w-lg rounded-xl bg-white p-6 shadow-xl mx-4">
          <h3 class="text-lg font-semibold text-gray-900 mb-4">
            Create SDK Key
          </h3>

          <form @submit.prevent="createApiKey" class="space-y-5">
            <div>
              <label for="key-label" class="block text-sm font-medium text-gray-700 mb-1">
                Label
              </label>
              <input
                id="key-label"
                v-model="newKeyLabel"
                type="text"
                placeholder="e.g. production, staging, CI..."
                class="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-primary-500 focus:outline-none focus:ring-1 focus:ring-primary-500"
              />
            </div>

            <div>
              <label class="block text-sm font-medium text-gray-700 mb-1">
                Scopes
              </label>
              <ScopeSelector v-model="newKeyScopes" :max-scopes="allScopes" />
            </div>

            <div>
              <label for="key-expires" class="block text-sm font-medium text-gray-700 mb-1">
                Expiry date <span class="text-gray-400 font-normal">(optional)</span>
              </label>
              <input
                id="key-expires"
                v-model="newKeyExpiresAt"
                type="date"
                :min="todayISO"
                class="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-primary-500 focus:outline-none focus:ring-1 focus:ring-primary-500"
              />
            </div>

            <div class="flex items-center justify-end gap-3 pt-2">
              <BaseButton variant="secondary" type="button" @click="showCreateModal = false">
                Cancel
              </BaseButton>
              <BaseButton
                variant="primary"
                type="submit"
                :loading="creatingKey"
                :disabled="newKeyScopes.length === 0"
              >
                Create
              </BaseButton>
            </div>
          </form>
        </div>
      </div>
    </Teleport>

    <!-- Key Reveal Dialog -->
    <Teleport to="body">
      <div
        v-if="showKeyReveal"
        class="fixed inset-0 z-50 flex items-center justify-center"
      >
        <div class="fixed inset-0 bg-black/40"></div>
        <div class="relative z-10 w-full max-w-lg rounded-xl bg-white p-6 shadow-xl mx-4">
          <div class="flex items-start gap-3 mb-4">
            <div class="flex-shrink-0 rounded-full bg-amber-100 p-2">
              <svg class="w-5 h-5 text-amber-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
            </div>
            <div>
              <h3 class="text-lg font-semibold text-gray-900">Your new API key</h3>
              <p class="mt-1 text-sm text-amber-700">
                This key will not be shown again. Copy it now and store it securely.
              </p>
            </div>
          </div>

          <div class="flex items-center gap-2 mb-6">
            <code class="flex-1 text-sm font-mono bg-gray-100 px-3 py-2 rounded-lg break-all select-all border border-gray-200">
              {{ revealedKey }}
            </code>
            <button
              @click="copyToClipboard(revealedKey)"
              class="flex-shrink-0 p-2 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-lg transition-colors"
              :class="copiedReveal ? 'text-green-600 hover:text-green-600' : ''"
              title="Copy to clipboard"
            >
              <svg v-if="!copiedReveal" class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
              </svg>
              <svg v-else class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
              </svg>
            </button>
          </div>

          <div class="flex justify-end">
            <BaseButton variant="primary" @click="closeKeyReveal">
              Done
            </BaseButton>
          </div>
        </div>
      </div>
    </Teleport>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { format } from 'date-fns'
import axios from 'axios'
import AppLayout from '@/Layouts/AppLayout.vue'
import BaseCard from '@/components/BaseCard.vue'
import BaseButton from '@/components/BaseButton.vue'
import ScopeSelector from '@/components/ScopeSelector.vue'
import { useAuth } from '@/composables/useAuth'

const route = useRoute()
const router = useRouter()
const { user } = useAuth()

const projectId = computed(() => route.params.id)
const project = ref(null)
const keys = ref([])
const loading = ref(false)
const creatingKey = ref(false)
const deletingKeyId = ref(null)
const savingPii = ref(false)
const savingSpanMetrics = ref(false)
const deleting = ref(false)
const piiMaskingEnabled = ref(true)
const spanMetricsEnabled = ref(false)

const activeKeyTab = ref('sdk')

const allScopes = [
  'project:read',
  'project:write',
  'llm:read',
  'llm:write',
  'observability:read',
  'observability:write',
  'herd:read',
  'herd:write',
]

const showCreateModal = ref(false)
const newKeyLabel = ref('')
const newKeyScopes = ref([])
const newKeyExpiresAt = ref('')

const showKeyReveal = ref(false)
const revealedKey = ref('')
const copiedReveal = ref(false)

const todayISO = computed(() => new Date().toISOString().slice(0, 10))

const filteredKeys = computed(() =>
  keys.value.filter((k) => k.key_type === activeKeyTab.value)
)

const SCOPE_COLORS = {
  project: 'bg-blue-50 text-blue-700 ring-1 ring-inset ring-blue-600/20',
  llm: 'bg-purple-50 text-purple-700 ring-1 ring-inset ring-purple-600/20',
  observability: 'bg-green-50 text-green-700 ring-1 ring-inset ring-green-600/20',
  billing: 'bg-amber-50 text-amber-700 ring-1 ring-inset ring-amber-600/20',
}

function scopeBadgeClass(scope) {
  const area = scope.split(':')[0]
  return SCOPE_COLORS[area] || 'bg-gray-50 text-gray-700 ring-1 ring-inset ring-gray-600/20'
}

const fetchProject = async () => {
  loading.value = true
  try {
    const response = await axios.get(`/api/projects/${projectId.value}`)
    project.value = response.data
    piiMaskingEnabled.value = project.value?.settings?.pii_masking_enabled ?? true
    spanMetricsEnabled.value = project.value?.settings?.span_metrics_enabled ?? false
  } catch (error) {
    console.error('Failed to fetch project:', error)
  } finally {
    loading.value = false
  }
}

const fetchKeys = async () => {
  try {
    const response = await axios.get(
      `/api/projects/${projectId.value}/keys`,
      { params: { key_type: activeKeyTab.value } }
    )
    keys.value = [
      ...keys.value.filter((k) => k.key_type !== activeKeyTab.value),
      ...(response.data || []),
    ]
  } catch (error) {
    console.error('Failed to fetch keys:', error)
  }
}


function openCreateModal() {
  newKeyLabel.value = ''
  newKeyScopes.value = []
  newKeyExpiresAt.value = ''
  showCreateModal.value = true
}

const createApiKey = async () => {
  creatingKey.value = true
  try {
    const body = {
      label: newKeyLabel.value || null,
      scopes: newKeyScopes.value,
      key_type: activeKeyTab.value,
      expires_at: newKeyExpiresAt.value || null,
    }
    const response = await axios.post(
      `/api/projects/${projectId.value}/keys`,
      body
    )
    revealedKey.value = response.data.key
    showCreateModal.value = false
    showKeyReveal.value = true
    copiedReveal.value = false
    await fetchKeys()
  } catch (error) {
    console.error('Failed to create API key:', error)
    alert('Failed to create API key')
  } finally {
    creatingKey.value = false
  }
}

function closeKeyReveal() {
  showKeyReveal.value = false
  revealedKey.value = ''
}

const copyToClipboard = async (text) => {
  try {
    await navigator.clipboard.writeText(text)
    if (showKeyReveal.value) {
      copiedReveal.value = true
      setTimeout(() => { copiedReveal.value = false }, 2000)
    }
  } catch (err) {
    console.error('Failed to copy:', err)
  }
}

const confirmDeleteKey = async (key) => {
  if (!confirm('Revoke this SDK key? It will immediately stop working.')) {
    return
  }
  deletingKeyId.value = key.id
  try {
    await axios.delete(`/api/projects/${projectId.value}/keys/${key.id}`)
    keys.value = keys.value.filter((k) => k.id !== key.id)
  } catch (error) {
    console.error('Failed to revoke API key:', error)
    alert('Failed to revoke API key')
  } finally {
    deletingKeyId.value = null
  }
}

const updatePiiMasking = async () => {
  savingPii.value = true
  try {
    const response = await axios.patch(`/api/projects/${projectId.value}`, {
      pii_masking_enabled: piiMaskingEnabled.value,
    })
    project.value = response.data
    alert('PII masking setting updated.')
  } catch (error) {
    console.error('Failed to update PII masking:', error)
    alert('Failed to update setting')
  } finally {
    savingPii.value = false
  }
}

const updateSpanMetrics = async () => {
  savingSpanMetrics.value = true
  try {
    const response = await axios.patch(`/api/projects/${projectId.value}`, {
      span_metrics_enabled: spanMetricsEnabled.value,
    })
    project.value = response.data
    alert('Span metrics setting updated.')
  } catch (error) {
    console.error('Failed to update span metrics:', error)
    alert('Failed to update setting')
  } finally {
    savingSpanMetrics.value = false
  }
}

const confirmDelete = async () => {
  if (!confirm(`Are you sure you want to delete "${project.value?.name}"? This action cannot be undone.`)) {
    return
  }
  
  const typed = prompt(`This will permanently delete all errors, traces, and settings. Type "${project.value?.name}" to confirm.`)
  if (typed !== project.value?.name) {
    return
  }
  
  deleting.value = true
  try {
    await axios.delete(`/api/projects/${projectId.value}`)
    router.push('/')
  } catch (error) {
    console.error('Failed to delete project:', error)
    alert('Failed to delete project')
    deleting.value = false
  }
}

const formatDate = (dateString) => {
  return format(new Date(dateString), 'MMM d, yyyy')
}

onMounted(async () => {
  await Promise.all([fetchProject(), fetchKeys()])
})
</script>

<style scoped>
.spinner {
  animation: spin 0.6s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}
</style>
