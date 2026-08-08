<template>
  <AppLayout :user="user" :current-project="project">
    <div class="max-w-[1200px] mx-auto px-8 py-6">
      <!-- Header -->
      <div class="mb-6">
        <div>
          <div class="flex items-center gap-2 text-sm text-gray-500 dark:text-gray-400 mb-1">
            <router-link
              :to="`/p/${projectId}/warehouse/sources`"
              class="hover:text-primary-600"
            >Sources</router-link>
            <span>/</span>
            <span>{{ sourceName }}</span>
            <span>/</span>
            <span class="text-gray-900 dark:text-gray-100">{{ tableName }}</span>
          </div>
          <h1 class="text-2xl font-semibold text-gray-900">{{ sourceName }}.{{ tableName }}</h1>
          <p class="text-sm text-gray-500 dark:text-gray-400 mt-1">
            Table schema and column configuration
          </p>
        </div>
      </div>

      <!-- Loading -->
      <div v-if="loading" class="flex items-center justify-center py-12">
        <svg class="animate-spin h-8 w-8 text-primary-600" fill="none" viewBox="0 0 24 24">
          <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4" />
          <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
        </svg>
      </div>

      <!-- Error -->
      <div v-else-if="error" class="text-center py-12">
        <p class="text-red-600 dark:text-red-400">{{ error }}</p>
        <BaseButton variant="secondary" size="sm" class="mt-4" @click="loadData">
          Retry
        </BaseButton>
      </div>

      <!-- Content -->
      <div v-else class="space-y-6">
        <!-- Full-Text Search Configuration -->
        <BaseCard>
          <template #header>
            <div class="flex items-center justify-between">
              <div>
                <h2 class="text-sm font-semibold text-gray-900 dark:text-gray-100">
                  Full-Text Search
                </h2>
                <p class="text-xs text-gray-500 dark:text-gray-400 mt-0.5">
                  Enable substring search (LIKE '%term%') on string columns. Indexed columns allow the query engine to skip files that don't contain the search term.
                </p>
              </div>
              <div v-if="fulltextDirty" class="flex items-center gap-2">
                <BaseButton variant="secondary" size="sm" @click="resetFulltext">
                  Reset
                </BaseButton>
                <BaseButton variant="primary" size="sm" @click="saveFulltext" :disabled="savingFulltext">
                  {{ savingFulltext ? 'Saving...' : 'Save' }}
                </BaseButton>
              </div>
            </div>
          </template>

          <div v-if="stringColumns.length === 0" class="text-sm text-gray-500 dark:text-gray-400 py-4 text-center">
            No string columns available for full-text indexing.
          </div>
          <div v-else class="divide-y divide-gray-200 dark:divide-gray-700">
            <div
              v-for="col in stringColumns"
              :key="col.name"
              class="flex items-center justify-between py-3 px-1"
            >
              <div>
                <span class="text-sm font-medium text-gray-900 dark:text-gray-100">{{ col.name }}</span>
                <span class="ml-2 text-xs text-gray-400">{{ col.source_type_name }}</span>
              </div>
              <label class="relative inline-flex items-center cursor-pointer">
                <input
                  type="checkbox"
                  :checked="pendingFulltext.includes(col.name)"
                  @change="toggleFulltext(col.name)"
                  class="sr-only peer"
                />
                <div class="w-9 h-5 bg-gray-200 peer-focus:outline-none peer-focus:ring-2 peer-focus:ring-primary-300 dark:peer-focus:ring-primary-800 rounded-full peer dark:bg-gray-700 peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-4 after:w-4 after:transition-all dark:after:border-gray-600 peer-checked:bg-primary-600"></div>
              </label>
            </div>
          </div>

          <div v-if="fulltextSaveError" class="mt-3 text-sm text-red-600 dark:text-red-400">
            {{ fulltextSaveError }}
          </div>
          <div v-if="fulltextSaveSuccess" class="mt-3 text-sm text-green-600 dark:text-green-400">
            Full-text search configuration saved.
          </div>
        </BaseCard>

        <!-- Schema / Columns -->
        <BaseCard>
          <template #header>
            <h2 class="text-sm font-semibold text-gray-900 dark:text-gray-100">
              Columns ({{ columns.length }})
            </h2>
          </template>

          <div class="overflow-x-auto">
            <table class="w-full text-sm">
              <thead>
                <tr class="text-left text-gray-500 dark:text-gray-400 border-b border-gray-200 dark:border-gray-700">
                  <th class="pb-2 font-medium">Name</th>
                  <th class="pb-2 font-medium">Type</th>
                  <th class="pb-2 font-medium">Nullable</th>
                  <th class="pb-2 font-medium">Source Type</th>
                  <th class="pb-2 font-medium">Full-Text</th>
                </tr>
              </thead>
              <tbody class="divide-y divide-gray-100 dark:divide-gray-800">
                <tr v-for="col in columns" :key="col.name" class="text-gray-900 dark:text-gray-100">
                  <td class="py-2 font-mono text-xs">{{ col.name }}</td>
                  <td class="py-2 text-xs text-gray-600 dark:text-gray-400">{{ col.data_type }}</td>
                  <td class="py-2">
                    <span
                      class="px-1.5 py-0.5 text-xs rounded"
                      :class="col.nullable ? 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400' : 'bg-gray-100 text-gray-600 dark:bg-gray-800 dark:text-gray-400'"
                    >
                      {{ col.nullable ? 'YES' : 'NO' }}
                    </span>
                  </td>
                  <td class="py-2 text-xs text-gray-500 dark:text-gray-400">{{ col.source_type_name }}</td>
                  <td class="py-2">
                    <span
                      v-if="pendingFulltext.includes(col.name)"
                      class="px-1.5 py-0.5 text-xs rounded bg-primary-100 text-primary-700 dark:bg-primary-900/30 dark:text-primary-400"
                    >
                      Indexed
                    </span>
                    <span v-else class="text-xs text-gray-400">—</span>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </BaseCard>
      </div>
    </div>
  </AppLayout>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue';
import { useRoute } from 'vue-router';
import axios from 'axios';
import AppLayout from '../../Layouts/AppLayout.vue';
import BaseCard from '../../components/BaseCard.vue';
import BaseButton from '../../components/BaseButton.vue';

const props = defineProps({
  user: { type: Object, required: true },
  project: { type: Object, required: true },
});

const route = useRoute();
const projectId = computed(() => route.params.id);
const sourceName = computed(() => route.params.source_name);
const tableName = computed(() => route.params.table_name);

const loading = ref(true);
const error = ref(null);
const columns = ref([]);
const fulltextColumns = ref([]);
const pendingFulltext = ref([]);
const savingFulltext = ref(false);
const fulltextSaveError = ref(null);
const fulltextSaveSuccess = ref(false);

const stringColumns = computed(() =>
  columns.value.filter(c => {
    const dt = (c.data_type || '').toLowerCase();
    return dt.includes('text') || dt.includes('varchar') || dt.includes('char')
      || dt.includes('string') || dt === 'utf8' || dt === 'largeutf8';
  })
);

const fulltextDirty = computed(() => {
  const a = [...fulltextColumns.value].sort();
  const b = [...pendingFulltext.value].sort();
  return JSON.stringify(a) !== JSON.stringify(b);
});

const toggleFulltext = (columnName) => {
  const idx = pendingFulltext.value.indexOf(columnName);
  if (idx >= 0) {
    pendingFulltext.value.splice(idx, 1);
  } else {
    pendingFulltext.value.push(columnName);
  }
};

const resetFulltext = () => {
  pendingFulltext.value = [...fulltextColumns.value];
  fulltextSaveError.value = null;
  fulltextSaveSuccess.value = false;
};

const saveFulltext = async () => {
  savingFulltext.value = true;
  fulltextSaveError.value = null;
  fulltextSaveSuccess.value = false;

  try {
    await axios.put(
      `/api/projects/${projectId.value}/warehouse/catalog/${sourceName.value}/${tableName.value}/fulltext-columns`,
      { fulltext_columns: pendingFulltext.value }
    );
    fulltextColumns.value = [...pendingFulltext.value];
    fulltextSaveSuccess.value = true;
    setTimeout(() => (fulltextSaveSuccess.value = false), 3000);
  } catch (err) {
    fulltextSaveError.value =
      err.response?.data?.error || err.message || 'Failed to save configuration';
  } finally {
    savingFulltext.value = false;
  }
};

const loadData = async () => {
  loading.value = true;
  error.value = null;

  try {
    // Load catalog entry (schema + fulltext config)
    const [catalogRes, fulltextRes] = await Promise.all([
      axios.get(
        `/api/projects/${projectId.value}/catalog/sources/${sourceName.value}/tables/${tableName.value}`
      ),
      axios.get(
        `/api/projects/${projectId.value}/warehouse/catalog/${sourceName.value}/${tableName.value}/fulltext-columns`
      ),
    ]);

    columns.value = catalogRes.data.columns || [];
    fulltextColumns.value = fulltextRes.data.fulltext_columns || [];
    pendingFulltext.value = [...fulltextColumns.value];
  } catch (err) {
    error.value =
      err.response?.data?.error || err.message || 'Failed to load table details';
  } finally {
    loading.value = false;
  }
};

onMounted(loadData);
</script>
