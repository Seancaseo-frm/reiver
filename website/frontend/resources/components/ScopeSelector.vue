<template>
  <div
    class="rounded-lg border border-gray-200 bg-white p-4 shadow-sm dark:border-gray-700 dark:bg-gray-900/50"
  >
    <div class="mb-4 flex flex-wrap gap-2">
      <button
        type="button"
        class="rounded-full border border-gray-200 bg-gray-50 px-3 py-1 text-xs font-medium text-gray-700 transition hover:border-indigo-300 hover:bg-indigo-50 hover:text-indigo-800 dark:border-gray-600 dark:bg-gray-800 dark:text-gray-300 dark:hover:border-indigo-500 dark:hover:bg-indigo-950/50 dark:hover:text-indigo-200"
        @click="applyPreset(PRESET_READ_ONLY)"
      >
        Read-only
      </button>
      <button
        type="button"
        class="rounded-full border border-gray-200 bg-gray-50 px-3 py-1 text-xs font-medium text-gray-700 transition hover:border-indigo-300 hover:bg-indigo-50 hover:text-indigo-800 dark:border-gray-600 dark:bg-gray-800 dark:text-gray-300 dark:hover:border-indigo-500 dark:hover:bg-indigo-950/50 dark:hover:text-indigo-200"
        @click="applyPreset(PRESET_STANDARD)"
      >
        Standard
      </button>
      <button
        type="button"
        class="rounded-full border border-gray-200 bg-gray-50 px-3 py-1 text-xs font-medium text-gray-700 transition hover:border-indigo-300 hover:bg-indigo-50 hover:text-indigo-800 dark:border-gray-600 dark:bg-gray-800 dark:text-gray-300 dark:hover:border-indigo-500 dark:hover:bg-indigo-950/50 dark:hover:text-indigo-200"
        @click="applyPreset(PRESET_FULL)"
      >
        Full access
      </button>
    </div>

    <div class="space-y-5">
      <div v-for="group in scopeGroups" :key="group.label">
        <h3 class="mb-2 text-sm font-medium text-gray-600 dark:text-gray-400">
          {{ group.label }}
        </h3>
        <div class="space-y-2">
          <label
            v-for="row in group.rows"
            :key="row.scope"
            class="flex cursor-pointer items-center gap-2"
            :class="[
              row.indent ? 'ml-6' : '',
              !isAllowed(row.scope) ? 'cursor-not-allowed opacity-50' : ''
            ]"
          >
            <input
              type="checkbox"
              :checked="isChecked(row.scope)"
              :disabled="!isAllowed(row.scope)"
              class="h-4 w-4 rounded border-gray-300 text-indigo-600 focus:ring-2 focus:ring-indigo-500 focus:ring-offset-0 disabled:cursor-not-allowed dark:border-gray-600 dark:bg-gray-800 dark:text-indigo-500 dark:focus:ring-indigo-400 dark:focus:ring-offset-gray-900"
              @change="onCheckboxChange(row.scope, $event)"
            />
            <span
              class="select-none font-mono text-sm text-gray-800 dark:text-gray-200"
              :class="!isAllowed(row.scope) ? 'text-gray-500 dark:text-gray-500' : ''"
            >
              {{ row.scope }}
            </span>
          </label>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'

const PRESET_READ_ONLY = [
  'project:read',
  'llm:read',
  'observability:read',
  'herd:read'
] as const

const PRESET_STANDARD = [
  ...PRESET_READ_ONLY,
  'llm:write',
  'observability:write',
  'herd:write'
] as const

const PRESET_FULL = [
  'project:read',
  'project:write',
  'llm:read',
  'llm:write',
  'observability:read',
  'observability:write',
  'herd:read',
  'herd:write'
] as const

const ALL_SCOPES_ORDERED = [...PRESET_FULL]

type ScopeRow = { scope: string; indent: boolean }

const scopeGroups: { label: string; rows: ScopeRow[] }[] = [
  {
    label: 'Project',
    rows: [
      { scope: 'project:read', indent: false },
      { scope: 'project:write', indent: true }
    ]
  },
  {
    label: 'Prompt Hub',
    rows: [
      { scope: 'llm:read', indent: false },
      { scope: 'llm:write', indent: true }
    ]
  },
  {
    label: 'Observability',
    rows: [
      { scope: 'observability:read', indent: false },
      { scope: 'observability:write', indent: true }
    ]
  },
  {
    label: 'Herd',
    rows: [
      { scope: 'herd:read', indent: false },
      { scope: 'herd:write', indent: true }
    ]
  }
]

const props = withDefaults(
  defineProps<{
    modelValue: string[]
    maxScopes: string[]
  }>(),
  {
    modelValue: () => [],
    maxScopes: () => ['project:read', 'project:write', 'llm:read', 'llm:write', 'observability:read', 'observability:write', 'herd:read', 'herd:write']
  }
)

const emit = defineEmits<{
  'update:modelValue': [value: string[]]
}>()

const maxSet = computed(() => new Set(props.maxScopes))

function isAllowed(scope: string): boolean {
  return maxSet.value.has(scope)
}

function isChecked(scope: string): boolean {
  return props.modelValue.includes(scope)
}

function pairedWrite(readScope: string): string | null {
  if (readScope.endsWith(':read')) {
    return readScope.replace(/:read$/, ':write')
  }
  return null
}

function pairedRead(writeScope: string): string {
  return writeScope.replace(/:write$/, ':read')
}

function orderSelection(set: Set<string>): string[] {
  return ALL_SCOPES_ORDERED.filter((s) => set.has(s))
}

function emitSelection(set: Set<string>) {
  emit('update:modelValue', orderSelection(set))
}

function onCheckboxChange(scope: string, e: Event) {
  const target = e.target as HTMLInputElement
  onToggle(scope, target.checked)
}

function onToggle(scope: string, checked: boolean) {
  if (!isAllowed(scope)) return

  const next = new Set(props.modelValue)

  if (scope.endsWith(':write')) {
    const read = pairedRead(scope)
    if (checked) {
      if (isAllowed(read)) next.add(read)
      next.add(scope)
    } else {
      next.delete(scope)
    }
  } else {
    const write = pairedWrite(scope)
    if (checked) {
      next.add(scope)
    } else {
      next.delete(scope)
      if (write && next.has(write)) next.delete(write)
    }
  }

  emitSelection(next)
}

function applyPreset(preset: readonly string[]) {
  const next = new Set<string>()
  for (const s of preset) {
    if (!isAllowed(s)) continue
    next.add(s)
    if (s.endsWith(':write')) {
      const read = pairedRead(s)
      if (isAllowed(read)) next.add(read)
    }
  }
  emitSelection(next)
}
</script>
