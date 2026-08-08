<template>
  <div class="space-y-4">
    <template v-for="(group, idx) in fieldGroups" :key="idx">
      <!-- Two half-width fields side by side -->
      <div v-if="group.length === 2" class="grid grid-cols-2 gap-4">
        <div v-for="field in group" :key="field.key">
          <label class="block text-sm font-medium text-gray-700 mb-1">
            {{ field.label }}<span v-if="field.required"> *</span>
          </label>
          <FieldInput :field="field" :value="modelValue[field.key]" :input-class="inputClass" @update="update(field, $event)" />
          <p v-if="field.help_text" class="text-xs text-gray-500 mt-1">{{ field.help_text }}</p>
        </div>
      </div>

      <!-- Single full-width field -->
      <div v-else v-for="field in group" :key="field.key">
        <template v-if="field.field_type === 'toggle'">
          <label class="flex items-center gap-2 cursor-pointer">
            <input
              type="checkbox"
              :checked="modelValue[field.key] ?? field.default_value ?? false"
              @change="emit('update:modelValue', { ...modelValue, [field.key]: $event.target.checked })"
              class="rounded border-gray-300 text-primary-600 focus:ring-primary-500"
            />
            <span class="text-sm text-gray-900">{{ field.label }}</span>
          </label>
          <p v-if="field.help_text" class="text-xs text-gray-500 mt-1 ml-6">{{ field.help_text }}</p>
        </template>
        <template v-else>
          <label class="block text-sm font-medium text-gray-700 mb-1">
            {{ field.label }}<span v-if="field.required"> *</span>
          </label>
          <FieldInput :field="field" :value="modelValue[field.key]" :input-class="inputClass" @update="update(field, $event)" />
          <p v-if="field.help_text" class="text-xs text-gray-500 mt-1">{{ field.help_text }}</p>
        </template>
      </div>
    </template>
  </div>
</template>

<script setup>
import { computed, defineComponent, h } from 'vue'

const inputClass = 'w-full px-4 py-2 bg-white border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500 text-gray-900'

const FieldInput = defineComponent({
  props: {
    field: { type: Object, required: true },
    value: { default: undefined },
    inputClass: { type: String, required: true },
  },
  emits: ['update'],
  setup(props, { emit }) {
    return () => {
      const field = props.field
      const cls = props.inputClass

      if (field.field_type === 'select') {
        const options = (field.options || []).map(opt =>
          h('option', { value: opt.value, selected: props.value === opt.value }, opt.label)
        )
        if (!props.value) {
          options.unshift(h('option', { value: '', disabled: true, selected: true }, field.placeholder || 'Select...'))
        }
        return h('select', {
          class: cls,
          required: field.required,
          value: props.value || '',
          onChange: (e) => emit('update', e.target.value),
        }, options)
      }

      if (field.field_type === 'textarea') {
        return h('textarea', {
          class: cls + ' font-mono text-sm',
          rows: 4,
          required: field.required,
          placeholder: field.placeholder || undefined,
          value: props.value || '',
          onInput: (e) => emit('update', e.target.value),
        })
      }

      const typeMap = { text: 'text', password: 'password', number: 'number' }
      return h('input', {
        class: cls,
        type: typeMap[field.field_type] || 'text',
        required: field.required,
        placeholder: field.placeholder || undefined,
        value: props.value || '',
        onInput: (e) => emit('update', e.target.value),
      })
    }
  },
})

const props = defineProps({
  fields: { type: Array, required: true },
  modelValue: { type: Object, required: true },
})

const emit = defineEmits(['update:modelValue'])

const fieldGroups = computed(() => {
  const groups = []
  let i = 0
  const fields = props.fields
  while (i < fields.length) {
    const field = fields[i]
    if (field.width === 'half' && i + 1 < fields.length && fields[i + 1].width === 'half') {
      groups.push([field, fields[i + 1]])
      i += 2
    } else {
      groups.push([field])
      i++
    }
  }
  return groups
})

function update(field, val) {
  const parsed = field.field_type === 'number' && val !== '' ? Number(val) : val
  emit('update:modelValue', { ...props.modelValue, [field.key]: parsed })
}
</script>
