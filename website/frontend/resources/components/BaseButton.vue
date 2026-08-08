<template>
  <button
    :class="[
      'base-button',
      variant,
      size,
      {
        'disabled': disabled,
        'loading': loading,
      },
      $attrs.class,
    ]"
    :disabled="disabled || loading"
    v-bind="$attrs"
    @click="$emit('click', $event)"
  >
    <span v-if="loading" class="spinner mr-2"></span>
    <slot />
  </button>
</template>

<script setup>
defineProps({
  variant: {
    type: String,
    default: 'primary',
    validator: (value) => ['primary', 'secondary', 'danger', 'ghost', 'link'].includes(value),
  },
  size: {
    type: String,
    default: 'md',
    validator: (value) => ['sm', 'md', 'lg'].includes(value),
  },
  disabled: {
    type: Boolean,
    default: false,
  },
  loading: {
    type: Boolean,
    default: false,
  },
})

defineEmits(['click'])
</script>

<style scoped>
.base-button {
  @apply inline-flex items-center justify-center font-medium rounded-md transition-colors;
  @apply focus:outline-none focus:ring-2 focus:ring-offset-2;
}

.base-button.primary {
  background-color: #4f46e5;
  color: #ffffff;
  @apply focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500;
}

.base-button.primary:hover {
  background-color: #4338ca;
}

.base-button.primary:active {
  background-color: #3730a3;
}

.base-button.secondary {
  background-color: #f3f4f6;
  color: #111827;
  border: 1px solid #e5e7eb;
  @apply focus:ring-2 focus:ring-offset-2 focus:ring-gray-400;
}

.base-button.secondary:hover {
  background-color: #f9fafb;
  border-color: #d1d5db;
}

.base-button.secondary:active {
  background-color: #e5e7eb;
}

.base-button.danger {
  background-color: #ef4444;
  color: #ffffff;
  @apply focus:ring-2 focus:ring-offset-2 focus:ring-red-400;
}

.base-button.danger:hover {
  background-color: #dc2626;
}

.base-button.danger:active {
  background-color: #b91c1c;
}

.base-button.ghost {
  background-color: transparent;
  color: #111827;
  @apply focus:ring-2 focus:ring-offset-2 focus:ring-indigo-400;
}

.base-button.ghost:hover {
  background-color: #eef2ff;
  color: #111827;
}

.base-button.ghost:active {
  background-color: #e0e7ff;
}

.base-button.link {
  background-color: transparent;
  color: #4f46e5;
  text-decoration: underline;
  @apply focus:ring-2 focus:ring-offset-2 focus:ring-indigo-400;
}

.base-button.link:hover {
  color: #4338ca;
}

/* Sizes */
.base-button.sm {
  @apply px-3 py-1.5 text-sm;
}

.base-button.md {
  @apply px-4 py-2 text-base;
}

.base-button.lg {
  @apply px-6 py-3 text-lg;
}

/* States */
.base-button.disabled,
.base-button:disabled {
  @apply opacity-50 cursor-not-allowed;
}

.spinner {
  @apply inline-block w-4 h-4 border-2 border-current border-t-transparent rounded-full;
  animation: spin 0.6s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}
</style>
