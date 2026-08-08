<template>
  <div
    :class="[
      'panel',
      {
        'panel-hoverable': hoverable,
        'panel-padded': padded !== false,
      },
      $attrs.class,
    ]"
    v-bind="$attrs"
  >
    <div v-if="$slots.header" class="panel-header">
      <slot name="header" />
    </div>
    <div v-if="$slots.default" :class="['panel-body', { 'panel-padded': padded !== false }]">
      <slot />
    </div>
    <div v-if="$slots.footer" class="panel-footer">
      <slot name="footer" />
    </div>
  </div>
</template>

<script setup>
defineProps({
  hoverable: {
    type: Boolean,
    default: false,
  },
  padded: {
    type: Boolean,
    default: true,
  },
})
</script>

<style scoped>
.panel {
  background-color: #ffffff;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  box-shadow: 0 1px 2px 0 rgba(0, 0, 0, 0.05);
  transition: all 0.2s ease;
  overflow: hidden;
}

.panel-hoverable {
  cursor: pointer;
}

.panel-hoverable:hover {
  border-color: #d1d5db;
  box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
  transform: translateY(-1px);
}

.panel-header {
  border-bottom: 1px solid #f3f4f6;
  padding: 16px 20px;
  background-color: #f9fafb;
  color: #111827;
}

.panel-body {
  color: #111827;
  background-color: #ffffff;
}

.panel-body.panel-padded {
  padding: 20px;
}

.panel-footer {
  border-top: 1px solid #f3f4f6;
  padding: 16px 20px;
  background-color: #f9fafb;
  color: #111827;
}
</style>
