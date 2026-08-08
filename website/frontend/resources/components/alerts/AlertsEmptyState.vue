<template>
  <BaseCard class="mt-6">
    <div class="text-center py-12">
      <svg class="w-16 h-16 mx-auto mb-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 17h5l-1.405-1.405A2.032 2.032 0 0118 14.158V11a6.002 6.002 0 00-4-5.659V5a2 2 0 10-4 0v.341C7.67 6.165 6 8.388 6 11v3.159c0 .538-.214 1.055-.595 1.436L4 17h5m6 0v1a3 3 0 11-6 0v-1m6 0H9" />
      </svg>
      <h3 class="text-lg font-medium text-gray-900 dark:text-gray-100 mb-2">{{ title }}</h3>
      <p class="text-sm text-gray-500 dark:text-gray-400 mb-4">{{ description }}</p>
      <slot name="action">
        <router-link
          :to="`/p/${projectId}/alerts/new`"
          class="inline-flex items-center gap-2 px-4 py-2 bg-indigo-600 text-white rounded-md hover:bg-indigo-700 font-medium text-sm"
        >
          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v16m8-8H4" />
          </svg>
          {{ actionText }}
        </router-link>
      </slot>
    </div>
  </BaseCard>
</template>

<script setup>
import BaseCard from '../BaseCard.vue';

defineProps({
  title: {
    type: String,
    default: 'No alert rules yet',
  },
  description: {
    type: String,
    default: 'Create an alert rule to get started with monitoring and notifications',
  },
  actionText: {
    type: String,
    default: 'Create Alert Rule',
  },
  projectId: {
    type: String,
    required: true,
  },
});
</script>
