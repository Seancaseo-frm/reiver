<template>
  <div class="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50" @click.self="$emit('close')">
    <div class="bg-white rounded-lg shadow-xl max-w-2xl w-full max-h-[90vh] overflow-y-auto">
      <!-- Header -->
      <div class="flex items-center justify-between p-4 border-b border-gray-200">
        <h2 class="text-lg font-semibold text-gray-900">
          {{ config?.id ? 'Edit SSO Configuration' : 'Add SSO Provider' }}
        </h2>
        <button
          @click="$emit('close')"
          class="text-gray-400 hover:text-gray-600"
        >
          <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
          </svg>
        </button>
      </div>

      <!-- Form -->
      <form @submit.prevent="handleSubmit" class="p-6 space-y-6">
        <!-- Basic Info -->
        <div class="space-y-4">
          <h3 class="text-md font-medium text-gray-900">Basic Information</h3>
          
          <div class="grid grid-cols-2 gap-4">
            <div>
              <label class="block text-sm font-medium text-gray-700 mb-1">
                Provider
              </label>
              <select
                v-model="form.provider"
                class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:ring-2 focus:ring-primary-500"
                required
              >
                <option value="okta">Okta</option>
                <option value="auth0">Auth0</option>
                <option value="entra_id">Microsoft Entra ID</option>
                <option value="onelogin">OneLogin</option>
                <option value="ping">Ping Identity</option>
                <option value="keycloak">Keycloak</option>
                <option value="google">Google Workspace</option>
                <option value="custom">Custom</option>
              </select>
            </div>
            
            <div>
              <label class="block text-sm font-medium text-gray-700 mb-1">
                SSO Type
              </label>
              <select
                v-model="form.sso_type"
                class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:ring-2 focus:ring-primary-500"
                required
              >
                <option value="oidc">OIDC (OpenID Connect)</option>
                <option value="saml">SAML 2.0</option>
              </select>
            </div>
          </div>

          <div>
            <label class="block text-sm font-medium text-gray-700 mb-1">
              Configuration Name
            </label>
            <input
              v-model="form.name"
              type="text"
              class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:ring-2 focus:ring-primary-500"
              placeholder="Production SSO"
              required
            />
          </div>

          <div>
            <label class="block text-sm font-medium text-gray-700 mb-1">
              Domain (for automatic detection)
            </label>
            <input
              v-model="form.domain_name"
              type="text"
              class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:ring-2 focus:ring-primary-500"
              placeholder="example.com"
            />
            <p class="mt-1 text-xs text-gray-500">
              Users with this email domain will be redirected to SSO automatically
            </p>
          </div>
        </div>

        <!-- OIDC Configuration -->
        <div v-if="form.sso_type === 'oidc'" class="space-y-4">
          <h3 class="text-md font-medium text-gray-900">OIDC Configuration</h3>
          
          <div>
            <label class="block text-sm font-medium text-gray-700 mb-1">
              Issuer URL
            </label>
            <input
              v-model="form.issuer_url"
              type="url"
              class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:ring-2 focus:ring-primary-500"
              placeholder="https://your-domain.okta.com"
              required
            />
          </div>

          <div>
            <label class="block text-sm font-medium text-gray-700 mb-1">
              Issuer Alias (optional)
            </label>
            <input
              v-model="form.issuer_alias"
              type="url"
              class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:ring-2 focus:ring-primary-500"
              placeholder="Alternative issuer URL for Azure/Oracle quirks"
            />
          </div>

          <div class="grid grid-cols-2 gap-4">
            <div>
              <label class="block text-sm font-medium text-gray-700 mb-1">
                Client ID
              </label>
              <input
                v-model="form.client_id"
                type="text"
                class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:ring-2 focus:ring-primary-500"
                required
              />
            </div>
            
            <div>
              <label class="block text-sm font-medium text-gray-700 mb-1">
                Client Secret
              </label>
              <input
                v-model="form.client_secret"
                type="password"
                class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:ring-2 focus:ring-primary-500"
                :required="!config?.id"
                :placeholder="config?.id ? '(unchanged)' : ''"
              />
            </div>
          </div>
        </div>

        <!-- SAML Configuration -->
        <div v-if="form.sso_type === 'saml'" class="space-y-4">
          <h3 class="text-md font-medium text-gray-900">SAML Configuration</h3>
          
          <div>
            <label class="block text-sm font-medium text-gray-700 mb-1">
              IdP Entity ID
            </label>
            <input
              v-model="form.saml_entity_id"
              type="text"
              class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:ring-2 focus:ring-primary-500"
              placeholder="https://your-idp.com/metadata"
              required
            />
          </div>

          <div>
            <label class="block text-sm font-medium text-gray-700 mb-1">
              IdP SSO URL
            </label>
            <input
              v-model="form.saml_sso_url"
              type="url"
              class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:ring-2 focus:ring-primary-500"
              placeholder="https://your-idp.com/sso"
              required
            />
          </div>

          <div>
            <label class="block text-sm font-medium text-gray-700 mb-1">
              IdP Certificate (PEM)
            </label>
            <textarea
              v-model="form.saml_certificate"
              rows="4"
              class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:ring-2 focus:ring-primary-500 font-mono text-xs"
              placeholder="-----BEGIN CERTIFICATE-----&#10;...&#10;-----END CERTIFICATE-----"
              :required="!config?.id"
            ></textarea>
          </div>

          <div class="flex items-center">
            <input
              v-model="form.saml_sign_requests"
              type="checkbox"
              id="saml_sign_requests"
              class="h-4 w-4 text-primary-600 focus:ring-primary-500 border-gray-300 rounded"
            />
            <label for="saml_sign_requests" class="ml-2 text-sm text-gray-700">
              Sign SAML requests
            </label>
          </div>
        </div>

        <!-- User Provisioning -->
        <div class="space-y-4">
          <h3 class="text-md font-medium text-gray-900">User Provisioning</h3>
          
          <div class="flex items-center">
            <input
              v-model="form.auto_create_users"
              type="checkbox"
              id="auto_create_users"
              class="h-4 w-4 text-primary-600 focus:ring-primary-500 border-gray-300 rounded"
            />
            <label for="auto_create_users" class="ml-2 text-sm text-gray-700">
              Automatically create users on first login
            </label>
          </div>

          <div>
            <label class="block text-sm font-medium text-gray-700 mb-1">
              Default Role
            </label>
            <select
              v-model="form.default_role"
              class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:ring-2 focus:ring-primary-500"
            >
              <option value="member">Member</option>
              <option value="admin">Admin</option>
              <option value="viewer">Viewer</option>
            </select>
          </div>

          <div>
            <label class="block text-sm font-medium text-gray-700 mb-1">
              Allowed Email Domains (comma-separated)
            </label>
            <input
              v-model="allowedDomainsInput"
              type="text"
              class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:ring-2 focus:ring-primary-500"
              placeholder="example.com, company.org"
            />
            <p class="mt-1 text-xs text-gray-500">
              Leave empty to allow all domains
            </p>
          </div>
        </div>

        <!-- Actions -->
        <div class="flex justify-end gap-3 pt-4 border-t border-gray-200">
          <BaseButton variant="secondary" @click="$emit('close')">
            Cancel
          </BaseButton>
          <BaseButton type="submit" variant="primary" :disabled="saving">
            {{ saving ? 'Saving...' : (config?.id ? 'Update' : 'Create') }}
          </BaseButton>
        </div>
      </form>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, watch } from 'vue';
import BaseButton from '@/components/BaseButton.vue';

const props = defineProps({
  config: {
    type: Object,
    default: null,
  },
});

const emit = defineEmits(['close', 'save']);

const saving = ref(false);

const form = ref({
  provider: 'okta',
  sso_type: 'oidc',
  name: '',
  domain_name: '',
  issuer_url: '',
  issuer_alias: '',
  client_id: '',
  client_secret: '',
  saml_entity_id: '',
  saml_sso_url: '',
  saml_certificate: '',
  saml_sign_requests: true,
  auto_create_users: true,
  default_role: 'member',
  allowed_email_domains: [],
  enabled: true,
});

const allowedDomainsInput = computed({
  get: () => form.value.allowed_email_domains?.join(', ') || '',
  set: (value) => {
    form.value.allowed_email_domains = value
      .split(',')
      .map(d => d.trim())
      .filter(d => d);
  },
});

// Initialize form with existing config
watch(() => props.config, (newConfig) => {
  if (newConfig) {
    form.value = {
      ...form.value,
      ...newConfig,
      client_secret: '', // Don't pre-fill secrets
    };
  }
}, { immediate: true });

const handleSubmit = async () => {
  saving.value = true;
  try {
    // Build the payload
    const payload = {
      provider: form.value.provider,
      sso_type: form.value.sso_type,
      name: form.value.name,
      domain_name: form.value.domain_name || null,
      auto_create_users: form.value.auto_create_users,
      default_role: form.value.default_role,
      allowed_email_domains: form.value.allowed_email_domains,
      enabled: form.value.enabled,
    };

    if (form.value.sso_type === 'oidc') {
      payload.issuer_url = form.value.issuer_url;
      payload.issuer_alias = form.value.issuer_alias || null;
      payload.client_id = form.value.client_id;
      if (form.value.client_secret) {
        payload.client_secret = form.value.client_secret;
      }
    } else {
      payload.saml_entity_id = form.value.saml_entity_id;
      payload.saml_sso_url = form.value.saml_sso_url;
      if (form.value.saml_certificate) {
        payload.saml_certificate = form.value.saml_certificate;
      }
      payload.saml_sign_requests = form.value.saml_sign_requests;
    }

    emit('save', payload);
  } finally {
    saving.value = false;
  }
};
</script>
