<template>
  <div class="min-h-screen flex items-center justify-center bg-gray-50 py-12 px-4 sm:px-6 lg:px-8">
    <div class="max-w-md w-full space-y-8">
      <div>
        <h1 class="text-center text-4xl font-bold text-brand-600">Reiver</h1>
        <h2 class="mt-6 text-center text-3xl font-extrabold text-gray-900">
          Sign in to your account
        </h2>
        <p class="mt-2 text-center text-sm text-gray-600">
          Choose a provider to continue
        </p>
      </div>

      <!-- Invite Required State -->
      <div v-if="inviteRequired" class="bg-blue-50 border border-blue-200 rounded-lg p-6 text-center">
        <svg class="mx-auto h-12 w-12 text-blue-400 mb-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 8l7.89 5.26a2 2 0 002.22 0L21 8M5 19h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
        </svg>
        <h3 class="text-lg font-semibold text-blue-800 mb-1">Invitation Required</h3>
        <p class="text-sm text-blue-700 mb-3">
          Your organization (<strong>{{ inviteDomain }}</strong>) requires an invitation to join.
        </p>
        <p class="text-xs text-blue-600 mb-4">Contact your organization admin to get an invite.</p>
        <button
          type="button"
          @click="signInDifferentAccount"
          class="text-sm font-medium text-indigo-600 hover:text-indigo-500 underline"
        >
          Sign in with a different account
        </button>
      </div>

      <!-- Pending Approval State -->
      <div v-if="pendingApproval" class="bg-yellow-50 border border-yellow-200 rounded-lg p-6 text-center">
        <svg class="mx-auto h-12 w-12 text-yellow-400 mb-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
        </svg>
        <h3 class="text-lg font-semibold text-yellow-800 mb-1">Registration Under Review</h3>
        <p class="text-sm text-yellow-700 mb-3">
          Your registration (<strong>{{ pendingEmail }}</strong>) has been received and will be reviewed by an administrator.
        </p>
        <p class="text-xs text-yellow-600 mb-4">You'll receive access once your account is approved. You can close this page.</p>
        <button
          type="button"
          @click="signInDifferentAccount"
          class="text-sm font-medium text-indigo-600 hover:text-indigo-500 underline"
        >
          Sign in with a different account
        </button>
      </div>

      <div v-if="error" class="bg-red-50 border border-red-200 rounded-lg p-4">
        <p class="text-sm text-red-700">{{ error }}</p>
      </div>

      <!-- Social OAuth Providers -->
      <div v-if="!pendingApproval && !inviteRequired && oauthProviders.length > 0" class="space-y-3">
        <div class="grid grid-cols-1 gap-3">
          <button
            v-for="p in oauthProviders"
            :key="p.id"
            type="button"
            @click="startOAuth(p.id)"
            class="w-full flex items-center justify-center py-3 px-4 border border-gray-300 rounded-lg shadow-sm text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 transition-colors"
          >
            <component :is="oauthIcons[p.id]" class="w-5 h-5 mr-3" />
            Continue with {{ p.name }}
          </button>
        </div>
      </div>

      <div v-else-if="!loading && !pendingApproval && !inviteRequired" class="text-center py-8">
        <p class="text-gray-500">No login providers are currently configured.</p>
        <p class="text-sm text-gray-400 mt-1">Please contact your administrator.</p>
      </div>

      <!-- Enterprise SSO Providers List -->
      <div v-if="!pendingApproval && !inviteRequired && ssoProviders.length > 0" class="space-y-3">
        <div class="relative">
          <div class="absolute inset-0 flex items-center">
            <div class="w-full border-t border-gray-300"></div>
          </div>
          <div class="relative flex justify-center text-sm">
            <span class="px-2 bg-gray-50 text-gray-500">Enterprise SSO</span>
          </div>
        </div>

        <div class="grid grid-cols-1 gap-2">
          <button
            v-for="provider in ssoProviders"
            :key="provider.id"
            type="button"
            @click="initiateSSO(provider)"
            :disabled="processing"
            class="w-full flex items-center justify-center py-3 px-4 border border-gray-300 rounded-lg shadow-sm text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 disabled:opacity-50 transition-colors"
          >
            <span class="w-5 h-5 mr-3 flex items-center justify-center text-xs font-bold text-indigo-600 bg-indigo-50 rounded">
              {{ getProviderInitials(provider.provider) }}
            </span>
            {{ provider.name }}
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, h } from 'vue';
import { useRouter, useRoute } from 'vue-router';
import axios from 'axios';
import { getProjectRedirectPath } from '@/composables/useProjectRedirect';

const GoogleIcon = { render: () => h('svg', { viewBox: '0 0 24 24', fill: 'currentColor' }, [h('path', { d: 'M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92a5.06 5.06 0 0 1-2.2 3.32v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.1z', fill: '#4285F4' }), h('path', { d: 'M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z', fill: '#34A853' }), h('path', { d: 'M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z', fill: '#FBBC05' }), h('path', { d: 'M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z', fill: '#EA4335' })]) };
const GitHubIcon = { render: () => h('svg', { viewBox: '0 0 24 24', fill: 'currentColor' }, [h('path', { d: 'M12 2C6.477 2 2 6.477 2 12c0 4.42 2.865 8.17 6.839 9.49.5.092.682-.217.682-.482 0-.237-.008-.866-.013-1.7-2.782.604-3.369-1.34-3.369-1.34-.454-1.156-1.11-1.464-1.11-1.464-.908-.62.069-.608.069-.608 1.003.07 1.531 1.03 1.531 1.03.892 1.529 2.341 1.087 2.91.831.092-.646.35-1.086.636-1.336-2.22-.253-4.555-1.11-4.555-4.943 0-1.091.39-1.984 1.029-2.683-.103-.253-.446-1.27.098-2.647 0 0 .84-.269 2.75 1.025A9.578 9.578 0 0 1 12 6.836c.85.004 1.705.115 2.504.337 1.909-1.294 2.747-1.025 2.747-1.025.546 1.377.203 2.394.1 2.647.64.699 1.028 1.592 1.028 2.683 0 3.842-2.339 4.687-4.566 4.935.359.309.678.919.678 1.852 0 1.336-.012 2.415-.012 2.743 0 .267.18.578.688.48C19.138 20.167 22 16.418 22 12c0-5.523-4.477-10-10-10z' })]) };
const MicrosoftIcon = { render: () => h('svg', { viewBox: '0 0 24 24', fill: 'currentColor' }, [h('rect', { x: '1', y: '1', width: '10', height: '10', fill: '#F25022' }), h('rect', { x: '13', y: '1', width: '10', height: '10', fill: '#7FBA00' }), h('rect', { x: '1', y: '13', width: '10', height: '10', fill: '#00A4EF' }), h('rect', { x: '13', y: '13', width: '10', height: '10', fill: '#FFB900' })]) };

const oauthIcons = { google: GoogleIcon, github: GitHubIcon, microsoft: MicrosoftIcon };

const router = useRouter();
const route = useRoute();
const error = ref('');
const processing = ref(false);
const loading = ref(true);
const pendingApproval = ref(false);
const pendingEmail = ref('');
const inviteRequired = ref(false);
const inviteDomain = ref('');
const inviteToken = ref('');

const oauthProviders = ref([]);
const ssoProviders = ref([]);

const providerLabels = {
  okta: 'Okta',
  auth0: 'Auth0',
  entra_id: 'Microsoft Entra ID',
  onelogin: 'OneLogin',
  ping: 'Ping Identity',
  keycloak: 'Keycloak',
  google: 'Google',
  custom: 'SSO',
};

const getProviderInitials = (provider) => {
  const label = providerLabels[provider] || provider;
  return label.split(' ').map(w => w[0]).join('').slice(0, 2).toUpperCase();
};

const initiateSSO = async (provider) => {
  processing.value = true;
  error.value = '';

  try {
    if (!provider) {
      error.value = 'No SSO configuration found';
      return;
    }

    const loginUrl = provider.sso_type === 'saml'
      ? `/api/sso/login/saml/${provider.id}`
      : `/api/sso/login/oidc/${provider.id}`;

    const response = await axios.get(loginUrl);
    if (response.data?.authorization_url) {
      window.location.href = response.data.authorization_url;
    } else {
      error.value = 'Failed to get SSO redirect URL';
    }
  } catch (err) {
    console.error('SSO initiation failed:', err);
    error.value = err.response?.data?.message || 'Failed to initiate SSO login';
  } finally {
    processing.value = false;
  }
};

const fetchSsoProviders = async () => {
  try {
    const response = await axios.get('/api/sso/configurations');
    ssoProviders.value = (response.data || []).filter(c => c.enabled);
  } catch {
    // SSO not available
  }
};

const fetchOAuthProviders = async () => {
  try {
    const response = await axios.get('/api/auth/oauth/providers');
    oauthProviders.value = (response.data || []).filter(p => p.enabled);
  } catch {
    // OAuth not available
  } finally {
    loading.value = false;
  }
};

const startOAuth = (providerId) => {
  const params = new URLSearchParams();
  if (inviteToken.value) {
    params.set('invite_token', inviteToken.value);
  }
  const redirect = route.query.redirect;
  if (redirect && redirect.startsWith('/')) {
    params.set('redirect', redirect);
  }
  const qs = params.toString();
  window.location.href = `/api/auth/oauth/${providerId}${qs ? '?' + qs : ''}`;
};

const signInDifferentAccount = () => {
  document.cookie = 'token=; path=/; max-age=0';
  pendingApproval.value = false;
  pendingEmail.value = '';
  inviteRequired.value = false;
  inviteDomain.value = '';
  inviteToken.value = '';
  error.value = '';
};

const getPostLoginPath = async () => {
  const redirect = route.query.redirect;
  if (redirect && redirect.startsWith('/')) return redirect;
  return await getProjectRedirectPath();
};

const handleAuthCallback = async () => {
  try {
    const res = await axios.get('/api/auth/me');
    const user = res.data;
    if (user && !user.is_approved && !user.is_platform_admin) {
      pendingApproval.value = true;
      pendingEmail.value = user.email || '';
      return;
    }
  } catch {
    error.value = 'Authentication failed. Please try again.';
    return;
  }

  const path = await getPostLoginPath();
  router.push(path);
};

const checkExistingSession = async () => {
  const hasToken = document.cookie.split('; ').some(c => c.trim().startsWith('token='));
  if (!hasToken && route.query.pending !== '1') return;

  try {
    const res = await axios.get('/api/auth/me');
    const user = res.data;
    if (user && !user.is_approved && !user.is_platform_admin) {
      pendingApproval.value = true;
      pendingEmail.value = user.email || '';
    } else if (user?.is_approved || user?.is_platform_admin) {
      const path = await getPostLoginPath();
      router.push(path);
    }
  } catch {
    // Not logged in or token expired, show login page
  }
};

onMounted(async () => {
  fetchOAuthProviders();
  fetchSsoProviders();

  if (route.query.error) {
    error.value = route.query.error;
  }

  if (route.query.invite_required === '1') {
    inviteRequired.value = true;
    inviteDomain.value = route.query.domain || '';
    return;
  }

  if (route.query.invite_token) {
    inviteToken.value = route.query.invite_token;
  }

  if (route.query.auth === '1') {
    await handleAuthCallback();
  } else {
    await checkExistingSession();
  }
});
</script>
