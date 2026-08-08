<template>
  <div class="fixed inset-0 z-50 overflow-y-auto" @click.self="handleClose">
    <div class="flex items-center justify-center min-h-screen px-4 pt-4 pb-20 text-center sm:block sm:p-0">
      <!-- Background overlay -->
      <div class="fixed inset-0 transition-opacity bg-gray-500 bg-opacity-75" @click="handleClose"></div>

      <!-- Modal panel -->
      <div class="inline-block align-bottom bg-white rounded-lg text-left overflow-hidden shadow-xl transform transition-all sm:my-8 sm:align-middle sm:max-w-2xl sm:w-full">
        <div class="bg-white px-4 pt-5 pb-4 sm:p-6 sm:pb-4">
          <!-- Header -->
          <div class="flex items-center justify-between mb-4">
            <h3 class="text-lg font-medium text-gray-900">
              {{ integration.id ? 'Configure Integration' : 'Add Integration' }}
            </h3>
            <button
              @click="handleClose"
              class="text-gray-400 hover:text-gray-600"
            >
              <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>

          <!-- Form -->
          <form @submit.prevent="handleSave" class="space-y-4">
            <!-- Integration Name -->
            <div>
              <label class="block text-sm font-medium text-gray-700 mb-1">
                Name
              </label>
              <input
                v-model="formData.name"
                type="text"
                required
                class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-primary-500"
                :placeholder="getNamePlaceholder()"
              />
            </div>

            <!-- Integration Type (readonly) -->
            <div>
              <label class="block text-sm font-medium text-gray-700 mb-1">
                Type
              </label>
              <input
                :value="getDisplayType()"
                type="text"
                disabled
                class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-gray-100 text-gray-600 cursor-not-allowed"
              />
            </div>

            <!-- ==================== Health Check Fields ==================== -->
            <template v-if="isHealthCheck">
              <!-- Target URL (HTTP/SSL) -->
              <div v-if="checkType === 'http' || checkType === 'ssl'">
                <label class="block text-sm font-medium text-gray-700 mb-1">
                  Target URL <span class="text-red-500">*</span>
                </label>
                <input
                  v-model="formData.target_url"
                  type="url"
                  required
                  class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-primary-500"
                  placeholder="https://api.example.com/health"
                />
              </div>

              <!-- Target Host (TCP) -->
              <div v-if="checkType === 'tcp'" class="grid grid-cols-2 gap-4">
                <div>
                  <label class="block text-sm font-medium text-gray-700 mb-1">
                    Host <span class="text-red-500">*</span>
                  </label>
                  <input
                    v-model="formData.target_host"
                    type="text"
                    required
                    class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-primary-500"
                    placeholder="db.example.com"
                  />
                </div>
                <div>
                  <label class="block text-sm font-medium text-gray-700 mb-1">
                    Port <span class="text-red-500">*</span>
                  </label>
                  <input
                    v-model.number="formData.target_port"
                    type="number"
                    required
                    min="1"
                    max="65535"
                    class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-primary-500"
                    placeholder="5432"
                  />
                </div>
              </div>

              <!-- HTTP-specific options -->
              <div v-if="checkType === 'http'" class="space-y-4 p-4 bg-blue-50 rounded-lg border border-blue-200">
                <h4 class="text-sm font-medium text-gray-700">HTTP Options</h4>
                
                <div class="grid grid-cols-2 gap-4">
                  <div>
                    <label class="block text-sm font-medium text-gray-700 mb-1">
                      Method
                    </label>
                    <select
                      v-model="formData.http_method"
                      class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-primary-500"
                    >
                      <option value="GET">GET</option>
                      <option value="POST">POST</option>
                      <option value="PUT">PUT</option>
                      <option value="HEAD">HEAD</option>
                    </select>
                  </div>
                  <div class="flex items-center pt-6">
                    <input
                      v-model="formData.http_follow_redirects"
                      type="checkbox"
                      id="follow_redirects"
                      class="mr-2 w-4 h-4 text-primary-600 border-gray-300 rounded focus:ring-primary-500"
                    />
                    <label for="follow_redirects" class="text-sm text-gray-700">
                      Follow redirects
                    </label>
                  </div>
                </div>
              </div>

              <!-- SSL-specific options -->
              <div v-if="checkType === 'ssl'" class="space-y-4 p-4 bg-green-50 rounded-lg border border-green-200">
                <h4 class="text-sm font-medium text-gray-700">SSL Certificate Options</h4>
                
                <div>
                  <label class="block text-sm font-medium text-gray-700 mb-1">
                    Warn before expiry (days)
                  </label>
                  <input
                    v-model.number="formData.ssl_expiry_warning_days"
                    type="number"
                    min="1"
                    max="365"
                    class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-primary-500"
                    placeholder="30"
                  />
                </div>

                <div class="flex items-center gap-4">
                  <label class="flex items-center">
                    <input
                      v-model="formData.ssl_check_expiry"
                      type="checkbox"
                      class="mr-2 w-4 h-4 text-primary-600 border-gray-300 rounded focus:ring-primary-500"
                    />
                    <span class="text-sm text-gray-700">Check expiry</span>
                  </label>
                  <label class="flex items-center">
                    <input
                      v-model="formData.ssl_check_chain"
                      type="checkbox"
                      class="mr-2 w-4 h-4 text-primary-600 border-gray-300 rounded focus:ring-primary-500"
                    />
                    <span class="text-sm text-gray-700">Validate chain</span>
                  </label>
                </div>
              </div>

              <!-- Test Frequency -->
              <div class="grid grid-cols-2 gap-4">
                <div>
                  <label class="block text-sm font-medium text-gray-700 mb-1">
                    Test Frequency
                  </label>
                  <select
                    v-model.number="formData.check_interval_seconds"
                    class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-primary-500"
                  >
                    <option :value="30">Every 30 seconds</option>
                    <option :value="60">Every 1 minute</option>
                    <option :value="300">Every 5 minutes</option>
                    <option :value="600">Every 10 minutes</option>
                    <option :value="900">Every 15 minutes</option>
                    <option :value="1800">Every 30 minutes</option>
                    <option :value="3600">Every 1 hour</option>
                  </select>
                </div>
                <div>
                  <label class="block text-sm font-medium text-gray-700 mb-1">
                    Timeout (seconds)
                  </label>
                  <input
                    v-model.number="formData.timeout_seconds"
                    type="number"
                    min="1"
                    max="120"
                    class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-primary-500"
                    placeholder="30"
                  />
                </div>
              </div>

              <!-- Locations -->
              <div class="space-y-2">
                <label class="block text-sm font-medium text-gray-700">
                  Test Locations
                </label>
                <div class="grid grid-cols-2 md:grid-cols-4 gap-2">
                  <label v-for="loc in availableLocations" :key="loc.id" class="flex items-center p-2 border border-gray-200 rounded hover:bg-gray-50 cursor-pointer">
                    <input
                      type="checkbox"
                      :value="loc.id"
                      v-model="formData.locations"
                      class="mr-2 w-4 h-4 text-primary-600 border-gray-300 rounded focus:ring-primary-500"
                    />
                    <span class="text-sm text-gray-700">{{ loc.name }}</span>
                  </label>
                </div>
                <p class="text-xs text-gray-500">Select regions to run checks from</p>
              </div>

              <!-- Assertions -->
              <div class="space-y-4 p-4 bg-yellow-50 rounded-lg border border-yellow-200">
                <h4 class="text-sm font-medium text-gray-700">Assertions</h4>
                
                <div class="grid grid-cols-2 gap-4">
                  <div v-if="checkType === 'http'">
                    <label class="block text-sm font-medium text-gray-700 mb-1">
                      Expected Status Code
                    </label>
                    <input
                      v-model="expectedStatusText"
                      type="text"
                      class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-primary-500"
                      placeholder="200, 201"
                    />
                  </div>
                  <div>
                    <label class="block text-sm font-medium text-gray-700 mb-1">
                      Response Time (max ms)
                    </label>
                    <input
                      v-model.number="formData.response_time_threshold_ms"
                      type="number"
                      min="0"
                      class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-primary-500"
                      placeholder="500"
                    />
                    <p class="text-xs text-gray-500 mt-1">Fail if response takes longer</p>
                  </div>
                </div>

                <div v-if="checkType === 'http'">
                  <label class="block text-sm font-medium text-gray-700 mb-1">
                    Response Body Contains
                  </label>
                  <input
                    v-model="formData.http_expected_body"
                    type="text"
                    class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-primary-500"
                    placeholder='{"status":"ok"}'
                  />
                </div>
              </div>

              <!-- Alert Conditions -->
              <div class="space-y-4 p-4 bg-red-50 rounded-lg border border-red-200">
                <h4 class="text-sm font-medium text-gray-700">Alert Conditions</h4>
                
                <div class="grid grid-cols-2 gap-4">
                  <div>
                    <label class="block text-sm font-medium text-gray-700 mb-1">
                      Min Failing Locations
                    </label>
                    <select
                      v-model.number="formData.min_failing_locations"
                      class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-primary-500"
                    >
                      <option :value="1">1 location</option>
                      <option :value="2">2+ locations</option>
                      <option :value="3">3+ locations</option>
                      <option :value="4">4+ locations</option>
                    </select>
                    <p class="text-xs text-gray-500 mt-1">Alert only if this many locations fail</p>
                  </div>
                  <div>
                    <label class="block text-sm font-medium text-gray-700 mb-1">
                      Alert After (minutes)
                    </label>
                    <select
                      v-model.number="formData.alert_after_minutes"
                      class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-primary-500"
                    >
                      <option :value="0">Immediately</option>
                      <option :value="1">1 minute</option>
                      <option :value="2">2 minutes</option>
                      <option :value="5">5 minutes</option>
                      <option :value="10">10 minutes</option>
                      <option :value="15">15 minutes</option>
                    </select>
                    <p class="text-xs text-gray-500 mt-1">Wait before alerting</p>
                  </div>
                </div>
              </div>
            </template>

            <!-- ==================== AWS Fields ==================== -->
            <template v-else-if="isAwsIntegration">
              <!-- Region -->
              <div>
                <label class="block text-sm font-medium text-gray-700 mb-1">
                  AWS Region
                </label>
                <select
                  v-model="formData.region"
                  required
                  class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-primary-500"
                >
                  <option value="us-east-1">US East (N. Virginia)</option>
                  <option value="us-east-2">US East (Ohio)</option>
                  <option value="us-west-1">US West (N. California)</option>
                  <option value="us-west-2">US West (Oregon)</option>
                  <option value="eu-west-1">Europe (Ireland)</option>
                  <option value="eu-central-1">Europe (Frankfurt)</option>
                  <option value="ap-southeast-1">Asia Pacific (Singapore)</option>
                  <option value="ap-southeast-2">Asia Pacific (Sydney)</option>
                  <option value="ap-northeast-1">Asia Pacific (Tokyo)</option>
                </select>
              </div>

              <!-- Authentication Method -->
              <div>
                <label class="block text-sm font-medium text-gray-700 mb-1">
                  Authentication Method
                </label>
                <div class="flex gap-4">
                  <label class="flex items-center">
                    <input
                      v-model="authMethod"
                      type="radio"
                      value="iam_role"
                      class="mr-2"
                    />
                    <span class="text-sm text-gray-700">IAM Role (Recommended)</span>
                  </label>
                  <label class="flex items-center">
                    <input
                      v-model="authMethod"
                      type="radio"
                      value="access_keys"
                      class="mr-2"
                    />
                    <span class="text-sm text-gray-700">Access Keys</span>
                  </label>
                </div>
              </div>

              <!-- IAM Role Fields -->
              <div v-if="authMethod === 'iam_role'" class="space-y-4 p-4 bg-blue-50 rounded-lg border border-blue-200">
                <div>
                  <label class="block text-sm font-medium text-gray-700 mb-1">
                    IAM Role ARN <span class="text-red-500">*</span>
                  </label>
                  <input
                    v-model="formData.role_arn"
                    type="text"
                    required
                    class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-primary-500"
                    placeholder="arn:aws:iam::123456789012:role/ReiverIntegrationRole"
                  />
                </div>
                <div>
                  <label class="block text-sm font-medium text-gray-700 mb-1">
                    External ID
                  </label>
                  <div class="flex gap-2">
                    <input
                      v-model="formData.external_id"
                      type="text"
                      class="flex-1 px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-primary-500"
                      placeholder="reiver-abc123-def456-ghi789"
                    />
                    <button
                      type="button"
                      @click="generateExternalId"
                      class="px-4 py-2 bg-gray-100 text-gray-700 rounded-lg hover:bg-gray-200 transition-colors text-sm font-medium"
                    >
                      Generate
                    </button>
                  </div>
                </div>
              </div>

              <!-- Access Keys Fields -->
              <div v-if="authMethod === 'access_keys'" class="space-y-4 p-4 bg-yellow-50 rounded-lg border border-yellow-200">
                <p class="text-sm text-yellow-800">
                  ⚠️ Using access keys is less secure than IAM roles.
                </p>
                <div>
                  <label class="block text-sm font-medium text-gray-700 mb-1">
                    Access Key ID
                  </label>
                  <input
                    v-model="formData.access_key_id"
                    type="text"
                    class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-primary-500"
                    placeholder="AKIAIOSFODNN7EXAMPLE"
                  />
                </div>
                <div>
                  <label class="block text-sm font-medium text-gray-700 mb-1">
                    Secret Access Key
                  </label>
                  <input
                    v-model="formData.secret_access_key"
                    type="password"
                    class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-primary-500"
                    placeholder="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
                  />
                </div>
              </div>
            </template>

            <!-- ==================== Slack OAuth (read-only info) ==================== -->
            <template v-else-if="currentType === 'slack'">
              <div class="space-y-4 p-4 bg-purple-50 rounded-lg border border-purple-200">
                <h4 class="text-sm font-medium text-gray-700">Slack Workspace (OAuth)</h4>
                <div v-if="integration.team_name" class="text-sm text-gray-600">
                  <p><span class="font-medium">Workspace:</span> {{ integration.team_name }}</p>
                  <p v-if="integration.channel"><span class="font-medium">Channel:</span> {{ integration.channel }}</p>
                </div>
                <p class="text-xs text-gray-500">
                  This integration was installed via Slack's "Add to Slack" OAuth flow. To change the channel, reinstall the app from Slack.
                </p>
              </div>
            </template>

            <!-- ==================== Alerting Fields (PagerDuty, Teams, Discord, etc.) ==================== -->
            <template v-else-if="isAlertingIntegration">
              <!-- Webhook URL -->
              <div v-if="currentType === 'teams' || currentType === 'discord'">
                <label class="block text-sm font-medium text-gray-700 mb-1">
                  Webhook URL <span class="text-red-500">*</span>
                </label>
                <input
                  v-model="formData.webhook_url"
                  type="url"
                  required
                  class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-primary-500"
                  :placeholder="getWebhookPlaceholder()"
                />
                <p class="mt-1 text-xs text-gray-500">
                  <span v-if="currentType === 'discord'">
                    To get your Discord webhook URL: Go to your Discord server → Server Settings → Integrations → Webhooks → New Webhook (or use an existing one) → Copy Webhook URL
                  </span>
                  <span v-else-if="currentType === 'teams'">
                    To get your Teams webhook URL: Go to your Teams channel → ... (three dots) → Connectors → Incoming Webhook → Configure → Copy Webhook URL
                  </span>
                </p>
              </div>

              <!-- PagerDuty Routing Key -->
              <div v-if="currentType === 'pagerduty'">
                <label class="block text-sm font-medium text-gray-700 mb-1">
                  Routing Key <span class="text-red-500">*</span>
                </label>
                <input
                  v-model="formData.routing_key"
                  type="text"
                  required
                  class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-primary-500"
                  placeholder="32-character routing key from PagerDuty"
                />
              </div>

              <!-- ServiceNow -->
              <div v-if="currentType === 'servicenow'" class="space-y-4">
                <div>
                  <label class="block text-sm font-medium text-gray-700 mb-1">
                    Instance URL <span class="text-red-500">*</span>
                  </label>
                  <input
                    v-model="formData.instance_url"
                    type="url"
                    required
                    class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-primary-500"
                    placeholder="https://yourinstance.service-now.com"
                  />
                </div>
                <div class="grid grid-cols-2 gap-4">
                  <div>
                    <label class="block text-sm font-medium text-gray-700 mb-1">
                      Username <span class="text-red-500">*</span>
                    </label>
                    <input
                      v-model="formData.username"
                      type="text"
                      required
                      class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-primary-500"
                    />
                  </div>
                  <div>
                    <label class="block text-sm font-medium text-gray-700 mb-1">
                      Password <span class="text-red-500">*</span>
                    </label>
                    <input
                      v-model="formData.password"
                      type="password"
                      :required="!integration.id"
                      class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-primary-500"
                    />
                  </div>
                </div>
              </div>
            </template>

            <!-- ==================== Auth Events Fields ==================== -->
            <template v-else-if="isAuthEventsIntegration">
              <div class="space-y-4 p-4 bg-purple-50 rounded-lg border border-purple-200">
                <h4 class="text-sm font-medium text-gray-700">IdP Configuration</h4>
                
                <!-- Domain -->
                <div>
                  <label class="block text-sm font-medium text-gray-700 mb-1">
                    Domain / URL <span class="text-red-500">*</span>
                  </label>
                  <input
                    v-model="formData.domain"
                    type="text"
                    required
                    class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-primary-500"
                    :placeholder="getAuthDomainPlaceholder()"
                  />
                </div>

                <!-- OAuth Credentials -->
                <div class="grid grid-cols-2 gap-4">
                  <div>
                    <label class="block text-sm font-medium text-gray-700 mb-1">
                      Client ID <span class="text-red-500">*</span>
                    </label>
                    <input
                      v-model="formData.client_id"
                      type="text"
                      required
                      class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-primary-500"
                    />
                  </div>
                  <div>
                    <label class="block text-sm font-medium text-gray-700 mb-1">
                      Client Secret <span class="text-red-500">*</span>
                    </label>
                    <input
                      v-model="formData.client_secret"
                      type="password"
                      :required="!integration.id"
                      class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-primary-500"
                    />
                  </div>
                </div>

                <!-- Poll Interval -->
                <div>
                  <label class="block text-sm font-medium text-gray-700 mb-1">
                    Poll Interval
                  </label>
                  <select
                    v-model.number="formData.poll_interval_seconds"
                    class="w-full px-3 py-2 border border-gray-300 rounded-lg bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-primary-500"
                  >
                    <option :value="60">Every 1 minute</option>
                    <option :value="300">Every 5 minutes</option>
                    <option :value="600">Every 10 minutes</option>
                    <option :value="900">Every 15 minutes</option>
                  </select>
                </div>
              </div>
            </template>

            <!-- ==================== Collector Integrations (Grafana Alloy, etc.) ==================== -->
            <template v-else-if="isCollectorIntegration">
              <div class="space-y-4">
                <p class="text-sm text-gray-600">
                  To have Grafana Alloy logs ingested in Reiver, paste this configuration
                  into your Alloy config file (typically <code class="bg-gray-100 px-1 rounded">/etc/alloy/config.alloy</code> on Linux
                  or <code class="bg-gray-100 px-1 rounded">$(brew --prefix)/etc/alloy/config.alloy</code> on macOS):
                </p>

                <div class="relative">
                  <pre class="bg-gray-50 text-gray-800 border border-gray-200 p-4 rounded-lg text-sm overflow-x-auto font-mono whitespace-pre">{{ collectorConfigSnippet }}</pre>
                  <button
                    type="button"
                    @click="copyCollectorConfig"
                    class="absolute top-2 right-2 px-2 py-1 text-xs bg-gray-100 text-gray-900 rounded hover:bg-gray-200 transition-colors"
                  >
                    {{ configCopied ? 'Copied!' : 'Copy' }}
                  </button>
                </div>

                <p class="text-xs text-gray-500">
                  After updating your config, restart Alloy to apply the changes.
                </p>
              </div>
            </template>

            <!-- Enabled Toggle -->
            <div v-if="!isCollectorIntegration" class="flex items-center">
              <input
                v-model="formData.enabled"
                type="checkbox"
                id="enabled"
                class="mr-2 w-4 h-4 text-primary-600 border-gray-300 rounded focus:ring-primary-500"
              />
              <label for="enabled" class="text-sm font-medium text-gray-700">
                Enable this integration
              </label>
            </div>

            <!-- Action Buttons -->
            <div class="flex justify-end gap-3 pt-4 border-t border-gray-200">
              <button
                type="button"
                @click="handleClose"
                class="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors"
              >
                {{ isCollectorIntegration ? 'Close' : 'Cancel' }}
              </button>
              <button
                v-if="!isCollectorIntegration"
                type="submit"
                class="px-4 py-2 text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 rounded-lg transition-colors"
              >
                {{ integration.id ? 'Save Changes' : 'Add Integration' }}
              </button>
            </div>
          </form>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, watch, computed } from 'vue';

const props = defineProps({
  integration: {
    type: Object,
    required: true,
  },
  integrationType: {
    type: String,
    default: null,
  },
  projectApiKey: {
    type: String,
    default: '',
  },
});

const emit = defineEmits(['close', 'save']);

const authMethod = ref('iam_role');
const expectedStatusText = ref('200');
const configCopied = ref(false);

// Available check locations
const availableLocations = [
  { id: 'us-east', name: 'US East' },
  { id: 'us-west', name: 'US West' },
  { id: 'eu-west', name: 'EU West' },
  { id: 'eu-central', name: 'EU Central' },
  { id: 'ap-south', name: 'Asia South' },
  { id: 'ap-northeast', name: 'Asia Northeast' },
  { id: 'ap-southeast', name: 'Asia Southeast' },
  { id: 'sa-east', name: 'South America' },
];

const formData = ref({
  name: '',
  enabled: true,
  // AWS
  integration_type: '',
  region: 'us-east-1',
  role_arn: null,
  external_id: null,
  access_key_id: null,
  secret_access_key: null,
  // Health Checks
  check_type: 'http',
  target_url: '',
  target_host: '',
  target_port: null,
  http_method: 'GET',
  http_expected_status: [200],
  http_expected_body: '',
  http_follow_redirects: true,
  ssl_expiry_warning_days: 30,
  ssl_check_expiry: true,
  ssl_check_chain: true,
  check_interval_seconds: 300,  // Default: every 5 minutes
  timeout_seconds: 30,
  // Locations
  locations: ['us-east'],
  // Assertions
  response_time_threshold_ms: null,
  // Alert conditions
  min_failing_locations: 1,
  alert_after_minutes: 0,
  failure_threshold: 3,
  success_threshold: 1,
  // Alerting
  webhook_url: '',
  routing_key: '',
  instance_url: '',
  username: '',
  password: '',
  // Auth Events
  domain: '',
  client_id: '',
  client_secret: '',
  poll_interval_seconds: 60,
});

const currentType = computed(() => {
  return props.integration.integration_type || props.integrationType || '';
});

const isHealthCheck = computed(() => currentType.value.startsWith('health_check_'));
const checkType = computed(() => currentType.value.replace('health_check_', ''));

const isAwsIntegration = computed(() => {
  return ['ec2', 'rds', 'lambda', 's3', 'ecs', 'eks', 'dynamodb', 'sqs', 'sns'].includes(currentType.value);
});

const isAlertingIntegration = computed(() => {
  return ['pagerduty', 'servicenow', 'teams', 'discord'].includes(currentType.value);
});

const isAuthEventsIntegration = computed(() => {
  return currentType.value.startsWith('auth_events_');
});

const isCollectorIntegration = computed(() => {
  return currentType.value.startsWith('collector_');
});

// Collector integration config snippet
const collectorConfigSnippet = computed(() => {
  const apiKey = props.projectApiKey || '<YOUR_PROJECT_API_KEY>';
  return `otelcol.exporter.otlphttp "reiver" {
  client {
    endpoint = "https://ingest.reiver.ai"
    headers = {
      "x-reiver-project-key" = "${apiKey}",
    }
  }
}`;
});

const copyCollectorConfig = async () => {
  try {
    await navigator.clipboard.writeText(collectorConfigSnippet.value);
    configCopied.value = true;
    setTimeout(() => {
      configCopied.value = false;
    }, 2000);
  } catch (err) {
    console.error('Failed to copy config:', err);
  }
};

// Initialize form data from integration prop
watch(() => props.integration, (newIntegration) => {
  if (newIntegration) {
    const type = newIntegration.integration_type || props.integrationType || '';
    
    formData.value = {
      name: newIntegration.name || '',
      enabled: newIntegration.enabled !== undefined ? newIntegration.enabled : true,
      // AWS
      integration_type: type,
      region: newIntegration.region || 'us-east-1',
      role_arn: newIntegration.role_arn || null,
      external_id: newIntegration.external_id || null,
      access_key_id: newIntegration.access_key_id || null,
      secret_access_key: null,
      // Health Checks
      check_type: type.replace('health_check_', '') || 'http',
      target_url: newIntegration.target_url || '',
      target_host: newIntegration.target_host || '',
      target_port: newIntegration.target_port || null,
      http_method: newIntegration.http_method || 'GET',
      http_expected_status: newIntegration.http_expected_status || [200],
      http_expected_body: newIntegration.http_expected_body || '',
      http_follow_redirects: newIntegration.http_follow_redirects !== false,
      ssl_expiry_warning_days: newIntegration.ssl_expiry_warning_days || 30,
      ssl_check_expiry: newIntegration.ssl_check_expiry !== false,
      ssl_check_chain: newIntegration.ssl_check_chain !== false,
      check_interval_seconds: newIntegration.check_interval_seconds || 300,
      timeout_seconds: newIntegration.timeout_seconds || 30,
      // Locations
      locations: newIntegration.locations || ['us-east'],
      // Assertions
      response_time_threshold_ms: newIntegration.response_time_threshold_ms || null,
      // Alert conditions
      min_failing_locations: newIntegration.min_failing_locations || 1,
      alert_after_minutes: newIntegration.alert_after_minutes || 0,
      failure_threshold: newIntegration.failure_threshold || 3,
      success_threshold: newIntegration.success_threshold || 1,
      // Alerting
      webhook_url: newIntegration.webhook_url || '',
      routing_key: newIntegration.routing_key || '',
      instance_url: newIntegration.instance_url || '',
      username: newIntegration.username || '',
      password: '',
      // Auth Events
      domain: newIntegration.domain || '',
      client_id: newIntegration.client_id || '',
      client_secret: '',
      poll_interval_seconds: newIntegration.poll_interval_seconds || 60,
    };
    
    expectedStatusText.value = (formData.value.http_expected_status || [200]).join(', ');
    
    if (newIntegration.role_arn) {
      authMethod.value = 'iam_role';
    } else if (newIntegration.access_key_id) {
      authMethod.value = 'access_keys';
    }
  }
}, { immediate: true });

// Parse expected status text into array
watch(expectedStatusText, (val) => {
  formData.value.http_expected_status = val
    .split(',')
    .map(s => parseInt(s.trim()))
    .filter(n => !isNaN(n));
});

const getDisplayType = () => {
  const typeMap = {
    'health_check_http': 'HTTP/HTTPS Health Check',
    'health_check_tcp': 'TCP Health Check',
    'health_check_ssl': 'SSL Certificate Monitor',
    'auth_events_okta': 'Okta Auth Events',
    'auth_events_auth0': 'Auth0 Auth Events',
    'auth_events_entra_id': 'Microsoft Entra ID Events',
    'auth_events_onelogin': 'OneLogin Auth Events',
    'auth_events_ping_identity': 'Ping Identity Auth Events',
    'auth_events_keycloak': 'Keycloak Auth Events',
  };
  return typeMap[currentType.value] || currentType.value.toUpperCase();
};

const getNamePlaceholder = () => {
  if (isHealthCheck.value) return 'Production API Health';
  if (isAlertingIntegration.value) return 'Production Alerts';
  if (isAuthEventsIntegration.value) return 'Okta Event Ingestion';
  return 'My AWS Integration';
};

const getWebhookPlaceholder = () => {
  const type = currentType.value;
  if (type === 'slack') return 'https://hooks.slack.com/services/T00/B00/xxx';
  if (type === 'teams') return 'https://outlook.office.com/webhook/xxx';
  if (type === 'discord') return 'https://discord.com/api/webhooks/xxx/xxx';
  return 'https://...';
};

const getAuthDomainPlaceholder = () => {
  const type = currentType.value;
  if (type === 'auth_events_okta') return 'your-domain.okta.com';
  if (type === 'auth_events_auth0') return 'your-tenant.auth0.com';
  if (type === 'auth_events_keycloak') return 'https://keycloak.example.com';
  return 'your-domain.example.com';
};

const generateExternalId = () => {
  const uuid = 'xxxx-xxxx-4xxx-yxxx-xxxx'.replace(/[xy]/g, (c) => {
    const r = Math.random() * 16 | 0;
    const v = c === 'x' ? r : (r & 0x3 | 0x8);
    return v.toString(16);
  });
  formData.value.external_id = `reiver-${uuid}`;
};

const handleClose = () => {
  emit('close');
};

const handleSave = () => {
  let payload = {
    name: formData.value.name,
    enabled: formData.value.enabled,
  };

  if (isHealthCheck.value) {
    payload = {
      ...payload,
      check_type: checkType.value,
      target_url: formData.value.target_url || null,
      target_host: formData.value.target_host || null,
      target_port: formData.value.target_port || null,
      http_method: formData.value.http_method,
      http_expected_status: formData.value.http_expected_status,
      http_expected_body: formData.value.http_expected_body || null,
      http_follow_redirects: formData.value.http_follow_redirects,
      ssl_expiry_warning_days: formData.value.ssl_expiry_warning_days,
      ssl_check_expiry: formData.value.ssl_check_expiry,
      ssl_check_chain: formData.value.ssl_check_chain,
      check_interval_seconds: formData.value.check_interval_seconds,
      timeout_seconds: formData.value.timeout_seconds,
      // Locations
      locations: formData.value.locations.length > 0 ? formData.value.locations : ['us-east'],
      // Assertions
      response_time_threshold_ms: formData.value.response_time_threshold_ms || null,
      // Alert conditions
      min_failing_locations: formData.value.min_failing_locations,
      alert_after_minutes: formData.value.alert_after_minutes,
      failure_threshold: formData.value.failure_threshold,
      success_threshold: formData.value.success_threshold,
    };
  } else if (isAwsIntegration.value) {
    payload.integration_type = formData.value.integration_type;
    payload.region = formData.value.region;
    if (authMethod.value === 'iam_role') {
      payload.role_arn = formData.value.role_arn;
      payload.external_id = formData.value.external_id;
    } else {
      payload.access_key_id = formData.value.access_key_id;
      payload.secret_access_key = formData.value.secret_access_key;
    }
  } else if (isAlertingIntegration.value) {
    const type = currentType.value;
    if (type === 'pagerduty') {
      payload.routing_key = formData.value.routing_key;
    } else if (type === 'servicenow') {
      payload.instance_url = formData.value.instance_url;
      payload.username = formData.value.username;
      if (formData.value.password) {
        payload.password = formData.value.password;
      }
    } else {
      payload.webhook_url = formData.value.webhook_url;
    }
  } else if (isAuthEventsIntegration.value) {
    const provider = currentType.value.replace('auth_events_', '');
    payload.provider = provider;
    payload.domain = formData.value.domain;
    payload.client_id = formData.value.client_id;
    if (formData.value.client_secret) {
      payload.client_secret = formData.value.client_secret;
    }
    payload.poll_interval_seconds = formData.value.poll_interval_seconds;
  }

  emit('save', payload);
};
</script>
