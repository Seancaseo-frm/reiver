import { ref } from 'vue';
import axios from 'axios';

const entitlements = ref(null);
const loading = ref(false);
let lastProjectId = null;

export function useEntitlements() {
  const fetchEntitlements = async (projectIdOrSlug) => {
    if (!projectIdOrSlug || projectIdOrSlug === lastProjectId) return;
    lastProjectId = projectIdOrSlug;
    loading.value = true;
    try {
      const { data } = await axios.get(`/api/projects/${projectIdOrSlug}/entitlements`);
      entitlements.value = data;
    } catch {
      entitlements.value = null;
    } finally {
      loading.value = false;
    }
  };

  const hasProduct = (product) => {
    const config = entitlements.value?.config;
    if (!config) return false;
    if (config[product]?.enabled !== undefined) {
      return config[product].enabled;
    }
    return false;
  };

  const hasFeature = (feature) => {
    const config = entitlements.value?.config;
    if (!config) return false;
    if (config.platform?.[feature] !== undefined) return config.platform[feature] === true;
    if (config.watch?.[feature] !== undefined) return config.watch[feature] === true;
    if (config.prompt_hub?.[feature] !== undefined) return config.prompt_hub[feature] === true;
    return false;
  };

  return {
    entitlements,
    loading,
    fetchEntitlements,
    hasProduct,
    hasFeature,
  };
}
