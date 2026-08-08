/**
 * Public documentation origin. Set VITE_DOCS_BASE_URL per environment (no trailing slash).
 * Production builds load .env.production (committed); override in CI with shell env if needed.
 * Example: http://localhost:5174 for a local docs dev server.
 */
const DEFAULT_DOCS_BASE = 'https://docs.reiver.ai';

export function getDocsBaseUrl() {
  const raw = import.meta.env.VITE_DOCS_BASE_URL;
  if (typeof raw === 'string' && raw.trim()) {
    return raw.trim().replace(/\/+$/, '');
  }
  return DEFAULT_DOCS_BASE.replace(/\/+$/, '');
}

/**
 * @param {string} [path] - Path on the docs site, e.g. '/' or 'intro/quickstart'
 */
export function docsUrl(path = '/') {
  const base = getDocsBaseUrl();
  let p = (path || '/').trim();
  if (!p.startsWith('/')) p = `/${p}`;
  return `${base}${p}`;
}
