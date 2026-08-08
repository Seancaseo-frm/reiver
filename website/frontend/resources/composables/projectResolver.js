const slugToUuid = new Map();

export function registerProject(slug, uuid) {
  if (slug && uuid && slug !== uuid) {
    slugToUuid.set(slug, uuid);
  }
}

export function registerProjects(projects) {
  for (const p of projects) {
    if (p.slug && p.id) {
      slugToUuid.set(p.slug, p.id);
    }
  }
}

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function isUuid(value) {
  return UUID_RE.test(value);
}

export function isKnownSlug(value) {
  return slugToUuid.has(value);
}

/**
 * Resolve a single value: if it's a known slug, return the UUID; otherwise pass through.
 */
export function resolveSlug(value) {
  if (typeof value !== 'string') return value;
  return slugToUuid.get(value) || value;
}

/**
 * Rewrite known project slugs in a URL (both path segments and query-string values).
 */
export function resolveApiUrl(url) {
  if (!url || slugToUuid.size === 0) return url;
  for (const [slug, uuid] of slugToUuid) {
    if (!url.includes(slug)) continue;
    // Path segments: /slug/ and trailing /slug
    url = url.replaceAll(`/${slug}/`, `/${uuid}/`);
    if (url.endsWith(`/${slug}`)) {
      url = url.slice(0, url.length - slug.length) + uuid;
    }
    // Query-string values: =slug& and trailing =slug
    url = url.replaceAll(`=${slug}&`, `=${uuid}&`);
    if (url.endsWith(`=${slug}`)) {
      url = url.slice(0, url.length - slug.length) + uuid;
    }
  }
  return url;
}
