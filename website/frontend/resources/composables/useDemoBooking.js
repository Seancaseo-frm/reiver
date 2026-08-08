/** Build-time: set VITE_CALENDLY_URL to your full Calendly event URL when ready. */
const FALLBACK_MAIL =
  'mailto:hello@reiver.ai?subject=Reiver%20demo%20request&body=Hi%2C%20I%27d%20like%20to%20schedule%20a%20demo.';

export function getCalendlyUrl() {
  const raw = import.meta.env.VITE_CALENDLY_URL;
  return typeof raw === 'string' ? raw.trim() : '';
}

export function getDemoHref() {
  const cal = getCalendlyUrl();
  return cal || FALLBACK_MAIL;
}

/** Attributes for <a> demo CTAs: external Calendly opens in new tab; mailto stays default. */
export function getDemoLinkAttrs() {
  const cal = getCalendlyUrl();
  if (cal) {
    return { href: cal, target: '_blank', rel: 'noopener noreferrer' };
  }
  return { href: FALLBACK_MAIL };
}
