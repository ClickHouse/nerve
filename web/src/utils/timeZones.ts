/**
 * IANA time zone names, for the About-you step.
 *
 * `Intl.supportedValuesOf` exists in every browser this app supports but is
 * not in the ES2022 `lib`, so it is reached through a typed shim rather than
 * a lib bump. A runtime without it (an old jsdom, an older WebKit) gets a
 * short list of common zones, which is enough for a `<datalist>`.
 */
const intl = Intl as unknown as { supportedValuesOf?: (key: string) => string[] };

const FALLBACK_ZONES = [
  'UTC', 'Europe/London', 'Europe/Berlin', 'Europe/Paris', 'Europe/Madrid',
  'Europe/Rome', 'Europe/Amsterdam', 'Europe/Warsaw', 'Europe/Kyiv',
  'Europe/Istanbul', 'Europe/Moscow', 'Africa/Cairo', 'Africa/Johannesburg',
  'Asia/Dubai', 'Asia/Karachi', 'Asia/Kolkata', 'Asia/Bangkok', 'Asia/Singapore',
  'Asia/Shanghai', 'Asia/Tokyo', 'Asia/Seoul', 'Australia/Sydney',
  'Pacific/Auckland', 'America/Sao_Paulo', 'America/Argentina/Buenos_Aires',
  'America/New_York', 'America/Chicago', 'America/Denver', 'America/Los_Angeles',
  'America/Toronto', 'America/Mexico_City', 'Pacific/Honolulu',
];

let cached: string[] | null = null;

/** Every zone name the runtime knows, sorted; a common subset otherwise. */
export function supportedTimeZones(): string[] {
  if (cached) return cached;
  try {
    const zones = intl.supportedValuesOf?.('timeZone');
    cached = zones && zones.length > 0 ? [...zones].sort() : FALLBACK_ZONES;
  } catch {
    cached = FALLBACK_ZONES;
  }
  return cached;
}

/** The browser's own zone, or `''` when it cannot say. */
export function browserTimeZone(): string {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone || '';
  } catch {
    return '';
  }
}

/**
 * A readable name for the browser's language, for the response-language
 * prefill ("English", "Deutsch"). `''` when the runtime cannot say.
 */
export function browserLanguageName(): string {
  try {
    const tag = navigator.language;
    if (!tag) return '';
    const base = tag.split('-')[0];
    const names = new Intl.DisplayNames([tag], { type: 'language' });
    const name = names.of(base);
    return name ? name.charAt(0).toUpperCase() + name.slice(1) : '';
  } catch {
    return '';
  }
}
