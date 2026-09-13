// The alert-type catalog — loaded from shared/alert-catalog.json at RUNTIME.
//
// Read from disk rather than imported/compiled so a `git pull` updates it
// whether or not anything rebuilt. That matters: the whitelist used to be a
// literal in this package, so a stale dist rejected keys the dashboard was
// already offering, and every preset save came back `bad_request`. A file the
// running process reads cannot drift from the file the bot reads.
//
// This module is also what the dashboard reads, via
// GET /api/dashboard/alert-catalog — so the UI renders exactly the set the
// server accepts, and the two cannot disagree at all.

import { readFileSync, existsSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

export interface AlertProvider {
  key: string;
  label: string;
  icon: string;
  color: string;
}

export interface AlertDiscriminator {
  field: string;
  equals?: string;
  notEquals?: string;
  in?: string[];
  caseInsensitive?: boolean;
}

export interface AlertType {
  key: string;
  provider: string;
  label: string;
  botLabel: string;
  agency: string;
  agencyLabel: string;
  archiveSource: string | null;
  endpoint: string | null;
  itemsPath: string | string[];
  discriminator?: AlertDiscriminator;
  extract?: string;
  generalPicker?: boolean;
  aliases?: string[];
  soon?: boolean;
  color: string;
  icon: string;
  severityScale: string | null;
  subtypeAware: boolean;
}

export interface AlertCatalog {
  version: number;
  severityScales: Record<string, string[]>;
  providers: AlertProvider[];
  types: AlertType[];
}

const CATALOG_RELATIVE = join('shared', 'alert-catalog.json');

/**
 * Walk up from this module looking for shared/alert-catalog.json.
 *
 * Walking rather than a fixed `../../..` because the module sits at a
 * different depth under src/ than under dist/, and the same code has to find
 * the file either way. ALERT_CATALOG_PATH overrides for tests.
 */
function locateCatalog(): string {
  const override = process.env.ALERT_CATALOG_PATH;
  if (override) return resolve(override);

  let dir = dirname(fileURLToPath(import.meta.url));
  for (let i = 0; i < 8; i += 1) {
    const candidate = join(dir, CATALOG_RELATIVE);
    if (existsSync(candidate)) return candidate;
    const parent = dirname(dir);
    if (parent === dir) break;
    dir = parent;
  }
  throw new Error(
    `alert-catalog.json not found (looked for ${CATALOG_RELATIVE} above ` +
      `${dirname(fileURLToPath(import.meta.url))}). Set ALERT_CATALOG_PATH to override.`,
  );
}

function validate(cat: AlertCatalog): void {
  const providers = new Set(cat.providers.map((p) => p.key));
  const seen = new Set<string>();

  for (const t of cat.types) {
    if (seen.has(t.key)) throw new Error(`duplicate alert type key: ${t.key}`);
    seen.add(t.key);
    if (!providers.has(t.provider)) {
      throw new Error(`alert type ${t.key} names unknown provider ${t.provider}`);
    }
    if (t.severityScale && !cat.severityScales[t.severityScale]) {
      throw new Error(`alert type ${t.key} names unknown severity scale ${t.severityScale}`);
    }
  }
  for (const t of cat.types) {
    for (const a of t.aliases ?? []) {
      if (seen.has(a)) throw new Error(`alias ${a} (on ${t.key}) collides with a real key`);
    }
  }
}

function load(): AlertCatalog {
  const path = locateCatalog();
  const cat = JSON.parse(readFileSync(path, 'utf8')) as AlertCatalog;
  validate(cat);
  return cat;
}

// Loaded once at startup. A malformed catalog is a hard failure rather than a
// silent fallback — serving a truncated whitelist is exactly the fault this
// file exists to prevent, so it must never be possible to do so quietly.
export const catalog: AlertCatalog = load();

export const ALERT_PROVIDERS: readonly AlertProvider[] = catalog.providers;
export const ALERT_TYPE_DEFS: readonly AlertType[] = catalog.types;

/** Canonical keys, in catalog order. */
export const ALERT_TYPES: readonly string[] = catalog.types.map((t) => t.key);

const BY_KEY = new Map(catalog.types.map((t) => [t.key, t]));

/** alias -> canonical key, for retired names that still sit in saved presets. */
const ALIASES = new Map<string, string>();
for (const t of catalog.types) {
  for (const a of t.aliases ?? []) ALIASES.set(a, t.key);
}

/**
 * Fold a possibly-retired key to its canonical form.
 * Returns null if the key is not known at all.
 */
export function canonicalAlertType(key: string): string | null {
  if (BY_KEY.has(key)) return key;
  return ALIASES.get(key) ?? null;
}

export function alertTypeDef(key: string): AlertType | undefined {
  const canon = canonicalAlertType(key);
  return canon ? BY_KEY.get(canon) : undefined;
}

/** Providers with their types nested — the shape the dashboard renders from. */
export function catalogForClient(): {
  version: number;
  providers: Array<AlertProvider & { types: AlertType[] }>;
} {
  return {
    version: catalog.version,
    providers: catalog.providers.map((p) => ({
      ...p,
      types: catalog.types.filter((t) => t.provider === p.key),
    })),
  };
}
