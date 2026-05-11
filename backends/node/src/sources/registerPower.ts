/**
 * Wire-up file for the three power sources.
 *
 * Each source module owns its own `register()` (registerSource calls into
 * the shared registry). This file is just a one-liner the entry point
 * imports so it doesn't need to know about every individual module.
 *
 * Idempotent — calling twice just re-registers the same names; the
 * registry is keyed by name so the second call overwrites with the same
 * definition.
 */
import registerEndeavour from './endeavour.js';
import registerAusgrid from './ausgrid.js';
import registerEssential from './essential.js';

export function registerAllPowerSources(): void {
  registerEndeavour();
  registerAusgrid();
  registerEssential();
}

export default registerAllPowerSources;
