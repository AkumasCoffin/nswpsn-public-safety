/**
 * Wire-up file for the power sources: NSW's three distributors, each with a
 * module of its own shape, and the other states' seven, which all answer in
 * the merged feed's shape (sources/outages/).
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
import registerAusnet from './outages/ausnet.js';
import registerCppc from './outages/cppc.js';
import registerJemena from './outages/jemena.js';
import registerSapn from './outages/sapn.js';
import registerWesternPower from './outages/westernpower.js';
import registerHorizon from './outages/horizon.js';

export function registerAllPowerSources(): void {
  registerEndeavour();
  registerAusgrid();
  registerEssential();
  registerAusnet(); // VIC
  registerCppc(); // VIC: CitiPower and Powercor, one file
  registerJemena(); // VIC
  registerSapn(); // SA
  registerWesternPower(); // WA
  registerHorizon(); // WA
}

export default registerAllPowerSources;
