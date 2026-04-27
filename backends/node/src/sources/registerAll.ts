/**
 * Single registration entrypoint for every W3 source. Called once from
 * src/index.ts at startup; the poller then walks the registry and
 * schedules each source on its own setInterval.
 */
import registerRfs from './rfs.js';
import registerBom from './bom.js';
import registerTraffic from './traffic.js';
import registerBeach from './beach.js';
import registerWeather from './weather.js';
import registerPager from './pager.js';
import registerAviation from './aviation.js';

export function registerAllSources(): void {
  registerRfs();
  registerBom();
  registerTraffic();
  registerBeach();
  registerWeather();
  registerPager();
  registerAviation();
}
