module.exports = {
  apps: [
    // Node/TypeScript backend — fully replaces the previous python
    // service (external_api_proxy.py). Cloudflare Tunnel routes
    // api.forcequit.xyz → this process on port 3000.
    //
    // Build before starting: `cd node && npm run build`
    // Start: `pm2 start ecosystem.config.js`
    {
      name: 'nswpsn-api-node',
      // Run via `npm start` so the prestart hook (npm run build) fires
      // automatically on every PM2 restart. Previously `script:
      // 'dist/index.js'` skipped the build and deploys carried stale
      // compiled code across multiple restarts (recurring "008 still
      // in skipped list" issue).
      script: 'npm',
      args: 'start',
      cwd: __dirname + '/node',
      env: {
        NODE_ENV: 'production',
        PORT: '3000',
        // Written on the way down when V8 is about to OOM. The 2026-08 crash
        // died with ~1.93GB live after a full mark-compact and there was no
        // way to tell what was holding it; this produces a .heapsnapshot in
        // cwd (backends/node/) that names the retainer in Chrome DevTools.
        //
        // COST: the file is roughly the size of the heap (~2GB), written once
        // per OOM. Make sure the disk has room, and delete old snapshots.
        // Remove this once the leak is found — it is a diagnostic, not a
        // permanent setting.
        //
        // NOT raising --max-old-space-size here on purpose: the process is
        // dying at V8's default ceiling, but this box also runs Postgres and
        // Playwright, and lifting the limit past free RAM just trades a clean
        // V8 OOM for the kernel OOM killer, which takes the process out
        // without a snapshot or a stack.
        NODE_OPTIONS: '--heapsnapshot-near-heap-limit=1'
      },
      env_dev: {
        NODE_ENV: 'dev',
        PORT: '3000'
      },
      watch: false,
      max_restarts: 10,
      restart_delay: 1000,
      // PM2 sends SIGINT first; our index.ts waits for the server to
      // close gracefully before exiting. 10s should be plenty.
      kill_timeout: 10_000
    }

    // Legacy python backend ('API-Proxy', external_api_proxy.py) used to
    // run here. It was retired when the Node port reached parity and the
    // python source has since been removed from the repo. Recover it from
    // git history (e.g. `git show <rev>:backends/external_api_proxy.py`)
    // if a rollback is ever needed.
  ]
};

