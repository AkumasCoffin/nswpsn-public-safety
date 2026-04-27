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
      script: 'dist/index.js',
      cwd: __dirname + '/node',
      interpreter: 'node',
      env: {
        NODE_ENV: 'production',
        PORT: '3000'
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

    // Legacy python backend was here ('API-Proxy', external_api_proxy.py).
    // Removed when the Node port reached parity. The python source is
    // still in this directory for reference/rollback; to bring it back:
    //   pm2 start external_api_proxy.py --interpreter python3 --name API-Proxy
    // and stop the node app first to free port 3000.
  ]
};

