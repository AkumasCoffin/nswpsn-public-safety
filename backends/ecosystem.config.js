module.exports = {
  apps: [
    {
      name: 'API-Proxy',
      script: 'external_api_proxy.py',
      interpreter: 'python3',
      cwd: process.env.BACKENDS_DIR || __dirname,

      // Production mode (default)
      args: '',
      env: {
        DEV_MODE: 'false'
      },

      // Dev mode - use: pm2 start ecosystem.config.js --env dev
      env_dev: {
        DEV_MODE: 'true'
      },

      // Restart settings
      watch: false,
      max_restarts: 10,
      restart_delay: 1000
    },

    // Node/TypeScript backend (W1+ of the migration). Sits next to the
    // Python service on a different port; Apache decides which backend
    // owns each /api/* route via the strangler-fig cutover plan.
    //
    // Start manually with: pm2 start ecosystem.config.js --only nswpsn-api-node
    // Run `npm run build` in backends/node first to produce dist/.
    {
      name: 'nswpsn-api-node',
      script: 'dist/index.js',
      cwd: __dirname + '/node',
      interpreter: 'node',
      env: {
        NODE_ENV: 'production',
        PORT: '3001'
      },
      env_dev: {
        NODE_ENV: 'dev',
        PORT: '3001'
      },
      watch: false,
      max_restarts: 10,
      restart_delay: 1000,
      // PM2 sends SIGINT first; our index.ts waits for the server to
      // close gracefully before exiting. 10s should be plenty.
      kill_timeout: 10_000
    }
  ]
};

