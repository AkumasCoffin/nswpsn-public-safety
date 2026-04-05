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
    }
  ]
};

