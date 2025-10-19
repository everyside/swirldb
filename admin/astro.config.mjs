// @ts-check
import { defineConfig } from 'astro/config';

// https://astro.build/config
export default defineConfig({
  vite: {
    plugins: [
      {
        name: 'configure-response-headers',
        configureServer: (server) => {
          server.middlewares.use((req, res, next) => {
            // Set permissive CSP for WASM
            res.setHeader('Content-Security-Policy', "default-src 'self'; script-src 'self' 'unsafe-eval' 'unsafe-inline'; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; font-src 'self' https://fonts.gstatic.com; worker-src 'self' blob:; connect-src 'self' ws://localhost:3030 http://localhost:3031;");
            next();
          });
        }
      }
    ]
  }
});
