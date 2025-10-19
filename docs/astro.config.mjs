// @ts-check
import { defineConfig } from 'astro/config';
import { fileURLToPath } from 'url';
import path from 'path';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// https://astro.build/config
export default defineConfig({
  server: {
    port: 4321
  },
  vite: {
    resolve: {
      alias: {
        '@swirldb/wasm': path.resolve(__dirname, '../packages/browser-wasm'),
        '@swirldb/js': path.resolve(__dirname, '../packages/swirldb-js/dist')
      }
    },
    server: {
      fs: {
        // Allow serving files from parent directories (for packages/)
        allow: ['..']
      }
    }
  }
});
