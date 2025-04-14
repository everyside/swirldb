import { build } from 'esbuild';

build({
  entryPoints: ['src/index.ts'],
  outfile: 'dist/index.js',
  bundle: true,
  platform: 'node',
  target: ['node18'],
  format: 'cjs',
  sourcemap: true,
  logLevel: 'info',
  external: ['ws', 'zod'],
}).catch(() => process.exit(1));
