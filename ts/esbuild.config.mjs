import { build } from 'esbuild';
import { wasmLoader } from 'esbuild-plugin-wasm';

build({
  entryPoints: ['src/example.ts'],
  bundle: true,
  outfile: 'dist/example.js',
  format: 'esm',
  target: ['es2022'],
  plugins: [wasmLoader()],
  loader: { '.wasm': 'file' },
  sourcemap: true,
}).catch(() => process.exit(1));
