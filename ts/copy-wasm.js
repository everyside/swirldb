import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const wasmSrc = path.resolve(__dirname, '../packages/core-wasm/index_bg.wasm');
const wasmDest = path.resolve(__dirname, 'dist/index_bg.wasm');

await fs.copyFile(wasmSrc, wasmDest);
console.log('✅ Copied index_bg.wasm to dist/');
