# SwirlDB

SwirlDB is a modular, pluggable, embedded database engine built in Rust and exposed to TypeScript/JavaScript via WebAssembly.

---

## 🦀 Rust Core (WASM)

The Rust crate lives in:  
```
/native/swirldb-core
```

### 🔧 Development

1. Edit Rust source code in `/native/swirldb-core/src/lib.rs`.
2. Build with:
   ```bash
   wasm-pack build --target bundler --out-dir ../../packages/core-wasm --out-name index
   ```
3. This outputs `.wasm`, `.js`, and `.d.ts` files into `packages/core-wasm/`.

### 🚀 Releasing Rust Core

1. Ensure all changes are committed and tested.
2. Optionally publish to npm (if configured for publishing):
   ```bash
   cd packages/core-wasm
   npm publish
   ```

---

## 📦 TypeScript Runtime + Integration

The TypeScript client code lives in:
```
/ts
```

### 🧪 Local Development

- Use `tsx` with native WASM support:
  ```bash
  npm run dev
  ```
  (equivalent to `tsx --experimental-wasm-modules src/example.ts`)

- Rust/WASM is auto-loaded via generated glue code.

### 🔨 Building

```bash
npm run build
```
This will:
- Compile TS → `dist/`
- Copy the compiled `.wasm` into `dist/` for runtime use

### ▶️ Running

```bash
npm run start
```
This runs `dist/example.js` with `tsx` and the `.wasm` file next to it.

---

## ✅ Notes

- Requires Node.js ≥ 20 and `--experimental-wasm-modules`
- `tsx` is used to support top-level await + WASM loading
- Build artifacts in `/packages/core-wasm` are never edited manually
- Bundling is optional but supported via esbuild/Vite/etc.

---

## 📁 Project Structure

```
native/swirldb-core       # Rust crate compiled to WASM
packages/core-wasm        # WASM output + npm wrapper
ts/                       # TypeScript app/tests/CLI
```