# SwirlDB

SwirlDB is a modular, reactive in-memory database framework designed for real-time applications and extensibility. It supports plugin hooks, transports, sync engines, encryption layers, and reactive adapters — all managed via an Nx monorepo.

---

## 🧱 Monorepo Structure

Each package lives in `packages/<name>` and is independently buildable and testable.

| Package            | Description                                   |
| ------------------ | --------------------------------------------- |
| `core`             | Core database logic with CRUD + subscriptions |
| `cli`              | CLI tools for backup, restore, and inspect    |
| `storage-json`     | File-based JSON persistence layer             |
| `storage-protobuf` | Protocol Buffers-based storage backend        |
| `encryption-none`  | No-op encryption (for dev/testing)            |
| `encryption-basic` | Base64-based placeholder encryption           |
| `transport-rest`   | Simple REST API for accessing SwirlDB         |
| `transport-ws`     | WebSocket-based push transport                |
| `sync`             | CRDT-style sync engine (WIP)                  |
| `react-adapter`    | React hooks for binding to SwirlDB            |

---

## 🛠 Tooling

- **Framework**: Nx v20+
- **Language**: TypeScript
- **Test runner**: Vitest
- **Bundler**: esbuild
- **Formatter**: Prettier
- **Linter**: ESLint (flat config, strict mode)

---

## 🚀 Commands

```bash
npm install        # install all dependencies
npm run lint       # lint all packages
npm run format     # format all packages
npm run format:check  # verify formatting
npm run test       # run tests for all packages
npm run build      # build all packages
npm run test:watch # watch tests across the repo
```

---

## 🧪 Testing with Vitest

Each package contains `.test.ts` files and its own `project.json` with a test target.

To test a specific package:

```bash
nx run <package>:test
```

Example:

```bash
nx run core:test
```

---

## ✅ Lint & Formatting

- ESLint is strict (`--max-warnings=0`)
- Unused variables prefixed with `_` are ignored
- Prettier is enforced via `npm run format:check` in CI

---

## 🧪 CI (GitHub Actions)

- Runs on PRs and pushes to `main`
- Executes format check, lint, build, and test
- Uses Node 22 with `npm ci`

---

## 🧼 Housekeeping

To clear Nx's cache or reset flaky test status:

```bash
npx nx reset
```

---

## 📍 License

Apache 2.0 © Everyside Innovations, LLC, 2025
