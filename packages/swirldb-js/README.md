# @swirldb/js

TypeScript wrapper for SwirlDB with native property access via Proxies.

## Features

- **Native property access**: `db.data.user.name = 'Alice'` instead of `db.setPath('user.name', 'Alice')`
- **Type-safe** (when using TypeScript)
- **Reactive observers**: `db.data.user.$observe(callback)`
- **Auto-persisting stores**: Sync to localStorage automatically
- **Batch operations**: Group changes for efficiency
- **Full WASM performance**: Thin wrapper, zero overhead

## Installation

```bash
npm install @swirldb/js @swirldb/core-wasm
```

## Usage

### Basic Example

```typescript
import init, { SwirlDB as WasmDB } from '@swirldb/core-wasm';
import { SwirlDB } from '@swirldb/js';

await init();
const db = new SwirlDB(new WasmDB());

// Native property access! 🎉
db.data.user.name = 'Alice';
db.data.user.age = 30;

// Read values
console.log(db.data.user.name.$value); // 'Alice'

// Observe changes
db.data.user.name.$observe((newName) => {
  console.log('Name changed:', newName);
});

// Change triggers observer
db.data.user.name = 'Bob'; // Observer fires!
```

### Nested Objects

```typescript
// Auto-creates nested structure
db.data.app.settings.theme = 'dark';
db.data.app.settings.fontSize = 14;

// Access nested values
const theme = db.data.app.settings.theme.$value;
```

### Observers

```typescript
// Method 1: Using .$observe
db.data.user.$observe((user) => {
  console.log('User changed:', user);
});

// Method 2: Traditional API
db.observe('user', (user) => {
  console.log('User changed:', user);
});

// Method 3: Subscribe with unsubscribe
const unsubscribe = db.subscribe('user.name', (name) => {
  console.log('Name changed:', name);
});

// Later...
unsubscribe();
```

### Scoped Access

```typescript
// Get a scoped proxy
const user = db.at('user');

user.name = 'Alice';
user.email = 'alice@example.com';

console.log(user.name.$value); // 'Alice'
```

### Persisted Store

```typescript
import { createPersistedStore } from '@swirldb/js';

// Auto-saves to localStorage on changes
const store = createPersistedStore(db, 'my-app-state', 'app');

store.count = 0;
store.todos = [];

// Changes auto-save after 500ms debounce
store.count++; // Saved to localStorage
```

### Batch Operations

```typescript
// Group multiple changes
db.batch((db) => {
  db.data.user.name = 'Alice';
  db.data.user.email = 'alice@example.com';
  db.data.user.age = 30;
  // Observers fire once at the end
});
```

### Traditional API (Still Available)

```typescript
// Mix and match!
db.setPath('user.name', 'Alice');
const name = db.getPath('user.name');
db.observe('user.name', callback);
```

## API Reference

### `SwirlDB`

**Constructor:**
```typescript
new SwirlDB(wasmDB: WasmSwirlDB)
```

**Properties:**
- `data` - Proxied root object for native access

**Methods:**
- `setPath(path, value)` - Set value at dot-path
- `getPath(path)` - Get value at dot-path
- `observe(path, callback)` - Watch for changes
- `at(path)` - Get scoped proxy
- `batch(fn)` - Group operations
- `saveState()` - Serialize to Uint8Array
- `loadState(bytes)` - Deserialize from Uint8Array

### Proxy Methods

When accessing via `db.data.*`:
- `.$value` - Get current value
- `.$observe(callback)` - Watch for changes
- `.$delete()` - Remove value (sets to null)

## Examples

### Todo List

```typescript
const todos = db.at('todos');

// Add todo
todos['1'] = { text: 'Buy milk', done: false };

// Watch for changes
todos['1'].$observe((todo) => {
  console.log('Todo updated:', todo);
});

// Toggle
todos['1'].done = true; // Observer fires!
```

### Counter

```typescript
db.data.counter = 0;

db.data.counter.$observe((count) => {
  document.getElementById('count').textContent = count;
});

// Increment
db.data.counter++; // UI updates via observer
```

### Form State

```typescript
const form = db.at('form');

form.username = '';
form.email = '';
form.errors = {};

// Validate on change
form.email.$observe((email) => {
  form.errors.email = email.includes('@') ? null : 'Invalid email';
});
```

## Performance

The Proxy wrapper is thin and has near-zero overhead:
- Property access → direct WASM call
- No intermediate allocations
- Observers use WASM implementation
- Batch operations reduce observer overhead

## TypeScript Support

Full type inference coming soon! For now, use explicit types:

```typescript
interface User {
  name: string;
  age: number;
}

const user = db.at('user') as User & { $value: User };
user.name = 'Alice'; // Type-safe!
```

## License

MIT
