# SwirlDB Browser Example

This example demonstrates SwirlDB running in the browser via WebAssembly.

## Running the Example

1. Build the WASM package:
   ```bash
   cd ../../native/swirldb-core
   npm run build:wasm
   ```

2. Serve the example with a local HTTP server:
   ```bash
   # From this directory
   python3 -m http.server 8000
   # Or use any other static file server
   ```

3. Open in your browser:
   ```
   http://localhost:8000
   ```

## Features Demonstrated

- **Path-based operations**: Set and get values using dot-notation paths
- **Observers**: React to changes at specific paths
- **State persistence**: Save/load state to/from localStorage
- **CRDT functionality**: All operations use Automerge under the hood

## Try It Out

1. Set a value: `user.name = "Alice"`
2. Get the value back
3. Add an observer for `user.name`
4. Change the value and see the observer fire
5. Save state to localStorage
6. Refresh the page
7. Load state back and see your data restored
