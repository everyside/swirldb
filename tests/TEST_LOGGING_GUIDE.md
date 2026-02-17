# Test Logging Guide

## Overview

All integration tests now use structured logging with `tracing` for better test output and debugging.

## Quick Start

### Running Tests with Logs

```bash
# Run all tests with logging
RUST_LOG=info cargo test --test integration -- --nocapture

# Run specific test with logs
RUST_LOG=info cargo test --test integration test_concurrent_writes_crdt_merge -- --nocapture

# Debug level for more details
RUST_LOG=debug cargo test --test integration test_two_client_sync -- --nocapture
```

## Example Output

### Before (no logging)
```
test multi_client_sync::test_concurrent_writes_crdt_merge ... ok
```

### After (with logging)
```
 INFO 🧪 Starting test: Concurrent writes with CRDT merge
 INFO    Scenario: Two clients write different fields → Both converge to same state
 INFO Test server started on port 50866
 INFO ✓ Server started
 INFO SubscribeAck: 1 added, 0 denied
 INFO Rust client rust-client-570336e6 connected
 INFO SubscribeAck: 1 added, 0 denied
 INFO Rust client rust-client-f328f3b3 connected
 INFO ✓ Two clients connected
 INFO → Client 1 writes: user.name = 'Alice'
 INFO 📤 BROADCAST: 1 changes (83 bytes) to 2 subscribers
 INFO ← Client 2 waiting for broadcast...
 INFO Received broadcast from rust-client-570336e6: 1 changes
 INFO ✓ Client 2 received broadcast from Client 1
 INFO → Client 2 writes: user.age = 30
 INFO 📤 BROADCAST: 2 changes (199 bytes) to 2 subscribers
 INFO ← Client 1 waiting for broadcast...
 INFO Received broadcast from rust-client-f328f3b3: 2 changes
 INFO ✓ Client 1 received broadcast from Client 2
 INFO ✓ Client 1 has both fields: {name: Alice, age: 30}
 INFO ✓ Client 2 has both fields: {name: Alice, age: 30}
 INFO ✅ Test passed: CRDT merge successful, no data loss
test multi_client_sync::test_concurrent_writes_crdt_merge ... ok
```

## Writing New Tests

### 1. Import the logging helper

```rust
use super::{init_test_logging, rust_client::RustClient, test_server::TestServer};
use tracing::info;
```

### 2. Initialize logging at test start

```rust
#[tokio::test]
async fn test_my_feature() {
    init_test_logging();

    info!("🧪 Starting test: My feature");
    info!("   Scenario: What this test does");

    // ... test code ...
}
```

### 3. Add documentation comments

```rust
/// # Scenario: Brief description
///
/// **Given:**
/// - Preconditions
/// - Initial state
///
/// **When:**
/// - Actions taken
/// - What happens
///
/// **Then:**
/// - Expected outcomes
/// - Assertions
#[tokio::test]
async fn test_my_feature() {
    // ...
}
```

### 4. Use structured logging throughout

```rust
info!("✓ Setup completed");
info!("→ Client writes: data = value");
info!("← Waiting for response...");
info!("✓ Received expected result: {:?}", result);
info!("✅ Test passed: Description of success");
```

## Logging Conventions

### Emoji Guide

- 🧪 Test starting
- ✓ Step completed successfully
- → Outgoing action (write, send)
- ← Incoming action (receive, wait)
- 📤 Server broadcast
- 📥 Client receive
- ✅ Test passed
- ❌ Test failed (in error handling)

### Log Levels

- `info!()` - Main test flow, important milestones
- `debug!()` - Detailed state information, rarely needed
- `warn!()` - Unexpected but non-fatal issues
- `error!()` - Test failures or errors

## Configuration

The logging is configured in `/tests/integration/mod.rs`:

```rust
pub fn init_test_logging() {
    tracing_subscriber::fmt()
        .with_test_writer()    // Works with cargo test
        .without_time()        // No timestamps (tests are fast)
        .with_target(false)    // No module names (cleaner)
        .with_level(true)      // Show log level (INFO, DEBUG, etc.)
        .try_init()
        .ok();
}
```

### Customizing

To change the format, edit `init_test_logging()` in `tests/integration/mod.rs`.

Options:
- `.with_time()` - Add timestamps
- `.with_target(true)` - Show module names
- `.with_level(false)` - Hide log levels
- `.with_max_level(Level::DEBUG)` - Set max level
- `.pretty()` - Multi-line pretty format
- `.json()` - JSON output

## CI/CD

In GitHub Actions, logging is automatically captured without needing `--nocapture`.

The workflow uses clean output (no timestamps, no module names) for better readability in CI logs.

## Benefits

1. **Better debugging** - See exactly what happens in failing tests
2. **Living documentation** - Logs describe test flow in plain English
3. **Performance insights** - See message sizes, timing, counts
4. **Easier troubleshooting** - Understand failures without reading code
5. **Consistent format** - All tests follow same pattern

## Examples

See these files for complete examples:
- `/tests/integration/multi_client_sync.rs` - Full logging examples
- `/tests/integration/network_resilience.rs` - Error handling examples
- `/tests/integration/browser_sync.rs` - Cross-platform examples
