# Native Array/Object Support Implementation Plan

## Problem
Current WASM bindings only support scalar types (string, number, boolean, null). Arrays and objects are rejected with "Unsupported value type" error.

**Current workaround**: Store arrays as JSON strings
```javascript
db.data.messages = JSON.stringify([{...}, {...}]);  // ❌ Wrong - breaks CRDT
```

**Desired behavior**: Native array/object support
```javascript
db.data.messages = [{...}, {...}];  // ✅ Should work with proper CRDT sync
```

## Why This Matters
Storing arrays as JSON strings defeats the purpose of using Automerge:
- ❌ No CRDT conflict resolution for array elements
- ❌ No granular change tracking (whole string changes instead of element changes)
- ❌ Inefficient sync (sends entire JSON string instead of delta changes)
- ❌ Can't subscribe to changes on individual array elements

## Implementation Plan

### 1. Core Layer (`native/swirldb-core/src/core.rs`)

Add methods to handle complex values:

```rust
use automerge::{ObjType, Value};
use serde_json::Value as JsonValue;

impl SwirlDB {
    /// Set a value that can be scalar, array, or object
    pub fn set_value(&self, path: &str, value: JsonValue) -> Result<()> {
        let segments = split_path(path);
        let mut doc = self.doc.lock().unwrap();

        if let Some(parent) = resolve_path(&mut doc, &segments, true) {
            let key = segments.last().unwrap();
            self.insert_value(&mut doc, &parent, key, &value)?;
            Ok(())
        } else {
            Err(anyhow!("Failed to resolve path"))
        }
    }

    /// Recursively insert JSON value as proper Automerge types
    fn insert_value(&self, doc: &mut AutoCommit, parent: &ObjId, key: &str, value: &JsonValue) -> Result<()> {
        match value {
            JsonValue::Null => doc.put(parent, key, ScalarValue::Null)?,
            JsonValue::Bool(b) => doc.put(parent, key, ScalarValue::Boolean(*b))?,
            JsonValue::Number(n) => {
                if let Some(i) = n.as_i64() {
                    doc.put(parent, key, ScalarValue::Int(i))?;
                } else if let Some(f) = n.as_f64() {
                    doc.put(parent, key, ScalarValue::F64(f))?;
                }
            },
            JsonValue::String(s) => doc.put(parent, key, ScalarValue::Str(s.clone().into()))?,
            JsonValue::Array(arr) => {
                let list_id = doc.put_object(parent, key, ObjType::List)?;
                for (i, item) in arr.iter().enumerate() {
                    self.insert_value_at_index(doc, &list_id, i, item)?;
                }
            },
            JsonValue::Object(obj) => {
                let map_id = doc.put_object(parent, key, ObjType::Map)?;
                for (k, v) in obj.iter() {
                    self.insert_value(doc, &map_id, k, v)?;
                }
            }
        }
        Ok(())
    }

    /// Get value as JSON (handles scalars, arrays, objects)
    pub fn get_value(&self, path: &str) -> Option<JsonValue> {
        let doc = self.doc.lock().unwrap();
        // ... implementation to traverse and convert back to JSON
    }
}
```

### 2. WASM Bindings (`native/swirldb-core/src/browser.rs`)

Update to use `serde_wasm_bindgen` for conversion:

```rust
use serde_wasm_bindgen::{from_value, to_value};

#[wasm_bindgen]
impl SwirlDB {
    /// Set any JavaScript value (scalar, array, or object)
    #[wasm_bindgen(js_name = setValue)]
    pub fn set_value_js(&mut self, path: String, value: JsValue) -> Result<(), JsValue> {
        // Convert JS value to serde_json::Value
        let json_value: serde_json::Value = from_value(value)
            .map_err(|e| JsValue::from_str(&format!("Failed to convert value: {}", e)))?;

        self.core
            .set_value(&path, json_value)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        self.check_observers();
        Ok(())
    }

    /// Get any JavaScript value
    #[wasm_bindgen(js_name = getValue)]
    pub fn get_value_js(&self, path: String) -> JsValue {
        match self.core.get_value(&path) {
            Some(value) => to_value(&value).unwrap_or(JsValue::NULL),
            None => JsValue::NULL,
        }
    }
}
```

### 3. TypeScript Wrapper Updates (`docs/public/swirldb.js`)

Update proxy to use `setValue`/`getValue` instead of `setPath`/`getPath`:

```javascript
set(target, prop, value) {
    const fullPath = [...this.path, prop].join('.');
    // Use setValue instead of setPath - supports any type
    this.db.setValue(fullPath, value);
    if (this.swirlDB) {
        this.swirlDB.triggerAutoPersist();
    }
    return true;
}

get(target, prop) {
    // ...
    if (prop === '$value') {
        return this.db.getValue(this.path.join('.'));  // Returns native JS types
    }
    // ...
}
```

### 4. Migration Path

**Phase 1**: Add new methods alongside existing ones
- Keep `setPath`/`getPath` for backwards compatibility
- Add `setValue`/`getValue` for complex types
- Update TypeScript wrapper to use new methods

**Phase 2**: Update all code to use new methods
- Chat client: Use native arrays instead of JSON strings
- Admin: Use native objects
- Server: Already uses proper types internally

**Phase 3**: Deprecate old methods
- Mark `setPath`/`getPath` as deprecated
- Eventually remove if not needed

## Benefits

✅ **Proper CRDT behavior**: Array elements sync independently
✅ **Granular observers**: Can watch individual array elements
✅ **Efficient sync**: Only changed elements transmitted
✅ **Better DX**: Natural JavaScript syntax works as expected
✅ **Unlocks full Automerge power**: Lists, Maps, nested structures

## Testing Plan

1. **Unit tests**: Test conversion between JS and Automerge types
2. **Integration tests**: Test array element sync between two clients
3. **Conflict resolution**: Test concurrent array modifications
4. **Performance**: Compare JSON string vs native array sync

## Example Usage After Implementation

```javascript
// Chat messages as native array
db.data.messages = [
    {id: '1', from: 'alice', text: 'Hello'},
    {id: '2', from: 'bob', text: 'Hi!'}
];

// Add a message - only this change syncs
db.data.messages.push({id: '3', from: 'alice', text: 'How are you?'});

// Modify a specific message - only this element syncs
db.data.messages[1].text = 'Hi there!';

// Subscribe to specific array index
db.data.messages[0].$observe((newVal) => {
    console.log('First message changed:', newVal);
});
```

## References

- Automerge docs: https://automerge.org/docs/
- serde_wasm_bindgen: https://docs.rs/serde-wasm-bindgen/
- Current limitation in `browser.rs:319`: "Unsupported value type"
