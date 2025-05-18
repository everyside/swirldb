use std::collections::HashMap;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsValue;

#[wasm_bindgen]
pub struct SwirlDB {
    store: HashMap<String, JsValue>,
}

#[wasm_bindgen]
impl SwirlDB {
    #[wasm_bindgen(constructor)]
    pub fn new() -> SwirlDB {
        console_error_panic_hook::set_once();
        SwirlDB {
            store: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: String, value: JsValue) {
        self.store.insert(key, value);
    }

    pub fn get(&self, key: String) -> JsValue {
        self.store.get(&key).cloned().unwrap_or(JsValue::NULL)
    }

    pub fn delete(&mut self, key: String) {
        self.store.remove(&key);
    }

    pub fn clear(&mut self) {
        self.store.clear();
    }
}
