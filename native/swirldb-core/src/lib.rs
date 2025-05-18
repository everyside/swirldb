use wasm_bindgen::prelude::*;
use wasm_bindgen::JsValue;
use js_sys::Object;
use std::cell::RefCell;
use once_cell::unsync::Lazy;
use automerge::{AutoCommit, ScalarValue, ROOT, ObjId, ReadDoc, transaction::Transactable};

thread_local! {
    static OBSERVERS: Lazy<RefCell<Vec<(String, js_sys::Function, JsValue)>>> =
        Lazy::new(|| RefCell::new(Vec::new()));
}

#[wasm_bindgen]
pub struct SwirlDB {
    doc: AutoCommit,
}

#[wasm_bindgen]
impl SwirlDB {
    #[wasm_bindgen(constructor)]
    pub fn new() -> SwirlDB {
        console_error_panic_hook::set_once();
        SwirlDB { doc: AutoCommit::new() }
    }

    #[wasm_bindgen(js_name = setPath)]
    pub fn set_path(&mut self, path: String, value: JsValue) {
        let segments = split_path(&path);
        if segments.is_empty() { return; }
        if let Some(parent) = resolve_path(&mut self.doc, &segments, true) {
            let key = segments.last().unwrap();
            let _ = self.doc.put(parent, key, js_to_am(value));
            self.check_observers();
        }
    }

    #[wasm_bindgen(js_name = getPath)]
    pub fn get_path(&self, path: String) -> JsValue {
        let segments = split_path(&path);
        if segments.is_empty() { return JsValue::NULL; }
        if let Some(parent) = resolve_path_read(&self.doc, &segments) {
            let key = segments.last().unwrap();
            return self.doc.get(&parent, key)
                .ok()
                .flatten()
                .and_then(|(val, _)| val.into_scalar().ok().map(|s| am_to_js(&s)))
                .unwrap_or(JsValue::NULL);
        }
        JsValue::NULL
    }

    #[wasm_bindgen(js_name = saveState)]
    pub fn save_state(&mut self) -> js_sys::Uint8Array {
        let bytes = self.doc.save();
        js_sys::Uint8Array::from(&bytes[..])
    }

    #[wasm_bindgen(js_name = loadState)]
    pub fn load_state(&mut self, input: js_sys::Uint8Array) {
        let vec = input.to_vec();
        if let Ok(doc) = AutoCommit::load(&vec) {
            self.doc = doc;
            self.check_observers();
        }
    }

    #[wasm_bindgen(js_name = observe)]
    pub fn observe(&self, path: String, callback: js_sys::Function) {
        let val = self.get_path(path.clone());
        OBSERVERS.with(|obs| {
            obs.borrow_mut().push((path, callback, val));
        });
    }

    #[wasm_bindgen(js_name = checkObservers)]
    pub fn check_observers(&self) {
        OBSERVERS.with(|obs| {
            for (path, callback, last_val) in obs.borrow_mut().iter_mut() {
                let current = self.get_path(path.clone());
                if !Object::is(&current, &last_val) {
                    let _ = callback.call1(&JsValue::NULL, &current);
                    *last_val = current;
                }
            }
        });
    }
}

fn split_path(dot_path: &str) -> Vec<String> {
    dot_path.split('.').map(|s| s.to_string()).collect()
}

fn resolve_path(doc: &mut AutoCommit, path: &[String], create: bool) -> Option<ObjId> {
    let mut current = ROOT;
    for key in path.iter().take(path.len() - 1) {
        match doc.get(&current, key).ok().flatten() {
            Some((_, obj_id)) => current = obj_id.into(),
            None if create => {
                let new_obj = doc.put_object(&current, key, automerge::ObjType::Map).ok()?;
                current = new_obj;
            }
            _ => return None,
        }
    }
    Some(current)
}

fn resolve_path_read(doc: &AutoCommit, path: &[String]) -> Option<ObjId> {
    let mut current = ROOT;
    for key in path.iter().take(path.len() - 1) {
        match doc.get(&current, key).ok().flatten() {
            Some((_, obj_id)) => current = obj_id.into(),
            None => return None,
        }
    }
    Some(current)
}

fn js_to_am(val: JsValue) -> ScalarValue {
    if val.is_string() {
        ScalarValue::Str(val.as_string().unwrap().into())
    } else if let Some(f) = val.as_f64() {
        ScalarValue::F64(f)
    } else if let Some(b) = val.as_bool() {
        ScalarValue::Boolean(b)
    } else {
        ScalarValue::Null
    }
}

fn am_to_js(val: &ScalarValue) -> JsValue {
    match val {
        ScalarValue::Str(s) => JsValue::from(s.as_str()),
        ScalarValue::F64(f) => JsValue::from(*f),
        ScalarValue::Boolean(b) => JsValue::from(*b),
        ScalarValue::Null => JsValue::NULL,
        _ => JsValue::NULL,
    }
}
