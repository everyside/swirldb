//! Path registry for mapping Automerge objects to document paths.

use super::types::PathBuf;
use anyhow::Result;
use automerge::{ObjId, ObjType, ReadDoc, Value, ROOT};
use std::collections::HashMap;

/// Maps objects and array elements to their paths in the document.
///
/// This is built by traversing the document and maintained incrementally
/// as changes occur.
#[derive(Clone)]
pub struct PathRegistry {
    /// Object ExId → path mapping
    exid_to_path: HashMap<ObjId, PathBuf>,

    /// Reverse lookup: path string → ExId
    path_to_exid: HashMap<String, ObjId>,

    /// For array elements: ExId → numeric index
    array_elements: HashMap<ObjId, usize>,
}

impl PathRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            exid_to_path: HashMap::new(),
            path_to_exid: HashMap::new(),
            array_elements: HashMap::new(),
        }
    }

    /// Build a registry from the current document state.
    ///
    /// This traverses the entire document and builds mappings for all
    /// objects and array elements.
    pub fn from_document<D: ReadDoc>(doc: &D) -> Result<Self> {
        let mut registry = Self::new();
        registry.traverse_and_register(doc, &ROOT, PathBuf::default())?;
        Ok(registry)
    }

    /// Recursively traverse the document and register paths.
    fn traverse_and_register<D: ReadDoc>(
        &mut self,
        doc: &D,
        obj: &ObjId,
        path: PathBuf,
    ) -> Result<()> {
        // Register this object
        self.exid_to_path.insert(obj.clone(), path.clone());
        self.path_to_exid.insert(path.to_string(), obj.clone());

        // Try to get the object type to determine how to traverse it
        match doc.object_type(obj) {
            Ok(ObjType::Map) => {
                // Traverse map keys
                for key in doc.keys(obj) {
                    if let Ok(Some((value, child_exid))) = doc.get(obj, &key) {
                        // Only traverse nested objects
                        if matches!(value, Value::Object(_)) {
                            let mut child_path = path.clone();
                            child_path.push_key(key.to_string());
                            self.traverse_and_register(doc, &child_exid, child_path)?;
                        }
                    }
                }
            }
            Ok(ObjType::List) | Ok(ObjType::Text) => {
                // Traverse list/text elements
                for item in doc.list_range(obj, ..) {
                    if let Value::Object(_) = item.value {
                        let elem_exid = item.id;

                        // Store element ExId → index mapping
                        self.array_elements.insert(elem_exid.clone(), item.index);

                        let mut child_path = path.clone();
                        child_path.push_index(item.index);
                        self.traverse_and_register(doc, &elem_exid, child_path)?;
                    }
                }
            }
            _ => {
                // Not an object, or error getting type - skip
            }
        }

        Ok(())
    }

    /// Look up the path for an object ExId.
    pub fn get_path(&self, exid: &ObjId) -> Option<&PathBuf> {
        self.exid_to_path.get(exid)
    }

    /// Look up the ExId for a path string.
    pub fn get_exid(&self, path: &str) -> Option<&ObjId> {
        self.path_to_exid.get(path)
    }

    /// Look up the numeric index for an array element ExId.
    pub fn get_array_index(&self, exid: &ObjId) -> Option<usize> {
        self.array_elements.get(exid).copied()
    }

    /// Register a single object at a given path.
    ///
    /// Used for incremental updates when new objects are created during set_path.
    pub fn register(&mut self, obj: ObjId, path: PathBuf) {
        self.path_to_exid.insert(path.to_string(), obj.clone());
        self.exid_to_path.insert(obj, path);
    }

    /// Get the number of registered objects.
    pub fn len(&self) -> usize {
        self.exid_to_path.len()
    }

    /// Check if the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.exid_to_path.is_empty()
    }
}

impl Default for PathRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use automerge::transaction::Transactable;
    use automerge::{AutoCommit, ObjType};

    #[test]
    fn test_empty_document() {
        let doc = AutoCommit::new();
        let registry = PathRegistry::from_document(&doc).unwrap();

        // Should have just the root
        assert_eq!(registry.len(), 1);
        assert!(registry.get_path(&ROOT).is_some());
    }

    #[test]
    fn test_simple_map() {
        let mut doc = AutoCommit::new();

        // Create: { users: { alice: {} } }
        let users = doc.put_object(&ROOT, "users", ObjType::Map).unwrap();
        let alice = doc.put_object(&users, "alice", ObjType::Map).unwrap();

        let registry = PathRegistry::from_document(&doc).unwrap();

        // Check root
        let root_path = registry.get_path(&ROOT).unwrap();
        assert_eq!(root_path.to_string(), "");

        // Check users
        let users_path = registry.get_path(&users).unwrap();
        assert_eq!(users_path.to_string(), "users");

        // Check alice
        let alice_path = registry.get_path(&alice).unwrap();
        assert_eq!(alice_path.to_string(), "users.alice");
    }

    #[test]
    fn test_nested_maps() {
        let mut doc = AutoCommit::new();

        // Create: { config: { settings: { theme: {} } } }
        let config = doc.put_object(&ROOT, "config", ObjType::Map).unwrap();
        let settings = doc.put_object(&config, "settings", ObjType::Map).unwrap();
        let theme = doc.put_object(&settings, "theme", ObjType::Map).unwrap();

        let registry = PathRegistry::from_document(&doc).unwrap();

        let theme_path = registry.get_path(&theme).unwrap();
        assert_eq!(theme_path.to_string(), "config.settings.theme");
    }

    #[test]
    fn test_simple_list() {
        let mut doc = AutoCommit::new();

        // Create: { items: [obj1, obj2] }
        let items = doc.put_object(&ROOT, "items", ObjType::List).unwrap();
        let obj1 = doc.insert_object(&items, 0, ObjType::Map).unwrap();
        let obj2 = doc.insert_object(&items, 1, ObjType::Map).unwrap();

        let registry = PathRegistry::from_document(&doc).unwrap();

        // Check items list
        let items_path = registry.get_path(&items).unwrap();
        assert_eq!(items_path.to_string(), "items");

        // Check obj1
        let obj1_path = registry.get_path(&obj1).unwrap();
        assert_eq!(obj1_path.to_string(), "items[0]");

        // Check obj2
        let obj2_path = registry.get_path(&obj2).unwrap();
        assert_eq!(obj2_path.to_string(), "items[1]");

        // Check array index lookups
        assert_eq!(registry.get_array_index(&obj1), Some(0));
        assert_eq!(registry.get_array_index(&obj2), Some(1));
    }

    #[test]
    fn test_mixed_structure() {
        let mut doc = AutoCommit::new();

        // Create: { users: [{ profile: {} }] }
        let users = doc.put_object(&ROOT, "users", ObjType::List).unwrap();
        let user = doc.insert_object(&users, 0, ObjType::Map).unwrap();
        let profile = doc.put_object(&user, "profile", ObjType::Map).unwrap();

        let registry = PathRegistry::from_document(&doc).unwrap();

        let profile_path = registry.get_path(&profile).unwrap();
        assert_eq!(profile_path.to_string(), "users[0].profile");
    }

    #[test]
    fn test_reverse_lookup() {
        let mut doc = AutoCommit::new();

        let users = doc.put_object(&ROOT, "users", ObjType::Map).unwrap();
        let alice = doc.put_object(&users, "alice", ObjType::Map).unwrap();

        let registry = PathRegistry::from_document(&doc).unwrap();

        // Forward lookup
        let path = registry.get_path(&alice).unwrap();
        assert_eq!(path.to_string(), "users.alice");

        // Reverse lookup
        let exid = registry.get_exid("users.alice").unwrap();
        assert_eq!(exid, &alice);
    }

    #[test]
    fn test_scalars_not_registered() {
        let mut doc = AutoCommit::new();

        // Create: { name: "alice", age: 30 }
        doc.put(&ROOT, "name", "alice").unwrap();
        doc.put(&ROOT, "age", 30).unwrap();

        let registry = PathRegistry::from_document(&doc).unwrap();

        // Only root should be registered (scalars don't get registered)
        assert_eq!(registry.len(), 1);
    }
}
