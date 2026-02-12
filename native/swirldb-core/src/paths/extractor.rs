//! Extract changed paths from Automerge documents.

use super::registry::PathRegistry;
use super::types::PathBuf;
use anyhow::Result;
use automerge::patches::{Patch, PatchAction};
use automerge::{ObjId, Prop};
use std::collections::HashSet;

/// Extracts changed paths from Automerge patches and operations.
pub struct PathExtractor {
    registry: PathRegistry,
}

impl PathExtractor {
    /// Create a new extractor with the given registry.
    pub fn new(registry: PathRegistry) -> Self {
        Self { registry }
    }

    /// Get a reference to the registry.
    pub fn registry(&self) -> &PathRegistry {
        &self.registry
    }

    /// Extract changed paths from patches.
    ///
    /// Returns a sorted, deduplicated list of path strings.
    pub fn extract_paths_from_patches(&self, patches: &[Patch]) -> Result<Vec<String>> {
        let mut paths = HashSet::new();

        for patch in patches {
            let path_str = self.patch_to_path(patch);
            paths.insert(path_str);
        }

        let mut sorted: Vec<_> = paths.into_iter().collect();
        sorted.sort();
        Ok(sorted)
    }

    /// Convert a patch's path to a path string.
    fn patch_to_path(&self, patch: &Patch) -> String {
        let mut path = PathBuf::new();

        // Build path from the patch's path segments
        for (_obj_id, prop) in &patch.path {
            match prop {
                Prop::Map(key) => path.push_key(key),
                Prop::Seq(index) => path.push_index(*index),
            }
        }

        // Add the final property from the action
        match &patch.action {
            PatchAction::PutMap { key, .. } | PatchAction::DeleteMap { key } => {
                path.push_key(key);
            }
            PatchAction::PutSeq { index, .. }
            | PatchAction::DeleteSeq { index, .. }
            | PatchAction::Insert { index, .. } => {
                path.push_index(*index);
            }
            _ => {
                // Other actions (Increment, SpliceText, Mark, Conflict) don't add to the path
            }
        }

        path.to_string()
    }

    /// Build a path string from an object ID and a property.
    ///
    /// This is useful for tracking changes as they happen.
    pub fn build_path(&self, obj: &ObjId, prop: &Prop) -> Option<String> {
        // Get the parent path from registry
        let mut path = self.registry.get_path(obj)?.clone();

        // Append the property
        match prop {
            Prop::Map(key) => path.push_key(key),
            Prop::Seq(index) => path.push_index(*index),
        }

        Some(path.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use automerge::transaction::Transactable;
    use automerge::{AutoCommit, ObjType, ROOT};

    #[test]
    fn test_build_path_map() {
        let mut doc = AutoCommit::new();

        // Create: { users: { alice: { email: "old" } } }
        let users = doc.put_object(&ROOT, "users", ObjType::Map).unwrap();
        let alice = doc.put_object(&users, "alice", ObjType::Map).unwrap();
        doc.put(&alice, "email", "old@example.com").unwrap();

        // Build registry
        let registry = PathRegistry::from_document(&doc).unwrap();
        let extractor = PathExtractor::new(registry);

        // Build path for alice.email
        let path = extractor
            .build_path(&alice, &Prop::Map("email".to_string()))
            .unwrap();
        assert_eq!(path, "users.alice.email");
    }

    #[test]
    fn test_build_path_list() {
        let mut doc = AutoCommit::new();

        // Create: { items: ["a", "b", "c"] }
        let items = doc.put_object(&ROOT, "items", ObjType::List).unwrap();
        doc.insert(&items, 0, "a").unwrap();
        doc.insert(&items, 1, "b").unwrap();
        doc.insert(&items, 2, "c").unwrap();

        let registry = PathRegistry::from_document(&doc).unwrap();
        let extractor = PathExtractor::new(registry);

        // Build path for items[1]
        let path = extractor.build_path(&items, &Prop::Seq(1)).unwrap();
        assert_eq!(path, "items[1]");
    }

    #[test]
    fn test_build_path_nested() {
        let mut doc = AutoCommit::new();

        // Create: { data: { items: [{ value: 1 }] } }
        let data = doc.put_object(&ROOT, "data", ObjType::Map).unwrap();
        let items = doc.put_object(&data, "items", ObjType::List).unwrap();
        let item = doc.insert_object(&items, 0, ObjType::Map).unwrap();
        doc.put(&item, "value", 1).unwrap();

        let registry = PathRegistry::from_document(&doc).unwrap();
        let extractor = PathExtractor::new(registry);

        // Build path for item.value (inside list)
        let path = extractor
            .build_path(&item, &Prop::Map("value".to_string()))
            .unwrap();
        assert_eq!(path, "data.items[0].value");
    }

    #[test]
    fn test_build_path_root() {
        let doc = AutoCommit::new();

        let registry = PathRegistry::from_document(&doc).unwrap();
        let extractor = PathExtractor::new(registry);

        // Build path for root.key
        let path = extractor
            .build_path(&ROOT, &Prop::Map("key".to_string()))
            .unwrap();
        assert_eq!(path, "key");
    }

    #[test]
    fn test_extract_from_patches() {
        use automerge::Automerge;

        // Create initial document with Automerge (not AutoCommit) to get patches
        let mut doc = Automerge::new();

        // Create initial structure: { value: 1 }
        let mut tx = doc.transaction();
        tx.put(&ROOT, "value", 1).unwrap();
        tx.commit();

        // Build registry from current state
        let registry = PathRegistry::from_document(&doc).unwrap();
        let extractor = PathExtractor::new(registry);

        // Make a change and capture patches
        let patch_log =
            automerge::patches::PatchLog::active(automerge::patches::TextRepresentation::String(
                automerge::TextEncoding::UnicodeCodePoint,
            ));
        let mut tx = doc.transaction_log_patches(patch_log);
        tx.put(&ROOT, "value", 2).unwrap();
        let (_, mut patch_log) = tx.commit();

        // Extract paths from patches
        let patches = doc.make_patches(&mut patch_log);
        let paths = extractor.extract_paths_from_patches(&patches).unwrap();

        // Should detect change at root.value
        assert_eq!(paths, vec!["value"]);
    }
}
