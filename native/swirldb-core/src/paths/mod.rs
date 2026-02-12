//! Field-level path tracking for Automerge documents.
//!
//! This module provides efficient tracking of which exact paths changed
//! in an Automerge document, enabling precise change notifications and
//! efficient sync.
//!
//! # Example
//!
//! ```rust,ignore
//! use swirldb_core::paths::{PathRegistry, PathExtractor};
//! use automerge::AutoCommit;
//!
//! let mut doc = AutoCommit::new();
//! // ... build document ...
//!
//! // Build registry from document
//! let registry = PathRegistry::from_document(&doc).unwrap();
//!
//! // Make changes
//! // ...
//!
//! // Extract changed paths
//! let changes = doc.get_changes(&[]);
//! let extractor = PathExtractor::new(registry);
//! let paths = extractor.extract_paths(&changes[0]).unwrap();
//! // paths = ["users.alice.email", "users.bob.name"]
//! ```

mod extractor;
mod registry;
mod types;

pub use extractor::PathExtractor;
pub use registry::PathRegistry;
pub use types::{PathBuf, PathSegment};
