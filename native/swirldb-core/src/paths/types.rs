//! Path types for tracking changes in Automerge documents.

use std::fmt;

/// A segment in a document path.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum PathSegment {
    /// Map key segment (e.g., "users", "alice", "email")
    Key(String),
    /// Array index segment (e.g., [0], [5], [42])
    Index(usize),
}

/// A path through the document composed of segments.
///
/// Examples:
/// - `users.alice.email` → [Key("users"), Key("alice"), Key("email")]
/// - `items[2].name` → [Key("items"), Index(2), Key("name")]
/// - `matrix[0][1]` → [Key("matrix"), Index(0), Index(1)]
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct PathBuf {
    segments: Vec<PathSegment>,
}

impl PathBuf {
    /// Create an empty path.
    pub fn new() -> Self {
        Self {
            segments: Vec::new(),
        }
    }

    /// Append a map key to the path.
    pub fn push_key(&mut self, key: impl Into<String>) {
        self.segments.push(PathSegment::Key(key.into()));
    }

    /// Append an array index to the path.
    pub fn push_index(&mut self, index: usize) {
        self.segments.push(PathSegment::Index(index));
    }

    /// Get the segments of this path.
    pub fn segments(&self) -> &[PathSegment] {
        &self.segments
    }

    /// Check if the path is empty (root).
    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    /// Get the length (number of segments).
    pub fn len(&self) -> usize {
        self.segments.len()
    }

    /// Create a PathBuf from a dot-separated path string (e.g., "user.profile").
    ///
    /// Note: This only handles key segments. Array indices in paths like
    /// "items[2].name" are not parsed by this method.
    pub fn from_dot_path(path: &str) -> Self {
        if path.is_empty() {
            return Self::new();
        }
        Self {
            segments: path
                .split('.')
                .map(|s| PathSegment::Key(s.to_string()))
                .collect(),
        }
    }
}

impl fmt::Display for PathBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, segment) in self.segments.iter().enumerate() {
            match segment {
                PathSegment::Key(key) => {
                    // Add dot separator if this is not the first segment
                    if i > 0 {
                        write!(f, ".")?;
                    }
                    write!(f, "{}", key)?;
                }
                PathSegment::Index(idx) => {
                    write!(f, "[{}]", idx)?;
                }
            }
        }
        Ok(())
    }
}

impl From<Vec<PathSegment>> for PathBuf {
    fn from(segments: Vec<PathSegment>) -> Self {
        Self { segments }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_path() {
        let path = PathBuf::new();
        assert!(path.is_empty());
        assert_eq!(path.len(), 0);
        assert_eq!(path.to_string(), "");
    }

    #[test]
    fn test_simple_map_path() {
        let mut path = PathBuf::new();
        path.push_key("users");
        path.push_key("alice");
        path.push_key("email");

        assert_eq!(path.to_string(), "users.alice.email");
    }

    #[test]
    fn test_array_path() {
        let mut path = PathBuf::new();
        path.push_key("items");
        path.push_index(2);
        path.push_key("name");

        assert_eq!(path.to_string(), "items[2].name");
    }

    #[test]
    fn test_nested_arrays() {
        let mut path = PathBuf::new();
        path.push_key("matrix");
        path.push_index(0);
        path.push_index(1);

        assert_eq!(path.to_string(), "matrix[0][1]");
    }

    #[test]
    fn test_array_of_objects() {
        let mut path = PathBuf::new();
        path.push_key("users");
        path.push_index(5);
        path.push_key("name");

        assert_eq!(path.to_string(), "users[5].name");
    }

    #[test]
    fn test_complex_path() {
        let mut path = PathBuf::new();
        path.push_key("data");
        path.push_key("items");
        path.push_index(3);
        path.push_key("values");
        path.push_index(0);

        assert_eq!(path.to_string(), "data.items[3].values[0]");
    }

    #[test]
    fn test_path_equality() {
        let mut path1 = PathBuf::new();
        path1.push_key("a");
        path1.push_key("b");

        let mut path2 = PathBuf::new();
        path2.push_key("a");
        path2.push_key("b");

        assert_eq!(path1, path2);
    }

    #[test]
    fn test_from_segments() {
        let segments = vec![
            PathSegment::Key("users".to_string()),
            PathSegment::Index(0),
            PathSegment::Key("email".to_string()),
        ];

        let path = PathBuf::from(segments);
        assert_eq!(path.to_string(), "users[0].email");
    }
}
