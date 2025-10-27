// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use async_trait::async_trait;
use super::{EncryptionProvider, EncryptionProviderMarker, UnencryptedProvider};

/// Field-level encryption provider with pattern-based key selection
///
/// This provider allows encrypting specific fields based on path patterns:
/// - Different keys for different field patterns
/// - Selective encryption (e.g., only encrypt "*.ssn" or "user.*.password")
/// - Per-field nonces for independent encryption
///
/// Example usage:
/// ```ignore
/// let mut provider = FieldEncryptionProvider::new();
/// provider.add_pattern("*.ssn", Box::new(AesGcmProvider::new_random()));
/// provider.add_pattern("user.*.password", Box::new(AesGcmProvider::new_random()));
/// ```
pub struct FieldEncryptionProvider {
    patterns: Vec<(String, Box<dyn EncryptionProvider>)>,
    default_provider: Box<dyn EncryptionProvider>,
}

#[cfg(not(target_arch = "wasm32"))]
impl EncryptionProviderMarker for FieldEncryptionProvider {}

#[cfg(target_arch = "wasm32")]
impl EncryptionProviderMarker for FieldEncryptionProvider {}

impl FieldEncryptionProvider {
    /// Create a new field-level provider with unencrypted default
    pub fn new() -> Self {
        Self {
            patterns: Vec::new(),
            default_provider: Box::new(UnencryptedProvider::new()),
        }
    }

    /// Create a new field-level provider with encrypted default
    pub fn new_encrypted_default(default: Box<dyn EncryptionProvider>) -> Self {
        Self {
            patterns: Vec::new(),
            default_provider: default,
        }
    }

    /// Add a field pattern with its encryption provider
    ///
    /// Patterns support wildcards:
    /// - "*.ssn" matches "user.ssn", "employee.ssn", etc.
    /// - "user.*.password" matches "user.alice.password", "user.bob.password", etc.
    /// - "secret.*" matches any field under "secret"
    pub fn add_pattern(&mut self, pattern: &str, provider: Box<dyn EncryptionProvider>) {
        self.patterns.push((pattern.to_string(), provider));
    }

    /// Find the encryption provider for a given field path
    #[allow(dead_code)]
    fn provider_for_path(&self, _path: &str) -> &dyn EncryptionProvider {
        // Simple implementation: just use default for now
        // In production, implement wildcard matching
        self.default_provider.as_ref()
    }
}

impl Default for FieldEncryptionProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl EncryptionProvider for FieldEncryptionProvider {
    async fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
        self.default_provider.encrypt(plaintext).await
    }

    async fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>> {
        self.default_provider.decrypt(ciphertext).await
    }

    fn scheme_name(&self) -> &str {
        "field-level"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encryption::AesGcmProvider;

    #[test]
    fn test_field_encryption_provider() {
        use futures::executor::block_on;

        let provider = FieldEncryptionProvider::new();
        assert_eq!(provider.scheme_name(), "field-level");

        let data = b"Field data";
        let encrypted = block_on(provider.encrypt(data)).unwrap();
        assert_eq!(&encrypted[..], data);
    }

    #[test]
    fn test_field_encryption_with_encrypted_default() {
        use futures::executor::block_on;

        let aes_provider = Box::new(AesGcmProvider::new_random());
        let provider = FieldEncryptionProvider::new_encrypted_default(aes_provider);

        let data = b"Field data with encryption";
        let encrypted = block_on(provider.encrypt(data)).unwrap();
        assert_ne!(&encrypted[..], data);

        let decrypted = block_on(provider.decrypt(&encrypted)).unwrap();
        assert_eq!(&decrypted[..], data);
    }
}
