// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use async_trait::async_trait;
use super::{EncryptionProvider, EncryptionProviderMarker};

/// Default unencrypted provider (no-op, data passes through unchanged)
///
/// This is the default encryption provider for SwirlDB. It provides
/// no encryption - data is stored in plaintext.
///
/// Use this when encryption is not required or is handled externally
/// (e.g., encrypted filesystem, database-level encryption).
#[derive(Debug, Clone, Default)]
pub struct UnencryptedProvider;

#[cfg(not(target_arch = "wasm32"))]
impl EncryptionProviderMarker for UnencryptedProvider {}

#[cfg(target_arch = "wasm32")]
impl EncryptionProviderMarker for UnencryptedProvider {}

impl UnencryptedProvider {
    pub fn new() -> Self {
        Self
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl EncryptionProvider for UnencryptedProvider {
    async fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
        Ok(plaintext.to_vec())
    }

    async fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>> {
        Ok(ciphertext.to_vec())
    }

    fn scheme_name(&self) -> &str {
        "unencrypted"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unencrypted_provider() {
        let provider = UnencryptedProvider::new();
        assert_eq!(provider.scheme_name(), "unencrypted");
    }

    #[test]
    fn test_unencrypted_provider_sync() {
        use futures::executor::block_on;

        let provider = UnencryptedProvider::new();
        let data = b"Hello, SwirlDB!";

        let encrypted = block_on(provider.encrypt(data)).unwrap();
        assert_eq!(encrypted, data);

        let decrypted = block_on(provider.decrypt(&encrypted)).unwrap();
        assert_eq!(decrypted, data);
    }
}
