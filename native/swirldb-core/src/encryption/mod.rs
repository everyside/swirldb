// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

//! Encryption providers for SwirlDB
//!
//! This module provides pluggable encryption strategies:
//! - `UnencryptedProvider` - No-op (default)
//! - `AesGcmProvider` - AES-256-GCM document-level encryption
//! - `FieldEncryptionProvider` - Pattern-based field-level encryption
//!
//! All providers work on all platforms (browser + server).

use anyhow::Result;
use async_trait::async_trait;

// Marker trait for Send + Sync requirements
#[cfg(not(target_arch = "wasm32"))]
pub trait EncryptionProviderMarker: Send + Sync {}

#[cfg(target_arch = "wasm32")]
pub trait EncryptionProviderMarker {}

/// Encryption provider for document-level or field-level encryption
///
/// This trait allows swappable encryption strategies:
/// - UnencryptedProvider (default, no-op)
/// - AesGcmProvider (AES-256-GCM document-level encryption)
/// - FieldEncryptionProvider (selective field encryption)
/// - Custom implementations
///
/// Implementations work on all platforms (browser and server).
/// For WASM targets, implement with #[async_trait(?Send)]
/// For native targets, implement with #[async_trait]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait EncryptionProvider: EncryptionProviderMarker {
    /// Encrypt data before storage
    async fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>>;

    /// Decrypt data after retrieval
    async fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>>;

    /// Get the encryption scheme name for debugging/metadata
    fn scheme_name(&self) -> &str;
}

// Implementation modules
mod unencrypted;
mod aes_gcm;
mod field_level;

// Re-exports
pub use unencrypted::UnencryptedProvider;
pub use aes_gcm::AesGcmProvider;
pub use field_level::FieldEncryptionProvider;
