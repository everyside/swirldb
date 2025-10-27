// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use aes_gcm::{
    aead::{Aead, KeyInit, OsRng},
    Aes256Gcm, Nonce,
};
use rand::RngCore;
use hkdf::Hkdf;
use sha2::Sha256;
use super::{EncryptionProvider, EncryptionProviderMarker};

/// AES-256-GCM encryption provider for document-level encryption
///
/// This provider encrypts entire documents using AES-256-GCM (Galois/Counter Mode).
/// GCM provides both confidentiality and authenticity (AEAD - Authenticated Encryption
/// with Associated Data).
///
/// Features:
/// - 256-bit key strength
/// - Random 96-bit nonce per encryption (stored with ciphertext)
/// - Authentication tag prevents tampering
/// - Works on all platforms (WASM + native)
///
/// Format: [12-byte nonce][ciphertext][16-byte auth tag]
pub struct AesGcmProvider {
    cipher: Aes256Gcm,
}

#[cfg(not(target_arch = "wasm32"))]
impl EncryptionProviderMarker for AesGcmProvider {}

#[cfg(target_arch = "wasm32")]
impl EncryptionProviderMarker for AesGcmProvider {}

impl AesGcmProvider {
    /// Create a new AES-GCM provider with a raw 32-byte key
    pub fn new(key: &[u8; 32]) -> Self {
        let cipher = Aes256Gcm::new(key.into());
        Self { cipher }
    }

    /// Create a new AES-GCM provider with a random key
    pub fn new_random() -> Self {
        let key = Aes256Gcm::generate_key(&mut OsRng);
        Self {
            cipher: Aes256Gcm::new(&key),
        }
    }

    /// Derive a key from a password using HKDF-SHA256
    ///
    /// This is NOT a password hashing function (use Argon2/bcrypt for that).
    /// This is for deriving encryption keys from existing secrets.
    pub fn from_password(password: &[u8], salt: &[u8]) -> Result<Self> {
        let hkdf = Hkdf::<Sha256>::new(Some(salt), password);
        let mut key = [0u8; 32];
        hkdf.expand(b"swirldb-encryption-key", &mut key)
            .map_err(|e| anyhow!("Key derivation failed: {}", e))?;
        Ok(Self::new(&key))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl EncryptionProvider for AesGcmProvider {
    async fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
        let mut nonce_bytes = [0u8; 12];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ciphertext = self
            .cipher
            .encrypt(nonce, plaintext)
            .map_err(|e| anyhow!("Encryption failed: {}", e))?;

        let mut result = Vec::with_capacity(12 + ciphertext.len());
        result.extend_from_slice(&nonce_bytes);
        result.extend_from_slice(&ciphertext);

        Ok(result)
    }

    async fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>> {
        if ciphertext.len() < 12 {
            return Err(anyhow!("Ciphertext too short (missing nonce)"));
        }

        let nonce = Nonce::from_slice(&ciphertext[..12]);
        let encrypted_data = &ciphertext[12..];

        let plaintext = self
            .cipher
            .decrypt(nonce, encrypted_data)
            .map_err(|e| anyhow!("Decryption failed: {}", e))?;

        Ok(plaintext)
    }

    fn scheme_name(&self) -> &str {
        "aes-256-gcm"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aes_gcm_provider() {
        use futures::executor::block_on;

        let provider = AesGcmProvider::new_random();
        assert_eq!(provider.scheme_name(), "aes-256-gcm");

        let data = b"Secret message for encryption";
        let encrypted = block_on(provider.encrypt(data)).unwrap();

        assert_ne!(&encrypted[..], data);
        assert!(encrypted.len() > data.len());

        let decrypted = block_on(provider.decrypt(&encrypted)).unwrap();
        assert_eq!(&decrypted[..], data);
    }

    #[test]
    fn test_aes_gcm_different_nonces() {
        use futures::executor::block_on;

        let provider = AesGcmProvider::new_random();
        let data = b"Same data, different nonces";

        let encrypted1 = block_on(provider.encrypt(data)).unwrap();
        let encrypted2 = block_on(provider.encrypt(data)).unwrap();

        assert_ne!(encrypted1, encrypted2);

        let decrypted1 = block_on(provider.decrypt(&encrypted1)).unwrap();
        let decrypted2 = block_on(provider.decrypt(&encrypted2)).unwrap();

        assert_eq!(&decrypted1[..], data);
        assert_eq!(&decrypted2[..], data);
    }

    #[test]
    fn test_aes_gcm_from_password() {
        use futures::executor::block_on;

        let password = b"my-secret-password";
        let salt = b"random-salt-12345";

        let provider1 = AesGcmProvider::from_password(password, salt).unwrap();
        let provider2 = AesGcmProvider::from_password(password, salt).unwrap();

        let data = b"Encrypted with derived key";
        let encrypted = block_on(provider1.encrypt(data)).unwrap();
        let decrypted = block_on(provider2.decrypt(&encrypted)).unwrap();

        assert_eq!(&decrypted[..], data);
    }
}
