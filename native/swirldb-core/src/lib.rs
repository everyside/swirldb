// Core module - always available
pub mod core;

// Storage adapters module
pub mod storage;

// Sync protocol module
pub mod sync;

// Re-export automerge types for convenience
pub use automerge;

// Browser WASM bindings - only when wasm feature is enabled
#[cfg(feature = "wasm")]
mod browser;

#[cfg(feature = "wasm")]
pub use browser::SwirlDB;

// For non-WASM targets, re-export the core directly
#[cfg(not(feature = "wasm"))]
pub use core::SwirlDB;
