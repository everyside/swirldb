// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

/// Binary WebSocket protocol for SwirlDB sync
///
/// Wire format is designed for minimal overhead with length-prefixed messages.
/// All multi-byte integers are big-endian (network byte order).
use anyhow::{anyhow, Context, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Maximum number of updates allowed in a single EphemeralBatch or EphemeralRelay message.
/// Prevents memory exhaustion from malicious or malformed messages.
const MAX_EPHEMERAL_BATCH_SIZE: usize = 10000;

/// Maximum allowed string length in protocol messages (10 MB).
const MAX_STRING_LENGTH: usize = 10_000_000;

/// Maximum allowed byte array length in protocol messages (100 MB).
const MAX_BYTES_LENGTH: usize = 100_000_000;

/// Maximum number of changes in a single message.
const MAX_CHANGES_COUNT: usize = 100_000;

/// Maximum number of strings in a string array field.
const MAX_STRING_ARRAY_COUNT: usize = 100_000;

/// The document a message is about when it names none.
///
/// A server holds many documents, each an Automerge history of its own,
/// synced whole. Every message that touches a document carries its id. A
/// client that never asks for a document — the chat demo, the single-document
/// tests, a peer server — works on this one, so nothing written before
/// multi-document had to change.
pub const DEFAULT_DOCUMENT: &str = "default";

/// What a subject may do with a document it has opened.
///
/// Decided by the server's authority when the document is opened and reported
/// back in `SubscribeAck`. A reader receives the document and every broadcast
/// on it, and may send ephemeral messages (presence is not a write), but a
/// `Push` from a reader is refused.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Access {
    Read = 0x01,
    Write = 0x02,
}

impl Access {
    pub fn may_write(self) -> bool {
        self == Access::Write
    }
}

impl TryFrom<u8> for Access {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self> {
        Ok(match value {
            0x01 => Self::Read,
            0x02 => Self::Write,
            _ => return Err(anyhow!("Unknown access: 0x{:02x}", value)),
        })
    }
}

/// Message type constants (must match client implementation)
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    Connect = 0x01,
    Sync = 0x02,
    Push = 0x03,
    Broadcast = 0x04,
    PushAck = 0x05,
    SubscribeAck = 0x06,
    Subscribe = 0x07,
    Open = 0x08,
    Close = 0x09,
    OpenDenied = 0x0A,
    Ping = 0x10,
    Pong = 0x11,
    Ephemeral = 0x20,
    EphemeralBatch = 0x21,
    EphemeralRelay = 0x25,
    Error = 0xFF,
}

impl TryFrom<u8> for MessageType {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self> {
        Ok(match value {
            0x01 => Self::Connect,
            0x02 => Self::Sync,
            0x03 => Self::Push,
            0x04 => Self::Broadcast,
            0x05 => Self::PushAck,
            0x06 => Self::SubscribeAck,
            0x07 => Self::Subscribe,
            0x08 => Self::Open,
            0x09 => Self::Close,
            0x0A => Self::OpenDenied,
            0x10 => Self::Ping,
            0x11 => Self::Pong,
            0x20 => Self::Ephemeral,
            0x21 => Self::EphemeralBatch,
            0x25 => Self::EphemeralRelay,
            0xFF => Self::Error,
            _ => return Err(anyhow!("Unknown message type: 0x{:02x}", value)),
        })
    }
}

/// Protocol messages for SwirlDB subscription-based sync.
///
/// All messages are encoded in a compact binary format with a 1-byte type tag
/// followed by type-specific fields. Strings and byte arrays are length-prefixed
/// with u32 (big-endian). See [`MessageType`] for type tag values.
///
/// # Documents
///
/// Every message that touches a document names it. The id is the last field
/// of each such message on the wire, and a decoder that runs out of bytes
/// before reaching it reads [`DEFAULT_DOCUMENT`]. That is what lets a frame
/// from before multi-document — a prebuilt browser module, a peer server not
/// yet rebuilt — still work on the default document, and it is why the field
/// is last rather than first.
///
/// # Message Flow
///
/// ## Connection handshake
/// ```text
/// Client → Server: Connect { client_id, subscriptions, heads, document }
/// Server → Client: SubscribeAck { added, denied, document, access }
/// Server → Client: Sync { heads, changes, document }
/// ```
/// `Connect` opens the document it names; the connection then carries it.
/// Further documents on the same connection use `Open`, answered by the same
/// `SubscribeAck` + `Sync` pair, or by `OpenDenied` when the server's
/// authority refuses. `Close` drops one document without closing the socket.
///
/// ## CRDT sync
/// ```text
/// Client → Server: Push { heads, changes, document }
/// Server → Client: PushAck { heads, document }
/// Server → Others: Broadcast { from_client_id, changes, affected_paths, document }
/// ```
///
/// ## Ephemeral pub/sub (bypasses CRDT/storage)
/// ```text
/// Client → Server: Ephemeral { path, data, document }
/// Client → Server: EphemeralBatch { updates, document }
/// Server → Subscribers: EphemeralBatch { updates, document }
/// ```
#[derive(Debug, Clone)]
pub enum Message {
    /// Initial connection from client to server.
    /// The client provides its ID, desired subscription patterns, and CRDT
    /// heads, and names the first document it opens.
    Connect {
        client_id: String,
        /// Path patterns to subscribe to (e.g., `["user.**", "chat.**"]`)
        subscriptions: Vec<String>,
        /// Client's current CRDT heads (empty for full sync)
        heads: Vec<u8>,
        /// The document this connection opens first
        document: String,
    },
    /// Open another document on an already connected socket.
    /// Answered with `SubscribeAck` and `Sync` for that document, or
    /// `OpenDenied`.
    Open {
        document: String,
        /// Path patterns to subscribe to within the document
        subscriptions: Vec<String>,
        /// Client's current heads for this document (empty for full sync)
        heads: Vec<u8>,
    },
    /// Stop receiving a document. The socket stays open for the others.
    Close { document: String },
    /// The server's authority refused to open the document for this subject.
    OpenDenied { document: String, reason: String },
    /// Server sends current state to client after connection.
    /// Contains the server's heads and either all changes (full sync) or
    /// only changes since the client's heads (delta sync).
    Sync {
        /// Server's current CRDT heads
        heads: Vec<u8>,
        changes: Vec<Vec<u8>>,
        document: String,
    },
    /// Dynamic subscription update (add/remove patterns mid-connection).
    Subscribe {
        /// Subscription patterns to add
        add: Vec<String>,
        /// Subscription patterns to remove
        remove: Vec<String>,
        document: String,
    },
    /// Server acknowledges a `Connect`, `Open` or `Subscribe`, reporting which
    /// patterns were accepted and which were denied by the policy engine, and
    /// what the subject may do with the document.
    SubscribeAck {
        /// Successfully added subscription patterns
        added: Vec<String>,
        /// Subscription patterns denied by policy
        denied: Vec<String>,
        document: String,
        access: Access,
    },
    /// Client pushes CRDT changes to the server.
    Push {
        /// Client's current CRDT heads (so server knows what client has)
        heads: Vec<u8>,
        changes: Vec<Vec<u8>>,
        document: String,
    },
    /// Server broadcasts CRDT changes to subscribers.
    /// Sent to all clients whose subscriptions match the affected paths.
    Broadcast {
        from_client_id: String,
        changes: Vec<Vec<u8>>,
        /// Dot-notation paths modified by these changes
        affected_paths: Vec<String>,
        document: String,
    },
    /// Server acknowledges a Push, returning its updated heads.
    PushAck {
        /// Server's new CRDT heads after applying the pushed changes
        heads: Vec<u8>,
        document: String,
    },
    /// Single ephemeral message (bypasses CRDT and storage).
    /// Used for high-frequency real-time data like cursor positions,
    /// DMX lighting values, or beat sync data.
    Ephemeral {
        /// Dot-notation path that determines subscriber routing
        path: String,
        /// Arbitrary binary payload
        data: Vec<u8>,
        document: String,
    },
    /// Batch of ephemeral messages (more efficient than individual sends).
    /// Limited to [`MAX_EPHEMERAL_BATCH_SIZE`] updates per message.
    EphemeralBatch {
        /// List of (path, data) updates
        updates: Vec<(String, Vec<u8>)>,
        document: String,
    },
    /// Server-to-server ephemeral relay with loop prevention.
    /// Carries provenance information to prevent infinite relay loops
    /// and duplicate processing in multi-server topologies.
    EphemeralRelay {
        /// Original sender's server ID
        origin: String,
        /// Monotonically increasing sequence number for dedup
        seq: u64,
        /// Servers this message has passed through (loop prevention)
        path_through: Vec<String>,
        /// The ephemeral updates to relay
        updates: Vec<(String, Vec<u8>)>,
        document: String,
    },
    /// Heartbeat ping (server → client or client → server).
    Ping,
    /// Heartbeat pong (response to Ping).
    Pong,
    /// Error message from server to client.
    Error { message: String },
}

impl Message {
    /// Decode a message from binary
    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Err(anyhow!("Empty message"));
        }

        let mut buf = Bytes::copy_from_slice(data);
        let msg_type = MessageType::try_from(buf.get_u8())?;

        match msg_type {
            MessageType::Connect => {
                let client_id = read_string(&mut buf)?;
                let subscriptions = read_string_array(&mut buf)?;
                let heads = if buf.remaining() > 0 {
                    read_bytes(&mut buf)?
                } else {
                    Vec::new()
                };
                let document = read_trailing_document(&mut buf)?;

                Ok(Self::Connect {
                    client_id,
                    subscriptions,
                    heads,
                    document,
                })
            }

            MessageType::Open => {
                let document = read_string(&mut buf)?;
                let subscriptions = read_string_array(&mut buf)?;
                let heads = read_bytes(&mut buf)?;
                Ok(Self::Open {
                    document,
                    subscriptions,
                    heads,
                })
            }

            MessageType::Close => {
                let document = read_string(&mut buf)?;
                Ok(Self::Close { document })
            }

            MessageType::OpenDenied => {
                let document = read_string(&mut buf)?;
                let reason = read_string(&mut buf)?;
                Ok(Self::OpenDenied { document, reason })
            }

            MessageType::Push => {
                let heads = read_bytes(&mut buf)?;
                let changes = read_changes(&mut buf)?;
                let document = read_trailing_document(&mut buf)?;
                Ok(Self::Push {
                    heads,
                    changes,
                    document,
                })
            }

            MessageType::Sync => {
                let heads = read_bytes(&mut buf)?;
                let changes = read_changes(&mut buf)?;
                let document = read_trailing_document(&mut buf)?;
                Ok(Self::Sync {
                    heads,
                    changes,
                    document,
                })
            }

            MessageType::Subscribe => {
                let add = read_string_array(&mut buf)?;
                let remove = read_string_array(&mut buf)?;
                let document = read_trailing_document(&mut buf)?;
                Ok(Self::Subscribe {
                    add,
                    remove,
                    document,
                })
            }

            MessageType::SubscribeAck => {
                let added = read_string_array(&mut buf)?;
                let denied = read_string_array(&mut buf)?;
                let document = read_trailing_document(&mut buf)?;
                // Before multi-document every subscriber could write, so a
                // frame without the field means Write.
                let access = if buf.remaining() > 0 {
                    Access::try_from(buf.get_u8())?
                } else {
                    Access::Write
                };
                Ok(Self::SubscribeAck {
                    added,
                    denied,
                    document,
                    access,
                })
            }

            MessageType::Broadcast => {
                let from_client_id = read_string(&mut buf)?;
                let changes = read_changes(&mut buf)?;
                let affected_paths = read_string_array(&mut buf)?;
                let document = read_trailing_document(&mut buf)?;
                Ok(Self::Broadcast {
                    from_client_id,
                    changes,
                    affected_paths,
                    document,
                })
            }

            MessageType::PushAck => {
                let heads = read_bytes(&mut buf)?;
                let document = read_trailing_document(&mut buf)?;
                Ok(Self::PushAck { heads, document })
            }

            MessageType::Error => {
                let message = read_string(&mut buf)?;
                Ok(Self::Error { message })
            }

            MessageType::Ephemeral => {
                let path = read_string(&mut buf)?;
                let data = read_bytes(&mut buf)?;
                let document = read_trailing_document(&mut buf)?;
                Ok(Self::Ephemeral {
                    path,
                    data,
                    document,
                })
            }

            MessageType::EphemeralBatch => {
                let updates = read_updates(&mut buf, "EphemeralBatch")?;
                let document = read_trailing_document(&mut buf)?;
                Ok(Self::EphemeralBatch { updates, document })
            }

            MessageType::EphemeralRelay => {
                let origin = read_string(&mut buf)?;
                if buf.remaining() < 8 {
                    return Err(anyhow!("Not enough data for EphemeralRelay seq"));
                }
                let seq = buf.get_u64();
                let path_through = read_string_array(&mut buf)?;
                let updates = read_updates(&mut buf, "EphemeralRelay")?;
                let document = read_trailing_document(&mut buf)?;
                Ok(Self::EphemeralRelay {
                    origin,
                    seq,
                    path_through,
                    updates,
                    document,
                })
            }

            MessageType::Ping => Ok(Self::Ping),

            MessageType::Pong => Ok(Self::Pong),
        }
    }

    /// Encode a message to binary
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = BytesMut::new();

        match self {
            Self::Connect {
                client_id,
                subscriptions,
                heads,
                document,
            } => {
                buf.put_u8(MessageType::Connect as u8);
                write_string(&mut buf, client_id);
                write_string_array(&mut buf, subscriptions);
                write_bytes(&mut buf, heads);
                write_string(&mut buf, document);
            }

            Self::Open {
                document,
                subscriptions,
                heads,
            } => {
                buf.put_u8(MessageType::Open as u8);
                write_string(&mut buf, document);
                write_string_array(&mut buf, subscriptions);
                write_bytes(&mut buf, heads);
            }

            Self::Close { document } => {
                buf.put_u8(MessageType::Close as u8);
                write_string(&mut buf, document);
            }

            Self::OpenDenied { document, reason } => {
                buf.put_u8(MessageType::OpenDenied as u8);
                write_string(&mut buf, document);
                write_string(&mut buf, reason);
            }

            Self::Sync {
                heads,
                changes,
                document,
            } => {
                buf.put_u8(MessageType::Sync as u8);
                write_bytes(&mut buf, heads);
                write_changes(&mut buf, changes);
                write_string(&mut buf, document);
            }

            Self::Subscribe {
                add,
                remove,
                document,
            } => {
                buf.put_u8(MessageType::Subscribe as u8);
                write_string_array(&mut buf, add);
                write_string_array(&mut buf, remove);
                write_string(&mut buf, document);
            }

            Self::SubscribeAck {
                added,
                denied,
                document,
                access,
            } => {
                buf.put_u8(MessageType::SubscribeAck as u8);
                write_string_array(&mut buf, added);
                write_string_array(&mut buf, denied);
                write_string(&mut buf, document);
                buf.put_u8(*access as u8);
            }

            Self::Push {
                heads,
                changes,
                document,
            } => {
                buf.put_u8(MessageType::Push as u8);
                write_bytes(&mut buf, heads);
                write_changes(&mut buf, changes);
                write_string(&mut buf, document);
            }

            Self::Broadcast {
                from_client_id,
                changes,
                affected_paths,
                document,
            } => {
                buf.put_u8(MessageType::Broadcast as u8);
                write_string(&mut buf, from_client_id);
                write_changes(&mut buf, changes);
                write_string_array(&mut buf, affected_paths);
                write_string(&mut buf, document);
            }

            Self::PushAck { heads, document } => {
                buf.put_u8(MessageType::PushAck as u8);
                write_bytes(&mut buf, heads);
                write_string(&mut buf, document);
            }

            Self::Ephemeral {
                path,
                data,
                document,
            } => {
                buf.put_u8(MessageType::Ephemeral as u8);
                write_string(&mut buf, path);
                write_bytes(&mut buf, data);
                write_string(&mut buf, document);
            }

            Self::EphemeralBatch { updates, document } => {
                buf.put_u8(MessageType::EphemeralBatch as u8);
                write_updates(&mut buf, updates);
                write_string(&mut buf, document);
            }

            Self::EphemeralRelay {
                origin,
                seq,
                path_through,
                updates,
                document,
            } => {
                buf.put_u8(MessageType::EphemeralRelay as u8);
                write_string(&mut buf, origin);
                buf.put_u64(*seq);
                write_string_array(&mut buf, path_through);
                write_updates(&mut buf, updates);
                write_string(&mut buf, document);
            }

            Self::Ping => {
                buf.put_u8(MessageType::Ping as u8);
            }

            Self::Pong => {
                buf.put_u8(MessageType::Pong as u8);
            }

            Self::Error { message } => {
                buf.put_u8(MessageType::Error as u8);
                write_string(&mut buf, message);
            }
        }

        buf.to_vec()
    }
}

// Helper functions for reading/writing length-prefixed data

fn read_string(buf: &mut Bytes) -> Result<String> {
    if buf.remaining() < 4 {
        return Err(anyhow!("Not enough data for string length"));
    }
    let len = buf.get_u32() as usize;
    if len > MAX_STRING_LENGTH {
        return Err(anyhow!(
            "String length {} exceeds maximum {}",
            len,
            MAX_STRING_LENGTH
        ));
    }
    if buf.remaining() < len {
        return Err(anyhow!("Not enough data for string"));
    }

    let bytes = buf.copy_to_bytes(len);
    String::from_utf8(bytes.to_vec()).context("Invalid UTF-8")
}

fn write_string(buf: &mut BytesMut, s: &str) {
    let bytes = s.as_bytes();
    buf.put_u32(bytes.len() as u32);
    buf.put_slice(bytes);
}

fn read_bytes(buf: &mut Bytes) -> Result<Vec<u8>> {
    if buf.remaining() < 4 {
        return Err(anyhow!("Not enough data for bytes length"));
    }
    let len = buf.get_u32() as usize;
    if len > MAX_BYTES_LENGTH {
        return Err(anyhow!(
            "Bytes length {} exceeds maximum {}",
            len,
            MAX_BYTES_LENGTH
        ));
    }
    if buf.remaining() < len {
        return Err(anyhow!("Not enough data for bytes"));
    }

    Ok(buf.copy_to_bytes(len).to_vec())
}

fn write_bytes(buf: &mut BytesMut, data: &[u8]) {
    buf.put_u32(data.len() as u32);
    buf.put_slice(data);
}

fn read_changes(buf: &mut Bytes) -> Result<Vec<Vec<u8>>> {
    if buf.remaining() < 4 {
        return Err(anyhow!("Not enough data for changes count"));
    }
    let count = buf.get_u32() as usize;
    if count > MAX_CHANGES_COUNT {
        return Err(anyhow!(
            "Changes count {} exceeds maximum {}",
            count,
            MAX_CHANGES_COUNT
        ));
    }
    let mut changes = Vec::with_capacity(count);

    for _ in 0..count {
        changes.push(read_bytes(buf)?);
    }

    Ok(changes)
}

fn write_changes(buf: &mut BytesMut, changes: &[Vec<u8>]) {
    buf.put_u32(changes.len() as u32);
    for change in changes {
        write_bytes(buf, change);
    }
}

fn read_string_array(buf: &mut Bytes) -> Result<Vec<String>> {
    if buf.remaining() < 4 {
        return Err(anyhow!("Not enough data for string array count"));
    }
    let count = buf.get_u32() as usize;
    if count > MAX_STRING_ARRAY_COUNT {
        return Err(anyhow!(
            "String array count {} exceeds maximum {}",
            count,
            MAX_STRING_ARRAY_COUNT
        ));
    }
    let mut strings = Vec::with_capacity(count);

    for _ in 0..count {
        strings.push(read_string(buf)?);
    }

    Ok(strings)
}

fn write_string_array(buf: &mut BytesMut, strings: &[String]) {
    buf.put_u32(strings.len() as u32);
    for s in strings {
        write_string(buf, s);
    }
}

/// The document id sits last in every message that has one, so a frame
/// written before the field existed simply ends early. Ending early means
/// the default document; anything present must be a well-formed string.
fn read_trailing_document(buf: &mut Bytes) -> Result<String> {
    if buf.remaining() == 0 {
        return Ok(DEFAULT_DOCUMENT.to_string());
    }
    let document = read_string(buf)?;
    if document.is_empty() {
        return Err(anyhow!("Document id must not be empty"));
    }
    Ok(document)
}

fn read_updates(buf: &mut Bytes, message_name: &str) -> Result<Vec<(String, Vec<u8>)>> {
    if buf.remaining() < 4 {
        return Err(anyhow!("Not enough data for {} count", message_name));
    }
    let count = buf.get_u32() as usize;
    if count > MAX_EPHEMERAL_BATCH_SIZE {
        return Err(anyhow!(
            "{} count {} exceeds maximum {}",
            message_name,
            count,
            MAX_EPHEMERAL_BATCH_SIZE
        ));
    }
    let mut updates = Vec::with_capacity(count);
    for _ in 0..count {
        let path = read_string(buf)?;
        let data = read_bytes(buf)?;
        updates.push((path, data));
    }
    Ok(updates)
}

fn write_updates(buf: &mut BytesMut, updates: &[(String, Vec<u8>)]) {
    buf.put_u32(updates.len() as u32);
    for (path, data) in updates {
        write_string(buf, path);
        write_bytes(buf, data);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_document() -> String {
        DEFAULT_DOCUMENT.to_string()
    }

    #[test]
    fn test_connect_message() {
        let msg = Message::Connect {
            client_id: "alice".to_string(),
            subscriptions: vec!["user.alice.**".to_string(), "public.**".to_string()],
            heads: vec![1, 2, 3],
            document: "pattern-7".to_string(),
        };

        let encoded = msg.encode();
        let decoded = Message::decode(&encoded).unwrap();

        match decoded {
            Message::Connect {
                client_id,
                subscriptions,
                heads,
                document,
            } => {
                assert_eq!(client_id, "alice");
                assert_eq!(subscriptions.len(), 2);
                assert_eq!(subscriptions[0], "user.alice.**");
                assert_eq!(subscriptions[1], "public.**");
                assert_eq!(heads, vec![1, 2, 3]);
                assert_eq!(document, "pattern-7");
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_open_close_denied_messages() {
        let open = Message::Open {
            document: "palette-3".to_string(),
            subscriptions: vec!["**".to_string()],
            heads: vec![9; 32],
        };
        match Message::decode(&open.encode()).unwrap() {
            Message::Open {
                document,
                subscriptions,
                heads,
            } => {
                assert_eq!(document, "palette-3");
                assert_eq!(subscriptions, vec!["**".to_string()]);
                assert_eq!(heads, vec![9; 32]);
            }
            _ => panic!("Wrong message type"),
        }

        let close = Message::Close {
            document: "palette-3".to_string(),
        };
        match Message::decode(&close.encode()).unwrap() {
            Message::Close { document } => assert_eq!(document, "palette-3"),
            _ => panic!("Wrong message type"),
        }

        let denied = Message::OpenDenied {
            document: "secret".to_string(),
            reason: "not a member".to_string(),
        };
        match Message::decode(&denied.encode()).unwrap() {
            Message::OpenDenied { document, reason } => {
                assert_eq!(document, "secret");
                assert_eq!(reason, "not a member");
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_ephemeral_message() {
        let msg = Message::Ephemeral {
            path: "fixtures.1.color".to_string(),
            data: vec![255, 0, 128, 255],
            document: "show-1".to_string(),
        };

        let encoded = msg.encode();
        let decoded = Message::decode(&encoded).unwrap();

        match decoded {
            Message::Ephemeral {
                path,
                data,
                document,
            } => {
                assert_eq!(path, "fixtures.1.color");
                assert_eq!(data, vec![255, 0, 128, 255]);
                assert_eq!(document, "show-1");
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_ephemeral_batch_message() {
        let msg = Message::EphemeralBatch {
            updates: vec![
                ("fixtures.1.color".to_string(), vec![255, 0, 0]),
                ("fixtures.2.color".to_string(), vec![0, 255, 0]),
                ("beat.bpm".to_string(), vec![0, 0, 0, 120]),
            ],
            document: default_document(),
        };

        let encoded = msg.encode();
        let decoded = Message::decode(&encoded).unwrap();

        match decoded {
            Message::EphemeralBatch { updates, document } => {
                assert_eq!(updates.len(), 3);
                assert_eq!(updates[0].0, "fixtures.1.color");
                assert_eq!(updates[0].1, vec![255, 0, 0]);
                assert_eq!(updates[1].0, "fixtures.2.color");
                assert_eq!(updates[1].1, vec![0, 255, 0]);
                assert_eq!(updates[2].0, "beat.bpm");
                assert_eq!(updates[2].1, vec![0, 0, 0, 120]);
                assert_eq!(document, DEFAULT_DOCUMENT);
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_ephemeral_batch_empty() {
        let msg = Message::EphemeralBatch {
            updates: vec![],
            document: default_document(),
        };

        let encoded = msg.encode();
        let decoded = Message::decode(&encoded).unwrap();

        match decoded {
            Message::EphemeralBatch { updates, .. } => {
                assert!(updates.is_empty());
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_ephemeral_relay_message() {
        let msg = Message::EphemeralRelay {
            origin: "server-1".to_string(),
            seq: 42,
            path_through: vec!["server-1".to_string(), "server-2".to_string()],
            updates: vec![
                ("fixtures.1.color".to_string(), vec![255, 0, 0]),
                ("beat.bpm".to_string(), vec![0, 120]),
            ],
            document: default_document(),
        };

        let encoded = msg.encode();
        let decoded = Message::decode(&encoded).unwrap();

        match decoded {
            Message::EphemeralRelay {
                origin,
                seq,
                path_through,
                updates,
                document,
            } => {
                assert_eq!(origin, "server-1");
                assert_eq!(seq, 42);
                assert_eq!(path_through.len(), 2);
                assert_eq!(path_through[0], "server-1");
                assert_eq!(path_through[1], "server-2");
                assert_eq!(updates.len(), 2);
                assert_eq!(updates[0].0, "fixtures.1.color");
                assert_eq!(updates[1].0, "beat.bpm");
                assert_eq!(document, DEFAULT_DOCUMENT);
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_subscribe_message() {
        let msg = Message::Subscribe {
            add: vec!["user.**".to_string(), "chat.**".to_string()],
            remove: vec!["old.topic.**".to_string()],
            document: default_document(),
        };

        let encoded = msg.encode();
        let decoded = Message::decode(&encoded).unwrap();

        match decoded {
            Message::Subscribe { add, remove, .. } => {
                assert_eq!(add.len(), 2);
                assert_eq!(add[0], "user.**");
                assert_eq!(add[1], "chat.**");
                assert_eq!(remove.len(), 1);
                assert_eq!(remove[0], "old.topic.**");
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_subscribe_ack_message() {
        let msg = Message::SubscribeAck {
            added: vec!["user.**".to_string()],
            denied: vec!["admin.**".to_string()],
            document: "pattern-7".to_string(),
            access: Access::Read,
        };

        let encoded = msg.encode();
        let decoded = Message::decode(&encoded).unwrap();

        match decoded {
            Message::SubscribeAck {
                added,
                denied,
                document,
                access,
            } => {
                assert_eq!(added.len(), 1);
                assert_eq!(added[0], "user.**");
                assert_eq!(denied.len(), 1);
                assert_eq!(denied[0], "admin.**");
                assert_eq!(document, "pattern-7");
                assert_eq!(access, Access::Read);
                assert!(!access.may_write());
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_push_message() {
        let msg = Message::Push {
            heads: vec![1, 2, 3, 4],
            changes: vec![vec![1, 2, 3], vec![4, 5, 6]],
            document: "pattern-7".to_string(),
        };

        let encoded = msg.encode();
        let decoded = Message::decode(&encoded).unwrap();

        match decoded {
            Message::Push {
                heads,
                changes,
                document,
            } => {
                assert_eq!(heads, vec![1, 2, 3, 4]);
                assert_eq!(changes.len(), 2);
                assert_eq!(changes[0], vec![1, 2, 3]);
                assert_eq!(changes[1], vec![4, 5, 6]);
                assert_eq!(document, "pattern-7");
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_sync_message() {
        let msg = Message::Sync {
            heads: vec![10, 20, 30],
            changes: vec![vec![1, 2], vec![3, 4, 5]],
            document: default_document(),
        };

        let encoded = msg.encode();
        let decoded = Message::decode(&encoded).unwrap();

        match decoded {
            Message::Sync {
                heads,
                changes,
                document,
            } => {
                assert_eq!(heads, vec![10, 20, 30]);
                assert_eq!(changes.len(), 2);
                assert_eq!(changes[0], vec![1, 2]);
                assert_eq!(changes[1], vec![3, 4, 5]);
                assert_eq!(document, DEFAULT_DOCUMENT);
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_broadcast_message() {
        let msg = Message::Broadcast {
            from_client_id: "client-1".to_string(),
            changes: vec![vec![42]],
            affected_paths: vec!["user.name".to_string(), "user.email".to_string()],
            document: "pattern-7".to_string(),
        };

        let encoded = msg.encode();
        let decoded = Message::decode(&encoded).unwrap();

        match decoded {
            Message::Broadcast {
                from_client_id,
                changes,
                affected_paths,
                document,
            } => {
                assert_eq!(from_client_id, "client-1");
                assert_eq!(changes.len(), 1);
                assert_eq!(changes[0], vec![42]);
                assert_eq!(affected_paths.len(), 2);
                assert_eq!(affected_paths[0], "user.name");
                assert_eq!(affected_paths[1], "user.email");
                assert_eq!(document, "pattern-7");
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_push_ack_message() {
        let msg = Message::PushAck {
            heads: vec![5, 6, 7],
            document: default_document(),
        };

        let encoded = msg.encode();
        let decoded = Message::decode(&encoded).unwrap();

        match decoded {
            Message::PushAck { heads, document } => {
                assert_eq!(heads, vec![5, 6, 7]);
                assert_eq!(document, DEFAULT_DOCUMENT);
            }
            _ => panic!("Wrong message type"),
        }
    }

    /// A frame written before the document field existed ends where the
    /// field would begin. It must decode as the default document, with
    /// write access, so a module built before multi-document still works.
    #[test]
    fn test_frames_without_a_document_decode_as_the_default() {
        let mut connect = BytesMut::new();
        connect.put_u8(MessageType::Connect as u8);
        write_string(&mut connect, "alice");
        write_string_array(&mut connect, &["**".to_string()]);
        write_bytes(&mut connect, &[]);
        match Message::decode(&connect).unwrap() {
            Message::Connect { document, .. } => assert_eq!(document, DEFAULT_DOCUMENT),
            _ => panic!("Wrong message type"),
        }

        let mut push = BytesMut::new();
        push.put_u8(MessageType::Push as u8);
        write_bytes(&mut push, &[]);
        write_changes(&mut push, &[vec![1, 2, 3]]);
        match Message::decode(&push).unwrap() {
            Message::Push {
                document, changes, ..
            } => {
                assert_eq!(document, DEFAULT_DOCUMENT);
                assert_eq!(changes, vec![vec![1, 2, 3]]);
            }
            _ => panic!("Wrong message type"),
        }

        let mut ack = BytesMut::new();
        ack.put_u8(MessageType::SubscribeAck as u8);
        write_string_array(&mut ack, &["**".to_string()]);
        write_string_array(&mut ack, &[]);
        match Message::decode(&ack).unwrap() {
            Message::SubscribeAck {
                document, access, ..
            } => {
                assert_eq!(document, DEFAULT_DOCUMENT);
                assert_eq!(access, Access::Write);
            }
            _ => panic!("Wrong message type"),
        }

        let mut batch = BytesMut::new();
        batch.put_u8(MessageType::EphemeralBatch as u8);
        batch.put_u32(1);
        write_string(&mut batch, "cursor.alice");
        write_bytes(&mut batch, &[7]);
        match Message::decode(&batch).unwrap() {
            Message::EphemeralBatch { document, updates } => {
                assert_eq!(document, DEFAULT_DOCUMENT);
                assert_eq!(updates[0].0, "cursor.alice");
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_empty_document_id_is_refused() {
        let mut push = BytesMut::new();
        push.put_u8(MessageType::Push as u8);
        write_bytes(&mut push, &[]);
        write_changes(&mut push, &[]);
        write_string(&mut push, "");
        assert!(Message::decode(&push).is_err());
    }

    #[test]
    fn test_error_message() {
        let msg = Message::Error {
            message: "something went wrong".to_string(),
        };

        let encoded = msg.encode();
        let decoded = Message::decode(&encoded).unwrap();

        match decoded {
            Message::Error { message } => {
                assert_eq!(message, "something went wrong");
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_ping_pong_messages() {
        let ping = Message::Ping;
        let pong = Message::Pong;

        let ping_decoded = Message::decode(&ping.encode()).unwrap();
        let pong_decoded = Message::decode(&pong.encode()).unwrap();

        assert!(matches!(ping_decoded, Message::Ping));
        assert!(matches!(pong_decoded, Message::Pong));
    }

    #[test]
    fn test_empty_message_error() {
        let result = Message::decode(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_unknown_message_type_error() {
        let result = Message::decode(&[0xFE]);
        assert!(result.is_err());
    }

    #[test]
    fn test_ephemeral_batch_max_count_validation() {
        // Construct a batch with count claiming 10001 items but no actual data
        let mut buf = BytesMut::new();
        buf.put_u8(MessageType::EphemeralBatch as u8);
        buf.put_u32(10001); // count exceeds MAX_EPHEMERAL_BATCH_SIZE
        let result = Message::decode(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_truncated_ephemeral_batch() {
        // EphemeralBatch type byte with no count field
        let result = Message::decode(&[MessageType::EphemeralBatch as u8]);
        assert!(result.is_err());

        // EphemeralBatch with partial count (only 2 bytes instead of 4)
        let mut buf = BytesMut::new();
        buf.put_u8(MessageType::EphemeralBatch as u8);
        buf.put_u16(0);
        let result = Message::decode(&buf);
        assert!(result.is_err());

        // EphemeralRelay type byte with no data
        let result = Message::decode(&[MessageType::EphemeralRelay as u8]);
        assert!(result.is_err());

        // Truncated string (length says 100 but only 3 bytes available)
        let mut buf = BytesMut::new();
        buf.put_u8(MessageType::Error as u8);
        buf.put_u32(100);
        buf.put_slice(&[0x41, 0x42, 0x43]); // only 3 bytes
        let result = Message::decode(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_oversized_string() {
        // String with length exceeding MAX_STRING_LENGTH
        let mut buf = BytesMut::new();
        buf.put_u8(MessageType::Error as u8);
        buf.put_u32(10_000_001); // exceeds 10MB limit
        let result = Message::decode(&buf);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("exceeds maximum"));
    }

    #[test]
    fn test_oversized_changes_count() {
        // Changes count exceeding MAX_CHANGES_COUNT
        let mut buf = BytesMut::new();
        buf.put_u8(MessageType::Sync as u8);
        buf.put_u32(0); // empty heads
        buf.put_u32(100_001); // exceeds 100K limit
        let result = Message::decode(&buf);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("exceeds maximum"));
    }
}
