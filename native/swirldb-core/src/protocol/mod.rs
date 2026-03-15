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
/// # Message Flow
///
/// ## Connection handshake
/// ```text
/// Client → Server: Connect { client_id, subscriptions, heads }
/// Server → Client: SubscribeAck { added, denied }
/// Server → Client: Sync { heads, changes }
/// ```
///
/// ## CRDT sync
/// ```text
/// Client → Server: Push { heads, changes }
/// Server → Client: PushAck { heads }
/// Server → Others: Broadcast { from_client_id, changes, affected_paths }
/// ```
///
/// ## Ephemeral pub/sub (bypasses CRDT/storage)
/// ```text
/// Client → Server: Ephemeral { path, data }
/// Client → Server: EphemeralBatch { updates }
/// Server → Subscribers: EphemeralBatch { updates }
/// ```
#[derive(Debug, Clone)]
pub enum Message {
    /// Initial connection from client to server.
    /// The client provides its ID, desired subscription patterns, and CRDT heads.
    Connect {
        client_id: String,
        /// Path patterns to subscribe to (e.g., `["user.**", "chat.**"]`)
        subscriptions: Vec<String>,
        /// Client's current CRDT heads (empty for full sync)
        heads: Vec<u8>,
    },
    /// Server sends current state to client after connection.
    /// Contains the server's heads and either all changes (full sync) or
    /// only changes since the client's heads (delta sync).
    Sync {
        /// Server's current CRDT heads
        heads: Vec<u8>,
        changes: Vec<Vec<u8>>,
    },
    /// Dynamic subscription update (add/remove patterns mid-connection).
    Subscribe {
        /// Subscription patterns to add
        add: Vec<String>,
        /// Subscription patterns to remove
        remove: Vec<String>,
    },
    /// Server acknowledges subscription request, reporting which patterns
    /// were accepted and which were denied by the policy engine.
    SubscribeAck {
        /// Successfully added subscription patterns
        added: Vec<String>,
        /// Subscription patterns denied by policy
        denied: Vec<String>,
    },
    /// Client pushes CRDT changes to the server.
    Push {
        /// Client's current CRDT heads (so server knows what client has)
        heads: Vec<u8>,
        changes: Vec<Vec<u8>>,
    },
    /// Server broadcasts CRDT changes to subscribers.
    /// Sent to all clients whose subscriptions match the affected paths.
    Broadcast {
        from_client_id: String,
        changes: Vec<Vec<u8>>,
        /// Dot-notation paths modified by these changes
        affected_paths: Vec<String>,
    },
    /// Server acknowledges a Push, returning its updated heads.
    PushAck {
        /// Server's new CRDT heads after applying the pushed changes
        heads: Vec<u8>,
    },
    /// Single ephemeral message (bypasses CRDT and storage).
    /// Used for high-frequency real-time data like cursor positions,
    /// DMX lighting values, or beat sync data.
    Ephemeral {
        /// Dot-notation path that determines subscriber routing
        path: String,
        /// Arbitrary binary payload
        data: Vec<u8>,
    },
    /// Batch of ephemeral messages (more efficient than individual sends).
    /// Limited to [`MAX_EPHEMERAL_BATCH_SIZE`] updates per message.
    EphemeralBatch {
        /// List of (path, data) updates
        updates: Vec<(String, Vec<u8>)>,
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

                Ok(Self::Connect {
                    client_id,
                    subscriptions,
                    heads,
                })
            }

            MessageType::Push => {
                let heads = read_bytes(&mut buf)?;
                let changes = read_changes(&mut buf)?;
                Ok(Self::Push { heads, changes })
            }

            MessageType::Sync => {
                let heads = read_bytes(&mut buf)?;
                let changes = read_changes(&mut buf)?;
                Ok(Self::Sync { heads, changes })
            }

            MessageType::Subscribe => {
                let add = read_string_array(&mut buf)?;
                let remove = read_string_array(&mut buf)?;
                Ok(Self::Subscribe { add, remove })
            }

            MessageType::SubscribeAck => {
                let added = read_string_array(&mut buf)?;
                let denied = read_string_array(&mut buf)?;
                Ok(Self::SubscribeAck { added, denied })
            }

            MessageType::Broadcast => {
                let from_client_id = read_string(&mut buf)?;
                let changes = read_changes(&mut buf)?;
                let affected_paths = read_string_array(&mut buf)?;
                Ok(Self::Broadcast {
                    from_client_id,
                    changes,
                    affected_paths,
                })
            }

            MessageType::PushAck => {
                let heads = read_bytes(&mut buf)?;
                Ok(Self::PushAck { heads })
            }

            MessageType::Error => {
                let message = read_string(&mut buf)?;
                Ok(Self::Error { message })
            }

            MessageType::Ephemeral => {
                let path = read_string(&mut buf)?;
                let data = read_bytes(&mut buf)?;
                Ok(Self::Ephemeral { path, data })
            }

            MessageType::EphemeralBatch => {
                if buf.remaining() < 4 {
                    return Err(anyhow!("Not enough data for EphemeralBatch count"));
                }
                let count = buf.get_u32() as usize;
                if count > MAX_EPHEMERAL_BATCH_SIZE {
                    return Err(anyhow!(
                        "EphemeralBatch count {} exceeds maximum {}",
                        count,
                        MAX_EPHEMERAL_BATCH_SIZE
                    ));
                }
                let mut updates = Vec::with_capacity(count);
                for _ in 0..count {
                    let path = read_string(&mut buf)?;
                    let data = read_bytes(&mut buf)?;
                    updates.push((path, data));
                }
                Ok(Self::EphemeralBatch { updates })
            }

            MessageType::EphemeralRelay => {
                let origin = read_string(&mut buf)?;
                if buf.remaining() < 8 {
                    return Err(anyhow!("Not enough data for EphemeralRelay seq"));
                }
                let seq = buf.get_u64();
                let path_through = read_string_array(&mut buf)?;
                if buf.remaining() < 4 {
                    return Err(anyhow!("Not enough data for EphemeralRelay count"));
                }
                let count = buf.get_u32() as usize;
                if count > MAX_EPHEMERAL_BATCH_SIZE {
                    return Err(anyhow!(
                        "EphemeralRelay count {} exceeds maximum {}",
                        count,
                        MAX_EPHEMERAL_BATCH_SIZE
                    ));
                }
                let mut updates = Vec::with_capacity(count);
                for _ in 0..count {
                    let path = read_string(&mut buf)?;
                    let data = read_bytes(&mut buf)?;
                    updates.push((path, data));
                }
                Ok(Self::EphemeralRelay {
                    origin,
                    seq,
                    path_through,
                    updates,
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
            } => {
                buf.put_u8(MessageType::Connect as u8);
                write_string(&mut buf, client_id);
                write_string_array(&mut buf, subscriptions);
                write_bytes(&mut buf, heads);
            }

            Self::Sync { heads, changes } => {
                buf.put_u8(MessageType::Sync as u8);
                write_bytes(&mut buf, heads);
                write_changes(&mut buf, changes);
            }

            Self::Subscribe { add, remove } => {
                buf.put_u8(MessageType::Subscribe as u8);
                write_string_array(&mut buf, add);
                write_string_array(&mut buf, remove);
            }

            Self::SubscribeAck { added, denied } => {
                buf.put_u8(MessageType::SubscribeAck as u8);
                write_string_array(&mut buf, added);
                write_string_array(&mut buf, denied);
            }

            Self::Push { heads, changes } => {
                buf.put_u8(MessageType::Push as u8);
                write_bytes(&mut buf, heads);
                write_changes(&mut buf, changes);
            }

            Self::Broadcast {
                from_client_id,
                changes,
                affected_paths,
            } => {
                buf.put_u8(MessageType::Broadcast as u8);
                write_string(&mut buf, from_client_id);
                write_changes(&mut buf, changes);
                write_string_array(&mut buf, affected_paths);
            }

            Self::PushAck { heads } => {
                buf.put_u8(MessageType::PushAck as u8);
                write_bytes(&mut buf, heads);
            }

            Self::Ephemeral { path, data } => {
                buf.put_u8(MessageType::Ephemeral as u8);
                write_string(&mut buf, path);
                write_bytes(&mut buf, data);
            }

            Self::EphemeralBatch { updates } => {
                buf.put_u8(MessageType::EphemeralBatch as u8);
                buf.put_u32(updates.len() as u32);
                for (path, data) in updates {
                    write_string(&mut buf, path);
                    write_bytes(&mut buf, data);
                }
            }

            Self::EphemeralRelay {
                origin,
                seq,
                path_through,
                updates,
            } => {
                buf.put_u8(MessageType::EphemeralRelay as u8);
                write_string(&mut buf, origin);
                buf.put_u64(*seq);
                write_string_array(&mut buf, path_through);
                buf.put_u32(updates.len() as u32);
                for (path, data) in updates {
                    write_string(&mut buf, path);
                    write_bytes(&mut buf, data);
                }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connect_message() {
        let msg = Message::Connect {
            client_id: "alice".to_string(),
            subscriptions: vec!["user.alice.**".to_string(), "public.**".to_string()],
            heads: vec![1, 2, 3],
        };

        let encoded = msg.encode();
        let decoded = Message::decode(&encoded).unwrap();

        match decoded {
            Message::Connect {
                client_id,
                subscriptions,
                heads,
            } => {
                assert_eq!(client_id, "alice");
                assert_eq!(subscriptions.len(), 2);
                assert_eq!(subscriptions[0], "user.alice.**");
                assert_eq!(subscriptions[1], "public.**");
                assert_eq!(heads, vec![1, 2, 3]);
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_ephemeral_message() {
        let msg = Message::Ephemeral {
            path: "fixtures.1.color".to_string(),
            data: vec![255, 0, 128, 255],
        };

        let encoded = msg.encode();
        let decoded = Message::decode(&encoded).unwrap();

        match decoded {
            Message::Ephemeral { path, data } => {
                assert_eq!(path, "fixtures.1.color");
                assert_eq!(data, vec![255, 0, 128, 255]);
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
        };

        let encoded = msg.encode();
        let decoded = Message::decode(&encoded).unwrap();

        match decoded {
            Message::EphemeralBatch { updates } => {
                assert_eq!(updates.len(), 3);
                assert_eq!(updates[0].0, "fixtures.1.color");
                assert_eq!(updates[0].1, vec![255, 0, 0]);
                assert_eq!(updates[1].0, "fixtures.2.color");
                assert_eq!(updates[1].1, vec![0, 255, 0]);
                assert_eq!(updates[2].0, "beat.bpm");
                assert_eq!(updates[2].1, vec![0, 0, 0, 120]);
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_ephemeral_batch_empty() {
        let msg = Message::EphemeralBatch { updates: vec![] };

        let encoded = msg.encode();
        let decoded = Message::decode(&encoded).unwrap();

        match decoded {
            Message::EphemeralBatch { updates } => {
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
        };

        let encoded = msg.encode();
        let decoded = Message::decode(&encoded).unwrap();

        match decoded {
            Message::EphemeralRelay {
                origin,
                seq,
                path_through,
                updates,
            } => {
                assert_eq!(origin, "server-1");
                assert_eq!(seq, 42);
                assert_eq!(path_through.len(), 2);
                assert_eq!(path_through[0], "server-1");
                assert_eq!(path_through[1], "server-2");
                assert_eq!(updates.len(), 2);
                assert_eq!(updates[0].0, "fixtures.1.color");
                assert_eq!(updates[1].0, "beat.bpm");
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_subscribe_message() {
        let msg = Message::Subscribe {
            add: vec!["user.**".to_string(), "chat.**".to_string()],
            remove: vec!["old.topic.**".to_string()],
        };

        let encoded = msg.encode();
        let decoded = Message::decode(&encoded).unwrap();

        match decoded {
            Message::Subscribe { add, remove } => {
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
        };

        let encoded = msg.encode();
        let decoded = Message::decode(&encoded).unwrap();

        match decoded {
            Message::SubscribeAck { added, denied } => {
                assert_eq!(added.len(), 1);
                assert_eq!(added[0], "user.**");
                assert_eq!(denied.len(), 1);
                assert_eq!(denied[0], "admin.**");
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_push_message() {
        let msg = Message::Push {
            heads: vec![1, 2, 3, 4],
            changes: vec![vec![1, 2, 3], vec![4, 5, 6]],
        };

        let encoded = msg.encode();
        let decoded = Message::decode(&encoded).unwrap();

        match decoded {
            Message::Push { heads, changes } => {
                assert_eq!(heads, vec![1, 2, 3, 4]);
                assert_eq!(changes.len(), 2);
                assert_eq!(changes[0], vec![1, 2, 3]);
                assert_eq!(changes[1], vec![4, 5, 6]);
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_sync_message() {
        let msg = Message::Sync {
            heads: vec![10, 20, 30],
            changes: vec![vec![1, 2], vec![3, 4, 5]],
        };

        let encoded = msg.encode();
        let decoded = Message::decode(&encoded).unwrap();

        match decoded {
            Message::Sync { heads, changes } => {
                assert_eq!(heads, vec![10, 20, 30]);
                assert_eq!(changes.len(), 2);
                assert_eq!(changes[0], vec![1, 2]);
                assert_eq!(changes[1], vec![3, 4, 5]);
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
        };

        let encoded = msg.encode();
        let decoded = Message::decode(&encoded).unwrap();

        match decoded {
            Message::Broadcast {
                from_client_id,
                changes,
                affected_paths,
            } => {
                assert_eq!(from_client_id, "client-1");
                assert_eq!(changes.len(), 1);
                assert_eq!(changes[0], vec![42]);
                assert_eq!(affected_paths.len(), 2);
                assert_eq!(affected_paths[0], "user.name");
                assert_eq!(affected_paths[1], "user.email");
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_push_ack_message() {
        let msg = Message::PushAck {
            heads: vec![5, 6, 7],
        };

        let encoded = msg.encode();
        let decoded = Message::decode(&encoded).unwrap();

        match decoded {
            Message::PushAck { heads } => {
                assert_eq!(heads, vec![5, 6, 7]);
            }
            _ => panic!("Wrong message type"),
        }
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
