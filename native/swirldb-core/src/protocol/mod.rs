// Copyright 2025 Everyside Innovations, LLC
// SPDX-License-Identifier: Apache-2.0

/// Binary WebSocket protocol for SwirlDB sync
///
/// Wire format is designed for minimal overhead with length-prefixed messages.
/// All multi-byte integers are big-endian (network byte order).

use anyhow::{anyhow, Context, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};

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
    Ping = 0x10,
    Pong = 0x11,
    Error = 0xFF,
}

impl TryFrom<u8> for MessageType {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0x01 => Ok(MessageType::Connect),
            0x02 => Ok(MessageType::Sync),
            0x03 => Ok(MessageType::Push),
            0x04 => Ok(MessageType::Broadcast),
            0x05 => Ok(MessageType::PushAck),
            0x06 => Ok(MessageType::SubscribeAck),
            0x10 => Ok(MessageType::Ping),
            0x11 => Ok(MessageType::Pong),
            0xFF => Ok(MessageType::Error),
            _ => Err(anyhow!("Unknown message type: 0x{:02x}", value)),
        }
    }
}

/// Protocol messages (subscription-based, no namespaces)
#[derive(Debug, Clone)]
pub enum Message {
    Connect {
        client_id: String,
        subscriptions: Vec<String>,  // Path patterns to subscribe to
        heads: Vec<u8>,
    },
    Sync {
        heads: Vec<u8>,  // Server's current heads (for incremental sync)
        changes: Vec<Vec<u8>>,
    },
    Subscribe {
        add: Vec<String>,     // Add these subscription patterns
        remove: Vec<String>,  // Remove these subscription patterns
    },
    SubscribeAck {
        added: Vec<String>,   // Successfully added subscriptions
        denied: Vec<String>,  // Subscriptions denied by policy
    },
    Push {
        heads: Vec<u8>,  // Client's current heads (for server to know what client has)
        changes: Vec<Vec<u8>>,
    },
    Broadcast {
        from_client_id: String,
        changes: Vec<Vec<u8>>,
        affected_paths: Vec<String>,  // Paths modified by these changes
    },
    PushAck {
        heads: Vec<u8>,  // Server's new heads after applying changes
    },
    Ping,
    Pong,
    Error {
        message: String,
    },
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

                Ok(Message::Connect {
                    client_id,
                    subscriptions,
                    heads,
                })
            }

            MessageType::Push => {
                let heads = read_bytes(&mut buf)?;
                let changes = read_changes(&mut buf)?;
                Ok(Message::Push { heads, changes })
            }

            MessageType::Sync => {
                let heads = read_bytes(&mut buf)?;
                let changes = read_changes(&mut buf)?;
                Ok(Message::Sync { heads, changes })
            }

            MessageType::SubscribeAck => {
                let added = read_string_array(&mut buf)?;
                let denied = read_string_array(&mut buf)?;
                Ok(Message::SubscribeAck { added, denied })
            }

            MessageType::Broadcast => {
                let from_client_id = read_string(&mut buf)?;
                let changes = read_changes(&mut buf)?;
                let affected_paths = read_string_array(&mut buf)?;
                Ok(Message::Broadcast {
                    from_client_id,
                    changes,
                    affected_paths,
                })
            }

            MessageType::PushAck => {
                let heads = read_bytes(&mut buf)?;
                Ok(Message::PushAck { heads })
            }

            MessageType::Error => {
                let message = read_string(&mut buf)?;
                Ok(Message::Error { message })
            }

            MessageType::Ping => Ok(Message::Ping),

            MessageType::Pong => Ok(Message::Pong),

            _ => Err(anyhow!("Cannot decode message type: {:?}", msg_type)),
        }
    }

    /// Encode a message to binary
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = BytesMut::new();

        match self {
            Message::Connect {
                client_id,
                subscriptions,
                heads,
            } => {
                buf.put_u8(MessageType::Connect as u8);
                write_string(&mut buf, client_id);
                write_string_array(&mut buf, subscriptions);
                write_bytes(&mut buf, heads);
            }

            Message::Sync { heads, changes } => {
                buf.put_u8(MessageType::Sync as u8);
                write_bytes(&mut buf, heads);
                write_changes(&mut buf, changes);
            }

            Message::Subscribe { add, remove } => {
                buf.put_u8(MessageType::Connect as u8); // TODO: Add Subscribe type
                write_string_array(&mut buf, add);
                write_string_array(&mut buf, remove);
            }

            Message::SubscribeAck { added, denied } => {
                buf.put_u8(MessageType::SubscribeAck as u8);
                write_string_array(&mut buf, added);
                write_string_array(&mut buf, denied);
            }

            Message::Push { heads, changes } => {
                buf.put_u8(MessageType::Push as u8);
                write_bytes(&mut buf, heads);
                write_changes(&mut buf, changes);
            }

            Message::Broadcast {
                from_client_id,
                changes,
                affected_paths,
            } => {
                buf.put_u8(MessageType::Broadcast as u8);
                write_string(&mut buf, from_client_id);
                write_changes(&mut buf, changes);
                write_string_array(&mut buf, affected_paths);
            }

            Message::PushAck { heads } => {
                buf.put_u8(MessageType::PushAck as u8);
                write_bytes(&mut buf, heads);
            }

            Message::Ping => {
                buf.put_u8(MessageType::Ping as u8);
            }

            Message::Pong => {
                buf.put_u8(MessageType::Pong as u8);
            }

            Message::Error { message } => {
                buf.put_u8(MessageType::Error as u8);
                write_string(&mut buf, message);
            }

            _ => {}
        }

        buf.to_vec()
    }
}

// Helper functions for reading/writing length-prefixed data

fn read_string(buf: &mut Bytes) -> Result<String> {
    let len = buf.get_u32() as usize;
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
    let len = buf.get_u32() as usize;
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
    let count = buf.get_u32() as usize;
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
    let count = buf.get_u32() as usize;
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
            subscriptions: vec!["/user/alice/**".to_string(), "/public/**".to_string()],
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
                assert_eq!(subscriptions[0], "/user/alice/**");
                assert_eq!(subscriptions[1], "/public/**");
                assert_eq!(heads, vec![1, 2, 3]);
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
}
