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
            0x10 => Ok(MessageType::Ping),
            0x11 => Ok(MessageType::Pong),
            0xFF => Ok(MessageType::Error),
            _ => Err(anyhow!("Unknown message type: 0x{:02x}", value)),
        }
    }
}

/// Protocol messages
#[derive(Debug, Clone)]
pub enum Message {
    Connect {
        client_id: String,
        namespace_id: String,
        heads: Vec<u8>,
    },
    Sync {
        heads: Vec<u8>,  // Server's current heads (for incremental sync)
        changes: Vec<Vec<u8>>,
    },
    Push {
        namespace_id: String,
        changes: Vec<Vec<u8>>,
    },
    Broadcast {
        from_client_id: String,
        changes: Vec<Vec<u8>>,
    },
    PushAck,
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
                let namespace_id = read_string(&mut buf)?;
                let heads = if buf.remaining() > 0 {
                    read_bytes(&mut buf)?
                } else {
                    Vec::new()
                };

                Ok(Message::Connect {
                    client_id,
                    namespace_id,
                    heads,
                })
            }

            MessageType::Push => {
                let namespace_id = read_string(&mut buf)?;
                let changes = read_changes(&mut buf)?;

                Ok(Message::Push { namespace_id, changes })
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
                namespace_id,
                heads,
            } => {
                buf.put_u8(MessageType::Connect as u8);
                write_string(&mut buf, client_id);
                write_string(&mut buf, namespace_id);
                write_bytes(&mut buf, heads);
            }

            Message::Sync { heads, changes } => {
                buf.put_u8(MessageType::Sync as u8);
                write_bytes(&mut buf, heads);
                write_changes(&mut buf, changes);
            }

            Message::Broadcast {
                from_client_id,
                changes,
            } => {
                buf.put_u8(MessageType::Broadcast as u8);
                write_string(&mut buf, from_client_id);
                write_changes(&mut buf, changes);
            }

            Message::PushAck => {
                buf.put_u8(MessageType::PushAck as u8);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connect_message() {
        let msg = Message::Connect {
            client_id: "alice".to_string(),
            namespace_id: "general".to_string(),
            heads: vec![1, 2, 3],
        };

        let encoded = msg.encode();
        let decoded = Message::decode(&encoded).unwrap();

        match decoded {
            Message::Connect {
                client_id,
                namespace_id,
                heads,
            } => {
                assert_eq!(client_id, "alice");
                assert_eq!(namespace_id, "general");
                assert_eq!(heads, vec![1, 2, 3]);
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_push_message() {
        let msg = Message::Push {
            namespace_id: "general".to_string(),
            changes: vec![vec![1, 2, 3], vec![4, 5, 6]],
        };

        let encoded = msg.encode();
        let decoded = Message::decode(&encoded).unwrap();

        match decoded {
            Message::Push { namespace_id, changes } => {
                assert_eq!(namespace_id, "general");
                assert_eq!(changes.len(), 2);
                assert_eq!(changes[0], vec![1, 2, 3]);
                assert_eq!(changes[1], vec![4, 5, 6]);
            }
            _ => panic!("Wrong message type"),
        }
    }
}
