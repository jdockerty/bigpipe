use bytes::Bytes;
use serde::{Deserialize, Serialize};

use std::io::{Read, Write};

pub mod proto {
    use tonic::include_proto;
    include_proto!("message");
}

/// A message sent by a client.
#[derive(Debug, Serialize, Deserialize)]
pub struct ClientMessage {
    key: String,
    value: Bytes,
}

impl ClientMessage {
    pub fn new(key: String, value: Bytes) -> Self {
        Self { key, value }
    }

    /// Consume this [`ClientMessage`] and turn it into the corresponding
    /// [`ServerMessage`].
    pub fn into_server_message(self, timestamp: i64) -> ServerMessage {
        ServerMessage {
            key: self.key,
            value: self.value,
            timestamp,
        }
    }

    /// Get a reference to the underlying key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Get a reference to the underlying value.
    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

/// A message which has been received by the queue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerMessage {
    key: String,
    value: Bytes,
    timestamp: i64,
}

impl ServerMessage {
    pub fn new(key: String, value: Bytes, timestamp: i64) -> Self {
        Self {
            key,
            value,
            timestamp,
        }
    }

    #[cfg(test)]
    pub fn test_message(timestamp: i64) -> Self {
        Self::new("hello".into(), "world".into(), timestamp)
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> Bytes {
        self.value.clone() // cheaply clonable
    }
}

impl TryFrom<&ServerMessage> for Vec<u8> {
    type Error = Box<dyn std::error::Error>;

    fn try_from(message: &ServerMessage) -> Result<Self, Self::Error> {
        let mut buf = Vec::with_capacity(1024);

        buf.write_all(&message.key.len().to_be_bytes())?;
        buf.write_all(message.key.as_bytes())?;

        buf.write_all(&message.value.len().to_be_bytes())?;
        buf.write_all(&message.value)?;

        buf.write_all(&message.timestamp.to_be_bytes())?;

        Ok(buf)
    }
}

impl TryFrom<&mut dyn Read> for ServerMessage {
    type Error = std::io::Error;
    fn try_from(reader: &mut dyn Read) -> Result<Self, Self::Error> {
        // Key
        let mut key_len_buf = [0u8; size_of::<usize>()];
        reader.read_exact(&mut key_len_buf)?;
        let key_len = usize::from_be_bytes(key_len_buf);

        let mut key_buf = vec![0u8; key_len];
        reader.read_exact(&mut key_buf)?;
        let key = String::from_utf8(key_buf).expect("must be utf8");

        // Value
        let mut val_len_buf = [0u8; size_of::<usize>()];
        reader.read_exact(&mut val_len_buf)?;
        let val_len = usize::from_be_bytes(val_len_buf);

        let mut value = vec![0u8; val_len];
        reader.read_exact(&mut value)?;

        // Timestamp
        let mut timestamp_buf = [0u8; 8];
        reader.read_exact(&mut timestamp_buf)?;
        let timestamp = i64::from_be_bytes(timestamp_buf);

        Ok(ServerMessage {
            key,
            value: value.into(),
            timestamp,
        })
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn message_conversion() {
        let client_msg = ClientMessage::new("hello".to_string(), "world".into());
        let timestamp = 100;

        let server_msg = client_msg.into_server_message(timestamp);
        assert_eq!(
            server_msg,
            ServerMessage {
                key: "hello".to_string(),
                value: "world".into(),
                timestamp
            }
        )
    }
}
