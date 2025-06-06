use bytes::Bytes;
use serde::{Deserialize, Serialize};

use std::{
    io::{Read, Write},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

pub mod proto {
    use tonic::include_proto;
    include_proto!("message");
    include_proto!("wal");
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

    pub fn timestamp(&self) -> i64 {
        self.timestamp
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

#[derive(Debug, Clone)]
pub struct BigPipeValue {
    queue: Vec<ServerMessage>,
    length: Arc<AtomicU64>,
}

impl Default for BigPipeValue {
    fn default() -> Self {
        Self::new()
    }
}

impl BigPipeValue {
    pub fn new() -> Self {
        Self {
            queue: Vec::with_capacity(100),
            length: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn len(&self) -> u64 {
        self.length.load(Ordering::Acquire)
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn push(&mut self, value: ServerMessage) {
        self.queue.push(value);
        self.length.fetch_add(1, Ordering::Release);
    }

    pub fn get(&self, offset: u64) -> Option<&ServerMessage> {
        self.queue.get(offset as usize)
    }

    pub fn get_range(&self, offset: u64) -> Vec<ServerMessage> {
        assert!(offset < self.len());
        let (_, after) = self.queue.split_at(offset as usize);
        after.to_vec()
    }
}

impl IntoIterator for BigPipeValue {
    type Item = ServerMessage;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.queue.into_iter()
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

    #[test]
    fn get_value() {
        let mut value = BigPipeValue::new();
        value.push(ServerMessage::test_message(100));
        assert_eq!(*value.get(0).unwrap(), ServerMessage::test_message(100));
        assert!(value.get(1).is_none());
    }

    #[test]
    fn get_value_range() {
        let mut value = BigPipeValue::new();

        let mut known_values = Vec::new();
        for i in 0..100 {
            let msg = ServerMessage::test_message(i);
            value.push(msg.clone());
            known_values.push(msg.clone());
        }

        assert_eq!(value.get_range(0), &known_values[..]);
        assert_eq!(value.get_range(5), &known_values[5..known_values.len()]);
        assert_eq!(value.get_range(99)[0], ServerMessage::test_message(99));
    }
}
