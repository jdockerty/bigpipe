mod wal;

use std::{
    io::{Read, Write},
    path::PathBuf,
};

use bytes::Bytes;
use hashbrown::HashMap;
use parking_lot::Mutex;

use serde::{Deserialize, Serialize};
use tracing::debug;
use wal::Wal;

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

#[derive(Debug)]
pub struct BigPipe {
    /// Internal queue to hold ordered messages as they are received,
    /// partitioned by their key.
    inner: Mutex<HashMap<String, Vec<ServerMessage>>>,
    /// Write ahead log to ensure durability of writes.
    wal: Wal,
}

impl BigPipe {
    pub fn new(wal_directory: PathBuf, wal_max_segment_size: Option<usize>) -> Self {
        Self {
            inner: Mutex::new(HashMap::with_capacity(100)),
            wal: Wal::new(wal_directory, wal_max_segment_size),
        }
    }

    /// Add a message.
    pub fn add_message(&mut self, message: ServerMessage) {
        self.inner
            .lock()
            .entry(message.key.clone())
            .and_modify(|messages| {
                debug!(key = message.key, "updating");
                messages.push(message.clone())
            })
            .or_insert_with(|| {
                debug!(key = message.key, "new key");
                let mut messages = Vec::with_capacity(100);
                messages.push(message);
                messages
            });
    }

    /// Get messages for a particular key, returning [`None`] if there are
    /// no messages.
    pub fn get_messages(&self, partition_key: &str) -> Option<Vec<ServerMessage>> {
        self.inner
            .lock()
            .get(partition_key)
            .map(|messages| messages.to_vec())
    }

    /// Get all messages.
    pub fn messages(&self) -> HashMap<String, Vec<ServerMessage>> {
        let guard = self.inner.lock();
        guard.clone()
    }

    /// Write to the underlying WAL.
    pub fn wal_write(&mut self, message: &ServerMessage) -> Result<(), Box<dyn std::error::Error>> {
        self.wal.write(message)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::{BigPipe, ClientMessage, ServerMessage};

    #[test]
    fn add_messages() {
        let wal_dir = TempDir::new().unwrap();
        let mut q = BigPipe::new(wal_dir.path().to_path_buf(), None);

        let msg_1 = ServerMessage::new(
            "hello".to_string(),
            "world".into(),
            chrono::Utc::now().timestamp_micros(),
        );

        let msg_2 = ServerMessage::new(
            "test".to_string(),
            "value".into(),
            chrono::Utc::now().timestamp_micros(),
        );

        for msg in [msg_1.clone(), msg_2.clone()] {
            q.add_message(msg);
        }

        let messages = q.messages();
        assert_eq!(messages.keys().len(), 2);
        assert_eq!(q.get_messages(&msg_1.key).unwrap()[0], msg_1);
        assert_eq!(q.get_messages(&msg_2.key).unwrap()[0], msg_2);
        assert!(q.get_messages("key_doesnt_exist").is_none());
    }

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
