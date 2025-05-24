use bytes::Bytes;
use hashbrown::HashMap;
use parking_lot::Mutex;

use serde::{Deserialize, Serialize};

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

#[derive(Debug)]
pub struct BigPipe {
    /// Internal queue to hold ordered messages as they are received,
    /// partitioned by their key.
    queue: Mutex<HashMap<String, Vec<ServerMessage>>>,
}

impl Default for BigPipe {
    fn default() -> Self {
        Self::new()
    }
}

impl BigPipe {
    pub fn new() -> Self {
        Self {
            queue: Mutex::new(HashMap::with_capacity(100)),
        }
    }

    /// Add a message.
    pub fn add_message(&mut self, message: ServerMessage) {
        self.queue
            .lock()
            .entry(message.key.clone())
            .and_modify(|messages| messages.push(message.clone()))
            .or_insert_with(|| {
                let mut messages = Vec::with_capacity(100);
                messages.push(message);
                messages
            });
    }

    /// Get messages for a particular key, returning [`None`] if there are
    /// no messages.
    pub fn get_messages(&self, partition_key: &str) -> Option<Vec<ServerMessage>> {
        self.queue
            .lock()
            .get(partition_key)
            .map(|messages| messages.to_vec())
    }

    /// Get all messages.
    pub fn messages(&self) -> HashMap<String, Vec<ServerMessage>> {
        let guard = self.queue.lock();
        guard.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::{BigPipe, ClientMessage, ServerMessage};

    #[test]
    fn add_messages() {
        let mut q = BigPipe::new();

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
