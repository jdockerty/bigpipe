use bytes::Bytes;
use serde::{Deserialize, Serialize};

pub mod message {
    tonic::include_proto!("message");
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

    /// Get the contained value bytes.
    pub fn value(&self) -> Bytes {
        self.value.clone()
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

#[cfg(test)]
mod test {

    use super::{ClientMessage, ServerMessage};

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
