use bytes::Bytes;
use serde::{Deserialize, Serialize};
use wal::{segment_entry, SegmentEntry as SegmentEntryProto};

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

pub mod message {
    tonic::include_proto!("message");
}

pub mod namespace {
    tonic::include_proto!("namespace");
}

pub mod wal {
    tonic::include_proto!("wal");
}

#[derive(Debug, Clone, PartialEq, Eq, prost::Enumeration, clap::ValueEnum)]
pub enum RetentionPolicy {
    Ttl = 0,
    DiskPressure = 1,
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

/// Definition of a message entry that resides in the WAL.
///
/// This is this crate's version of the protobuf equivalent.
#[derive(Debug, Clone)]
pub struct WalMessageEntry {
    pub key: String,
    pub value: Bytes,
    pub timestamp: i64,
}

/// Definition of a namespace entry that resides in the WAL.
///
/// This is this crate's version of the protobuf equivalent.
#[derive(Debug, Clone)]
pub struct WalNamespaceEntry {
    pub key: String,
    pub retention_policy: RetentionPolicy,
}

/// An operation that can be enacted onto the WAL.
#[derive(Debug, Clone)]
pub enum WalOperation {
    /// Message ingest
    Message(WalMessageEntry),
    /// Namespace creation
    Namespace(WalNamespaceEntry),
}

impl WalOperation {
    #[cfg(test)]
    pub fn test_message(timestamp: i64) -> Self {
        WalOperation::Message(WalMessageEntry {
            key: "hello".to_string(),
            value: "world".into(),
            timestamp,
        })
    }

    #[cfg(test)]
    pub fn with_key(self, key: &str) -> Self {
        match self {
            WalOperation::Message(msg) => WalOperation::Message(WalMessageEntry {
                key: key.to_string(),
                value: msg.value,
                timestamp: msg.timestamp,
            }),
            WalOperation::Namespace(namespace) => WalOperation::Namespace(WalNamespaceEntry {
                key: namespace.key,
                retention_policy: namespace.retention_policy,
            }),
        }
    }
}

impl TryFrom<SegmentEntryProto> for WalOperation {
    type Error = Box<dyn std::error::Error>;
    fn try_from(value: SegmentEntryProto) -> Result<Self, Self::Error> {
        match value.entry {
            Some(entry) => match entry {
                segment_entry::Entry::MessageEntry(message) => {
                    Ok(WalOperation::Message(WalMessageEntry {
                        key: message.key,
                        value: message.value.into(),
                        timestamp: message.timestamp,
                    }))
                }
                segment_entry::Entry::NamespaceEntry(namespace) => {
                    Ok(WalOperation::Namespace(WalNamespaceEntry {
                        key: namespace.key.clone(),
                        retention_policy: RetentionPolicy::try_from(
                            namespace.retention_policy() as i32
                        )
                        .unwrap(),
                    }))
                }
            },
            None => Err("unknown variant encoded".into()),
        }
    }
}

impl TryFrom<WalOperation> for SegmentEntryProto {
    type Error = Box<dyn std::error::Error>;
    fn try_from(value: WalOperation) -> Result<Self, Self::Error> {
        match value {
            WalOperation::Message(message) => Ok(SegmentEntryProto {
                entry: Some(segment_entry::Entry::MessageEntry(wal::MessageEntry {
                    key: message.key,
                    value: message.value.into(),
                    timestamp: message.timestamp,
                })),
            }),
            WalOperation::Namespace(namespace) => Ok(SegmentEntryProto {
                entry: Some(segment_entry::Entry::NamespaceEntry(wal::NamespaceEntry {
                    key: namespace.key,
                    retention_policy: namespace.retention_policy as i32,
                })),
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BigPipeValue {
    queue: Vec<ServerMessage>,
    length: Arc<AtomicU64>,
    retention_policy: RetentionPolicy,
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
            retention_policy: RetentionPolicy::DiskPressure,
        }
    }

    pub fn len(&self) -> u64 {
        self.length.load(Ordering::Acquire)
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn retention_policy(&self) -> &RetentionPolicy {
        &self.retention_policy
    }

    pub fn set_retention_policy(&mut self, retention_policy: RetentionPolicy) {
        self.retention_policy = retention_policy;
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
