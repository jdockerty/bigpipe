use bytes::Bytes;

use wal::MessageEntry as MessageEntryProto;
use wal::SegmentEntry as SegmentEntryProto;

// HACK:
//
// The WAL relies upon importing the RetentionPolicy proto which resides here.
// So this must be included so that it can also find it here through
// `super::namespace::RetentionPolicy`.
mod namespace {
    tonic::include_proto!("namespace");
}

// tonic requires that this is contained within a module
// that has the same name as the proto file.
//
// This is renamed to NAME_proto in the `data_types` module file.
#[expect(clippy::module_inception)]
pub mod wal {
    tonic::include_proto!("wal");
}

/// Definition of a message entry that resides in the WAL.
///
/// This is this crate's version of the protobuf equivalent.
#[derive(Debug, Clone)]
pub struct WalMessageEntry {
    pub key: String,
    pub value: Bytes,
    pub timestamp: i64,
    pub offset: u64,
}

impl WalMessageEntry {
    #[cfg(test)]
    pub fn test_message(timestamp: i64, offset: u64) -> Self {
        Self {
            key: "hello".to_string(),
            value: "world".into(),
            timestamp,
            offset,
        }
    }

    #[cfg(test)]
    pub fn with_key(self, key: &str) -> Self {
        Self {
            key: key.to_string(),
            ..self
        }
    }
}

/// An operation that can be enacted onto the WAL.
#[derive(Debug, Clone)]
pub enum WalOperation {
    /// Message ingest
    Message(WalMessageEntry),
}

impl WalOperation {
    #[cfg(test)]
    pub fn test_message(timestamp: i64, offset: u64) -> Self {
        WalOperation::Message(WalMessageEntry {
            key: "hello".to_string(),
            value: "world".into(),
            timestamp,
            offset,
        })
    }

    #[cfg(test)]
    pub fn with_key(self, key: &str) -> Self {
        match self {
            WalOperation::Message(msg) => WalOperation::Message(WalMessageEntry {
                key: key.to_string(),
                value: msg.value,
                timestamp: msg.timestamp,
                offset: msg.offset,
            }),
        }
    }
}

impl TryFrom<MessageEntryProto> for WalMessageEntry {
    type Error = Box<dyn std::error::Error>;
    fn try_from(value: MessageEntryProto) -> Result<Self, Self::Error> {
        Ok(WalMessageEntry {
            key: value.key,
            value: value.value.into(),
            timestamp: value.timestamp,
            offset: value.offset,
        })
    }
}

impl TryFrom<SegmentEntryProto> for WalOperation {
    type Error = Box<dyn std::error::Error>;
    fn try_from(value: SegmentEntryProto) -> Result<Self, Self::Error> {
        match value.message_entry {
            Some(message) => Ok(WalOperation::Message(WalMessageEntry {
                key: message.key,
                value: message.value.into(),
                timestamp: message.timestamp,
                offset: message.offset,
            })),
            None => Err("unknown variant encoded".into()),
        }
    }
}

impl TryFrom<WalOperation> for SegmentEntryProto {
    type Error = Box<dyn std::error::Error>;
    fn try_from(value: WalOperation) -> Result<Self, Self::Error> {
        match value {
            WalOperation::Message(message) => Ok(SegmentEntryProto {
                message_entry: Some(wal::MessageEntry {
                    key: message.key,
                    value: message.value.into(),
                    timestamp: message.timestamp,
                    offset: message.offset,
                }),
            }),
        }
    }
}
