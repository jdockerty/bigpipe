use bytes::Bytes;

use super::value::RetentionPolicy;
use wal::segment_entry;
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
