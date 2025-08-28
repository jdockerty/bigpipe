use bytes::Bytes;

use log::MessageEntry as MessageEntryProto;

// tonic requires that this is contained within a module
// that has the same name as the proto file.
//
// This is renamed to NAME_proto in the `mod.rs` file.
#[expect(clippy::module_inception)]
pub mod log {
    tonic::include_proto!("log");
}

/// Definition of a message entry that resides in the WAL.
///
/// This is this crate's version of the protobuf equivalent.
#[derive(Debug, Clone)]
pub struct LogMessageEntry {
    pub key: String,
    pub value: Bytes,
    pub timestamp: i64,
    pub offset: u64,
}

impl LogMessageEntry {
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

impl TryFrom<MessageEntryProto> for LogMessageEntry {
    type Error = Box<dyn std::error::Error>;
    fn try_from(value: MessageEntryProto) -> Result<Self, Self::Error> {
        Ok(LogMessageEntry {
            key: value.key,
            value: value.value.into(),
            timestamp: value.timestamp,
            offset: value.offset,
        })
    }
}
