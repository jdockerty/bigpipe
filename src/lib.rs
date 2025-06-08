pub mod data_types;
pub mod server;
mod wal;

use std::path::PathBuf;

use hashbrown::HashMap;
use parking_lot::Mutex;
use tracing::debug;

use data_types::{BigPipeValue, ServerMessage, WalMessageEntry};
use wal::{Wal, WAL_DEFAULT_ID};

#[derive(Debug)]
pub struct BigPipe {
    /// Internal queue to hold ordered messages as they are received,
    /// partitioned by their key.
    inner: Mutex<HashMap<String, BigPipeValue>>,
    /// Write ahead log to ensure durability of writes.
    wal: Wal,
}

impl BigPipe {
    pub fn try_new(
        wal_directory: PathBuf,
        wal_max_segment_size: Option<usize>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        if let Some((last_segment_id, inner)) = Wal::replay(&wal_directory) {
            let wal = Wal::try_new(last_segment_id + 1, wal_directory, wal_max_segment_size)?;
            let inner = Mutex::new(inner);
            Ok(Self { inner, wal })
        } else {
            Ok(Self {
                inner: Mutex::new(HashMap::with_capacity(100)),
                wal: Wal::try_new(WAL_DEFAULT_ID, wal_directory, wal_max_segment_size)?,
            })
        }
    }

    /// Write a message.
    pub fn write(&mut self, message: &ServerMessage) -> Result<(), Box<dyn std::error::Error>> {
        self.wal_write(message)?;
        self.add_message(message);
        Ok(())
    }

    /// Add a message to the internal structure.
    fn add_message(&mut self, message: &ServerMessage) {
        self.inner
            .lock()
            .entry(message.key().to_string())
            .and_modify(|messages| {
                debug!(key = message.key(), "updating");
                messages.push(message.clone())
            })
            .or_insert_with(|| {
                debug!(key = message.key(), "new key");
                let mut messages = BigPipeValue::new();
                messages.push(message.clone());
                messages
            });
    }

    /// Get messages for a particular key, returning [`None`] if there are
    /// no messages.
    pub fn get_messages(&self, partition_key: &str) -> Option<BigPipeValue> {
        self.inner.lock().get(partition_key).cloned()
    }

    /// Get a range of messages starting from the `offset`.
    pub fn get_message_range(
        &self,
        partition_key: &str,
        offset: u64,
    ) -> Option<Vec<ServerMessage>> {
        self.get_messages(partition_key)
            .map(|messages| messages.get_range(offset))
    }

    /// Get all messages.
    pub fn messages(&self) -> HashMap<String, BigPipeValue> {
        let guard = self.inner.lock();
        guard.clone()
    }

    /// Write to the underlying WAL.
    pub fn wal_write(&mut self, message: &ServerMessage) -> Result<(), Box<dyn std::error::Error>> {
        // TODO: keep this as a WalOperation and allow callee to decide on inbound op?
        self.wal
            .write(data_types::WalOperation::Message(WalMessageEntry {
                key: message.key().to_string(),
                value: message.value(),
                timestamp: message.timestamp(),
            }))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::{BigPipe, ServerMessage};

    #[test]
    fn add_messages() {
        let wal_dir = TempDir::new().unwrap();
        let mut q = BigPipe::try_new(wal_dir.path().to_path_buf(), None).unwrap();

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
            q.add_message(&msg);
        }

        let messages = q.messages();
        assert_eq!(messages.keys().len(), 2);
        assert_eq!(*q.get_messages(msg_1.key()).unwrap().get(0).unwrap(), msg_1);
        assert_eq!(*q.get_messages(msg_2.key()).unwrap().get(0).unwrap(), msg_2);
        assert!(q.get_messages("key_doesnt_exist").is_none());
    }

    #[test]
    fn wal_replay() {
        let dir = TempDir::new().unwrap();
        let mut bigpipe = BigPipe::try_new(dir.path().to_path_buf(), None).unwrap();

        bigpipe.write(&ServerMessage::test_message(1)).unwrap();
        bigpipe.wal.flush().unwrap();
        drop(bigpipe); // drop to demonstrate replay capability

        let bigpipe = BigPipe::try_new(dir.path().to_path_buf(), None).unwrap();

        let messages = bigpipe.get_messages("hello").unwrap();
        assert_eq!(messages.len(), 1);
        let message = messages.get(0);
        assert_eq!(
            message.cloned(),
            Some(ServerMessage::test_message(1)),
            "Expected previous message being available from replay"
        );
    }

    #[test]
    fn message_range() {
        let dir = TempDir::new().unwrap();
        let mut bigpipe = BigPipe::try_new(dir.path().to_path_buf(), None).unwrap();

        for i in 0..100 {
            bigpipe.write(&ServerMessage::test_message(i)).unwrap();
        }
        bigpipe.wal.flush().unwrap();

        assert_eq!(
            bigpipe.get_message_range("hello", 10).unwrap(),
            (10..100)
                .map(ServerMessage::test_message)
                .collect::<Vec<_>>()
        );
    }
}
