pub mod client;
pub mod data_types;
mod metrics;
pub mod server;
mod wal;

pub use metrics::run_metrics_task;

use std::path::PathBuf;

use hashbrown::HashMap;
use prometheus::{HistogramOpts, HistogramVec, IntCounter, Registry};
use tracing::debug;

use data_types::{BigPipeValue, ServerMessage, WalMessageEntry};
use wal::NamespaceWal;

#[derive(Debug)]
pub struct BigPipe {
    /// Internal queue to hold ordered messages as they are received,
    /// partitioned by their key.
    inner: HashMap<String, BigPipeValue>,
    /// Write ahead log to ensure durability of writes.
    wal: NamespaceWal,

    /// Total number of messages received throughout the process
    /// lifetime.
    ///
    /// A message is only considered "received" after it has
    /// been made durable with a WAL write AND then added
    /// to the in-memory map.
    received_messages: IntCounter,
}

impl BigPipe {
    pub fn try_new(
        wal_directory: PathBuf,
        wal_max_segment_size: Option<usize>,
        metrics: &Registry,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        if !wal_directory.exists() {
            std::fs::create_dir_all(&wal_directory)?;
        }

        let wal_replay_duration = HistogramVec::new(
            HistogramOpts::new(
                "bigpipe_wal_replay_duration_seconds",
                "Total time taken for a WAL replay to complete",
            ),
            &["namespace"],
        )
        .unwrap();

        let received_messages =
            IntCounter::new("bigpipe_received_messages", "Number of messages received").unwrap();

        metrics
            .register(Box::new(wal_replay_duration.clone()))
            .unwrap();
        metrics
            .register(Box::new(received_messages.clone()))
            .unwrap();

        let inner = NamespaceWal::replay(&wal_directory, &wal_replay_duration);
        let wal = NamespaceWal::new(wal_directory, wal_max_segment_size, metrics);
        Ok(Self {
            wal,
            inner,
            received_messages,
        })
    }

    /// Write a message.
    pub fn write(&mut self, message: &ServerMessage) -> Result<(), Box<dyn std::error::Error>> {
        self.wal_write(message)?;
        self.add_message(message);
        self.received_messages.inc();
        Ok(())
    }

    /// Add a message to the internal structure.
    fn add_message(&mut self, message: &ServerMessage) {
        self.inner
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
        self.inner.get(partition_key).cloned()
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
        self.inner.clone()
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
    use prometheus::Registry;
    use tempfile::TempDir;

    use crate::{BigPipe, ServerMessage};

    #[test]
    fn add_messages() {
        let wal_dir = TempDir::new().unwrap();
        let metrics = Registry::new();
        let mut q = BigPipe::try_new(wal_dir.path().to_path_buf(), None, &metrics).unwrap();

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
        let metrics = Registry::new();
        let mut bigpipe = BigPipe::try_new(dir.path().to_path_buf(), None, &metrics).unwrap();

        bigpipe.write(&ServerMessage::test_message(1)).unwrap();
        bigpipe.wal.flush("hello").unwrap();
        drop(bigpipe); // drop to demonstrate replay capability

        let metrics = Registry::new(); // use new registry, we cannot re-register metrics.
        let bigpipe = BigPipe::try_new(dir.path().to_path_buf(), None, &metrics).unwrap();

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
        let metrics = Registry::new();
        let mut bigpipe = BigPipe::try_new(dir.path().to_path_buf(), None, &metrics).unwrap();

        for i in 0..100 {
            bigpipe.write(&ServerMessage::test_message(i)).unwrap();
        }
        bigpipe.wal.flush("hello").unwrap();
        assert_eq!(bigpipe.received_messages.get(), 100);

        assert_eq!(
            bigpipe.get_message_range("hello", 10).unwrap(),
            (10..100)
                .map(ServerMessage::test_message)
                .collect::<Vec<_>>()
        );
    }
}
