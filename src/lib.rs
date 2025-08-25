pub mod client;
pub mod data_types;
mod log;
mod metrics;
pub mod server;

pub use metrics::run_metrics_task;

use std::path::PathBuf;

use hashbrown::HashMap;
use prometheus::{IntCounter, Registry};

use data_types::{message::ServerMessage, namespace::Namespace, wal::WalMessageEntry};
use log::MultiLog;

#[derive(Debug)]
pub struct BigPipe {
    /// Write ahead log to ensure durability of writes.
    ///
    /// This is composed of multiple WALs, partitioned by
    /// [`Namespace`] to isolate data.
    log: MultiLog,

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

        let received_messages =
            IntCounter::new("bigpipe_received_messages", "Number of messages received").unwrap();

        metrics
            .register(Box::new(received_messages.clone()))
            .unwrap();

        let log = MultiLog::new(wal_directory, wal_max_segment_size, metrics);
        Ok(Self {
            log,
            received_messages,
        })
    }

    /// Write a message.
    pub fn write(&mut self, message: &ServerMessage) -> Result<(), Box<dyn std::error::Error>> {
        self.log_write(message)?;
        self.log.flush(&Namespace::new(message.key()))?;
        self.received_messages.inc();
        Ok(())
    }

    /// Get messages for a particular key, returning [`None`] if there are
    /// no messages.
    pub fn get_messages(&self, namespace: &str, offset: u64) -> Option<Vec<ServerMessage>> {
        self.log.read(&Namespace::new(namespace), offset)
    }

    /// Get all messages.
    pub fn messages(&self) -> HashMap<Namespace, Vec<ServerMessage>> {
        let namespaces = self.log.namespaces();
        let mut messages = HashMap::with_capacity(namespaces.len());
        for namespace in &namespaces {
            messages.insert(namespace.clone(), self.log.read(namespace, 0).unwrap());
        }
        messages
    }

    /// Write to the underlying WAL.
    pub fn log_write(&mut self, message: &ServerMessage) -> Result<(), Box<dyn std::error::Error>> {
        self.log.write(WalMessageEntry {
            key: message.key().to_string(),
            value: message.value(),
            timestamp: message.timestamp(),
            offset: 0, // TODO: no hardcode
        })?;
        Ok(())
    }

    pub fn contains_namespace(&self, namespace: &Namespace) -> bool {
        self.log.contains_namespace(namespace)
    }

    pub fn create_namespace(&mut self, namespace: Namespace) {
        self.log.create_namespace(&namespace);
    }
}

#[cfg(test)]
mod tests {
    use prometheus::Registry;
    use tempfile::TempDir;

    use crate::{data_types::namespace::Namespace, BigPipe, ServerMessage};

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
            q.write(&msg).unwrap();
        }

        let messages = q.messages();
        assert_eq!(messages.keys().len(), 2);
        assert_eq!(
            *q.get_messages(msg_1.key(), 0)
                .expect("messages exist")
                .first()
                .expect("first message exists"),
            msg_1
        );
        assert_eq!(
            *q.get_messages(msg_2.key(), 0).unwrap().get(0).unwrap(),
            msg_2
        );
        assert!(q.get_messages("key_doesnt_exist", 0).is_none());
    }

    // TODO: replays
    // #[test]
    // fn wal_replay() {
    //     let dir = TempDir::new().unwrap();
    //     let metrics = Registry::new();
    //     let mut bigpipe = BigPipe::try_new(dir.path().to_path_buf(), None, &metrics).unwrap();

    //     bigpipe.write(&ServerMessage::test_message(1)).unwrap();
    //     bigpipe.wal.flush(&Namespace::new("hello")).unwrap();
    //     drop(bigpipe); // drop to demonstrate replay capability

    //     let metrics = Registry::new(); // use new registry, we cannot re-register metrics.
    //     let bigpipe = BigPipe::try_new(dir.path().to_path_buf(), None, &metrics).unwrap();

    //     let messages = bigpipe.get_messages("hello", 0).unwrap();
    //     assert_eq!(messages.len(), 1);
    //     let message = messages.get(0);
    //     assert_eq!(
    //         message.cloned(),
    //         Some(ServerMessage::test_message(1)),
    //         "Expected previous message being available from replay"
    //     );
    // }

    #[test]
    fn message_range() {
        let dir = TempDir::new().unwrap();
        let metrics = Registry::new();
        let mut bigpipe = BigPipe::try_new(dir.path().to_path_buf(), None, &metrics).unwrap();

        for i in 0..100 {
            bigpipe.write(&ServerMessage::test_message(i)).unwrap();
        }
        bigpipe.log.flush(&Namespace::new("hello")).unwrap();
        assert_eq!(bigpipe.received_messages.get(), 100);

        assert_eq!(
            bigpipe.get_messages("hello", 10).unwrap(),
            (10..100)
                .map(ServerMessage::test_message)
                .collect::<Vec<_>>()
        );
    }
}
