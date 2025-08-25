use std::{path::PathBuf, sync::Arc};

use hashbrown::hash_map::Entry;
use hashbrown::HashMap;
use parking_lot::Mutex;
use prometheus::{HistogramOpts, HistogramVec, IntCounter, Registry};
use tokio::time::Instant;

use super::single::ScopedLog;
use super::DEFAULT_MAX_SEGMENT_SIZE;
use crate::data_types::message::ServerMessage;
use crate::data_types::namespace::Namespace;
use crate::data_types::wal::WalMessageEntry;

/// A multi-log which is an abstraction over numerous
/// [`ScopedLog`] implementations.
///
/// Each [`ScopedLog`] is keyed by a particular [`Namespace`],
/// handling incoming messages for that namespace. This means
/// that messages are partitioned by the namespace they are
/// scoped to.
#[derive(Debug)]
pub struct MultiLog {
    namespaces: Arc<Mutex<HashMap<Namespace, ScopedLog>>>,
    root_directory: PathBuf,
    max_segment_size: usize,

    /// Counter for the total number of namespaces
    /// that have been observed during this process'
    /// lifetime.
    total_namespaces: IntCounter,
    /// Distribution of time taken for different kinds
    /// of writes to be applied to the underlying WAL.
    wal_write_duration: HistogramVec,
    /// Distribution of time taken for a WAL replay
    /// to complete.
    wal_replay_duration: HistogramVec,
}

impl MultiLog {
    /// Create a new instance of [`NamespaceWal`].
    pub fn new(
        root_directory: PathBuf,
        max_segment_size: Option<usize>,
        metrics: &Registry,
    ) -> Self {
        let total_namespaces = IntCounter::new(
            "bigpipe_wal_tracked_namespaces",
            "Total namespaces being tracked over this processes lifetime",
        )
        .unwrap();

        let wal_write_duration = HistogramVec::new(
            HistogramOpts::new("bigpipe_wal_write_duraiton_ms", "Duration, in milliseconds, that it takes for a write to be made durable in the WAL"),
            &["kind"],
        )
        .unwrap();

        let wal_replay_duration = HistogramVec::new(
            HistogramOpts::new(
                "bigpipe_wal_replay_duration_seconds",
                "Total time taken for a WAL replay to complete",
            ),
            &["namespace"],
        )
        .unwrap();

        metrics
            .register(Box::new(total_namespaces.clone()))
            .unwrap();
        metrics
            .register(Box::new(wal_write_duration.clone()))
            .unwrap();
        metrics
            .register(Box::new(wal_replay_duration.clone()))
            .unwrap();

        Self {
            namespaces: Arc::new(Mutex::new(HashMap::with_capacity(100))),
            root_directory,
            max_segment_size: max_segment_size.unwrap_or(DEFAULT_MAX_SEGMENT_SIZE),
            total_namespaces,
            wal_write_duration,
            wal_replay_duration,
        }
    }

    pub fn namespaces(&self) -> Vec<Namespace> {
        self.namespaces.lock().keys().cloned().collect()
    }

    pub fn namespace_count(&self) -> u64 {
        self.total_namespaces.get()
    }

    /// Create the WAL directory structure that will be used for the underlying
    /// [`Wal`] for a particular key, returning the associated [`PathBuf`].
    fn create_wal_directory(&self, namespace: &Namespace) -> PathBuf {
        let wal_dir = PathBuf::from(format!("{}/{namespace}", self.root_directory.display()));
        std::fs::create_dir(&wal_dir).unwrap();
        wal_dir
    }

    /// Check whether a namespace already exists.
    ///
    /// A namespace existing means that the directory structure and
    /// MAYBE some logs exist for it.
    pub fn contains_namespace(&self, namespace: &Namespace) -> bool {
        self.namespaces.lock().contains_key(namespace)
    }

    /// Create a new namespace.
    pub fn create_namespace(&self, namespace: &Namespace) {
        self.namespaces
            .lock()
            .entry(namespace.clone())
            .or_insert_with(|| {
                let dir = self.create_wal_directory(namespace);
                self.total_namespaces.inc();
                ScopedLog::try_new(dir.clone(), Some(self.max_segment_size)).unwrap()
            });
    }

    #[allow(dead_code)]
    /// Flush the [`Wal`] of a particular namespace.
    pub(crate) fn flush(&self, namespace: &Namespace) -> Result<(), Box<dyn std::error::Error>> {
        self.namespaces.lock().get_mut(namespace).unwrap().flush()
    }

    #[allow(dead_code)]
    /// Flush the [`Wal`] of all namespaces.
    pub(crate) fn flush_all(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.namespaces
            .lock()
            .iter_mut()
            .try_for_each(|(_, wal)| wal.flush())
    }

    /// Write the incoming operation for a namespace whilst taking
    /// the internal namespace lock.
    ///
    /// When a namespace does not exist, it is eagerly created.
    fn write_under_lock(&self, namespace: &Namespace, op: &WalMessageEntry) -> (usize, usize) {
        match self.namespaces.lock().entry(namespace.clone()) {
            Entry::Occupied(mut n) => n.get_mut().write(op.clone()).unwrap(),
            Entry::Vacant(n) => {
                let wal_dir = self.create_wal_directory(namespace);
                let mut wal = ScopedLog::try_new(wal_dir, Some(self.max_segment_size)).unwrap();
                let (sz, offset) = wal.write(op.clone()).unwrap(); // Ensure that the inbound op is not lost
                n.insert(wal);
                (sz, offset)
            }
        }
    }

    /// Write a [`WalOperation`] to the respective [`Wal`]. The key within
    /// the operation is used as the namespace.
    pub fn write(&self, op: WalMessageEntry) -> Result<(usize, usize), Box<dyn std::error::Error>> {
        let start = Instant::now();
        let (sz, offset) = self.write_under_lock(&Namespace::new(&op.key), &op);
        self.wal_write_duration
            .with_label_values(&["message"])
            .observe(start.elapsed().as_secs_f64());
        Ok((sz, offset))
    }

    pub fn read(&self, namespace: &Namespace, offset: u64) -> Option<Vec<ServerMessage>> {
        let mut guard = self.namespaces.lock();
        match guard.entry(namespace.clone()) {
            Entry::Occupied(n) => n.get().read(offset).unwrap(),
            Entry::Vacant(_) => None,
        }
    }

    // Replay all [`Wal`] files that are found from the given directory.
    //
    // This will walk the given directory, it is a expectation that
    // sub-directories are keys for a namespace and will contain
    // individual segment files.
    //
    // For example:
    //
    // /tmp/example-multi-wal
    // ├── bar <-- namespace 'bar'
    // │   └── 0-bp.wal
    // └── foo <-- namespace 'foo'
    //     ├── 0-bp.wal
    //     └── 1-bp.wal
    // pub fn replay(&mut self) -> HashMap<Namespace, BigPipeValue> {
    //     let mut multi = HashMap::new();
    //     for entry in WalkDir::new(&self.root_directory)
    //         .max_depth(1) // current directory only
    //         .into_iter()
    //     {
    //         let entry = entry.unwrap();
    //         // Hack to skip over the initially passed
    //         // directory, as this counts as an entry.
    //         // Find a better way to do this.
    //         if entry.depth() == 1 {
    //             let start = Instant::now();
    //             let (id, inner) = Wal::replay(entry.path()).expect("should exist");
    //             self.wal_replay_duration
    //                 .with_label_values(&["namespace"])
    //                 .observe(start.elapsed().as_secs_f64());
    //             let wal = Wal::try_new(entry.path().to_path_buf(), None).unwrap();
    //             let namespace = Namespace::new(
    //                 entry
    //                     .path()
    //                     .file_name()
    //                     .expect("basename is the namespace name")
    //                     .to_string_lossy()
    //                     .as_ref(),
    //             );
    //             self.namespaces.lock().insert(namespace, wal);
    //             multi.extend(inner);
    //         }
    //     }
    //     multi
    // }
}

#[cfg(test)]
mod test {
    use super::*;

    use tempfile::TempDir;

    #[test]
    fn directory_structure() {
        let dir = TempDir::new().unwrap();
        let metrics = Registry::new();
        let multi = MultiLog::new(dir.path().to_path_buf(), None, &metrics);
        let namespace = Namespace::new("my_new_namespace");
        let wal_dir = multi.create_wal_directory(&namespace);
        assert_eq!(
            wal_dir.to_string_lossy(),
            format!("{}/my_new_namespace", dir.path().display())
        );
        assert!(std::fs::exists(&wal_dir).unwrap());
    }

    // #[test]
    // fn ops() {
    //     let dir = TempDir::new().unwrap();
    //     let metrics = Registry::new();
    //     let multi = MultiLog::new(dir.path().to_path_buf(), None, &metrics);

    //     multi.write(WalOperation::test_message(10)).unwrap();
    //     multi.flush(&Namespace::new("hello")).unwrap();
    //     assert_eq!(multi.total_namespaces.get(), 1);

    //     drop(multi);

    //     let previous_dir = format!("{}/hello", dir.path().display());
    //     let (_, inner) = Wal::replay(&previous_dir).unwrap();
    //     assert_eq!(
    //         inner
    //             .get(&Namespace::new("hello"))
    //             .unwrap()
    //             .get_range(0)
    //             .len(),
    //         1
    //     );
    //     assert_eq!(
    //         inner.get(&Namespace::new("hello")).unwrap().get_range(0),
    //         vec![ServerMessage::new("hello".to_string(), "world".into(), 10)]
    //     );
    // }

    // #[test]
    // fn replay() {
    //     let dir = TempDir::new().unwrap();
    //     let metrics = Registry::new();

    //     let multi = MultiLog::new(dir.path().to_path_buf(), None, &metrics);

    //     multi
    //         .write(WalOperation::test_message(10).with_key("foo"))
    //         .unwrap();
    //     multi
    //         .write(WalOperation::test_message(20).with_key("bar"))
    //         .unwrap();

    //     multi.flush_all().unwrap();
    //     assert_eq!(multi.total_namespaces.get(), 2);

    //     drop(multi);

    //     let metrics = Registry::new();
    //     let mut same_wal = MultiLog::new(dir.path().to_path_buf(), None, &metrics);
    //     let multi = same_wal.replay();
    //     assert_eq!(multi.keys().len(), 2);
    //     assert_eq!(
    //         multi.get(&Namespace::new("foo")).unwrap().get_range(0),
    //         vec![ServerMessage::new("foo".to_string(), "world".into(), 10)]
    //     );
    //     assert_eq!(
    //         multi.get(&Namespace::new("bar")).unwrap().get_range(0),
    //         vec![ServerMessage::new("bar".to_string(), "world".into(), 20)]
    //     );
    // }
}
