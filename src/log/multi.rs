use std::path::Path;
use std::{path::PathBuf, sync::Arc};

use hashbrown::hash_map::Entry;
use hashbrown::HashMap;
use parking_lot::Mutex;
use prometheus::{Histogram, HistogramOpts, HistogramVec, IntCounter, Registry};
use tokio::time::Instant;
use walkdir::WalkDir;

use super::single::ScopedLog;
use super::DEFAULT_MAX_SEGMENT_SIZE;
use crate::data_types::message::ServerMessage;
use crate::data_types::namespace::Namespace;

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
    /// Time taken for a full replay to complete.
    ///
    /// This involves performing individual replays
    /// on all namespaces that are discovered.
    #[allow(dead_code)]
    wal_replay_duration: Histogram,
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

        let wal_replay_duration = Histogram::with_opts(HistogramOpts::new(
            "bigpipe_wal_replay_duration_seconds",
            "Total time taken for a WAL replay to complete",
        ))
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

        let namespaces = replay(
            &root_directory,
            max_segment_size,
            wal_replay_duration.clone(),
        );

        Self {
            namespaces: Arc::new(Mutex::new(namespaces)),
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

    #[allow(dead_code)]
    pub fn namespace_count(&self) -> u64 {
        self.total_namespaces.get()
    }

    /// Create the WAL directory structure that will be used for the underlying
    /// [`Wal`] for a particular key, returning the associated [`PathBuf`].
    ///
    /// If the directory already exists, this is does nothing.
    fn create_wal_directory(&self, namespace: &Namespace) -> PathBuf {
        let wal_dir = PathBuf::from(format!("{}/{namespace}", self.root_directory.display()));

        if !wal_dir.exists() {
            std::fs::create_dir(&wal_dir).unwrap();
        }

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
    fn write_under_lock(&self, namespace: &Namespace, op: &ServerMessage) -> (usize, usize) {
        match self.namespaces.lock().entry(namespace.clone()) {
            Entry::Occupied(mut n) => n.get_mut().write(op).unwrap(),
            Entry::Vacant(n) => {
                let wal_dir = self.create_wal_directory(namespace);
                let mut wal = ScopedLog::try_new(wal_dir, Some(self.max_segment_size)).unwrap();
                let (sz, offset) = wal.write(op).unwrap(); // Ensure that the inbound op is not lost
                n.insert(wal);
                self.total_namespaces.inc();
                (sz, offset)
            }
        }
    }

    /// Write a [`ServerMessage`] to the respective [`ScopedLog`].
    /// The key within the operation is used as the namespace.
    ///
    /// This returns the number of bytes written and the physical byte
    /// offset of the write.
    pub fn write(&self, msg: &ServerMessage) -> Result<(usize, usize), Box<dyn std::error::Error>> {
        let start = Instant::now();
        let (sz, offset) = self.write_under_lock(&Namespace::new(msg.key()), msg);
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
}

// Replay all [`ScopedLog`] files that are found from the given directory.
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
fn replay(
    path: impl AsRef<Path>,
    max_segment_size: Option<usize>,
    wal_replay_duration: Histogram,
) -> HashMap<Namespace, ScopedLog> {
    let mut multi_log = HashMap::new();
    for entry in WalkDir::new(&path)
        .max_depth(1) // current directory only
        .into_iter()
    {
        let entry = entry.unwrap();
        // Hack to skip over the initially passed
        // directory, as this counts as an entry.
        // Find a better way to do this.
        if entry.depth() == 1 {
            let timer = wal_replay_duration.start_timer();
            let log = ScopedLog::try_new(entry.path().to_path_buf(), max_segment_size).unwrap();
            let namespace = Namespace::new(
                entry
                    .path()
                    .file_name()
                    .expect("basename is the namespace name")
                    .to_string_lossy()
                    .as_ref(),
            );
            timer.stop_and_record();
            multi_log.insert(namespace, log);
        }
    }
    multi_log
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

    #[test]
    fn replay() {
        let dir = TempDir::new().unwrap();
        let metrics = Registry::new();

        let multi = MultiLog::new(dir.path().to_path_buf(), None, &metrics);

        multi
            .write(&ServerMessage::test_message(10).with_key("foo"))
            .unwrap();
        multi
            .write(&ServerMessage::test_message(20).with_key("bar"))
            .unwrap();

        multi.flush_all().unwrap();
        assert_eq!(multi.total_namespaces.get(), 2);

        drop(multi);

        let metrics = Registry::new();
        let same_wal = MultiLog::new(dir.path().to_path_buf(), None, &metrics);

        assert_eq!(same_wal.namespaces.lock().keys().len(), 2);
        assert_eq!(
            same_wal.wal_replay_duration.get_sample_count(),
            2,
            "Expected 2 replays from 2 namespaces existing"
        );

        assert_eq!(
            same_wal.read(&Namespace::new("foo"), 0).unwrap(),
            vec![ServerMessage::new("foo".to_string(), "world".into(), 10)]
        );
        assert_eq!(
            same_wal.read(&Namespace::new("bar"), 0).unwrap(),
            vec![ServerMessage::new("bar".to_string(), "world".into(), 20)]
        );
        assert!(same_wal.read(&Namespace::new("not_exist"), 0).is_none());
    }
}
