use std::path::Path;
use std::{path::PathBuf, sync::Arc};

use hashbrown::HashMap;
use parking_lot::Mutex;
use prometheus::{Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, Registry};
use tokio::time::Instant;
use walkdir::WalkDir;

use super::Wal;
use super::DEFAULT_MAX_SEGMENT_SIZE;
use crate::data_types::WalOperation;
use crate::wal::WAL_DEFAULT_ID;
use crate::BigPipeValue;

/// A write-ahead log implementation which will handle
/// multiple underlying [`Wal`]s at once, keyed by
/// namespace.
#[derive(Debug)]
pub struct NamespaceWal {
    namespaces: Arc<Mutex<HashMap<String, Wal>>>,
    root_directory: PathBuf,
    max_segment_size: usize,

    total_namespaces: IntCounter,
    wal_write_duration: HistogramVec,
}

impl NamespaceWal {
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
            &["namespace"],
        )
        .unwrap();

        metrics
            .register(Box::new(total_namespaces.clone()))
            .unwrap();
        metrics
            .register(Box::new(wal_write_duration.clone()))
            .unwrap();

        Self {
            namespaces: Arc::new(Mutex::new(HashMap::with_capacity(100))),
            root_directory,
            max_segment_size: max_segment_size.unwrap_or(DEFAULT_MAX_SEGMENT_SIZE),
            total_namespaces,
            wal_write_duration,
        }
    }

    /// Create the WAL directory structure that will be used for the underlying
    /// [`Wal`] for a particular key, returning the associated [`PathBuf`].
    fn create_wal_directory(&self, namespace: &str) -> PathBuf {
        let wal_dir = PathBuf::from(format!("{}/{namespace}", self.root_directory.display()));
        std::fs::create_dir(&wal_dir).unwrap();
        wal_dir
    }

    #[allow(dead_code)]
    /// Flush the [`Wal`] of a particular namespace.
    pub(crate) fn flush(&self, namespace: &str) -> Result<(), Box<dyn std::error::Error>> {
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

    fn write_under_lock(&self, namespace: &str, op: &WalOperation) {
        self.namespaces
            .lock()
            .entry(namespace.to_string())
            .and_modify(|wal| {
                wal.write(op.clone()).unwrap();
            })
            .or_insert_with(|| {
                let wal_dir = self.create_wal_directory(namespace);
                let mut wal =
                    Wal::try_new(WAL_DEFAULT_ID, wal_dir, Some(self.max_segment_size)).unwrap();
                wal.write(op.clone()).unwrap(); // Ensure that the inbound op is not lost
                wal
            });
    }

    /// Write a [`WalOperation`] to the respective [`Wal`]. The key within
    /// the operation is used as the namespace.
    pub fn write(&self, op: WalOperation) -> Result<(), Box<dyn std::error::Error>> {
        match &op {
            WalOperation::Message(msg) => {
                self.write_under_lock(&msg.key, &op);
                Ok(())
            }
            WalOperation::Namespace(namespace) => {
                self.write_under_lock(&namespace.key, &op);
                Ok(())
            }
        }
    }

    /// Replay all [`Wal`] files that are found from the given directory.
    ///
    /// This will walk the given directory, it is a expectation that
    /// sub-directories are keys for a namespace and will contain
    /// individual segment files.
    ///
    /// For example:
    ///
    /// /tmp/example-multi-wal
    /// ├── bar <-- namespace 'bar'
    /// │   └── 0-bp.wal
    /// └── foo <-- namespace 'foo'
    ///     ├── 0-bp.wal
    ///     └── 1-bp.wal
    pub fn replay<P: AsRef<Path>>(
        directory: P,
        wal_replay_duration: &HistogramVec,
    ) -> HashMap<String, BigPipeValue> {
        let mut multi = HashMap::new();
        for entry in WalkDir::new(directory)
            .max_depth(1) // current directory only
            .into_iter()
        {
            let entry = entry.unwrap();
            // Hack to skip over the initially passed
            // directory, as this counts as an entry.
            // Find a better way to do this.
            if entry.depth() == 1 {
                let start = Instant::now();
                let (_, inner) = Wal::replay(entry.path()).expect("should exist");
                wal_replay_duration
                    .with_label_values(&["namespace"])
                    .observe(start.elapsed().as_secs_f64());
                multi.extend(inner);
            }
        }
        multi
    }
}

#[cfg(test)]
mod test {
    use crate::data_types::ServerMessage;

    use super::*;

    use tempfile::TempDir;

    #[test]
    fn directory_structure() {
        let dir = TempDir::new().unwrap();
        let multi = NamespaceWal::new(dir.path().to_path_buf(), None);

        let wal_dir = multi.create_wal_directory("my_new_namespace");
        assert_eq!(
            wal_dir.to_string_lossy(),
            format!("{}/my_new_namespace", dir.path().display())
        );
        assert!(std::fs::exists(&wal_dir).unwrap());
    }

    #[test]
    fn ops() {
        let dir = TempDir::new().unwrap();
        let multi = NamespaceWal::new(dir.path().to_path_buf(), None);

        multi.write(WalOperation::test_message(10)).unwrap();
        multi.flush("hello").unwrap();

        drop(multi);

        let previous_dir = format!("{}/hello", dir.path().display());
        let (_, inner) = Wal::replay(&previous_dir).unwrap();
        assert_eq!(inner.get("hello").unwrap().get_range(0).len(), 1);
        assert_eq!(
            inner.get("hello").unwrap().get_range(0),
            vec![ServerMessage::new("hello".to_string(), "world".into(), 10)]
        );
    }

    #[test]
    fn replay() {
        let dir = TempDir::new().unwrap();
        let multi = NamespaceWal::new(dir.path().to_path_buf(), None);

        multi
            .write(WalOperation::test_message(10).with_key("foo"))
            .unwrap();
        multi
            .write(WalOperation::test_message(20).with_key("bar"))
            .unwrap();

        multi.flush_all().unwrap();

        drop(multi);

        let multi = NamespaceWal::replay(dir.path());
        assert_eq!(multi.keys().len(), 2);
        assert_eq!(
            multi.get("foo").unwrap().get_range(0),
            vec![ServerMessage::new("foo".to_string(), "world".into(), 10)]
        );
        assert_eq!(
            multi.get("bar").unwrap().get_range(0),
            vec![ServerMessage::new("bar".to_string(), "world".into(), 20)]
        );
    }
}
