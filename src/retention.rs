use hashbrown::HashMap;
use parking_lot::RwLock;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::log::WAL_EXTENSION;
use crate::{data_types::namespace::Namespace, log::SegmentId};

pub const DEFAULT_RETENTION_MAX_BYTES: u64 = 1024 * 1024 * 1024; // 1 GiB
pub const DEFAULT_RETENTION_MAX_AGE: Duration = Duration::from_secs(4 * 60 * 60); // 4 hours
pub const DEFAULT_RETENTION_CHECK_INTERVAL: Duration = Duration::from_secs(5);

/// Configuration for retention
#[derive(Debug, Clone)]
pub struct RetentionConfig {
    /// Maximum total size in bytes for all segments
    pub max_bytes: Option<u64>,
    /// Maximum age for segments before deletion
    pub max_age: Option<Duration>,
    /// How often to check retention policies
    pub check_interval: Duration,
}

impl RetentionConfig {
    pub fn new(max_bytes: u64, max_age: Duration, check_interval: Duration) -> Self {
        Self {
            max_bytes: Some(max_bytes),
            max_age: Some(max_age),
            check_interval,
        }
    }
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            max_bytes: Some(DEFAULT_RETENTION_MAX_BYTES),
            max_age: Some(DEFAULT_RETENTION_MAX_AGE),
            check_interval: DEFAULT_RETENTION_CHECK_INTERVAL,
        }
    }
}

/// A [`RetentionEnforcer`] will enforce the disk pressure and time-to-live (TTL)
/// policy over a namespace contained at the specified directory.
#[derive(Debug, Clone)]
pub struct RetentionEnforcer {
    config: RetentionConfig,
    directory: PathBuf,
}

impl RetentionEnforcer {
    pub fn new(directory: PathBuf, config: RetentionConfig) -> Self {
        Self { directory, config }
    }

    /// Apply retention policy to a list of closed segments
    /// Returns segments that should be deleted
    pub fn evaluate(&self, segments: &[SegmentId]) -> Result<Vec<SegmentId>, std::io::Error> {
        let mut segments_to_delete = Vec::new();
        info!(directory = %self.directory.display(), "retention evaluation");

        // Apply disk pressure retention if configured
        if let Some(max_bytes) = self.config.max_bytes {
            let deletions = self.apply_size_retention(segments, max_bytes)?;
            segments_to_delete.extend(deletions);
        }

        // Apply TTL retention if configured
        if let Some(max_age) = self.config.max_age {
            let deletions = self.apply_time_retention(segments, max_age)?;
            segments_to_delete.extend(deletions);
        }
        Ok(segments_to_delete)
    }

    /// Delete segments based on total size limit
    fn apply_size_retention(
        &self,
        segments: &[SegmentId],
        max_bytes: u64,
    ) -> Result<Vec<SegmentId>, std::io::Error> {
        let mut segments_with_size: Vec<(SegmentId, u64)> = Vec::new();
        let mut total_size = 0;

        // Get size and modification time for each segment
        for segment in segments {
            let path = self.segment_path(segment);
            let metadata = std::fs::metadata(&path)?;
            let size = metadata.len();

            segments_with_size.push((segment.clone(), size));
            total_size += size;
        }

        if total_size <= max_bytes {
            return Ok(Vec::new());
        }

        // Sort by [`SegmentId`] as these are monotonically increasing and
        // implies older segments have a lower ID value.
        segments_with_size.sort_by_key(|(id, _)| id.get());

        let mut to_delete = Vec::new();
        let mut bytes_to_free = total_size - max_bytes;

        for (segment, size) in segments_with_size {
            if bytes_to_free == 0 {
                break;
            }

            to_delete.push(segment.clone());
            bytes_to_free = bytes_to_free.saturating_sub(size);

            debug!(
                segment = ?segment,
                size_bytes = size,
                "Marking segment for deletion due to size limit"
            );
        }

        Ok(to_delete)
    }

    /// Delete segments based on ttl
    fn apply_time_retention(
        &self,
        segments: &[SegmentId],
        max_age: Duration,
    ) -> Result<Vec<SegmentId>, std::io::Error> {
        let mut to_delete = Vec::new();
        let now = SystemTime::now();

        for segment in segments {
            let path = self.segment_path(segment);
            let metadata = std::fs::metadata(&path)?;
            let modified = metadata.modified()?;

            if let Ok(age) = now.duration_since(modified) {
                if age > max_age {
                    to_delete.push(segment.clone());
                    debug!(
                        segment = ?segment,
                        age_seconds = age.as_secs(),
                        "Marking segment for deletion due to ttl"
                    );
                }
            }
        }

        Ok(to_delete)
    }

    /// Delete the specified segments from disk
    pub fn delete_segments(&self, segments: &[SegmentId]) -> Result<(), std::io::Error> {
        for segment in segments {
            let path = self.segment_path(segment);
            if path.exists() {
                std::fs::remove_file(&path)?;
                info!(segment = ?segment, "Deleted segment");
            }
        }
        Ok(())
    }

    fn segment_path(&self, segment: &SegmentId) -> PathBuf {
        self.directory
            .join(format!("{}{WAL_EXTENSION}", segment.get()))
    }
}

#[derive(Debug)]
/// Manager for retention enforcement across multiple namespaces.
pub struct RetentionManager {
    enforcers: Arc<RwLock<HashMap<Namespace, (RetentionEnforcer, JoinHandle<()>)>>>,
}

impl RetentionManager {
    pub fn new() -> Self {
        Self {
            enforcers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Start retention for a new [`Namespace`].
    pub fn add_namespace(
        &self,
        namespace: Namespace,
        log_directory: PathBuf,
        config: RetentionConfig,
        // TODO: SegmentProvider trait to capture this behaviour?
        segment_provider: impl Fn() -> Vec<SegmentId> + Send + Sync + 'static,
    ) {
        let policy = RetentionEnforcer::new(log_directory.clone(), config.clone());

        let policy_internal = policy.clone();
        let namespace_internal = namespace.clone();
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.check_interval);
            loop {
                interval.tick().await;

                let segments = segment_provider();
                if segments.is_empty() {
                    continue;
                }

                match policy_internal.evaluate(&segments) {
                    Ok(to_delete) => {
                        if !to_delete.is_empty() {
                            info!(
                                namespace = %namespace_internal.clone().inner(),
                                count = to_delete.len(),
                                "Applying retention policy"
                            );

                            if let Err(e) = policy_internal.delete_segments(&to_delete) {
                                warn!(
                                    namespace = %namespace_internal.inner(),
                                    error = %e,
                                    "Failed to delete segments"
                                );
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            namespace = %namespace_internal.inner(),
                            error = %e,
                            "Failed to evaluate retention policy"
                        );
                    }
                }
            }
        });

        self.enforcers
            .write()
            .insert(namespace.clone(), (policy, handle));
    }

    /// Stop retention management for a namespace, returning the [`Namespace`] if it
    /// has been removed from retention management.
    pub fn remove_namespace(&self, namespace: &Namespace) -> Option<Namespace> {
        if let Some((_, handle)) = self.enforcers.write().remove(namespace) {
            handle.abort();
            Some(namespace.clone())
        } else {
            None
        }
    }

    pub fn contains_namespace(&self, namespace: &Namespace) -> bool {
        self.enforcers.read().contains_key(namespace)
    }

    /// Force a retention check against a namespace and specified
    /// segments.
    fn check_retention(
        &self,
        namespace: &Namespace,
        segments: &[SegmentId],
    ) -> Result<Option<Vec<SegmentId>>, std::io::Error> {
        let policies = self.enforcers.read();

        if let Some((policy, _)) = policies.get(namespace) {
            policy.evaluate(segments).map(|s| Some(s))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use std::path::Path;
    use tempfile::TempDir;

    fn create_test_segment(dir: &Path, id: u64, size: usize, age_secs: u64) -> SegmentId {
        let path = dir.join(format!("{}-bp.wal", id));
        let mut file = File::create(&path).unwrap();
        // Write junk data, it doesn't have to be a [`ServerMessage`]
        // for retention tests.
        file.write_all(&vec![0u8; size]).unwrap();
        file.sync_all().unwrap();

        let modified_time = SystemTime::now() - Duration::from_secs(age_secs);
        file.set_modified(modified_time).unwrap();

        SegmentId::new(id)
    }

    #[test]
    fn retain_by_size() {
        let dir = TempDir::new().unwrap();
        let segments = vec![
            create_test_segment(dir.path(), 1, 1000, 3600),
            create_test_segment(dir.path(), 2, 1000, 1800),
            create_test_segment(dir.path(), 3, 1000, 900),
        ];

        let config = RetentionConfig {
            max_bytes: Some(2000), // Keep only 2KB
            max_age: None,
            check_interval: Duration::from_secs(1),
        };

        let policy = RetentionEnforcer::new(dir.path().to_path_buf(), config);
        let to_delete = policy.evaluate(&segments).unwrap();

        // Should delete the oldest segment (id=1)
        assert_eq!(to_delete.len(), 1);
        assert_eq!(to_delete[0].get(), 1);
    }

    #[test]
    fn retain_by_ttl() {
        let dir = TempDir::new().unwrap();
        let segments = vec![
            create_test_segment(dir.path(), 1, 1000, 7200), // 2 hours old
            create_test_segment(dir.path(), 2, 1000, 3600), // 1 hour old
            create_test_segment(dir.path(), 3, 1000, 1800), // 30 minutes old
        ];

        let config = RetentionConfig {
            max_bytes: None,
            max_age: Some(Duration::from_secs(5400)), // 1.5 hours
            check_interval: Duration::from_secs(1),
        };

        let policy = RetentionEnforcer::new(dir.path().to_path_buf(), config);
        let to_delete = policy.evaluate(&segments).unwrap();

        // SegmentId=1 should be the only output.
        // It is over the 1.5 hour retention window.
        assert_eq!(to_delete.len(), 1);
        assert_eq!(to_delete[0].get(), 1);
    }

    #[tokio::test]
    async fn retention_manager() {
        let dir = TempDir::new().unwrap();
        let namespace = Namespace::new("test");
        let namespace_dir = dir.path().join("test");
        std::fs::create_dir(&namespace_dir).unwrap();

        let segments = vec![
            create_test_segment(&namespace_dir, 1, 1000, 3600),
            create_test_segment(&namespace_dir, 2, 1000, 1800),
        ];

        let config = RetentionConfig {
            max_bytes: Some(1500),
            max_age: None,
            check_interval: Duration::from_secs(1),
        };

        let manager = RetentionManager::new();

        manager.add_namespace(
            namespace.clone(),
            namespace_dir.clone(),
            config.clone(),
            || vec![],
        );

        let to_delete = manager.check_retention(&namespace, &segments).unwrap();
        assert_eq!(to_delete.expect("contains namespace").len(), 1);
        assert_eq!(manager.remove_namespace(&namespace), Some(namespace));

        let not_exist = manager
            .check_retention(&Namespace::new("not_exists"), &segments)
            .unwrap();
        assert!(
            not_exist.is_none(),
            "Should return None for non-existent namespace"
        );
    }
}
