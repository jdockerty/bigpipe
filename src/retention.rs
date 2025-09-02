use hashbrown::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::{data_types::namespace::Namespace, log::SegmentId};

/// Configuration for retention policies
#[derive(Debug, Clone)]
pub struct RetentionConfig {
    /// Maximum total size in bytes for all segments
    pub max_bytes: Option<u64>,
    /// Maximum age for segments before deletion
    pub max_age: Option<Duration>,
    /// How often to check retention policies
    pub check_interval: Duration,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            max_bytes: Some(1024 * 1024 * 1024),             // 1GiB
            max_age: Some(Duration::from_secs(4 * 60 * 60)), // 4 hours
            check_interval: Duration::from_secs(60),         // Every minute
        }
    }
}

/// Core retention policy enforcement
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

        // Deduplicate
        segments_to_delete.sort();
        segments_to_delete.dedup();

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
        self.directory.join(format!("{}-bp.wal", segment.get()))
    }
}

/// Manager for retention across multiple namespaces
pub struct RetentionManager {
    policies: Arc<RwLock<HashMap<Namespace, RetentionEnforcer>>>,
    handles: HashMap<Namespace, tokio::task::JoinHandle<()>>,
}

impl RetentionManager {
    pub fn new() -> Self {
        Self {
            policies: Arc::new(RwLock::new(HashMap::new())),
            handles: HashMap::new(),
        }
    }

    /// Start retention for a new [`Namespace`].
    pub async fn add_namespace(
        &mut self,
        namespace: Namespace,
        log_directory: PathBuf,
        config: RetentionConfig,
        segment_provider: impl Fn() -> Vec<SegmentId> + Send + Sync + 'static,
    ) {
        let directory = log_directory.join(namespace.inner());
        let policy = RetentionEnforcer::new(directory.clone(), config.clone());

        self.policies
            .write()
            .await
            .insert(namespace.clone(), policy);

        let policies = Arc::clone(&self.policies);
        let namespace_clone = namespace.clone();

        // Spawn background task for this namespace
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.check_interval);

            loop {
                interval.tick().await;

                let segments = segment_provider();
                if segments.is_empty() {
                    continue;
                }

                // Get policy and evaluate
                let policies = policies.read().await;
                if let Some(policy) = policies.get(&namespace_clone) {
                    match policy.evaluate(&segments) {
                        Ok(to_delete) => {
                            if !to_delete.is_empty() {
                                info!(
                                    namespace = %namespace_clone.inner(),
                                    count = to_delete.len(),
                                    "Applying retention policy"
                                );

                                if let Err(e) = policy.delete_segments(&to_delete) {
                                    warn!(
                                        namespace = %namespace_clone.inner(),
                                        error = %e,
                                        "Failed to delete segments"
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            warn!(
                                namespace = %namespace_clone.inner(),
                                error = %e,
                                "Failed to evaluate retention policy"
                            );
                        }
                    }
                }
            }
        });

        self.handles.insert(namespace, handle);
    }

    /// Stop retention management for a namespace
    pub fn remove_namespace(&mut self, namespace: &Namespace) {
        if let Some(handle) = self.handles.remove(namespace) {
            handle.abort();
        }
    }

    /// Force a retention check against a namespace and specified
    /// segments.
    pub async fn check_retention(
        &self,
        namespace: &Namespace,
        segments: &[SegmentId],
    ) -> Result<Vec<SegmentId>, std::io::Error> {
        let policies = self.policies.read().await;

        if let Some(policy) = policies.get(namespace) {
            policy.evaluate(segments)
        } else {
            Ok(Vec::new())
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

        manager.policies.write().await.insert(
            namespace.clone(),
            RetentionEnforcer::new(namespace_dir, config),
        );

        let to_delete = manager
            .check_retention(&namespace, &segments)
            .await
            .unwrap();
        assert_eq!(to_delete.len(), 1);
    }
}
