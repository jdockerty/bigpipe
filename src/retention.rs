use std::{path::PathBuf, time::Duration};

use hashbrown::HashMap;
use tokio::{
    sync::mpsc::{self, Sender},
    task::JoinHandle,
};
use tracing::{info, warn};

use crate::data_types::namespace::Namespace;

pub struct RetentionManager {
    namespaces: HashMap<Namespace, RetentionWorker>,
}

impl RetentionManager {
    pub fn new(namespaces: impl Iterator<Item = Namespace>, log_directory: PathBuf) -> Self {
        Self {
            namespaces: HashMap::from_iter(namespaces.map(|n| {
                (
                    n.clone(),
                    RetentionWorker::new(log_directory.join(n.inner())),
                )
            })),
        }
    }
}

struct RetentionWorker {
    directory: PathBuf,
    task: JoinHandle<()>,
}

impl RetentionWorker {
    pub fn new(directory: PathBuf) -> Self {
        let task = tokio::spawn(run_retention(directory.clone()));
        Self { directory, task }
    }
}

async fn run_retention(directory: PathBuf) {
    info!(directory = %directory.display(), "starting retention task");
    loop {
        tokio::select! {
            _ = run_disk_pressure(directory.clone()) => warn!(directory = %directory.display(), "stopping disk pressure"),
            _ = run_ttl(directory.clone()) => warn!(directory = %directory.display(), "stopping tll"),
        }
    }
}

async fn run_disk_pressure(directory: PathBuf) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    let max_size: u64 = 1024 * 1024;
    // TODO: pass closed segments via channel?
    loop {
        interval.tick().await;
    }
}
async fn run_ttl(directory: PathBuf) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        interval.tick().await;
    }
}
