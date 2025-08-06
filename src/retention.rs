use std::{sync::Arc, time::Duration};

use hashbrown::HashMap;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tracing::info;

use crate::data_types::{BigPipeValue, Namespace};

#[derive(Debug)]
pub struct RetentionManager {
    namespaces: HashMap<Namespace, RetentionHandle>,
}

impl RetentionManager {
    pub fn new() -> Self {
        Self {
            namespaces: HashMap::new(),
        }
    }

    pub async fn check_policy(&mut self, message: RetentionMessage) {
        match message {
            RetentionMessage::Ttl {
                namespace,
                max_duration,
                values,
            } => {
                let handle = self
                    .namespaces
                    .entry(namespace.clone())
                    .or_insert(RetentionHandle::new());
                handle
                    .send_message(RetentionMessage::Ttl {
                        namespace,
                        max_duration,
                        values,
                    })
                    .await;
            }
            RetentionMessage::DiskPressure { namespace } => {
                let handle = self
                    .namespaces
                    .entry(namespace.clone())
                    .or_insert(RetentionHandle::new());
                handle
                    .send_message(RetentionMessage::DiskPressure { namespace })
                    .await;
            }
        }
    }
}

#[derive(Debug)]
pub enum RetentionMessage {
    Ttl {
        namespace: Namespace,
        max_duration: Duration,
        values: Arc<BigPipeValue>,
    },
    DiskPressure {
        namespace: Namespace,
    },
}

#[derive(Debug)]
pub struct RetentionHandle {
    tx: Sender<RetentionMessage>,
}

impl RetentionHandle {
    pub fn new() -> Self {
        let (tx, rx) = channel(100);
        tokio::spawn(start_retention(RetentionActor::new(rx)));
        Self { tx }
    }

    async fn send_message(&self, message: RetentionMessage) {
        self.tx.send(message).await.unwrap()
    }
}

struct RetentionActor {
    rx: Receiver<RetentionMessage>,
}

impl RetentionActor {
    fn new(rx: Receiver<RetentionMessage>) -> Self {
        Self { rx }
    }
}

async fn start_retention(mut actor: RetentionActor) {
    while let Some(msg) = actor.rx.recv().await {
        info!(?msg);
    }
}
