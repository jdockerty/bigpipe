use tokio::sync::mpsc::{channel, Receiver, Sender};

pub struct RetentionHandle {
    tx: Sender<()>,
}

impl RetentionHandle {
    pub fn new() -> Self {
        let (tx, rx) = channel(100);
        tokio::spawn(start_retention(RetentionActor::new(rx)));
        Self { tx }
    }
}

struct RetentionActor {
    rx: Receiver<()>,
}

impl RetentionActor {
    fn new(rx: Receiver<()>) -> Self {
        Self { rx }
    }
}

async fn start_retention(mut actor: RetentionActor) {
    while let Some(_) = actor.rx.recv().await {
        todo!()
    }
}
