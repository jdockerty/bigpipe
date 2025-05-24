use bytes::Bytes;
use hashbrown::HashMap;
use parking_lot::Mutex;

use serde::{Deserialize, Serialize};

/// A message which has been received by the queue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    key: String,
    value: Bytes,
    timestamp: u64,
}

impl Message {
    pub fn new(key: String, value: Bytes) -> Self {
        Self {
            key,
            value,
            timestamp: 0,
        }
    }
}

#[derive(Debug)]
pub struct BigPipe {
    /// Internal queue to hold ordered messages as they are received,
    /// partitioned by their key.
    queue: Mutex<HashMap<String, Vec<Message>>>,
}

impl Default for BigPipe {
    fn default() -> Self {
        Self::new()
    }
}

impl BigPipe {
    pub fn new() -> Self {
        Self {
            queue: Mutex::new(HashMap::with_capacity(100)),
        }
    }

    pub fn add_message(&mut self, message: Message) {
        self.queue
            .lock()
            .entry(message.key.clone())
            .and_modify(|messages| messages.push(message.clone()))
            .or_insert_with(|| {
                let mut messages = Vec::with_capacity(100);
                messages.push(message);
                messages
            });
    }

    pub fn messages(&self) -> HashMap<String, Vec<Message>> {
        let guard = self.queue.lock();
        guard.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::{BigPipe, Message};

    #[test]
    fn add_messages() {
        let mut q = BigPipe::new();

        let msg_1 = Message {
            key: "hello".to_string(),
            value: b"world".to_vec().into(),
            timestamp: 0,
        };

        let msg_2 = Message {
            key: "test".to_string(),
            value: b"value".to_vec().into(),
            timestamp: 1,
        };

        for msg in [msg_1.clone(), msg_2.clone()] {
            q.add_message(msg);
        }

        let messages = q.messages();
        assert_eq!(messages.keys().len(), 2);
        assert_eq!(messages.get(&msg_1.key).unwrap()[0], msg_1);
        assert_eq!(messages.get(&msg_2.key).unwrap()[0], msg_2);
    }
}
