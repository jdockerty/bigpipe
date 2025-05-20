use parking_lot::Mutex;

use serde::{Deserialize, Serialize};

/// A message which has been received by the queue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    key: String,
    value: Vec<u8>,
    timestamp: u64,
}

impl Message {
    pub fn new(key: String, value: Vec<u8>) -> Self {
        Self {
            key,
            value,
            timestamp: 0,
        }
    }
}

#[derive(Debug)]
pub struct BigPipe {
    /// Internal queue to hold ordered messages as they are received.
    queue: Mutex<Vec<Message>>,
}

impl Default for BigPipe {
    fn default() -> Self {
        Self::new()
    }
}

impl BigPipe {
    pub fn new() -> Self {
        Self {
            queue: Mutex::new(Vec::with_capacity(100)),
        }
    }

    pub fn add_message(&self, message: Message) {
        self.queue.lock().push(message);
    }

    pub fn messages(&self) -> Vec<Message> {
        let guard = self.queue.lock();
        guard.clone() // snapshot of messages at the time of call, not important for now.
    }
}

#[cfg(test)]
mod tests {
    use crate::{BigPipe, Message};

    #[test]
    fn add_messages() {
        let q = BigPipe::new();

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
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0], msg_1);
        assert_eq!(messages[1], msg_2);
    }
}
