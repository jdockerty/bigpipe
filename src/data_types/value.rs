use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use super::message::ServerMessage;

#[derive(Debug, Clone, PartialEq, Eq, prost::Enumeration, clap::ValueEnum)]
pub enum RetentionPolicy {
    Ttl = 0,
    DiskPressure = 1,
}

#[derive(Debug, Clone)]
pub struct BigPipeValue {
    queue: Vec<ServerMessage>,
    length: Arc<AtomicU64>,
    retention_policy: RetentionPolicy,
}

impl Default for BigPipeValue {
    fn default() -> Self {
        Self::new()
    }
}

impl BigPipeValue {
    pub fn new() -> Self {
        Self {
            queue: Vec::with_capacity(100),
            length: Arc::new(AtomicU64::new(0)),
            retention_policy: RetentionPolicy::DiskPressure,
        }
    }

    pub fn len(&self) -> u64 {
        self.length.load(Ordering::Acquire)
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn retention_policy(&self) -> &RetentionPolicy {
        &self.retention_policy
    }

    pub fn set_retention_policy(&mut self, retention_policy: RetentionPolicy) {
        self.retention_policy = retention_policy;
    }

    pub fn push(&mut self, value: ServerMessage) {
        self.queue.push(value);
        self.length.fetch_add(1, Ordering::Release);
    }

    pub fn get(&self, offset: u64) -> Option<&ServerMessage> {
        self.queue.get(offset as usize)
    }

    pub fn get_range(&self, offset: u64) -> Vec<ServerMessage> {
        assert!(offset < self.len());
        let (_, after) = self.queue.split_at(offset as usize);
        after.to_vec()
    }
}

impl IntoIterator for BigPipeValue {
    type Item = ServerMessage;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.queue.into_iter()
    }
}

#[cfg(test)]
mod test {

    use crate::data_types::message::ServerMessage;

    use super::BigPipeValue;

    #[test]
    fn get_value() {
        let mut value = BigPipeValue::new();
        value.push(ServerMessage::test_message(100));
        assert_eq!(*value.get(0).unwrap(), ServerMessage::test_message(100));
        assert!(value.get(1).is_none());
    }

    #[test]
    fn get_value_range() {
        let mut value = BigPipeValue::new();

        let mut known_values = Vec::new();
        for i in 0..100 {
            let msg = ServerMessage::test_message(i);
            value.push(msg.clone());
            known_values.push(msg.clone());
        }

        assert_eq!(value.get_range(0), &known_values[..]);
        assert_eq!(value.get_range(5), &known_values[5..known_values.len()]);
        assert_eq!(value.get_range(99)[0], ServerMessage::test_message(99));
    }
}
