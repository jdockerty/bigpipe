use std::fmt::Display;
use std::sync::Arc;

pub mod namespace {
    tonic::include_proto!("namespace");
}

/// Representation of a 'namespace' within BigPipe.
///
/// A namespace is used to partition incoming data
/// by the provided name. This means that is also
/// used to access data for a particular partition
/// too.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct Namespace(Arc<String>);

impl Namespace {
    pub fn new(namespace: &str) -> Self {
        Self(Arc::new(namespace.to_string()))
    }
}

impl Clone for Namespace {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl Display for Namespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for Namespace {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}
