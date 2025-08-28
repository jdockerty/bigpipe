use std::fmt::Display;
use std::sync::Arc;

use super::value::RetentionPolicy;

// tonic requires that this is contained within a module
// that has the same name as the proto file.
//
// This is renamed to NAME_proto in the `data_types` module file.
#[expect(clippy::module_inception)]
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

#[allow(dead_code)]
pub struct NamespaceConfig {
    name: Namespace,
    retention_policy: RetentionPolicy,
}
