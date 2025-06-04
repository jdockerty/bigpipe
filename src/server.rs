use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::{
    data_types::proto::{
        message_server::Message, CreateNamespaceRequest, CreateNamespaceResponse,
        SendMessageRequest, SendMessageResponse,
    },
    BigPipe,
};

/// A server which wraps an instance of [`BigPipe`] and exposes
/// it over HTTP/2 for incoming gRPC connections.
pub struct BigPipeServer {
    inner: Arc<BigPipe>,
}

impl BigPipeServer {
    pub fn new(inner: BigPipe) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

#[tonic::async_trait]
impl Message for BigPipeServer {
    async fn send(
        &self,
        _request: Request<SendMessageRequest>,
    ) -> Result<Response<SendMessageResponse>, Status> {
        unimplemented!();
    }

    async fn create_namespace(
        &self,
        _request: Request<CreateNamespaceRequest>,
    ) -> Result<Response<CreateNamespaceResponse>, Status> {
        unimplemented!();
    }
}
