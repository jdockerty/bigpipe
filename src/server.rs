use parking_lot::Mutex;
use tonic::{Request, Response, Status};
use tracing::debug;

use crate::{
    data_types::{
        proto::{
            message_server::Message, CreateNamespaceRequest, CreateNamespaceResponse,
            SendMessageRequest, SendMessageResponse,
        },
        ServerMessage,
    },
    BigPipe,
};

/// A server which wraps an instance of [`BigPipe`] and exposes
/// it over HTTP/2 for incoming gRPC connections.
pub struct BigPipeServer {
    inner: Mutex<BigPipe>,
}

impl BigPipeServer {
    pub fn new(inner: BigPipe) -> Self {
        Self {
            inner: Mutex::new(inner),
        }
    }
}

#[tonic::async_trait]
impl Message for BigPipeServer {
    async fn send(
        &self,
        request: Request<SendMessageRequest>,
    ) -> Result<Response<SendMessageResponse>, Status> {
        let SendMessageRequest { key, value } = request.into_inner();
        debug!(key, value_size = value.len(), "received client message");
        let timestamp = chrono::Utc::now().timestamp_micros();
        self.inner
            .lock()
            .write(&ServerMessage::new(key, value.into(), timestamp))
            .unwrap();
        Ok(Response::new(SendMessageResponse {}))
    }

    async fn create_namespace(
        &self,
        _request: Request<CreateNamespaceRequest>,
    ) -> Result<Response<CreateNamespaceResponse>, Status> {
        unimplemented!();
    }
}
