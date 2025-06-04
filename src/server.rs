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

#[cfg(test)]
mod test {
    use assert_matches::assert_matches;
    use tempfile::TempDir;
    use tonic::Request;

    use crate::{
        data_types::{
            proto::{
                message_server::Message, CreateNamespaceRequest, SendMessageRequest,
                SendMessageResponse,
            },
            ServerMessage,
        },
        server::BigPipeServer,
        BigPipe,
    };

    #[tokio::test]
    async fn server_send_message() {
        let wal_dir = TempDir::new().unwrap();
        let server =
            BigPipeServer::new(BigPipe::try_new(wal_dir.path().to_path_buf(), None).unwrap());

        let resp = server
            .send(Request::new(SendMessageRequest {
                key: "hello".to_string(),
                value: "world".into(),
            }))
            .await
            .unwrap();

        let bigpipe = server.inner.lock();
        let messages = bigpipe.messages();
        assert_eq!(messages.get("hello").unwrap().len(), 1);
        assert_matches!(messages.get("hello").unwrap()[0], ServerMessage { .. });
        assert!(messages.get("no_msg").is_none());
        assert_eq!(resp.into_inner(), SendMessageResponse {});
    }

    #[tokio::test]
    #[should_panic]
    async fn server_create_namespace() {
        let wal_dir = TempDir::new().unwrap();
        let server =
            BigPipeServer::new(BigPipe::try_new(wal_dir.path().to_path_buf(), None).unwrap());

        let namespace = CreateNamespaceRequest {
            key: "hello".to_string(),
        };
        let _ = server.create_namespace(Request::new(namespace)).await;
    }
}
