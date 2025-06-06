use std::pin::Pin;

use parking_lot::Mutex;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{Request, Response, Status};
use tracing::{debug, error, info};

use crate::{
    data_types::{
        proto::{
            message_server::Message, CreateNamespaceRequest, CreateNamespaceResponse,
            ReadMessageRequest, ReadMessageResponse, SendMessageRequest, SendMessageResponse,
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
    type ReadStream =
        Pin<Box<dyn Stream<Item = Result<ReadMessageResponse, Status>> + Send + 'static>>;

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

    async fn read(
        &self,
        request: Request<ReadMessageRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        debug!(
            remote_addr = %request.remote_addr().unwrap(),
            "client connection"
        );
        let ReadMessageRequest { key, offset } = request.into_inner();

        let messages = self
            .inner
            .lock()
            .get_message_range(&key, offset as u64)
            .iter()
            .map(|m| ReadMessageResponse {
                key: m.key().to_string(),
                value: m.value().to_vec(),
            })
            .collect::<Vec<_>>();

        let mut stream = Box::pin(tokio_stream::iter(messages));

        let (tx, rx) = tokio::sync::mpsc::channel(100);
        tokio::spawn(async move {
            // Read items from the server stream to send into the response stream
            while let Some(item) = stream.next().await {
                match tx.send(Ok::<ReadMessageResponse, Status>(item)).await {
                    Ok(_) => {}
                    Err(e) => {
                        error!(?e, "unable to send response part");
                        break;
                    }
                }
            }
            info!("client disconnected");
        });

        let output_stream = ReceiverStream::new(rx);

        Ok(Response::new(Box::pin(output_stream)))
    }
}

#[cfg(test)]
mod test {
    use assert_matches::assert_matches;
    use tempfile::TempDir;
    use tokio_stream::StreamExt;
    use tonic::Request;

    use crate::{
        data_types::{
            proto::{
                message_server::Message, CreateNamespaceRequest, ReadMessageRequest,
                ReadMessageResponse, SendMessageRequest, SendMessageResponse,
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
        assert_matches!(messages.get("hello").unwrap().get(0), ServerMessage { .. });
        assert!(messages.get("no_msg").is_none());
        assert_eq!(resp.into_inner(), SendMessageResponse {});
    }

    #[tokio::test]
    async fn server_read_messages() {
        let wal_dir = TempDir::new().unwrap();
        let server =
            BigPipeServer::new(BigPipe::try_new(wal_dir.path().to_path_buf(), None).unwrap());

        for i in 0..10 {
            server
                .inner
                .lock()
                .write(&ServerMessage::test_message(i))
                .unwrap();
        }
        server.inner.lock().wal.flush().unwrap();

        let messages = server
            .read(Request::new(ReadMessageRequest {
                key: "hello".to_string(),
                offset: 0,
            }))
            .await
            .unwrap()
            .into_inner();

        let all_messages = messages.collect::<Vec<_>>().await;
        assert_eq!(all_messages.len(), 10);
        for message in all_messages {
            let message = message.unwrap();
            assert_eq!(
                message.clone(),
                ReadMessageResponse {
                    key: message.key,
                    value: message.value
                }
            );
        }

        let messages = server
            .read(Request::new(ReadMessageRequest {
                key: "hello".to_string(),
                offset: 8,
            }))
            .await
            .unwrap()
            .into_inner();

        let partial_messages = messages.collect::<Vec<_>>().await;
        assert_eq!(
            partial_messages.len(),
            2,
            "An offset of 8 expects only 2 messages to be produced"
        );
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
