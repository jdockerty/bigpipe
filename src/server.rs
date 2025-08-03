use std::{pin::Pin, sync::Arc};

use parking_lot::Mutex;
use prometheus::{HistogramOpts, HistogramVec, Registry};
use tokio::time::Instant;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{Code, Request, Response, Status};
use tracing::{debug, error, info, warn};

use crate::{
    data_types::{
        message::{
            message_server::Message, ReadMessageRequest, ReadMessageResponse, SendMessageRequest,
            SendMessageResponse,
        },
        namespace::{
            namespace_server::Namespace, CreateNamespaceRequest, CreateNamespaceResponse,
            UpdateNamespaceRequest, UpdateNamespaceResponse,
        },
        BigPipeValue, RetentionPolicy, ServerMessage,
    },
    BigPipe,
};

/// A server which wraps an instance of [`BigPipe`] and exposes
/// it over HTTP/2 for incoming gRPC connections.
pub struct BigPipeServer {
    inner: Mutex<BigPipe>,

    ingest_path_duration: HistogramVec,
    read_path_duration: HistogramVec,
}

impl BigPipeServer {
    pub fn new(inner: BigPipe, metrics: &Registry) -> Self {
        let ingest_path_duration = HistogramVec::new(
            HistogramOpts::new(
                "bigpipe_ingest_path_duration_seconds",
                "Time taken for a write to traverse the full ingest path and a response is returned to the caller",
            ),
            &["outcome"],
        )
        .unwrap();

        let read_path_duration = HistogramVec::new(
            HistogramOpts::new(
                "bigpipe_read_path_duration_seconds",
                "Time taken for a read to be surfaced",
            ),
            &["outcome"],
        )
        .unwrap();

        metrics
            .register(Box::new(ingest_path_duration.clone()))
            .unwrap();
        metrics
            .register(Box::new(read_path_duration.clone()))
            .unwrap();
        Self {
            inner: Mutex::new(inner),
            ingest_path_duration,
            read_path_duration,
        }
    }
}

#[tonic::async_trait]
impl Message for Arc<BigPipeServer> {
    type ReadStream =
        Pin<Box<dyn Stream<Item = Result<ReadMessageResponse, Status>> + Send + 'static>>;

    async fn send(
        &self,
        request: Request<SendMessageRequest>,
    ) -> Result<Response<SendMessageResponse>, Status> {
        let write_start = Instant::now();
        let SendMessageRequest { key, value } = request.into_inner();
        debug!(key, value_size = value.len(), "received client message");
        let timestamp = chrono::Utc::now().timestamp_micros();
        match self
            .inner
            .lock()
            .write(&ServerMessage::new(key, value.into(), timestamp))
        {
            Ok(_) => self
                .ingest_path_duration
                .with_label_values(&["success"])
                .observe(write_start.elapsed().as_secs_f64()),
            Err(e) => {
                warn!(?e, "unable to write message");
                self.ingest_path_duration
                    .with_label_values(&["error"])
                    .observe(write_start.elapsed().as_secs_f64());
                return Err(Status::new(Code::Internal, "unable to process write"));
            }
        };
        Ok(Response::new(SendMessageResponse {}))
    }

    async fn read(
        &self,
        request: Request<ReadMessageRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let read_start = Instant::now();
        debug!(
            remote_addr = %request.remote_addr().unwrap(),
            "client connection"
        );
        let ReadMessageRequest { key, offset } = request.into_inner();
        match self.inner.lock().get_message_range(&key, offset) {
            Some(messages) => {
                let messages = messages
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
                            Ok(_) => {} // sent to client
                            Err(e) => {
                                error!(?e, "unable to send response part");
                                break;
                            }
                        }
                    }
                    info!("client disconnected");
                });
                let output_stream = ReceiverStream::new(rx);
                self.read_path_duration
                    .with_label_values(&["success"])
                    .observe(read_start.elapsed().as_secs_f64());
                Ok(Response::new(Box::pin(output_stream)))
            }
            None => {
                self.read_path_duration
                    .with_label_values(&["not_found"])
                    .observe(read_start.elapsed().as_secs_f64());
                Err(Status::new(Code::NotFound, format!("{key} not found")))
            }
        }
    }
}

#[tonic::async_trait]
impl Namespace for Arc<BigPipeServer> {
    async fn create(
        &self,
        request: Request<CreateNamespaceRequest>,
    ) -> Result<Response<CreateNamespaceResponse>, Status> {
        let CreateNamespaceRequest {
            key,
            retention_policy: _,
        } = request.into_inner();

        match self.inner.lock().inner.entry(key.clone()) {
            hashbrown::hash_map::Entry::Occupied(_) => Err(Status::new(
                Code::AlreadyExists,
                format!("{key} already exists"),
            )),
            hashbrown::hash_map::Entry::Vacant(e) => {
                e.insert(BigPipeValue::new());
                Ok(Response::new(CreateNamespaceResponse { key }))
            }
        }
    }

    async fn update(
        &self,
        request: Request<UpdateNamespaceRequest>,
    ) -> Result<Response<UpdateNamespaceResponse>, Status> {
        let UpdateNamespaceRequest {
            key,
            retention_policy,
        } = request.into_inner();

        let retention_policy = RetentionPolicy::try_from(retention_policy).unwrap();

        match self.inner.lock().inner.get_key_value_mut(&key) {
            Some((_, value)) => {
                value.set_retention_policy(retention_policy);
                Ok(Response::new(UpdateNamespaceResponse { key }))
            }
            None => Err(Status::new(
                Code::NotFound,
                format!("cannot update non-existent namespace '{key}'"),
            )),
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use prometheus::Registry;
    use tempfile::TempDir;
    use tokio_stream::StreamExt;
    use tonic::{Code, Request};

    use crate::{
        data_types::{
            message::{
                message_server::Message, ReadMessageRequest, ReadMessageResponse,
                SendMessageRequest, SendMessageResponse,
            },
            namespace::{
                namespace_server::Namespace, CreateNamespaceRequest, UpdateNamespaceRequest,
                UpdateNamespaceResponse,
            },
            RetentionPolicy, ServerMessage,
        },
        server::BigPipeServer,
        BigPipe,
    };

    #[tokio::test]
    async fn send_message() {
        let wal_dir = TempDir::new().unwrap();
        let metrics = Registry::new();
        let server = Arc::new(BigPipeServer::new(
            BigPipe::try_new(wal_dir.path().to_path_buf(), None, &metrics).unwrap(),
            &metrics,
        ));

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
        assert_matches!(
            messages.get("hello").unwrap().get(0).unwrap(),
            ServerMessage { .. }
        );
        assert!(messages.get("no_msg").is_none());
        assert_eq!(resp.into_inner(), SendMessageResponse {});
    }

    #[tokio::test]
    async fn read_messages() {
        let wal_dir = TempDir::new().unwrap();
        let metrics = Registry::new();
        let server = Arc::new(BigPipeServer::new(
            BigPipe::try_new(wal_dir.path().to_path_buf(), None, &metrics).unwrap(),
            &metrics,
        ));

        for i in 0..10 {
            server
                .inner
                .lock()
                .write(&ServerMessage::test_message(i))
                .unwrap();
        }
        server.inner.lock().wal.flush("hello").unwrap();

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
            server.inner.lock().get_messages("hello").unwrap().len(),
            10,
            "The 'hello' key should contain a total of 10 messages"
        );
        assert_eq!(
            partial_messages.len(),
            2,
            "An offset of 8 expects only 2 messages to be produced"
        );

        match server
            .read(Request::new(ReadMessageRequest {
                key: "not_found".to_string(),
                offset: 0,
            }))
            .await
        {
            Ok(_) => panic!("read against non-existent key should not exist"),
            Err(e) => assert_eq!(e.code(), Code::NotFound),
        }
    }

    #[tokio::test]
    async fn create_namespace() {
        let wal_dir = TempDir::new().unwrap();
        let metrics = Registry::new();
        let server = Arc::new(BigPipeServer::new(
            BigPipe::try_new(wal_dir.path().to_path_buf(), None, &metrics).unwrap(),
            &metrics,
        ));

        let namespace = CreateNamespaceRequest {
            key: "hello".to_string(),
            retention_policy: RetentionPolicy::Ttl as i32,
        };
        let resp = server
            .create(Request::new(namespace.clone()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            resp.key, namespace.key,
            "Returned key should be the same as what was provided"
        );

        let resp = server
            .create(Request::new(namespace.clone()))
            .await
            .unwrap_err();
        assert_eq!(
            resp.code(),
            Code::AlreadyExists,
            "Cannot create namespace which already exists"
        );
    }

    #[tokio::test]
    async fn update_namespace() {
        let wal_dir = TempDir::new().unwrap();
        let metrics = Registry::new();
        let server = Arc::new(BigPipeServer::new(
            BigPipe::try_new(wal_dir.path().to_path_buf(), None, &metrics).unwrap(),
            &metrics,
        ));

        let update = UpdateNamespaceRequest {
            key: "hello".to_string(),
            retention_policy: RetentionPolicy::Ttl as i32,
        };
        let resp = server
            .update(Request::new(update.clone()))
            .await
            .unwrap_err();
        assert_eq!(
            resp.code(),
            Code::NotFound,
            "Not possible to update a namespace which does not exist"
        );

        server
            .create(Request::new(CreateNamespaceRequest {
                key: update.key.clone(),
                retention_policy: update.retention_policy,
            }))
            .await
            .unwrap();

        let new_update = UpdateNamespaceRequest {
            retention_policy: RetentionPolicy::DiskPressure as i32,
            key: update.key.clone(),
        };

        let resp = server
            .update(Request::new(new_update.clone()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            resp,
            UpdateNamespaceResponse {
                key: new_update.key.clone()
            }
        );

        assert_eq!(
            server
                .inner
                .lock()
                .get_messages(&new_update.key)
                .unwrap()
                .retention_policy(),
            &crate::data_types::RetentionPolicy::DiskPressure
        );
    }
}
