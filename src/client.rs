use tokio_stream::{Stream, StreamExt};
use tonic::{transport::Channel, Status};

use crate::data_types::{
    message::{
        message_client::MessageClient, ReadMessageRequest, ReadMessageResponse, SendMessageRequest,
    },
    ClientMessage,
};

pub struct BigPipeClient {
    client: MessageClient<Channel>,
}

impl BigPipeClient {
    /// Construct a new [`BigPipeClient`].
    pub async fn new(address: &str) -> Self {
        let client = MessageClient::connect(address.to_string()).await.unwrap();
        Self { client }
    }

    /// Send a message to the configured server.
    pub async fn send(&mut self, message: ClientMessage) -> Result<(), Box<dyn std::error::Error>> {
        self.client
            .send(SendMessageRequest {
                key: message.key().to_string(),
                value: message.value().to_vec(),
            })
            .await
            .unwrap();
        Ok(())
    }

    /// Stream messages for a key, from a particular offset.
    pub async fn read(
        &mut self,
        key: &str,
        offset: u64,
    ) -> Result<impl Stream<Item = Result<ReadMessageResponse, Status>>, Box<dyn std::error::Error>>
    {
        Ok(self
            .client
            .read(ReadMessageRequest {
                key: key.to_string(),
                offset,
            })
            .await?
            .into_inner())
    }

    /// Read a collection of messages from an offset.
    ///
    /// This does NOT produce a stream, instead it handles the collection
    /// of messages into a `Vec` before returning.
    pub async fn read_collection(
        &mut self,
        key: &str,
        offset: u64,
    ) -> Result<Vec<ReadMessageResponse>, Box<dyn std::error::Error>> {
        let mut message_stream = self.read(key, offset).await?;
        let mut messages = Vec::new();
        while let Some(message) = message_stream.next().await {
            let message = message?;
            messages.push(ReadMessageResponse {
                key: message.key,
                value: message.value,
            });
        }
        Ok(messages)
    }
}

#[cfg(test)]
mod test {
    use std::{net::SocketAddr, str::FromStr, sync::Arc};

    use tempfile::TempDir;
    use tokio_stream::StreamExt;
    use tonic::transport::{server::TcpIncoming, Server};

    use crate::{
        client::BigPipeClient,
        data_types::{
            message::{message_server::MessageServer, ReadMessageResponse},
            namespace::namespace_server::NamespaceServer,
            ClientMessage,
        },
        server::BigPipeServer,
        BigPipe,
    };

    // Utility for running a bigpipe server in the background
    fn run_server() -> (TempDir, SocketAddr) {
        let dir = TempDir::new().unwrap();
        let bigpipe_server = Arc::new(BigPipeServer::new(
            BigPipe::try_new(dir.path().to_path_buf(), None).unwrap(),
        ));
        let addr = SocketAddr::from_str("127.0.0.1:0").unwrap();
        let tcp = TcpIncoming::bind(addr).unwrap();
        let bind_addr = tcp.local_addr().unwrap();
        tokio::spawn(async move {
            Server::builder()
                .add_service(MessageServer::new(Arc::clone(&bigpipe_server)))
                .add_service(NamespaceServer::new(Arc::clone(&bigpipe_server)))
                .serve_with_incoming(tcp)
                .await
                .unwrap();
        });
        (dir, bind_addr)
    }

    #[tokio::test]
    async fn send() {
        let (_wal_dir, addr) = run_server();
        let mut client = BigPipeClient::new(&format!("http://{}", addr)).await;

        assert!(client
            .send(ClientMessage::new(
                "client_send".to_string(),
                "some_value".into(),
            ))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn read() {
        let (_wal_dir, addr) = run_server();
        let mut client = BigPipeClient::new(&format!("http://{}", addr)).await;

        let message_count = 10;

        for _ in 0..message_count {
            client
                .send(ClientMessage::new(
                    "test_read".to_string(),
                    "hello_world".into(),
                ))
                .await
                .unwrap();
        }

        let mut stream = client.read("test_read", 0).await.unwrap();
        for _ in 0..message_count {
            assert_eq!(
                stream.next().await.unwrap().unwrap(),
                ReadMessageResponse {
                    key: "test_read".to_string(),
                    value: "hello_world".into()
                }
            );
        }
        assert!(stream.next().await.is_none(), "No more messages expected");

        let messages = client.read_collection("test_read", 8).await.unwrap();
        assert_eq!(
            messages.len(),
            2,
            "Using offset=8, there should only be 2 messages"
        );
        for message in messages {
            assert_eq!(
                message,
                ReadMessageResponse {
                    key: "test_read".to_string(),
                    value: "hello_world".into()
                }
            );
        }
    }
}
