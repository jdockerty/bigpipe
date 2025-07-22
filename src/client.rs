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
    pub async fn send(&mut self, message: ClientMessage) {
        self.client
            .send(SendMessageRequest {
                key: message.key().to_string(),
                value: message.value().to_vec(),
            })
            .await
            .unwrap();
    }

    /// Stream messages for a key, from a particular offset.
    pub async fn read(
        &mut self,
        key: &str,
        offset: u64,
    ) -> impl Stream<Item = Result<ReadMessageResponse, Status>> {
        self.client
            .read(ReadMessageRequest {
                key: key.to_string(),
                offset,
            })
            .await
            .unwrap()
            .into_inner()
    }

    /// Read a collection of messages from an offset.
    ///
    /// This does NOT produce a stream, instead it handles the collection
    /// of messages into a `Vec` before returning.
    pub async fn read_collection(&mut self, key: &str, offset: u64) -> Vec<ReadMessageResponse> {
        let mut message_stream = self.read(key, offset).await;

        let mut messages = Vec::new();
        while let Some(message) = message_stream.next().await {
            let message = message.unwrap();
            messages.push(ReadMessageResponse {
                key: message.key,
                value: message.value,
            });
        }

        messages
    }
}
