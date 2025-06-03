mod proto {
    use tonic::include_proto;
    include_proto!("message");
}

use proto::{
    message_server::Message, CreateNamespaceRequest, CreateNamespaceResponse, SendMessageRequest,
    SendMessageResponse,
};
use tonic::{Request, Response, Status};

pub struct BigPipeServer;

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
