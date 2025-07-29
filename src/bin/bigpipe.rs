use std::{net::SocketAddr, path::PathBuf, str::FromStr, sync::Arc};

use clap::{Parser, Subcommand};
use clap_verbosity_flag::{InfoLevel, Verbosity};
use tokio_stream::StreamExt;
use tonic::transport::Server;

use bigpipe::{
    client::BigPipeClient,
    data_types::{
        message::message_server::MessageServer,
        namespace::{
            namespace_client::NamespaceClient, namespace_server::NamespaceServer,
            CreateNamespaceRequest,
        },
        ClientMessage, RetentionPolicy,
    },
    server::BigPipeServer,
    BigPipe,
};

#[derive(Parser)]
struct Cli {
    #[clap(subcommand)]
    commands: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Write a message into a running bigpipe server.
    ///
    /// If the namespace which is being written to does not
    /// exist, it is eagerly created with default values.
    Write {
        key: String,
        value: String,
        /// Address of the server to write to.
        #[arg(long, env = "BIGPIPE_ADDRESS", default_value = "http://0.0.0.0:7050")]
        addr: String,
    },
    /// Create a namespace that can begin to accept messages.
    Create {
        namespace: String,
        policy: RetentionPolicy,
        /// Address of the server to write to.
        #[arg(long, env = "BIGPIPE_ADDRESS", default_value = "http://0.0.0.0:7050")]
        addr: String,
    },
    /// Read messages from a bigpipe server.
    Read {
        key: String,
        offset: u64,
        /// Address of the server to write to.
        #[arg(long, env = "BIGPIPE_ADDRESS", default_value = "http://0.0.0.0:7050")]
        addr: String,
    },
    /// Start a bigpipe server.
    Server {
        /// Bind address for the server.
        #[arg(long, env = "BIGPIPE_ADDRESS", default_value = "0.0.0.0:7050")]
        addr: String,

        /// Directory where the write-ahead log (WAL) files will be written to.
        #[arg(long, env = "BIGPIPE_WAL_DIRECTORY", default_value = default_wal_directory().into_os_string() )]
        wal_directory: PathBuf,

        /// Max size of a segment for the WAL.
        #[arg(long, env = "BIGPIPE_WAL_SEGMENT_MAX_SIZE")]
        wal_segment_max_size: Option<usize>,

        #[command(flatten)]
        verbosity: Verbosity<InfoLevel>,
    },
}

fn default_wal_directory() -> PathBuf {
    std::env::temp_dir().join("bigpipe")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.commands {
        Commands::Create {
            namespace,
            policy,
            addr,
        } => {
            let mut client = NamespaceClient::connect(addr).await?;

            match client
                .create(CreateNamespaceRequest {
                    key: namespace.clone(),
                    retention_policy: policy as i32,
                })
                .await
            {
                Ok(_) => eprintln!("{namespace} created"),
                Err(e) if matches!(e.code(), tonic::Code::AlreadyExists) => {
                    return Err("{namespace} already exists".to_string().into())
                }
                Err(e) => return Err(format!("unknown error from server: {e}").into()),
            }
        }
        Commands::Read { key, offset, addr } => {
            let mut client = BigPipeClient::new(&addr).await?;

            let mut response_stream = client.read(&key, offset).await?;

            while let Some(message) = response_stream.next().await {
                let message = message?;
                // assumes string values for now
                println!("{}", String::from_utf8_lossy(&message.value));
            }
        }
        Commands::Write { key, value, addr } => {
            let mut client = BigPipeClient::new(&addr).await?;
            client.send(ClientMessage::new(key, value.into())).await?;
        }
        Commands::Server {
            addr,
            wal_directory,
            wal_segment_max_size,
            verbosity,
        } => {
            tracing_subscriber::fmt().with_max_level(verbosity).init();
            let bigpipe = BigPipe::try_new(wal_directory.clone(), wal_segment_max_size)?;
            let bigpipe_server = Arc::new(BigPipeServer::new(bigpipe));

            Server::builder()
                .add_service(MessageServer::new(Arc::clone(&bigpipe_server)))
                .add_service(NamespaceServer::new(Arc::clone(&bigpipe_server)))
                .serve(SocketAddr::from_str(&addr)?)
                .await?;
        }
    }
    Ok(())
}
