use std::{net::TcpListener, path::PathBuf};

use clap::{Parser, Subcommand};
use clap_verbosity_flag::{InfoLevel, Verbosity};
use tonic::Request;
use tracing::{debug, error, info};

use bigpipe::{
    data_types::{ClientMessage, ServerMessage},
    BigPipe,
};
use protos::{message_client::MessageClient, SendMessageRequest};

mod protos {
    use tonic::include_proto;

    include_proto!("message");
}

#[derive(Parser)]
struct Cli {
    #[clap(subcommand)]
    commands: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Write a message into a running bigpipe server.
    Write {
        key: String,
        value: String,
        /// Address of the server to write to.
        #[arg(long, env = "BIGPIPE_ADDRESS", default_value = "0.0.0.0:7050")]
        addr: String,
    },
    /// Start a bigpipe server.
    Server {
        /// Bind address for the server.
        #[arg(long, env = "BIGPIPE_ADDRESS", default_value = "0.0.0.0:7050")]
        addr: String,

        /// Directory where the write-ahead log (WAL) files will be written to.
        #[arg(long, env = "BIGPIPE_WAL_DIRECTORY", default_value = "./")]
        wal_directory: PathBuf,

        /// Max size of a segment for the WAL.
        #[arg(long, env = "BIGPIPE_WAL_SEGMENT_MAX_SIZE")]
        wal_segment_max_size: Option<usize>,

        #[command(flatten)]
        verbosity: Verbosity<InfoLevel>,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.commands {
        Commands::Write { key, value, addr } => {
            let mut client = MessageClient::connect(addr).await?;
            let req = Request::new(SendMessageRequest {
                key,
                value: value.into_bytes(),
            });
            client.send(req).await?;
        }
        Commands::Server {
            addr,
            wal_directory,
            wal_segment_max_size,
            verbosity,
        } => {
            tracing_subscriber::fmt().with_max_level(verbosity).init();
            let mut bigpipe = BigPipe::try_new(wal_directory.clone(), wal_segment_max_size)?;

            let listener = TcpListener::bind(addr).unwrap();
            info!(address = %listener.local_addr().unwrap(), wal_directory = %wal_directory.to_string_lossy(), "bigpipe running");

            for stream in listener.incoming() {
                let stream = stream?;
                let message: ClientMessage = match rmp_serde::from_read(&stream) {
                    Ok(message) => message,
                    Err(e) => {
                        let peer = stream.peer_addr()?;
                        error!(%peer, ?e);
                        continue;
                    }
                };

                debug!(
                    key = message.key(),
                    value_size = message.value().len(),
                    "received client message"
                );

                let timestamp = chrono::Utc::now().timestamp_micros();
                let server_msg: ServerMessage = message.into_server_message(timestamp);
                bigpipe.write(&server_msg)?;
            }
        }
    }
    Ok(())
}
