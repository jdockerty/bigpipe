use std::{
    io::Write,
    net::{TcpListener, TcpStream},
    path::PathBuf,
};

use clap::{Parser, Subcommand};
use tracing::info;
use tracing_subscriber;

use bigpipe::{BigPipe, ClientMessage, ServerMessage};

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
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.commands {
        Commands::Write { key, value, addr } => {
            let message = ClientMessage::new(key, value.into());

            let mut stream = TcpStream::connect(addr).unwrap();

            stream
                .write_all(&rmp_serde::to_vec(&message).unwrap())
                .unwrap();
        }
        Commands::Server {
            addr,
            wal_directory,
            wal_segment_max_size,
        } => {
            tracing_subscriber::fmt().init();
            let mut bigpipe = BigPipe::new(wal_directory.clone(), wal_segment_max_size);

            let listener = TcpListener::bind(addr).unwrap();
            info!(address = %listener.local_addr().unwrap(), wal_directory = %wal_directory.to_string_lossy(), "bigpipe running");

            for stream in listener.incoming() {
                let stream = stream.unwrap();
                let message: ClientMessage = rmp_serde::from_read(stream).unwrap();

                let timestamp = chrono::Utc::now().timestamp_micros();
                let server_msg: ServerMessage = message.into_server_message(timestamp);
                bigpipe.wal_write(&server_msg)?;
                bigpipe.add_message(server_msg);
            }
        }
    }
    Ok(())
}
