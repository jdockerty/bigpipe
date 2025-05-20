use std::{
    io::Write,
    net::{TcpListener, TcpStream},
    sync::Arc,
    time::Duration,
};

use clap::{Parser, Subcommand};

use bigpipe::{BigPipe, Message};

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

        #[arg(long, hide = true)]
        debug: bool,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.commands {
        Commands::Write { key, value, addr } => {
            let message = Message::new(key, value.into_bytes());

            let mut stream = TcpStream::connect(addr).unwrap();

            stream
                .write_all(&rmp_serde::to_vec(&message).unwrap())
                .unwrap();
        }
        Commands::Server { addr, debug } => {
            let bigpipe = Arc::new(BigPipe::new());

            let listener = TcpListener::bind(addr).unwrap();
            println!("bigpipe running at {}", listener.local_addr().unwrap());

            if debug {
                let bigpipe_background = Arc::clone(&bigpipe);
                std::thread::spawn(move || loop {
                    std::thread::sleep(Duration::from_millis(500));
                    println!("{:?}", bigpipe_background.messages());
                });
            }
            for stream in listener.incoming() {
                let stream = stream.unwrap();
                let message: Message = rmp_serde::from_read(stream).unwrap();
                bigpipe.add_message(message);
            }
        }
    }
    Ok(())
}
