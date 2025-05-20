use std::io::Write;
use std::net::TcpStream;

use bigpipe::Message;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    let key = args.get(1).cloned().expect("Key must be provided");

    let value = args.get(2).cloned().expect("Key must be provided");

    let message = Message::new(key, value.into_bytes());

    let mut stream = TcpStream::connect("0.0.0.0:7050").unwrap();

    stream
        .write_all(&rmp_serde::to_vec(&message).unwrap())
        .unwrap();

    Ok(())
}
