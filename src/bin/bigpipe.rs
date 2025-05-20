use std::{net::TcpListener, sync::Arc, time::Duration};

use bigpipe::{BigPipe, Message};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bigpipe = Arc::new(BigPipe::new());

    let listener = TcpListener::bind("0.0.0.0:7050").unwrap();
    println!("bigpipe running at {}", listener.local_addr().unwrap());

    // TODO: debug background task to show the items in the queue.
    let bigpipe_background = Arc::clone(&bigpipe);
    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_millis(500));
        println!("{:?}", bigpipe_background.messages());
    });

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        let message: Message = rmp_serde::from_read(stream).unwrap();
        bigpipe.add_message(message);
    }

    Ok(())
}
