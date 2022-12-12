use config::ServerConfig;
use std::sync::Arc;
use tokio_uring::net::TcpListener;

const QUEUE_DEPTH: u32 = 128;

fn main() {
    let server_config = Arc::new(ServerConfig::parse());

    let mut handles = vec![];

    for processor in server_config.cores.iter() {
        let core_id = *processor as _;
        let config = Arc::clone(&server_config);
        let handle = std::thread::spawn(move || {
            core_affinity::set_for_current(core_affinity::CoreId { id: core_id });
            tokio_uring::builder()
                .entries(QUEUE_DEPTH)
                .uring_builder(
                    tokio_uring::uring_builder()
                        .setup_cqsize(QUEUE_DEPTH * 2)
                )
                .start(async {
                    serve(config).await;
                });
        });
        handles.push(handle);
    }

    for handle in handles.into_iter() {
        handle.join().unwrap();
    }
}

async fn serve(server_config: Arc<config::ServerConfig>) {
    println!("Listening {}", server_config.bind);
    let addr = server_config.bind.parse().unwrap();
    let listener = match TcpListener::bind(addr) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("Failed to bind: {:?}", e);
            return;
        }
    };

    loop {
        match listener.accept().await {
            Ok((stream, _peer_address)) => {
                tokio_uring::spawn(async move {
                    let mut buffer = vec![0; config::PACKET_SIZE];
                    loop {
                        let (r, buff_r) = stream.read(buffer).await;

                        if let Err(_e) = r {
                            // Something is wrong with the connection, close it.
                            break;
                        }

                        let (r, buff_w) = stream.write_all(buff_r).await;
                        if let Err(_e) = r {
                            break;
                        }

                        buffer = buff_w;
                    }
                });
            }

            Err(e) => {
                eprintln!("Failed to accept: {:?}", e);
                break;
            }
        }
    }
}
