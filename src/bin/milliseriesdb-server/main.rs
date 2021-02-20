use milliseriesdb::db::{SyncMode, DB};
use std::io;
use std::env;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::process::exit;
use warp::Filter;

struct Server {
    db: Arc<Mutex<DB>>,
    port: u16,
}

impl Server {
    fn create<P: AsRef<Path>>(base_path: P, port: u16) -> io::Result<Server> {
        Ok(Server {
            db: Arc::new(Mutex::new(DB::open(base_path, SyncMode::Every(100))?)),
            port: port,
        })
    }

    async fn run(&mut self) {
        let get_series = warp::path!("series" / String).map(|name| format!("Here are series: {}", name));

        warp::serve(get_series).run(([127, 0, 0, 1], self.port)).await
    }
}

#[tokio::main]
async fn main() {
    let mut args = env::args();
    args.next();

    match (args.next(), args.next().and_then(|port| port.parse::<u16>().ok())) {
        (Some(base_path), Some(port)) => {
            let mut server = Server::create(base_path, port).unwrap();
            server.run().await;
        }
        _ => exit(1)
    }
}
