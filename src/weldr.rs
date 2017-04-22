extern crate env_logger;
extern crate hyper;
extern crate weldr;
extern crate clap;
extern crate tokio_core;

use std::env;
use std::net::SocketAddr;

use hyper::Url;
use clap::{Arg, App};

use tokio_core::reactor::Core;

use weldr::server::Server;
use weldr::pool::Pool;
//use weldr::health;
use weldr::mgmt::{worker, manager};

fn main() {
    env_logger::init().expect("Failed to start logger");

    let matches = App::new("weldr")
        .arg(Arg::with_name("worker")
             .long("worker")
             .value_name("id")
             .takes_value(true))
        .get_matches();


    let core = Core::new().unwrap();
    let handle = core.handle();

    let internal_addr = "127.0.0.1:4000";
    let internal_addr = internal_addr.parse::<SocketAddr>().expect("Failed to parse addr");

    //let backend = env::args().nth(2).unwrap_or("http://127.0.0.1:12345".to_string());
    let backend = "http://127.0.0.1:12345";
    let backend = backend.parse::<Url>().unwrap();
    let map_host = env::args().nth(4).unwrap_or("false".to_string());
    let map_host = if map_host == "true" { true } else { false };
    let server = Server::new(backend, map_host);
    let pool = Pool::default();
    let _ = pool.add(server);

    if let Some(id) = matches.value_of("worker") {
        println!("I am worker {}", id);
        let _result = worker::subscribe(internal_addr, handle);

        //let addr = env::args().nth(1).unwrap_or("127.0.0.1:8080".to_string());
        let addr = "127.0.0.1:8080".to_string();
        let addr = addr.parse::<SocketAddr>().unwrap();
        weldr::proxy::run(addr, pool, core).expect("Failed to start server");
    } else {
        let manager = manager::Manager::start_workers(5).expect("Failed to start manager");
        manager.listen(internal_addr, handle.clone());
        let backend = "http://127.0.0.1:12345";
        let backend = backend.parse::<Url>().unwrap();
        manager.publish_new_server(backend, handle);

        //let admin_ip = env::args().nth(3).unwrap_or("127.0.0.1:8687".to_string());
        let admin_ip = "127.0.0.1:8687";
        let admin_addr = admin_ip.parse::<SocketAddr>().unwrap();
        weldr::mgmt::run(admin_addr, pool, core).expect("Failed to start server");
    }
}
