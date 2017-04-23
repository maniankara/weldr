//! Managers the underlying worker pool
//!
//! The manager publishes messages to the workers. Currently, the pubsub system uses capnp RPC
//! functionality. The pubsub implementation is subject to change, so the implementation is
//! hidden from the rest of the system.

use std::net::SocketAddr;
use std::io;
use std::process::Command;
use std::os::unix::process::CommandExt;
use std::cell::RefCell;
use std::rc::Rc;

use libc::pid_t;
use nix::unistd::{fork, ForkResult};
use tokio_core::reactor::Handle;
use hyper::Url;

#[derive(Debug)]
pub struct Worker {
    id: u64,
    pid: pid_t,
}

#[derive(Clone, Debug)]
pub struct Manager {
    inner: Rc<RefCell<Inner>>,
}

#[derive(Debug)]
pub struct Inner {
    workers: Vec<Worker>,
    subscribers: Rc<RefCell<capnp::SubscriberMap>>
}

impl Manager {
    pub fn start_workers(count: usize) -> io::Result<Manager> {
        (0..count as u64).map(|id| {
            start_worker(id)
        }).collect::<io::Result<Vec<Worker>>>().and_then(|workers| {
            Ok(Manager {
                inner: Rc::new(RefCell::new(Inner {
                    workers: workers,
                    subscribers: Rc::new(RefCell::new(capnp::SubscriberMap::new())),
                }))
            })
        })
    }

    /// Listen for workers requesting to subscribe
    ///
    /// This works using a handle instead of running on the main core. This was done to allow the
    /// manager to perform other essential functions using the main core.
    pub fn listen(&self, addr: SocketAddr, handle: Handle) {

        // TODO should the publisher should check against the worker list?
        capnp::listen(addr, handle, self.inner.borrow().subscribers.clone())
    }

    /// Ask all workers to add a new server to their pool
    pub fn publish_new_server(&self, url: Url, handle: Handle) {
        capnp::publish_new_server(url, handle, self.inner.borrow().subscribers.clone())
    }
}

fn start_worker(id: u64) -> io::Result<Worker> {
    let path = weldr_path();

    match fork()? {
        ForkResult::Parent { child, .. } => {
            info!("Spawned worker id {} as child pid {}", id, child);

            return Ok(Worker {
                id: id,
                pid: child
            })
        }
        ForkResult::Child => {
            trace!("I am a new child");

            Command::new(&path)
                .arg("worker")
                .arg("--id")
                .arg(id.to_string())
                .exec();

            unreachable!();
        }
    }
}

fn weldr_path() -> String {
    "/Users/herman/projects/weldr/target/debug/weldr".to_owned()
}

mod capnp {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::rc::Rc;
    use std::fmt;

    use weldr_capnp::{publisher, subscriber, subscription, add_backend_server_request};

    use futures::{Future, Stream};

    use capnp_rpc::{RpcSystem, twoparty, rpc_twoparty_capnp};
    use capnp::capability::Promise;
    use capnp::Error;
    use capnp::message::Builder;
    use capnp::serialize;

    use tokio_io::AsyncRead;
    use tokio_core::reactor::Handle;

    use hyper::Url;

    struct SubscriberHandle {
        client: subscriber::Client<::capnp::data::Owned>,
        requests_in_flight: i32,
    }

    pub struct SubscriberMap {
        subscribers: HashMap<u64, SubscriberHandle>,
    }

    impl fmt::Debug for SubscriberMap {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "SubscriberMap {{ subscribers: {} }}", self.subscribers.iter().count())
        }
    }

    impl SubscriberMap {
        pub fn new() -> SubscriberMap {
            SubscriberMap { subscribers: HashMap::new() }
        }
    }

    struct SubscriptionImpl {
        id: u64,
        subscribers: Rc<RefCell<SubscriberMap>>,
    }

    impl SubscriptionImpl {
        fn new(id: u64, subscribers: Rc<RefCell<SubscriberMap>>) -> SubscriptionImpl {
            SubscriptionImpl { id: id, subscribers: subscribers }
        }
    }

    impl Drop for SubscriptionImpl {
        fn drop(&mut self) {
            info!("subscription dropped");
            self.subscribers.borrow_mut().subscribers.remove(&self.id);
        }
    }

    impl subscription::Server for SubscriptionImpl {}

    pub struct PublisherImpl {
        next_id: u64,
        subscribers: Rc<RefCell<SubscriberMap>>,
    }

    impl PublisherImpl {
        pub fn new(subscribers: Rc<RefCell<SubscriberMap>>) -> PublisherImpl {
            PublisherImpl { next_id: 0, subscribers: subscribers }
        }
    }

    impl publisher::Server<::capnp::data::Owned> for PublisherImpl {
        fn subscribe(&mut self,
                     params: publisher::SubscribeParams<::capnp::data::Owned>,
                     mut results: publisher::SubscribeResults<::capnp::data::Owned>,)
            -> Promise<(), ::capnp::Error>
            {
                info!("subscribe");
                self.subscribers.borrow_mut().subscribers.insert(
                    self.next_id,
                    SubscriberHandle {
                        client: pry!(pry!(params.get()).get_subscriber()),
                        requests_in_flight: 0,
                    }
                );

                results.get().set_subscription(
                    subscription::ToClient::new(SubscriptionImpl::new(self.next_id, self.subscribers.clone()))
                    .from_server::<::capnp_rpc::Server>());

                self.next_id += 1;
                Promise::ok(())
            }
    }

    pub fn listen(addr: SocketAddr, handle: Handle, subscribers: Rc<RefCell<SubscriberMap>>) {
        let socket = ::tokio_core::net::TcpListener::bind(&addr, &handle).unwrap();

        let publisher_impl = PublisherImpl::new(subscribers);

        let publisher = publisher::ToClient::new(publisher_impl).from_server::<::capnp_rpc::Server>();

        let handle1 = handle.clone();
        let done = socket.incoming().for_each(move |(socket, _addr)| {
            try!(socket.set_nodelay(true));
            let (reader, writer) = socket.split();
            let handle = handle1.clone();

            let network =
                twoparty::VatNetwork::new(reader, writer,
                                          rpc_twoparty_capnp::Side::Server, Default::default());

            let rpc_system = RpcSystem::new(Box::new(network), Some(publisher.clone().client));

            handle.spawn(rpc_system.map_err(|_| ()));
            Ok(())
        }).map_err(|_| ());

        handle.spawn(done);
    }

    pub fn publish_new_server(url: Url, handle: Handle, subscribers: Rc<RefCell<SubscriberMap>>) {
        trace!("publish_new_server");

        let mut message = Builder::new_default();
        {
            let mut request = message.init_root::<add_backend_server_request::Builder>();
            request.set_url(&format!("{}", url));
        }

        let mut buf = Vec::new();
        serialize::write_message(&mut buf, &message).unwrap();

        let subscribers1 = subscribers.clone();
        let subs = &mut subscribers.borrow_mut().subscribers;
        for (&idx, mut subscriber) in subs.iter_mut() {
            if subscriber.requests_in_flight < 5 {
                subscriber.requests_in_flight += 1;

                let mut request = subscriber.client.push_message_request();

                request.get().set_message(&buf[..]).unwrap();

                let subscribers2 = subscribers1.clone();
                handle.spawn(request.send().promise.then(move |r| {
                    match r {
                        Ok(_) => {
                            subscribers2.borrow_mut().subscribers.get_mut(&idx).map(|ref mut s| {
                                s.requests_in_flight -= 1;
                            });
                        }
                        Err(e) => {
                            error!("Got error: {:?}. Dropping subscriber.", e);
                            subscribers2.borrow_mut().subscribers.remove(&idx);
                        }
                    }
                    Ok::<(), Error>(())
                }).map_err(|_| unreachable!()));
            }
        }
    }
}
