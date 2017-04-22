use std::cell::RefCell;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use weldr_capnp::{publisher, subscriber, subscription, add_backend_server_request};

use futures::{Future, Stream};

use capnp_rpc::{RpcSystem, twoparty, rpc_twoparty_capnp};
use capnp::capability::Promise;
use capnp::Error;
use capnp::message::{Builder, HeapAllocator};
use capnp::serialize;

use tokio_io::AsyncRead;
use tokio_core::reactor::Handle;
use tokio_timer::Timer;

struct SubscriberHandle {
    client: subscriber::Client<::capnp::data::Owned>,
    requests_in_flight: i32,
}

struct SubscriberMap {
    subscribers: HashMap<u64, SubscriberHandle>,
}

impl SubscriberMap {
    fn new() -> SubscriberMap {
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

struct PublisherImpl {
    next_id: u64,
    subscribers: Rc<RefCell<SubscriberMap>>,
}

impl PublisherImpl {
    pub fn new() -> (PublisherImpl, Rc<RefCell<SubscriberMap>>) {
        let subscribers = Rc::new(RefCell::new(SubscriberMap::new()));
        (PublisherImpl { next_id: 0, subscribers: subscribers.clone() },
         subscribers.clone())
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

pub fn publish(addr: SocketAddr, handle: Handle) {
    let socket = ::tokio_core::net::TcpListener::bind(&addr, &handle).unwrap();

    let (publisher_impl, subscribers) = PublisherImpl::new();

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

    let mut message = Builder::new_default();
    {
        let mut request = message.init_root::<add_backend_server_request::Builder>();
        request.set_addr("127.0.0.1:8080");
    }

    let mut buf = Vec::new();
    serialize::write_message(&mut buf, &message).unwrap();

    let timer = Timer::default();

    let handle2 = handle.clone();
    let timer = timer.interval(Duration::from_secs(5)).map_err(|_| ());
    let wrk = timer.for_each(move |_| {
        info!("Sending message");

        let subscribers1 = subscribers.clone();
        let subs = &mut subscribers.borrow_mut().subscribers;
        for (&idx, mut subscriber) in subs.iter_mut() {
            if subscriber.requests_in_flight < 5 {
                subscriber.requests_in_flight += 1;

                let mut request = subscriber.client.push_message_request();
                //request.get().set_message(
                //    &format!("system time is: {:?}", ::std::time::SystemTime::now())[..]).unwrap();

                request.get().set_message(&buf[..]);

                let subscribers2 = subscribers1.clone();
                handle2.spawn(request.send().promise.then(move |r| {
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

        Ok(())
    });

    handle.spawn(wrk);
}
