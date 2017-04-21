use std::net::SocketAddr;
use std::rc::Rc;
use std::cell::RefCell;

use weldr_capnp::{publisher, subscriber};

use futures::Future;

use capnp_rpc::{RpcSystem, twoparty, rpc_twoparty_capnp};
use capnp::capability::{Response, Promise};

use tokio_io::AsyncRead;
use tokio_core::reactor::Handle;
use tokio_core::net::TcpStream;

struct SubscriberImpl;

impl subscriber::Server<::capnp::text::Owned> for SubscriberImpl {
    fn push_message(&mut self,
                    params: subscriber::PushMessageParams<::capnp::text::Owned>,
                    _results: subscriber::PushMessageResults<::capnp::text::Owned>)
        -> Promise<(), ::capnp::Error>
        {
            info!("message from publisher: {}", pry!(pry!(params.get()).get_message()));
            Promise::ok(())
        }
}

pub struct S {
    pub response: Option<Response<publisher::subscribe_results::Owned<::capnp::text::Owned>>>,
}

pub fn subscribe(addr: SocketAddr, handle: Handle) -> Rc<RefCell<S>> {
    let handle1 = handle.clone();

    let s = S { response: None };
    let s = Rc::new(RefCell::new(s));
    let s1 = s.clone();

    let request = TcpStream::connect(&addr, &handle).map_err(|e| e.into()).and_then(move |stream| {
        stream.set_nodelay(true).unwrap();
        let (reader, writer) = stream.split();

        let rpc_network =
            Box::new(twoparty::VatNetwork::new(reader, writer,
                                               rpc_twoparty_capnp::Side::Client,
                                               Default::default()));

        let mut rpc_system = RpcSystem::new(rpc_network, None);
        let publisher: publisher::Client<::capnp::text::Owned> =
            rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);

        let sub = subscriber::ToClient::new(SubscriberImpl).from_server::<::capnp_rpc::Server>();

        let mut request = publisher.subscribe_request();
        request.get().set_subscriber(sub);
        handle1.spawn(rpc_system.map_err(|e| {
            error!("Subscribe RPC System error {:?}", e);
        }));

        request.send().promise
    }).map_err(|e| {
        error!("Subscribe request error {:?}", e);
    }).and_then(move |r| {
        info!("Got a subscribe response");
        s1.borrow_mut().response = Some(r);
        ::futures::finished(())
    });

    handle.spawn(request);

    s
}
