use std::net::SocketAddr;

use weldr_capnp::{publisher, subscriber};

use futures::Future;

use capnp_rpc::{RpcSystem, twoparty, rpc_twoparty_capnp};
use capnp::capability::Promise;

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

pub fn subscribe(addr: SocketAddr, handle: Handle) {
    let handle1 = handle.clone();

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
    }).and_then(|_| {
        info!("Got a subscribe response");
        ::futures::finished(())
    });

    handle.spawn(request);
}
