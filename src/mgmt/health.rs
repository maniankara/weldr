use futures::Future;
use tokio_core::reactor::Handle;
use hyper::Client;
use hyper_tls::HttpsConnector;

use pool::Pool;

pub fn run(pool: Pool, handle: &Handle) {
    let client = Client::configure()
        .connector(HttpsConnector::new(4, &handle))
        .build(&handle);

    // FIXME cofnigure health check uri path
    let uri_path = "/";

    let servers = pool.all();
    for server in servers {
        let url = match server.url().join(&uri_path) {
            Ok(url) => url,
            Err(e) => {
                error!("Invalid health check url: {:?}",e);
                continue;
            }
        };
        debug!("Health check {:?}", url);

        let pool1 = pool.clone();
        let pool2 = pool.clone();
        let server1 = server.clone();
        let server2 = server.clone();
        let req = client.get(url).and_then(move |res| {
            debug!("Response: {}", res.status());
            debug!("Headers: \n{}", res.headers());

            if ! res.status().is_success() {
                info!("Removing {:?} from pool", server1);
                pool1.remove(&server1);
            }

            ::futures::finished(())
        }).map_err(move |e| {
            error!("Error connecting to backend: {:?}", e);
            info!("Removing {:?} from pool", server2);
            pool2.remove(&server2);
        });

        handle.spawn(req);
    }
}
