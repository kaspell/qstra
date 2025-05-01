use std::cell::RefCell;
use std::env;
use std::io;
use std::rc::Rc;

mod cfg;
mod cmd;
mod ctl;
mod db;
mod reg;
mod srv;
mod wal;


struct SocketGuard(String);


impl Drop for SocketGuard {
        fn drop(&mut self) {
                println!("Cleaning up socket: {}", self.0);
                let _ = std::fs::remove_file(&self.0);
        }
}


async fn serve_local(
        addr: String,
        ctl_rc: Rc<RefCell<ctl::Ctl>>,
        mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) -> io::Result<()>
{
        let _ = std::fs::remove_file(&addr);
        let _guard = SocketGuard(addr.clone());
        let listener = tokio::net::UnixListener::bind(&addr)?;

        loop {
                tokio::select! {
                        biased;
                        _ = shutdown_rx.recv() => {
                                break;
                        }
                        res = listener.accept() => {
                                match res {
                                        Ok((stream, _unix_addr)) => {
                                                let ctl_clone = Rc::clone(&ctl_rc);
                                                tokio::task::spawn_local(srv::handle_client(stream, ctl_clone));
                                        }
                                        Err(e) => {
                                                eprintln!("Error accepting a local connection: {e}");
                                        }
                                }
                        }
                }
        }

        Ok(())
}


async fn serve_network(
        addr: String,
        ctl_rc: Rc<RefCell<ctl::Ctl>>,
        mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) -> io::Result<()>
{
        let listener = tokio::net::TcpListener::bind(&addr).await?;

        loop {
                tokio::select! {
                        biased;
                        _ = shutdown_rx.recv() => {
                                break;
                        }
                        res = listener.accept() => {
                                match res {
                                        Ok((stream, _peer_addr)) => {
                                                let ctl_clone = Rc::clone(&ctl_rc);
                                                tokio::task::spawn_local(srv::handle_client(stream, ctl_clone));
                                        }
                                        Err(e) => {
                                                eprintln!("Error accepting a network connection: {e}");
                                        }
                                }
                        }
                }
        }

        Ok(())
}


#[tokio::main(flavor = "current_thread")]
async fn main() -> io::Result<()> {
        let local = tokio::task::LocalSet::new();

        local.run_until(async move {
                let args: Vec<String> = env::args().collect();
                let mut conf_path = cfg::CONF_FILE;

                match args.len() {
                        1 => {}
                        2 => {
                                conf_path = args[1].as_str();
                        }
                        _ => {
                                panic!("Error: too many arguments");
                        }
                }

                let conf = cfg::Config::new(conf_path);
                let mut ctl = ctl::Ctl::new_blank(conf)?;
                ctl.load_from_storage()?;
                let pctl = Rc::new(RefCell::new(ctl));

                let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);

                let listener_cfg = {
                        let rctl = pctl.borrow();
                        (
                                rctl.config().listen_local,
                                rctl.config().sock_addr.clone(),
                                rctl.config().listen_network,
                                rctl.config().inet_addr.clone(),
                        )
                };

                let listen_local = listener_cfg.0;
                let local_addr = listener_cfg.1;
                let listen_network = listener_cfg.2;
                let network_addr = listener_cfg.3;

                let mut handles = Vec::new();

                if listen_local {
                        let ctl_clone = Rc::clone(&pctl);
                        let shutdown_rx = shutdown_tx.subscribe();
                        handles.push(
                                tokio::task::spawn_local(serve_local(local_addr, ctl_clone, shutdown_rx))
                        );
                }

                if listen_network {
                        let ctl_clone = Rc::clone(&pctl);
                        let shutdown_rx = shutdown_tx.subscribe();
                        handles.push(
                                tokio::task::spawn_local(serve_network(network_addr, ctl_clone, shutdown_rx))
                        );
                }
                if handles.is_empty() {
                        return Err(io::Error::new(io::ErrorKind::InvalidInput, "No listeners configured"));
                }

                println!("Server running. Press Ctrl+C to shut down...");
                tokio::signal::ctrl_c().await?;

                if shutdown_tx.send(()).is_err() {
                        eprintln!("Warning: No listeners were active to receive shutdown signal.");
                }

                for (i, handle) in handles.into_iter().enumerate() {
                        if let Err(e) = handle.await {
                                eprintln!("Error waiting for listener task {i}: {e:?}");
                        }
                }

                drop(pctl);
                Ok(())
        }).await
}