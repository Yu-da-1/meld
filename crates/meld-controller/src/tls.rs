//! Accepting connections over TLS.

use std::{io, net::SocketAddr, time::Duration};

use meld_tls::{TlsAcceptor, TlsStream};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time::{sleep, timeout},
};

/// How long a peer may take to finish the handshake.
const HANDSHAKE_LIMIT: Duration = Duration::from_secs(10);
/// How long to wait after the system fails to accept, such as for lack of
/// file descriptors, before trying again.
const ACCEPT_RETRY_DELAY: Duration = Duration::from_secs(1);

/// A listener that hands out connections whose handshake has finished.
///
/// Handshakes run in tasks of their own, so a peer that connects and says
/// nothing holds up only itself, never the connections behind it.
pub struct TlsListener {
    local_addr: SocketAddr,
    ready: mpsc::Receiver<(TlsStream<TcpStream>, SocketAddr)>,
}

impl TlsListener {
    pub fn new(listener: TcpListener, acceptor: TlsAcceptor) -> io::Result<Self> {
        let local_addr = listener.local_addr()?;
        let (sender, ready) = mpsc::channel(64);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    // The server is gone.
                    () = sender.closed() => return,
                    accepted = listener.accept() => match accepted {
                        Ok((stream, peer)) => {
                            let acceptor = acceptor.clone();
                            let sender = sender.clone();
                            tokio::spawn(async move {
                                match timeout(HANDSHAKE_LIMIT, acceptor.accept(stream)).await {
                                    Ok(Ok(stream)) => {
                                        let _ = sender.send((stream, peer)).await;
                                    }
                                    Ok(Err(error)) => {
                                        tracing::debug!(%peer, %error, "TLS handshake failed");
                                    }
                                    Err(_) => {
                                        tracing::debug!(%peer, "TLS handshake timed out");
                                    }
                                }
                            });
                        }
                        Err(error) => {
                            tracing::warn!(%error, "accepting a connection failed");
                            sleep(ACCEPT_RETRY_DELAY).await;
                        }
                    },
                }
            }
        });
        Ok(Self { local_addr, ready })
    }
}

impl axum::serve::Listener for TlsListener {
    type Io = TlsStream<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        // The accepting task owns the sender for as long as this value
        // exists, so the channel cannot close first.
        self.ready
            .recv()
            .await
            .expect("the accepting task runs while the listener exists")
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        Ok(self.local_addr)
    }
}
