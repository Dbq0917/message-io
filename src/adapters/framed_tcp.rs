pub use socket2::{TcpKeepalive};

use crate::network::adapter::{
    Resource, Remote, Local, Adapter, SendStatus, AcceptedType, ReadStatus, ConnectionInfo,
    ListeningInfo, PendingStatus,
};
use crate::network::{RemoteAddr, Readiness, TransportConnect, TransportListen};
use crate::util::encoding::{self, Decoder, MAX_ENCODED_SIZE};
use crate::adapters::write_queue::{self, WriteQueue, DEFAULT_PENDING_SEND_LIMIT};

use mio::net::{TcpListener, TcpStream};
use mio::event::{Source};

use socket2::{Socket};

use std::net::{SocketAddr};
use std::io::{self, ErrorKind, Read};
use std::cell::{RefCell};
use std::sync::Mutex;
use std::mem::{forget, MaybeUninit};
#[cfg(target_os = "windows")]
use std::os::windows::io::{FromRawSocket, AsRawSocket};
#[cfg(not(target_os = "windows"))]
use std::os::{fd::AsRawFd, unix::io::FromRawFd};

const INPUT_BUFFER_SIZE: usize = u16::MAX as usize; // 2^16 - 1

#[derive(Clone, Debug)]
pub struct FramedTcpConnectConfig {
    keepalive: Option<TcpKeepalive>,
    pending_send_limit: usize,
}

impl Default for FramedTcpConnectConfig {
    fn default() -> Self {
        Self { keepalive: None, pending_send_limit: DEFAULT_PENDING_SEND_LIMIT }
    }
}

impl FramedTcpConnectConfig {
    /// Enables TCP keepalive settings on the socket.
    pub fn with_keepalive(mut self, keepalive: TcpKeepalive) -> Self {
        self.keepalive = Some(keepalive);
        self
    }

    /// Sets the byte high-water mark for encoded frame data retained in userspace
    /// while the peer is slow. The queue can overshoot by one send payload;
    /// returns [`SendStatus::Backpressure`] once the mark is reached.
    pub fn with_pending_send_limit(mut self, bytes: usize) -> Self {
        self.pending_send_limit = bytes.max(1);
        self
    }
}

#[derive(Clone, Debug)]
pub struct FramedTcpListenConfig {
    keepalive: Option<TcpKeepalive>,
    pending_send_limit: usize,
}

impl Default for FramedTcpListenConfig {
    fn default() -> Self {
        Self { keepalive: None, pending_send_limit: DEFAULT_PENDING_SEND_LIMIT }
    }
}

impl FramedTcpListenConfig {
    /// Enables TCP keepalive settings on client connection sockets.
    pub fn with_keepalive(mut self, keepalive: TcpKeepalive) -> Self {
        self.keepalive = Some(keepalive);
        self
    }

    /// Sets the pending send limit used by sockets accepted from this listener.
    /// See [`FramedTcpConnectConfig::with_pending_send_limit`].
    pub fn with_pending_send_limit(mut self, bytes: usize) -> Self {
        self.pending_send_limit = bytes.max(1);
        self
    }
}

pub(crate) struct FramedTcpAdapter;
impl Adapter for FramedTcpAdapter {
    type Remote = RemoteResource;
    type Local = LocalResource;
}

pub(crate) struct RemoteResource {
    stream: TcpStream,
    decoder: RefCell<Decoder>,
    keepalive: Option<TcpKeepalive>,
    pending_writes: Mutex<WriteQueue>,
}

// SAFETY:
// That RefCell<Decoder> can be used with Sync because the decoder is only used in the read_event,
// that will be called always from the same thread. This way, we save the cost of a Mutex.
unsafe impl Sync for RemoteResource {}

impl RemoteResource {
    fn new(stream: TcpStream, keepalive: Option<TcpKeepalive>, pending_send_limit: usize) -> Self {
        Self {
            stream,
            decoder: RefCell::new(Decoder::default()),
            keepalive,
            pending_writes: Mutex::new(WriteQueue::new(pending_send_limit)),
        }
    }
}

impl Resource for RemoteResource {
    fn source(&mut self) -> &mut dyn Source {
        &mut self.stream
    }
}

impl Remote for RemoteResource {
    fn connect_with(
        config: TransportConnect,
        remote_addr: RemoteAddr,
    ) -> io::Result<ConnectionInfo<Self>> {
        let config = match config {
            TransportConnect::FramedTcp(config) => config,
            _ => panic!("Internal error: Got wrong config"),
        };
        let peer_addr = *remote_addr.socket_addr();
        let stream = TcpStream::connect(peer_addr)?;
        let local_addr = stream.local_addr()?;
        Ok(ConnectionInfo {
            remote: RemoteResource::new(stream, config.keepalive, config.pending_send_limit),
            local_addr,
            peer_addr,
        })
    }

    fn receive(&self, mut process_data: impl FnMut(&[u8])) -> ReadStatus {
        let buffer: MaybeUninit<[u8; INPUT_BUFFER_SIZE]> = MaybeUninit::uninit();
        let mut input_buffer = unsafe { buffer.assume_init() }; // Avoid to initialize the array

        loop {
            let mut stream = &self.stream;
            match stream.read(&mut input_buffer) {
                Ok(0) => break ReadStatus::Disconnected,
                Ok(size) => {
                    let data = &input_buffer[..size];
                    log::trace!("Decoding {} bytes", data.len());
                    self.decoder.borrow_mut().decode(data, |decoded_data| {
                        process_data(decoded_data);
                    });
                }
                Err(ref err) if err.kind() == ErrorKind::Interrupted => continue,
                Err(ref err) if err.kind() == ErrorKind::WouldBlock => {
                    break ReadStatus::WaitNextEvent
                }
                Err(ref err) if err.kind() == ErrorKind::ConnectionReset => {
                    break ReadStatus::Disconnected
                }
                Err(err) => {
                    log::error!("TCP receive error: {}", err);
                    break ReadStatus::Disconnected; // should not happen
                }
            }
        }
    }

    fn send(&self, data: &[u8]) -> SendStatus {
        let mut buf = [0; MAX_ENCODED_SIZE]; // used to avoid a heap allocation
        let encoded_size = encoding::encode_size(data, &mut buf);

        let mut pending_writes =
            self.pending_writes.lock().expect(crate::util::thread::OTHER_THREAD_ERR);

        if let Err(err) = pending_writes.flush(&mut &self.stream) {
            log::error!("Framed TCP send flush error: {}", err);
            return SendStatus::ResourceNotFound;
        }

        if pending_writes.is_full() {
            return SendStatus::Backpressure;
        }

        if pending_writes.pending_bytes() > 0 {
            // A previous frame is still queued. Keep this complete frame behind it.
            pending_writes.enqueue(encoded_size.to_vec());
            pending_writes.enqueue(data.to_vec());
            return SendStatus::Queued;
        }

        let header_remaining = match write_queue::write_prefix(&mut &self.stream, encoded_size) {
            Ok(remaining) => remaining,
            Err(err) => {
                log::error!("Framed TCP header send error: {}", err);
                return SendStatus::ResourceNotFound;
            }
        };

        if let Some(header) = header_remaining {
            pending_writes.enqueue(header.to_vec());
            pending_writes.enqueue(data.to_vec());
            return SendStatus::Queued;
        }

        match write_queue::write_prefix(&mut &self.stream, data) {
            Ok(None) => SendStatus::Sent,
            Ok(Some(pending_data)) => {
                pending_writes.enqueue(pending_data.to_vec());
                SendStatus::Queued
            }
            Err(err) => {
                log::error!("Framed TCP send error: {}", err);
                SendStatus::ResourceNotFound
            }
        }
    }

    fn ready_to_write(&self) -> bool {
        let flush_result = self
            .pending_writes
            .lock()
            .expect(crate::util::thread::OTHER_THREAD_ERR)
            .flush(&mut &self.stream);

        match flush_result {
            Ok(_) => true,
            Err(err) => {
                log::error!("Framed TCP pending write flush error: {}", err);
                false
            }
        }
    }

    fn pending_send_bytes(&self) -> usize {
        self.pending_writes.lock().expect(crate::util::thread::OTHER_THREAD_ERR).pending_bytes()
    }

    fn pending(&self, _readiness: Readiness) -> PendingStatus {
        let status = super::tcp::check_stream_ready(&self.stream);

        if status == PendingStatus::Ready {
            if let Some(keepalive) = &self.keepalive {
                #[cfg(target_os = "windows")]
                let socket = unsafe { Socket::from_raw_socket(self.stream.as_raw_socket()) };
                #[cfg(not(target_os = "windows"))]
                let socket = unsafe { Socket::from_raw_fd(self.stream.as_raw_fd()) };

                if let Err(e) = socket.set_tcp_keepalive(keepalive) {
                    log::warn!("TCP set keepalive error: {}", e);
                }

                // Don't drop so the underlying socket is not closed.
                forget(socket);
            }
        }

        status
    }
}

pub(crate) struct LocalResource {
    listener: TcpListener,
    keepalive: Option<TcpKeepalive>,
    pending_send_limit: usize,
}

impl Resource for LocalResource {
    fn source(&mut self) -> &mut dyn Source {
        &mut self.listener
    }
}

impl Local for LocalResource {
    type Remote = RemoteResource;

    fn listen_with(config: TransportListen, addr: SocketAddr) -> io::Result<ListeningInfo<Self>> {
        let config = match config {
            TransportListen::FramedTcp(config) => config,
            _ => panic!("Internal error: Got wrong config"),
        };
        let listener = TcpListener::bind(addr)?;
        let local_addr = listener.local_addr().unwrap();
        Ok(ListeningInfo {
            local: {
                LocalResource {
                    listener,
                    keepalive: config.keepalive,
                    pending_send_limit: config.pending_send_limit,
                }
            },
            local_addr,
        })
    }

    fn accept(&self, mut accept_remote: impl FnMut(AcceptedType<'_, Self::Remote>)) {
        loop {
            match self.listener.accept() {
                Ok((stream, addr)) => accept_remote(AcceptedType::Remote(
                    addr,
                    RemoteResource::new(stream, self.keepalive.clone(), self.pending_send_limit),
                )),
                Err(ref err) if err.kind() == ErrorKind::WouldBlock => break,
                Err(ref err) if err.kind() == ErrorKind::Interrupted => continue,
                Err(err) => break log::error!("TCP accept error: {}", err), // Should not happen
            }
        }
    }
}
