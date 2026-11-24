use message_io::network::{NetEvent, Transport, SendStatus};
use message_io::node::{self, NodeEvent};
use message_io::util::thread::{NamespacedThread};
use message_io::adapters::udp::{self};

use test_case::test_case;

use rand::{SeedableRng, Rng};

use std::collections::{HashSet};
use std::net::{SocketAddr};
use std::time::{Duration, Instant};

const LOCAL_ADDR: &'static str = "127.0.0.1:0";
const MIN_MESSAGE: &'static [u8] = &[42];
const SMALL_MESSAGE: &'static str = "Integration test message";
const BIG_MESSAGE_SIZE: usize = 1024 * 1024 * 8; // 8MB

lazy_static::lazy_static! {
    pub static ref TIMEOUT: Duration = Duration::from_secs(60);
    pub static ref TIMEOUT_SMALL: Duration = Duration::from_secs(1);
}

// Common error messages
const TIMEOUT_EVENT_RECV_ERR: &'static str = "Timeout, but an event was expected.";

mod util {
    use std::sync::{Once};

    // Used to init the log only one time for all tests;
    static INIT: Once = Once::new();

    #[allow(dead_code)]
    pub enum LogThread {
        Enabled,
        Disabled,
    }

    #[allow(dead_code)]
    pub fn init_logger(log_thread: LogThread) {
        INIT.call_once(|| configure_logger(log_thread).unwrap());
    }

    fn configure_logger(log_thread: LogThread) -> Result<(), fern::InitError> {
        fern::Dispatch::new()
            .filter(|metadata| metadata.target().starts_with("message_io"))
            .format(move |out, message, record| {
                let thread_name = format!("[{}]", std::thread::current().name().unwrap());
                out.finish(format_args!(
                    "[{}][{}][{}]{} {}",
                    chrono::Local::now().format("%M:%S:%f"), // min:sec:nano
                    record.level(),
                    record.target().strip_prefix("message_io::").unwrap_or(record.target()),
                    if let LogThread::Enabled = log_thread { thread_name } else { String::new() },
                    message,
                ))
            })
            .chain(std::io::stdout())
            .apply()?;
        Ok(())
    }
}

#[allow(unused_imports)]
use util::{LogThread};

fn start_echo_server(
    transport: Transport,
    expected_clients: usize,
) -> (NamespacedThread<()>, SocketAddr) {
    let (tx, rx) = crossbeam_channel::bounded(1);
    let thread = NamespacedThread::spawn("test-server", move || {
        let mut messages_received = 0;
        let mut disconnections = 0;
        let mut clients = HashSet::new();

        let (node, listener) = node::split();
        node.signals().send_with_timer((), *TIMEOUT);

        let (listener_id, server_addr) = node.network().listen(transport, LOCAL_ADDR).unwrap();
        tx.send(server_addr).unwrap();

        listener.for_each(move |event| match event {
            NodeEvent::Signal(_) => panic!("{}", TIMEOUT_EVENT_RECV_ERR),
            NodeEvent::Network(net_event) => match net_event {
                NetEvent::Connected(..) => unreachable!(),
                NetEvent::Accepted(endpoint, id) => {
                    assert_eq!(listener_id, id);
                    match transport.is_connection_oriented() {
                        true => assert!(clients.insert(endpoint)),
                        false => unreachable!(),
                    }
                }
                NetEvent::Message(endpoint, data) => {
                    assert_eq!(MIN_MESSAGE, data);

                    let status = node.network().send(endpoint, &data);
                    assert_eq!(SendStatus::Sent, status);

                    messages_received += 1;

                    if !transport.is_connection_oriented() {
                        // We assume here that if the protocol is not
                        // connection-oriented it will no create a resource.
                        // The remote will be managed from the listener resource
                        assert_eq!(listener_id, endpoint.resource_id());
                        if messages_received == expected_clients {
                            node.stop() //Exit from thread.
                        }
                    }
                }
                NetEvent::Disconnected(endpoint) => {
                    match transport.is_connection_oriented() {
                        true => {
                            disconnections += 1;
                            assert!(clients.remove(&endpoint));
                            if disconnections == expected_clients {
                                assert_eq!(expected_clients, messages_received);
                                assert_eq!(0, clients.len());
                                node.stop() //Exit from thread.
                            }
                        }
                        false => unreachable!(),
                    }
                }
            },
        });
    });

    let server_addr = rx.recv_timeout(*TIMEOUT).expect(TIMEOUT_EVENT_RECV_ERR);
    (thread, server_addr)
}

fn start_echo_client_manager(
    transport: Transport,
    server_addr: SocketAddr,
    clients_number: usize,
) -> NamespacedThread<()> {
    NamespacedThread::spawn("test-client", move || {
        let (node, listener) = node::split();
        node.signals().send_with_timer((), *TIMEOUT);

        let mut clients = HashSet::new();
        let mut received = 0;

        for _ in 0..clients_number {
            node.network().connect(transport, server_addr).unwrap();
        }

        listener.for_each(move |event| match event {
            NodeEvent::Signal(_) => panic!("{}", TIMEOUT_EVENT_RECV_ERR),
            NodeEvent::Network(net_event) => match net_event {
                NetEvent::Connected(server, status) => {
                    assert!(status);
                    let status = node.network().send(server, MIN_MESSAGE);
                    assert_eq!(SendStatus::Sent, status);
                    assert!(clients.insert(server));
                }
                NetEvent::Message(endpoint, data) => {
                    assert!(clients.remove(&endpoint));
                    assert_eq!(MIN_MESSAGE, data);
                    node.network().remove(endpoint.resource_id());

                    received += 1;
                    if received == clients_number {
                        node.stop(); //Exit from thread.
                    }
                }
                NetEvent::Accepted(..) => unreachable!(),
                NetEvent::Disconnected(_) => unreachable!(),
            },
        });
    })
}

fn start_burst_receiver(
    transport: Transport,
    expected_count: usize,
) -> (NamespacedThread<()>, SocketAddr) {
    let (tx, rx) = crossbeam_channel::bounded(1);
    let thread = NamespacedThread::spawn("test-receiver", move || {
        let (node, listener) = node::split();
        node.signals().send_with_timer((), *TIMEOUT);

        let (_, receiver_addr) = node.network().listen(transport, LOCAL_ADDR).unwrap();
        tx.send(receiver_addr).unwrap();

        let mut count = 0;
        listener.for_each(move |event| match event {
            NodeEvent::Signal(_) => panic!("{}", TIMEOUT_EVENT_RECV_ERR),
            NodeEvent::Network(net_event) => match net_event {
                NetEvent::Connected(..) => unreachable!(),
                NetEvent::Accepted(..) => (),
                NetEvent::Message(_, data) => {
                    let expected_message = format!("{}: {}", SMALL_MESSAGE, count);
                    assert_eq!(expected_message, String::from_utf8_lossy(&data));
                    count += 1;
                    if count == expected_count {
                        node.stop();
                    }
                }
                NetEvent::Disconnected(_) => (),
            },
        });
    });
    (thread, rx.recv().unwrap())
}

fn start_burst_sender(
    transport: Transport,
    receiver_addr: SocketAddr,
    expected_count: usize,
) -> NamespacedThread<()> {
    NamespacedThread::spawn("test-sender", move || {
        let (node, listener) = node::split::<()>();

        let (receiver, _) = node.network().connect(transport, receiver_addr).unwrap();

        let mut count = 0;
        listener.for_each(move |event| match event {
            NodeEvent::Signal(_) => {
                if count < expected_count {
                    let message = format!("{}: {}", SMALL_MESSAGE, count);
                    let status = node.network().send(receiver, message.as_bytes());
                    assert_eq!(SendStatus::Sent, status);

                    count += 1;
                    if !transport.is_connection_oriented() {
                        // We need a rate to not lose packet.
                        node.signals().send_with_timer((), Duration::from_micros(50));
                    }
                    else {
                        node.signals().send(());
                    }
                }
                else {
                    node.stop();
                }
            }
            NodeEvent::Network(net_event) => match net_event {
                NetEvent::Connected(_, status) => {
                    assert!(status);
                    node.signals().send(());
                }
                NetEvent::Disconnected(_) => (),
                _ => unreachable!(),
            },
        });
    })
}

#[cfg_attr(feature = "tcp", test_case(Transport::Tcp, 1))]
#[cfg_attr(feature = "tcp", test_case(Transport::Tcp, 100))]
#[cfg_attr(feature = "tcp", test_case(Transport::FramedTcp, 1))]
#[cfg_attr(feature = "tcp", test_case(Transport::FramedTcp, 100))]
#[cfg_attr(feature = "udp", test_case(Transport::Udp, 1))]
#[cfg_attr(feature = "udp", test_case(Transport::Udp, 100))]
#[cfg_attr(feature = "websocket", test_case(Transport::Ws, 1))]
#[cfg_attr(feature = "websocket", test_case(Transport::Ws, 100))]
// NOTE: A medium-high `clients` value can exceeds the "open file" limits of an OS in CI
// with an obfuscated error message.
fn echo(transport: Transport, clients: usize) {
    //util::init_logger(LogThread::Enabled); // Enable it for better debugging

    let (_server_thread, server_addr) = start_echo_server(transport, clients);
    let _client_thread = start_echo_client_manager(transport, server_addr, clients);
}

// Tcp: Does not apply: it's stream based
#[cfg_attr(feature = "udp", test_case(Transport::Udp, 2000))]
#[cfg_attr(feature = "tcp", test_case(Transport::FramedTcp, 200000))]
#[cfg_attr(feature = "websocket", test_case(Transport::Ws, 200000))]
fn burst(transport: Transport, messages_count: usize) {
    //util::init_logger(LogThread::Enabled); // Enable it for better debugging

    let (_receiver_thread, server_addr) = start_burst_receiver(transport, messages_count);
    let _sender_thread = start_burst_sender(transport, server_addr, messages_count);
}

// Two frames flushed in one write reach the listener in one socket read. Both must
// come out of that readiness event: the poll is edge-triggered and the peer sends
// nothing more.
#[cfg(feature = "websocket")]
#[test]
fn websocket_frames_sharing_one_read() {
    //util::init_logger(LogThread::Enabled); // Enable it for better debugging

    let (tx, rx) = crossbeam_channel::bounded(1);
    let mut receiver_thread = NamespacedThread::spawn("test-receiver", move || {
        let (node, listener) = node::split::<()>();
        node.signals().send_with_timer((), Duration::from_secs(5));

        let (_, receiver_addr) = node.network().listen(Transport::Ws, LOCAL_ADDR).unwrap();
        tx.send(receiver_addr).unwrap();

        let mut count = 0;
        listener.for_each(move |event| match event {
            NodeEvent::Signal(_) => panic!("{}", TIMEOUT_EVENT_RECV_ERR),
            NodeEvent::Network(net_event) => match net_event {
                NetEvent::Connected(..) => unreachable!(),
                NetEvent::Accepted(..) => (),
                NetEvent::Message(_, data) => {
                    assert_eq!(SMALL_MESSAGE.as_bytes(), data);
                    count += 1;
                    if count == 2 {
                        node.stop();
                    }
                }
                NetEvent::Disconnected(_) => (),
            },
        });
    });

    let receiver_addr = rx.recv().unwrap();
    let (mut socket, _) = tungstenite::connect(format!("ws://{}", receiver_addr)).unwrap();
    for _ in 0..2 {
        let frame = tungstenite::Message::Binary(SMALL_MESSAGE.as_bytes().to_vec().into());
        socket.write(frame).unwrap();
    }
    socket.flush().unwrap();

    receiver_thread.join();
}

#[cfg_attr(feature = "tcp", test_case(Transport::Tcp, BIG_MESSAGE_SIZE))]
#[cfg_attr(feature = "tcp", test_case(Transport::FramedTcp, BIG_MESSAGE_SIZE))]
#[cfg_attr(feature = "udp", test_case(Transport::Udp, udp::MAX_LOCAL_PAYLOAD_LEN))]
#[cfg_attr(feature = "websocket", test_case(Transport::Ws, BIG_MESSAGE_SIZE))]
fn message_size(transport: Transport, message_size: usize) {
    //util::init_logger(LogThread::Disabled); // Enable it for better debugging

    assert!(message_size <= transport.max_message_size());

    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    let sent_message: Vec<u8> = (0..message_size).map(|_| rng.random()).collect();

    let (node, listener) = node::split();
    node.signals().send_with_timer((), *TIMEOUT);

    let (_, receiver_addr) = node.network().listen(transport, LOCAL_ADDR).unwrap();
    let (receiver, _) = node.network().connect(transport, receiver_addr).unwrap();

    let mut _async_sender: Option<NamespacedThread<()>> = None;
    let mut received_message = Vec::new();

    listener.for_each(move |event| match event {
        NodeEvent::Signal(_) => panic!("{}", TIMEOUT_EVENT_RECV_ERR),
        NodeEvent::Network(net_event) => match net_event {
            NetEvent::Connected(endpoint, status) => {
                assert!(status);
                assert_eq!(receiver, endpoint);

                let node = node.clone();
                let sent_message = sent_message.clone();

                // Protocols as TCP blocks the sender if the receiver is not reading data
                // and its buffer is fill.
                _async_sender = Some(NamespacedThread::spawn("test-sender", move || {
                    let status = node.network().send(receiver, &sent_message);
                    assert!(matches!(status, SendStatus::Sent | SendStatus::Queued));

                    // A queued write is flushed by the network thread. Wait for the
                    // userspace queue to drain before removing the endpoint; the OS
                    // still retains its normal send buffer.
                    for _ in 0..1000 {
                        if node.network().pending_send_bytes(receiver.resource_id()) == Some(0) {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    assert_eq!(node.network().pending_send_bytes(receiver.resource_id()), Some(0));
                    assert!(node.network().remove(receiver.resource_id()));
                }));
            }
            NetEvent::Accepted(..) => (),
            NetEvent::Message(_, data) => {
                if transport.is_packet_based() {
                    received_message = data.to_vec();
                    assert_eq!(sent_message, received_message);
                    node.stop();
                }
                else {
                    received_message.extend_from_slice(&data);
                }
            }
            NetEvent::Disconnected(_) => {
                assert_eq!(sent_message.len(), received_message.len());
                assert_eq!(sent_message, received_message);
                node.stop();
            }
        },
    });
}

#[cfg_attr(feature = "tcp", test_case(Transport::Tcp))]
#[cfg_attr(feature = "tcp", test_case(Transport::FramedTcp))]
fn slow_consumer_applies_backpressure(transport: Transport) {
    let peer_listener = std::net::TcpListener::bind(LOCAL_ADDR).unwrap();
    let peer_addr = peer_listener.local_addr().unwrap();
    let (start_reading_tx, start_reading_rx) = crossbeam_channel::bounded(1);

    let peer_thread = std::thread::spawn(move || {
        let (mut socket, _) = peer_listener.accept().unwrap();
        start_reading_rx.recv_timeout(*TIMEOUT).unwrap();

        let mut received = 0usize;
        let mut buffer = [0u8; 64 * 1024];
        loop {
            match std::io::Read::read(&mut socket, &mut buffer) {
                Ok(0) => break,
                Ok(bytes) => received += bytes,
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(_) => break,
            }
        }
        received
    });

    let (handler, listener) = node::split::<()>();
    let node_thread = std::thread::spawn(move || listener.for_each(|_| {}));

    let (endpoint, _) = handler.network().connect(transport, peer_addr).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while handler.network().is_ready(endpoint.resource_id()) != Some(true) {
        assert!(Instant::now() < deadline, "connection did not become ready");
        std::thread::sleep(Duration::from_millis(10));
    }

    let chunk = vec![7u8; 512 * 1024]; // File blocks are hundreds of KiB.
    let mut accepted_before_backpressure = 0;
    let mut saw_backpressure = false;

    for _ in 0..1000 {
        match handler.network().send(endpoint, &chunk) {
            SendStatus::Sent | SendStatus::Queued => accepted_before_backpressure += 1,
            SendStatus::Backpressure => {
                saw_backpressure = true;
                break;
            }
            status => panic!("unexpected send status while peer is slow: {:?}", status),
        }
    }

    assert!(saw_backpressure, "a full send queue must report backpressure");
    assert!(
        accepted_before_backpressure < 64,
        "too many chunks accepted before backpressure: {}",
        accepted_before_backpressure
    );

    let pending = handler.network().pending_send_bytes(endpoint.resource_id()).unwrap();
    assert!(pending < 4 * 1024 * 1024, "unexpectedly large pending queue: {}", pending);

    // Ignoring the signal and continuing to call send must not grow the queue.
    for _ in 0..1000 {
        assert_eq!(handler.network().send(endpoint, &chunk), SendStatus::Backpressure);
    }
    assert_eq!(
        handler.network().pending_send_bytes(endpoint.resource_id()),
        Some(pending),
        "rejected sends must not append more data"
    );

    // The signal tells the producer to slow down, not to disconnect. Once the peer
    // reads again, the bounded queue drains and the same rejected chunk can be retried.
    start_reading_tx.send(()).unwrap();

    let deadline = Instant::now() + Duration::from_secs(10);
    while handler.network().pending_send_bytes(endpoint.resource_id()) != Some(0) {
        assert!(Instant::now() < deadline, "pending writes did not drain");
        std::thread::sleep(Duration::from_millis(20));
    }

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match handler.network().send(endpoint, &chunk) {
            SendStatus::Sent => break,
            SendStatus::Queued => {
                let drain_deadline = Instant::now() + Duration::from_secs(10);
                while handler.network().pending_send_bytes(endpoint.resource_id()) != Some(0) {
                    assert!(Instant::now() < drain_deadline, "retried chunk did not drain");
                    std::thread::sleep(Duration::from_millis(20));
                }
                break;
            }
            SendStatus::Backpressure => {
                assert!(Instant::now() < deadline, "connection did not recover");
                std::thread::sleep(Duration::from_millis(20));
            }
            status => panic!("unexpected send status during recovery: {:?}", status),
        }
    }

    handler.stop();
    node_thread.join().unwrap();
    drop(handler);

    let received = peer_thread.join().unwrap();
    assert!(received > 0, "peer should receive queued and retried data");
}

#[cfg(target_os = "macos")]
#[ignore = "macos requires to enable permission for multicast"]
#[test]
fn multicast_reuse_addr() {
    //util::init_logger(LogThread::Disabled); // Enable it for better debugging

    let (node, listener) = node::split();
    node.signals().send_with_timer((), *TIMEOUT_SMALL);

    let multicast_addr = "239.255.0.1:3015";
    let (id_1, _) = node.network().listen(Transport::Udp, multicast_addr).unwrap();
    let (id_2, _) = node.network().listen(Transport::Udp, multicast_addr).unwrap();

    let (target, local_addr) = node.network().connect(Transport::Udp, multicast_addr).unwrap();

    let mut received = 0;
    listener.for_each(move |event| match event {
        NodeEvent::Signal(_) => panic!("{}", TIMEOUT_EVENT_RECV_ERR),
        NodeEvent::Network(net_event) => match net_event {
            NetEvent::Connected(endpoint, status) => {
                assert!(status);
                assert_eq!(endpoint, target);
                let status = node.network().send(target, &[42]);
                assert_eq!(status, SendStatus::Sent);
            }
            NetEvent::Message(endpoint, data) => {
                if endpoint.resource_id() != id_1 {
                    assert_eq!(endpoint.resource_id(), id_2);
                }
                if endpoint.resource_id() != id_2 {
                    assert_eq!(endpoint.resource_id(), id_1);
                }
                assert_eq!(data, [42]);
                assert_eq!(endpoint.addr(), local_addr);

                received += 1;
                if received == 2 {
                    node.stop();
                }
            }
            NetEvent::Accepted(..) => unreachable!(),
            NetEvent::Disconnected(_) => unreachable!(),
        },
    });
}
