//! Reproducible C10K idle-connection and mixed-load probe.
//!
//! The normal mode starts a `message-io` framed TCP echo node. A single mio poller
//! manages every connection: opening more connections does not create a read thread
//! per connection.
//!
//! Quick check:
//!
//! ```sh
//! cargo run --release --example c10k_idle -- \
//!   local --connections 10000 --active-percent 20 --rps 2000 \
//!   --idle-secs 5 --duration 15
//! ```
//!
//! Measure server and load generator separately:
//!
//! ```sh
//! cargo run --release --example c10k_idle -- server --port 7100 --duration 40
//! cargo run --release --example c10k_idle -- load --addr 127.0.0.1:7100 \
//!   --connections 10000 --active-percent 20 --rps 2000
//! ```
//!
//! A blocking thread-per-connection baseline is also included:
//!
//! ```sh
//! cargo run --release --example c10k_idle -- blocking-server --port 7101 --duration 40
//! cargo run --release --example c10k_idle -- load --addr 127.0.0.1:7101 \
//!   --connections 10000 --active-percent 20 --rps 2000
//! ```

use message_io::network::{NetEvent, SendStatus, Transport};
use message_io::node::{self, NodeEvent};

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[cfg(unix)]
use libc::{getrlimit, getrusage, rlimit, rusage, setrlimit, RLIM_INFINITY, RLIMIT_NOFILE, RUSAGE_SELF};

lazy_static::lazy_static! {
    static ref START: Instant = Instant::now();
}

#[derive(Clone)]
struct Options {
    mode: String,
    addr: String,
    port: u16,
    connections: usize,
    active_percent: u8,
    rps: u64,
    idle_secs: u64,
    duration_secs: u64,
    payload_bytes: usize,
}

impl Options {
    fn parse() -> Self {
        let mut arguments: VecDeque<String> = std::env::args().skip(1).collect();
        let mode = match arguments.front() {
            Some(first) if !first.starts_with("--") => arguments.pop_front().unwrap(),
            _ => "local".to_string(),
        };

        let server_mode = mode == "server" || mode == "blocking-server";
        let mut options = Self {
            mode,
            addr: "127.0.0.1:7100".to_string(),
            port: 7100,
            connections: 10_000,
            active_percent: 20,
            rps: 2_000,
            idle_secs: 5,
            duration_secs: if server_mode { 60 } else { 15 },
            payload_bytes: 32,
        };

        while let Some(flag) = arguments.pop_front() {
            let read_value = |arguments: &mut VecDeque<String>, flag: String| {
                arguments.pop_front().unwrap_or_else(|| panic!("missing value for {}", flag))
            };

            match flag.as_str() {
                "--addr" => options.addr = read_value(&mut arguments, flag),
                "--port" => {
                    options.port = read_value(&mut arguments, flag).parse().expect("invalid port")
                }
                "--connections" => {
                    options.connections =
                        read_value(&mut arguments, flag).parse().expect("invalid connections")
                }
                "--active-percent" => {
                    options.active_percent =
                        read_value(&mut arguments, flag).parse().expect("invalid percent")
                }
                "--rps" => {
                    options.rps = read_value(&mut arguments, flag).parse().expect("invalid rps")
                }
                "--idle-secs" => {
                    options.idle_secs =
                        read_value(&mut arguments, flag).parse().expect("invalid seconds")
                }
                "--duration" | "--duration-secs" => {
                    options.duration_secs =
                        read_value(&mut arguments, flag).parse().expect("invalid seconds")
                }
                "--payload-bytes" => {
                    options.payload_bytes =
                        read_value(&mut arguments, flag).parse().expect("invalid payload size")
                }
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                other => panic!("unknown argument: {}", other),
            }
        }

        if options.active_percent > 100 {
            panic!("--active-percent must be between 0 and 100");
        }
        options
    }

    fn socket_addr(&self) -> SocketAddr {
        self.addr
            .to_socket_addrs()
            .expect("invalid --addr")
            .next()
            .expect("--addr resolves to no socket address")
    }
}

fn print_help() {
    println!(
        "Usage:\n  \
         c10k_idle [local|server|load|blocking-server] [options]\n\n\
         Options:\n  \
         --addr HOST:PORT       load target (default 127.0.0.1:7100)\n  \
         --port PORT            server listen port (default 7100)\n  \
         --connections N        established idle connections (default 10000)\n  \
         --active-percent P     active connection percentage (default 20)\n  \
         --rps N                total requests/second across active connections (default 2000)\n  \
         --idle-secs N          idle measurement period (default 5)\n  \
         --duration N           active measurement period/server lifetime (default 15/60)\n  \
         --payload-bytes N      payload size, at least 8 (default 32)"
    );
}

#[cfg(unix)]
fn increase_open_file_limit(connections: usize) {
    unsafe {
        let mut limit: rlimit = std::mem::zeroed();
        if getrlimit(RLIMIT_NOFILE, &mut limit) != 0 {
            eprintln!("Could not read RLIMIT_NOFILE: {}", std::io::Error::last_os_error());
            return;
        }

        let desired = connections as libc::rlim_t * 2 + 256;
        let hard =
            if limit.rlim_max == RLIM_INFINITY { desired.max(65_536) } else { limit.rlim_max };
        let target = desired.min(hard);

        if limit.rlim_cur >= target {
            return;
        }

        let previous = limit.rlim_cur;
        limit.rlim_cur = target;
        if setrlimit(RLIMIT_NOFILE, &limit) == 0 {
            println!("Raised RLIMIT_NOFILE: {previous} -> {target}");
        }
        else {
            eprintln!(
                "Could not raise RLIMIT_NOFILE to {target}: {}",
                std::io::Error::last_os_error()
            );
        }
    }
}

#[cfg(not(unix))]
fn increase_open_file_limit(_connections: usize) {}

#[derive(Clone, Copy)]
struct CpuSample {
    at: Instant,
    cpu: Duration,
}

fn sample_cpu() -> CpuSample {
    #[cfg(unix)]
    unsafe {
        let mut usage: rusage = std::mem::zeroed();
        getrusage(RUSAGE_SELF, &mut usage);
        let user = Duration::new(
            usage.ru_utime.tv_sec.max(0) as u64,
            (usage.ru_utime.tv_usec.max(0) as u32) * 1_000,
        );
        let system = Duration::new(
            usage.ru_stime.tv_sec.max(0) as u64,
            (usage.ru_stime.tv_usec.max(0) as u32) * 1_000,
        );
        CpuSample { at: Instant::now(), cpu: user + system }
    }

    #[cfg(not(unix))]
    {
        CpuSample { at: Instant::now(), cpu: Duration::ZERO }
    }
}

fn report_cpu(label: &str, start: CpuSample, end: CpuSample) -> Duration {
    let wall = end.at.duration_since(start.at);
    let cpu = end.cpu.saturating_sub(start.cpu);
    let wall_seconds = wall.as_secs_f64().max(f64::EPSILON);
    let cpu_cores = cpu.as_secs_f64() / wall_seconds;
    let logical_cpus = std::thread::available_parallelism().map(usize::from).unwrap_or(1) as f64;

    println!(
        "{label}: wall={:.3}s cpu={:.3}s cpu_cores={:.3} one_core={:.1}% machine={:.2}%",
        wall.as_secs_f64(),
        cpu.as_secs_f64(),
        cpu_cores,
        cpu_cores * 100.0,
        cpu_cores * 100.0 / logical_cpus,
    );

    wall
}

fn linux_thread_count() -> Option<usize> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status
        .lines()
        .find_map(|line| line.strip_prefix("Threads:"))
        .and_then(|value| value.trim().parse().ok())
}

fn linux_fd_count() -> Option<usize> {
    Some(std::fs::read_dir("/proc/self/fd").ok()?.count())
}

fn print_runtime_shape() {
    if let Some(threads) = linux_thread_count() {
        println!("os_threads={threads}");
    }
    if let Some(fds) = linux_fd_count() {
        println!("open_fds={fds}");
    }
    if let Ok(parallelism) = std::thread::available_parallelism() {
        println!("logical_cpus={}", usize::from(parallelism));
    }
}

fn percentile(samples: &[u64], percentile: f64) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    let index = ((percentile / 100.0) * (samples.len() - 1) as f64).round() as usize;
    samples[index.min(samples.len() - 1)]
}

fn print_latencies(mut samples: Vec<u64>) {
    if samples.is_empty() {
        println!("latency: no completed requests");
        return;
    }

    samples.sort_unstable();
    let sum: u128 = samples.iter().map(|value| *value as u128).sum();
    let mean = sum as f64 / samples.len() as f64;

    println!(
        "latency_us: samples={} mean={:.1} p50={:.1} p90={:.1} p95={:.1} p99={:.1} max={:.1}",
        samples.len(),
        mean / 1_000.0,
        percentile(&samples, 50.0) as f64 / 1_000.0,
        percentile(&samples, 90.0) as f64 / 1_000.0,
        percentile(&samples, 95.0) as f64 / 1_000.0,
        percentile(&samples, 99.0) as f64 / 1_000.0,
        *samples.last().unwrap() as f64 / 1_000.0,
    );
}

#[derive(Default)]
struct Counters {
    sent: AtomicU64,
    responses: AtomicU64,
    send_errors: AtomicU64,
    connect_failures: AtomicUsize,
}

#[derive(Clone)]
enum ServerSignal {
    Stop,
}

fn main() {
    let options = Options::parse();
    increase_open_file_limit(options.connections);

    match options.mode.as_str() {
        "local" => run_local(options),
        "server" => run_reactor_server(options),
        "load" => {
            let success = run_load(&options);
            if !success {
                std::process::exit(1);
            }
        }
        "blocking-server" => run_blocking_server(options),
        other => panic!("unknown mode: {}. Use --help", other),
    }
}

fn run_local(options: Options) {
    let lifetime = Duration::from_secs(options.idle_secs + options.duration_secs + 120);
    let server_addr = spawn_reactor_server(lifetime);
    println!("in-process reactor server listening on {server_addr}");

    let mut load_options = options;
    load_options.addr = server_addr.to_string();
    if !run_load(&load_options) {
        std::process::exit(1);
    }
}

fn spawn_reactor_server(lifetime: Duration) -> SocketAddr {
    let (address_sender, address_receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let (handler, listener) = node::split::<ServerSignal>();
        let (_, address) =
            handler.network().listen(Transport::FramedTcp, "127.0.0.1:0").expect("local listen");
        address_sender.send(address).unwrap();

        handler.signals().send_with_timer(ServerSignal::Stop, lifetime);
        listener.for_each(move |event| match event {
            NodeEvent::Network(NetEvent::Message(endpoint, data)) => {
                handler.network().send(endpoint, data);
            }
            NodeEvent::Signal(ServerSignal::Stop) => handler.stop(),
            _ => {}
        });
    });

    address_receiver.recv_timeout(Duration::from_secs(10)).expect("server address")
}

fn run_reactor_server(options: Options) {
    let listen_addr: SocketAddr = format!("0.0.0.0:{}", options.port).parse().unwrap();
    let (handler, listener) = node::split::<ServerSignal>();
    let (listener_id, bound_addr) = handler
        .network()
        .listen(Transport::FramedTcp, listen_addr)
        .unwrap_or_else(|error| panic!("listen on {}: {}", listen_addr, error));

    println!(
        "reactor server listening on {bound_addr}, listener={listener_id}, target_connections={}",
        options.connections
    );
    if options.duration_secs > 0 {
        handler
            .signals()
            .send_with_timer(ServerSignal::Stop, Duration::from_secs(options.duration_secs));
    }

    let accepted = Arc::new(AtomicU64::new(0));
    let messages = Arc::new(AtomicU64::new(0));
    let target_connections = options.connections as u64;
    let cpu_start = sample_cpu();
    let callback_accepted = accepted.clone();
    let callback_messages = messages.clone();

    listener.for_each(move |event| match event {
        NodeEvent::Network(NetEvent::Accepted(_, _)) => {
            let previous = callback_accepted.fetch_add(1, Ordering::Relaxed);
            if previous + 1 == target_connections {
                println!("accepted {target_connections} connections");
                print_runtime_shape();
            }
        }
        NodeEvent::Network(NetEvent::Message(endpoint, data)) => {
            callback_messages.fetch_add(1, Ordering::Relaxed);
            if handler.network().send(endpoint, data) != SendStatus::Sent {
                eprintln!("server failed to echo {}", endpoint);
            }
        }
        NodeEvent::Signal(ServerSignal::Stop) => handler.stop(),
        _ => {}
    });

    let cpu_end = sample_cpu();
    report_cpu("server total", cpu_start, cpu_end);
    println!(
        "server summary: accepted={} messages={}",
        accepted.load(Ordering::Relaxed),
        messages.load(Ordering::Relaxed),
    );
}

fn run_load(options: &Options) -> bool {
    let server_addr = options.socket_addr();
    let active_connections = options.connections * options.active_percent as usize / 100;

    println!(
        "load -> {server_addr}: connections={}, active={} ({}%), target_rps={}, idle={}s, active_window={}s",
        options.connections,
        active_connections,
        options.active_percent,
        options.rps,
        options.idle_secs,
        options.duration_secs,
    );

    let (handler, listener) = node::split::<()>();
    let endpoints = Arc::new(Mutex::new(Vec::with_capacity(options.connections)));
    let established = Arc::new(AtomicUsize::new(0));
    let latencies = Arc::new(Mutex::new(Vec::new()));
    let counters = Arc::new(Counters::default());
    let active_wall = Arc::new(Mutex::new(Duration::ZERO));

    for _ in 0..options.connections {
        if handler.network().connect(Transport::FramedTcp, server_addr).is_err() {
            counters.connect_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    let scheduler = {
        let handler = handler.clone();
        let endpoints = endpoints.clone();
        let established = established.clone();
        let counters = counters.clone();
        let active_wall = active_wall.clone();
        let options = options.clone();

        std::thread::spawn(move || {
            let connect_deadline = Instant::now() + Duration::from_secs(180);
            while established.load(Ordering::Relaxed) < options.connections
                && Instant::now() < connect_deadline
            {
                std::thread::sleep(Duration::from_millis(25));
            }

            let connected = established.load(Ordering::Relaxed);
            if connected != options.connections {
                eprintln!(
                    "connection setup incomplete: {connected}/{} (failures={})",
                    options.connections,
                    counters.connect_failures.load(Ordering::Relaxed),
                );
                handler.stop();
                return;
            }

            println!("established {connected} framed connections");
            print_runtime_shape();
            std::thread::sleep(Duration::from_millis(500));

            let idle_start = sample_cpu();
            std::thread::sleep(Duration::from_secs(options.idle_secs));
            let idle_end = sample_cpu();
            report_cpu("idle  ", idle_start, idle_end);

            if active_connections == 0 || options.rps == 0 || options.duration_secs == 0 {
                handler.stop();
                return;
            }

            let active_endpoints = endpoints.lock().unwrap()[..active_connections].to_vec();
            let interval_nanos = 1_000_000_000u64 / options.rps.max(1);
            let interval = Duration::from_nanos(interval_nanos.max(1));
            let mut payload = vec![0_u8; options.payload_bytes.max(8)];

            let active_start = sample_cpu();
            let send_deadline = Instant::now() + Duration::from_secs(options.duration_secs);
            let mut next_send = Instant::now();
            let mut selector = 0_usize;

            while Instant::now() < send_deadline {
                let endpoint = active_endpoints[selector % active_endpoints.len()];
                selector += 1;

                let sent_nanos = START.elapsed().as_nanos() as u64;
                payload[..8].copy_from_slice(&sent_nanos.to_be_bytes());

                match handler.network().send(endpoint, &payload) {
                    SendStatus::Sent => {
                        counters.sent.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {
                        counters.send_errors.fetch_add(1, Ordering::Relaxed);
                    }
                }

                next_send += interval;
                let now = Instant::now();
                if next_send > now {
                    std::thread::sleep(next_send - now);
                }
                else {
                    next_send = now + interval;
                }
            }

            let active_end = sample_cpu();
            *active_wall.lock().unwrap() = report_cpu("active", active_start, active_end);

            let drain_deadline = Instant::now() + Duration::from_secs(10);
            while counters.responses.load(Ordering::Relaxed) < counters.sent.load(Ordering::Relaxed)
                && Instant::now() < drain_deadline
            {
                std::thread::sleep(Duration::from_millis(25));
            }

            handler.stop();
        })
    };

    let callback_endpoints = endpoints.clone();
    let callback_established = established.clone();
    let callback_latencies = latencies.clone();
    let callback_counters = counters.clone();
    listener.for_each(move |event| {
        let NodeEvent::Network(net_event) = event
        else {
            return;
        };

        match net_event {
            NetEvent::Connected(endpoint, true) => {
                callback_endpoints.lock().unwrap().push(endpoint);
                callback_established.fetch_add(1, Ordering::Relaxed);
            }
            NetEvent::Connected(_, false) => {
                callback_counters.connect_failures.fetch_add(1, Ordering::Relaxed);
            }
            NetEvent::Message(_, data) => {
                if data.len() >= 8 {
                    let mut timestamp = [0_u8; 8];
                    timestamp.copy_from_slice(&data[..8]);
                    let sent_nanos = u64::from_be_bytes(timestamp);
                    let now_nanos = START.elapsed().as_nanos() as u64;
                    if now_nanos >= sent_nanos {
                        callback_latencies.lock().unwrap().push(now_nanos - sent_nanos);
                    }
                }
                callback_counters.responses.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    });

    scheduler.join().expect("scheduler thread");

    let samples = latencies.lock().unwrap().clone();
    let sent = counters.sent.load(Ordering::Relaxed);
    let responses = counters.responses.load(Ordering::Relaxed);
    let send_errors = counters.send_errors.load(Ordering::Relaxed);
    let connect_failures = counters.connect_failures.load(Ordering::Relaxed);
    let connected = established.load(Ordering::Relaxed);
    let active_wall = *active_wall.lock().unwrap();
    let response_percent = if sent == 0 { 100.0 } else { responses as f64 * 100.0 / sent as f64 };

    let actual_rps =
        if active_wall.is_zero() { 0.0 } else { sent as f64 / active_wall.as_secs_f64() };

    println!(
        "load summary: established={connected}/{} connect_failures={connect_failures} sent={sent} responses={responses} send_errors={send_errors} actual_rps={actual_rps:.0} response_ratio={response_percent:.2}%",
        options.connections,
    );
    print_latencies(samples);

    connected == options.connections
        && connect_failures == 0
        && (active_connections == 0
            || options.rps == 0
            || (responses > 0 && responses as u128 * 100 >= sent as u128 * 99))
}

fn run_blocking_server(options: Options) {
    let listen_addr: SocketAddr = format!("0.0.0.0:{}", options.port).parse().unwrap();
    let listener = TcpListener::bind(listen_addr).expect("blocking server bind");
    listener.set_nonblocking(true).expect("nonblocking accept");

    println!(
        "thread-per-connection baseline listening on {}, target_connections={}",
        listener.local_addr().unwrap(),
        options.connections,
    );

    let accepted = Arc::new(AtomicU64::new(0));
    let read_chunks = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + Duration::from_secs(options.duration_secs.max(1));
    let cpu_start = sample_cpu();

    while Instant::now() < deadline {
        match listener.accept() {
            Ok((stream, _)) => {
                stream.set_nonblocking(false).expect("blocking socket");
                stream.set_nodelay(true).ok();

                let previous = accepted.fetch_add(1, Ordering::Relaxed);
                if previous + 1 == options.connections as u64 {
                    println!("accepted {} connections", options.connections);
                    print_runtime_shape();
                }

                let read_chunks = read_chunks.clone();
                std::thread::spawn(move || handle_blocking_connection(stream, read_chunks));
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("accept: {}", error),
        }
    }

    let cpu_end = sample_cpu();
    report_cpu("blocking server total", cpu_start, cpu_end);
    println!(
        "blocking server summary: accepted={} read_chunks={}",
        accepted.load(Ordering::Relaxed),
        read_chunks.load(Ordering::Relaxed),
    );
}

fn handle_blocking_connection(mut stream: TcpStream, read_chunks: Arc<AtomicU64>) {
    let mut buffer = [0_u8; 65_535];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(size) => {
                read_chunks.fetch_add(1, Ordering::Relaxed);
                if stream.write_all(&buffer[..size]).is_err() {
                    break;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        }
    }
}
