// quicheclient connects to an HTTP/3 echo server several times with
// cloudflare/quiche, handing each connection the session the previous one
// left behind, and says what quiche made of it: whether the handshake
// resumed, and whether the request went out as 0-RTT early data.
//
//	quicheclient <addr> <rounds> <early: 0|1> <body bytes>
use std::net::{SocketAddr, UdpSocket};
use std::time::{Duration, Instant};

use quiche::h3::NameValue;

struct Round {
    session: Vec<u8>,
    echoed: bool,
    resumed: bool,
    sent_as_0rtt: bool,
    status: u16,
}

fn round(addr: SocketAddr, session: Option<&[u8]>, early: bool, size: usize) -> Result<Round, String> {
    let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION).map_err(|e| e.to_string())?;
    config.verify_peer(false);
    config.set_application_protos(&[b"h3"]).unwrap();
    config.set_max_idle_timeout(10_000);
    config.set_max_recv_udp_payload_size(1350);
    config.set_max_send_udp_payload_size(1350);
    config.set_initial_max_data(10_000_000);
    config.set_initial_max_stream_data_bidi_local(1_000_000);
    config.set_initial_max_stream_data_bidi_remote(1_000_000);
    config.set_initial_max_stream_data_uni(1_000_000);
    config.set_initial_max_streams_bidi(100);
    config.set_initial_max_streams_uni(100);
    if early {
        config.enable_early_data();
    }

    let socket = UdpSocket::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
    let local = socket.local_addr().unwrap();
    let mut scid = [0u8; quiche::MAX_CONN_ID_LEN];
    let seed = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    for (i, b) in scid.iter_mut().enumerate() {
        *b = (seed >> ((i % 16) * 8)) as u8 ^ (local.port() >> (i % 2 * 8)) as u8 ^ (i as u8).wrapping_mul(37);
    }
    let scid = quiche::ConnectionId::from_ref(&scid);
    let mut conn = quiche::connect(Some("localhost"), &scid, local, addr, &mut config).map_err(|e| e.to_string())?;
    if let Some(s) = session {
        conn.set_session(s).map_err(|e| format!("set_session: {e}"))?;
    }

    let body: Vec<u8> = (0..size).map(|i| (i * 7 + 3) as u8).collect();
    let mut h3: Option<quiche::h3::Connection> = None;
    let h3cfg = quiche::h3::Config::new().unwrap();
    let mut stream: Option<u64> = None;
    let mut sent = 0usize;
    let mut sent_fin = false;
    let mut got = Vec::new();
    let mut status = 0u16;
    let mut done = false;
    let mut early_sent = false;
    let mut buf = [0u8; 65535];
    let mut out = [0u8; 1350];
    let deadline = Instant::now() + Duration::from_secs(15);

    while Instant::now() < deadline && !done {
        if conn.is_closed() {
            return Err(format!("connection closed before the response (resumed={})", conn.is_resumed()));
        }
        if h3.is_none() && (conn.is_established() || conn.is_in_early_data()) {
            h3 = Some(quiche::h3::Connection::with_transport(&mut conn, &h3cfg).map_err(|e| e.to_string())?);
        }
        if let Some(h) = h3.as_mut() {
            if stream.is_none() {
                let hdrs = [
                    quiche::h3::Header::new(b":method", b"POST"),
                    quiche::h3::Header::new(b":scheme", b"https"),
                    quiche::h3::Header::new(b":authority", b"localhost"),
                    quiche::h3::Header::new(b":path", b"/echo"),
                    quiche::h3::Header::new(b"content-length", size.to_string().as_bytes()),
                ];
                stream = Some(h.send_request(&mut conn, &hdrs, size == 0).map_err(|e| e.to_string())?);
                sent_fin = size == 0;
                early_sent = conn.is_in_early_data();
            }
            if let (Some(s), false) = (stream, sent_fin) {
                match h.send_body(&mut conn, s, &body[sent..], true) {
                    Ok(n) => {
                        sent += n;
                        sent_fin = sent == size;
                    }
                    Err(quiche::h3::Error::Done) => {}
                    Err(e) => return Err(format!("send_body: {e}")),
                }
            }
            loop {
                match h.poll(&mut conn) {
                    Ok((_, quiche::h3::Event::Headers { list, .. })) => {
                        for hd in &list {
                            if hd.name() == b":status" {
                                status = std::str::from_utf8(hd.value()).unwrap().parse().unwrap();
                            }
                        }
                    }
                    Ok((s, quiche::h3::Event::Data)) => {
                        while let Ok(n) = h.recv_body(&mut conn, s, &mut buf) {
                            got.extend_from_slice(&buf[..n]);
                        }
                    }
                    Ok((_, quiche::h3::Event::Finished)) => {
                        done = true;
                        break;
                    }
                    Ok((_, quiche::h3::Event::Reset(e))) => return Err(format!("stream reset {e}")),
                    Ok(_) => {}
                    Err(quiche::h3::Error::Done) => break,
                    Err(e) => return Err(format!("h3 poll: {e}")),
                }
            }
        }
        if done {
            break;
        }
        loop {
            match conn.send(&mut out) {
                Ok((n, info)) => {
                    socket.send_to(&out[..n], info.to).map_err(|e| e.to_string())?;
                }
                Err(quiche::Error::Done) => break,
                Err(e) => return Err(format!("send: {e}")),
            }
        }
        let wait = conn.timeout().unwrap_or(Duration::from_millis(100)).max(Duration::from_millis(1));
        socket.set_read_timeout(Some(wait.min(Duration::from_millis(50)))).unwrap();
        match socket.recv_from(&mut buf) {
            Ok((n, from)) => {
                let _ = conn.recv(&mut buf[..n], quiche::RecvInfo { from, to: local });
            }
            Err(_) => conn.on_timeout(),
        }
    }
    if !done {
        return Err("timeout".into());
    }
    let resumed = conn.is_resumed();
    // The server's NewSessionTicket arrives after the handshake; give it a
    // moment before asking for the session.
    let until = Instant::now() + Duration::from_millis(500);
    while conn.session().is_none() && Instant::now() < until {
        socket.set_read_timeout(Some(Duration::from_millis(20))).unwrap();
        if let Ok((n, from)) = socket.recv_from(&mut buf) {
            let _ = conn.recv(&mut buf[..n], quiche::RecvInfo { from, to: local });
        }
        while let Ok((n, info)) = conn.send(&mut out) {
            let _ = socket.send_to(&out[..n], info.to);
        }
    }
    let sess = conn.session().map(|s| s.to_vec()).unwrap_or_default();
    let _ = conn.close(true, 0, b"done");
    while let Ok((n, info)) = conn.send(&mut out) {
        let _ = socket.send_to(&out[..n], info.to);
    }
    Ok(Round { session: sess, echoed: got == body, resumed, sent_as_0rtt: early_sent, status })
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 5 {
        eprintln!("usage: quicheclient <addr> <rounds> <early 0|1> <body bytes>");
        std::process::exit(2);
    }
    let addr: SocketAddr = args[1].parse().unwrap();
    let rounds: usize = args[2].parse().unwrap();
    let early = args[3] == "1";
    let size: usize = args[4].parse().unwrap();

    let mut session: Option<Vec<u8>> = None;
    let mut resumed_rounds = 0;
    for i in 0..rounds {
        match round(addr, session.as_deref(), early, size) {
            Ok(r) => {
                println!(
                    "round {i}: status={} echoed={} resumed={} sent-as-0rtt={} ticket-received={}",
                    r.status, r.echoed, r.resumed, r.sent_as_0rtt, !r.session.is_empty()
                );
                if r.resumed {
                    resumed_rounds += 1;
                }
                if r.status != 200 || !r.echoed {
                    std::process::exit(1);
                }
                session = if r.session.is_empty() { None } else { Some(r.session) };
            }
            Err(e) => {
                println!("round {i}: FAILED: {e}");
                std::process::exit(1);
            }
        }
    }
    println!("resumed {resumed_rounds} of {} rounds after the first", rounds.saturating_sub(1));
}
