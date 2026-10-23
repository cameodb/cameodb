//! SIGHUP re-reads the TLS certificate and key, through the real binary.
//!
//! A certificate is renewed every few weeks (Let's Encrypt's lifetime is 90 days), and a node
//! that only read it at startup needed a restart each time. The reload is a wire between three
//! things a unit test cannot see together: the signal reaches the handler, the handler reaches
//! the same `RustlsConfig` the listener was bound with, and the next handshake presents what
//! was read. So this asks the listener what it presents, before and after.

use std::io::Write as _;
use std::net::TcpStream;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, ring};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, ClientConnection, DigitallySignedStruct, SignatureScheme, Stream};

mod common;

/// Accepts any certificate: the test is about *which* one is presented, not whether it is
/// trusted, and a self-signed one is not.
#[derive(Debug)]
struct AcceptAny(Arc<CryptoProvider>);

impl ServerCertVerifier for AcceptAny {
    fn verify_server_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// The leaf certificate the node presents on `port`, or `None` while it is not answering.
fn presented_certificate(port: u16) -> Option<Vec<u8>> {
    let provider = Arc::new(ring::default_provider());
    let config = ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .ok()?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAny(provider)))
        .with_no_client_auth();
    let mut conn =
        ClientConnection::new(Arc::new(config), ServerName::try_from("localhost").ok()?).ok()?;
    let mut tcp = TcpStream::connect(("127.0.0.1", port)).ok()?;
    tcp.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    // Any bytes written drive the handshake; the response is not read.
    let mut tls = Stream::new(&mut conn, &mut tcp);
    tls.write_all(b"GET /_cluster/health HTTP/1.0\r\n\r\n")
        .ok()?;
    tls.flush().ok()?;
    conn.peer_certificates()?.first().map(|c| c.to_vec())
}

fn wait_for(port: u16, expected: &[u8], what: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if presented_certificate(port).as_deref() == Some(expected) {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("{what}: the node never presented the expected certificate");
}

/// Writes a fresh self-signed certificate and its key, returning the certificate's DER.
fn write_certificate(dir: &Path) -> Vec<u8> {
    let key = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("self-signed certificate");
    std::fs::write(dir.join("cert.pem"), key.cert.pem()).expect("write cert");
    std::fs::write(dir.join("key.pem"), key.key_pair.serialize_pem()).expect("write key");
    key.cert.der().to_vec()
}

struct Node {
    child: Child,
}

impl Node {
    fn hangup(&self) {
        let status = Command::new("kill")
            .args(["-HUP", &self.child.id().to_string()])
            .status()
            .expect("kill");
        assert!(status.success());
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn sighup_makes_the_node_present_the_renewed_certificate() {
    let dir = tempfile::tempdir().expect("temp dir");
    let certs = dir.path().join("certs");
    std::fs::create_dir_all(&certs).expect("certs dir");
    let port = common::reserve_port();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).expect("data dir");
    let first = write_certificate(&certs);

    let config = format!(
        r#"
[node]
label = "tls-reload-test"
profile = "local"

[network.http]
bind_address = "127.0.0.1"
port = {port}

[network.http.tls]
enabled = true
cert_file = "{cert}"
key_file = "{key}"

[network.cluster]
enabled = false

[storage]
data_paths = ["{data}"]
num_shards_init = 1
max_shards_per_node = 1
"#,
        cert = certs.join("cert.pem").display(),
        key = certs.join("key.pem").display(),
        data = data.display(),
    );
    let config_path = dir.path().join("cameodb.toml");
    std::fs::write(&config_path, config).expect("write config");

    let node = Node {
        child: Command::new(env!("CARGO_BIN_EXE_cameodb"))
            .arg("-c")
            .arg(&config_path)
            .env("RUST_LOG", "warn")
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn cameodb"),
    };

    wait_for(port, &first, "startup");

    // A renewed certificate on disk changes nothing until the node is told.
    let second = write_certificate(&certs);
    assert_ne!(first, second);
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        presented_certificate(port).as_deref(),
        Some(first.as_slice()),
        "the node picked a certificate up without being signalled"
    );

    node.hangup();
    wait_for(port, &second, "after SIGHUP");

    // A reload that cannot read the files keeps what is in use: the node does not lose its
    // certificate, and does not stop answering, because a renewal went wrong.
    std::fs::write(certs.join("cert.pem"), "not a certificate").expect("corrupt cert");
    node.hangup();
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(
        presented_certificate(port).as_deref(),
        Some(second.as_slice()),
        "a refused reload must keep the certificate in use"
    );

    // And a later good one still takes effect.
    let third = write_certificate(&certs);
    node.hangup();
    wait_for(port, &third, "after the repair");
}
