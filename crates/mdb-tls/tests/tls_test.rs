//! TLS 集成测试。

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

use mdb_tls::config::TlsConfig;
use mdb_tls::stream::TlsStream;
use mdb_tls::cert;
use mdb_tls::cipher::{self, GCM_IV_LEN};
use mdb_tls::prf;
use mdb_tls::handshake::{ClientHello, ECC_SM4_SM3, SUPPORTED_CIPHERS};

/// 基本的 SM4-GCM 加密解密测试。
#[test]
fn sm4_gcm_roundtrip() {
    let key = [0x42u8; 16];
    let iv = [0x99u8; GCM_IV_LEN];
    let plaintext = b"Hello, TLCP!";
    let aad = b"additional data";

    let output = cipher::gcm_encrypt(&key, &iv, plaintext, aad).unwrap();
    let decrypted = cipher::gcm_decrypt(&key, &iv, &output.ciphertext, aad, &output.tag).unwrap();
    assert_eq!(decrypted, plaintext);
}

/// SM4-GCM 不同密钥解密失败。
#[test]
fn sm4_gcm_wrong_key_fails() {
    let key1 = [0x42u8; 16];
    let key2 = [0x43u8; 16];
    let iv = [0x99u8; GCM_IV_LEN];
    let plaintext = b"secret data";

    let output = cipher::gcm_encrypt(&key1, &iv, plaintext, b"").unwrap();
    assert!(cipher::gcm_decrypt(&key2, &iv, &output.ciphertext, b"", &output.tag).is_err());
}

/// SM4-GCM 不同 AAD 验证失败。
#[test]
fn sm4_gcm_wrong_aad_fails() {
    let key = [0x42u8; 16];
    let iv = [0x99u8; GCM_IV_LEN];

    let output = cipher::gcm_encrypt(&key, &iv, b"data", b"aad1").unwrap();
    assert!(cipher::gcm_decrypt(&key, &iv, &output.ciphertext, b"aad2", &output.tag).is_err());
}

/// SM4-GCM 大数据加解密。
#[test]
fn sm4_gcm_large_data() {
    let key = [0x01u8; 16];
    let iv = [0x02u8; GCM_IV_LEN];
    let plaintext: Vec<u8> = (0..8192).map(|i| (i % 256) as u8).collect();

    let output = cipher::gcm_encrypt(&key, &iv, &plaintext, b"large").unwrap();
    let decrypted = cipher::gcm_decrypt(&key, &iv, &output.ciphertext, b"large", &output.tag).unwrap();
    assert_eq!(decrypted, plaintext);
}

/// PRF 密钥派生测试。
#[test]
fn prf_key_derivation() {
    let secret = [0xabu8; 48];
    let client_random = [0x01u8; 32];
    let server_random = [0x02u8; 32];

    let (ck, sk, ci, si) = prf::extract_keys(&secret, &server_random, &client_random);
    assert_ne!(ck, sk);
    assert_ne!(ci, si);
    assert_eq!(ck.len(), 16);
    assert_eq!(sk.len(), 16);
    assert_eq!(ci.len(), 12);
    assert_eq!(si.len(), 12);
}

/// PRF Finished verify_data 测试。
#[test]
fn prf_finished_verify() {
    let master = [0x33u8; 48];
    let hs_hash = [0x44u8; 32];

    let client_vd = prf::verify_data(&master, b"client finished", &hs_hash);
    let server_vd = prf::verify_data(&master, b"server finished", &hs_hash);
    assert_ne!(client_vd, server_vd);
    assert_eq!(client_vd.len(), 12);
    assert_eq!(server_vd.len(), 12);
}

/// 证书生成与 PEM 编解码测试。
#[test]
fn cert_pem_roundtrip() {
    let key = mdb_crypto::random::bytes::<32>();
    let cert = cert::generate_self_signed("test.mdb.local", &key);
    assert!(cert.is_valid_now());

    let pem = cert::to_pem(&cert);
    assert!(pem.contains("BEGIN CERTIFICATE"));
    let decoded = cert::from_pem(&pem).unwrap();
    assert_eq!(decoded.subject, "test.mdb.local");
    assert_eq!(decoded.public_key, cert.public_key);
}

/// ClientHello 编解码测试。
#[test]
fn client_hello_encode_decode() {
    let ch = ClientHello::new(SUPPORTED_CIPHERS.to_vec());
    let body = ch.encode_body();
    let decoded = ClientHello::decode_body(&body).unwrap();
    assert_eq!(decoded.cipher_suites, vec![ECC_SM4_SM3]);
    assert_eq!(decoded.random.random, ch.random.random);
}

/// TLS 握手全流程测试（使用 TCP 回环）。
#[test]
fn tls_handshake_full() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let config = TlsConfig::new().with_enabled(true);

    let server_config = config.clone();
    let server_handle = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut tls = TlsStream::accept(stream, &server_config).unwrap();
        // 读取客户端发送的数据
        let mut buf = [0u8; 1024];
        let n = tls.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"hello from client");
        // 回显
        tls.write_all(b"hello from server").unwrap();
        tls.flush().unwrap();
    });

    let client_config = config;
    let stream = TcpStream::connect(addr).unwrap();
    let mut tls = TlsStream::connect(stream, &client_config).unwrap();
    tls.write_all(b"hello from client").unwrap();
    tls.flush().unwrap();

    let mut buf = [0u8; 1024];
    let n = tls.read(&mut buf).unwrap();
    assert_eq!(&buf[..n], b"hello from server");

    server_handle.join().unwrap();
}

/// TLS 多消息传输测试。
#[test]
fn tls_multiple_messages() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let config = TlsConfig::new().with_enabled(true);

    let server_config = config.clone();
    let server_handle = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut tls = TlsStream::accept(stream, &server_config).unwrap();
        let mut buf = [0u8; 4096];
        for i in 0..10 {
            let n = tls.read(&mut buf).unwrap();
            let msg = format!("echo-{i}");
            assert_eq!(&buf[..n], msg.as_bytes());
        }
        // 回送确认
        tls.write_all(b"done").unwrap();
        tls.flush().unwrap();
    });

    let client_config = config;
    let stream = TcpStream::connect(addr).unwrap();
    let mut tls = TlsStream::connect(stream, &client_config).unwrap();

    for i in 0..10 {
        let msg = format!("echo-{i}");
        tls.write_all(msg.as_bytes()).unwrap();
        tls.flush().unwrap();
    }

    let mut buf = [0u8; 64];
    let n = tls.read(&mut buf).unwrap();
    assert_eq!(&buf[..n], b"done");

    server_handle.join().unwrap();
}

/// TLS close_notify 测试。
#[test]
fn tls_close_notify() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let config = TlsConfig::new().with_enabled(true);

    let server_config = config.clone();
    let server_handle = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut tls = TlsStream::accept(stream, &server_config).unwrap();
        tls.write_all(b"data").unwrap();
        tls.flush().unwrap();
        tls.close().unwrap();
    });

    let client_config = config;
    let stream = TcpStream::connect(addr).unwrap();
    let mut tls = TlsStream::connect(stream, &client_config).unwrap();

    // 读取数据
    let mut buf = [0u8; 64];
    let n = tls.read(&mut buf).unwrap();
    assert_eq!(&buf[..n], b"data");

    // 读取应返回 EOF（close_notify）
    let n = tls.read(&mut buf).unwrap();
    assert_eq!(n, 0);

    server_handle.join().unwrap();
}
