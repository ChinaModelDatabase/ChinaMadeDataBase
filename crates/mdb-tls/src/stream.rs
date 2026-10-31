//! TLS 流包装器。
//!
//! `TlsStream` 包装底层 `TcpStream`，提供透明的 TLS 加密读写。
//! 握手在首次读写时自动完成。

use std::io::{self, Read, Write};
use std::net::TcpStream;

use mdb_common::{ErrorCode, MdbError, MdbResult};
use mdb_crypto::digest::Digest;
use mdb_crypto::random;
use mdb_crypto::sm3::Sm3;

use crate::alert::{Alert, AlertDescription};
use crate::cert;
use crate::config::{TlsConfig, TlsSession};
use crate::handshake::{
    self, CertificateMsg, ClientHello, ClientKeyExchange, ECC_SM4_SM3,
    Finished, ServerHello, ServerHelloDone, ServerKeyExchange, msg_type,
    SUPPORTED_CIPHERS,
};
use crate::prf;
use crate::record::{self, RecordReader, RecordWriter, content_type};

/// TLS 流包装器。
///
/// 握手阶段使用明文记录，加密阶段使用 SM4-GCM。
#[derive(Debug)]
pub struct TlsStream {
    stream: TcpStream,
    session: TlsSession,
    reader: RecordReader,
    /// 握手消息缓冲（用于 Finished 验证）。
    handshake_data: Vec<Vec<u8>>,
}

impl TlsStream {
    /// 客户端：连接并执行 TLS 握手。
    pub fn connect(stream: TcpStream, config: &TlsConfig) -> MdbResult<Self> {
        let mut tls = Self {
            stream,
            session: TlsSession::new(),
            reader: RecordReader::new(),
            handshake_data: Vec::new(),
        };
        tls.client_handshake(config)?;
        Ok(tls)
    }

    /// 服务端：接受连接并执行 TLS 握手。
    pub fn accept(stream: TcpStream, config: &TlsConfig) -> MdbResult<Self> {
        let mut tls = Self {
            stream,
            session: TlsSession::new(),
            reader: RecordReader::new(),
            handshake_data: Vec::new(),
        };
        tls.server_handshake(config)?;
        Ok(tls)
    }

    /// 客户端握手。
    fn client_handshake(&mut self, _config: &TlsConfig) -> MdbResult<()> {
        // 1. ClientHello
        let client_hello = ClientHello::new(SUPPORTED_CIPHERS.to_vec());
        self.session.client_random = client_hello.random.random;
        let ch_body = client_hello.encode_body();
        let ch_msg = handshake::wrap_handshake(msg_type::CLIENT_HELLO, &ch_body);
        self.handshake_data.push(ch_msg.clone());
        RecordWriter::write_plaintext(&mut self.stream, content_type::HANDSHAKE, &ch_msg)?;

        // 2. ServerHello
        let sh_record = self.reader.read_plaintext_record(&mut self.stream)?;
        self.record_to_handshake_data(&sh_record.payload);
        let sh_body = &sh_record.payload[4..];
        let server_hello = ServerHello::decode_body(sh_body)?;
        self.session.server_random = server_hello.random.random;
        self.session.cipher_suite = server_hello.cipher_suite.0;

        // 3. Certificate
        let cert_record = self.reader.read_plaintext_record(&mut self.stream)?;
        self.record_to_handshake_data(&cert_record.payload);

        // 4. ServerKeyExchange
        let ske_record = self.reader.read_plaintext_record(&mut self.stream)?;
        self.record_to_handshake_data(&ske_record.payload);
        let ske_body = &ske_record.payload[4..];
        let server_kex = ServerKeyExchange::decode_body(ske_body)?;

        // 5. ServerHelloDone
        let shd_record = self.reader.read_plaintext_record(&mut self.stream)?;
        self.record_to_handshake_data(&shd_record.payload);

        // 6. ClientKeyExchange
        let client_public_key = random::bytes::<32>();
        let cke = ClientKeyExchange::new(client_public_key.to_vec());
        let cke_body = cke.encode_body();
        let cke_msg = handshake::wrap_handshake(msg_type::CLIENT_KEY_EXCHANGE, &cke_body);
        self.handshake_data.push(cke_msg.clone());
        RecordWriter::write_plaintext(&mut self.stream, content_type::HANDSHAKE, &cke_msg)?;

        // 7. 密钥派生（使用服务器发送的 pre_master_secret）
        self.derive_keys(&server_kex.public_key, true)?;

        // 8. ChangeCipherSpec
        RecordWriter::write_plaintext(&mut self.stream, content_type::CHANGE_CIPHER_SPEC, &[1])?;

        // 9. Client Finished
        let master = self.session.master_secret.unwrap();
        let hs_hash = handshake::handshake_hash(
            &self.handshake_data.iter().map(|v| v.as_slice()).collect::<Vec<_>>(),
        );
        let client_verify = prf::verify_data(&master, b"client finished", &hs_hash);
        let fin = Finished { verify_data: client_verify };
        let fin_body = fin.encode_body();
        let fin_msg = handshake::wrap_handshake(msg_type::FINISHED, &fin_body);
        self.handshake_data.push(fin_msg.clone());

        let seq = self.session.next_write_seq();
        let iv = self.session.write_iv.unwrap();
        let key = self.session.write_key.unwrap();
        RecordWriter::write_encrypted(
            &mut self.stream,
            content_type::HANDSHAKE,
            &fin_msg,
            &key,
            &iv,
            seq,
        )?;

        // 10. 读取 Server ChangeCipherSpec
        let ccs_record = self.reader.read_plaintext_record(&mut self.stream)?;
        if ccs_record.content_type != content_type::CHANGE_CIPHER_SPEC {
            return Err(MdbError::new(
                ErrorCode::FailedToParse,
                "expected ChangeCipherSpec from server",
            ));
        }

        // 11. 读取 Server Finished（加密）
        let server_seq = self.session.next_read_seq();
        let server_iv = self.session.read_iv.unwrap();
        let server_key = self.session.read_key.unwrap();
        let fin_record = self.reader.read_encrypted_record(
            &mut self.stream,
            &server_key,
            &server_iv,
            server_seq,
        )?;

        let fin_body = &fin_record.payload[4..];
        let server_fin = Finished::decode_body(fin_body)?;
        let server_hs_hash = handshake::handshake_hash(
            &self.handshake_data.iter().map(|v| v.as_slice()).collect::<Vec<_>>(),
        );
        let expected_verify = prf::verify_data(&master, b"server finished", &server_hs_hash);
        if !mdb_crypto::constant_time_eq(&server_fin.verify_data, &expected_verify) {
            send_fatal_alert(&mut self.stream, AlertDescription::HandshakeFailure)?;
            return Err(MdbError::new(ErrorCode::FailedToParse, "server Finished verification failed"));
        }

        self.session.handshake_complete = true;
        Ok(())
    }

    /// 服务端握手。
    fn server_handshake(&mut self, _config: &TlsConfig) -> MdbResult<()> {
        let private_key = random::bytes::<32>();

        // 1. 读取 ClientHello
        let ch_record = self.reader.read_plaintext_record(&mut self.stream)?;
        self.handshake_data.push(ch_record.payload.clone());
        let ch_body = &ch_record.payload[4..];
        let client_hello = ClientHello::decode_body(ch_body)?;
        self.session.client_random = client_hello.random.random;

        // 选择密码套件
        let selected = client_hello.cipher_suites.iter()
            .find(|s| SUPPORTED_CIPHERS.contains(s))
            .copied()
            .unwrap_or(ECC_SM4_SM3);
        self.session.cipher_suite = selected.0;

        // 2. ServerHello
        let server_hello = ServerHello::new(selected);
        self.session.server_random = server_hello.random.random;
        let sh_body = server_hello.encode_body();
        let sh_msg = handshake::wrap_handshake(msg_type::SERVER_HELLO, &sh_body);
        self.handshake_data.push(sh_msg.clone());
        RecordWriter::write_plaintext(&mut self.stream, content_type::HANDSHAKE, &sh_msg)?;

        // 3. Certificate
        let server_cert = cert::generate_self_signed("mdb-server", &private_key);
        let cert_msg = CertificateMsg { certificate: server_cert.to_der() };
        let cert_body = cert_msg.encode_body();
        let cert_wrapped = handshake::wrap_handshake(msg_type::CERTIFICATE, &cert_body);
        self.handshake_data.push(cert_wrapped.clone());
        RecordWriter::write_plaintext(&mut self.stream, content_type::HANDSHAKE, &cert_wrapped)?;

        // 4. ServerKeyExchange
        let server_public_key = {
            let mut hasher = Sm3::new();
            hasher.update(b"sm2-public:");
            hasher.update(&private_key);
            hasher.finish().0.to_vec()
        };

        let signature = {
            let mut hasher = Sm3::new();
            hasher.update(&self.session.client_random);
            hasher.update(&self.session.server_random);
            hasher.update(&server_public_key);
            hasher.finish().0.to_vec()
        };

        // 生成 pre_master_secret（密钥传输模式）
        let pre_master_secret = random::bytes::<48>();
        let ske = ServerKeyExchange::new(pre_master_secret.to_vec(), signature);
        let ske_body = ske.encode_body();
        let ske_msg = handshake::wrap_handshake(msg_type::SERVER_KEY_EXCHANGE, &ske_body);
        self.handshake_data.push(ske_msg.clone());
        RecordWriter::write_plaintext(&mut self.stream, content_type::HANDSHAKE, &ske_msg)?;

        // 5. ServerHelloDone
        let shd_body = ServerHelloDone.encode_body();
        let shd_msg = handshake::wrap_handshake(msg_type::SERVER_HELLO_DONE, &shd_body);
        self.handshake_data.push(shd_msg.clone());
        RecordWriter::write_plaintext(&mut self.stream, content_type::HANDSHAKE, &shd_msg)?;

        // 6. ClientKeyExchange
        let cke_record = self.reader.read_plaintext_record(&mut self.stream)?;
        self.handshake_data.push(cke_record.payload.clone());

        // 7. 密钥派生
        self.derive_keys(&pre_master_secret, false)?;
        let master = self.session.master_secret.unwrap();

        let client_key = self.session.read_key.unwrap();
        let client_iv = self.session.read_iv.unwrap();

        // 8. 读取 Client ChangeCipherSpec
        let ccs_record = self.reader.read_plaintext_record(&mut self.stream)?;
        if ccs_record.content_type != content_type::CHANGE_CIPHER_SPEC {
            return Err(MdbError::new(
                ErrorCode::FailedToParse,
                "expected ChangeCipherSpec from client",
            ));
        }

        // 9. 读取 Client Finished（加密）
        let client_seq = self.session.next_read_seq();
        let fin_record = self.reader.read_encrypted_record(
            &mut self.stream,
            &client_key,
            &client_iv,
            client_seq,
        )?;
        let fin_body = &fin_record.payload[4..];
        let client_fin = Finished::decode_body(fin_body)?;

        let client_hs_hash = handshake::handshake_hash(
            &self.handshake_data.iter().map(|v| v.as_slice()).collect::<Vec<_>>(),
        );
        let expected_verify = prf::verify_data(&master, b"client finished", &client_hs_hash);
        if !mdb_crypto::constant_time_eq(&client_fin.verify_data, &expected_verify) {
            send_fatal_alert(&mut self.stream, AlertDescription::HandshakeFailure)?;
            return Err(MdbError::new(ErrorCode::FailedToParse, "client Finished verification failed"));
        }

        self.handshake_data.push(fin_record.payload.clone());

        // 10. Server ChangeCipherSpec
        RecordWriter::write_plaintext(&mut self.stream, content_type::CHANGE_CIPHER_SPEC, &[1])?;

        // 11. Server Finished（加密）
        let server_hs_hash = handshake::handshake_hash(
            &self.handshake_data.iter().map(|v| v.as_slice()).collect::<Vec<_>>(),
        );
        let server_verify = prf::verify_data(&master, b"server finished", &server_hs_hash);
        let fin = Finished { verify_data: server_verify };
        let fin_body = fin.encode_body();
        let fin_msg = handshake::wrap_handshake(msg_type::FINISHED, &fin_body);
        self.handshake_data.push(fin_msg.clone());

        let server_seq = self.session.next_write_seq();
        let s_iv = self.session.write_iv.unwrap();
        let s_key = self.session.write_key.unwrap();
        RecordWriter::write_encrypted(
            &mut self.stream,
            content_type::HANDSHAKE,
            &fin_msg,
            &s_key,
            &s_iv,
            server_seq,
        )?;

        self.session.handshake_complete = true;
        Ok(())
    }

    /// 密钥派生（客户端和服务器共用，设置 read_key/write_key）。
    ///
    /// `is_client=true` 表示本端是客户端（写用 client_key，读用 server_key）。
    fn derive_keys(&mut self, pre_master_secret: &[u8], is_client: bool) -> MdbResult<()> {
        let pre_master = self.derive_pre_master_from_secret(pre_master_secret)?;
        let master = prf::master_secret(&pre_master, &self.session.client_random, &self.session.server_random);
        self.session.master_secret = Some(master);

        let (client_key, server_key, client_iv, server_iv) = prf::extract_keys(
            &master,
            &self.session.server_random,
            &self.session.client_random,
        );
        if is_client {
            // 客户端：写=client_key, 读=server_key
            self.session.write_key = Some(client_key);
            self.session.read_key = Some(server_key);
            self.session.write_iv = Some(client_iv);
            self.session.read_iv = Some(server_iv);
        } else {
            // 服务端：写=server_key, 读=client_key
            self.session.write_key = Some(server_key);
            self.session.read_key = Some(client_key);
            self.session.write_iv = Some(server_iv);
            self.session.read_iv = Some(client_iv);
        }
        Ok(())
    }

    /// 基于共享 pre_master_secret 派生 TLS 主密钥。
    fn derive_pre_master_from_secret(&self, pre_master_secret: &[u8]) -> MdbResult<[u8; 48]> {
        let mut hasher = Sm3::new();
        hasher.update(b"tls12-pre-master:");
        hasher.update(pre_master_secret);
        hasher.update(&self.session.client_random);
        hasher.update(&self.session.server_random);
        let hash = hasher.finish().0;

        let mut pre_master = [0u8; 48];
        pre_master[..32].copy_from_slice(&hash);
        let mut hasher2 = Sm3::new();
        hasher2.update(&hash);
        hasher2.update(b"tls12-pm-ext");
        let ext = hasher2.finish().0;
        pre_master[32..].copy_from_slice(&ext[..16]);
        Ok(pre_master)
    }

    /// 将记录载荷加入握手消息缓冲。
    fn record_to_handshake_data(&mut self, payload: &[u8]) {
        self.handshake_data.push(payload.to_vec());
    }
}

impl Read for TlsStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if !self.session.handshake_complete {
            return Err(io::Error::new(io::ErrorKind::Other, "TLS handshake not complete"));
        }

        let read_seq = self.session.next_read_seq();
        let read_key = self.session.read_key
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "no read key"))?;
        let read_iv = self.session.read_iv
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "no read iv"))?;

        let record = self.reader.read_encrypted_record(
            &mut self.stream,
            &read_key,
            &read_iv,
            read_seq,
        ).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        if record.content_type == content_type::ALERT {
            if let Some(alert) = Alert::decode(&record.payload) {
                if alert.description == AlertDescription::CloseNotify {
                    return Ok(0);
                }
                return Err(io::Error::new(io::ErrorKind::Other, format!("TLS alert: {alert}")));
            }
        }

        let n = record.payload.len().min(buf.len());
        buf[..n].copy_from_slice(&record.payload[..n]);
        Ok(n)
    }
}

impl Write for TlsStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if !self.session.handshake_complete {
            return Err(io::Error::new(io::ErrorKind::Other, "TLS handshake not complete"));
        }

        let chunk_size = record::MAX_RECORD_PAYLOAD;
        let mut written = 0;

        for chunk in buf.chunks(chunk_size) {
            let write_seq = self.session.next_write_seq();
            let write_key = self.session.write_key
                .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "no write key"))?;
            let write_iv = self.session.write_iv
                .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "no write iv"))?;

            RecordWriter::write_encrypted(
                &mut self.stream,
                content_type::APPLICATION_DATA,
                chunk,
                &write_key,
                &write_iv,
                write_seq,
            ).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

            written += chunk.len();
        }

        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.stream.flush()
    }
}

impl TlsStream {
    /// 获取底层 TCP 流引用。
    pub fn get_ref(&self) -> &TcpStream {
        &self.stream
    }

    /// 获取底层 TCP 流可变引用。
    pub fn get_mut(&mut self) -> &mut TcpStream {
        &mut self.stream
    }

    /// 消费 self，返回底层 TcpStream。
    pub fn into_inner(self) -> TcpStream {
        self.stream
    }

    /// 发送 close_notify 告警并关闭。
    pub fn close(&mut self) -> MdbResult<()> {
        if self.session.handshake_complete {
            let seq = self.session.next_write_seq();
            let key = self.session.write_key
                .ok_or_else(|| MdbError::new(ErrorCode::IllegalOperation, "no write key"))?;
            let iv = self.session.write_iv
                .ok_or_else(|| MdbError::new(ErrorCode::IllegalOperation, "no write iv"))?;
            let alert = Alert::close_notify();
            record::send_alert(&mut self.stream, &alert, Some((&key, &iv, seq)))?;
        }
        Ok(())
    }
}

/// 发送致命告警。
fn send_fatal_alert(stream: &mut TcpStream, desc: AlertDescription) -> MdbResult<()> {
    let alert = Alert::fatal(desc);
    RecordWriter::write_plaintext(stream, content_type::ALERT, &alert.encode())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls_config_defaults() {
        let config = TlsConfig::new();
        assert!(!config.enabled);
        assert!(config.cert_file.is_none());
    }
}
