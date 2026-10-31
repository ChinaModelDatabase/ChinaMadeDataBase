//! SM3-based TLS PRF（伪随机函数）。
//!
//! TLS 1.2 PRF 定义为 `P_hash(secret, label + seed)` 的扩展，
//! 其中 `P_hash` 基于 HMAC-SM3。

use mdb_crypto::hmac::HmacSm3;

/// TLS 1.2 PRF：P_hash(secret, label || seed) 扩展到 `output_len` 字节。
///
/// ```text
/// P_SM3(secret, seed) = HMAC_SM3(secret, A(1) || seed)
///                     || HMAC_SM3(secret, A(2) || seed)
///                     || ...
/// A(0) = seed
/// A(i) = HMAC_SM3(secret, A(i-1))
/// ```
pub fn prf(secret: &[u8], label: &[u8], seed: &[u8], output_len: usize) -> Vec<u8> {
    let mut result = Vec::with_capacity(output_len);

    // label + seed 作为 TLS 1.2 PRF 的输入
    let mut label_seed = Vec::with_capacity(label.len() + seed.len());
    label_seed.extend_from_slice(label);
    label_seed.extend_from_slice(seed);

    let mut a = label_seed.clone();
    while result.len() < output_len {
        // A(i) = HMAC(secret, A(i-1))
        let hmac_a = HmacSm3::mac(secret, &a);
        a = hmac_a.to_vec();

        // HMAC(secret, A(i) || seed)
        let mut hmac_input = Vec::with_capacity(32 + label_seed.len());
        hmac_input.extend_from_slice(&hmac_a);
        hmac_input.extend_from_slice(&label_seed);
        let block = HmacSm3::mac(secret, &hmac_input);

        let take = (output_len - result.len()).min(32);
        result.extend_from_slice(&block[..take]);
    }

    result.truncate(output_len);
    result
}

/// PRF 输出固定长度字节。
pub fn prf_bytes<const N: usize>(secret: &[u8], label: &[u8], seed: &[u8]) -> [u8; N] {
    let v = prf(secret, label, seed, N);
    let mut out = [0u8; N];
    out.copy_from_slice(&v);
    out
}

/// 导出主密钥（master secret）。
///
/// `master_secret = PRF(pre_master_secret, "master secret", client_random + server_random)[0..48]`
pub fn master_secret(pre_master: &[u8], client_random: &[u8], server_random: &[u8]) -> [u8; 48] {
    let mut seed = Vec::with_capacity(64);
    seed.extend_from_slice(client_random);
    seed.extend_from_slice(server_random);
    prf_bytes::<48>(pre_master, b"master secret", &seed)
}

/// 导出密钥块（key block）。
///
/// `key_block = PRF(master_secret, "key expansion", server_random + client_random)`
pub fn key_block(master: &[u8; 48], server_random: &[u8], client_random: &[u8], len: usize) -> Vec<u8> {
    let mut seed = Vec::with_capacity(64);
    seed.extend_from_slice(server_random);
    seed.extend_from_slice(client_random);
    prf(master, b"key expansion", &seed, len)
}

/// 生成 Finished 消息的 verify_data。
///
/// `verify_data = PRF(master_secret, "client finished" | "server finished", Hash(handshake_messages))[0..12]`
pub fn verify_data(master: &[u8; 48], label: &[u8], handshake_hash: &[u8]) -> [u8; 12] {
    prf_bytes::<12>(master, label, handshake_hash)
}

/// 提取 client/server 写密钥和 IV。
///
/// 返回 `(client_key, server_key, client_iv, server_iv)`，各为16/16/12/12 字节。
pub fn extract_keys(
    master: &[u8; 48],
    server_random: &[u8],
    client_random: &[u8],
) -> ([u8; 16], [u8; 16], [u8; 12], [u8; 12]) {
    // key_block = PRF(master, "key expansion", server_random + client_random)
    // 需要16+16+12+12 = 56 字节
    let block = key_block(master, server_random, client_random, 56);
    let mut client_key = [0u8; 16];
    let mut server_key = [0u8; 16];
    let mut client_iv = [0u8; 12];
    let mut server_iv = [0u8; 12];
    client_key.copy_from_slice(&block[0..16]);
    server_key.copy_from_slice(&block[16..32]);
    client_iv.copy_from_slice(&block[32..44]);
    server_iv.copy_from_slice(&block[44..56]);
    (client_key, server_key, client_iv, server_iv)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prf_deterministic() {
        let a = prf(b"secret", b"label", b"seed", 32);
        let b = prf(b"secret", b"label", b"seed", 32);
        assert_eq!(a, b);
    }

    #[test]
    fn prf_different_lengths() {
        let v16 = prf(b"k", b"l", b"s", 16);
        let v32 = prf(b"k", b"l", b"s", 32);
        let v48 = prf(b"k", b"l", b"s", 48);
        assert_eq!(v16.len(), 16);
        assert_eq!(v32.len(), 32);
        assert_eq!(v48.len(), 48);
        assert_eq!(&v32[..16], &v16[..]);
        assert_eq!(&v48[..32], &v32[..]);
    }

    #[test]
    fn master_secret_is_48_bytes() {
        let ms = master_secret(&[1u8; 48], &[2u8; 32], &[3u8; 32]);
        assert_eq!(ms.len(), 48);
    }

    #[test]
    fn extract_keys_lengths() {
        let ms = [0u8; 48];
        let (ck, sk, ci, si) = extract_keys(&ms, &[0u8; 32], &[0u8; 32]);
        assert_eq!(ck.len(), 16);
        assert_eq!(sk.len(), 16);
        assert_eq!(ci.len(), 12);
        assert_eq!(si.len(), 12);
    }
}
