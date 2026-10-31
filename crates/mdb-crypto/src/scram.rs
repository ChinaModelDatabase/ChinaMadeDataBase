//! SCRAM（RFC 5802 / RFC 7677）认证。
//!
//! # MongoDB 兼容性要点
//!
//! MDB 必须同时支持两个机制，因为不同版本的官方驱动默认协商的机制不同：
//!
//! | 机制 | 默认协商的驱动 | MDB 状态 |
//! |------|----------------|----------|
//! | `SCRAM-SHA-256` | MongoDB 4.0+ 的全部官方驱动 | **默认开启** |
//! | `SCRAM-SHA-1` | Java 驱动 3.x~4.x、Spring Data MongoDB 2.x | 迁移期开启 |
//!
//! Java 驱动（信创客户最常见的栈）在未显式配置时按 `SCRAM-SHA-256` →
//! `SCRAM-SHA-1` 顺序降级，因此两个机制都必须实现。
//!
//! # SCRAM 流程（服务端视角）
//!
//! ```text
//! 1. C -> S: n,,n=<user>,r=<clientNonce>                        (saslStart)
//! 2. S -> C: r=<clientNonce+serverNonce>,s=<b64 salt>,i=<iters> (saslStart reply)
//! 3. C -> S: c=<b64 gs2Header>,r=<nonce>,p=<b64 clientProof>    (saslContinue)
//! 4. S -> C: v=<b64 serverSignature>                            (saslContinue reply)
//! 5. C -> S: (empty)                                            (saslContinue, done)
//! ```
//!
//! # 安全设计
//!
//! - 服务端**不存储**明文口令，只持久化 `storedKey`/`serverKey`；
//!   proof 通过 `ClientKey = Proof XOR HMAC(StoredKey, AuthMessage)` 还原后
//!   校验 `H(ClientKey) == StoredKey`，全程无需 `SaltedPassword`
//! - 服务端签名 `v=` 证明服务端持有 `serverKey`，防中间人伪造服务端
//! - 客户端签名 `p=` 证明客户端持有口令，防服务端离线爆破
//! - 两次 nonce 拼接防重放
//! - 客户端签名校验必须**常量时间**，否则存在时序侧信道
//! - 用户不存在与口令错误返回**相同**错误形状，防用户枚举

use mdb_common::{ErrorCode, MdbError, MdbResult};

use crate::base64;
use crate::digest::Digest;
use crate::hmac::{HmacSha1, HmacSha256};
use crate::pbkdf2::pbkdf2_hmac;
use crate::random;
use crate::sha1::Sha1;
use crate::sha256::Sha256;

/// SCRAM 机制。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScramMechanism {
    /// SCRAM-SHA-1（RFC 5802）。仅迁移期兼容。
    Sha1,
    /// SCRAM-SHA-256（RFC 7677）。默认。
    Sha256,
}

impl ScramMechanism {
    /// wire 协议中的机制名。
    pub const fn as_str(self) -> &'static str {
        match self {
            ScramMechanism::Sha1 => "SCRAM-SHA-1",
            ScramMechanism::Sha256 => "SCRAM-SHA-256",
        }
    }

    /// 解析 wire 协议传入的机制名（不区分大小写，兼容更严格的驱动实现）。
    pub fn parse(s: &str) -> Option<Self> {
        if s.eq_ignore_ascii_case("SCRAM-SHA-1") {
            Some(ScramMechanism::Sha1)
        } else if s.eq_ignore_ascii_case("SCRAM-SHA-256") {
            Some(ScramMechanism::Sha256)
        } else {
            None
        }
    }

    /// MongoDB 错误消息中使用的摘要名。
    pub const fn digest_name(self) -> &'static str {
        match self {
            ScramMechanism::Sha1 => "SHA1",
            ScramMechanism::Sha256 => "SHA256",
        }
    }

    /// 派生密钥长度（bytes）。
    ///
    /// 注意：**与摘要长度无关**，RFC 7677 规定 `dkLen=32`，
    /// 即 SCRAM-SHA-1 的输出也是 32 字节。
    /// dkLen：所选杂凑算法的输出长度（RFC 5802 §3）。
    ///
    /// SCRAM-SHA-1 -> 20 字节，SCRAM-SHA-256 -> 32 字节。此值同时决定
    /// `storedKey`/`serverKey` 的长度，必须与 `pbkdf2_for` 的输出一致。
    const fn dk_len(self) -> usize {
        match self {
            ScramMechanism::Sha1 => 20,
            ScramMechanism::Sha256 => 32,
        }
    }

    /// 最小迭代次数。MongoDB 显式拒绝低于此值的配置。
    const fn min_iterations(self) -> u32 {
        4096
    }

    /// 默认迭代次数，与 MongoDB 一致。
    pub const DEFAULT_ITERATIONS: u32 = 15_000;
}

fn pbkdf2_for(mechanism: ScramMechanism, password: &[u8], salt: &[u8], iters: u32, out: &mut [u8]) {
    match mechanism {
        ScramMechanism::Sha1 => pbkdf2_hmac::<Sha1, 20, 64>(password, salt, iters, out),
        ScramMechanism::Sha256 => pbkdf2_hmac::<Sha256, 32, 64>(password, salt, iters, out),
    }
}

fn hmac_for(mechanism: ScramMechanism, key: &[u8], data: &[u8]) -> Vec<u8> {
    match mechanism {
        ScramMechanism::Sha1 => HmacSha1::mac(key, data).to_vec(),
        ScramMechanism::Sha256 => HmacSha256::mac(key, data).to_vec(),
    }
}

fn hash_for(mechanism: ScramMechanism, data: &[u8]) -> Vec<u8> {
    match mechanism {
        ScramMechanism::Sha1 => Sha1::digest(data).as_bytes().to_vec(),
        ScramMechanism::Sha256 => Sha256::digest(data).as_bytes().to_vec(),
    }
}

/// 账号存储的 SCRAM 凭证。
///
/// 对齐 MongoDB `admin.system.users` 的 `credentials.SCRAM-SHA-256` 子文档：
///
/// ```text
/// { iterationCount: <int>, salt: <b64>, storedKey: <b64>, serverKey: <b64> }
/// ```
///
/// 关系：
///
/// ```text
/// SaltedPassword = PBKDF2(H(SHA-xxx), password, salt, iterationCount, 32)
/// ClientKey      = HMAC(SaltedPassword, "Client Key")
/// StoredKey      = H(ClientKey)
/// ServerKey      = HMAC(SaltedPassword, "Server Key")
/// ```
///
/// `StoredKey`/`ServerKey` **不是**口令哈希的密码学承诺——拿到它们仍可
/// 直接冒充客户端提交 proof。因此 `iterationCount` 必须足够大，
/// 且 `local.system.users` 需 TDE 保护。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredKey {
    pub mechanism: ScramMechanism,
    pub iteration_count: u32,
    /// 16 字节随机 salt。
    pub salt: Vec<u8>,
    pub stored_key: Vec<u8>,
    pub server_key: Vec<u8>,
}

impl StoredKey {
    /// 从明文口令创建凭证。仅在 `createUser` / `updateUser` 调用一次。
    pub fn from_password(
        mechanism: ScramMechanism,
        password: &str,
        iterations: u32,
    ) -> MdbResult<Self> {
        Self::validate_iterations(mechanism, iterations)?;
        // salt 不属于机密材料：需求是"不可预测"以阻止预计算攻击，而非
        // 密码学密钥强度。std 播种的 DRBG 在主流平台上由 OS CSPRNG 供种，
        // 16 字节 salt 足够，故走 `fill` 而非 `fill_key_material`。
        let salt = random::bytes::<16>().to_vec();
        let mut salted = vec![0u8; mechanism.dk_len()];
        pbkdf2_for(mechanism, password.as_bytes(), &salt, iterations, &mut salted);
        Ok(Self::from_salted_password(mechanism, &salted, salt, iterations))
    }

    /// 从已持久化的 SCRAM 材料重建 `StoredKey`。
    ///
    /// 用于启动恢复：把元数据里逐字段保存的 salt/storedKey/serverKey
    /// 还原成可参与认证的对象。`mechanism` 未知或迭代次数非法时返回
    /// `None`，调用方应跳过该用户而不是构造一个残缺凭据。
    pub fn from_parts(
        mechanism: &str,
        iteration_count: u32,
        salt: Vec<u8>,
        stored_key: Vec<u8>,
        server_key: Vec<u8>,
    ) -> Option<Self> {
        let mechanism = ScramMechanism::parse(mechanism)?;
        Self::validate_iterations(mechanism, iteration_count).ok()?;
        let dk = mechanism.dk_len();
        if salt.is_empty() || stored_key.len() != dk || server_key.len() != dk {
            return None;
        }
        Some(Self { mechanism, iteration_count, salt, stored_key, server_key })
    }

    /// 由已派生的 SaltedPassword 构造 `storedKey` / `serverKey`。
    pub fn from_salted_password(
        mechanism: ScramMechanism,
        salted_password: &[u8],
        salt: Vec<u8>,
        iteration_count: u32,
    ) -> Self {
        let client_key = hmac_for(mechanism, salted_password, b"Client Key");
        let stored_key = hash_for(mechanism, &client_key);
        let server_key = hmac_for(mechanism, salted_password, b"Server Key");
        Self { mechanism, iteration_count, salt, stored_key, server_key }
    }

    /// 从 MongoDB `system.users` 文档的 credential 字段反序列化（迁移路径）。
    ///
    /// 入参格式：`{ iterationCount: int, salt: b64, storedKey: b64, serverKey: b64 }`。
    pub fn from_mongo_credentials(
        mechanism: ScramMechanism,
        iteration_count: u32,
        salt_b64: &str,
        stored_key_b64: &str,
        server_key_b64: &str,
    ) -> MdbResult<Self> {
        Self::validate_iterations(mechanism, iteration_count)?;
        let salt = base64::decode(salt_b64)
            .map_err(|_| auth_err("SCRAM: credential.salt is not valid base64"))?;
        let stored_key = base64::decode(stored_key_b64)
            .map_err(|_| auth_err("SCRAM: credential.storedKey is not valid base64"))?;
        let server_key = base64::decode(server_key_b64)
            .map_err(|_| auth_err("SCRAM: credential.serverKey is not valid base64"))?;
        if salt.len() < 8 {
            return Err(auth_err("SCRAM: credential.salt too short (min 8 bytes)"));
        }
        Ok(Self { mechanism, iteration_count, salt, stored_key, server_key })
    }

    fn validate_iterations(mechanism: ScramMechanism, iterations: u32) -> MdbResult<()> {
        if iterations < mechanism.min_iterations() {
            return Err(MdbError::new(
                ErrorCode::FailedToParse,
                format!(
                    "iteration count must be at least {} (got {iterations})",
                    mechanism.min_iterations()
                ),
            ));
        }
        Ok(())
    }

    /// 由明文口令重算 `saltedPassword`（仅用于验证 client proof）。
    pub fn salted_password(&self, password: &str) -> Vec<u8> {
        let mut out = vec![0u8; self.mechanism.dk_len()];
        pbkdf2_for(
            self.mechanism,
            password.as_bytes(),
            &self.salt,
            self.iteration_count,
            &mut out,
        );
        out
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// 等待 saslContinue。
    AwaitingClientProof,
    /// 客户端证明已验证，等待最终 saslContinue。
    AwaitingFinal,
    /// 完成。
    Done,
    /// 失败。
    Failed,
}

/// SCRAM 服务端会话状态机。
#[derive(Debug)]
pub struct ServerScram {
    mechanism: ScramMechanism,
    state: State,
    client_nonce: String,
    server_nonce: String,
    /// GS2 channel-binding 头：`n,,` 或 `y,,`。
    gs2_header: String,
    username: String,
    credential: Option<StoredKey>,
    /// client-first-message 的 bare 部分（不含 gs2 头与扩展）。
    ///
    /// 参与 `AuthMessage` 计算，必须与客户端逐字节一致。
    client_first_bare: String,
    /// wire 协议的 `conversationId`。
    pub conversation_id: u32,
}

impl ServerScram {
    /// 创建会话。
    pub fn new(mechanism: ScramMechanism, conversation_id: u32) -> Self {
        Self {
            mechanism,
            state: State::AwaitingClientProof,
            client_nonce: String::new(),
            server_nonce: String::new(),
            gs2_header: String::new(),
            username: String::new(),
            credential: None,
            client_first_bare: String::new(),
            conversation_id,
        }
    }

    pub const fn mechanism(&self) -> ScramMechanism {
        self.mechanism
    }

    pub const fn is_done(&self) -> bool {
        matches!(self.state, State::Done)
    }


    /// 处理 `saslStart` 的 client-first-message，返回 server-first-message。
    ///
    /// `resolve` 由调用方实现（查 `local.system.users`），MDB 强制使用
    /// **常量时间**的查询路径，避免响应时延泄漏账号是否存在。
    pub fn on_client_first(
        &mut self,
        client_first: &[u8],
        resolve: &mut dyn FnMut(&str) -> Option<StoredKey>,
    ) -> MdbResult<String> {
        if self.state != State::AwaitingClientProof {
            return Err(auth_err("SCRAM: unexpected saslStart"));
        }
        let text = std::str::from_utf8(client_first)
            .map_err(|_| auth_err("SCRAM: client-first-message is not valid UTF-8"))?;

        // gs2-cbind-flag "," [authzid] "," nonce ["," extensions]
        let mut parts = text.splitn(3, ',');
        let cbind = parts
            .next()
            .ok_or_else(|| auth_err("SCRAM: malformed client-first-message"))?;
        let authzid = parts
            .next()
            .ok_or_else(|| auth_err("SCRAM: malformed client-first-message"))?;
        let rest = parts
            .next()
            .ok_or_else(|| auth_err("SCRAM: malformed client-first-message"))?;

        if cbind.starts_with("p=") {
            // GSSAPI 通道绑定。MongoDB 不支持，直接拒绝。
            return Err(auth_err("SCRAM: channel binding is not supported"));
        }
        if cbind != "n" && cbind != "y" {
            return Err(auth_err("SCRAM: invalid gs2-cbind-flag"));
        }

        // RFC 5802 §3.1：gs2-header 之后即 `client-first-message-bare`，
        // 形如 `n=user,r=nonce`。nonce 之后的字段为 reserved/extensions，
        // MongoDB 服务端一律拒绝。
        //
        // 注意不能把 bare 按逗号切开再把第二段当扩展——`n=user,r=nonce`
        // 本身就以逗号分隔，`r=` 段是 nonce 而非扩展。原实现正是因此
        // 把合法的 `n,,n=user,r=abc` 误判为 "unsupported GS2 extension"。
        let nonce_start = rest
            .find(",r=")
            .map_or_else(|| (!rest.starts_with("r=")).then_some(0), Some);
        let nonce_start =
            nonce_start.ok_or_else(|| auth_err("SCRAM: client-first-message missing nonce"))?;
        let (user_field, nonce_and_rest) = rest.split_at(nonce_start);
        // nonce_and_rest 以 ",r=" 或 "r=" 开头
          let nonce: &str = nonce_and_rest
              .trim_start_matches(',')
              .trim_start_matches("r=");
        if nonce.is_empty() {
            return Err(auth_err("SCRAM: empty client nonce"));
        }
        if nonce.contains(',') {
            // nonce 之后仍有字段 -> 客户端发送了不支持的扩展
            return Err(auth_err("SCRAM: unsupported GS2 extension"));
        }

        // `n=` 前缀按 RFC 5802 可选；缺省时 bare 首字段直接是用户名，
        // 且 authzid 字段承载用户名。
        let raw_user: &str = match user_field.strip_prefix("n=") {
            Some(u) => u,
            None => {
                if authzid.is_empty() {
                    return Err(auth_err("SCRAM: client-first-message missing username"));
                }
                authzid
            }
        };
        let user = saslprep(raw_user);
        if user.is_empty() || user.contains('\0') {
            return Err(auth_err("SCRAM: invalid username"));
        }

        let credential =
            resolve(&user).ok_or_else(|| auth_err_user(&user, self.mechanism))?;
        if credential.mechanism != self.mechanism {
            return Err(auth_err("SCRAM: mechanism mismatch"));
        }
        StoredKey::validate_iterations(self.mechanism, credential.iteration_count)?;

        self.client_nonce = nonce.to_owned();
        self.server_nonce = random_nonce();
        self.gs2_header = if cbind == "n" { "n,,".to_owned() } else { "y,,".to_owned() };
        self.username = user;
        self.credential = Some(credential);
        // AuthMessage 中的 bare 段必须与客户端发送的**原始**文本逐字节一致，
        // 不能使用 saslprep 后的用户名（规范化可能改变字节），也不能只取
        // `n=` 字段——`client-first-message-bare` 包含 nonce，即完整的
        // `n=user,r=nonce`（RFC 5802 §3.1）。本处 extensions 已被拒绝，
        // 因此 bare 就是 gs2-header 之后的全部内容。
        self.client_first_bare = rest.to_owned();

        Ok(format!(
            "r={},s={},i={}",
            self.combined_nonce(),
            base64::encode(&self.credential.as_ref().expect("set above").salt),
            self.credential.as_ref().expect("set above").iteration_count
        ))
    }

    fn combined_nonce(&self) -> String {
        format!("{}{}", self.client_nonce, self.server_nonce)
    }

    /// 处理 `saslContinue` 的 client-final-message，返回 server-final-message。
    pub fn on_client_final(&mut self, client_final: &[u8]) -> MdbResult<String> {
        if self.state != State::AwaitingClientProof {
            return Err(auth_err("SCRAM: unexpected saslContinue"));
        }
        let text = std::str::from_utf8(client_final)
            .map_err(|_| auth_err("SCRAM: client-final-message is not valid UTF-8"))?;
        let credential = self
            .credential
            .clone()
            .ok_or_else(|| auth_err("SCRAM: no credential in session"))?;

        let mut nonce_ok = false;
        let mut proof_b64: Option<&str> = None;
        let mut cbind_b64: Option<&str> = None;
        for part in text.split(',') {
            if let Some(v) = part.strip_prefix("r=") {
                nonce_ok = crate::constant_time_eq(v.as_bytes(), self.combined_nonce().as_bytes());
            } else if let Some(v) = part.strip_prefix("p=") {
                proof_b64 = Some(v);
            } else if let Some(v) = part.strip_prefix("c=") {
                cbind_b64 = Some(v);
            } else if part.starts_with("v=") {
                // 客户端在收到 server-final 前不应发 v=，视为协议违规
                return Err(auth_err("SCRAM: unexpected 'v=' in client-final-message"));
            }
        }

        if !nonce_ok {
            return Err(auth_err("SCRAM: nonce mismatch (possible replay)"));
        }
        let proof_b64 =
            proof_b64.ok_or_else(|| auth_err("SCRAM: client-final-message missing 'p='"))?;
        if let Some(c) = cbind_b64 {
            let decoded = base64::decode(c).map_err(|_| auth_err("SCRAM: bad c= value"))?;
            if String::from_utf8_lossy(&decoded) != self.gs2_header {
                return Err(auth_err("SCRAM: channel binding mismatch"));
            }
        }

        let client_proof = base64::decode(proof_b64)
            .map_err(|_| auth_err("SCRAM: bad 'p=' encoding"))?;
        if client_proof.len() != credential.stored_key.len() {
            self.state = State::Failed;
            return Err(auth_err_user(&self.username, self.mechanism));
        }

        // AuthMessage = client-first-bare "," server-first "," client-final-without-proof
        let server_first = format!(
            "r={},s={},i={}",
            self.combined_nonce(),
            base64::encode(&credential.salt),
            credential.iteration_count
        );
        let cbind_b64 = base64::encode(self.gs2_header.as_bytes());
        let auth_message = format!(
            "{bare_first},{server_first},c={cbind_b64},r={nonce_combo}",
            bare_first = self.client_first_bare,
            nonce_combo = self.combined_nonce()
        );

        // # 无需明文口令的验证路径
        //
        // 朴素实现会用 `ClientKey = HMAC(SaltedPassword, "Client Key")` 重算
        // expected proof，这需要 `SaltedPassword`，因而需要明文口令。但 RFC 5802
        // 允许服务端只持久化 `StoredKey = H(ClientKey)`：
        //
        // ```text
        // ClientSignature = HMAC(StoredKey, AuthMessage)
        // ClientKey       = ClientProof XOR ClientSignature
        // ```
        //
        // 随后校验 `H(ClientKey) == StoredKey`。整个过程无需 `SaltedPassword`，
        // 因此**服务端不需要保存明文口令**。MongoDB 亦如此：仅存
        // `iterationCount`/`salt`/`storedKey`/`serverKey`（`cacheCredentialPassword`
        // 控制的额外 `password` 字段是可选加速项，MDB 不实现）。
        let client_signature =
            hmac_for(self.mechanism, &credential.stored_key, auth_message.as_bytes());
        let mut candidate_key = client_proof;
        for (c, s) in candidate_key.iter_mut().zip(client_signature.iter()) {
            *c ^= s;
        }
        // H(ClientKey) 应等于持久化的 storedKey
        let candidate_stored_key = hash_for(self.mechanism, &candidate_key);

        if !crate::constant_time_eq_padded(&candidate_stored_key, &credential.stored_key) {
            // 失败路径同样执行一次 HMAC，消除时延差异
            let _ = hmac_for(self.mechanism, &credential.server_key, auth_message.as_bytes());
            self.state = State::Failed;
            return Err(auth_err_user(&self.username, self.mechanism));
        }

        let server_sig = hmac_for(self.mechanism, &credential.server_key, auth_message.as_bytes());
        self.state = State::AwaitingFinal;
        Ok(format!("v={}", base64::encode(&server_sig)))
    }

    /// 处理最后一次 `saslContinue`（payload 为空）。
    pub fn on_done(&mut self) -> MdbResult<()> {
        match self.state {
            State::AwaitingFinal | State::Done => {
                self.state = State::Done;
                Ok(())
            }
            _ => Err(auth_err("SCRAM: authentication not complete")),
        }
    }

    /// 认证成功的账号名。
    pub fn authenticated_user(&self) -> Option<&str> {
        match self.state {
            State::AwaitingFinal | State::Done => self.credential.as_ref().map(|_| self.username.as_str()),
            _ => None,
        }
    }
}

fn auth_err(msg: &'static str) -> MdbError {
    MdbError::new(ErrorCode::UserNotFound, msg)
}

/// 用户不存在/口令错误统一错误形状（防用户枚举）。
fn auth_err_user(user: &str, mechanism: ScramMechanism) -> MdbError {
    MdbError::new(
        ErrorCode::UserNotFound,
        format!(
            "Authentication failed. SCRAM-{}: user not found: \"{user}\"",
            mechanism.digest_name()
        ),
    )
}

/// 服务端 nonce：24 字节随机数的 Base64（与 MongoDB 一致）。
///
/// nonce 绑定客户端 nonce 并抵御重放与会话混淆，需求是**不可预测**而非
/// 密钥强度，因此使用 `fill`（含 std 播种的降级 DRBG）而非
/// `fill_key_material`。真正的强度边界在密钥材料一侧强制。
fn random_nonce() -> String {
    base64::encode(&random::bytes::<24>())
}

/// SASLprep（RFC 4013）最小实现。
///
/// # 已知限制
///
/// 完整 SASLprep 需要 Unicode NFKC 规范化表（数 MB），需要引入 Unicode
/// 数据依赖。本实现：
///
/// - **不规范化**。MongoDB 服务端同样不做 SASLprep——由官方驱动的
///   `saslPrep` 在客户端完成（`mongodb` Node 库的 `saslPrepareUsername`）。
/// - **trim ASCII 空白**：SASLprep 要求把非 ASCII space 映射为 SPACE。
/// - **拒绝控制字符/NUL**：防止用户名中的控制字符被用于日志注入。
/// - 若未来需支持非 ASCII 用户名等价性，引入 NFKC 表的改动仅限本函数，
///   不触及协议层。
fn saslprep(s: &str) -> String {
    s.trim_matches(|c: char| c == ' ' || c == '\t' || c == '\n' || c == '\r').to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 7677 §3 交互向量（user=pencil）。
    fn rfc7677_credential() -> StoredKey {
        let salt = base64::decode("W22ZaJ0SNY7soEsUEjb6gQ==").unwrap();
        assert_eq!(salt.len(), 16);
        let mut salted = vec![0u8; 32];
        pbkdf2_for(ScramMechanism::Sha256, b"pencil", &salt, 4096, &mut salted);
        StoredKey::from_salted_password(ScramMechanism::Sha256, &salted, salt, 4096)
    }

    /// 用给定口令走完整流程，返回是否成功。
    fn attempt(user: &str, password: &str, client_nonce: &str) -> Result<String, MdbError> {
        let cred = rfc7677_credential();
        let mut s = ServerScram::new(ScramMechanism::Sha256, 1);
        let sf = s
            .on_client_first(
                format!("n,,n={user},r={client_nonce}").as_bytes(),
                &mut |_| Some(cred.clone()),
            )?;
        let nonce = sf
            .split(',')
            .find_map(|p| p.strip_prefix("r=").map(str::to_owned))
            .unwrap();

        // 客户端侧独立计算 proof
        let salted = cred.salted_password(password);
        let client_key = hmac_for(ScramMechanism::Sha256, &salted, b"Client Key");
        let stored_key = hash_for(ScramMechanism::Sha256, &client_key);
        let auth_msg = format!("n={user},r={client_nonce},{sf},c=biws,r={nonce}");
        let client_sig = hmac_for(ScramMechanism::Sha256, &stored_key, auth_msg.as_bytes());
        let mut proof = client_key;
        for (e, sg) in proof.iter_mut().zip(client_sig.iter()) {
            *e ^= sg;
        }
        let out = s.on_client_final(format!("c=biws,r={nonce},p={}", base64::encode(&proof)).as_bytes())?;
        s.on_done()?;
        Ok(out)
    }

    #[test]
    fn rfc7677_salt_and_iteration_in_server_first() {
        let cred = rfc7677_credential();
        let mut s = ServerScram::new(ScramMechanism::Sha256, 1);
        let sf = s
            .on_client_first(b"n,,n=user,r=rOprNGfwEbeRWgbNEkqO", &mut |u| {
                assert_eq!(u, "user");
                Some(cred.clone())
            })
            .unwrap();
        assert!(sf.contains("s=W22ZaJ0SNY7soEsUEjb6gQ=="), "{sf}");
        assert!(sf.contains("i=4096"), "{sf}");
        assert!(sf.starts_with("r=rOprNGfwEbeRWgbNEkqO"), "{sf}");
    }

    #[test]
    fn full_exchange_succeeds() {
        let out = attempt("user", "pencil", "rOprNGfwEbeRWgbNEkqO").unwrap();
        assert!(out.starts_with("v="), "expected server signature, got {out}");
    }

    #[test]
    fn wrong_password_rejected() {
        let e = attempt("user", "wrong", "abcdefgh").unwrap_err();
        assert_eq!(e.code(), ErrorCode::UserNotFound);
        assert!(e.message().contains("Authentication failed"));
    }

    #[test]
    fn unknown_user_indistinguishable_from_wrong_password() {
        let mut s = ServerScram::new(ScramMechanism::Sha256, 1);
        let e = s.on_client_first(b"n,,n=ghost,r=abcdefgh", &mut |_| None).unwrap_err();
        assert_eq!(e.code(), ErrorCode::UserNotFound);
        assert!(e.message().starts_with("Authentication failed."));
    }

    #[test]
    fn nonce_replay_rejected() {
        let cred = rfc7677_credential();
        let mut s = ServerScram::new(ScramMechanism::Sha256, 1);
        s.on_client_first(b"n,,n=user,r=abcdefgh", &mut |_| Some(cred.clone())).unwrap();
        let e = s.on_client_final(b"c=biws,r=DIFFERENTNONCE,p=AAAA").unwrap_err();
        assert!(e.message().contains("nonce mismatch"), "{}", e.message());
    }

    #[test]
    fn gssapi_channel_binding_rejected() {
        let cred = rfc7677_credential();
        let mut s = ServerScram::new(ScramMechanism::Sha256, 1);
        let e = s
            .on_client_first(b"p=tls-unique,,n=user,r=abc", &mut |_| Some(cred.clone()))
            .unwrap_err();
        assert!(e.message().contains("channel binding"));
    }

    #[test]
    fn missing_nonce_rejected() {
        let mut s = ServerScram::new(ScramMechanism::Sha256, 1);
        assert!(s.on_client_first(b"n,,n=user", &mut |_| None).is_err());
    }

    #[test]
    fn missing_proof_rejected() {
        let cred = rfc7677_credential();
        let mut s = ServerScram::new(ScramMechanism::Sha256, 1);
        let sf = s.on_client_first(b"n,,n=user,r=abc", &mut |_| Some(cred.clone())).unwrap();
        let nonce = sf.split(',').find_map(|p| p.strip_prefix("r=").map(str::to_owned)).unwrap();
        let e = s.on_client_final(format!("c=biws,r={nonce}").as_bytes()).unwrap_err();
        assert!(e.message().contains("missing 'p='"));
    }

    #[test]
    fn cbind_mismatch_rejected() {
        let cred = rfc7677_credential();
        let mut s = ServerScram::new(ScramMechanism::Sha256, 1);
        let sf = s.on_client_first(b"n,,n=user,r=abc", &mut |_| Some(cred.clone())).unwrap();
        let nonce = sf.split(',').find_map(|p| p.strip_prefix("r=").map(str::to_owned)).unwrap();
        // 声明 y,, 但 client-first 用了 n,,
        let wrong_c = base64::encode(b"y,,");
        let e = s
            .on_client_final(format!("c={wrong_c},r={nonce},p=AAAA").as_bytes())
            .unwrap_err();
        assert!(e.message().contains("channel binding mismatch"), "{}", e.message());
    }

    #[test]
    fn stored_key_only_verifies_without_password() {
        // 回归：服务端只持有 storedKey/serverKey，验证路径不得依赖明文口令。
        // 伪造的 proof 必须落入与错误口令完全相同的失败分支。
        let cred = rfc7677_credential();
        let mut s = ServerScram::new(ScramMechanism::Sha256, 1);
        let sf = s.on_client_first(b"n,,n=user,r=abc", &mut |_| Some(cred.clone())).unwrap();
        let nonce = sf.split(',').find_map(|p| p.strip_prefix("r=").map(str::to_owned)).unwrap();
        let e = s
            .on_client_final(format!("c=biws,r={nonce},p=AAAA").as_bytes())
            .unwrap_err();
        assert_eq!(e.code(), ErrorCode::UserNotFound);
        assert!(e.message().starts_with("Authentication failed."), "{}", e.message());
    }

    #[test]
    fn low_iteration_rejected() {
        assert!(StoredKey::from_password(ScramMechanism::Sha256, "pw", 1000).is_err());
        assert!(StoredKey::from_password(ScramMechanism::Sha256, "pw", 4096).is_ok());
    }

    #[test]
    fn mechanism_names_and_parsing() {
        assert_eq!(ScramMechanism::Sha256.as_str(), "SCRAM-SHA-256");
        assert_eq!(ScramMechanism::Sha1.as_str(), "SCRAM-SHA-1");
        assert_eq!(ScramMechanism::parse("scram-sha-256"), Some(ScramMechanism::Sha256));
        assert_eq!(ScramMechanism::parse("PLAIN"), None);
    }

    #[test]
    fn stored_key_shape() {
        // RFC 5802 的 dkLen 等于所选杂凑的输出长度：SHA-1 -> 20，SHA-256 -> 32。
        for (m, dk_len) in [(ScramMechanism::Sha1, 20), (ScramMechanism::Sha256, 32)] {
            let c = StoredKey::from_password(m, "pw", 4096).unwrap();
            assert_eq!(c.salt.len(), 16, "salt must be 16 bytes");
            assert_eq!(c.stored_key.len(), dk_len, "dkLen for {}", m.as_str());
            assert_eq!(c.server_key.len(), dk_len);
            // stored_key != server_key
            assert_ne!(c.stored_key, c.server_key);
        }
    }


    #[test]
    fn mongo_credential_roundtrip() {
        let c = StoredKey::from_password(ScramMechanism::Sha256, "pw", 4096).unwrap();
        let doc = (
            c.iteration_count,
            base64::encode(&c.salt),
            base64::encode(&c.stored_key),
            base64::encode(&c.server_key),
        );
        let back = StoredKey::from_mongo_credentials(
            ScramMechanism::Sha256,
            doc.0,
            &doc.1,
            &doc.2,
            &doc.3,
        )
        .unwrap();
        assert_eq!(back, c, "migration path must preserve credentials bit-exactly");
    }

    #[test]
    fn mongo_credential_rejects_short_salt() {
        let e = StoredKey::from_mongo_credentials(
            ScramMechanism::Sha256,
            15000,
            &base64::encode(&[1u8; 4]),
            &base64::encode(&[0u8; 32]),
            &base64::encode(&[0u8; 32]),
        );
        assert!(e.is_err());
    }
}
