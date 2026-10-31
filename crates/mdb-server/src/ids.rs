use std::collections::{HashMap, HashSet, VecDeque};
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::Instant;

use mdb_doc::{Bson, Document, ObjectId};
use mdb_wire::OpMsg;

use crate::{ok_body, command_failed, ok_reply};
use mdb_common::ErrorCode;

fn doc_get_str<'a>(doc: &'a Document, key: &str) -> Option<&'a str> {
    doc.get(key).and_then(|v| v.as_str())
}

fn doc_get_i64(doc: &Document, key: &str) -> Option<i64> {
    doc.get(key).and_then(|v| v.as_i64())
}

fn doc_get_bool(doc: &Document, key: &str) -> Option<bool> {
    doc.get(key).and_then(|v| v.as_bool())
}

#[derive(Debug, Clone)]
pub struct IdsConfig {
    pub enabled: bool,
    pub max_login_attempts: u32,
    pub lockout_duration_secs: u64,
    pub max_queries_per_second: u32,
    pub max_connections_per_ip: u32,
    pub alert_on_anomaly: bool,
}

impl Default for IdsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_login_attempts: 5,
            lockout_duration_secs: 300,
            max_queries_per_second: 1000,
            max_connections_per_ip: 100,
            alert_on_anomaly: true,
        }
    }
}

#[derive(Debug, Clone)]
pub enum DetectionRule {
    BruteForce {
        max_attempts: u32,
        window_secs: u64,
    },
    DDoS {
        max_queries_per_sec: u32,
    },
    UnauthorizedAccess {
        max_attempts: u32,
    },
    SuspiciousQuery {
        patterns: Vec<String>,
    },
    PrivilegeEscalation,
    DataExfiltration {
        max_bytes_per_hour: u64,
    },
}

#[derive(Debug, Clone)]
pub enum SecurityEvent {
    LoginFailed {
        ip: IpAddr,
        username: String,
        reason: String,
    },
    LoginSuccess {
        ip: IpAddr,
        username: String,
    },
    BruteForceDetected {
        ip: IpAddr,
        attempts: u32,
    },
    UnauthorizedAttempt {
        ip: IpAddr,
        user: String,
        command: String,
    },
    SuspiciousQuery {
        ip: IpAddr,
        query: String,
        reason: String,
    },
    RateLimitExceeded {
        ip: IpAddr,
        current_rate: u32,
        limit: u32,
    },
    BlacklistBlocked {
        ip: IpAddr,
    },
    PrivilegeEscalationAttempt {
        ip: IpAddr,
        user: String,
        target: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertSeverity {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionTaken {
    Logged,
    RateLimited,
    Blocked,
    UserLocked,
}

#[derive(Debug, Clone)]
pub struct SecurityAlert {
    pub id: ObjectId,
    pub timestamp: i64,
    pub severity: AlertSeverity,
    pub event: SecurityEvent,
    pub action_taken: ActionTaken,
}

#[derive(Debug)]
struct RateInfo {
    timestamps: VecDeque<Instant>,
    tokens: u32,
    last_refill: Instant,
}

#[derive(Debug)]
pub struct IntrusionDetector {
    rules: Vec<DetectionRule>,
    pub alerts: Mutex<Vec<SecurityAlert>>,
    pub ip_blacklist: Mutex<HashSet<IpAddr>>,
    rate_limiter: Mutex<HashMap<IpAddr, RateInfo>>,
    login_attempts: Mutex<HashMap<IpAddr, VecDeque<Instant>>>,
    locked_users: Mutex<HashMap<String, Instant>>,
    pub config: Mutex<IdsConfig>,
    stats: Mutex<IdsStats>,
}

#[derive(Debug, Clone)]
pub struct IdsStats {
    pub total_alerts: u64,
    pub total_blocked: u64,
    pub total_rate_limited: u64,
    pub total_login_failures: u64,
    pub uptime_secs: u64,
}

impl Default for IdsStats {
    fn default() -> Self {
        Self {
            total_alerts: 0,
            total_blocked: 0,
            total_rate_limited: 0,
            total_login_failures: 0,
            uptime_secs: 0,
        }
    }
}

impl IntrusionDetector {
    pub fn new(config: IdsConfig) -> Self {
        let max_qps = config.max_queries_per_second;
        let rules = vec![
            DetectionRule::BruteForce {
                max_attempts: config.max_login_attempts,
                window_secs: config.lockout_duration_secs,
            },
            DetectionRule::DDoS {
                max_queries_per_sec: max_qps,
            },
            DetectionRule::UnauthorizedAccess { max_attempts: 3 },
            DetectionRule::SuspiciousQuery {
                patterns: vec![
                    "dropDatabase".to_string(),
                    "drop".to_string(),
                    "dropUser".to_string(),
                ],
            },
            DetectionRule::PrivilegeEscalation,
            DetectionRule::DataExfiltration {
                max_bytes_per_hour: 1_073_741_824,
            },
        ];

        Self {
            rules,
            alerts: Mutex::new(Vec::new()),
            ip_blacklist: Mutex::new(HashSet::new()),
            rate_limiter: Mutex::new(HashMap::new()),
            login_attempts: Mutex::new(HashMap::new()),
            locked_users: Mutex::new(HashMap::new()),
            config: Mutex::new(config),
            stats: Mutex::new(IdsStats::default()),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.config.lock().unwrap().enabled
    }

    pub fn is_ip_blocked(&self, ip: &IpAddr) -> bool {
        self.ip_blacklist.lock().unwrap().contains(ip)
    }

    pub fn is_user_locked(&self, username: &str) -> bool {
        let locked = self.locked_users.lock().unwrap();
        if let Some(locked_at) = locked.get(username) {
            let config = self.config.lock().unwrap();
            let lockout = std::time::Duration::from_secs(config.lockout_duration_secs);
            if locked_at.elapsed() < lockout {
                return true;
            }
        }
        false
    }

    pub fn record_login_attempt(&self, ip: &IpAddr, success: bool, username: &str) {
        if !self.is_enabled() {
            return;
        }

        if success {
            self.login_attempts.lock().unwrap().remove(ip);
            self.emit_event(SecurityEvent::LoginSuccess {
                ip: *ip,
                username: username.to_string(),
            });
            return;
        }

        let config = self.config.lock().unwrap();
        let max_attempts = config.max_login_attempts;
        let window = std::time::Duration::from_secs(config.lockout_duration_secs);
        drop(config);

        let mut attempts = self.login_attempts.lock().unwrap();
        let timestamps = attempts.entry(*ip).or_insert_with(VecDeque::new);
        let now = Instant::now();

        timestamps.push_back(now);
        timestamps.retain(|t| now.duration_since(*t) < window);

        let count = timestamps.len() as u32;

        self.emit_event(SecurityEvent::LoginFailed {
            ip: *ip,
            username: username.to_string(),
            reason: "invalid credentials".to_string(),
        });

        self.stats.lock().unwrap().total_login_failures += 1;

        if count >= max_attempts {
            self.emit_event(SecurityEvent::BruteForceDetected {
                ip: *ip,
                attempts: count,
            });

            let alert = self.create_alert(
                SecurityEvent::BruteForceDetected {
                    ip: *ip,
                    attempts: count,
                },
                AlertSeverity::High,
                ActionTaken::UserLocked,
            );
            self.alerts.lock().unwrap().push(alert);

            self.locked_users
                .lock()
                .unwrap()
                .insert(username.to_string(), Instant::now());
            self.stats.lock().unwrap().total_blocked += 1;
        }
    }

    pub fn check_rate_limit(&self, ip: &IpAddr) -> RateLimitResult {
        if !self.is_enabled() {
            return RateLimitResult::Allowed;
        }

        let config = self.config.lock().unwrap();
        let max_qps = config.max_queries_per_second;
        drop(config);

        let mut limiter = self.rate_limiter.lock().unwrap();
        let now = Instant::now();
        let window = std::time::Duration::from_secs(1);

        let info = limiter.entry(*ip).or_insert_with(|| RateInfo {
            timestamps: VecDeque::new(),
            tokens: max_qps,
            last_refill: now,
        });

        let elapsed = now.duration_since(info.last_refill);
        if elapsed >= window {
            let refill_count = (elapsed.as_secs_f64() * max_qps as f64) as u32;
            info.tokens = (info.tokens + refill_count).min(max_qps);
            info.last_refill = now;
        }

        info.timestamps.retain(|t| now.duration_since(*t) < window);
        info.timestamps.push_back(now);

        let current_rate = info.timestamps.len() as u32;

        if info.tokens == 0 {
            self.emit_event(SecurityEvent::RateLimitExceeded {
                ip: *ip,
                current_rate,
                limit: max_qps,
            });

            let alert = self.create_alert(
                SecurityEvent::RateLimitExceeded {
                    ip: *ip,
                    current_rate,
                    limit: max_qps,
                },
                AlertSeverity::Medium,
                ActionTaken::RateLimited,
            );
            self.alerts.lock().unwrap().push(alert);
            self.stats.lock().unwrap().total_rate_limited += 1;

            RateLimitResult::Denied
        } else {
            info.tokens -= 1;
            RateLimitResult::Allowed
        }
    }

    pub fn check_command(&self, ip: &IpAddr, username: &str, command: &str, is_admin: bool) -> bool {
        if !self.is_enabled() {
            return true;
        }

        match command {
            "dropDatabase" | "drop" => {
                if !is_admin {
                    self.emit_event(SecurityEvent::SuspiciousQuery {
                        ip: *ip,
                        query: command.to_string(),
                        reason: "destructive operation by non-admin".to_string(),
                    });

                    let alert = self.create_alert(
                        SecurityEvent::SuspiciousQuery {
                            ip: *ip,
                            query: command.to_string(),
                            reason: "destructive operation by non-admin".to_string(),
                        },
                        AlertSeverity::High,
                        ActionTaken::Logged,
                    );
                    self.alerts.lock().unwrap().push(alert);

                    if self.config.lock().unwrap().alert_on_anomaly {
                        return false;
                    }
                }
            }
            "grantRolesToUser" | "createUser" | "dropUser" => {
                if !is_admin {
                    self.emit_event(SecurityEvent::PrivilegeEscalationAttempt {
                        ip: *ip,
                        user: username.to_string(),
                        target: command.to_string(),
                    });

                    let alert = self.create_alert(
                        SecurityEvent::PrivilegeEscalationAttempt {
                            ip: *ip,
                            user: username.to_string(),
                            target: command.to_string(),
                        },
                        AlertSeverity::Critical,
                        ActionTaken::Logged,
                    );
                    self.alerts.lock().unwrap().push(alert);

                    if self.config.lock().unwrap().alert_on_anomaly {
                        return false;
                    }
                }
            }
            _ => {}
        }

        true
    }

    pub fn block_ip(&self, ip: IpAddr) {
        self.ip_blacklist.lock().unwrap().insert(ip);
        self.emit_event(SecurityEvent::BlacklistBlocked { ip });
        let alert = self.create_alert(
            SecurityEvent::BlacklistBlocked { ip },
            AlertSeverity::High,
            ActionTaken::Blocked,
        );
        self.alerts.lock().unwrap().push(alert);
        self.stats.lock().unwrap().total_blocked += 1;
    }

    pub fn unblock_ip(&self, ip: &IpAddr) -> bool {
        self.ip_blacklist.lock().unwrap().remove(ip)
    }

    pub fn get_alerts(&self, limit: usize) -> Vec<SecurityAlert> {
        let alerts = self.alerts.lock().unwrap();
        alerts.iter().rev().take(limit).cloned().collect()
    }

    pub fn get_stats(&self) -> IdsStats {
        let mut stats = self.stats.lock().unwrap().clone();
        stats.total_alerts = self.alerts.lock().unwrap().len() as u64;
        stats
    }

    pub fn add_rule(&mut self, rule: DetectionRule) {
        self.rules.push(rule);
    }

    pub fn remove_rule(&mut self, index: usize) -> bool {
        if index < self.rules.len() {
            self.rules.remove(index);
            true
        } else {
            false
        }
    }

    pub fn update_config(&self, new_config: IdsConfig) {
        *self.config.lock().unwrap() = new_config;
    }

    fn emit_event(&self, event: SecurityEvent) {
        let severity = match &event {
            SecurityEvent::LoginFailed { .. } => AlertSeverity::Low,
            SecurityEvent::LoginSuccess { .. } => AlertSeverity::Low,
            SecurityEvent::BruteForceDetected { .. } => AlertSeverity::High,
            SecurityEvent::UnauthorizedAttempt { .. } => AlertSeverity::High,
            SecurityEvent::SuspiciousQuery { .. } => AlertSeverity::High,
            SecurityEvent::RateLimitExceeded { .. } => AlertSeverity::Medium,
            SecurityEvent::BlacklistBlocked { .. } => AlertSeverity::High,
            SecurityEvent::PrivilegeEscalationAttempt { .. } => AlertSeverity::Critical,
        };

        mdb_common::log_info!(
            "mdb-ids",
            "Security event: {:?} (severity: {:?})",
            event,
            severity
        );
    }

    fn create_alert(
        &self,
        event: SecurityEvent,
        severity: AlertSeverity,
        action_taken: ActionTaken,
    ) -> SecurityAlert {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        SecurityAlert {
            id: ObjectId::new().expect("ObjectId::new() failed"),
            timestamp,
            severity,
            event,
            action_taken,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimitResult {
    Allowed,
    Denied,
}

impl SecurityAlert {
    pub fn to_document(&self) -> Document {
        let mut doc = Document::new();
        doc.insert("_id", Bson::ObjectId(self.id));
        doc.insert("timestamp", Bson::Int64(self.timestamp));
        doc.insert(
            "severity",
            Bson::String(format!("{:?}", self.severity)),
        );
        doc.insert(
            "action_taken",
            Bson::String(format!("{:?}", self.action_taken)),
        );

        let event_doc = match &self.event {
            SecurityEvent::LoginFailed {
                ip,
                username,
                reason,
            } => {
                let mut d = Document::new();
                d.insert("type", Bson::String("LoginFailed".to_string()));
                d.insert("ip", Bson::String(ip.to_string()));
                d.insert("username", Bson::String(username.clone()));
                d.insert("reason", Bson::String(reason.clone()));
                d
            }
            SecurityEvent::LoginSuccess { ip, username } => {
                let mut d = Document::new();
                d.insert("type", Bson::String("LoginSuccess".to_string()));
                d.insert("ip", Bson::String(ip.to_string()));
                d.insert("username", Bson::String(username.clone()));
                d
            }
            SecurityEvent::BruteForceDetected { ip, attempts } => {
                let mut d = Document::new();
                d.insert("type", Bson::String("BruteForceDetected".to_string()));
                d.insert("ip", Bson::String(ip.to_string()));
                d.insert("attempts", Bson::Int32(*attempts as i32));
                d
            }
            SecurityEvent::UnauthorizedAttempt {
                ip,
                user,
                command,
            } => {
                let mut d = Document::new();
                d.insert(
                    "type",
                    Bson::String("UnauthorizedAttempt".to_string()),
                );
                d.insert("ip", Bson::String(ip.to_string()));
                d.insert("user", Bson::String(user.clone()));
                d.insert("command", Bson::String(command.clone()));
                d
            }
            SecurityEvent::SuspiciousQuery {
                ip,
                query,
                reason,
            } => {
                let mut d = Document::new();
                d.insert("type", Bson::String("SuspiciousQuery".to_string()));
                d.insert("ip", Bson::String(ip.to_string()));
                d.insert("query", Bson::String(query.clone()));
                d.insert("reason", Bson::String(reason.clone()));
                d
            }
            SecurityEvent::RateLimitExceeded {
                ip,
                current_rate,
                limit,
            } => {
                let mut d = Document::new();
                d.insert(
                    "type",
                    Bson::String("RateLimitExceeded".to_string()),
                );
                d.insert("ip", Bson::String(ip.to_string()));
                d.insert("current_rate", Bson::Int32(*current_rate as i32));
                d.insert("limit", Bson::Int32(*limit as i32));
                d
            }
            SecurityEvent::BlacklistBlocked { ip } => {
                let mut d = Document::new();
                d.insert("type", Bson::String("BlacklistBlocked".to_string()));
                d.insert("ip", Bson::String(ip.to_string()));
                d
            }
            SecurityEvent::PrivilegeEscalationAttempt {
                ip,
                user,
                target,
            } => {
                let mut d = Document::new();
                d.insert(
                    "type",
                    Bson::String("PrivilegeEscalationAttempt".to_string()),
                );
                d.insert("ip", Bson::String(ip.to_string()));
                d.insert("user", Bson::String(user.clone()));
                d.insert("target", Bson::String(target.clone()));
                d
            }
        };

        doc.insert("event", Bson::Document(event_doc));
        doc
    }
}

pub fn handle_security_alerts(
    _body: &Document,
    response_to: i32,
    detector: &IntrusionDetector,
) -> OpMsg {
    let alerts = detector.get_alerts(100);
    let alert_docs: Vec<Bson> = alerts.iter().map(|a| Bson::Document(a.to_document())).collect();

    let mut reply = ok_body();
    reply.insert("alerts", Bson::Array(alert_docs));
    reply.insert("total", Bson::Int64(alerts.len() as i64));
    OpMsg::reply(response_to, reply)
}

pub fn handle_ids_status(
    _body: &Document,
    response_to: i32,
    detector: &IntrusionDetector,
) -> OpMsg {
    let stats = detector.get_stats();
    let config = detector.config.lock().unwrap();
    let blacklist = detector.ip_blacklist.lock().unwrap();

    let mut reply = ok_body();
    reply.insert("enabled", Bson::Boolean(config.enabled));
    reply.insert(
        "maxLoginAttempts",
        Bson::Int32(config.max_login_attempts as i32),
    );
    reply.insert(
        "lockoutDurationSecs",
        Bson::Int64(config.lockout_duration_secs as i64),
    );
    reply.insert(
        "maxQueriesPerSecond",
        Bson::Int32(config.max_queries_per_second as i32),
    );
    reply.insert(
        "maxConnectionsPerIp",
        Bson::Int32(config.max_connections_per_ip as i32),
    );
    reply.insert("totalAlerts", Bson::Int64(stats.total_alerts as i64));
    reply.insert(
        "totalBlocked",
        Bson::Int64(stats.total_blocked as i64),
    );
    reply.insert(
        "totalRateLimited",
        Bson::Int64(stats.total_rate_limited as i64),
    );
    reply.insert(
        "totalLoginFailures",
        Bson::Int64(stats.total_login_failures as i64),
    );
    reply.insert(
        "blacklistedIps",
        Bson::Int32(blacklist.len() as i32),
    );

    OpMsg::reply(response_to, reply)
}

pub fn handle_block_ip(
    body: &Document,
    response_to: i32,
    detector: &IntrusionDetector,
) -> OpMsg {
    let ip_str = match doc_get_str(body, "blockIp") {
        Some(s) => s,
        None => {
            return OpMsg::reply(
                response_to,
                command_failed("blockIp: missing IP address", ErrorCode::BadValue),
            )
        }
    };

    let ip: IpAddr = match ip_str.parse() {
        Ok(ip) => ip,
        Err(_) => {
            return OpMsg::reply(
                response_to,
                command_failed("blockIp: invalid IP address", ErrorCode::BadValue),
            )
        }
    };

    detector.block_ip(ip);
    ok_reply(response_to)
}

pub fn handle_unblock_ip(
    body: &Document,
    response_to: i32,
    detector: &IntrusionDetector,
) -> OpMsg {
    let ip_str = match doc_get_str(body, "unblockIp") {
        Some(s) => s,
        None => {
            return OpMsg::reply(
                response_to,
                command_failed("unblockIp: missing IP address", ErrorCode::BadValue),
            )
        }
    };

    let ip: IpAddr = match ip_str.parse() {
        Ok(ip) => ip,
        Err(_) => {
            return OpMsg::reply(
                response_to,
                command_failed("unblockIp: invalid IP address", ErrorCode::BadValue),
            )
        }
    };

    let removed = detector.unblock_ip(&ip);
    let mut reply = ok_body();
    reply.insert("removed", Bson::Boolean(removed));
    reply.insert("ip", Bson::String(ip.to_string()));
    OpMsg::reply(response_to, reply)
}

pub fn handle_ids_config(
    body: &Document,
    response_to: i32,
    detector: &IntrusionDetector,
) -> OpMsg {
    let mut new_config = detector.config.lock().unwrap().clone();

    if let Some(enabled) = doc_get_bool(body, "enabled") {
        new_config.enabled = enabled;
    }
    if let Some(v) = doc_get_i64(body, "maxLoginAttempts") {
        new_config.max_login_attempts = v as u32;
    }
    if let Some(v) = doc_get_i64(body, "lockoutDurationSecs") {
        new_config.lockout_duration_secs = v as u64;
    }
    if let Some(v) = doc_get_i64(body, "maxQueriesPerSecond") {
        new_config.max_queries_per_second = v as u32;
    }
    if let Some(v) = doc_get_i64(body, "maxConnectionsPerIp") {
        new_config.max_connections_per_ip = v as u32;
    }
    if let Some(v) = doc_get_bool(body, "alertOnAnomaly") {
        new_config.alert_on_anomaly = v;
    }

    detector.update_config(new_config);

    let mut reply = ok_body();
    reply.insert("ok", 1.0f64);
    reply.insert("note", Bson::String("IDS configuration updated".to_string()));
    OpMsg::reply(response_to, reply)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_detector() -> IntrusionDetector {
        IntrusionDetector::new(IdsConfig::default())
    }

    fn make_detector_disabled() -> IntrusionDetector {
        let config = IdsConfig {
            enabled: false,
            ..Default::default()
        };
        IntrusionDetector::new(config)
    }

    fn make_detector_custom(max_attempts: u32, window_secs: u64, max_qps: u32) -> IntrusionDetector {
        let config = IdsConfig {
            max_login_attempts: max_attempts,
            lockout_duration_secs: window_secs,
            max_queries_per_second: max_qps,
            ..Default::default()
        };
        IntrusionDetector::new(config)
    }

    #[test]
    fn test_detector_creation() {
        let detector = make_detector();
        assert!(detector.is_enabled());
        assert!(!detector.is_ip_blocked(&"127.0.0.1".parse().unwrap()));
    }

    #[test]
    fn test_detector_disabled() {
        let detector = make_detector_disabled();
        assert!(!detector.is_enabled());
    }

    #[test]
    fn test_login_failure_records_attempt() {
        let detector = make_detector();
        let ip: IpAddr = "192.168.1.100".parse().unwrap();

        detector.record_login_attempt(&ip, false, "admin");

        let attempts = detector.login_attempts.lock().unwrap();
        assert!(attempts.contains_key(&ip));
        assert_eq!(attempts[&ip].len(), 1);
    }

    #[test]
    fn test_login_success_clears_attempts() {
        let detector = make_detector();
        let ip: IpAddr = "192.168.1.100".parse().unwrap();

        detector.record_login_attempt(&ip, false, "admin");
        detector.record_login_attempt(&ip, true, "admin");

        let attempts = detector.login_attempts.lock().unwrap();
        assert!(!attempts.contains_key(&ip));
    }

    #[test]
    fn test_brute_force_detection() {
        let detector = make_detector_custom(3, 300, 1000);
        let ip: IpAddr = "10.0.0.1".parse().unwrap();

        for _ in 0..2 {
            detector.record_login_attempt(&ip, false, "admin");
        }

        let alerts = detector.alerts.lock().unwrap();
        assert!(alerts.is_empty());
        drop(alerts);

        detector.record_login_attempt(&ip, false, "admin");

        let alerts = detector.alerts.lock().unwrap();
        assert!(!alerts.is_empty());

        let last_alert = alerts.last().unwrap();
        assert_eq!(last_alert.severity, AlertSeverity::High);
        assert_eq!(last_alert.action_taken, ActionTaken::UserLocked);
    }

    #[test]
    fn test_user_locked_after_brute_force() {
        let detector = make_detector_custom(2, 300, 1000);
        let ip: IpAddr = "10.0.0.1".parse().unwrap();

        detector.record_login_attempt(&ip, false, "admin");
        detector.record_login_attempt(&ip, false, "admin");

        assert!(detector.is_user_locked("admin"));
        assert!(!detector.is_user_locked("other"));
    }

    #[test]
    fn test_rate_limit_allowed() {
        let detector = make_detector_custom(5, 300, 100);
        let ip: IpAddr = "10.0.0.2".parse().unwrap();

        let result = detector.check_rate_limit(&ip);
        assert_eq!(result, RateLimitResult::Allowed);
    }

    #[test]
    fn test_rate_limit_exceeded() {
        let detector = make_detector_custom(5, 300, 2);
        let ip: IpAddr = "10.0.0.3".parse().unwrap();

        assert_eq!(detector.check_rate_limit(&ip), RateLimitResult::Allowed);
        assert_eq!(detector.check_rate_limit(&ip), RateLimitResult::Allowed);
        assert_eq!(detector.check_rate_limit(&ip), RateLimitResult::Denied);

        let alerts = detector.alerts.lock().unwrap();
        let rate_alert = alerts
            .iter()
            .find(|a| matches!(a.event, SecurityEvent::RateLimitExceeded { .. }));
        assert!(rate_alert.is_some());
    }

    #[test]
    fn test_block_ip() {
        let detector = make_detector();
        let ip: IpAddr = "10.0.0.4".parse().unwrap();

        assert!(!detector.is_ip_blocked(&ip));

        detector.block_ip(ip);

        assert!(detector.is_ip_blocked(&ip));
        assert!(!detector.is_ip_blocked(&"10.0.0.5".parse().unwrap()));
    }

    #[test]
    fn test_unblock_ip() {
        let detector = make_detector();
        let ip: IpAddr = "10.0.0.4".parse().unwrap();

        detector.block_ip(ip);
        assert!(detector.is_ip_blocked(&ip));

        let removed = detector.unblock_ip(&ip);
        assert!(removed);
        assert!(!detector.is_ip_blocked(&ip));
    }

    #[test]
    fn test_unblock_nonexistent_ip() {
        let detector = make_detector();
        let ip: IpAddr = "10.0.0.99".parse().unwrap();

        let removed = detector.unblock_ip(&ip);
        assert!(!removed);
    }

    #[test]
    fn test_check_command_admin_allowed() {
        let detector = make_detector();
        let ip: IpAddr = "10.0.0.5".parse().unwrap();

        assert!(detector.check_command(&ip, "admin", "dropDatabase", true));
        assert!(detector.check_command(&ip, "admin", "drop", true));
    }

    #[test]
    fn test_check_command_non_admin_blocked() {
        let detector = make_detector();
        let ip: IpAddr = "10.0.0.5".parse().unwrap();

        assert!(!detector.check_command(&ip, "user1", "dropDatabase", false));
        assert!(!detector.check_command(&ip, "user1", "drop", false));

        let alerts = detector.alerts.lock().unwrap();
        let suspicious = alerts
            .iter()
            .filter(|a| matches!(a.event, SecurityEvent::SuspiciousQuery { .. }))
            .count();
        assert!(suspicious >= 2);
    }

    #[test]
    fn test_privilege_escalation_detected() {
        let detector = make_detector();
        let ip: IpAddr = "10.0.0.6".parse().unwrap();

        assert!(!detector.check_command(&ip, "user1", "grantRolesToUser", false));
        assert!(!detector.check_command(&ip, "user1", "createUser", false));
        assert!(!detector.check_command(&ip, "user1", "dropUser", false));

        let alerts = detector.alerts.lock().unwrap();
        let priv_alerts = alerts
            .iter()
            .filter(|a| {
                matches!(
                    a.event,
                    SecurityEvent::PrivilegeEscalationAttempt { .. }
                )
            })
            .count();
        assert!(priv_alerts >= 3);
    }

    #[test]
    fn test_get_alerts_limit() {
        let detector = make_detector_custom(1, 300, 1);
        let ip: IpAddr = "10.0.0.7".parse().unwrap();

        for _ in 0..5 {
            detector.record_login_attempt(&ip, false, "admin");
        }

        let alerts_1 = detector.get_alerts(1);
        assert_eq!(alerts_1.len(), 1);

        let alerts_10 = detector.get_alerts(10);
        assert!(alerts_10.len() >= 1);
    }

    #[test]
    fn test_stats_tracking() {
        let detector = make_detector();
        let ip: IpAddr = "10.0.0.8".parse().unwrap();

        detector.record_login_attempt(&ip, false, "admin");
        detector.record_login_attempt(&ip, false, "admin");
        detector.block_ip(ip);

        let stats = detector.get_stats();
        assert_eq!(stats.total_login_failures, 2);
        assert_eq!(stats.total_blocked, 1);
    }

    #[test]
    fn test_disabled_detector_no_events() {
        let detector = make_detector_disabled();
        let ip: IpAddr = "10.0.0.9".parse().unwrap();

        detector.record_login_attempt(&ip, false, "admin");

        let attempts = detector.login_attempts.lock().unwrap();
        assert!(!attempts.contains_key(&ip));
    }

    #[test]
    fn test_alert_to_document() {
        let alert = SecurityAlert {
            id: ObjectId::new().unwrap(),
            timestamp: 1234567890,
            severity: AlertSeverity::High,
            event: SecurityEvent::BruteForceDetected {
                ip: "10.0.0.1".parse().unwrap(),
                attempts: 5,
            },
            action_taken: ActionTaken::UserLocked,
        };

        let doc = alert.to_document();
        assert!(doc.get("_id").is_some());
        assert_eq!(doc_get_i64(&doc, "timestamp"), Some(1234567890));
        assert_eq!(doc_get_str(&doc, "severity"), Some("High"));
        assert_eq!(doc_get_str(&doc, "action_taken"), Some("UserLocked"));

        let event = doc.get("event").and_then(|v| v.as_document()).unwrap();
        assert_eq!(
            doc_get_str(event, "type"),
            Some("BruteForceDetected")
        );
        assert_eq!(doc_get_str(event, "ip"), Some("10.0.0.1"));
        assert_eq!(doc_get_i64(event, "attempts"), Some(5));
    }

    #[test]
    fn test_add_remove_rule() {
        let mut detector = make_detector();
        let initial_count = detector.rules.len();

        detector.add_rule(DetectionRule::SuspiciousQuery {
            patterns: vec!["test".to_string()],
        });
        assert_eq!(detector.rules.len(), initial_count + 1);

        assert!(detector.remove_rule(initial_count));
        assert_eq!(detector.rules.len(), initial_count);

        assert!(!detector.remove_rule(initial_count + 100));
    }

    #[test]
    fn test_update_config() {
        let detector = make_detector();
        let new_config = IdsConfig {
            enabled: false,
            max_login_attempts: 10,
            ..Default::default()
        };

        detector.update_config(new_config);

        assert!(!detector.is_enabled());
        let config = detector.config.lock().unwrap();
        assert_eq!(config.max_login_attempts, 10);
    }
}
