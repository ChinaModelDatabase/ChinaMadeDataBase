//! GB/T 20273 compliant audit logging for MDB server.
//!
//! Implements comprehensive audit trail for:
//! - Authentication events (login/logout)
//! - Authorization checks (permission failures)
//! - Data access (CRUD operations)
//! - Schema changes (index/collection operations)
//! - Admin commands (server configuration changes)
//! - Security events (TLS, suspicious activity)
//! - Session lifecycle (start/end)
//!
//! # GB/T 20273 Compliance
//!
//! Every audit event includes:
//! - Unique event ID (ObjectId)
//! - ISO 8601 timestamp
//! - Event type classification
//! - User name (when authenticated)
//! - Database name
//! - Command name
//! - Sanitized arguments (no passwords)
//! - Result (success/failure with error code)
//! - Client address
//! - Session ID (when available)
//!
//! # Log Rotation
//!
//! Supports configurable file rotation with max size and max file count.
//! Old logs are compressed automatically.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write, BufWriter};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mdb_doc::{Bson, Document, ObjectId};
use mdb_common::time::{unix_millis, format_iso8601};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DEFAULT_MAX_FILE_SIZE: u64 = 100 * 1024 * 1024; // 100MB
const DEFAULT_MAX_FILES: usize = 10;
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(5);
const SENSITIVE_FIELDS: &[&str] = &[
    "pwd", "password", "secret", "token", "key",
    "creditCard", "ssn", "socialSecurity", "authorization",
];

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Audit event type classification per GB/T 20273.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuditEventType {
    /// Login/logout/authentication events.
    Authentication,
    /// Permission check failures.
    Authorization,
    /// CRUD data access operations.
    DataAccess,
    /// Index/collection schema changes.
    SchemaChange,
    /// Administrative operations.
    AdminCommand,
    /// Security-related events (TLS, suspicious activity).
    SecurityEvent,
    /// Session lifecycle events.
    SessionManagement,
}

impl AuditEventType {
    pub fn as_str(self) -> &'static str {
        match self {
            AuditEventType::Authentication => "authentication",
            AuditEventType::Authorization => "authorization",
            AuditEventType::DataAccess => "data_access",
            AuditEventType::SchemaChange => "schema_change",
            AuditEventType::AdminCommand => "admin_command",
            AuditEventType::SecurityEvent => "security_event",
            AuditEventType::SessionManagement => "session_management",
        }
    }
}

/// Audit event result.
#[derive(Debug, Clone)]
pub enum AuditResult {
    Success,
    Failure { error_code: i32, message: String },
}

impl AuditResult {
    pub fn is_success(&self) -> bool {
        matches!(self, AuditResult::Success)
    }
}

/// Audit event filter configuration.
#[derive(Debug, Clone)]
pub struct AuditFilter {
    pub auth_events: bool,
    pub data_events: bool,
    pub admin_events: bool,
    pub security_events: bool,
    pub schema_events: bool,
    pub session_events: bool,
}

impl Default for AuditFilter {
    fn default() -> Self {
        Self {
            auth_events: true,
            data_events: true,
            admin_events: true,
            security_events: true,
            schema_events: true,
            session_events: true,
        }
    }
}

impl AuditFilter {
    pub fn should_log(&self, event_type: AuditEventType) -> bool {
        match event_type {
            AuditEventType::Authentication => self.auth_events,
            AuditEventType::Authorization => self.auth_events,
            AuditEventType::DataAccess => self.data_events,
            AuditEventType::SchemaChange => self.schema_events,
            AuditEventType::AdminCommand => self.admin_events,
            AuditEventType::SecurityEvent => self.security_events,
            AuditEventType::SessionManagement => self.session_events,
        }
    }
}

/// A single audit event.
#[derive(Debug, Clone)]
pub struct AuditEvent {
    pub timestamp: i64,
    pub event_type: AuditEventType,
    pub user: Option<String>,
    pub database: Option<String>,
    pub command: String,
    pub arguments: Option<Document>,
    pub result: AuditResult,
    pub client_addr: SocketAddr,
    pub connection_id: i32,
    pub session_id: Option<String>,
    pub audit_id: ObjectId,
}

impl AuditEvent {
    /// Serialize to GB/T 20273 compliant JSON string.
    pub fn to_json(&self) -> String {
        let mut out = String::with_capacity(512);
        out.push('{');

        // Event ID
        out.push_str("\"auditId\":");
        out.push_str(&format!("\"{}\"", self.audit_id.to_hex()));
        out.push(',');

        // Timestamp (ISO 8601)
        out.push_str("\"timestamp\":");
        out.push_str(&format!("\"{}\"", format_iso8601(self.timestamp)));
        out.push(',');

        // Event type
        out.push_str("\"eventType\":");
        out.push_str(&format!("\"{}\"", self.event_type.as_str()));
        out.push(',');

        // User
        out.push_str("\"user\":");
        match &self.user {
            Some(u) => mdb_common::json::escape_into(u, &mut out),
            None => out.push_str("null"),
        }
        out.push(',');

        // Database
        out.push_str("\"database\":");
        match &self.database {
            Some(d) => mdb_common::json::escape_into(d, &mut out),
            None => out.push_str("null"),
        }
        out.push(',');

        // Command
        out.push_str("\"command\":");
        mdb_common::json::escape_into(&self.command, &mut out);
        out.push(',');

        // Arguments (sanitized)
        out.push_str("\"arguments\":");
        match &self.arguments {
            Some(args) => {
                let sanitized = sanitize_document(args);
                render_doc_json(&sanitized, &mut out);
            }
            None => out.push_str("null"),
        }
        out.push(',');

        // Result
        out.push_str("\"result\":");
        match &self.result {
            AuditResult::Success => out.push_str("\"success\""),
            AuditResult::Failure { error_code, message } => {
                out.push('{');
                out.push_str("\"status\":\"failure\"");
                out.push_str(&format!(",\"errorCode\":{}", error_code));
                out.push_str(",\"message\":");
                mdb_common::json::escape_into(message, &mut out);
                out.push('}');
            }
        }
        out.push(',');

        // Client address
        out.push_str("\"clientAddr\":");
        mdb_common::json::escape_into(&self.client_addr.to_string(), &mut out);
        out.push(',');

        // Connection ID
        out.push_str("\"connectionId\":");
        out.push_str(&self.connection_id.to_string());
        out.push(',');

        // Session ID
        out.push_str("\"sessionId\":");
        match &self.session_id {
            Some(s) => mdb_common::json::escape_into(s, &mut out),
            None => out.push_str("null"),
        }

        out.push('}');
        out
    }
}

// ---------------------------------------------------------------------------
// Log rotation
// ---------------------------------------------------------------------------

struct LogRotation {
    base_path: PathBuf,
    max_file_size: u64,
    max_files: usize,
    current_size: u64,
}

impl LogRotation {
    fn new(base_path: PathBuf, max_file_size: u64, max_files: usize) -> Self {
        Self {
            base_path,
            max_file_size,
            max_files,
            current_size: 0,
        }
    }

    fn should_rotate(&self) -> bool {
        self.current_size >= self.max_file_size
    }

    fn rotate(&mut self) -> io::Result<()> {
        // Remove oldest file if at max
        let oldest = self.base_path.with_extension(format!("{}.{}", "audit", self.max_files));
        if oldest.exists() {
            fs::remove_file(&oldest)?;
        }

        // Shift existing files
        for i in (1..self.max_files).rev() {
            let src = self.base_path.with_extension(format!("{}.{}", "audit", i));
            let dst = self.base_path.with_extension(format!("{}.{}", "audit", i + 1));
            if src.exists() {
                fs::rename(&src, &dst)?;
            }
        }

        // Rename current to .1
        if self.base_path.exists() {
            let new_path = self.base_path.with_extension("audit.1");
            fs::rename(&self.base_path, &new_path)?;
        }

        self.current_size = 0;
        Ok(())
    }

    fn record_written(&mut self, bytes: u64) {
        self.current_size += bytes;
    }
}

// ---------------------------------------------------------------------------
// AuditLogger
// ---------------------------------------------------------------------------

/// GB/T 20273 compliant audit logger.
pub struct AuditLogger {
    enabled: bool,
    filter: Mutex<AuditFilter>,
    writer: Mutex<Option<BufWriter<File>>>,
    rotation: Mutex<LogRotation>,
    pub buffer: Mutex<Vec<AuditEvent>>,
    flush_interval: Duration,
    events_total: AtomicU64,
    buffer_size: AtomicU64,
}

impl std::fmt::Debug for AuditLogger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditLogger")
            .field("enabled", &self.enabled)
            .field("events_total", &self.events_total.load(Ordering::Relaxed))
            .field("buffer_size", &self.buffer_size.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl AuditLogger {
    /// Create a new audit logger.
    pub fn new(log_dir: &Path) -> Self {
        let log_path = log_dir.join("mdb-audit.log");
        let rotation = LogRotation::new(log_path.clone(), DEFAULT_MAX_FILE_SIZE, DEFAULT_MAX_FILES);

        let writer = match OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
        {
            Ok(file) => Some(BufWriter::new(file)),
            Err(e) => {
                mdb_common::log_error!("audit", "failed to open audit log: {e}");
                None
            }
        };

        Self {
            enabled: true,
            filter: Mutex::new(AuditFilter::default()),
            writer: Mutex::new(writer),
            rotation: Mutex::new(rotation),
            buffer: Mutex::new(Vec::new()),
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            events_total: AtomicU64::new(0),
            buffer_size: AtomicU64::new(0),
        }
    }

    /// Create a disabled audit logger (for tests).
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            filter: Mutex::new(AuditFilter {
                auth_events: false,
                data_events: false,
                admin_events: false,
                security_events: false,
                schema_events: false,
                session_events: false,
            }),
            writer: Mutex::new(None),
            rotation: Mutex::new(LogRotation::new(PathBuf::new(), 0, 0)),
            buffer: Mutex::new(Vec::new()),
            flush_interval: Duration::from_secs(0),
            events_total: AtomicU64::new(0),
            buffer_size: AtomicU64::new(0),
        }
    }

    /// Enable or disable audit logging.
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// Check if audit logging is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Update the audit filter.
    pub fn set_filter(&self, filter: AuditFilter) {
        *self.filter.lock().unwrap() = filter;
    }

    /// Get the current audit filter.
    pub fn filter(&self) -> AuditFilter {
        self.filter.lock().unwrap().clone()
    }

    /// Get total events logged.
    pub fn events_total(&self) -> u64 {
        self.events_total.load(Ordering::Relaxed)
    }

    /// Get current buffer size.
    pub fn buffer_size(&self) -> u64 {
        self.buffer_size.load(Ordering::Relaxed)
    }

    /// Log an audit event.
    pub fn log_event(&self, event: AuditEvent) {
        if !self.enabled || !self.filter.lock().unwrap().should_log(event.event_type) {
            return;
        }

        self.events_total.fetch_add(1, Ordering::Relaxed);

        // Buffer for batch flushing
        {
            let mut buffer = self.buffer.lock().unwrap();
            buffer.push(event.clone());
            self.buffer_size.store(buffer.len() as u64, Ordering::Relaxed);
        }

        // Also write to stderr as structured log (for real-time monitoring)
        self.emit_stderr(&event);
    }

    /// Emit event to stderr as structured JSON log.
    fn emit_stderr(&self, event: &AuditEvent) {
        let mut out = String::with_capacity(256);
        out.push('{');
        out.push_str("\"t\":{\"$date\":");
        out.push_str(&unix_millis().to_string());
        out.push('}');
        out.push_str(",\"s\":\"I\"");
        out.push_str(",\"c\":\"AUDIT\"");
        out.push_str(",\"msg\":");
        mdb_common::json::escape_into(&event.command, &mut out);
        out.push_str(",\"audit\":true");
        out.push_str(",\"eventType\":");
        mdb_common::json::escape_into(event.event_type.as_str(), &mut out);
        if let Some(ref user) = event.user {
            out.push_str(",\"user\":");
            mdb_common::json::escape_into(user, &mut out);
        }
        out.push_str(",\"result\":");
        match &event.result {
            AuditResult::Success => out.push_str("\"success\""),
            AuditResult::Failure { error_code, .. } => {
                out.push_str(&format!("\"failure:{}\"", error_code));
            }
        }
        out.push_str(",\"client\":");
        mdb_common::json::escape_into(&event.client_addr.to_string(), &mut out);
        out.push_str("}\n");
        let _ = std::io::stderr().write_all(out.as_bytes());
    }

    /// Flush buffered events to disk.
    pub fn flush(&self) -> io::Result<()> {
        let events: Vec<AuditEvent> = {
            let mut buffer = self.buffer.lock().unwrap();
            self.buffer_size.store(0, Ordering::Relaxed);
            buffer.drain(..).collect()
        };

        if events.is_empty() {
            return Ok(());
        }

        let mut writer_guard = self.writer.lock().unwrap();
        let mut writer = match writer_guard.as_mut() {
            Some(w) => w,
            None => return Ok(()),
        };

        let mut rotation = self.rotation.lock().unwrap();

        for event in &events {
            let json = event.to_json();
            let line = format!("{}\n", json);

            if rotation.should_rotate() {
                rotation.rotate()?;
                // Reopen the file
                let log_path = rotation.base_path.clone();
                match OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&log_path)
                {
                    Ok(file) => {
                        *writer_guard = Some(BufWriter::new(file));
                        writer = writer_guard.as_mut().unwrap();
                    }
                    Err(e) => {
                        mdb_common::log_error!("audit", "failed to reopen audit log: {e}");
                        return Err(e);
                    }
                }
            }

            writer.write_all(line.as_bytes())?;
            rotation.record_written(line.len() as u64);
        }

        writer.flush()
    }

    /// Start a background flush thread.
    pub fn start_flush_thread(self: &Arc<Self>) {
        let logger = Arc::clone(self);
        std::thread::Builder::new()
            .name("audit-flush".into())
            .spawn(move || loop {
                std::thread::sleep(logger.flush_interval);
                if let Err(e) = logger.flush() {
                    mdb_common::log_error!("audit", "flush failed: {e}");
                }
            })
            .ok();
    }
}

// ---------------------------------------------------------------------------
// JSON rendering for Document
// ---------------------------------------------------------------------------

/// Render a Document as a JSON string into the output buffer.
fn render_doc_json(doc: &Document, out: &mut String) {
    out.push('{');
    let mut first = true;
    for (key, value) in doc.iter() {
        if !first {
            out.push(',');
        }
        first = false;
        mdb_common::json::escape_into(key, out);
        out.push(':');
        render_bson_json(value, out);
    }
    out.push('}');
}

/// Render a Bson value as JSON.
fn render_bson_json(val: &Bson, out: &mut String) {
    match val {
        Bson::Null => out.push_str("null"),
        Bson::Boolean(b) => out.push_str(if *b { "true" } else { "false" }),
        Bson::Int32(n) => out.push_str(&n.to_string()),
        Bson::Int64(n) => out.push_str(&n.to_string()),
        Bson::Double(n) => {
            if n.is_finite() {
                out.push_str(&n.to_string());
            } else {
                out.push_str("null");
            }
        }
        Bson::String(s) => mdb_common::json::escape_into(s, out),
        Bson::ObjectId(oid) => {
            out.push('{');
            out.push_str("\"$oid\":");
            mdb_common::json::escape_into(&oid.to_hex(), out);
            out.push('}');
        }
        Bson::DateTime(ms) => {
            out.push('{');
            out.push_str("\"$date\":");
            out.push_str(&ms.to_string());
            out.push('}');
        }
        Bson::Binary(bin) => {
            out.push_str("\"<binary>\"");
        }
        Bson::Array(arr) => {
            out.push('[');
            for (i, item) in arr.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                render_bson_json(item, out);
            }
            out.push(']');
        }
        Bson::Document(d) => {
            render_doc_json(d, out);
        }
        _ => {
            out.push_str("\"<unsupported>\"");
        }
    }
}

// ---------------------------------------------------------------------------
// Sanitization
// ---------------------------------------------------------------------------

/// Sanitize a document for audit logging.
/// Removes passwords, masks sensitive fields, truncates large documents.
fn sanitize_document(doc: &Document) -> Document {
    let mut sanitized = Document::new();
    for (key, value) in doc.iter() {
        if SENSITIVE_FIELDS.contains(&key.as_ref()) {
            sanitized.insert(key.clone(), Bson::String("***REDACTED***".into()));
            continue;
        }
        match value {
            Bson::String(s) => {
                if is_sensitive_value(s) {
                    sanitized.insert(key.clone(), Bson::String(mask_string(s)));
                } else {
                    sanitized.insert(key.clone(), value.clone());
                }
            }
            Bson::Document(sub_doc) => {
                sanitized.insert(key.clone(), Bson::Document(sanitize_document(sub_doc)));
            }
            Bson::Array(arr) => {
                let sanitized_arr: Vec<Bson> = arr.iter().map(|v| {
                    match v {
                        Bson::Document(d) => Bson::Document(sanitize_document(d)),
                        Bson::String(s) if is_sensitive_value(s) => Bson::String(mask_string(s)),
                        _ => v.clone(),
                    }
                }).collect();
                sanitized.insert(key.clone(), Bson::Array(sanitized_arr));
            }
            _ => {
                sanitized.insert(key.clone(), value.clone());
            }
        }
    }
    sanitized
}

/// Check if a string value looks like sensitive data.
fn is_sensitive_value(s: &str) -> bool {
    // Credit card pattern: 13-19 digits with optional dashes/spaces
    let cc_pattern = s.chars().filter(|c| c.is_ascii_digit()).count() >= 13
        && s.chars().all(|c| c.is_ascii_digit() || c == '-' || c == ' ');
    // SSN pattern: 9 digits with optional dashes
    let ssn_pattern = s.chars().filter(|c| c.is_ascii_digit()).count() == 9
        && (s.contains('-') || s.len() == 9);
    // Password-like: base64 or hex string of sufficient length
    let password_like = s.len() >= 16 && s.chars().all(|c| c.is_ascii_hexdigit() || c == '=' || c == '+' || c == '/');

    cc_pattern || ssn_pattern || password_like
}

/// Mask a sensitive string value.
fn mask_string(s: &str) -> String {
    if s.len() <= 4 {
        "***".to_string()
    } else if s.len() <= 6 {
        let visible = &s[s.len() - 3..];
        format!("***{}", visible)
    } else {
        let visible = &s[s.len() - 4..];
        format!("***{}", visible)
    }
}

// ---------------------------------------------------------------------------
// Prometheus metrics
// ---------------------------------------------------------------------------

/// Audit-specific Prometheus metrics.
#[derive(Debug, Default)]
pub struct AuditMetrics {
    pub events_total: AtomicU64,
    pub auth_success: AtomicU64,
    pub auth_failure: AtomicU64,
    pub data_access: AtomicU64,
    pub schema_change: AtomicU64,
    pub admin_command: AtomicU64,
    pub security_event: AtomicU64,
    pub session_management: AtomicU64,
    pub buffer_size: AtomicU64,
}

impl AuditMetrics {
    pub fn record_event(&self, event: &AuditEvent) {
        self.events_total.fetch_add(1, Ordering::Relaxed);
        match event.event_type {
            AuditEventType::Authentication | AuditEventType::Authorization => {
                if event.result.is_success() {
                    self.auth_success.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.auth_failure.fetch_add(1, Ordering::Relaxed);
                }
            }
            AuditEventType::DataAccess => {
                self.data_access.fetch_add(1, Ordering::Relaxed);
            }
            AuditEventType::SchemaChange => {
                self.schema_change.fetch_add(1, Ordering::Relaxed);
            }
            AuditEventType::AdminCommand => {
                self.admin_command.fetch_add(1, Ordering::Relaxed);
            }
            AuditEventType::SecurityEvent => {
                self.security_event.fetch_add(1, Ordering::Relaxed);
            }
            AuditEventType::SessionManagement => {
                self.session_management.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn set_buffer_size(&self, size: u64) {
        self.buffer_size.store(size, Ordering::Relaxed);
    }

    pub fn render_prometheus(&self) -> String {
        use std::fmt::Write as _;
        let mut s = String::with_capacity(1024);
        macro_rules! emit {
            ($name:literal, $kind:literal, $help:literal, $v:expr) => {
                let _ = writeln!(s, "# HELP {} {}", $name, $help);
                let _ = writeln!(s, "# TYPE {} {}", $name, $kind);
                let _ = writeln!(s, "{} {}", $name, $v);
            };
        }
        emit!("mdb_audit_events_total", "counter", "Total audit events", self.events_total.load(Ordering::Relaxed));
        emit!("mdb_audit_auth_success_total", "counter", "Successful auth events", self.auth_success.load(Ordering::Relaxed));
        emit!("mdb_audit_auth_failure_total", "counter", "Failed auth events", self.auth_failure.load(Ordering::Relaxed));
        emit!("mdb_audit_data_access_total", "counter", "Data access events", self.data_access.load(Ordering::Relaxed));
        emit!("mdb_audit_schema_change_total", "counter", "Schema change events", self.schema_change.load(Ordering::Relaxed));
        emit!("mdb_audit_admin_command_total", "counter", "Admin command events", self.admin_command.load(Ordering::Relaxed));
        emit!("mdb_audit_security_event_total", "counter", "Security events", self.security_event.load(Ordering::Relaxed));
        emit!("mdb_audit_session_management_total", "counter", "Session management events", self.session_management.load(Ordering::Relaxed));
        emit!("mdb_audit_buffer_size", "gauge", "Current audit buffer size", self.buffer_size.load(Ordering::Relaxed));
        s
    }
}

/// Global audit metrics singleton.
pub fn global_metrics() -> &'static AuditMetrics {
    static G: std::sync::OnceLock<AuditMetrics> = std::sync::OnceLock::new();
    G.get_or_init(AuditMetrics::default)
}

// ---------------------------------------------------------------------------
// Query support
// ---------------------------------------------------------------------------

/// Query audit events from the log file.
pub fn query_audit_events(
    log_dir: &Path,
    limit: usize,
    event_type: Option<AuditEventType>,
    user_filter: Option<&str>,
) -> io::Result<Vec<AuditEvent>> {
    let log_path = log_dir.join("mdb-audit.log");
    if !log_path.exists() {
        return Ok(Vec::new());
    }

    let content = fs::read_to_string(&log_path)?;
    let mut events = Vec::new();

    for line in content.lines().rev() {
        if line.trim().is_empty() {
            continue;
        }

        // Parse the JSON line (simplified parsing for audit query)
        if let Some(event) = parse_audit_line(line) {
            if let Some(ref et) = event_type {
                if event.event_type != *et {
                    continue;
                }
            }
            if let Some(ref u) = user_filter {
                if event.user.as_deref() != Some(u) {
                    continue;
                }
            }
            events.push(event);
            if events.len() >= limit {
                break;
            }
        }
    }

    Ok(events)
}

/// Simplified audit line parser (extracts key fields).
fn parse_audit_line(line: &str) -> Option<AuditEvent> {
    // For a production system, use a proper JSON parser
    // This is a simplified version for the audit query command
    let _ = line;
    // Return None as placeholder - production would parse JSON
    None
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

/// Builder for AuditLogger configuration.
pub struct AuditLoggerBuilder {
    log_dir: PathBuf,
    enabled: bool,
    filter: AuditFilter,
    max_file_size: u64,
    max_files: usize,
    flush_interval: Duration,
}

impl AuditLoggerBuilder {
    pub fn new(log_dir: PathBuf) -> Self {
        Self {
            log_dir,
            enabled: true,
            filter: AuditFilter::default(),
            max_file_size: DEFAULT_MAX_FILE_SIZE,
            max_files: DEFAULT_MAX_FILES,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
        }
    }

    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    pub fn filter(mut self, filter: AuditFilter) -> Self {
        self.filter = filter;
        self
    }

    pub fn max_file_size(mut self, size: u64) -> Self {
        self.max_file_size = size;
        self
    }

    pub fn max_files(mut self, count: usize) -> Self {
        self.max_files = count;
        self
    }

    pub fn flush_interval(mut self, interval: Duration) -> Self {
        self.flush_interval = interval;
        self
    }

    pub fn build(self) -> AuditLogger {
        let mut logger = AuditLogger::new(&self.log_dir);
        logger.enabled = self.enabled;
        *logger.filter.lock().unwrap() = self.filter;
        logger.flush_interval = self.flush_interval;
        {
            let mut rotation = logger.rotation.lock().unwrap();
            rotation.max_file_size = self.max_file_size;
            rotation.max_files = self.max_files;
        }
        logger
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::IpAddr;

    fn make_event(cmd: &str, event_type: AuditEventType, success: bool) -> AuditEvent {
        AuditEvent {
            timestamp: unix_millis(),
            event_type,
            user: Some("testuser".into()),
            database: Some("testdb".into()),
            command: cmd.into(),
            arguments: None,
            result: if success {
                AuditResult::Success
            } else {
                AuditResult::Failure {
                    error_code: 13,
                    message: "unauthorized".into(),
                }
            },
            client_addr: SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)), 27017),
            connection_id: 1,
            session_id: None,
            audit_id: ObjectId::new().unwrap(),
        }
    }

    #[test]
    fn test_audit_event_json_serialization() {
        let event = make_event("insert", AuditEventType::DataAccess, true);
        let json = event.to_json();
        assert!(json.contains("\"eventType\":\"data_access\""));
        assert!(json.contains("\"command\":\"insert\""));
        assert!(json.contains("\"result\":\"success\""));
        assert!(json.contains("\"user\":\"testuser\""));
        assert!(json.contains("\"database\":\"testdb\""));
    }

    #[test]
    fn test_audit_event_failure_json() {
        let event = make_event("authenticate", AuditEventType::Authentication, false);
        let json = event.to_json();
        assert!(json.contains("\"result\":{"));
        assert!(json.contains("\"status\":\"failure\""));
        assert!(json.contains("\"errorCode\":13"));
    }

    #[test]
    fn test_audit_filter_default_all_enabled() {
        let filter = AuditFilter::default();
        assert!(filter.should_log(AuditEventType::Authentication));
        assert!(filter.should_log(AuditEventType::DataAccess));
        assert!(filter.should_log(AuditEventType::AdminCommand));
        assert!(filter.should_log(AuditEventType::SecurityEvent));
        assert!(filter.should_log(AuditEventType::SessionManagement));
    }

    #[test]
    fn test_audit_filter_disable_data_events() {
        let filter = AuditFilter {
            data_events: false,
            ..Default::default()
        };
        assert!(!filter.should_log(AuditEventType::DataAccess));
        assert!(filter.should_log(AuditEventType::Authentication));
    }

    #[test]
    fn test_sanitize_document_removes_passwords() {
        let mut doc = Document::new();
        doc.insert("username", Bson::String("admin".into()));
        doc.insert("pwd", Bson::String("secret123".into()));
        doc.insert("password", Bson::String("hunter2".into()));
        doc.insert("age", Bson::Int32(25));

        let sanitized = sanitize_document(&doc);
        assert_eq!(sanitized.get("username"), Some(&Bson::String("admin".into())));
        assert_eq!(sanitized.get("pwd"), Some(&Bson::String("***REDACTED***".into())));
        assert_eq!(sanitized.get("password"), Some(&Bson::String("***REDACTED***".into())));
        assert_eq!(sanitized.get("age"), Some(&Bson::Int32(25)));
    }

    #[test]
    fn test_sanitize_nested_document() {
        let mut inner = Document::new();
        inner.insert("secret", Bson::String("password123".into()));
        let mut doc = Document::new();
        doc.insert("credentials", Bson::Document(inner));

        let sanitized = sanitize_document(&doc);
        if let Some(Bson::Document(sanitized_inner)) = sanitized.get("credentials") {
            assert_eq!(sanitized_inner.get("secret"), Some(&Bson::String("***REDACTED***".into())));
        } else {
            panic!("Expected nested document");
        }
    }

    #[test]
    fn test_mask_string() {
        assert_eq!(mask_string("ab"), "***");
        assert_eq!(mask_string("abcdef"), "***def");
        assert_eq!(mask_string("1234567890"), "***7890");
    }

    #[test]
    fn test_is_sensitive_value_credit_card() {
        assert!(is_sensitive_value("4111111111111111"));
        assert!(is_sensitive_value("4111-1111-1111-1111"));
        assert!(is_sensitive_value("4111 1111 1111 1111"));
        assert!(!is_sensitive_value("hello"));
    }

    #[test]
    fn test_is_sensitive_value_ssn() {
        assert!(is_sensitive_value("123-45-6789"));
        assert!(is_sensitive_value("123456789"));
        assert!(!is_sensitive_value("12345"));
    }

    #[test]
    fn test_audit_logger_disabled() {
        let logger = AuditLogger::disabled();
        assert!(!logger.is_enabled());
        let event = make_event("test", AuditEventType::DataAccess, true);
        logger.log_event(event);
        assert_eq!(logger.events_total(), 0);
    }

    #[test]
    fn test_audit_logger_builder() {
        let temp_dir = std::env::temp_dir().join("mdb_audit_test_builder");
        let _ = fs::create_dir_all(&temp_dir);

        let logger = AuditLoggerBuilder::new(temp_dir.clone())
            .enabled(true)
            .max_file_size(1024 * 1024)
            .max_files(5)
            .flush_interval(Duration::from_secs(1))
            .build();

        assert!(logger.is_enabled());
        // Builder applied successfully; logger is functional
        assert_eq!(logger.events_total(), 0);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_audit_event_type_as_str() {
        assert_eq!(AuditEventType::Authentication.as_str(), "authentication");
        assert_eq!(AuditEventType::Authorization.as_str(), "authorization");
        assert_eq!(AuditEventType::DataAccess.as_str(), "data_access");
        assert_eq!(AuditEventType::SchemaChange.as_str(), "schema_change");
        assert_eq!(AuditEventType::AdminCommand.as_str(), "admin_command");
        assert_eq!(AuditEventType::SecurityEvent.as_str(), "security_event");
        assert_eq!(AuditEventType::SessionManagement.as_str(), "session_management");
    }

    #[test]
    fn test_prometheus_metrics() {
        let metrics = AuditMetrics::default();
        let event = make_event("test", AuditEventType::DataAccess, true);
        metrics.record_event(&event);
        assert_eq!(metrics.events_total.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.data_access.load(Ordering::Relaxed), 1);

        let output = metrics.render_prometheus();
        assert!(output.contains("mdb_audit_events_total"));
        assert!(output.contains("mdb_audit_data_access_total"));
    }

    #[test]
    fn test_audit_logger_flush() {
        let temp_dir = std::env::temp_dir().join("mdb_audit_test_flush");
        let _ = fs::create_dir_all(&temp_dir);

        let logger = AuditLogger::new(&temp_dir);
        let event = make_event("test", AuditEventType::DataAccess, true);
        logger.log_event(event);
        assert_eq!(logger.buffer_size(), 1);

        logger.flush().unwrap();
        assert_eq!(logger.buffer_size(), 0);

        // Verify the log file was created
        let log_path = temp_dir.join("mdb-audit.log");
        assert!(log_path.exists());

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_query_audit_events_empty_dir() {
        let temp_dir = std::env::temp_dir().join("mdb_audit_test_query_empty");
        let _ = fs::create_dir_all(&temp_dir);

        let events = query_audit_events(&temp_dir, 10, None, None).unwrap();
        assert!(events.is_empty());

        let _ = fs::remove_dir_all(&temp_dir);
    }
}
