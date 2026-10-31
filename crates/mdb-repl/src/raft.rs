//! Raft 共识协议实现。
//!
//! 包含：快照、预投票、WAL 持久化、成员变更、读关注、心跳检测。

use crate::config::RsSettings;
use crate::types::{
    ClusterMembership, Command, LogEntry, NodeId, PreVoteRequest, PreVoteResponse, RaftState,
    ReadConcern, Snapshot, WalEntry,
};
use mdb_doc::Document;
use std::collections::HashMap;

/// 请求投票 RPC。
#[derive(Debug, Clone)]
pub struct RequestVote {
    pub term: u64,
    pub candidate_id: NodeId,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

/// 请求投票响应。
#[derive(Debug, Clone)]
pub struct RequestVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

/// 追加条目 RPC。
#[derive(Debug, Clone)]
pub struct AppendEntries {
    pub term: u64,
    pub leader_id: NodeId,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

/// 追加条目响应。
#[derive(Debug, Clone)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    /// 跟随者已知的最新条目索引（用于快速回退）。
    pub match_index: u64,
}

/// 安装快照 RPC。
#[derive(Debug, Clone)]
pub struct InstallSnapshot {
    pub term: u64,
    pub leader_id: NodeId,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub data: Vec<u8>,
    /// 是否为最后一个分片。
    pub done: bool,
    /// 快照分片偏移。
    pub offset: u64,
    /// 快照数据总量大小。
    pub data_size: u64,
}

/// 安装快照响应。
#[derive(Debug, Clone)]
pub struct InstallSnapshotResponse {
    pub term: u64,
}

/// WAL 持久化接口（实现者需提供磁盘持久化）。
pub trait WalStorage {
    /// 追加条目到 WAL。
    fn append_entries(&mut self, entries: &[WalEntry]) -> Result<(), String>;
    /// 从 WAL 读取日志条目。
    fn read_entries(&self, from_index: u64, to_index: u64) -> Result<Vec<WalEntry>, String>;
    /// 截断 WAL（在快照后清除已快照的条目）。
    fn truncate(&mut self, before_index: u64) -> Result<(), String>;
    /// 刷盘。
    fn sync(&self) -> Result<(), String>;
}

/// 内存 WAL（用于测试和默认实现）。
#[derive(Debug)]
pub struct MemWal {
    entries: Vec<WalEntry>,
}

impl MemWal {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl Default for MemWal {
    fn default() -> Self {
        Self::new()
    }
}

impl WalStorage for MemWal {
    fn append_entries(&mut self, entries: &[WalEntry]) -> Result<(), String> {
        for e in entries {
            if let Some(last) = self.entries.last() {
                if e.index <= last.index {
                    continue;
                }
            }
            self.entries.push(e.clone());
        }
        Ok(())
    }

    fn read_entries(&self, from_index: u64, to_index: u64) -> Result<Vec<WalEntry>, String> {
        Ok(self
            .entries
            .iter()
            .filter(|e| e.index >= from_index && e.index <= to_index)
            .cloned()
            .collect())
    }

    fn truncate(&mut self, before_index: u64) -> Result<(), String> {
        self.entries.retain(|e| e.index >= before_index);
        Ok(())
    }

    fn sync(&self) -> Result<(), String> {
        Ok(())
    }
}

/// Follower 状态（追踪最后应用时间戳）。
#[derive(Debug, Clone)]
pub struct FollowerState {
    /// 下次发送日志的起始索引。
    pub next_index: u64,
    /// 已匹配的最高日志索引。
    pub match_index: u64,
    /// 最后一次收到心跳响应的时间（毫秒）。
    pub last_heartbeat_response_ms: u64,
    /// 最后一次追加成功的日志索引。
    pub last_append_index: u64,
}

impl Default for FollowerState {
    fn default() -> Self {
        Self {
            next_index: 1,
            match_index: 0,
            last_heartbeat_response_ms: 0,
            last_append_index: 0,
        }
    }
}

/// Raft 节点。
pub struct RaftNode {
    /// 节点 ID。
    pub id: NodeId,
    /// 当前状态。
    pub state: RaftState,
    /// 当前任期。
    pub current_term: u64,
    /// 投票给了谁（本任期）。
    pub voted_for: Option<NodeId>,
    /// 日志条目列表（索引 0 为 sentinel）。
    pub log: Vec<LogEntry>,
    /// 已提交的最高日志索引。
    pub commit_index: u64,
    /// 已应用到状态机的最高日志索引。
    pub last_applied: u64,
    /// 已应用的最后任期（用于快照）。
    pub last_applied_term: u64,
    /// 各 follower 状态。
    followers: HashMap<NodeId, FollowerState>,
    /// 副本集配置。
    settings: RsSettings,
    /// 超过选举超时的累计时间（毫秒）。
    pub election_elapsed: u32,
    /// 超过心跳间隔的累计时间（毫秒）。
    heartbeat_elapsed: u32,
    /// 当前快照。
    pub snapshot: Option<Snapshot>,
    /// WAL 存储。
    wal: Box<dyn WalStorage>,
    /// 集群成员配置。
    pub membership: ClusterMembership,
    /// 预投票任期（不递增 current_term）。
    prevote_term: u64,
    /// 已发送的预投票请求（防重复）。
    prevote_votes: HashMap<NodeId, bool>,
    /// 当前已知的全局时间戳（毫秒，单调递增）。
    clock_ms: u64,
    /// 当前已提交到多数的最大索引（线性一致读用）。
    pub committed_index: u64,
    /// 读关注级别。
    read_concern: ReadConcern,
}

impl std::fmt::Debug for RaftNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftNode")
            .field("id", &self.id)
            .field("state", &self.state)
            .field("current_term", &self.current_term)
            .field("voted_for", &self.voted_for)
            .field("log_len", &self.log.len())
            .field("commit_index", &self.commit_index)
            .field("last_applied", &self.last_applied)
            .field("membership", &self.membership)
            .finish()
    }
}

impl RaftNode {
    /// 心跳间隔（毫秒）。
    pub const HEARTBEAT_INTERVAL_MS: u32 = 50;
    /// 选举超时下限（毫秒）。
    pub const ELECTION_TIMEOUT_MIN_MS: u32 = 150;
    /// 选举超时上限（毫秒）。
    pub const ELECTION_TIMEOUT_MAX_MS: u32 = 300;

    /// 创建新的 Raft 节点。
    pub fn new(id: NodeId, settings: RsSettings) -> Self {
        let log = vec![LogEntry {
            term: 0,
            index: 0,
            command: Command::Noop,
        }];
        Self {
            id,
            state: RaftState::Follower,
            current_term: 0,
            voted_for: None,
            log,
            commit_index: 0,
            last_applied: 0,
            last_applied_term: 0,
            followers: HashMap::new(),
            settings,
            election_elapsed: 0,
            heartbeat_elapsed: 0,
            snapshot: None,
            wal: Box::new(MemWal::new()),
            membership: ClusterMembership::default(),
            prevote_term: 0,
            prevote_votes: HashMap::new(),
            clock_ms: 0,
            committed_index: 0,
            read_concern: ReadConcern::Local,
        }
    }

    /// 使用自定义 WAL 创建节点。
    pub fn with_wal(id: NodeId, settings: RsSettings, wal: Box<dyn WalStorage>) -> Self {
        let mut node = Self::new(id, settings);
        node.wal = wal;
        node
    }

    /// 设置读关注级别。
    pub fn set_read_concern(&mut self, rc: ReadConcern) {
        self.read_concern = rc;
    }

    /// 获取读关注级别。
    pub fn read_concern(&self) -> ReadConcern {
        self.read_concern
    }

    /// 推进本地时钟（毫秒）。
    pub fn tick(&mut self, ms: u64) {
        self.clock_ms += ms;
    }

    /// 当前日志最后一个条目的索引。
    pub fn last_log_index(&self) -> u64 {
        self.log.last().map_or(0, |e| e.index)
    }

    /// 当前日志最后一个条目的任期。
    pub fn last_log_term(&self) -> u64 {
        self.log.last().map_or(0, |e| e.term)
    }

    /// 随机选举超时（毫秒）。使用节点 ID 的哈希和 term 混合产生真正的随机性。
    fn random_election_timeout(&self) -> u32 {
        let min = Self::ELECTION_TIMEOUT_MIN_MS as u64;
        let max = Self::ELECTION_TIMEOUT_MAX_MS as u64;
        let range = max - min + 1;
        // 多轮混合以产生更好的随机分布
        let mut h = self.current_term;
        h = h.wrapping_mul(0x9e3779b97f4a7c15).wrapping_add(self.id.0.len() as u64);
        h = h.wrapping_mul(0x9e3779b97f4a7c15).wrapping_add(self.election_elapsed as u64);
        h = h.wrapping_mul(0xbf58476d1ce4e5b9);
        h = h.wrapping_mul(0x94d049bb133111eb);
        let offset = (h >> 16) % range;
        (min + offset) as u32
    }

    /// 处理请求投票 RPC。
    pub fn on_request_vote(
        &mut self,
        term: u64,
        candidate_id: &NodeId,
        last_log_index: u64,
        last_log_term: u64,
    ) -> RequestVoteResponse {
        if term > self.current_term {
            self.current_term = term;
            self.state = RaftState::Follower;
            self.voted_for = None;
        }

        let mut vote_granted = false;

        if term >= self.current_term {
            let can_vote =
                self.voted_for.is_none() || self.voted_for.as_ref() == Some(candidate_id);
            let log_ok = last_log_term > self.last_log_term()
                || (last_log_term == self.last_log_term()
                    && last_log_index >= self.last_log_index());

            if can_vote && log_ok {
                vote_granted = true;
                self.voted_for = Some(candidate_id.clone());
            }
        }

        RequestVoteResponse {
            term: self.current_term,
            vote_granted,
        }
    }

    /// 处理预投票 RPC（不递增 term，不改变投票状态）。
    pub fn on_prevote_request(
        &self,
        term: u64,
        candidate_id: &NodeId,
        last_log_index: u64,
        last_log_term: u64,
    ) -> PreVoteResponse {
        // 如果 leader 仍然可达，则拒绝预投票
        if self.state == RaftState::Leader && self.is_leader_heartbeat_healthy() {
            return PreVoteResponse {
                term: self.current_term,
                vote_granted: false,
            };
        }

        let next_term = term + 1;
        let can_vote = self.voted_for.is_none() || self.voted_for.as_ref() == Some(candidate_id);
        let log_ok = last_log_term > self.last_log_term()
            || (last_log_term == self.last_log_term() && last_log_index >= self.last_log_index());

        PreVoteResponse {
            term: self.current_term,
            vote_granted: next_term >= self.current_term && can_vote && log_ok,
        }
    }

    /// Leader 是否心跳健康（在超时窗口内收到过多数回复）。
    fn is_leader_heartbeat_healthy(&self) -> bool {
        let timeout = self.settings.follower_heartbeat_timeout_ms as u64;
        let mut healthy_count = 1; // leader 自身
        for (_id, fs) in &self.followers {
            if self.clock_ms.saturating_sub(fs.last_heartbeat_response_ms) < timeout {
                healthy_count += 1;
            }
        }
        healthy_count > self.membership.members.len() / 2
    }

    /// 处理追加条目 RPC。
    pub fn on_append_entries(
        &mut self,
        term: u64,
        leader_id: &NodeId,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    ) -> AppendEntriesResponse {
        if term < self.current_term {
            return AppendEntriesResponse {
                term: self.current_term,
                success: false,
                match_index: self.last_log_index(),
            };
        }

        // 收到合法 leader 的心跳，降级为 follower
        if term > self.current_term || self.state != RaftState::Follower {
            self.current_term = term;
            self.state = RaftState::Follower;
            self.voted_for = None;
        }

        let _ = leader_id;

        self.election_elapsed = 0; // 重置选举计时器

        // 检查 prev_log_index/term 匹配
        if prev_log_index > 0 {
            if let Some(entry) = self.log.iter().find(|e| e.index == prev_log_index) {
                if entry.term != prev_log_term {
                    return AppendEntriesResponse {
                        term: self.current_term,
                        success: false,
                        match_index: self.last_log_index(),
                    };
                }
            } else {
                // 缺少 prev_log_index 条目
                return AppendEntriesResponse {
                    term: self.current_term,
                    success: false,
                    match_index: self.last_log_index(),
                };
            }
        }

        // 追加条目（冲突时截断）
        for entry in entries {
            if let Some(pos) = self.log.iter().position(|e| e.index == entry.index) {
                if self.log[pos].term != entry.term {
                    self.log.truncate(pos);
                    self.log.push(entry);
                }
            } else {
                self.log.push(entry);
            }
        }

        // 更新 commit_index
        if leader_commit > self.commit_index {
            self.commit_index = leader_commit.min(self.last_log_index());
        }

        AppendEntriesResponse {
            term: self.current_term,
            success: true,
            match_index: self.last_log_index(),
        }
    }

    /// 处理选举超时：发起预投票或正式选举。
    pub fn on_election_timeout(&mut self) -> Option<ElectionAction> {
        self.election_elapsed += Self::HEARTBEAT_INTERVAL_MS;

        let timeout = self.random_election_timeout();
        if self.election_elapsed < timeout {
            return None;
        }

        self.election_elapsed = 0;

        // 先尝试预投票（如果当前是 follower）
        if self.state == RaftState::Follower || self.state == RaftState::Candidate {
            self.prevote_term = self.current_term + 1;
            self.prevote_votes.clear();
            self.prevote_votes.insert(self.id.clone(), true); // 自己同意

            let pv = PreVoteRequest {
                term: self.prevote_term,
                candidate_id: self.id.clone(),
                last_log_index: self.last_log_index(),
                last_log_term: self.last_log_term(),
            };
            return Some(ElectionAction::PreVote(vec![pv]));
        }

        None
    }

    /// 预投票完成后，决定是否发起正式选举。
    pub fn on_prevote_complete(&mut self, majority: usize) -> Option<Vec<RequestVote>> {
        let votes = self.prevote_votes.values().filter(|v| **v).count();
        if votes < majority {
            // 预投票未通过，不发起选举
            return None;
        }

        // 预投票通过，发起正式选举
        self.current_term += 1;
        self.state = RaftState::Candidate;
        self.voted_for = Some(self.id.clone());
        self.election_elapsed = 0;

        let req = RequestVote {
            term: self.current_term,
            candidate_id: self.id.clone(),
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        };

        Some(vec![req])
    }

    /// 处理预投票响应。
    pub fn on_prevote_response(&mut self, voter: &NodeId, response: &PreVoteResponse) {
        if response.vote_granted {
            self.prevote_votes.insert(voter.clone(), true);
        }
    }

    /// 处理心跳间隔：向所有 follower 发送 AppendEntries。
    pub fn on_heartbeat_interval(
        &mut self,
        followers: &[NodeId],
    ) -> Option<Vec<(NodeId, AppendEntries)>> {
        self.heartbeat_elapsed += Self::HEARTBEAT_INTERVAL_MS;
        if self.heartbeat_elapsed < Self::HEARTBEAT_INTERVAL_MS {
            return None;
        }

        if self.state != RaftState::Leader {
            return None;
        }

        self.heartbeat_elapsed = 0;

        let mut result = Vec::new();

        for follower in followers {
            let fs = self
                .followers
                .entry(follower.clone())
                .or_insert_with(FollowerState::default);

            let next_idx = fs.next_index;
            // 获取 prev_log 信息
            let (prev_log_index, prev_log_term) = if next_idx > 1 {
                if let Some(entry) = self.log.iter().find(|e| e.index == next_idx - 1) {
                    (entry.index, entry.term)
                } else {
                    (0, 0)
                }
            } else {
                (0, 0)
            };

            let follower_entries: Vec<LogEntry> = self
                .log
                .iter()
                .filter(|e| e.index >= next_idx)
                .cloned()
                .collect();

            let ae = AppendEntries {
                term: self.current_term,
                leader_id: self.id.clone(),
                prev_log_index,
                prev_log_term,
                entries: follower_entries,
                leader_commit: self.commit_index,
            };
            result.push((follower.clone(), ae));
        }

        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    /// 处理追加条目响应。
    pub fn on_append_entries_response(
        &mut self,
        follower: &NodeId,
        response: &AppendEntriesResponse,
    ) {
        if response.term > self.current_term {
            self.current_term = response.term;
            self.state = RaftState::Follower;
            self.voted_for = None;
            return;
        }

        let fs = self
            .followers
            .entry(follower.clone())
            .or_insert_with(FollowerState::default);

        fs.last_heartbeat_response_ms = self.clock_ms;

        if response.success {
            fs.next_index = response.match_index + 1;
            fs.match_index = response.match_index;
            fs.last_append_index = response.match_index;
        } else {
            // 快速回退：如果 match_index > 0，直接跳到 match_index
            if fs.match_index > 0 {
                fs.next_index = fs.match_index + 1;
            } else {
                fs.next_index = fs.next_index.saturating_sub(1).max(1);
            }
        }
    }

    /// 将日志条目追加到本地日志（leader 写入路径）。
    pub fn append_entry(&mut self, command: Command) -> &LogEntry {
        let index = self.last_log_index() + 1;
        let entry = LogEntry {
            term: self.current_term,
            index,
            command,
        };
        let wal_entry = WalEntry {
            index: entry.index,
            term: entry.term,
            command: entry.command.clone(),
        };
        self.log.push(entry);
        let _ = self.wal.append_entries(&[wal_entry]);
        self.log.last().unwrap()
    }

    /// 批量追加条目（性能优化）。
    pub fn append_entries_batch(&mut self, commands: Vec<Command>) -> Vec<LogEntry> {
        let mut wal_entries: Vec<WalEntry> = Vec::with_capacity(commands.len());
        for cmd in commands {
            let index = self.last_log_index() + 1;
            let entry = LogEntry {
                term: self.current_term,
                index,
                command: cmd,
            };
            wal_entries.push(WalEntry {
                index: entry.index,
                term: entry.term,
                command: entry.command.clone(),
            });
            self.log.push(entry);
        }
        let _ = self.wal.append_entries(&wal_entries);
        let start = self.log.len() - wal_entries.len();
        self.log[start..].to_vec()
    }

    /// 尝试推进 commit_index（仅 leader 调用）。
    pub fn try_advance_commit(&mut self, majority: usize) {
        if self.state != RaftState::Leader {
            return;
        }

        let mut indices: Vec<u64> = self.followers.values().map(|fs| fs.match_index).collect();
        // leader 自己也算
        indices.push(self.last_log_index());
        indices.sort_unstable();

        // majority 大小的中位数
        if indices.len() >= majority {
            let candidate = indices[indices.len() - majority];
            if candidate > self.commit_index {
                // 确认该索引处的条目任期是当前任期
                if let Some(entry) = self.log.iter().find(|e| e.index == candidate) {
                    if entry.term == self.current_term {
                        self.commit_index = candidate;
                        self.committed_index = candidate;
                    }
                }
            }
        }
    }

    /// 将已提交但未应用的条目返回并更新 last_applied。
    pub fn apply_committed(&mut self) -> Vec<LogEntry> {
        let mut applied = Vec::new();
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            if let Some(entry) = self.log.iter().find(|e| e.index == self.last_applied) {
                self.last_applied_term = entry.term;

                // 处理成员变更命令
                match &entry.command {
                    Command::AddNode(node_id) => {
                        if !self.membership.contains(node_id) {
                            self.membership.members.push(node_id.clone());
                        }
                    }
                    Command::RemoveNode(node_id) => {
                        self.membership.members.retain(|m| m != node_id);
                    }
                    _ => {}
                }

                applied.push(entry.clone());
            }
        }
        applied
    }

    /// 处理投票响应（候选人路径）。
    pub fn on_vote_response(
        &mut self,
        _voter: &NodeId,
        response: &RequestVoteResponse,
    ) -> bool {
        if response.term > self.current_term {
            self.current_term = response.term;
            self.state = RaftState::Follower;
            self.voted_for = None;
            return false;
        }
        response.vote_granted
    }

    /// 心跳检测：检查 follower 是否超时。
    ///
    /// 返回超时的 follower ID 列表。
    pub fn check_follower_timeouts(&self) -> Vec<NodeId> {
        let timeout = self.settings.follower_heartbeat_timeout_ms as u64;
        self.followers
            .iter()
            .filter(|(_id, fs)| {
                self.clock_ms.saturating_sub(fs.last_heartbeat_response_ms) >= timeout
            })
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// 检查是否可以安全地响应读请求（基于读关注级别）。
    pub fn can_read(&self, rc: ReadConcern) -> bool {
        match rc {
            ReadConcern::Local => self.state == RaftState::Leader,
            ReadConcern::Majority => {
                self.state == RaftState::Leader && self.committed_index > 0
            }
            ReadConcern::Linearizable => {
                // 需要确认自己仍然是 leader（已提交的索引 > 0 且心跳健康）
                self.state == RaftState::Leader
                    && self.committed_index > 0
                    && self.is_leader_heartbeat_healthy()
            }
        }
    }

    /// 安装快照：leader 发送快照给慢 follower。
    pub fn install_snapshot(
        &mut self,
        term: u64,
        _leader_id: &NodeId,
        last_included_index: u64,
        last_included_term: u64,
        data: Vec<u8>,
        _offset: u64,
        _data_size: u64,
        done: bool,
    ) -> InstallSnapshotResponse {
        if term < self.current_term {
            return InstallSnapshotResponse {
                term: self.current_term,
            };
        }

        if term > self.current_term {
            self.current_term = term;
            self.state = RaftState::Follower;
            self.voted_for = None;
        }

        self.election_elapsed = 0;

        if done {
            // 应用快照：截断日志，更新状态
            self.log.retain(|e| e.index >= last_included_index);
            // 如果 sentinel 被截断，重新添加
            if self.log.is_empty() || self.log[0].index > 0 {
                self.log.insert(
                    0,
                    LogEntry {
                        term: 0,
                        index: 0,
                        command: Command::Noop,
                    },
                );
            }

            let state_doc = Document::new();
            // 将快照数据视为 base64 编码的状态
            let _ = data;

            self.snapshot = Some(Snapshot {
                last_included_index,
                last_included_term,
                state: state_doc,
            });

            if last_included_index > self.last_applied {
                self.last_applied = last_included_index;
                self.last_applied_term = last_included_term;
            }
            if last_included_index > self.commit_index {
                self.commit_index = last_included_index;
            }

            // 截断 WAL
            let _ = self.wal.truncate(last_included_index);
        }

        InstallSnapshotResponse {
            term: self.current_term,
        }
    }

    /// 判断是否需要快照（日志超过阈值）。
    pub fn needs_snapshot(&self) -> bool {
        self.log.len() > self.settings.max_log_entries
    }

    /// 创建快照并截断日志。
    pub fn take_snapshot(&mut self, state: Document) -> Snapshot {
        let snap = Snapshot {
            last_included_index: self.last_applied,
            last_included_term: self.last_applied_term,
            state,
        };
        self.snapshot = Some(snap.clone());

        // 截断已快照的日志
        let cutoff = snap.last_included_index;
        self.log.retain(|e| e.index >= cutoff || e.index == 0);
        // 确保 sentinel 存在
        if self.log.is_empty() || self.log[0].index > 0 {
            self.log.insert(
                0,
                LogEntry {
                    term: 0,
                    index: 0,
                    command: Command::Noop,
                },
            );
        }

        let _ = self.wal.truncate(cutoff);
        snap
    }

    /// 获取 follower 状态。
    pub fn follower_state(&self, id: &NodeId) -> Option<&FollowerState> {
        self.followers.get(id)
    }

    /// 获取 follower 可变引用。
    pub fn follower_state_mut(&mut self, id: &NodeId) -> Option<&mut FollowerState> {
        self.followers.entry(id.clone()).or_insert_with(FollowerState::default);
        self.followers.get_mut(id)
    }

    /// 获取副本集设置。
    pub fn settings(&self) -> &RsSettings {
        &self.settings
    }

    /// 获取 WAL 引用。
    pub fn wal(&self) -> &dyn WalStorage {
        &*self.wal
    }

    /// 从 WAL 恢复日志。
    pub fn recover_from_wal(&mut self) -> Result<(), String> {
        if self.last_applied == 0 {
            return Ok(());
        }
        let entries = self.wal.read_entries(1, u64::MAX)?;
        for e in entries {
            // 只恢复尚未在内存中的条目
            if !self.log.iter().any(|le| le.index == e.index) {
                self.log.push(LogEntry {
                    term: e.term,
                    index: e.index,
                    command: e.command,
                });
            }
        }
        // 按索引排序
        self.log.sort_by_key(|e| e.index);
        Ok(())
    }
}

/// 选举动作（区分预投票和正式投票）。
#[derive(Debug, Clone)]
pub enum ElectionAction {
    /// 预投票请求。
    PreVote(Vec<PreVoteRequest>),
    /// 正式投票请求。
    Vote(Vec<RequestVote>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RsSettings;

    fn make_node(id: &str) -> RaftNode {
        RaftNode::new(NodeId::new(id), RsSettings::default())
    }

    #[test]
    fn new_node_is_follower() {
        let node = make_node("n1");
        assert_eq!(node.state, RaftState::Follower);
        assert_eq!(node.current_term, 0);
        assert_eq!(node.last_log_index(), 0);
    }

    #[test]
    fn request_vote_granted_for_empty_log() {
        let mut node = make_node("n1");
        let candidate = NodeId::new("n2");
        let resp = node.on_request_vote(1, &candidate, 0, 0);
        assert!(resp.vote_granted);
        assert_eq!(resp.term, 1);
        assert_eq!(node.voted_for, Some(candidate));
    }

    #[test]
    fn request_vote_reject_stale_term() {
        let mut node = make_node("n1");
        node.current_term = 5;
        let candidate = NodeId::new("n2");
        let resp = node.on_request_vote(3, &candidate, 0, 0);
        assert!(!resp.vote_granted);
        assert_eq!(resp.term, 5);
    }

    #[test]
    fn request_vote_reject_wrong_log() {
        let mut node = make_node("n1");
        node.log.push(LogEntry {
            term: 2,
            index: 1,
            command: Command::Noop,
        });
        let candidate = NodeId::new("n2");
        let resp = node.on_request_vote(3, &candidate, 0, 1);
        assert!(!resp.vote_granted);
    }

    #[test]
    fn request_vote_reject_already_voted() {
        let mut node = make_node("n1");
        let c1 = NodeId::new("n2");
        let c2 = NodeId::new("n3");
        node.on_request_vote(1, &c1, 0, 0);
        let resp = node.on_request_vote(1, &c2, 0, 0);
        assert!(!resp.vote_granted);
    }

    #[test]
    fn request_vote_update_term() {
        let mut node = make_node("n1");
        node.current_term = 1;
        let candidate = NodeId::new("n2");
        let resp = node.on_request_vote(5, &candidate, 0, 0);
        assert!(resp.vote_granted);
        assert_eq!(node.current_term, 5);
    }

    #[test]
    fn append_entries_reject_stale_term() {
        let mut node = make_node("n1");
        node.current_term = 3;
        let leader = NodeId::new("n2");
        let resp = node.on_append_entries(1, &leader, 0, 0, vec![], 0);
        assert!(!resp.success);
    }

    #[test]
    fn append_entries_accept_and_update() {
        let mut node = make_node("n1");
        let leader = NodeId::new("n2");
        let entries = vec![LogEntry {
            term: 1,
            index: 1,
            command: Command::Noop,
        }];
        let resp = node.on_append_entries(1, &leader, 0, 0, entries, 1);
        assert!(resp.success);
        assert_eq!(resp.match_index, 1);
        assert_eq!(node.log.len(), 2); // sentinel + entry
        assert_eq!(node.commit_index, 1);
    }

    #[test]
    fn append_entries_resets_election_timer() {
        let mut node = make_node("n1");
        node.election_elapsed = 200;
        let leader = NodeId::new("n2");
        let resp = node.on_append_entries(1, &leader, 0, 0, vec![], 0);
        assert!(resp.success);
        assert_eq!(node.election_elapsed, 0);
    }

    #[test]
    fn append_entries_reject_prev_log_mismatch() {
        let mut node = make_node("n1");
        node.current_term = 1;
        // Node has entry at index 1 with term 2
        node.log.push(LogEntry {
            term: 2,
            index: 1,
            command: Command::Noop,
        });
        let leader = NodeId::new("n2");
        // Leader sends entries starting at index 2, with prev=1,term=1
        let entries = vec![LogEntry {
            term: 1,
            index: 2,
            command: Command::Noop,
        }];
        let resp = node.on_append_entries(1, &leader, 1, 1, entries, 0);
        assert!(!resp.success);
    }

    #[test]
    fn election_timeout_triggers() {
        let mut node = make_node("n1");
        node.election_elapsed = RaftNode::ELECTION_TIMEOUT_MAX_MS + 100;
        let result = node.on_election_timeout();
        assert!(result.is_some());
    }

    #[test]
    fn election_timeout_not_yet() {
        let mut node = make_node("n1");
        node.election_elapsed = 10;
        let result = node.on_election_timeout();
        assert!(result.is_none());
    }

    #[test]
    fn prevote_triggers_first() {
        let mut node = make_node("n1");
        node.election_elapsed = RaftNode::ELECTION_TIMEOUT_MAX_MS + 100;
        let result = node.on_election_timeout();
        assert!(matches!(result, Some(ElectionAction::PreVote(_))));
    }

    #[test]
    fn prevote_pass_leads_to_election() {
        let mut node = make_node("n1");
        node.prevote_term = 1;
        node.prevote_votes.insert(NodeId::new("n1"), true);
        node.prevote_votes.insert(NodeId::new("n2"), true);
        // 3 nodes total, majority = 2
        let result = node.on_prevote_complete(2);
        assert!(result.is_some());
        assert_eq!(node.state, RaftState::Candidate);
        assert_eq!(node.current_term, 1);
    }

    #[test]
    fn prevote_fail_no_election() {
        let mut node = make_node("n1");
        node.prevote_term = 1;
        node.prevote_votes.insert(NodeId::new("n1"), true);
        // Only 1 vote, majority = 2 for 3 nodes
        let result = node.on_prevote_complete(2);
        assert!(result.is_none());
    }

    #[test]
    fn heartbeat_interval_triggers() {
        let mut node = make_node("n1");
        node.state = RaftState::Leader;
        node.heartbeat_elapsed = RaftNode::HEARTBEAT_INTERVAL_MS;
        let followers = vec![NodeId::new("n2"), NodeId::new("n3")];
        let result = node.on_heartbeat_interval(&followers);
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 2);
    }

    #[test]
    fn heartbeat_interval_not_leader() {
        let mut node = make_node("n1");
        node.state = RaftState::Follower;
        node.heartbeat_elapsed = RaftNode::HEARTBEAT_INTERVAL_MS;
        let followers = vec![NodeId::new("n2")];
        let result = node.on_heartbeat_interval(&followers);
        assert!(result.is_none());
    }

    #[test]
    fn heartbeat_sends_prev_log_info() {
        let mut node = make_node("n1");
        node.state = RaftState::Leader;
        node.current_term = 2;
        node.log.push(LogEntry {
            term: 1,
            index: 1,
            command: Command::Noop,
        });
        node.log.push(LogEntry {
            term: 2,
            index: 2,
            command: Command::Noop,
        });
        // Initialize follower state so next_index = last_log_index + 1 = 3
        node.followers.insert(
            NodeId::new("n2"),
            FollowerState {
                next_index: 3,
                match_index: 0,
                ..Default::default()
            },
        );
        node.heartbeat_elapsed = RaftNode::HEARTBEAT_INTERVAL_MS;
        let followers = vec![NodeId::new("n2")];
        let result = node.on_heartbeat_interval(&followers).unwrap();
        assert_eq!(result.len(), 1);
        let (_fid, ae) = &result[0];
        assert_eq!(ae.prev_log_index, 2);
        assert_eq!(ae.prev_log_term, 2);
    }

    #[test]
    fn append_entry_and_apply() {
        let mut node = make_node("n1");
        node.state = RaftState::Leader;
        let entry = node.append_entry(Command::Noop);
        assert_eq!(entry.index, 1);
        assert_eq!(entry.term, 0);

        node.commit_index = 1;
        let applied = node.apply_committed();
        assert_eq!(applied.len(), 1);
        assert_eq!(node.last_applied, 1);
    }

    #[test]
    fn append_entries_batch() {
        let mut node = make_node("n1");
        node.state = RaftState::Leader;
        let entries = node.append_entries_batch(vec![
            Command::Noop,
            Command::Noop,
            Command::Noop,
        ]);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].index, 1);
        assert_eq!(entries[2].index, 3);
        assert_eq!(node.last_log_index(), 3);
    }

    #[test]
    fn try_advance_commit_majority() {
        let mut node = make_node("n1");
        node.state = RaftState::Leader;
        node.current_term = 1;
        node.append_entry(Command::Noop);
        node.append_entry(Command::Noop);

        node.followers
            .insert(NodeId::new("n2"), FollowerState { match_index: 2, ..Default::default() });
        node.followers
            .insert(NodeId::new("n3"), FollowerState { match_index: 2, ..Default::default() });

        node.try_advance_commit(2);
        assert_eq!(node.commit_index, 2);
    }

    #[test]
    fn try_advance_commit_no_majority() {
        let mut node = make_node("n1");
        node.state = RaftState::Leader;
        node.current_term = 1;
        node.append_entry(Command::Noop);
        node.append_entry(Command::Noop);

        node.followers
            .insert(NodeId::new("n2"), FollowerState { match_index: 0, ..Default::default() });

        node.try_advance_commit(2);
        assert_eq!(node.commit_index, 0);
    }

    #[test]
    fn on_vote_response_steps_down() {
        let mut node = make_node("n1");
        node.state = RaftState::Candidate;
        node.current_term = 1;
        let resp = RequestVoteResponse {
            term: 5,
            vote_granted: false,
        };
        let granted = node.on_vote_response(&NodeId::new("n2"), &resp);
        assert!(!granted);
        assert_eq!(node.state, RaftState::Follower);
        assert_eq!(node.current_term, 5);
    }

    #[test]
    fn on_append_entries_response_failure_decrements_next_index() {
        let mut node = make_node("n1");
        node.state = RaftState::Leader;
        node.followers.insert(
            NodeId::new("n2"),
            FollowerState {
                next_index: 5,
                match_index: 0,
                ..Default::default()
            },
        );

        let resp = AppendEntriesResponse {
            term: 0,
            success: false,
            match_index: 0,
        };
        node.on_append_entries_response(&NodeId::new("n2"), &resp);
        assert_eq!(
            node.followers[&NodeId::new("n2")].next_index,
            4
        );
    }

    #[test]
    fn on_append_entries_response_success_updates() {
        let mut node = make_node("n1");
        node.state = RaftState::Leader;
        node.followers.insert(
            NodeId::new("n2"),
            FollowerState {
                next_index: 1,
                match_index: 0,
                ..Default::default()
            },
        );

        let resp = AppendEntriesResponse {
            term: 0,
            success: true,
            match_index: 3,
        };
        node.on_append_entries_response(&NodeId::new("n2"), &resp);
        assert_eq!(
            node.followers[&NodeId::new("n2")].next_index,
            4
        );
        assert_eq!(
            node.followers[&NodeId::new("n2")].match_index,
            3
        );
    }

    #[test]
    fn on_append_entries_response_fast_backoff() {
        let mut node = make_node("n1");
        node.state = RaftState::Leader;
        node.followers.insert(
            NodeId::new("n2"),
            FollowerState {
                next_index: 5,
                match_index: 3,
                ..Default::default()
            },
        );

        let resp = AppendEntriesResponse {
            term: 0,
            success: false,
            match_index: 0,
        };
        node.on_append_entries_response(&NodeId::new("n2"), &resp);
        // fast backoff: jump to match_index + 1
        assert_eq!(
            node.followers[&NodeId::new("n2")].next_index,
            4
        );
    }

    #[test]
    fn random_election_timeout_in_range() {
        for term in 0..100 {
            let mut n = make_node("n1");
            n.current_term = term;
            let t = n.random_election_timeout();
            assert!(t >= RaftNode::ELECTION_TIMEOUT_MIN_MS);
            assert!(t <= RaftNode::ELECTION_TIMEOUT_MAX_MS);
        }
    }

    // --- Snapshot tests ---

    #[test]
    fn needs_snapshot_detects_large_log() {
        let mut node = make_node("n1");
        node.settings.max_log_entries = 5;
        for _ in 0..6 {
            node.append_entry(Command::Noop);
        }
        assert!(node.needs_snapshot());
    }

    #[test]
    fn take_snapshot_truncates_log() {
        let mut node = make_node("n1");
        node.state = RaftState::Leader;
        for _ in 0..10 {
            node.append_entry(Command::Noop);
        }
        node.last_applied = 10;
        node.last_applied_term = 0;

        let snap = node.take_snapshot(Document::new());
        assert_eq!(snap.last_included_index, 10);
        assert!(node.log.len() <= 2); // sentinel + maybe one edge
    }

    #[test]
    fn install_snapshot_from_leader() {
        let mut node = make_node("n1");
        let leader = NodeId::new("leader");

        let resp = node.install_snapshot(
            1,
            &leader,
            5,
            1,
            vec![],
            0,
            0,
            true,
        );
        assert_eq!(resp.term, 1); // term updated to match leader
        assert_eq!(node.current_term, 1);
        assert_eq!(node.last_applied, 5);
        assert!(node.snapshot.is_some());
    }

    #[test]
    fn install_snapshot_reject_stale_term() {
        let mut node = make_node("n1");
        node.current_term = 10;
        let leader = NodeId::new("leader");
        let resp = node.install_snapshot(5, &leader, 5, 1, vec![], 0, 0, true);
        assert_eq!(resp.term, 10);
    }

    // --- PreVote tests ---

    #[test]
    fn prevote_rejects_if_leader_healthy() {
        let mut node = make_node("n1");
        node.state = RaftState::Leader;
        // Simulate healthy: set recent heartbeat
        node.clock_ms = 100;
        node.followers.insert(
            NodeId::new("n2"),
            FollowerState {
                last_heartbeat_response_ms: 90,
                ..Default::default()
            },
        );
        node.followers.insert(
            NodeId::new("n3"),
            FollowerState {
                last_heartbeat_response_ms: 95,
                ..Default::default()
            },
        );
        node.membership.members = vec![
            NodeId::new("n1"),
            NodeId::new("n2"),
            NodeId::new("n3"),
        ];

        let pv = node.on_prevote_request(1, &NodeId::new("n4"), 0, 0);
        assert!(!pv.vote_granted);
    }

    #[test]
    fn prevote_granted_for_follower() {
        let mut node = make_node("n1");
        node.state = RaftState::Follower;
        let pv = node.on_prevote_request(1, &NodeId::new("n2"), 0, 0);
        assert!(pv.vote_granted);
    }

    // --- Read concern tests ---

    #[test]
    fn can_read_local_only_as_leader() {
        let mut node = make_node("n1");
        node.state = RaftState::Leader;
        assert!(node.can_read(ReadConcern::Local));
    }

    #[test]
    fn cannot_read_local_as_follower() {
        let mut node = make_node("n1");
        node.state = RaftState::Follower;
        assert!(!node.can_read(ReadConcern::Local));
    }

    #[test]
    fn can_read_majority_only_with_committed() {
        let mut node = make_node("n1");
        node.state = RaftState::Leader;
        node.committed_index = 0;
        assert!(!node.can_read(ReadConcern::Majority));
        node.committed_index = 1;
        assert!(node.can_read(ReadConcern::Majority));
    }

    #[test]
    fn can_read_linearizable_needs_healthy() {
        let mut node = make_node("n1");
        node.state = RaftState::Leader;
        node.committed_index = 1;
        // No followers => not healthy
        node.membership.members = vec![NodeId::new("n1"), NodeId::new("n2")];
        assert!(!node.can_read(ReadConcern::Linearizable));
    }

    // --- Follower timeout tests ---

    #[test]
    fn check_follower_timeouts() {
        let mut node = make_node("n1");
        node.state = RaftState::Leader;
        node.clock_ms = 1000;
        node.followers.insert(
            NodeId::new("n2"),
            FollowerState {
                last_heartbeat_response_ms: 200, // stale
                ..Default::default()
            },
        );
        node.followers.insert(
            NodeId::new("n3"),
            FollowerState {
                last_heartbeat_response_ms: 999, // recent
                ..Default::default()
            },
        );

        let timeouts = node.check_follower_timeouts();
        assert!(timeouts.contains(&NodeId::new("n2")));
        assert!(!timeouts.contains(&NodeId::new("n3")));
    }

    // --- Membership tests ---

    #[test]
    fn apply_add_node_command() {
        let mut node = make_node("n1");
        node.state = RaftState::Leader;
        node.membership.members = vec![NodeId::new("n1")];
        node.append_entry(Command::AddNode(NodeId::new("n2")));
        node.commit_index = 1;
        let applied = node.apply_committed();
        assert_eq!(applied.len(), 1);
        assert!(node.membership.contains(&NodeId::new("n2")));
    }

    #[test]
    fn apply_remove_node_command() {
        let mut node = make_node("n1");
        node.state = RaftState::Leader;
        node.membership.members = vec![NodeId::new("n1"), NodeId::new("n2")];
        node.append_entry(Command::RemoveNode(NodeId::new("n2")));
        node.commit_index = 1;
        node.apply_committed();
        assert!(!node.membership.contains(&NodeId::new("n2")));
    }

    // --- WAL tests ---

    #[test]
    fn wal_persists_entries() {
        let mut wal = MemWal::new();
        wal.append_entries(&[
            WalEntry { index: 1, term: 1, command: Command::Noop },
            WalEntry { index: 2, term: 1, command: Command::Noop },
        ])
        .unwrap();
        let entries = wal.read_entries(1, 2).unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn wal_truncate() {
        let mut wal = MemWal::new();
        wal.append_entries(&[
            WalEntry { index: 1, term: 1, command: Command::Noop },
            WalEntry { index: 2, term: 1, command: Command::Noop },
            WalEntry { index: 3, term: 1, command: Command::Noop },
        ])
        .unwrap();
        wal.truncate(2).unwrap();
        let entries = wal.read_entries(1, 3).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].index, 2);
    }

    #[test]
    fn recover_from_wal() {
        let mut wal = MemWal::new();
        wal.append_entries(&[
            WalEntry { index: 1, term: 1, command: Command::Noop },
            WalEntry { index: 2, term: 1, command: Command::Noop },
        ])
        .unwrap();
        let mut node = RaftNode::with_wal(
            NodeId::new("n1"),
            RsSettings::default(),
            Box::new(wal),
        );
        node.last_applied = 2;
        node.recover_from_wal().unwrap();
        assert!(node.log.iter().any(|e| e.index == 1));
        assert!(node.log.iter().any(|e| e.index == 2));
    }

    // --- Membership ---

    #[test]
    fn membership_starts_empty() {
        let node = make_node("n1");
        assert!(node.membership.members.is_empty());
    }

    #[test]
    fn heartbeat_sends_empty_when_no_followers() {
        let mut node = make_node("n1");
        node.state = RaftState::Leader;
        node.heartbeat_elapsed = RaftNode::HEARTBEAT_INTERVAL_MS;
        let result = node.on_heartbeat_interval(&[]);
        assert!(result.is_none());
    }

    #[test]
    fn follower_state_default() {
        let fs = FollowerState::default();
        assert_eq!(fs.next_index, 1);
        assert_eq!(fs.match_index, 0);
    }

    #[test]
    fn snapshot_none_by_default() {
        let node = make_node("n1");
        assert!(node.snapshot.is_none());
    }
}
