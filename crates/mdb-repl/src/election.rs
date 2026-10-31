//! Leader 选举管理。

use crate::raft::RequestVote;
use crate::types::NodeId;

/// 选举管理器。
#[derive(Debug)]
pub struct ElectionManager {
    /// 候选人列表（当前参与选举的节点）。
    pub candidates: Vec<NodeId>,
    /// 各候选人的得票数。
    pub votes: std::collections::HashMap<NodeId, u32>,
    /// 当前任期。
    pub term: u64,
    /// 本轮选举发起者。
    pub election_term: u64,
    /// 已投票的节点（防重复计票）。
    pub voted_nodes: std::collections::HashSet<NodeId>,
}

impl ElectionManager {
    /// 创建新的选举管理器。
    pub fn new() -> Self {
        Self {
            candidates: Vec::new(),
            votes: std::collections::HashMap::new(),
            term: 0,
            election_term: 0,
            voted_nodes: std::collections::HashSet::new(),
        }
    }

    /// 开始新一轮选举。
    ///
    /// 返回 `RequestVote` 列表（应发送给所有其他节点）。
    pub fn start_election(
        &mut self,
        term: u64,
        candidate_id: &NodeId,
        last_log_index: u64,
        last_log_term: u64,
    ) -> Vec<RequestVote> {
        self.election_term = term;
        self.term = term;
        self.voted_nodes.clear();
        self.votes.clear();
        self.candidates.clear();

        // candidate 先给自己投一票
        self.votes.insert(candidate_id.clone(), 1);
        self.voted_nodes.insert(candidate_id.clone());

        let req = RequestVote {
            term,
            candidate_id: candidate_id.clone(),
            last_log_index,
            last_log_term,
        };

        vec![req]
    }

    /// 计数投票并判断是否赢得选举。
    ///
    /// 返回 `Some(NodeId)` 表示当选者，`None` 表示选举尚未结束。
    pub fn count_votes(
        &mut self,
        responses: &[(NodeId, bool, u64)],
        total_nodes: usize,
        candidate_id: &NodeId,
    ) -> Option<NodeId> {
        for (voter, granted, resp_term) in responses {
            if *resp_term > self.term {
                // 任期更高者存在，终止选举
                self.term = *resp_term;
                return None;
            }

            if *granted && !self.voted_nodes.contains(voter) {
                self.voted_nodes.insert(voter.clone());
                *self.votes.entry(candidate_id.clone()).or_insert(0) += 1;
            }
        }

        let votes_received = self.votes.get(candidate_id).copied().unwrap_or(0);
        if Self::has_majority(votes_received as usize, total_nodes) {
            Some(candidate_id.clone())
        } else {
            None
        }
    }

    /// 判断是否获得多数票。
    ///
    /// 多数定义：votes > total_nodes / 2。
    pub fn has_majority(votes: usize, total_nodes: usize) -> bool {
        votes > total_nodes / 2
    }

    /// 记录投票拒绝。
    pub fn record_rejection(&mut self, voter: &NodeId) {
        // 不做特别处理，只是确保不重复计票
        self.voted_nodes.insert(voter.clone());
    }

    /// 重置选举状态。
    pub fn reset(&mut self) {
        self.candidates.clear();
        self.votes.clear();
        self.voted_nodes.clear();
    }
}

impl Default for ElectionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// 选举结果。
#[derive(Debug, Clone, PartialEq)]
pub enum ElectionResult {
    /// 选举成功，产生新 leader。
    Winner(NodeId),
    /// 选举失败（票数不足或更高任期）。
    NoWinner,
    /// 需要重新选举。
    SplitVote,
}

/// 运行完整选举流程的辅助函数。
pub fn run_election(
    manager: &mut ElectionManager,
    candidate_id: &NodeId,
    _peers: &[NodeId],
    last_log_index: u64,
    last_log_term: u64,
    _total_nodes: usize,
) -> (ElectionResult, Vec<RequestVote>) {
    let term = manager.term + 1;
    let requests = manager.start_election(term, candidate_id, last_log_index, last_log_term);

    // 模拟：实际网络中 peer 会回复，这里仅返回 requests
    // 投票响应应在外部收集后调用 count_votes
    (ElectionResult::SplitVote, requests)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_manager_is_empty() {
        let mgr = ElectionManager::new();
        assert_eq!(mgr.term, 0);
        assert!(mgr.candidates.is_empty());
        assert!(mgr.votes.is_empty());
    }

    #[test]
    fn start_election_sets_term_and_candidate() {
        let mut mgr = ElectionManager::new();
        let candidate = NodeId::new("n1");
        let reqs = mgr.start_election(1, &candidate, 0, 0);
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0].term, 1);
        assert_eq!(reqs[0].candidate_id, candidate);
        assert_eq!(mgr.term, 1);
    }

    #[test]
    fn count_votes_majority() {
        let mut mgr = ElectionManager::new();
        mgr.start_election(1, &NodeId::new("n1"), 0, 0);

        let responses = vec![
            (NodeId::new("n2"), true, 1),
            (NodeId::new("n3"), true, 1),
        ];

        // 3 nodes, n1 has 3 votes (self + 2), majority = 2
        let result = mgr.count_votes(&responses, 3, &NodeId::new("n1"));
        assert_eq!(result, Some(NodeId::new("n1")));
    }

    #[test]
    fn count_votes_not_enough() {
        let mut mgr = ElectionManager::new();
        mgr.start_election(1, &NodeId::new("n1"), 0, 0);

        let responses = vec![
            (NodeId::new("n2"), false, 1),
            (NodeId::new("n3"), false, 1),
        ];

        let result = mgr.count_votes(&responses, 3, &NodeId::new("n1"));
        assert_eq!(result, None);
    }

    #[test]
    fn count_votes_higher_term_terminates() {
        let mut mgr = ElectionManager::new();
        mgr.start_election(1, &NodeId::new("n1"), 0, 0);

        let responses = vec![(NodeId::new("n2"), false, 5)];

        let result = mgr.count_votes(&responses, 3, &NodeId::new("n1"));
        assert_eq!(result, None);
        assert_eq!(mgr.term, 5);
    }

    #[test]
    fn has_majority_logic() {
        assert!(ElectionManager::has_majority(2, 3)); // 2 > 1
        assert!(!ElectionManager::has_majority(1, 3)); // 1 > 1.5? no
        assert!(ElectionManager::has_majority(3, 5)); // 3 > 2
        assert!(!ElectionManager::has_majority(2, 5)); // 2 > 2.5? no
        assert!(ElectionManager::has_majority(1, 1)); // 1 > 0
    }

    #[test]
    fn reset_clears_state() {
        let mut mgr = ElectionManager::new();
        mgr.start_election(1, &NodeId::new("n1"), 0, 0);
        mgr.reset();
        assert!(mgr.candidates.is_empty());
        assert!(mgr.votes.is_empty());
        assert!(mgr.voted_nodes.is_empty());
    }

    #[test]
    fn record_rejection() {
        let mut mgr = ElectionManager::new();
        mgr.record_rejection(&NodeId::new("n2"));
        assert!(mgr.voted_nodes.contains(&NodeId::new("n2")));
    }

    #[test]
    fn run_election_returns_requests() {
        let mut mgr = ElectionManager::new();
        let candidate = NodeId::new("n1");
        let peers = vec![NodeId::new("n2"), NodeId::new("n3")];
        let (result, requests) = run_election(&mut mgr, &candidate, &peers, 0, 0, 3);
        assert_eq!(result, ElectionResult::SplitVote);
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].candidate_id, candidate);
    }
}
