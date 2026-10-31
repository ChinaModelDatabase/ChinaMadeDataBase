//! 查询执行计划：描述查询如何执行的详细信息。

use mdb_doc::Document;

/// 查询执行计划：完整的 explain 输出。
#[derive(Debug, Clone)]
pub struct ExplainPlan {
    /// 查询计划摘要（"COLLSCAN" 或 "IXSCAN { index: ... }"）。
    pub query_plan: String,
    /// 使用的索引名称（全扫描时为 None）。
    pub index_name: Option<String>,
    /// 返回的文档数。
    pub n_returned: u64,
    /// 扫描的索引键数。
    pub n_scanned: u64,
    /// 扫描的对象数。
    pub n_scanned_objects: u64,
    /// 检查的总索引键数。
    pub total_keys_examined: u64,
    /// 检查的总文档数。
    pub total_docs_examined: u64,
    /// 各阶段详细信息。
    pub stage_details: Vec<ExplainStage>,
    /// 执行时间（毫秒）。
    pub execution_time_ms: u64,
    /// 内存使用（字节）。
    pub memory_usage_bytes: u64,
}

impl ExplainPlan {
    /// 创建全扫描计划。
    pub fn collscan(n_returned: u64, n_scanned_objects: u64) -> Self {
        Self {
            query_plan: "COLLSCAN".into(),
            index_name: None,
            n_returned,
            n_scanned: 0,
            n_scanned_objects,
            total_keys_examined: 0,
            total_docs_examined: n_scanned_objects,
            stage_details: vec![ExplainStage {
                stage: "COLLSCAN".into(),
                n_returned,
                n_scanned: n_scanned_objects,
                index_name: None,
                keys: None,
                input_stages: vec![],
            }],
            execution_time_ms: 0,
            memory_usage_bytes: 0,
        }
    }

    /// 创建索引扫描计划。
    pub fn index_scan(
        index_name: String,
        n_returned: u64,
        n_scanned: u64,
        n_scanned_objects: u64,
    ) -> Self {
        Self {
            query_plan: format!("IXSCAN {{ index: {} }}", index_name),
            index_name: Some(index_name.clone()),
            n_returned,
            n_scanned,
            n_scanned_objects,
            total_keys_examined: n_scanned,
            total_docs_examined: n_scanned_objects,
            stage_details: vec![
                ExplainStage {
                    stage: "IXSCAN".into(),
                    n_returned: n_scanned,
                    n_scanned,
                    index_name: Some(index_name),
                    keys: None,
                    input_stages: vec![],
                },
                ExplainStage {
                    stage: "FETCH".into(),
                    n_returned,
                    n_scanned: n_scanned_objects,
                    index_name: None,
                    keys: None,
                    input_stages: vec![],
                },
            ],
            execution_time_ms: 0,
            memory_usage_bytes: 0,
        }
    }

    /// 创建带排序的计划。
    pub fn with_sort(mut self, n_returned: u64) -> Self {
        self.stage_details.push(ExplainStage {
            stage: "SORT".into(),
            n_returned,
            n_scanned: self.n_returned,
            index_name: None,
            keys: None,
            input_stages: vec![],
        });
        self.n_returned = n_returned;
        self
    }
}

/// 查询执行阶段：单个执行步骤的详细信息。
#[derive(Debug, Clone)]
pub struct ExplainStage {
    /// 阶段名称（"IXSCAN", "FETCH", "SORT", "SORT_KEY_GENERATOR" 等）。
    pub stage: String,
    /// 返回的文档数。
    pub n_returned: u64,
    /// 扫描的文档/键数。
    pub n_scanned: u64,
    /// 索引名称（仅索引扫描阶段）。
    pub index_name: Option<String>,
    /// 使用的索引键（可选）。
    pub keys: Option<Document>,
    /// 输入子阶段（嵌套阶段）。
    pub input_stages: Vec<ExplainStage>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explain_plan_collscan() {
        let plan = ExplainPlan::collscan(100, 500);
        assert_eq!(plan.query_plan, "COLLSCAN");
        assert!(plan.index_name.is_none());
        assert_eq!(plan.n_returned, 100);
        assert_eq!(plan.n_scanned, 0);
        assert_eq!(plan.n_scanned_objects, 500);
        assert_eq!(plan.total_keys_examined, 0);
        assert_eq!(plan.total_docs_examined, 500);
        assert_eq!(plan.stage_details.len(), 1);
        assert_eq!(plan.stage_details[0].stage, "COLLSCAN");
    }

    #[test]
    fn explain_plan_index_scan() {
        let plan = ExplainPlan::index_scan("idx_age".into(), 50, 50, 50);
        assert_eq!(plan.query_plan, "IXSCAN { index: idx_age }");
        assert_eq!(plan.index_name.as_deref(), Some("idx_age"));
        assert_eq!(plan.n_returned, 50);
        assert_eq!(plan.n_scanned, 50);
        assert_eq!(plan.total_keys_examined, 50);
        assert_eq!(plan.total_docs_examined, 50);
        assert_eq!(plan.stage_details.len(), 2);
        assert_eq!(plan.stage_details[0].stage, "IXSCAN");
        assert_eq!(plan.stage_details[1].stage, "FETCH");
    }

    #[test]
    fn explain_plan_with_sort() {
        let plan = ExplainPlan::collscan(100, 500).with_sort(100);
        assert_eq!(plan.stage_details.len(), 2);
        assert_eq!(plan.stage_details[1].stage, "SORT");
        assert_eq!(plan.n_returned, 100);
    }

    #[test]
    fn explain_plan_index_scan_with_sort() {
        let plan = ExplainPlan::index_scan("idx_name".into(), 30, 30, 30).with_sort(10);
        assert_eq!(plan.stage_details.len(), 3);
        assert_eq!(plan.stage_details[0].stage, "IXSCAN");
        assert_eq!(plan.stage_details[1].stage, "FETCH");
        assert_eq!(plan.stage_details[2].stage, "SORT");
        assert_eq!(plan.n_returned, 10);
    }

    #[test]
    fn explain_stage_defaults() {
        let stage = ExplainStage {
            stage: "SORT".into(),
            n_returned: 0,
            n_scanned: 0,
            index_name: None,
            keys: None,
            input_stages: vec![],
        };
        assert_eq!(stage.stage, "SORT");
        assert!(stage.index_name.is_none());
        assert!(stage.keys.is_none());
        assert!(stage.input_stages.is_empty());
    }

    #[test]
    fn explain_plan_clone() {
        let plan = ExplainPlan::collscan(10, 50);
        let cloned = plan.clone();
        assert_eq!(cloned.query_plan, plan.query_plan);
        assert_eq!(cloned.n_returned, plan.n_returned);
    }
}
