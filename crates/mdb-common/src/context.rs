//! 运行上下文。
//!
//! 拆出独立模块是为了让 `mdb-common::Context` 成为稳定的公开路径
//! （运维/日志过滤器文档中引用的路径），实际定义与指标同处 [`crate::metrics`]。

pub use crate::metrics::Context;

/// 当前进程是否在容器内运行。
///
/// 影响：日志默认输出 stderr 而非文件、`/metrics` 绑定地址、cgroup 内存限制。
/// 通过 `/.dockerenv` 与 `/run/.containerenv` 探测（麒麟 podman 环境）。
pub fn in_container() -> bool {
    std::path::Path::new("/.dockerenv").exists()
        || std::path::Path::new("/run/.containerenv").exists()
}

/// 物理内存总量（字节）。
///
/// 优先读 `/proc/meminfo`（Linux 全架构通用：aarch64/x86_64/loongarch64/sw64）。
/// 读不到时返回 `None`，调用方按 4 GiB 保守值处理。
pub fn sys_total_mem_bytes() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kb: u64 = rest.trim().trim_end_matches(" kB").trim().parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

/// cgroup v2 / v1 内存上限（字节）。
///
/// 返回 `None` 表示无限制。这决定 MDB 的缓存计算：
/// 容器内绝不能按物理内存算缓存，否则会被 OOMKilled。
pub fn cgroup_memory_limit() -> Option<u64> {
    // cgroup v2
    if let Ok(text) = std::fs::read_to_string("/sys/fs/cgroup/memory.max") {
        let t = text.trim();
        if t != "max" {
            if let Ok(v) = t.parse::<u64>() {
                return Some(v);
            }
        }
    }
    // cgroup v1
    if let Ok(text) = std::fs::read_to_string(
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ) {
        let v: u64 = text.trim().parse().unwrap_or(u64::MAX);
        // v1 用一个极大值表示无限制
        if v < u64::MAX / 2 {
            return Some(v);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mem_limit_prefers_cgroup_over_physical() {
        let physical = sys_total_mem_bytes();
        let cgroup = cgroup_memory_limit();
        if let (Some(p), Some(c)) = (physical, cgroup) {
            // 容器场景下 cgroup 上限必然小于等于物理内存
            assert!(c <= p, "cgroup limit {c} must not exceed physical {p}");
        }
    }

    #[test]
    fn context_is_five_chars() {
        assert_eq!(Context::new("ABCDE").component.len(), 5);
    }
}
