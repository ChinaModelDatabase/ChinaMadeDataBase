//! 最小 JSON 转义工具。
//!
//! 仅服务于日志与诊断输出，**不参与 BSON 协议**（BSON 编码见 `mdb-doc`）。
//! 放在 `mdb-common` 是为了避免 `mdb-common -> mdb-doc` 的反向依赖。

/// 将字符串按 JSON 字符串规则（含双引号）追加到 `out`。
pub fn escape_into(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            // 审计日志必须防注入：控制字符一律 \u00XX 转义，不允许裸写
            c if (c as u32) < 0x20 || c == '\u{7f}' => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// 渲染为带引号的 JSON 字符串。
pub fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    escape_into(s, &mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_control_chars_for_log_injection_resistance() {
        // 日志注入防护：来自 errmsg 的用户输入不得能伪造出换行 + 假字段
        assert_eq!(escape("a\n\"b\""), r#""a\n\"b\"""#);
        assert_eq!(escape("\u{1b}"), r#""\u001b""#);
        assert_eq!(escape("\u{0}"), r#""\u0000""#);
    }
}
