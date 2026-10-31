//! 简化 JavaScript 表达式求值器：用于 $where 查询。
//!
//! 支持的语法：
//! - 字面量：数字、字符串、布尔值、null
//! - 字段访问：`this.field`、`this.field.subfield`
//! - 比较运算：`==`、`!=`、`>`、`<`、`>=`、`<=`
//! - 逻辑运算：`&&`、`||`、`!`
//! - 算术运算：`+`、`-`、`*`、`/`、`%`
//! - 函数调用：`this.field.startsWith("prefix")`

use mdb_doc::{Bson, Document};

/// 表达式求值错误。
#[derive(Debug)]
pub enum EvalError {
    Syntax(String),
    Type(String),
    MissingField(String),
}

impl std::fmt::Display for EvalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EvalError::Syntax(msg) => write!(f, "SyntaxError: {msg}"),
            EvalError::Type(msg) => write!(f, "TypeError: {msg}"),
            EvalError::MissingField(field) => write!(f, "MissingField: {field}"),
        }
    }
}

/// 检查文档是否满足 $where 条件。
pub fn matches_where(where_clause: &str, doc: &Document) -> bool {
    match eval_expression(where_clause, doc) {
        Ok(Bson::Boolean(b)) => b,
        _ => false,
    }
}

/// 在文档上下文中求值表达式。
pub fn eval_expression(expr: &str, doc: &Document) -> Result<Bson, EvalError> {
    let tokens = tokenize(expr.trim())?;
    let mut ev = Evaluator { tokens, pos: 0, doc };
    ev.parse_or()
}

// ---------------------------------------------------------------------------
// Token
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    Number(f64),
    Str(String),
    Bool(bool),
    Null,
    Op(String),
    LParen,
    RParen,
    Dot,
    Eof,
}

// ---------------------------------------------------------------------------
// Lexer
// ---------------------------------------------------------------------------

fn tokenize(input: &str) -> Result<Vec<Token>, EvalError> {
    let mut tokens = Vec::new();
    let b = input.as_bytes();
    let mut i = 0usize;

    while i < b.len() {
        while i < b.len() && b[i].is_ascii_whitespace() { i += 1; }
        if i >= b.len() { break; }

        let c = b[i];

        // Identifiers
        if c.is_ascii_alphabetic() || c == b'_' || c == b'$' {
            let s = i;
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'$') { i += 1; }
            let w = &input[s..i];
            tokens.push(match w {
                "true" => Token::Bool(true),
                "false" => Token::Bool(false),
                "null" => Token::Null,
                _ => Token::Ident(w.into()),
            });
            continue;
        }

        // Numbers (only if starts with digit, or dot followed by digit)
        if c.is_ascii_digit() || (c == b'.' && i + 1 < b.len() && b[i + 1].is_ascii_digit()) {
            let s = i;
            while i < b.len() && (b[i].is_ascii_digit() || b[i] == b'.') { i += 1; }
            let num_str = &input[s..i];
            let n: f64 = num_str.parse().map_err(|_| EvalError::Syntax("bad number".into()))?;
            tokens.push(Token::Number(n));
            continue;
        }

        // Strings
        if c == b'"' || c == b'\'' {
            let q = c;
            i += 1;
            let s = i;
            while i < b.len() && b[i] != q { i += 1; }
            if i >= b.len() { return Err(EvalError::Syntax("unterminated string".into())); }
            tokens.push(Token::Str(input[s..i].into()));
            i += 1;
            continue;
        }

        // Operators & punctuation
        i += 1;
        match c {
            b'(' => tokens.push(Token::LParen),
            b')' => tokens.push(Token::RParen),
            b'.' => tokens.push(Token::Dot),
            b'=' if i < b.len() && b[i] == b'=' => { i += 1; tokens.push(Token::Op("==".into())); }
            b'!' if i < b.len() && b[i] == b'=' => { i += 1; tokens.push(Token::Op("!=".into())); }
            b'>' if i < b.len() && b[i] == b'=' => { i += 1; tokens.push(Token::Op(">=".into())); }
            b'<' if i < b.len() && b[i] == b'=' => { i += 1; tokens.push(Token::Op("<=".into())); }
            b'&' if i < b.len() && b[i] == b'&' => { i += 1; tokens.push(Token::Op("&&".into())); }
            b'|' if i < b.len() && b[i] == b'|' => { i += 1; tokens.push(Token::Op("||".into())); }
            b'!' => tokens.push(Token::Op("!".into())),
            b'>' => tokens.push(Token::Op(">".into())),
            b'<' => tokens.push(Token::Op("<".into())),
            b'+' | b'-' | b'*' | b'/' | b'%' => tokens.push(Token::Op((c as char).to_string())),
            _ => return Err(EvalError::Syntax(format!("unexpected: {}", c as char))),
        }
    }

    tokens.push(Token::Eof);
    Ok(tokens)
}

// ---------------------------------------------------------------------------
// Evaluator
// ---------------------------------------------------------------------------

struct Evaluator<'a> {
    tokens: Vec<Token>,
    pos: usize,
    doc: &'a Document,
}

impl<'a> Evaluator<'a> {
    fn peek(&self) -> &Token { self.tokens.get(self.pos).unwrap_or(&Token::Eof) }
    fn bump(&mut self) -> Token {
        let t = self.tokens.get(self.pos).cloned().unwrap_or(Token::Eof);
        if self.pos < self.tokens.len() { self.pos += 1; }
        t
    }

    fn parse_or(&mut self) -> Result<Bson, EvalError> {
        let mut l = self.parse_and()?;
        while let Token::Op(op) = self.peek() {
            if op != "||" { break; }
            self.bump();
            let r = self.parse_and()?;
            l = Bson::Boolean(truthy(&l) || truthy(&r));
        }
        Ok(l)
    }

    fn parse_and(&mut self) -> Result<Bson, EvalError> {
        let mut l = self.parse_cmp()?;
        while let Token::Op(op) = self.peek() {
            if op != "&&" { break; }
            self.bump();
            let r = self.parse_cmp()?;
            l = Bson::Boolean(truthy(&l) && truthy(&r));
        }
        Ok(l)
    }

    fn parse_cmp(&mut self) -> Result<Bson, EvalError> {
        let l = self.parse_add()?;
        if let Token::Op(op) = self.peek() {
            let op = op.clone();
            if ["==", "!=", ">", "<", ">=", "<="].contains(&op.as_str()) {
                self.bump();
                let r = self.parse_add()?;
                let b = match op.as_str() {
                    "==" => eq(&l, &r),
                    "!=" => !eq(&l, &r),
                    ">" => cmp(&l, &r)? > std::cmp::Ordering::Equal,
                    "<" => cmp(&l, &r)? < std::cmp::Ordering::Equal,
                    ">=" => cmp(&l, &r)? != std::cmp::Ordering::Less,
                    "<=" => cmp(&l, &r)? != std::cmp::Ordering::Greater,
                    _ => unreachable!(),
                };
                return Ok(Bson::Boolean(b));
            }
        }
        Ok(l)
    }

    fn parse_add(&mut self) -> Result<Bson, EvalError> {
        let mut l = self.parse_mul()?;
        while let Token::Op(op) = self.peek() {
            if op != "+" && op != "-" { break; }
            let op = op.clone();
            self.bump();
            let r = self.parse_mul()?;
            l = if op == "+" { add(&l, &r)? } else { sub(&l, &r)? };
        }
        Ok(l)
    }

    fn parse_mul(&mut self) -> Result<Bson, EvalError> {
        let mut l = self.parse_unary()?;
        while let Token::Op(op) = self.peek() {
            if op != "*" && op != "/" && op != "%" { break; }
            let op = op.clone();
            self.bump();
            let r = self.parse_unary()?;
            l = match op.as_str() { "*" => mul(&l, &r)?, "/" => div(&l, &r)?, _ => modulo(&l, &r)? };
        }
        Ok(l)
    }

    fn parse_unary(&mut self) -> Result<Bson, EvalError> {
        if let Token::Op(op) = self.peek() {
            if op == "!" { self.bump(); let v = self.parse_unary()?; return Ok(Bson::Boolean(!truthy(&v))); }
            if op == "-" { self.bump(); let v = self.parse_unary()?; return match v { Bson::Int32(x) => Ok(Bson::Int32(-x)), Bson::Int64(x) => Ok(Bson::Int64(-x)), Bson::Double(x) => Ok(Bson::Double(-x)), _ => Err(EvalError::Type("negate non-number".into())) }; }
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<Bson, EvalError> {
        match self.bump() {
            Token::Number(n) => Ok(if n.fract() == 0.0 && n >= i32::MIN as f64 && n <= i32::MAX as f64 { Bson::Int32(n as i32) } else { Bson::Double(n) }),
            Token::Str(s) => Ok(Bson::String(s)),
            Token::Bool(b) => Ok(Bson::Boolean(b)),
            Token::Null => Ok(Bson::Null),
            Token::LParen => { let v = self.parse_or()?; match self.bump() { Token::RParen => Ok(v), _ => Err(EvalError::Syntax("expected )".into())) } }
            Token::Ident(name) if name == "this" => self.parse_this(),
            Token::Ident(name) => Err(EvalError::Syntax(format!("unsupported: {name}"))),
            t => Err(EvalError::Syntax(format!("unexpected: {t:?}"))),
        }
    }

    fn parse_this(&mut self) -> Result<Bson, EvalError> {
        let mut val: Option<Bson> = None;
        let mut path = String::from("this");

        loop {
            match self.peek() {
                Token::Dot => {
                    self.bump();
                    match self.bump() {
                        Token::Ident(f) => {
                            // 检查是否是方法调用（后面跟 '('）
                            if self.peek() == &Token::LParen {
                                // 方法调用
                                self.bump(); // consume '('
                                let arg = self.parse_or()?;
                                match self.bump() {
                                    Token::RParen => {}
                                    _ => return Err(EvalError::Syntax("expected )".into())),
                                }
                                let obj = val.take().unwrap_or(Bson::Null);
                                return self.call(&obj, &f, &arg);
                            }
                            // 字段访问
                            path.push('.');
                            path.push_str(&f);
                            val = Some(match val {
                                Some(Bson::Document(d)) => d.get(&f).cloned().unwrap_or(Bson::Null),
                                Some(_) => Bson::Null,
                                None => self.doc.get(&f).cloned().unwrap_or(Bson::Null),
                            });
                        }
                        _ => return Err(EvalError::Syntax("expected field".into())),
                    }
                }
                _ => break,
            }
        }
        val.ok_or_else(|| EvalError::MissingField(path))
    }

    fn call(&self, obj: &Bson, method: &str, arg: &Bson) -> Result<Bson, EvalError> {
        match method {
            "startsWith" => { let p = arg.as_str().ok_or_else(|| EvalError::Type("expects str".into()))?; let s = obj.as_str().ok_or_else(|| EvalError::Type("non-string".into()))?; Ok(Bson::Boolean(s.starts_with(p))) }
            "endsWith" => { let p = arg.as_str().ok_or_else(|| EvalError::Type("expects str".into()))?; let s = obj.as_str().ok_or_else(|| EvalError::Type("non-string".into()))?; Ok(Bson::Boolean(s.ends_with(p))) }
            "includes" => { let p = arg.as_str().ok_or_else(|| EvalError::Type("expects str".into()))?; let s = obj.as_str().ok_or_else(|| EvalError::Type("non-string".into()))?; Ok(Bson::Boolean(s.contains(p))) }
            "toString" => Ok(Bson::String(format!("{obj}"))),
            m => Err(EvalError::Syntax(format!("unsupported method: {m}"))),
        }
    }
}

// ---------------------------------------------------------------------------
// Value helpers
// ---------------------------------------------------------------------------

fn truthy(v: &Bson) -> bool {
    match v {
        Bson::Boolean(b) => *b,
        Bson::Null => false,
        Bson::Int32(i) => *i != 0,
        Bson::Int64(i) => *i != 0,
        Bson::Double(d) => *d != 0.0,
        Bson::String(s) => !s.is_empty(),
        _ => true,
    }
}

fn eq(a: &Bson, b: &Bson) -> bool {
    match (a, b) {
        (Bson::Int32(x), Bson::Int32(y)) => x == y,
        (Bson::Int64(x), Bson::Int64(y)) => x == y,
        (Bson::Int32(x), Bson::Int64(y)) => *x as i64 == *y,
        (Bson::Int64(x), Bson::Int32(y)) => *x == *y as i64,
        (Bson::Double(x), Bson::Double(y)) => x == y,
        (Bson::Int32(x), Bson::Double(y)) => *x as f64 == *y,
        (Bson::Double(x), Bson::Int32(y)) => *x == *y as f64,
        (Bson::String(x), Bson::String(y)) => x == y,
        (Bson::Boolean(x), Bson::Boolean(y)) => x == y,
        (Bson::Null, Bson::Null) => true,
        _ => false,
    }
}

fn num(v: &Bson) -> Option<f64> {
    match v {
        Bson::Int32(i) => Some(*i as f64),
        Bson::Int64(i) => Some(*i as f64),
        Bson::Double(d) => Some(*d),
        _ => None,
    }
}

fn cmp(a: &Bson, b: &Bson) -> Result<std::cmp::Ordering, EvalError> {
    if let (Some(x), Some(y)) = (num(a), num(b)) { return x.partial_cmp(&y).ok_or_else(|| EvalError::Type("cmp".into())); }
    if let (Some(x), Some(y)) = (a.as_str(), b.as_str()) { return x.partial_cmp(y).ok_or_else(|| EvalError::Type("cmp".into())); }
    Err(EvalError::Type(format!("cannot compare {a:?} {b:?}")))
}

fn add(a: &Bson, b: &Bson) -> Result<Bson, EvalError> {
    if let (Some(x), Some(y)) = (a.as_str(), b.as_str()) { return Ok(Bson::String(format!("{x}{y}"))); }
    if let (Some(x), Some(y)) = (num(a), num(b)) { return Ok(Bson::Double(x + y)); }
    Err(EvalError::Type("add".into()))
}

fn sub(a: &Bson, b: &Bson) -> Result<Bson, EvalError> {
    if let (Some(x), Some(y)) = (num(a), num(b)) { Ok(Bson::Double(x - y)) } else { Err(EvalError::Type("sub".into())) }
}

fn mul(a: &Bson, b: &Bson) -> Result<Bson, EvalError> {
    if let (Some(x), Some(y)) = (num(a), num(b)) { Ok(Bson::Double(x * y)) } else { Err(EvalError::Type("mul".into())) }
}

fn div(a: &Bson, b: &Bson) -> Result<Bson, EvalError> {
    if let (Some(x), Some(y)) = (num(a), num(b)) { if y == 0.0 { Err(EvalError::Type("div0".into())) } else { Ok(Bson::Double(x / y)) } } else { Err(EvalError::Type("div".into())) }
}

fn modulo(a: &Bson, b: &Bson) -> Result<Bson, EvalError> {
    if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) { if y == 0 { Err(EvalError::Type("mod0".into())) } else { Ok(Bson::Int32((x % y) as i32)) } } else { Err(EvalError::Type("mod".into())) }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn d(fields: &[(&str, Bson)]) -> Document {
        let mut doc = Document::new();
        for (k, v) in fields { doc.insert(*k, v.clone()); }
        doc
    }

    #[test]
    fn test_eq() {
        let doc = d(&[("a", Bson::Int32(1))]);
        assert!(matches_where("this.a == 1", &doc));
        assert!(!matches_where("this.a == 2", &doc));
    }

    #[test]
    fn test_gt() {
        let doc = d(&[("n", Bson::Int32(10))]);
        assert!(matches_where("this.n > 5", &doc));
        assert!(!matches_where("this.n > 15", &doc));
    }

    #[test]
    fn test_string() {
        let doc = d(&[("s", Bson::String("hello".into()))]);
        assert!(matches_where("this.s == 'hello'", &doc));
        let result = eval_expression("this.s.startsWith('he')", &doc);
        eprintln!("startsWith result: {result:?}");
        assert!(matches_where("this.s.startsWith('he')", &doc));
    }

    #[test]
    fn test_and_or() {
        let doc = d(&[("a", Bson::Int32(1)), ("b", Bson::Int32(2))]);
        assert!(matches_where("this.a == 1 && this.b == 2", &doc));
        assert!(matches_where("this.a == 1 || this.b == 99", &doc));
    }

    #[test]
    fn test_arithmetic() {
        let doc = d(&[("x", Bson::Int32(10)), ("y", Bson::Int32(3))]);
        assert!(matches_where("this.x + this.y == 13", &doc));
        assert!(matches_where("this.x - this.y == 7", &doc));
    }

    #[test]
    fn test_nested() {
        let mut inner = Document::new();
        inner.insert("city", Bson::String("BJ".into()));
        let doc = d(&[("addr", Bson::Document(inner))]);
        assert!(matches_where("this.addr.city == 'BJ'", &doc));
    }
}
