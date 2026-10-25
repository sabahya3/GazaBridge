//! Reads the query syntax people type in MongoDB tools (mongosh, Compass,
//! Atlas) into Extended JSON:
//!
//! * unquoted keys (`{status: "a"}`) and single-quoted strings,
//! * `ObjectId("…")`, `ISODate("…")`, `new Date("…")`, `NumberLong(…)`,
//!   `NumberInt(…)`, `NumberDecimal("…")`, `UUID("…")`,
//! * regular expressions (`/^abc/i`), trailing commas and `//` comments.
//!
//! Plain JSON passes through unchanged.

use serde_json::Value as J;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message} (at character {position})")]
pub struct ShellJsonError {
    pub position: usize,
    pub message: String,
    /// A corrected query, when the mistake has an obvious fix.
    pub suggestion: Option<J>,
}

struct Reader<'a> {
    chars: Vec<char>,
    pos: usize,
    _src: &'a str,
}

fn quote(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_default()
}

impl<'a> Reader<'a> {
    fn err<T>(&self, message: impl Into<String>) -> Result<T, ShellJsonError> {
        Err(ShellJsonError {
            position: self.pos,
            message: message.into(),
            suggestion: None,
        })
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn skip_ws(&mut self) {
        while let Some(c) = self.peek() {
            if c.is_whitespace() {
                self.pos += 1;
            } else if c == '/' && self.chars.get(self.pos + 1) == Some(&'/') {
                while self.peek().is_some_and(|c| c != '\n') {
                    self.pos += 1;
                }
            } else {
                break;
            }
        }
    }

    fn expect(&mut self, c: char) -> Result<(), ShellJsonError> {
        self.skip_ws();
        if self.peek() == Some(c) {
            self.pos += 1;
            Ok(())
        } else {
            self.err(format!("expected `{c}`"))
        }
    }

    fn string(&mut self) -> Result<String, ShellJsonError> {
        let q = self.peek().unwrap_or('"');
        self.pos += 1;
        let mut out = String::new();
        while let Some(c) = self.peek() {
            self.pos += 1;
            match c {
                '\\' => {
                    let Some(e) = self.peek() else { break };
                    self.pos += 1;
                    match e {
                        'n' => out.push('\n'),
                        't' => out.push('\t'),
                        'r' => out.push('\r'),
                        'b' => out.push('\u{8}'),
                        'f' => out.push('\u{c}'),
                        'u' => {
                            let hex: String = self.chars.iter().skip(self.pos).take(4).collect();
                            let Some(ch) =
                                u32::from_str_radix(&hex, 16).ok().and_then(char::from_u32)
                            else {
                                return self.err("invalid \\u escape");
                            };
                            self.pos += 4;
                            out.push(ch);
                        }
                        other => out.push(other),
                    }
                }
                c if c == q => return Ok(out),
                c => out.push(c),
            }
        }
        self.err("unterminated string")
    }

    fn ident(&mut self) -> String {
        let start = self.pos;
        while self
            .peek()
            .is_some_and(|c| c.is_alphanumeric() || c == '_' || c == '$' || c == '.')
        {
            self.pos += 1;
        }
        self.chars[start..self.pos].iter().collect()
    }

    fn number(&mut self) -> Result<String, ShellJsonError> {
        let start = self.pos;
        while self
            .peek()
            .is_some_and(|c| c.is_ascii_digit() || "+-.eE".contains(c))
        {
            self.pos += 1;
        }
        let text: String = self.chars[start..self.pos].iter().collect();
        if serde_json::from_str::<serde_json::Number>(&text).is_err() {
            return self.err(format!("`{text}` is not a number"));
        }
        Ok(text)
    }

    /// The single argument of a constructor call, as a string.
    fn call_arg(&mut self) -> Result<Option<String>, ShellJsonError> {
        self.expect('(')?;
        self.skip_ws();
        let arg = match self.peek() {
            Some(')') => None,
            Some('"' | '\'') => Some(self.string()?),
            Some(c) if c.is_ascii_digit() || c == '-' => Some(self.number()?),
            _ => return self.err("expected a string or number argument"),
        };
        self.expect(')')?;
        Ok(arg)
    }

    fn regex(&mut self) -> Result<String, ShellJsonError> {
        self.pos += 1; // opening '/'
        let mut pattern = String::new();
        let mut in_class = false;
        loop {
            let Some(c) = self.peek() else {
                return self.err("unterminated regular expression");
            };
            self.pos += 1;
            match c {
                '\\' => {
                    // `\/` only escapes the delimiter; other escapes belong to the pattern.
                    if let Some(n) = self.peek() {
                        if n != '/' {
                            pattern.push('\\');
                        }
                        pattern.push(n);
                        self.pos += 1;
                    }
                }
                '[' => {
                    in_class = true;
                    pattern.push(c);
                }
                ']' => {
                    in_class = false;
                    pattern.push(c);
                }
                '/' if !in_class => break,
                c => pattern.push(c),
            }
        }
        let options = self.ident();
        if !options.chars().all(|c| "imsxu".contains(c)) {
            return self.err(format!("unknown regular expression option in `{options}`"));
        }
        Ok(format!(
            "{{\"$regularExpression\":{{\"pattern\":{},\"options\":{}}}}}",
            quote(&pattern),
            quote(&options)
        ))
    }

    fn value(&mut self) -> Result<String, ShellJsonError> {
        self.skip_ws();
        match self.peek() {
            None => self.err("unexpected end of query"),
            Some('{') => self.object(),
            Some('[') => self.array(),
            Some('"' | '\'') => Ok(quote(&self.string()?)),
            Some('/') => self.regex(),
            Some(c) if c.is_ascii_digit() || c == '-' || c == '+' || c == '.' => self.number(),
            Some(c) if c.is_alphabetic() || c == '_' || c == '$' => {
                let mut name = self.ident();
                if name == "new" {
                    self.skip_ws();
                    name = self.ident();
                }
                match name.as_str() {
                    "true" | "false" | "null" => Ok(name),
                    "undefined" => Ok("null".into()),
                    "ObjectId" => match self.call_arg()? {
                        Some(hex) => Ok(format!("{{\"$oid\":{}}}", quote(&hex))),
                        None => self.err("ObjectId() needs a value"),
                    },
                    "ISODate" | "Date" => match self.call_arg()? {
                        Some(d)
                            if d.chars().all(|c| c.is_ascii_digit() || c == '-')
                                && !d.contains('T')
                                && d.len() > 10 =>
                        {
                            Ok(format!("{{\"$date\":{{\"$numberLong\":{}}}}}", quote(&d)))
                        }
                        Some(d) => Ok(format!("{{\"$date\":{}}}", quote(&normalize_date(&d)))),
                        None => self.err(format!("{name}() needs a value")),
                    },
                    "NumberLong" | "Long" => self.typed("$numberLong"),
                    "NumberInt" | "Int32" => self.typed("$numberInt"),
                    "NumberDecimal" | "Decimal128" => self.typed("$numberDecimal"),
                    "UUID" => match self.call_arg()? {
                        Some(u) => Ok(format!("{{\"$uuid\":{}}}", quote(&u))),
                        None => self.err("UUID() needs a value"),
                    },
                    other => self.err(format!(
                        "unknown value `{other}`; quote text, e.g. \"{other}\""
                    )),
                }
            }
            Some(c) => self.err(format!("unexpected `{c}`")),
        }
    }

    fn typed(&mut self, tag: &str) -> Result<String, ShellJsonError> {
        match self.call_arg()? {
            Some(v) => Ok(format!("{{{}:{}}}", quote(tag), quote(&v))),
            None => self.err("a value is required"),
        }
    }

    fn key(&mut self) -> Result<String, ShellJsonError> {
        self.skip_ws();
        match self.peek() {
            Some('"' | '\'') => self.string(),
            Some(c) if c.is_alphanumeric() || c == '_' || c == '$' => Ok(self.ident()),
            _ => self.err("expected a field name"),
        }
    }

    fn object(&mut self) -> Result<String, ShellJsonError> {
        self.pos += 1;
        let mut parts = Vec::new();
        loop {
            self.skip_ws();
            if self.peek() == Some('}') {
                self.pos += 1;
                return Ok(format!("{{{}}}", parts.join(",")));
            }
            let k = self.key()?;
            self.expect(':')?;
            let v = self.value()?;
            parts.push(format!("{}:{v}", quote(&k)));
            self.skip_ws();
            match self.peek() {
                Some(',') => self.pos += 1,
                Some('}') => {}
                _ => return self.err("expected `,` or `}`"),
            }
        }
    }

    fn array(&mut self) -> Result<String, ShellJsonError> {
        self.pos += 1;
        let mut parts = Vec::new();
        loop {
            self.skip_ws();
            if self.peek() == Some(']') {
                self.pos += 1;
                return Ok(format!("[{}]", parts.join(",")));
            }
            parts.push(self.value()?);
            self.skip_ws();
            match self.peek() {
                Some(',') => self.pos += 1,
                Some(']') => {}
                _ => return self.err("expected `,` or `]`"),
            }
        }
    }
}

/// One document requiring all of `docs`: their fields side by side when
/// no field repeats, otherwise `$and`.
fn merge(docs: Vec<J>) -> J {
    let mut merged = serde_json::Map::new();
    for d in &docs {
        for (k, v) in d.as_object().into_iter().flatten() {
            if merged.insert(k.clone(), v.clone()).is_some() || k.starts_with('$') {
                let mut and = serde_json::Map::new();
                and.insert("$and".into(), J::Array(docs));
                return J::Object(and);
            }
        }
    }
    J::Object(merged)
}

/// `2026-01-02` → `2026-01-02T00:00:00Z`; full timestamps pass through.
fn normalize_date(d: &str) -> String {
    if d.len() == 10 && d.chars().filter(|c| *c == '-').count() == 2 {
        format!("{d}T00:00:00Z")
    } else {
        d.to_owned()
    }
}

/// Parses mongosh/Compass query syntax into Extended JSON. Empty text is `{}`.
pub fn parse(text: &str) -> Result<J, ShellJsonError> {
    if text.trim().is_empty() {
        return Ok(J::Object(serde_json::Map::new()));
    }
    let mut r = Reader {
        chars: text.chars().collect(),
        pos: 0,
        _src: text,
    };
    let json = r.value()?;
    r.skip_ws();
    if r.pos < r.chars.len() {
        // `{a: 1}, {b: 2}`: several documents where one was meant.
        if r.peek() == Some(',') {
            let position = r.pos;
            let mut docs = vec![json.clone()];
            while r.peek() == Some(',') {
                r.pos += 1;
                r.skip_ws();
                if r.pos >= r.chars.len() {
                    break;
                }
                match r.value() {
                    Ok(v) => docs.push(v),
                    Err(_) => break,
                }
                r.skip_ws();
            }
            let parsed: Vec<J> = docs
                .iter()
                .filter_map(|d| serde_json::from_str(d).ok())
                .collect();
            if parsed.len() > 1 && parsed.iter().all(J::is_object) {
                return Err(ShellJsonError {
                    position,
                    message: "write one document: put all conditions inside one pair of braces"
                        .into(),
                    suggestion: Some(merge(parsed)),
                });
            }
        }
        return r.err("unexpected text after the query");
    }
    serde_json::from_str(&json).map_err(|e| ShellJsonError {
        position: 0,
        message: e.to_string(),
        suggestion: None,
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn reads_shell_syntax() {
        assert_eq!(
            parse(r#"{ status: 'open', total: { $gt: 5 }, tags: ["a", 'b',], }"#).unwrap(),
            json!({"status": "open", "total": {"$gt": 5}, "tags": ["a", "b"]})
        );
        assert_eq!(
            parse(r#"{_id: ObjectId("65134b87d147eab2a5d15df6")}"#).unwrap(),
            json!({"_id": {"$oid": "65134b87d147eab2a5d15df6"}})
        );
        assert_eq!(
            parse(r#"{at: {$gte: ISODate("2026-01-02"), $lt: new Date('2026-02-01T00:00:00Z')}}"#)
                .unwrap(),
            json!({"at": {"$gte": {"$date": "2026-01-02T00:00:00Z"}, "$lt": {"$date": "2026-02-01T00:00:00Z"}}})
        );
        assert_eq!(
            parse(r#"{n: NumberLong(5), d: NumberDecimal("1.50")}"#).unwrap(),
            json!({"n": {"$numberLong": "5"}, "d": {"$numberDecimal": "1.50"}})
        );
        assert_eq!(
            parse(r#"{name: /^ahm[a/]d\//i}"#).unwrap(),
            json!({"name": {"$regularExpression": {"pattern": "^ahm[a/]d/", "options": "i"}}})
        );
        assert_eq!(
            parse("{'a.b': -1.5e3} // note").unwrap(),
            json!({"a.b": -1.5e3})
        );
        assert_eq!(parse("  ").unwrap(), json!({}));
        assert_eq!(
            parse(r#"{"plain": "json"}"#).unwrap(),
            json!({"plain": "json"})
        );
    }

    #[test]
    fn several_documents_get_a_merged_suggestion() {
        let e = parse(r#"{ phoneNumber: "966123456789" },{nationalId:"1234567895"}"#).unwrap_err();
        assert_eq!(
            e.suggestion,
            Some(json!({"phoneNumber": "966123456789", "nationalId": "1234567895"}))
        );
        let e = parse("{a: 1}, {a: 2}").unwrap_err();
        assert_eq!(e.suggestion, Some(json!({"$and": [{"a": 1}, {"a": 2}]})));
        assert!(parse("{a: 1} x").unwrap_err().suggestion.is_none());
    }

    #[test]
    fn reports_where_it_failed() {
        let e = parse("{status: open}").unwrap_err();
        assert!(e.message.contains("quote text"), "{e}");
        assert!(parse("{a: 1").is_err());
        assert!(parse("{a: 'x'} extra").is_err());
        assert!(parse("{a: /x/z}").is_err());
    }
}
