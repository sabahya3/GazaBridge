//! Splits a JSON array into raw elements without parsing the whole file.

use std::io::{BufRead, ErrorKind};

/// Longest single element accepted (protects memory on corrupt input).
pub const MAX_ELEMENT_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug)]
pub enum Next {
    /// Raw bytes of one element.
    Element(Vec<u8>),
    End,
}

/// Streaming scanner over `[elem, elem, …]` or a single top-level object.
#[derive(Debug)]
pub struct ArrayScanner<R> {
    inner: R,
    offset: u64,
    peeked: Option<u8>,
    started: bool,
    single: bool,
    done: bool,
}

impl<R: BufRead> ArrayScanner<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            offset: 0,
            peeked: None,
            started: false,
            single: false,
            done: false,
        }
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    fn byte(&mut self) -> std::io::Result<Option<u8>> {
        if let Some(b) = self.peeked.take() {
            return Ok(Some(b));
        }
        let mut buf = [0u8; 1];
        loop {
            match self.inner.read(&mut buf) {
                Ok(0) => return Ok(None),
                Ok(_) => {
                    self.offset += 1;
                    return Ok(Some(buf[0]));
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
    }

    fn unread(&mut self, b: u8) {
        self.peeked = Some(b);
    }

    fn skip_ws(&mut self) -> std::io::Result<Option<u8>> {
        loop {
            match self.byte()? {
                Some(b) if b.is_ascii_whitespace() => {}
                other => return Ok(other),
            }
        }
    }

    fn structure(message: &str) -> std::io::Error {
        std::io::Error::new(ErrorKind::InvalidData, message.to_owned())
    }

    /// Reads the next element. Structural problems (no array, unterminated
    /// strings or brackets) are `InvalidData` errors: the rest of the file
    /// cannot be split reliably.
    pub fn next(&mut self) -> std::io::Result<Next> {
        if self.done {
            return Ok(Next::End);
        }
        if !self.started {
            self.started = true;
            // UTF-8 byte order mark.
            let mut first = self.skip_ws()?;
            if first == Some(0xEF) {
                let (b, c) = (self.byte()?, self.byte()?);
                if (b, c) != (Some(0xBB), Some(0xBF)) {
                    return Err(Self::structure("invalid byte order mark"));
                }
                first = self.skip_ws()?;
            }
            match first {
                Some(b'[') => {}
                Some(b'{') => {
                    self.single = true;
                    self.unread(b'{');
                }
                None => {
                    self.done = true;
                    return Ok(Next::End);
                }
                Some(_) => {
                    return Err(Self::structure(
                        "a JSON file must contain an array of records or one object",
                    ));
                }
            }
        }
        let first = self.skip_ws()?;
        let Some(first) = first else {
            return if self.single {
                self.done = true;
                Ok(Next::End)
            } else {
                Err(Self::structure("unterminated array"))
            };
        };
        if first == b']' && !self.single {
            self.done = true;
            if self.skip_ws()?.is_some() {
                return Err(Self::structure("unexpected data after the array"));
            }
            return Ok(Next::End);
        }
        let mut out = vec![first];
        let mut depth: i64 = match first {
            b'{' | b'[' => 1,
            _ => 0,
        };
        let mut in_string = first == b'"';
        let mut escaped = false;
        loop {
            if out.len() > MAX_ELEMENT_BYTES {
                return Err(Self::structure("a record is larger than 64 MiB"));
            }
            let Some(b) = self.byte()? else {
                if in_string || depth > 0 {
                    return Err(Self::structure("unterminated record at end of file"));
                }
                if self.single {
                    self.done = true;
                    return Ok(Next::Element(out));
                }
                return Err(Self::structure("unterminated array"));
            };
            if in_string {
                out.push(b);
                if escaped {
                    escaped = false;
                } else if b == b'\\' {
                    escaped = true;
                } else if b == b'"' {
                    in_string = false;
                }
                continue;
            }
            match b {
                b'"' => {
                    in_string = true;
                    out.push(b);
                }
                b'{' | b'[' => {
                    depth += 1;
                    out.push(b);
                }
                b'}' | b']' if depth > 0 => {
                    depth -= 1;
                    out.push(b);
                    if depth == 0 && self.single {
                        self.done = true;
                        if self.skip_ws()?.is_some() {
                            return Err(Self::structure("unexpected data after the object"));
                        }
                        return Ok(Next::Element(out));
                    }
                }
                b',' if depth == 0 => return Ok(Next::Element(out)),
                b']' if depth == 0 => {
                    self.unread(b']');
                    return Ok(Next::Element(out));
                }
                _ => out.push(b),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn elements(text: &str) -> std::io::Result<Vec<String>> {
        let mut s = ArrayScanner::new(text.as_bytes());
        let mut out = Vec::new();
        loop {
            match s.next()? {
                Next::Element(e) => out.push(String::from_utf8(e).unwrap().trim().to_owned()),
                Next::End => return Ok(out),
            }
        }
    }

    #[test]
    fn splits_arrays_respecting_strings_and_nesting() {
        let e = elements(r#" [ {"a": "x,]}\"y"}, [1, [2]], 3 , "s" ] "#).unwrap();
        assert_eq!(e, [r#"{"a": "x,]}\"y"}"#, "[1, [2]]", "3", r#""s""#]);
        assert!(elements("[]").unwrap().is_empty());
        assert!(elements("").unwrap().is_empty());
        assert_eq!(elements("\u{feff}{\"a\": 1}\n").unwrap(), [r#"{"a": 1}"#]);
    }

    #[test]
    fn reports_structural_errors() {
        assert!(elements(r#"[{"a": 1}"#).is_err());
        assert!(elements(r#"[{"a": "unterminated}]"#).is_err());
        assert!(elements("42").is_err());
        assert!(elements("[1] trailing").is_err());
    }

    #[test]
    fn malformed_elements_are_still_split() {
        // Element-level errors are left to the JSON parser.
        assert_eq!(
            elements("[{bad}, {\"ok\": 1}]").unwrap(),
            ["{bad}", r#"{"ok": 1}"#]
        );
    }
}
