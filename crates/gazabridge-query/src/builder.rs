//! The visual query builder's model and its exact translation to and from
//! MongoDB query documents.
//!
//! A builder query is a tree of AND/OR groups holding conditions. It always
//! converts to a filter document; a filter document converts back when every
//! part has a builder form (otherwise it stays in the text editor).

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as J, json};

use crate::shell;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{field}: {message}")]
pub struct BuildError {
    pub field: String,
    pub message: String,
}

fn err(field: &str, message: impl Into<String>) -> BuildError {
    BuildError {
        field: field.to_owned(),
        message: message.into(),
    }
}

/// How a typed value is interpreted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueType {
    /// The field's detected type, or the literal's own type.
    #[default]
    Auto,
    String,
    Number,
    Bool,
    ObjectId,
    Date,
    Null,
}

impl ValueType {
    pub const ALL: [ValueType; 7] = [
        Self::Auto,
        Self::String,
        Self::Number,
        Self::Bool,
        Self::ObjectId,
        Self::Date,
        Self::Null,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::String => "string",
            Self::Number => "number",
            Self::Bool => "bool",
            Self::ObjectId => "object_id",
            Self::Date => "date",
            Self::Null => "null",
        }
    }

    pub fn parse(s: &str) -> Self {
        Self::ALL
            .into_iter()
            .find(|t| t.as_str() == s)
            .unwrap_or_default()
    }

    /// Builder type for a value kind reported by the viewer.
    pub fn from_kind(kind: &str) -> Option<Self> {
        Some(match kind {
            "string" | "enum" => Self::String,
            "int" | "uint" | "bigint" | "decimal" | "float" => Self::Number,
            "bool" => Self::Bool,
            "object_id" => Self::ObjectId,
            "utc_datetime" | "local_datetime" | "zoned_datetime" | "date" => Self::Date,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Op {
    #[default]
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    /// Comma-separated values.
    In,
    Nin,
    Between,
    Regex,
    Exists,
    Missing,
    IsNull,
    NotNull,
    /// BSON type name, e.g. `string`, `int`, `objectId`.
    Type,
    Size,
    /// A query (in shell syntax) one array element must match.
    ElemMatch,
}

impl Op {
    pub const ALL: [Op; 17] = [
        Self::Eq,
        Self::Ne,
        Self::Gt,
        Self::Gte,
        Self::Lt,
        Self::Lte,
        Self::In,
        Self::Nin,
        Self::Between,
        Self::Regex,
        Self::Exists,
        Self::Missing,
        Self::IsNull,
        Self::NotNull,
        Self::Type,
        Self::Size,
        Self::ElemMatch,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Eq => "eq",
            Self::Ne => "ne",
            Self::Gt => "gt",
            Self::Gte => "gte",
            Self::Lt => "lt",
            Self::Lte => "lte",
            Self::In => "in",
            Self::Nin => "nin",
            Self::Between => "between",
            Self::Regex => "regex",
            Self::Exists => "exists",
            Self::Missing => "missing",
            Self::IsNull => "is_null",
            Self::NotNull => "not_null",
            Self::Type => "type",
            Self::Size => "size",
            Self::ElemMatch => "elem_match",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|o| o.as_str() == s)
    }

    /// Number of values the operator takes (0, 1 or 2).
    pub fn arity(self) -> u8 {
        match self {
            Self::Exists | Self::Missing | Self::IsNull | Self::NotNull => 0,
            Self::Between => 2,
            _ => 1,
        }
    }

    /// Whether the value's type matters (and the type picker is shown).
    pub fn typed(self) -> bool {
        matches!(
            self,
            Self::Eq
                | Self::Ne
                | Self::Gt
                | Self::Gte
                | Self::Lt
                | Self::Lte
                | Self::In
                | Self::Nin
                | Self::Between
        )
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Condition {
    pub field: String,
    pub op: Op,
    #[serde(default)]
    pub value: String,
    /// Upper bound for `Between`.
    #[serde(default)]
    pub value2: String,
    #[serde(default)]
    pub value_type: ValueType,
    /// `Regex` only.
    #[serde(default)]
    pub case_insensitive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "node", rename_all = "snake_case")]
pub enum Node {
    Condition(Condition),
    Group(Group),
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Group {
    /// Match any child (OR) instead of all (AND).
    #[serde(default)]
    pub any: bool,
    pub children: Vec<Node>,
}

impl Group {
    pub fn is_empty(&self) -> bool {
        self.children.iter().all(|c| match c {
            Node::Condition(c) => c.field.trim().is_empty(),
            Node::Group(g) => g.is_empty(),
        })
    }
}

fn unquote(s: &str) -> &str {
    let t = s.trim();
    for q in ['"', '\''] {
        if t.len() >= 2 && t.starts_with(q) && t.ends_with(q) {
            return &t[1..t.len() - 1];
        }
    }
    t
}

fn number(field: &str, text: &str) -> Result<J, BuildError> {
    let t = text.trim();
    if let Ok(i) = t.parse::<i64>() {
        return Ok(J::from(i));
    }
    t.parse::<f64>()
        .ok()
        .and_then(serde_json::Number::from_f64)
        .map(J::Number)
        .ok_or_else(|| err(field, format!("`{t}` is not a number")))
}

fn date(field: &str, text: &str) -> Result<J, BuildError> {
    let t = unquote(text);
    let ok = t.len() >= 10 && t.as_bytes()[4] == b'-' && t.as_bytes()[7] == b'-';
    if !ok {
        return Err(err(
            field,
            format!("`{t}` is not a date (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ)"),
        ));
    }
    let full = if t.len() == 10 {
        format!("{t}T00:00:00Z")
    } else {
        t.to_owned()
    };
    Ok(json!({ "$date": full }))
}

/// A value as typed, interpreted by its type (`Auto` uses the field's
/// detected type, then the literal's own form).
pub fn typed_value(
    field: &str,
    text: &str,
    ty: ValueType,
    detected: Option<ValueType>,
) -> Result<J, BuildError> {
    let ty = match (ty, detected) {
        (ValueType::Auto, Some(d)) => d,
        (t, _) => t,
    };
    match ty {
        ValueType::String => Ok(J::String(unquote(text).to_owned())),
        ValueType::Number => number(field, text),
        ValueType::Bool => match text.trim() {
            "true" => Ok(J::Bool(true)),
            "false" => Ok(J::Bool(false)),
            other => Err(err(field, format!("`{other}` is not true or false"))),
        },
        ValueType::ObjectId => {
            let t = unquote(text);
            let t = t
                .strip_prefix("ObjectId(")
                .and_then(|r| r.strip_suffix(')'))
                .map_or(t, unquote);
            if t.len() == 24 && t.chars().all(|c| c.is_ascii_hexdigit()) {
                Ok(json!({ "$oid": t.to_ascii_lowercase() }))
            } else {
                Err(err(
                    field,
                    format!("`{t}` is not an ObjectId (24 hex characters)"),
                ))
            }
        }
        ValueType::Date => date(field, text),
        ValueType::Null => Ok(J::Null),
        ValueType::Auto => Ok(shell::parse(text)
            .ok()
            .filter(|v| {
                !v.is_object()
                    || v.as_object()
                        .is_some_and(|o| o.keys().all(|k| k.starts_with('$')))
            })
            .unwrap_or_else(|| J::String(text.trim().to_owned()))),
    }
}

fn list(c: &Condition, detected: Option<ValueType>) -> Result<J, BuildError> {
    let items = c
        .value
        .split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| typed_value(&c.field, v, c.value_type, detected))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(J::Array(items))
}

fn condition(c: &Condition, detected: &dyn Fn(&str) -> Option<ValueType>) -> Result<J, BuildError> {
    let f = c.field.trim();
    let d = detected(f);
    let v = || typed_value(f, &c.value, c.value_type, d);
    let op = |name: &str, value: J| json!({ f: { name: value } });
    Ok(match c.op {
        Op::Eq => json!({ f: v()? }),
        Op::Ne => op("$ne", v()?),
        Op::Gt => op("$gt", v()?),
        Op::Gte => op("$gte", v()?),
        Op::Lt => op("$lt", v()?),
        Op::Lte => op("$lte", v()?),
        Op::In => op("$in", list(c, d)?),
        Op::Nin => op("$nin", list(c, d)?),
        Op::Between => {
            json!({ f: { "$gte": v()?, "$lte": typed_value(f, &c.value2, c.value_type, d)? } })
        }
        Op::Regex => {
            let mut inner = Map::new();
            inner.insert("$regex".into(), J::String(c.value.clone()));
            if c.case_insensitive {
                inner.insert("$options".into(), J::String("i".into()));
            }
            json!({ f: J::Object(inner) })
        }
        Op::Exists => op("$exists", J::Bool(true)),
        Op::Missing => op("$exists", J::Bool(false)),
        Op::IsNull => op("$type", J::String("null".into())),
        Op::NotNull => op("$ne", J::Null),
        Op::Type => op("$type", J::String(c.value.trim().to_owned())),
        Op::Size => op("$size", number(f, &c.value)?),
        Op::ElemMatch => {
            let inner = shell::parse(&c.value).map_err(|e| err(f, e.to_string()))?;
            op("$elemMatch", inner)
        }
    })
}

/// The filter document for a builder query. `detected` gives a field's
/// type as seen in the data (used for `Auto` values).
pub fn to_filter(g: &Group, detected: &dyn Fn(&str) -> Option<ValueType>) -> Result<J, BuildError> {
    let mut parts = Vec::new();
    for child in &g.children {
        match child {
            Node::Condition(c) if c.field.trim().is_empty() => {}
            Node::Condition(c) => parts.push(condition(c, detected)?),
            Node::Group(inner) if inner.is_empty() => {}
            Node::Group(inner) => parts.push(to_filter(inner, detected)?),
        }
    }
    Ok(match parts.len() {
        0 => json!({}),
        1 if !g.any => parts.remove(0),
        _ => json!({ (if g.any { "$or" } else { "$and" }): parts }),
    })
}

/// Text and type of a literal value, when it has a builder form.
fn value_text(v: &J) -> Option<(String, ValueType)> {
    Some(match v {
        J::String(s) => (s.clone(), ValueType::String),
        J::Number(n) => (n.to_string(), ValueType::Number),
        J::Bool(b) => (b.to_string(), ValueType::Bool),
        J::Null => (String::new(), ValueType::Null),
        J::Object(o) if o.len() == 1 => match o.iter().next() {
            Some((k, J::String(s))) if k == "$oid" => (s.clone(), ValueType::ObjectId),
            Some((k, J::String(s))) if k == "$date" => (s.clone(), ValueType::Date),
            Some((k, J::String(s)))
                if matches!(
                    k.as_str(),
                    "$numberLong" | "$numberInt" | "$numberDecimal" | "$numberDouble"
                ) =>
            {
                (s.clone(), ValueType::Number)
            }
            _ => return None,
        },
        _ => return None,
    })
}

fn cond(field: &str, op: Op) -> Condition {
    Condition {
        field: field.to_owned(),
        op,
        ..Condition::default()
    }
}

/// Builder conditions for one `field: spec` entry.
fn field_conditions(field: &str, spec: &J) -> Option<Vec<Condition>> {
    let ops = match spec {
        J::Object(o) if o.keys().all(|k| k.starts_with('$')) && value_text(spec).is_none() => o,
        literal => {
            let (value, value_type) = value_text(literal)?;
            let mut c = cond(
                field,
                if value_type == ValueType::Null {
                    Op::IsNull
                } else {
                    Op::Eq
                },
            );
            if value_type == ValueType::Null {
                // `{f: null}` also matches missing fields; keep it in text.
                return None;
            }
            c.value = value;
            c.value_type = value_type;
            return Some(vec![c]);
        }
    };
    if let Some(J::Object(r)) = ops.get("$regularExpression") {
        let mut c = cond(field, Op::Regex);
        c.value = r.get("pattern")?.as_str()?.to_owned();
        c.case_insensitive = r
            .get("options")
            .and_then(J::as_str)
            .is_some_and(|o| o.contains('i'));
        return (ops.len() == 1).then_some(vec![c]);
    }
    if let Some(p) = ops.get("$regex") {
        let mut c = cond(field, Op::Regex);
        c.value = p.as_str()?.to_owned();
        c.case_insensitive = ops
            .get("$options")
            .and_then(J::as_str)
            .is_some_and(|o| o.contains('i'));
        let extra = ops
            .keys()
            .filter(|k| *k != "$regex" && *k != "$options")
            .count();
        return (extra == 0).then_some(vec![c]);
    }
    if ops.len() == 2
        && let (Some(lo), Some(hi)) = (ops.get("$gte"), ops.get("$lte"))
    {
        let (a, ta) = value_text(lo)?;
        let (b, tb) = value_text(hi)?;
        if ta == tb {
            let mut c = cond(field, Op::Between);
            c.value = a;
            c.value2 = b;
            c.value_type = ta;
            return Some(vec![c]);
        }
    }
    let mut out = Vec::new();
    for (k, v) in ops {
        let c = match (k.as_str(), v) {
            ("$exists", J::Bool(true)) => cond(field, Op::Exists),
            ("$exists", J::Bool(false)) => cond(field, Op::Missing),
            ("$type", J::String(t)) if t == "null" => cond(field, Op::IsNull),
            ("$type", J::String(t)) => Condition {
                value: t.clone(),
                ..cond(field, Op::Type)
            },
            ("$ne", J::Null) => cond(field, Op::NotNull),
            ("$size", J::Number(n)) => Condition {
                value: n.to_string(),
                ..cond(field, Op::Size)
            },
            ("$elemMatch", inner) => Condition {
                value: to_shell(inner),
                ..cond(field, Op::ElemMatch)
            },
            ("$in" | "$nin", J::Array(items)) => {
                let texts = items.iter().map(value_text).collect::<Option<Vec<_>>>()?;
                let ty = texts.first().map_or(ValueType::String, |(_, t)| *t);
                if texts.iter().any(|(t, vt)| *vt != ty || t.contains(',')) {
                    return None;
                }
                Condition {
                    value: texts
                        .into_iter()
                        .map(|(t, _)| t)
                        .collect::<Vec<_>>()
                        .join(", "),
                    value_type: ty,
                    ..cond(field, if k == "$in" { Op::In } else { Op::Nin })
                }
            }
            (name, literal) => {
                let op = match name {
                    "$eq" => Op::Eq,
                    "$ne" => Op::Ne,
                    "$gt" => Op::Gt,
                    "$gte" => Op::Gte,
                    "$lt" => Op::Lt,
                    "$lte" => Op::Lte,
                    _ => return None,
                };
                let (value, value_type) = value_text(literal)?;
                Condition {
                    value,
                    value_type,
                    ..cond(field, op)
                }
            }
        };
        out.push(c);
    }
    Some(out)
}

/// The builder form of a filter document, or `None` when part of it has no
/// builder form (it then stays in the text editor).
pub fn from_filter(filter: &J) -> Option<Group> {
    let J::Object(map) = filter else { return None };
    let mut group = Group::default();
    if map.len() == 1
        && let Some((k, J::Array(items))) = map.iter().next()
        && (k == "$and" || k == "$or")
    {
        group.any = k == "$or";
        for item in items {
            let inner = from_filter(item)?;
            // A one-condition item is just that condition.
            if inner.children.len() == 1 {
                group.children.extend(inner.children);
            } else {
                group.children.push(Node::Group(inner));
            }
        }
        return Some(group);
    }
    for (k, v) in map {
        match (k.as_str(), v) {
            ("$and" | "$or", J::Array(_)) => {
                let mut single = Map::new();
                single.insert(k.clone(), v.clone());
                group
                    .children
                    .push(Node::Group(from_filter(&J::Object(single))?));
            }
            (name, _) if name.starts_with('$') => return None,
            (field, spec) => {
                for c in field_conditions(field, spec)? {
                    group.children.push(Node::Condition(c));
                }
            }
        }
    }
    Some(group)
}

// ---------------------------------------------------------------- sort

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SortField {
    pub field: String,
    pub descending: bool,
}

pub fn sort_to_json(keys: &[SortField]) -> J {
    let mut m = Map::new();
    for k in keys.iter().filter(|k| !k.field.trim().is_empty()) {
        m.insert(
            k.field.trim().to_owned(),
            J::from(if k.descending { -1 } else { 1 }),
        );
    }
    J::Object(m)
}

pub fn sort_from_json(v: &J) -> Option<Vec<SortField>> {
    v.as_object()?
        .iter()
        .map(|(k, d)| match d.as_i64() {
            Some(1) => Some(SortField {
                field: k.clone(),
                descending: false,
            }),
            Some(-1) => Some(SortField {
                field: k.clone(),
                descending: true,
            }),
            _ => None,
        })
        .collect()
}

// ---------------------------------------------------------- projection

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Projection {
    /// Hide the listed fields instead of showing only them.
    pub exclude: bool,
    pub fields: Vec<String>,
}

pub fn projection_to_json(p: &Projection) -> J {
    let v = if p.exclude { 0 } else { 1 };
    J::Object(
        p.fields
            .iter()
            .filter(|f| !f.trim().is_empty())
            .map(|f| (f.trim().to_owned(), J::from(v)))
            .collect(),
    )
}

pub fn projection_from_json(v: &J) -> Option<Projection> {
    let map = v.as_object()?;
    let mut p = Projection::default();
    let flags: Vec<(String, bool)> = map
        .iter()
        .map(|(k, v)| {
            let on = match v {
                J::Bool(b) => Some(*b),
                J::Number(n) => n.as_f64().map(|f| f != 0.0),
                _ => None,
            }?;
            Some((k.clone(), on))
        })
        .collect::<Option<_>>()?;
    let shown: Vec<_> = flags
        .iter()
        .filter(|(_, on)| *on)
        .map(|(k, _)| k.clone())
        .collect();
    let hidden: Vec<_> = flags
        .iter()
        .filter(|(_, on)| !*on)
        .map(|(k, _)| k.clone())
        .collect();
    if shown.is_empty() {
        p.exclude = true;
        p.fields = hidden;
    } else if hidden.iter().all(|k| k == "_id") && hidden.is_empty() {
        p.fields = shown;
    } else {
        return None;
    }
    Some(p)
}

// --------------------------------------------------------- shell output

fn ident(k: &str) -> bool {
    let mut chars = k.chars();
    chars
        .next()
        .is_some_and(|c| c.is_alphabetic() || c == '_' || c == '$')
        && k.chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '$')
}

/// Renders typed JSON the way mongosh and Compass show queries:
/// unquoted keys, `ObjectId("…")`, `ISODate("…")`, `/regex/i`.
pub fn to_shell(v: &J) -> String {
    match v {
        J::Object(o) if o.is_empty() => "{}".into(),
        J::Object(o) if o.len() == 1 => {
            let (k, inner) = o
                .iter()
                .next()
                .map(|(k, v)| (k.as_str(), v))
                .unwrap_or(("", &J::Null));
            match (k, inner) {
                ("$oid", J::String(s)) => format!("ObjectId({})", J::String(s.clone())),
                ("$date", J::String(s)) => format!("ISODate({})", J::String(s.clone())),
                ("$numberLong", J::String(s)) => format!("NumberLong({s})"),
                ("$numberDecimal", J::String(s)) => {
                    format!("NumberDecimal({})", J::String(s.clone()))
                }
                ("$regularExpression", J::Object(r)) => format!(
                    "/{}/{}",
                    r.get("pattern")
                        .and_then(J::as_str)
                        .unwrap_or_default()
                        .replace('/', "\\/"),
                    r.get("options").and_then(J::as_str).unwrap_or_default()
                ),
                _ => object_shell(o),
            }
        }
        J::Object(o) => object_shell(o),
        J::Array(items) => format!(
            "[{}]",
            items.iter().map(to_shell).collect::<Vec<_>>().join(", ")
        ),
        other => other.to_string(),
    }
}

fn object_shell(o: &Map<String, J>) -> String {
    let parts: Vec<String> = o
        .iter()
        .map(|(k, v)| {
            let key = if ident(k) {
                k.clone()
            } else {
                J::String(k.clone()).to_string()
            };
            format!("{key}: {}", to_shell(v))
        })
        .collect();
    format!("{{ {} }}", parts.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(field: &str, op: Op, value: &str) -> Node {
        Node::Condition(Condition {
            field: field.into(),
            op,
            value: value.into(),
            ..Condition::default()
        })
    }

    fn none(_: &str) -> Option<ValueType> {
        None
    }

    #[test]
    fn builds_filters_with_groups_and_operators() {
        let g = Group {
            any: false,
            children: vec![
                c("age", Op::Gte, "30"),
                Node::Group(Group {
                    any: true,
                    children: vec![c("city", Op::Eq, "Jeddah"), c("city", Op::Eq, "Riyadh")],
                }),
                c("email", Op::Exists, ""),
                c("tags", Op::In, "a, b"),
            ],
        };
        let f = to_filter(&g, &none).unwrap();
        assert_eq!(
            f,
            json!({"$and": [
                {"age": {"$gte": 30}},
                {"$or": [{"city": "Jeddah"}, {"city": "Riyadh"}]},
                {"email": {"$exists": true}},
                {"tags": {"$in": ["a", "b"]}}
            ]})
        );
        // And back.
        assert_eq!(to_filter(&from_filter(&f).unwrap(), &none).unwrap(), f);
    }

    #[test]
    fn text_fields_keep_numbers_as_text() {
        let g = Group {
            any: false,
            children: vec![c("phoneNumber", Op::Eq, "966123456789")],
        };
        let text = |f: &str| (f == "phoneNumber").then_some(ValueType::String);
        assert_eq!(
            to_filter(&g, &text).unwrap(),
            json!({"phoneNumber": "966123456789"})
        );
        assert_eq!(
            to_filter(&g, &none).unwrap(),
            json!({"phoneNumber": 966123456789_i64})
        );
    }

    #[test]
    fn typed_values_validate() {
        assert_eq!(
            typed_value("d", "2026-01-02", ValueType::Date, None).unwrap(),
            json!({"$date": "2026-01-02T00:00:00Z"})
        );
        assert_eq!(
            typed_value(
                "i",
                "ObjectId('65134b87d147eab2a5d15df6')",
                ValueType::ObjectId,
                None
            )
            .unwrap(),
            json!({"$oid": "65134b87d147eab2a5d15df6"})
        );
        assert!(typed_value("i", "nothex", ValueType::ObjectId, None).is_err());
        assert!(typed_value("n", "abc", ValueType::Number, None).is_err());
        assert_eq!(
            typed_value("s", "'quoted'", ValueType::String, None).unwrap(),
            json!("quoted")
        );
        assert_eq!(
            typed_value("a", "true", ValueType::Auto, None).unwrap(),
            json!(true)
        );
        assert_eq!(
            typed_value("a", "plain words", ValueType::Auto, None).unwrap(),
            json!("plain words")
        );
    }

    #[test]
    fn regex_between_and_special_operators_round_trip() {
        let g = Group {
            any: true,
            children: vec![
                Node::Condition(Condition {
                    field: "name".into(),
                    op: Op::Regex,
                    value: "^ah".into(),
                    case_insensitive: true,
                    ..Condition::default()
                }),
                Node::Condition(Condition {
                    field: "age".into(),
                    op: Op::Between,
                    value: "20".into(),
                    value2: "40".into(),
                    ..Condition::default()
                }),
                c("deleted", Op::Missing, ""),
                c("note", Op::IsNull, ""),
                c("items", Op::Size, "3"),
                c("items", Op::ElemMatch, "{qty: {$gt: 2}}"),
            ],
        };
        let f = to_filter(&g, &none).unwrap();
        let back = from_filter(&f).unwrap();
        assert_eq!(to_filter(&back, &none).unwrap(), f);
        // Shell regex literals come back as regex conditions.
        let parsed = shell::parse("{name: /^ah/i}").unwrap();
        let g = from_filter(&parsed).unwrap();
        assert!(
            matches!(&g.children[0], Node::Condition(c) if c.op == Op::Regex && c.case_insensitive)
        );
    }

    #[test]
    fn unsupported_filters_stay_in_text() {
        assert!(from_filter(&json!({"$where": "this.a > 1"})).is_none());
        assert!(from_filter(&json!({"a": {"$geoWithin": {}}})).is_none());
        assert!(
            from_filter(&json!({"a": null})).is_none(),
            "null also matches missing"
        );
        assert_eq!(from_filter(&json!({})).unwrap(), Group::default());
    }

    #[test]
    fn sort_and_projection_models() {
        let keys = vec![
            SortField {
                field: "a".into(),
                descending: false,
            },
            SortField {
                field: "b".into(),
                descending: true,
            },
        ];
        let j = sort_to_json(&keys);
        assert_eq!(j, json!({"a": 1, "b": -1}));
        assert_eq!(sort_from_json(&j).unwrap(), keys);
        let p = Projection {
            exclude: false,
            fields: vec!["name".into(), "email".into()],
        };
        assert_eq!(projection_from_json(&projection_to_json(&p)).unwrap(), p);
        let hide = Projection {
            exclude: true,
            fields: vec!["secret".into()],
        };
        assert_eq!(
            projection_from_json(&projection_to_json(&hide)).unwrap(),
            hide
        );
        assert!(projection_from_json(&json!({"a": 1, "b": 0})).is_none());
    }

    #[test]
    fn shell_output_parses_back() {
        let f = json!({"_id": {"$oid": "65134b87d147eab2a5d15df6"}, "at": {"$gte": {"$date": "2026-01-01T00:00:00Z"}}, "a.b": 1, "n": {"$regularExpression": {"pattern": "^x/y", "options": "i"}}});
        let text = to_shell(&f);
        assert_eq!(
            text,
            r#"{ _id: ObjectId("65134b87d147eab2a5d15df6"), at: { $gte: ISODate("2026-01-01T00:00:00Z") }, "a.b": 1, n: /^x\/y/i }"#
        );
        assert_eq!(shell::parse(&text).unwrap(), f);
    }
}
