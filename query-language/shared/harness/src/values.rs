use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Exact, tagged values; floating point bits preserve NaNs and signed zero in reports.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i128),
    Float(u64),
    String(String),
    List(Vec<Value>),
    Map(BTreeMap<String, Value>),
    Node(Node),
    Relationship(Relationship),
    Path(Path),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Node {
    pub labels: BTreeSet<String>,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Relationship {
    pub relationship_type: String,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Direction {
    Incoming,
    Outgoing,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Path {
    pub start: Node,
    pub segments: Vec<(Direction, Relationship, Node)>,
}

/// Parse a complete TCK literal, rejecting trailing text and malformed values.
pub fn parse(input: &str) -> Result<Value, String> {
    let mut p = Parser { rest: input };
    let result = p.value()?;
    p.ws();
    if !p.rest.is_empty() {
        return Err(format!("trailing value text: {}", p.rest));
    }
    Ok(result)
}
struct Parser<'a> {
    rest: &'a str,
}
impl Parser<'_> {
    fn ws(&mut self) {
        self.rest = self.rest.trim_start();
    }
    fn take(&mut self, token: &str) -> bool {
        self.ws();
        if let Some(s) = self.rest.strip_prefix(token) {
            self.rest = s;
            true
        } else {
            false
        }
    }
    fn require(&mut self, token: &str) -> Result<(), String> {
        if self.take(token) {
            Ok(())
        } else {
            Err(format!("expected {token:?} near {:?}", self.rest))
        }
    }
    fn char(&mut self) -> Option<char> {
        let ch = self.rest.chars().next()?;
        self.rest = &self.rest[ch.len_utf8()..];
        Some(ch)
    }
    fn string(&mut self, quote: char) -> Result<String, String> {
        if self.char() != Some(quote) {
            return Err("expected string".into());
        }
        let mut out = String::new();
        loop {
            match self.char().ok_or("unterminated string")? {
                c if c == quote => return Ok(out),
                '\\' => match self.char().ok_or("unterminated escape")? {
                    '\\' => out.push('\\'),
                    '\'' => out.push('\''),
                    '"' => out.push('"'),
                    'n' => out.push('\n'),
                    'r' => out.push('\r'),
                    't' => out.push('\t'),
                    'b' => out.push('\u{8}'),
                    'f' => out.push('\u{c}'),
                    'u' => {
                        let digits: String = (0..4)
                            .map(|_| self.char().ok_or("short unicode escape"))
                            .collect::<Result<_, _>>()?;
                        let n = u32::from_str_radix(&digits, 16).map_err(|_| "invalid unicode escape")?;
                        let n = if (0xd800..=0xdbff).contains(&n) {
                            if self.char() != Some('\\') || self.char() != Some('u') {
                                return Err("missing low surrogate".into());
                            }
                            let digits: String = (0..4).map(|_| self.char().ok_or("short low surrogate")).collect::<Result<_, _>>()?;
                            let low = u32::from_str_radix(&digits, 16).map_err(|_| "invalid low surrogate")?;
                            if !(0xdc00..=0xdfff).contains(&low) {
                                return Err("invalid low surrogate".into());
                            }
                            0x10000 + ((n - 0xd800) << 10) + (low - 0xdc00)
                        } else {
                            n
                        };
                        out.push(char::from_u32(n).ok_or("invalid unicode code point")?);
                    }
                    c => return Err(format!("unknown string escape: \\{c}")),
                },
                c => out.push(c),
            }
        }
    }
    fn identifier(&mut self) -> Result<String, String> {
        self.ws();
        if self.take("`") {
            let mut out = String::new();
            loop {
                match self.char().ok_or("unterminated identifier")? {
                    '`' if self.rest.starts_with('`') => {
                        self.char();
                        out.push('`');
                    }
                    '`' => return Ok(out),
                    c => out.push(c),
                }
            }
        }
        if self.rest.starts_with('\'') || self.rest.starts_with('"') {
            let q = self.rest.chars().next().unwrap();
            return self.string(q);
        }
        let len = self
            .rest
            .char_indices()
            .take_while(|(_, c)| c.is_alphanumeric() || *c == '_')
            .map(|(i, c)| i + c.len_utf8())
            .last()
            .unwrap_or(0);
        if len == 0 {
            return Err(format!("expected identifier near {:?}", self.rest));
        }
        let name = self.rest[..len].to_string();
        self.rest = &self.rest[len..];
        Ok(name)
    }
    fn map(&mut self) -> Result<BTreeMap<String, Value>, String> {
        self.require("{")?;
        let mut map = BTreeMap::new();
        if self.take("}") {
            return Ok(map);
        }
        loop {
            let key = self.identifier()?;
            self.require(":")?;
            let value = self.value()?;
            if map.insert(key, value).is_some() {
                return Err("duplicate map key".into());
            }
            if self.take("}") {
                return Ok(map);
            }
            self.require(",")?;
        }
    }
    fn node(&mut self) -> Result<Node, String> {
        self.require("(")?;
        let mut labels = BTreeSet::new();
        while self.take(":") {
            if !labels.insert(self.identifier()?) {
                return Err("duplicate node label".into());
            }
        }
        self.ws();
        let properties = if self.rest.starts_with('{') { self.map()? } else { BTreeMap::new() };
        self.require(")")?;
        Ok(Node { labels, properties })
    }
    fn relationship(&mut self) -> Result<Relationship, String> {
        self.require("[")?;
        self.require(":")?;
        let relationship_type = self.identifier()?;
        self.ws();
        let properties = if self.rest.starts_with('{') { self.map()? } else { BTreeMap::new() };
        self.require("]")?;
        Ok(Relationship {
            relationship_type,
            properties,
        })
    }
    fn path(&mut self) -> Result<Path, String> {
        self.require("<")?;
        let start = self.node()?;
        let mut segments = Vec::new();
        while !self.take(">") {
            let direction = if self.take("<-") {
                Direction::Incoming
            } else {
                self.require("-")?;
                Direction::Outgoing
            };
            let rel = self.relationship()?;
            match direction {
                Direction::Incoming => self.require("-")?,
                Direction::Outgoing => self.require("->")?,
            }
            let node = self.node()?;
            segments.push((direction, rel, node));
        }
        Ok(Path { start, segments })
    }
    fn value(&mut self) -> Result<Value, String> {
        self.ws();
        match self.rest.chars().next().ok_or("missing value")? {
            '\'' | '"' => {
                let q = self.rest.chars().next().unwrap();
                Ok(Value::String(self.string(q)?))
            }
            '{' => Ok(Value::Map(self.map()?)),
            '(' => Ok(Value::Node(self.node()?)),
            '<' => Ok(Value::Path(self.path()?)),
            '[' => {
                if self.rest[1..].trim_start().starts_with(':') {
                    return Ok(Value::Relationship(self.relationship()?));
                }
                self.require("[")?;
                let mut list = Vec::new();
                if self.take("]") {
                    return Ok(Value::List(list));
                }
                loop {
                    list.push(self.value()?);
                    if self.take("]") {
                        return Ok(Value::List(list));
                    }
                    self.require(",")?;
                }
            }
            _ => {
                let len = self
                    .rest
                    .find(|c: char| c.is_whitespace() || ",]}>)".contains(c))
                    .unwrap_or(self.rest.len());
                if len == 0 {
                    return Err("missing scalar".into());
                }
                let word = &self.rest[..len];
                self.rest = &self.rest[len..];
                match word {
                    "null" => Ok(Value::Null),
                    "true" => Ok(Value::Boolean(true)),
                    "false" => Ok(Value::Boolean(false)),
                    "NaN" => Ok(Value::Float(f64::NAN.to_bits())),
                    "Inf" | "+Inf" => Ok(Value::Float(f64::INFINITY.to_bits())),
                    "-Inf" => Ok(Value::Float(f64::NEG_INFINITY.to_bits())),
                    _ if word.contains(['.', 'e', 'E']) => word.parse::<f64>().map_err(|_| format!("invalid float {word}")).and_then(|n| {
                        if n.is_finite() {
                            Ok(Value::Float(n.to_bits()))
                        } else {
                            Err(format!("float out of range {word}"))
                        }
                    }),
                    _ => word
                        .parse::<i128>()
                        .map(Value::Integer)
                        .map_err(|_| format!("invalid integer {word}")),
                }
            }
        }
    }
}
