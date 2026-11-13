use crate::{Direction, Identifier, PathLength};

/// The §16.12 algebra before semantic restrictions or executable adaptation.
#[derive(Clone, Debug, PartialEq)]
pub enum SimplifiedPath {
    Label(Identifier),
    Alternatives { multiset: bool, operands: Vec<Self> },
    Concatenation(Vec<Self>),
    Conjunction(Vec<Self>),
    Quantified { primary: Box<Self>, length: PathLength },
    Questioned(Box<Self>),
    DirectionOverride { direction: Direction, primary: Box<Self> },
    Negated(Box<Self>),
}

impl SimplifiedPath {
    pub fn children(&self) -> &[Self] {
        match self {
            Self::Label(_) => &[],
            Self::Alternatives { operands, .. } | Self::Concatenation(operands) | Self::Conjunction(operands) => operands,
            Self::Quantified { primary, .. }
            | Self::Questioned(primary)
            | Self::DirectionOverride { primary, .. }
            | Self::Negated(primary) => std::slice::from_ref(primary.as_ref()),
        }
    }
}

/// The factor structure retained before a path's executable adapter flattens
/// it. Implicit nodes belong inside repeated edge primaries (§16.7 SR14).
/// Predicates' nested queries are separate graph-matching depths, not factors
/// of the containing path.
#[derive(Clone, Debug, PartialEq)]
pub enum PathFactor {
    Node(Option<Identifier>),
    Edge(Option<Identifier>),
    Simplified(SimplifiedPath),
    Concatenation(Vec<PathFactor>),
    Alternatives(Vec<PathFactor>),
    Quantified {
        primary: Box<PathFactor>,
        length: PathLength,
    },
    Questioned(Box<PathFactor>),
    Parenthesized {
        binding: Option<Identifier>,
        restrictive: bool,
        search_prefix_written: bool,
        inner: Box<PathFactor>,
    },
}

impl PathFactor {
    /// §16.7 SR14 b)–d), at this factor's own structural level.
    pub fn concatenation(factors: Vec<Self>) -> Self {
        let mut normalized = Vec::new();
        for factor in factors {
            if matches!(factor, Self::Edge(_)) && !matches!(normalized.last(), Some(Self::Node(_))) {
                normalized.push(Self::Node(None));
            }
            if matches!(normalized.last(), Some(Self::Edge(_))) && !matches!(factor, Self::Node(_)) {
                normalized.push(Self::Node(None));
            }
            normalized.push(factor);
        }
        if matches!(normalized.last(), Some(Self::Edge(_))) {
            normalized.push(Self::Node(None));
        }
        Self::Concatenation(normalized)
    }

    /// §16.7 SR14 a): normalize an edge primary before repetition weights.
    pub fn quantified(self, length: PathLength) -> Self {
        let primary = if matches!(self, Self::Edge(_)) {
            Self::concatenation(vec![self])
        } else {
            self
        };
        Self::Quantified {
            primary: Box::new(primary),
            length,
        }
    }

    pub fn has_written_search(&self) -> bool {
        match self {
            Self::Parenthesized {
                search_prefix_written,
                inner,
                ..
            } => *search_prefix_written || inner.has_written_search(),
            Self::Concatenation(parts) | Self::Alternatives(parts) => parts.iter().any(Self::has_written_search),
            Self::Quantified { primary, .. } | Self::Questioned(primary) => primary.has_written_search(),
            Self::Node(_) | Self::Edge(_) | Self::Simplified(_) => false,
        }
    }
}
