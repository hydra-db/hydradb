use std::fmt;

/// Backend-neutral identity of a graph node.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NodeId(pub u64);

/// Backend-neutral record identity of a relationship.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RelationshipId(pub u64);

/// Cypher relationship identity is either an identified stored record or a
/// structural relationship. These variants never compare as the same kind of
/// identity, even if their endpoints and type happen to match.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RelationshipIdentity {
    Structural {
        source: NodeId,
        destination: NodeId,
        relationship_type: String,
    },
    Identified(RelationshipId),
}

/// A relationship binding. The descriptive fields are carried for providers
/// and Arrow batches; semantic identity is defined by `identity`.
#[derive(Clone, Debug, PartialEq)]
pub struct RelationshipRef {
    identity: RelationshipIdentity,
    source: NodeId,
    destination: NodeId,
    relationship_type: String,
}

impl RelationshipRef {
    pub fn structural(source: NodeId, destination: NodeId, relationship_type: impl Into<String>) -> Self {
        let relationship_type = relationship_type.into();
        Self {
            identity: RelationshipIdentity::Structural {
                source,
                destination,
                relationship_type: relationship_type.clone(),
            },
            source,
            destination,
            relationship_type,
        }
    }

    pub fn identified(id: RelationshipId, source: NodeId, destination: NodeId, relationship_type: impl Into<String>) -> Self {
        Self {
            identity: RelationshipIdentity::Identified(id),
            source,
            destination,
            relationship_type: relationship_type.into(),
        }
    }

    pub fn identity(&self) -> &RelationshipIdentity {
        &self.identity
    }

    pub fn source(&self) -> NodeId {
        self.source
    }

    pub fn destination(&self) -> NodeId {
        self.destination
    }

    pub fn relationship_type(&self) -> &str {
        &self.relationship_type
    }
}

/// Direction in which a path traverses a relationship relative to the
/// relationship's stored source-to-destination direction.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum TraversalOrientation {
    Forward,
    Reverse,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PathRelationship {
    identity: RelationshipIdentity,
    orientation: TraversalOrientation,
}

impl PathRelationship {
    pub fn new(identity: RelationshipIdentity, orientation: TraversalOrientation) -> Self {
        Self { identity, orientation }
    }

    pub fn identity(&self) -> &RelationshipIdentity {
        &self.identity
    }

    pub fn orientation(&self) -> TraversalOrientation {
        self.orientation
    }
}

/// An oriented Cypher path. A valid path always contains one more node than
/// relationship; one node and no relationships is the zero-hop path.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Path {
    nodes: Vec<NodeId>,
    relationships: Vec<PathRelationship>,
}

impl Path {
    pub fn new(nodes: Vec<NodeId>, relationships: Vec<PathRelationship>) -> Result<Self, PathError> {
        if nodes.is_empty() {
            return Err(PathError::EmptyNodes);
        }
        if nodes.len() != relationships.len() + 1 {
            return Err(PathError::LengthMismatch {
                nodes: nodes.len(),
                relationships: relationships.len(),
            });
        }
        Ok(Self { nodes, relationships })
    }

    pub fn nodes(&self) -> &[NodeId] {
        &self.nodes
    }

    pub fn relationships(&self) -> &[PathRelationship] {
        &self.relationships
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PathError {
    #[error("a path must contain at least one node")]
    EmptyNodes,
    #[error("path has {nodes} nodes but {relationships} relationships")]
    LengthMismatch { nodes: usize, relationships: usize },
}

impl fmt::Display for NodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl fmt::Display for RelationshipId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}
