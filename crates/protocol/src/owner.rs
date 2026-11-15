//! The owner hint on a refused write.
//!
//! A node that does not own a cell refuses a write with the owner's node id
//! when it knows one (Bolt `NotALeader`, HTTP 421). Bolt has no field for it,
//! so it rides in the message text. This module is the only writer and the only
//! reader of that text: `GraphError::NotCellWriter`'s Display formats with
//! [`format_not_cell_writer`] and the proxy parses with [`parse_owner_hint`].

const OWNER_MARKER: &str = "; owner: ";
const UNKNOWN_OWNER: &str = "unknown";

/// The message of a refused write: `cell {cell} is not owned by this node;
/// owner: {owner|unknown}`.
pub fn format_not_cell_writer(cell_id: &str, owner: Option<&str>) -> String {
    format!(
        "cell {cell_id} is not owned by this node{OWNER_MARKER}{}",
        owner.unwrap_or(UNKNOWN_OWNER)
    )
}

/// The owner named by a refused write's message, `None` when the node had
/// nothing truthful to name or the text is not one of ours.
pub fn parse_owner_hint(message: &str) -> Option<&str> {
    let (_, owner) = message.rsplit_once(OWNER_MARKER)?;
    let owner = owner.trim();
    if owner.is_empty() || owner == UNKNOWN_OWNER {
        return None;
    }
    // Node ids use the key alphabet of `hydradb_scope::validate_component`,
    // repeated here since this crate does not depend on scope. Anything else
    // is not a hint a node wrote.
    owner
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
        .then_some(owner)
}
