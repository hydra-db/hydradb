//! The Bolt database name of a scope.
//!
//! Bolt has no tenancy field, so a tenant and optional sub-tenant ride in the
//! database name: `{base}.scope1.{tenant}.{sub_tenant}`, each id as URL-safe
//! unpadded base64 and `_` for "no sub-tenant". The base database alone names
//! the process's root scope. The kernel's `HierarchicalClientDatabaseResolver`
//! (`src/client/service.rs`) builds on these functions, and so does the proxy,
//! so both resolve a database name to the same scope.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;

use crate::{encode_scope_namespace_id, NamespaceId, ScopeError, ScopeResult};

/// The version segment of a scoped database name.
pub const SCOPED_DATABASE_VERSION: &str = "scope1";

/// The sub-tenant segment that means "none".
pub const NO_SUB_TENANT: &str = "_";

/// The namespace segments a scoped database name adds under the root scope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScopedDatabase {
    pub tenant: NamespaceId,
    pub sub_tenant: Option<NamespaceId>,
}

/// The longest database name accepted, in bytes.
pub const MAX_DATABASE_NAME_BYTES: usize = 255;

/// A Bolt database name: trimmed, nonempty, at most
/// [`MAX_DATABASE_NAME_BYTES`], no control characters.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DatabaseName(String);

impl DatabaseName {
    /// Check a name that comes from outside: config, or a client's `db`.
    pub fn parse(database: &str) -> ScopeResult<Self> {
        let database = database.trim();
        if database.is_empty() || database.chars().any(char::is_control) {
            return Err(invalid_database(database.to_string()));
        }
        Self::within_limit(database.to_string())
    }

    fn within_limit(database: String) -> ScopeResult<Self> {
        if database.len() > MAX_DATABASE_NAME_BYTES {
            return Err(invalid_database(database));
        }
        Ok(Self(database))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for DatabaseName {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::borrow::Borrow<str> for DatabaseName {
    fn borrow(&self) -> &str {
        &self.0
    }
}

fn invalid_database(value: String) -> ScopeError {
    ScopeError::InvalidKeyComponent {
        component: "database",
        value,
    }
}

/// The scoped database name for a tenant and optional sub-tenant under
/// `base`. An empty sub-tenant is the same as none.
///
/// The segments are base64, so the name is valid by construction except for
/// its length: ids have no length bound, and a long one can push the name past
/// [`MAX_DATABASE_NAME_BYTES`].
pub fn scoped_database_name(base: &DatabaseName, tenant_id: &str, sub_tenant_id: Option<&str>) -> ScopeResult<DatabaseName> {
    let tenant = encode_scope_namespace_id("tenant_id", tenant_id)?;
    let sub_tenant = sub_tenant_id
        .filter(|value| !value.is_empty())
        .map(|value| encode_scope_namespace_id("sub_tenant_id", value))
        .transpose()?
        .map_or_else(|| NO_SUB_TENANT.to_string(), |encoded| encoded.to_string());
    DatabaseName::within_limit(format!("{base}.{SCOPED_DATABASE_VERSION}.{tenant}.{sub_tenant}"))
}

/// Parse a scoped database name back into its segments. `database` is not
/// the base database itself.
///
/// Anything that is not exactly what [`scoped_database_name`] would have
/// written is [`ScopeError::UnknownDatabase`], including base64 that decodes
/// but is not canonical, so two spellings never name one tenant.
pub fn parse_scoped_database(base: &DatabaseName, database: &DatabaseName) -> ScopeResult<ScopedDatabase> {
    let database = database.as_str();
    let prefix = format!("{base}.{SCOPED_DATABASE_VERSION}.");
    let encoded = database.strip_prefix(&prefix).ok_or_else(|| unknown_database(database))?;
    let (tenant, sub_tenant) = encoded
        .split_once('.')
        .filter(|(_, sub_tenant)| !sub_tenant.contains('.'))
        .ok_or_else(|| unknown_database(database))?;
    let tenant = NamespaceId::new(canonical_database_component(database, tenant)?)?;
    let sub_tenant = if sub_tenant == NO_SUB_TENANT {
        None
    } else {
        Some(NamespaceId::new(canonical_database_component(database, sub_tenant)?)?)
    };
    Ok(ScopedDatabase { tenant, sub_tenant })
}

fn canonical_database_component(database: &str, encoded: &str) -> ScopeResult<String> {
    if encoded.is_empty() {
        return Err(unknown_database(database));
    }
    let bytes = URL_SAFE_NO_PAD.decode(encoded).map_err(|_| unknown_database(database))?;
    let value = String::from_utf8(bytes).map_err(|_| unknown_database(database))?;
    if value.is_empty() || value.chars().any(char::is_control) || URL_SAFE_NO_PAD.encode(value.as_bytes()) != encoded {
        return Err(unknown_database(database));
    }
    Ok(encoded.to_string())
}

fn unknown_database(database: &str) -> ScopeError {
    ScopeError::UnknownDatabase {
        database: database.to_string(),
    }
}

/// Decode a scope segment written by [`scoped_database_name`], for display.
/// `None` for anything else, never a panic or an error.
///
/// The canonical round trip [`parse_scoped_database`] checks is not enough
/// here. That function already knows from the database prefix that the
/// segment should be encoded; this one does not, and short literals pass by
/// accident: `acme` is canonical base64 for two valid UTF-8 code points, so
/// the namespace `acme` would read as a tenant named `iʮ`.
///
/// The decoded value must also be printable ASCII, which rejects `acme` and
/// accepts every id and sub-tenant name the platform issues. The trade-off is
/// that a non-ASCII sub-tenant name shows as its base64 spelling.
pub fn decode_scope_id(encoded: &str) -> Option<String> {
    let bytes = URL_SAFE_NO_PAD.decode(encoded).ok()?;
    let value = String::from_utf8(bytes).ok()?;
    if value.is_empty()
        || !value.chars().all(|ch| ch.is_ascii_graphic() || ch == ' ')
        || URL_SAFE_NO_PAD.encode(value.as_bytes()) != encoded
    {
        return None;
    }
    Some(value)
}
