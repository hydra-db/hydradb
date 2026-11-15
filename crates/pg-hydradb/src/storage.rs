use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use hydradb_pg_graph_core::GraphError;
use pgrx::{pg_sys, JsonB};
use serde::Deserialize;

use crate::adjacency::{source_error, AdjacencyGraph};

/// A query-time refusal, counted in the usage report under `code`.
fn refused(code: &'static str, message: impl std::fmt::Display) -> GraphError {
    crate::usage::fail(code);
    source_error(message)
}
use crate::keys::{KeyKind, Label};
use crate::spi::select_one;
use crate::Options;

pub(crate) const MAX_SNAPSHOT_TEXT_BYTES: usize = 64 * 1024;
pub(crate) const VERTEX_ROW_BYTES: usize = 128;
pub(crate) const EDGE_ROW_BYTES: usize = 192;
pub(crate) const INTEGER_BYTES: usize = 8;

/// A graph's catalog rows, tables in label order.
#[derive(Deserialize)]
struct Declaration {
    vertex_tables: Vec<VertexDeclaration>,
    edge_tables: Vec<EdgeDeclaration>,
}

#[derive(Deserialize)]
struct VertexDeclaration {
    label: String,
    relation: u32,
    key: String,
    sort_key: Option<String>,
}

#[derive(Deserialize)]
struct EdgeDeclaration {
    label: String,
    relation: u32,
    key: String,
    source: String,
    source_label: String,
    target: String,
    target_label: String,
    kind: Option<String>,
}

/// A declared table's label and key type, from the current catalog.
struct SourceTable {
    label: String,
    key_kind: KeyKind,
}

struct EdgeSource {
    table: SourceTable,
    source_label: u16,
    target_label: u16,
    kind: bool,
}

/// The key and endpoint types whose text form identifies a value, so keys
/// compare as text exactly as PostgreSQL compares them; `create_graph` accepts
/// no other.
const KEY_TYPES: [pg_sys::Oid; 7] = [
    pg_sys::INT2OID,
    pg_sys::INT4OID,
    pg_sys::INT8OID,
    pg_sys::TEXTOID,
    pg_sys::VARCHAROID,
    pg_sys::BPCHAROID,
    pg_sys::UUIDOID,
];

fn is_integer(type_oid: pg_sys::Oid) -> bool {
    [pg_sys::INT2OID, pg_sys::INT4OID, pg_sys::INT8OID].contains(&type_oid)
}

/// Column types whose text form does not depend on session settings.
fn session_independent(type_oid: pg_sys::Oid) -> bool {
    [
        pg_sys::INT2OID,
        pg_sys::INT4OID,
        pg_sys::INT8OID,
        pg_sys::TEXTOID,
        pg_sys::VARCHAROID,
        pg_sys::BPCHAROID,
        pg_sys::UUIDOID,
    ]
    .contains(&type_oid)
}

/// What `open_graph` reads from the catalogs to open an indexed graph from the
/// adjacency tables, kept for the backend's later calls. Its declaration is
/// fixed for a registration, and the rest comes from the definitions of its
/// source relations and the adjacency table, any change to which PostgreSQL
/// announces with a relcache invalidation that drops the entry. The
/// registration, the caller's privileges and whether row-level security
/// applies to the caller are checked on every call: the caller can change
/// within a backend without an invalidation.
struct CachedGraph {
    registration: i64,
    /// Owner of the adjacency tables, whom lookups run as.
    owner: pg_sys::Oid,
    /// Source relations in OID order, each with its schema.
    relations: Vec<(pg_sys::Oid, pg_sys::Oid)>,
    adjacency: pg_sys::Oid,
    vertex_labels: Vec<Label>,
    edge_labels: Vec<Label>,
    edge_ends: Vec<(u16, u16)>,
    kind_column: bool,
}

impl CachedGraph {
    fn depends_on(&self, relation: pg_sys::Oid) -> bool {
        relation == self.adjacency || self.relations.iter().any(|&(source, _)| source == relation)
    }

    fn open<C>(&self, options: &Options<C>) -> Box<AdjacencyGraph> {
        Box::new(AdjacencyGraph::new(
            self.registration,
            self.owner,
            self.vertex_labels.clone(),
            self.edge_labels.clone(),
            self.edge_ends.clone(),
            options.max_snapshot_bytes,
            self.kind_column,
        ))
    }
}

thread_local! {
    static GRAPHS: RefCell<HashMap<String, Rc<CachedGraph>>> = RefCell::new(HashMap::new());
    /// Set when an invalidation arrives while `GRAPHS` is borrowed, so the
    /// next lookup drops every entry.
    static FORGET_ALL: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static CALLBACK_REGISTERED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Relcache invalidation callback: drops the graphs that depend on `relation`,
/// or all of them when PostgreSQL resets the whole cache.
unsafe extern "C-unwind" fn forget_graphs(_arg: pg_sys::Datum, relation: pg_sys::Oid) {
    let forgotten = GRAPHS.with(|graphs| {
        let Ok(mut graphs) = graphs.try_borrow_mut() else {
            return false;
        };
        if relation == pg_sys::InvalidOid {
            graphs.clear();
        } else {
            graphs.retain(|_, graph| !graph.depends_on(relation));
        }
        true
    });
    if !forgotten {
        FORGET_ALL.set(true);
    }
}

fn remember_graph(name: &str, graph: Rc<CachedGraph>) {
    if !CALLBACK_REGISTERED.replace(true) {
        // SAFETY: registers a callback for the life of the backend; it only
        // touches this module's thread-local state.
        unsafe { pg_sys::CacheRegisterRelcacheCallback(Some(forget_graphs), pg_sys::Datum::from(0usize)) };
    }
    GRAPHS.with_borrow_mut(|graphs| graphs.insert(name.to_owned(), graph));
}

/// Fails unless the caller may read every source and use its schema, as
/// `has_table_privilege` and `has_schema_privilege` would answer.
fn check_privileges(relations: &[(pg_sys::Oid, pg_sys::Oid)]) -> Result<(), GraphError> {
    // SAFETY: plain catalog lookups for relations the caller has locked.
    let allowed = unsafe {
        let user = pg_sys::GetUserId();
        relations.iter().all(|&(relation, schema)| {
            pg_sys::pg_class_aclcheck(relation, user, pg_sys::ACL_SELECT as pg_sys::AclMode) == pg_sys::AclResult::ACLCHECK_OK
                && pg_sys::object_aclcheck(pg_sys::NamespaceRelationId, schema, user, pg_sys::ACL_USAGE as pg_sys::AclMode)
                    == pg_sys::AclResult::ACLCHECK_OK
        })
    };
    if !allowed {
        return Err(refused(
            "privileges",
            "SELECT and schema USAGE privileges on every graph source are required",
        ));
    }
    Ok(())
}

/// The first source row-level security would filter for the caller, as
/// `row_security_active` would answer. The adjacency tables hold every row, so
/// they cannot answer such a caller.
fn row_security_source(relations: &[(pg_sys::Oid, pg_sys::Oid)]) -> Option<pg_sys::Oid> {
    // SAFETY: plain catalog lookups for relations the caller has locked.
    relations.iter().map(|&(relation, _)| relation).find(|&relation| unsafe {
        pg_sys::check_enable_rls(relation, pg_sys::InvalidOid, true) == pg_sys::CheckEnableRlsResult::RLS_ENABLED as i32
    })
}

/// The cached graph `name`, if it is still the registered one. Its sources are
/// locked first, which processes pending invalidations, so a definition
/// changed by a committed transaction drops the entry before it is used.
fn cached_graph(name: &str) -> Result<Option<Rc<CachedGraph>>, GraphError> {
    if FORGET_ALL.replace(false) {
        GRAPHS.with_borrow_mut(HashMap::clear);
    }
    let Some(graph) = GRAPHS.with_borrow(|graphs| graphs.get(name).cloned()) else {
        return Ok(None);
    };
    for &(relation, _) in &graph.relations {
        // SAFETY: takes the lock the full path takes; held to transaction end.
        unsafe { pg_sys::LockRelationOid(relation, pg_sys::AccessShareLock as pg_sys::LOCKMODE) };
    }
    let current = GRAPHS.with_borrow(|graphs| graphs.get(name).is_some_and(|entry| Rc::ptr_eq(entry, &graph)));
    if !current || FORGET_ALL.get() {
        return Ok(None);
    }
    if crate::adjacency::registration(name, graph.owner)? != Some(graph.registration) {
        GRAPHS.with_borrow_mut(|graphs| graphs.remove(name));
        return Ok(None);
    }
    check_privileges(&graph.relations)?;
    if row_security_source(&graph.relations).is_some() {
        return Ok(None);
    }
    Ok(Some(graph))
}

/// Opens a graph for one query, read from the adjacency tables. A graph they
/// cannot answer is an error that says why and what to do; nothing is loaded
/// in full from the source tables.
pub fn open_graph<C>(name: &str, options: &Options<C>) -> Result<Box<AdjacencyGraph>, GraphError> {
    if let Some(graph) = cached_graph(name)? {
        return Ok(graph.open(options));
    }
    let name_literal = pgrx::spi::quote_literal(name);
    let declaration_sql = format!(
        "SELECT jsonb_build_object(
            'vertex_tables', COALESCE((SELECT jsonb_agg(jsonb_build_object(
                'label', t.label, 'relation', t.relation::oid::bigint, 'key', t.key_column,
                'sort_key', t.sort_key_column) ORDER BY t.label COLLATE \"C\")
                FROM hydradb.graph_vertex_tables t WHERE t.graph = g.name), '[]'),
            'edge_tables', COALESCE((SELECT jsonb_agg(jsonb_build_object(
                'label', t.label, 'relation', t.relation::oid::bigint, 'key', t.key_column,
                'source', t.source_column, 'source_label', t.source_label,
                'target', t.target_column, 'target_label', t.target_label,
                'kind', t.kind_column) ORDER BY t.label COLLATE \"C\")
                FROM hydradb.graph_edge_tables t WHERE t.graph = g.name), '[]'))
        FROM hydradb.graphs g WHERE g.name = {name_literal}"
    );
    let declaration = select_one::<JsonB>(&declaration_sql)?
        .ok_or_else(|| refused("unknown_graph", "unknown graph"))?
        .0;
    let declaration: Declaration = serde_json::from_value(declaration).map_err(source_error)?;
    if declaration.vertex_tables.is_empty() || declaration.vertex_tables.len() > u16::MAX as usize {
        return Err(source_error("graph must have between one and 65535 vertex tables"));
    }
    if declaration.edge_tables.len() > u16::MAX as usize {
        return Err(source_error("graph has more than 65535 edge tables"));
    }

    // Every column the graph reads, by relation.
    let mut columns: Vec<(u32, &str)> = Vec::new();
    for table in &declaration.vertex_tables {
        columns.push((table.relation, &table.key));
        columns.extend(table.sort_key.as_deref().map(|column| (table.relation, column)));
    }
    for table in &declaration.edge_tables {
        columns.extend([
            (table.relation, table.key.as_str()),
            (table.relation, &table.source),
            (table.relation, &table.target),
        ]);
        columns.extend(table.kind.as_deref().map(|column| (table.relation, column)));
    }
    let mut relation_ids = columns.iter().map(|&(oid, _)| oid).collect::<Vec<_>>();
    relation_ids.sort_unstable();
    relation_ids.dedup();
    let relations = relation_ids
        .iter()
        .map(|&id| unsafe {
            let oid = pg_sys::Oid::from(id);
            pg_sys::LockRelationOid(oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
            pgrx::PgRelation::open(oid)
        })
        .collect::<Vec<_>>();
    // The first declared column whose type's text form depends on session
    // settings, which the adjacency tables could not compare.
    let mut session_dependent = None;
    let mut column_types = HashMap::new();
    let mut schemas = Vec::with_capacity(relations.len());
    let mut collations = HashMap::new();
    for (&id, relation) in relation_ids.iter().zip(&relations) {
        let class = unsafe { &*relation.rd_rel };
        schemas.push(class.relnamespace);
        let descriptor = relation.tuple_desc();
        for &(_, column) in columns.iter().filter(|&&(oid, _)| oid == id) {
            let attribute = descriptor
                .iter()
                .find(|attribute| !attribute.attisdropped && attribute.name() == column)
                .ok_or_else(|| source_error(format!("graph source {} has no column {column:?}", relation.name())))?;
            if !session_independent(attribute.atttypid) && session_dependent.is_none() {
                session_dependent = Some(format!("column {column} of {}", relation.name()));
            }
            column_types.insert((id, column), attribute.atttypid);
            collations.insert((id, column), attribute.attcollation);
        }
    }
    // create_graph's type checks, again: a column can change type after
    // registration, and an endpoint read as its key's type would then round
    // to the wrong vertex.
    let relation_names = relation_ids
        .iter()
        .zip(&relations)
        .map(|(&id, relation)| (id, relation.name()))
        .collect::<HashMap<_, _>>();
    let named = |oid: u32, column: &str| {
        let relation = relation_names.get(&oid).copied().unwrap_or_default();
        format!("column {column} of {relation}")
    };
    let key_type = |oid: u32, column: &str| -> Result<pg_sys::Oid, GraphError> {
        let type_oid = column_types[&(oid, column)];
        if !KEY_TYPES.contains(&type_oid) {
            return Err(refused(
                "unsupported_type",
                format!(
                    "{} is a graph key or endpoint of a type whose text form does not identify its value; \
                 keys and endpoints must be integer, text, varchar, char or uuid",
                    named(oid, column)
                ),
            ));
        }
        let collation = collations[&(oid, column)];
        // SAFETY: a catalog lookup of a collation the locked column uses.
        if collation != pg_sys::InvalidOid && !unsafe { pg_sys::get_collation_isdeterministic(collation) } {
            return Err(refused(
                "nondeterministic_collation",
                format!(
                    "{} is a graph key or endpoint with a nondeterministic collation, under which differently spelled keys are equal",
                    named(oid, column)
                ),
            ));
        }
        Ok(type_oid)
    };
    for table in &declaration.vertex_tables {
        key_type(table.relation, &table.key)?;
    }
    for table in &declaration.edge_tables {
        key_type(table.relation, &table.key)?;
        for (column, label) in [(&table.source, &table.source_label), (&table.target, &table.target_label)] {
            let vertex = declaration
                .vertex_tables
                .iter()
                .find(|vertex| &vertex.label == label)
                .ok_or_else(|| source_error(format!("edge endpoint names unknown vertex table {label:?}")))?;
            let (endpoint, key) = (key_type(table.relation, column)?, key_type(vertex.relation, &vertex.key)?);
            if endpoint != key && !(is_integer(endpoint) && is_integer(key)) {
                return Err(refused(
                    "endpoint_type_mismatch",
                    format!(
                        "{} no longer has the type of the key {} it references; register the graph again",
                        named(table.relation, column),
                        named(vertex.relation, &vertex.key)
                    ),
                ));
            }
        }
    }
    let oids = pgrx::spi::quote_literal(format!(
        "{{{}}}",
        relation_ids.iter().map(u32::to_string).collect::<Vec<_>>().join(",")
    ));
    // A source qualifies for the adjacency tables while it passes
    // hydradb.source_unfit and its four triggers still run.
    let sql = format!(
        "SELECT jsonb_build_object(
            'registration', g.registration,
            'needs_refresh', g.needs_refresh,
            'owner', (SELECT relowner FROM pg_class WHERE oid = 'hydradb.graph_adjacency'::regclass)::bigint,
            'adjacency', 'hydradb.graph_adjacency'::regclass::oid::bigint,
            'relations', (SELECT jsonb_agg(jsonb_build_object(
                'oid', c.oid::bigint,
                'allowed', has_table_privilege(c.oid, 'SELECT') AND has_schema_privilege(c.relnamespace, 'USAGE'),
                'unfit', hydradb.source_unfit(c.oid),
                'triggers', CASE WHEN (SELECT count(*) FROM pg_trigger t WHERE t.tgrelid = c.oid
                        AND t.tgname IN (SELECT 'pg_hydradb_' || md5(g.name) || suffix
                            FROM unnest(ARRAY['_i', '_d', '_r', '_t']) suffix)
                        AND t.tgenabled = 'A'
                        AND t.tgfoid IN ('hydradb.maintain_rows()'::regprocedure)
                        AND t.tgnargs = 1 AND t.tgargs = convert_to(g.name, 'UTF8') || decode('00', 'hex')) <> 4
                    THEN format('%s does not have the four enabled pg_hydradb triggers that keep the adjacency tables current', c.oid::regclass)
                    END)
                ORDER BY c.oid)
                FROM pg_class c WHERE c.oid = ANY({oids}::oid[])))
        FROM hydradb.graphs g WHERE g.name = {name_literal}"
    );
    let metadata = select_one::<JsonB>(&sql)?
        .ok_or_else(|| refused("unknown_graph", "unknown graph or dropped source relation"))?
        .0;
    let described = metadata["relations"].as_array().map_or(&[][..], Vec::as_slice);
    if described.len() != relation_ids.len() {
        return Err(refused("unknown_graph", "unknown graph or dropped source relation"));
    }
    if !described.iter().all(|relation| relation["allowed"].as_bool() == Some(true)) {
        return Err(refused(
            "privileges",
            "SELECT and schema USAGE privileges on every graph source are required",
        ));
    }
    let refresh = pgrx::spi::quote_literal(name);
    if let Some(reason) = metadata["needs_refresh"].as_str() {
        return Err(refused(
            "needs_refresh",
            format!("graph {name} cannot be queried until hydradb.refresh_graph({refresh}) rebuilds its adjacency tables: {reason}"),
        ));
    }
    let unfit = described
        .iter()
        .find_map(|relation| relation["unfit"].as_str().map(|reason| ("source_unfit", reason)));
    let untriggered = || {
        described
            .iter()
            .find_map(|relation| relation["triggers"].as_str().map(|reason| ("triggers_disabled", reason)))
    };
    if let Some((code, reason)) = unfit.or_else(untriggered) {
        return Err(refused(
            code,
            format!("graph {name} cannot be queried: {reason}; fix that, then run hydradb.refresh_graph({refresh})"),
        ));
    }
    if let Some(column) = session_dependent {
        return Err(refused(
            "session_dependent_type",
            format!(
                "graph {name} cannot be queried: {column} has a type whose text form depends on session settings; \
             register the graph again over a key type that does not"
            ),
        ));
    }

    let table = |label: &str, oid: u32, key: &str| SourceTable {
        label: label.to_owned(),
        key_kind: KeyKind::of(column_types[&(oid, key)]),
    };
    let vertex_label = |name: &str| -> Result<u16, GraphError> {
        declaration
            .vertex_tables
            .iter()
            .position(|table| table.label == name)
            .map(|index| index as u16)
            .ok_or_else(|| source_error(format!("edge endpoint names unknown vertex table {name:?}")))
    };
    let vertex_tables = declaration
        .vertex_tables
        .iter()
        .map(|t| table(&t.label, t.relation, &t.key))
        .collect::<Vec<_>>();
    let edge_tables = declaration
        .edge_tables
        .iter()
        .map(|t| {
            Ok(EdgeSource {
                table: table(&t.label, t.relation, &t.key),
                source_label: vertex_label(&t.source_label)?,
                target_label: vertex_label(&t.target_label)?,
                kind: t.kind.is_some(),
            })
        })
        .collect::<Result<Vec<_>, GraphError>>()?;

    let sources = relation_ids
        .iter()
        .map(|&id| pg_sys::Oid::from(id))
        .zip(schemas)
        .collect::<Vec<_>>();
    let label = |table: &SourceTable| Label {
        name: table.label.clone(),
        kind: table.key_kind,
    };
    let registration = metadata["registration"]
        .as_i64()
        .ok_or_else(|| source_error("invalid graph registration"))?;
    let oid = |key: &str| {
        metadata[key]
            .as_u64()
            .and_then(|oid| u32::try_from(oid).ok())
            .map(pg_sys::Oid::from)
            .ok_or_else(|| source_error(format!("invalid graph metadata: {key}")))
    };
    let filtered = row_security_source(&sources);
    let graph = Rc::new(CachedGraph {
        registration,
        owner: oid("owner")?,
        relations: sources,
        adjacency: oid("adjacency")?,
        vertex_labels: vertex_tables.iter().map(label).collect(),
        edge_labels: edge_tables.iter().map(|t| label(&t.table)).collect(),
        edge_ends: edge_tables.iter().map(|t| (t.source_label, t.target_label)).collect(),
        kind_column: edge_tables.iter().any(|t| t.kind),
    });
    remember_graph(name, graph.clone());
    // The adjacency tables hold every row, so a caller row-level security
    // filters would see rows its policies hide.
    if let Some(relation) = filtered {
        let relation = relation_names.get(&u32::from(relation)).copied().unwrap_or_default();
        return Err(refused(
            "rls_caller",
            format!(
                "graph {name} cannot be queried by this role: row-level security on {relation} applies to it, and the \
             adjacency tables hold every row; query as a role the policies do not apply to"
            ),
        ));
    }
    Ok(graph.open(options))
}
