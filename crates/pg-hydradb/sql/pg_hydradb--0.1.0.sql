CREATE SCHEMA hydradb;
REVOKE CREATE ON SCHEMA hydradb FROM PUBLIC;
GRANT USAGE ON SCHEMA hydradb TO PUBLIC;

-- Numbers each registration. The adjacency tables below key their rows by it,
-- so a graph dropped and registered again under the same name never sees the
-- rows of the old one.
CREATE SEQUENCE hydradb.graph_registrations;

CREATE TABLE hydradb.graphs (
    name text PRIMARY KEY CHECK (name <> ''),
    registration bigint NOT NULL UNIQUE DEFAULT nextval('hydradb.graph_registrations'),
    -- Whether triggers keep hydradb.graph_vertices and hydradb.graph_adjacency
    -- current for this graph. create_graph refuses a graph it could not index,
    -- so this is true for every graph it registers.
    indexed boolean NOT NULL,
    -- Why an indexed graph's adjacency tables cannot be trusted until
    -- hydradb.refresh_graph rebuilds them, or NULL. While it is set, queries
    -- fail with it and the triggers do nothing.
    needs_refresh text
);
GRANT SELECT ON hydradb.graphs TO PUBLIC;

-- One row per table declared as vertices. `label` names the table inside the
-- graph; seeds and results identify a vertex by (label, key). `ordinal` is the
-- label's rank in byte order, which the adjacency tables store in its place.
CREATE TABLE hydradb.graph_vertex_tables (
    graph text NOT NULL REFERENCES hydradb.graphs ON DELETE CASCADE,
    label text NOT NULL CHECK (label <> ''),
    ordinal smallint NOT NULL,
    relation regclass NOT NULL,
    key_column name NOT NULL,
    sort_key_column name,
    -- What each declared column was at registration (hydradb.column_state),
    -- by role: an ALTER TABLE that changes any of it is refused. Collation
    -- names follow renames (hydradb.rerecord_states).
    state jsonb,
    PRIMARY KEY (graph, label)
);
GRANT SELECT ON hydradb.graph_vertex_tables TO PUBLIC;

-- One row per table declared as edges. Each endpoint column references the key
-- of exactly one vertex table. The label is the relationship type unless
-- `kind_column` names a column that holds it.
CREATE TABLE hydradb.graph_edge_tables (
    graph text NOT NULL REFERENCES hydradb.graphs ON DELETE CASCADE,
    label text NOT NULL CHECK (label <> ''),
    ordinal smallint NOT NULL,
    relation regclass NOT NULL,
    key_column name NOT NULL,
    source_column name NOT NULL,
    source_label text NOT NULL,
    target_column name NOT NULL,
    target_label text NOT NULL,
    kind_column name,
    -- As graph_vertex_tables.state.
    state jsonb,
    PRIMARY KEY (graph, label),
    FOREIGN KEY (graph, source_label) REFERENCES hydradb.graph_vertex_tables (graph, label),
    FOREIGN KEY (graph, target_label) REFERENCES hydradb.graph_vertex_tables (graph, label)
);
GRANT SELECT ON hydradb.graph_edge_tables TO PUBLIC;

-- Every vertex key of an indexed graph, numbered once with a vid that never
-- changes. `rows` counts the source rows with that key: 0 for a key that only
-- edges name, or whose row was deleted. check_mirrorable requires a unique
-- key, so it is more than 1 only inside a transaction that defers that key's
-- check. Queries treat anything but 1 as an error when they reach the vertex,
-- as reading its table would. Keys are stored as
-- the text PostgreSQL prints, which is the same for every integer type, and
-- compared bytewise (COLLATE "C"), as the query kernels compare them, whatever the
-- source column's collation.
CREATE SEQUENCE hydradb.vertex_ids;
CREATE TABLE hydradb.graph_vertices (
    graph bigint NOT NULL,
    label smallint NOT NULL,
    key text COLLATE "C" NOT NULL,
    vid bigint NOT NULL DEFAULT nextval('hydradb.vertex_ids'),
    rows integer NOT NULL DEFAULT 0,
    sort_key text,
    PRIMARY KEY (graph, label, key)
);
-- Sort keys can be long, and B-tree entries cannot, so they stay in the table.
CREATE UNIQUE INDEX graph_vertices_vid ON hydradb.graph_vertices (graph, vid) INCLUDE (label, key, rows);


-- One row per edge of an indexed graph, by vid. A traversal reads a vertex's
-- edges from one of the two covering indexes, without visiting the table on
-- all-visible pages. `kind` is NULL when the edge table has no kind column;
-- kinds can be longer than a B-tree entry allows, so a graph with a kind
-- column reads them from the table.
CREATE TABLE hydradb.graph_adjacency (
    graph bigint NOT NULL,
    label smallint NOT NULL,
    key text COLLATE "C" NOT NULL,
    source bigint NOT NULL,
    target bigint NOT NULL,
    kind text,
    PRIMARY KEY (graph, label, key)
);
CREATE INDEX graph_adjacency_out ON hydradb.graph_adjacency (graph, source) INCLUDE (target, label, key);
CREATE INDEX graph_adjacency_in ON hydradb.graph_adjacency (graph, target) INCLUDE (source, label, key);

-- Only the extension's own functions read and write the adjacency tables:
-- they would show rows of tables a caller may not read. Graph queries check
-- the caller's privileges on the source tables and then read them as their
-- owner.
REVOKE ALL ON hydradb.graph_vertices, hydradb.graph_adjacency FROM PUBLIC;

-- pg_dump includes these rows, as it does the source tables and their
-- triggers. A restore loads table data before it creates triggers, so the
-- adjacency tables must come from the dump rather than from the triggers.
SELECT pg_catalog.pg_extension_config_dump('hydradb.graphs', '');
SELECT pg_catalog.pg_extension_config_dump('hydradb.graph_vertex_tables', '');
SELECT pg_catalog.pg_extension_config_dump('hydradb.graph_edge_tables', '');
SELECT pg_catalog.pg_extension_config_dump('hydradb.graph_registrations', '');
SELECT pg_catalog.pg_extension_config_dump('hydradb.graph_vertices', '');
SELECT pg_catalog.pg_extension_config_dump('hydradb.graph_adjacency', '');
SELECT pg_catalog.pg_extension_config_dump('hydradb.vertex_ids', '');

-- Raised by the maintenance statements below for a declared field that is NULL.
CREATE FUNCTION hydradb.graph_null_field(graph_name text, label text, fields text) RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    RAISE EXCEPTION 'graph %: % of % must not be null', graph_name, fields, label
        USING ERRCODE = 'not_null_violation';
END
$$;
REVOKE ALL ON FUNCTION hydradb.graph_null_field(text, text, text) FROM PUBLIC;

-- The select list an UPDATE's old and new rows are compared by: each declared
-- column once, as its text form in the C collation, under its own name. Text
-- forms are what the adjacency tables store, and the C collation compares them
-- bytewise, so a nondeterministic collation that calls 'link' and 'LINK' equal
-- cannot hide a change; listing a column once lets one column fill two roles.
CREATE FUNCTION hydradb.compared_columns(column_names name[]) RETURNS text
LANGUAGE sql IMMUTABLE SET search_path = pg_catalog, pg_temp AS $$
    SELECT string_agg(format('(%1$I)::text COLLATE "C" AS %1$I', c), ', ' ORDER BY c)
    FROM (SELECT DISTINCT c FROM unnest(column_names) c WHERE c IS NOT NULL) declared
$$;
REVOKE ALL ON FUNCTION hydradb.compared_columns(name[]) FROM PUBLIC;

-- Returns, as text, the statements that apply rows of `source_relation` to an
-- indexed graph's adjacency tables, in order; it runs none of them, and
-- maintain_rows and fill_graph execute them. Each looks rows up by primary key,
-- as a per-row probe or `= ANY`, so a plan stays proportional to the rows
-- applied even when the adjacency tables' statistics are stale. `op` is 'i' to
-- add the rows of `rows_sql`, a FROM item aliased `r` (a transition table, or a
-- whole table for the initial fill), and 'd' to remove them. 'u' applies an
-- UPDATE from the transition tables `old_rows` and `new_rows`: per declaration
-- it removes the old rows and adds the new ones whose declared columns differ,
-- compared as multisets, so rows whose graph columns did not change cancel out.
-- Every statement takes the graph's registration as $1.
CREATE FUNCTION hydradb.adjacency_statements(graph_name text, source_relation oid, op "char", rows_sql text)
RETURNS SETOF text LANGUAGE plpgsql STABLE SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    declared record;
    columns text;
    pass "char";
    source text;
    sort_text text;
    kind_text text;
BEGIN
    FOR declared IN
        SELECT * FROM hydradb.graph_vertex_tables WHERE graph = graph_name AND relation = source_relation ORDER BY ordinal
    LOOP
        sort_text := CASE WHEN declared.sort_key_column IS NULL THEN 'NULL::text'
            ELSE format('(r.%I)::text', declared.sort_key_column) END;
        columns := hydradb.compared_columns(ARRAY[declared.key_column, declared.sort_key_column]);
        FOREACH pass IN ARRAY CASE op WHEN 'u' THEN ARRAY['d', 'i']::"char"[] ELSE ARRAY[op] END LOOP
            source := CASE WHEN op <> 'u' THEN rows_sql
                WHEN pass = 'd' THEN format('(SELECT %1$s FROM old_rows EXCEPT ALL SELECT %1$s FROM new_rows) r', columns)
                ELSE format('(SELECT %1$s FROM new_rows EXCEPT ALL SELECT %1$s FROM old_rows) r', columns) END;
            IF pass = 'i' THEN
                RETURN NEXT format('SELECT hydradb.graph_null_field(%L, %L, %L) FROM %s WHERE r.%I IS NULL%s LIMIT 1',
                    graph_name, declared.label, 'key and sort key', source, declared.key_column,
                    CASE WHEN declared.sort_key_column IS NULL THEN ''
                        ELSE format(' OR r.%I IS NULL', declared.sort_key_column) END);
                RETURN NEXT format('INSERT INTO hydradb.graph_vertices AS v (graph, label, key, rows, sort_key)
                    SELECT $1, %s, k, count(*), min(s) FROM (SELECT (r.%I)::text COLLATE "C", %s FROM %s) n(k, s) GROUP BY k
                    ON CONFLICT (graph, label, key) DO UPDATE SET rows = v.rows + excluded.rows, sort_key = excluded.sort_key',
                    declared.ordinal, declared.key_column, sort_text, source);
            ELSE
                -- A key's sort key is the least of its rows'. When some of a
                -- duplicate key's rows go and some stay, the least of those
                -- left is unknown here, so the graph cannot be queried until
                -- refresh_graph recomputes it. The reason names the table, not
                -- the key: hydradb.graphs is readable by every role, and the
                -- key is a value of a table they may not read.
                IF declared.sort_key_column IS NOT NULL THEN
                    RETURN NEXT format('WITH d AS (SELECT (r.%I)::text COLLATE "C" AS k, count(*) AS n FROM %s GROUP BY 1)
                        SELECT hydradb.flag_graph($1, %L) FROM hydradb.graph_vertices v JOIN d ON v.key = d.k
                        WHERE v.graph = $1 AND v.label = %s AND v.key = ANY (ARRAY(SELECT k FROM d)) AND v.rows > d.n LIMIT 1',
                        declared.key_column, source,
                        'a vertex of ' || source_relation::regclass::text
                            || ' has several rows and lost some, so its sort key must be recomputed from the rows left',
                        declared.ordinal);
                END IF;
                RETURN NEXT format('WITH d AS (SELECT (r.%I)::text COLLATE "C" AS k, count(*) AS n FROM %s GROUP BY 1)
                    UPDATE hydradb.graph_vertices v SET rows = v.rows - d.n FROM d
                    WHERE v.graph = $1 AND v.label = %s AND v.key = ANY (ARRAY(SELECT k FROM d)) AND v.key = d.k',
                    declared.key_column, source, declared.ordinal);
            END IF;
        END LOOP;
    END LOOP;
    FOR declared IN
        SELECT e.*, s.ordinal AS source_ordinal, t.ordinal AS target_ordinal
        FROM hydradb.graph_edge_tables e
        JOIN hydradb.graph_vertex_tables s ON s.graph = e.graph AND s.label = e.source_label
        JOIN hydradb.graph_vertex_tables t ON t.graph = e.graph AND t.label = e.target_label
        WHERE e.graph = graph_name AND e.relation = source_relation ORDER BY e.ordinal
    LOOP
        kind_text := CASE WHEN declared.kind_column IS NULL THEN 'NULL::text'
            ELSE format('(r.%I)::text', declared.kind_column) END;
        columns := hydradb.compared_columns(ARRAY[declared.key_column, declared.source_column,
            declared.target_column, declared.kind_column]);
        FOREACH pass IN ARRAY CASE op WHEN 'u' THEN ARRAY['d', 'i']::"char"[] ELSE ARRAY[op] END LOOP
            source := CASE WHEN op <> 'u' THEN rows_sql
                WHEN pass = 'd' THEN format('(SELECT %1$s FROM old_rows EXCEPT ALL SELECT %1$s FROM new_rows) r', columns)
                ELSE format('(SELECT %1$s FROM new_rows EXCEPT ALL SELECT %1$s FROM old_rows) r', columns) END;
            IF pass = 'd' THEN
                RETURN NEXT format('DELETE FROM hydradb.graph_adjacency a
                    WHERE a.graph = $1 AND a.label = %s AND a.key = ANY (ARRAY(SELECT (r.%I)::text COLLATE "C" FROM %s))',
                    declared.ordinal, declared.key_column, source);
                CONTINUE;
            END IF;
            RETURN NEXT format('SELECT hydradb.graph_null_field(%L, %L, %L) FROM %s
                    WHERE r.%I IS NULL OR r.%I IS NULL OR r.%I IS NULL%s LIMIT 1',
                graph_name, declared.label, 'key, source, target and kind', source,
                declared.key_column, declared.source_column, declared.target_column,
                CASE WHEN declared.kind_column IS NULL THEN '' ELSE format(' OR r.%I IS NULL', declared.kind_column) END);
            -- Endpoints get a vid before their vertex rows exist, so an edge can
            -- name a vertex that is inserted later, or never (a dangling edge).
            RETURN NEXT format('INSERT INTO hydradb.graph_vertices (graph, label, key)
                SELECT DISTINCT $1, e.label, e.key FROM (
                    SELECT %s::smallint, (r.%I)::text COLLATE "C" FROM %s UNION ALL SELECT %s::smallint, (r.%I)::text COLLATE "C" FROM %s
                ) e(label, key)
                WHERE NOT EXISTS (SELECT 1 FROM hydradb.graph_vertices v WHERE v.graph = $1 AND v.label = e.label AND v.key = e.key)
                ON CONFLICT (graph, label, key) DO NOTHING',
                declared.source_ordinal, declared.source_column, source,
                declared.target_ordinal, declared.target_column, source);
            RETURN NEXT format('INSERT INTO hydradb.graph_adjacency (graph, label, key, source, target, kind)
                SELECT $1, %s, (r.%I)::text COLLATE "C",
                    (SELECT v.vid FROM hydradb.graph_vertices v WHERE v.graph = $1 AND v.label = %s AND v.key = (r.%I)::text COLLATE "C"),
                    (SELECT v.vid FROM hydradb.graph_vertices v WHERE v.graph = $1 AND v.label = %s AND v.key = (r.%I)::text COLLATE "C"),
                    %s FROM %s',
                declared.ordinal, declared.key_column,
                declared.source_ordinal, declared.source_column, declared.target_ordinal, declared.target_column,
                kind_text, source);
        END LOOP;
    END LOOP;
END
$$;
REVOKE ALL ON FUNCTION hydradb.adjacency_statements(text, oid, "char", text) FROM PUBLIC;

-- The keys, of type `key_column`, an INSERT ('i'), DELETE ('d') or UPDATE
-- ('u') touched for one declaration, as a query over its transition tables:
-- for an UPDATE, those of rows whose declared columns (`column_names`, as
-- compared_columns compares them) changed, old and new, so rows it left alone
-- cancel out. The compared rows' columns are renamed by position, so no
-- declared column's name can collide with the key's.
CREATE FUNCTION hydradb.changed_keys(op "char", key_column name, column_names name[]) RETURNS text
LANGUAGE sql STABLE SET search_path = pg_catalog, pg_temp AS $$
    SELECT CASE op
        WHEN 'i' THEN format('SELECT %I FROM new_rows', key_column)
        WHEN 'd' THEN format('SELECT %I FROM old_rows', key_column)
        WHEN 'u' THEN format('SELECT k FROM (SELECT %1$I, %2$s FROM old_rows EXCEPT ALL SELECT %1$I, %2$s FROM new_rows) c(%3$s)
            UNION SELECT k FROM (SELECT %1$I, %2$s FROM new_rows EXCEPT ALL SELECT %1$I, %2$s FROM old_rows) c(%3$s)',
            key_column, hydradb.compared_columns(column_names),
            (SELECT string_agg(a, ', ' ORDER BY n) FROM (
                SELECT 'k', 0 UNION ALL
                SELECT 'c' || n, n FROM generate_series(1, (SELECT count(DISTINCT c) FROM unnest(column_names) c WHERE c IS NOT NULL)) n
            ) aliases(a, n)))
        END
$$;
REVOKE ALL ON FUNCTION hydradb.changed_keys("char", name, name[]) FROM PUBLIC;

-- Returns, as text, the statements that bring an indexed graph's adjacency
-- rows for the keys an INSERT ('i'), DELETE ('d') or UPDATE ('u') of
-- `source_relation` touched in line with the table's rows with those keys as
-- they are now; maintain_rows runs them, in order, in place of
-- adjacency_statements', when another trigger or a rule could have written the
-- table (hydradb.mirror_rereads). It reads the transition tables only for the
-- keys, per declaration: for an UPDATE, those of rows whose declared columns
-- changed (changed_keys), so an UPDATE that leaves them alone touches nothing.
-- The transition tables hold the rows as this statement wrote them, which
-- another AFTER trigger on the table can change, remove or write again before
-- pg_hydradb's AFTER STATEMENT trigger runs; each of that trigger's writes is
-- mirrored by its own nested statement first, so mirroring the outer
-- statement's transition rows last would undo them. Reading the table instead
-- leaves the adjacency tables matching it whatever ran in between. Other
-- transactions cannot change those keys meanwhile: this one holds the rows it
-- wrote, and the key's unique index makes a writer of the same key wait. Each
-- touched vertex key's rows and sort key are counted again, so a deferred
-- duplicate key needs no refresh here.
CREATE FUNCTION hydradb.reread_statements(graph_name text, source_relation oid, op "char")
RETURNS SETOF text LANGUAGE plpgsql STABLE SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    declared record;
    changed text;
    source text;
    sort_text text;
    kind_text text;
    vid_of CONSTANT text := '(SELECT v.vid FROM hydradb.graph_vertices v WHERE v.graph = $1 AND v.label = %s AND v.key = (r.%I)::text COLLATE "C")';
BEGIN
    FOR declared IN
        SELECT * FROM hydradb.graph_vertex_tables WHERE graph = graph_name AND relation = source_relation ORDER BY ordinal
    LOOP
        sort_text := CASE WHEN declared.sort_key_column IS NULL THEN 'NULL::text'
            ELSE format('(r.%I)::text', declared.sort_key_column) END;
        changed := hydradb.changed_keys(op, declared.key_column, ARRAY[declared.key_column, declared.sort_key_column]);
        source := format('(SELECT * FROM ONLY %s r WHERE r.%I IN (%s)) r', source_relation::regclass, declared.key_column, changed);
        RETURN NEXT format('SELECT hydradb.graph_null_field(%L, %L, %L) FROM %s WHERE r.%I IS NULL%s LIMIT 1',
            graph_name, declared.label, 'key and sort key', source, declared.key_column,
            CASE WHEN declared.sort_key_column IS NULL THEN ''
                ELSE format(' OR r.%I IS NULL', declared.sort_key_column) END);
        -- Each touched key's row count and least sort key, counted again. A
        -- key with no row left keeps its vid, with no rows, as one only edges
        -- name does; one that never had a row gets none.
        RETURN NEXT format('INSERT INTO hydradb.graph_vertices AS v (graph, label, key, rows, sort_key)
            SELECT $1, %1$s, k.key, COALESCE(c.n, 0), c.s
            FROM (SELECT DISTINCT (x.k)::text COLLATE "C" AS key FROM (%2$s) x(k)) k
            LEFT JOIN (SELECT (r.%3$I)::text COLLATE "C" AS key, count(*)::integer AS n, min(%4$s) AS s FROM %5$s GROUP BY 1) c
                ON c.key = k.key
            WHERE c.n IS NOT NULL
                OR EXISTS (SELECT 1 FROM hydradb.graph_vertices e WHERE e.graph = $1 AND e.label = %1$s AND e.key = k.key)
            ON CONFLICT (graph, label, key) DO UPDATE SET rows = excluded.rows, sort_key = excluded.sort_key
            WHERE v.rows IS DISTINCT FROM excluded.rows OR v.sort_key IS DISTINCT FROM excluded.sort_key',
            declared.ordinal, changed, declared.key_column, sort_text, source);
    END LOOP;
    FOR declared IN
        SELECT e.*, s.ordinal AS source_ordinal, t.ordinal AS target_ordinal
        FROM hydradb.graph_edge_tables e
        JOIN hydradb.graph_vertex_tables s ON s.graph = e.graph AND s.label = e.source_label
        JOIN hydradb.graph_vertex_tables t ON t.graph = e.graph AND t.label = e.target_label
        WHERE e.graph = graph_name AND e.relation = source_relation ORDER BY e.ordinal
    LOOP
        kind_text := CASE WHEN declared.kind_column IS NULL THEN 'NULL::text'
            ELSE format('(r.%I)::text', declared.kind_column) END;
        changed := hydradb.changed_keys(op, declared.key_column, ARRAY[declared.key_column, declared.source_column,
            declared.target_column, declared.kind_column]);
        source := format('(SELECT * FROM ONLY %s r WHERE r.%I IN (%s)) r', source_relation::regclass, declared.key_column, changed);
        RETURN NEXT format('SELECT hydradb.graph_null_field(%L, %L, %L) FROM %s
                WHERE r.%I IS NULL OR r.%I IS NULL OR r.%I IS NULL%s LIMIT 1',
            graph_name, declared.label, 'key, source, target and kind', source,
            declared.key_column, declared.source_column, declared.target_column,
            CASE WHEN declared.kind_column IS NULL THEN '' ELSE format(' OR r.%I IS NULL', declared.kind_column) END);
        -- Endpoints get a vid before their vertex rows exist, so an edge can
        -- name a vertex that is inserted later, or never (a dangling edge).
        RETURN NEXT format('INSERT INTO hydradb.graph_vertices (graph, label, key)
            SELECT DISTINCT $1, e.label, e.key FROM (
                SELECT %s::smallint, (r.%I)::text COLLATE "C" FROM %s UNION ALL SELECT %s::smallint, (r.%I)::text COLLATE "C" FROM %s
            ) e(label, key)
            WHERE NOT EXISTS (SELECT 1 FROM hydradb.graph_vertices v WHERE v.graph = $1 AND v.label = e.label AND v.key = e.key)
            ON CONFLICT (graph, label, key) DO NOTHING',
            declared.source_ordinal, declared.source_column, source,
            declared.target_ordinal, declared.target_column, source);
        -- Touched keys' adjacency rows that no longer match a row of the
        -- table go; the matching ones stay, and the missing ones are added
        -- below. Kinds compare bytewise, as the update comparison does.
        RETURN NEXT format('DELETE FROM hydradb.graph_adjacency a
            WHERE a.graph = $1 AND a.label = %1$s
                AND a.key = ANY (ARRAY(SELECT (x.k)::text COLLATE "C" FROM (%2$s) x(k)))
                AND NOT EXISTS (SELECT 1 FROM %3$s
                    WHERE (r.%4$I)::text COLLATE "C" = a.key AND %5$s = a.source AND %6$s = a.target
                        AND (%7$s) COLLATE "C" IS NOT DISTINCT FROM a.kind COLLATE "C")',
            declared.ordinal, changed, source, declared.key_column,
            format(vid_of, declared.source_ordinal, declared.source_column),
            format(vid_of, declared.target_ordinal, declared.target_column), kind_text);
        RETURN NEXT format('INSERT INTO hydradb.graph_adjacency (graph, label, key, source, target, kind)
            SELECT $1, %1$s, (r.%2$I)::text COLLATE "C", %3$s, %4$s, %5$s FROM %6$s%7$s',
            declared.ordinal, declared.key_column,
            format(vid_of, declared.source_ordinal, declared.source_column),
            format(vid_of, declared.target_ordinal, declared.target_column),
            kind_text, source,
            format('
            WHERE NOT EXISTS (SELECT 1 FROM hydradb.graph_adjacency a
                WHERE a.graph = $1 AND a.label = %s AND a.key = (r.%I)::text COLLATE "C")', declared.ordinal, declared.key_column));
    END LOOP;
END
$$;
REVOKE ALL ON FUNCTION hydradb.reread_statements(text, oid, "char") FROM PUBLIC;

-- Whether a write to `relation` can be changed again before pg_hydradb's AFTER
-- STATEMENT trigger mirrors it: the table has an AFTER trigger other than
-- pg_hydradb's own and the foreign key checks that only read (a user's
-- trigger, or a foreign key action that writes another table, whose own
-- triggers could write this one), or a rule. Such a write is mirrored by
-- reread_statements; any other by adjacency_statements, which reads only the
-- transition tables, so writers of different keys of one graph take no
-- predicate locks on each other's rows under SERIALIZABLE.
CREATE FUNCTION hydradb.mirror_rereads(relation oid) RETURNS boolean
LANGUAGE sql STABLE SET search_path = pg_catalog, pg_temp AS $$
    SELECT EXISTS (
        SELECT 1 FROM pg_trigger t JOIN pg_proc p ON p.oid = t.tgfoid
        WHERE t.tgrelid = relation AND t.tgenabled <> 'D'
            -- Neither BEFORE (2) nor INSTEAD OF (64).
            AND t.tgtype & (2 | 64) = 0
            AND t.tgfoid <> 'hydradb.maintain_rows()'::regprocedure
            AND NOT (p.pronamespace = 'pg_catalog'::regnamespace AND p.proname IN ('RI_FKey_check_ins', 'RI_FKey_check_upd',
                'RI_FKey_noaction_del', 'RI_FKey_noaction_upd', 'RI_FKey_restrict_del', 'RI_FKey_restrict_upd')))
        OR (SELECT relhasrules FROM pg_class WHERE oid = relation)
$$;
REVOKE ALL ON FUNCTION hydradb.mirror_rereads(oid) FROM PUBLIC;

-- Marks graph `registration` as needing refresh_graph, for `reason`, unless it
-- already does, and warns that it cannot be queried until then.
CREATE FUNCTION hydradb.flag_graph(registration bigint, reason text) RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    graph_name text;
BEGIN
    UPDATE hydradb.graphs g SET needs_refresh = reason
    WHERE g.registration = flag_graph.registration AND g.needs_refresh IS NULL
    RETURNING g.name INTO graph_name;
    IF graph_name IS NOT NULL THEN
        RAISE WARNING 'graph %: %', graph_name, reason
            USING DETAIL = format('Queries of graph %s fail until hydradb.refresh_graph(%L) rebuilds its '
                'adjacency tables.', graph_name, graph_name);
    END IF;
END
$$;
REVOKE ALL ON FUNCTION hydradb.flag_graph(bigint, text) FROM PUBLIC;

-- A query returning why the rows of `rows_sql` (a FROM item aliased `r`)
-- cannot be mirrored, or no row: a text key, edge key or endpoint of
-- `source_relation` longer than 2000 bytes; NULL when no such column is text.
-- Keys sit in B-tree entries, which hold about 2.7 kB after compression, so a
-- longer one would fail the write it came with; 2000 bytes fits uncompressed in
-- every index. Integer and uuid columns cannot be that long and are not
-- checked. The query is run by its caller, as adjacency_statements' are, so it
-- can read a trigger's transition tables.
CREATE FUNCTION hydradb.long_key_check(graph_name text, source_relation oid, rows_sql text) RETURNS text
LANGUAGE sql STABLE SET search_path = pg_catalog, pg_temp AS $$
    SELECT format('SELECT format(%L, c, l) FROM (%s) long(c, l) LIMIT 1',
        'column %s of ' || replace(source_relation::regclass::text, '%', '%%')
            || ' has a value of %s bytes, longer than the 2000 a graph index entry holds',
        string_agg(format('SELECT %1$L, octet_length((r.%1$I)::text) FROM %2$s WHERE octet_length((r.%1$I)::text) > 2000',
            columns.column_name, rows_sql), ' UNION ALL ' ORDER BY columns.column_name))
    FROM (
        SELECT DISTINCT d.column_name FROM (
            SELECT key_column AS column_name FROM hydradb.graph_vertex_tables WHERE graph = graph_name AND relation = source_relation
            UNION ALL SELECT key_column FROM hydradb.graph_edge_tables WHERE graph = graph_name AND relation = source_relation
            UNION ALL SELECT source_column FROM hydradb.graph_edge_tables WHERE graph = graph_name AND relation = source_relation
            UNION ALL SELECT target_column FROM hydradb.graph_edge_tables WHERE graph = graph_name AND relation = source_relation
        ) d
        JOIN pg_attribute a ON a.attrelid = source_relation AND a.attname = d.column_name AND NOT a.attisdropped
        WHERE a.atttypid IN ('text'::regtype, 'varchar'::regtype, 'bpchar'::regtype)
    ) columns
    HAVING count(*) > 0
$$;
REVOKE ALL ON FUNCTION hydradb.long_key_check(text, oid, text) FROM PUBLIC;

-- Statement-level trigger for INSERT, UPDATE, DELETE and TRUNCATE on a table
-- of an indexed graph. It runs in the writing transaction, so the adjacency
-- rows commit, roll back and become visible together with the rows they
-- mirror, and applies each statement's rows as a set.
CREATE FUNCTION hydradb.maintain_rows() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    graph_id bigint;
    stale text;
    statement text;
    long_key text;
    op "char";
BEGIN
    IF current_setting('transaction_isolation') = 'read committed' THEN
        SELECT registration, needs_refresh INTO graph_id, stale FROM hydradb.graphs WHERE name = TG_ARGV[0];
    ELSE
        -- A transaction-wide snapshot can predate a refresh_graph or
        -- drop_graph that committed since: the vertex ids it would mirror
        -- with are gone. Both change the row's key (refresh_graph takes a new
        -- registration), so locking it makes such a writer fail with a
        -- serialization error to retry instead. KEY SHARE leaves the row free
        -- for the needs_refresh update below, which two writers holding a
        -- plain SHARE lock would deadlock on.
        SELECT registration, needs_refresh INTO graph_id, stale FROM hydradb.graphs WHERE name = TG_ARGV[0] FOR KEY SHARE;
    END IF;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'graph registration is not visible; retry the source transaction'
            USING ERRCODE = '40001',
                HINT = format('If graph %s was deleted, hydradb.drop_graph(%L) removes its triggers.', TG_ARGV[0], TG_ARGV[0]);
    END IF;
    -- refresh_graph rebuilds a graph that needs it from scratch.
    IF stale IS NOT NULL THEN
        RETURN NULL;
    END IF;
    -- A key the indexes cannot hold would fail this write; leave the write be
    -- and stop trusting the graph instead.
    IF TG_OP IN ('INSERT', 'UPDATE') THEN
        long_key := hydradb.long_key_check(TG_ARGV[0], TG_RELID, 'new_rows r');
        IF long_key IS NOT NULL THEN
            EXECUTE long_key INTO stale;
        END IF;
        IF stale IS NOT NULL THEN
            UPDATE hydradb.graphs SET needs_refresh = stale WHERE name = TG_ARGV[0];
            RAISE WARNING 'graph %: %', TG_ARGV[0], stale
                USING DETAIL = format('Queries of graph %s fail until hydradb.refresh_graph(%L) rebuilds its '
                        'adjacency tables.', TG_ARGV[0], TG_ARGV[0]),
                    HINT = 'Shorten or remove that value, then refresh the graph.';
            RETURN NULL;
        END IF;
    END IF;
    IF TG_OP = 'TRUNCATE' THEN
        UPDATE hydradb.graph_vertices v SET rows = 0 FROM hydradb.graph_vertex_tables t
            WHERE t.graph = TG_ARGV[0] AND t.relation = TG_RELID AND v.graph = graph_id AND v.label = t.ordinal AND v.rows <> 0;
        DELETE FROM hydradb.graph_adjacency a USING hydradb.graph_edge_tables t
            WHERE t.graph = TG_ARGV[0] AND t.relation = TG_RELID AND a.graph = graph_id AND a.label = t.ordinal;
        RETURN NULL;
    END IF;
    op := CASE TG_OP WHEN 'INSERT' THEN 'i' WHEN 'UPDATE' THEN 'u' ELSE 'd' END;
    -- Another trigger may have changed this statement's rows since it wrote
    -- them: mirror the table, not the transition tables (reread_statements).
    IF hydradb.mirror_rereads(TG_RELID) THEN
        FOR statement IN SELECT * FROM hydradb.reread_statements(TG_ARGV[0], TG_RELID, op) LOOP
            EXECUTE statement USING graph_id;
        END LOOP;
        RETURN NULL;
    END IF;
    FOR statement IN
        SELECT * FROM hydradb.adjacency_statements(TG_ARGV[0], TG_RELID, op,
            CASE TG_OP WHEN 'INSERT' THEN 'new_rows r' WHEN 'DELETE' THEN 'old_rows r' END)
    LOOP
        EXECUTE statement USING graph_id;
    END LOOP;
    RETURN NULL;
END
$$;
REVOKE ALL ON FUNCTION hydradb.maintain_rows() FROM PUBLIC;

-- Flags an indexed graph (flag_graph) when an ALTER TABLE
-- rewrites the column values of a table it reads. A rewrite such as ALTER
-- COLUMN ... TYPE ... USING writes new values without firing the table's
-- triggers, so the adjacency tables would keep the old ones; flagged, the graph
-- cannot be queried until hydradb.refresh_graph rebuilds it from the new
-- values. PostgreSQL does not say which columns a rewrite changes, so any
-- column rewrite of such a table flags it. A rewrite for a column default
-- (reason AT_REWRITE_DEFAULT_VAL) keeps existing values when it adds a column;
-- it also recomputes a stored generated column for ALTER COLUMN ... SET
-- EXPRESSION (PostgreSQL 17), but an indexed graph declares none
-- (check_mirrorable). Changing persistence or access method keeps the values,
-- as does a type change that needs no rewrite.
CREATE FUNCTION hydradb.flag_column_rewrite() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    rewritten regclass := pg_event_trigger_table_rewrite_oid();
    graph_id bigint;
BEGIN
    -- AT_REWRITE_COLUMN_REWRITE.
    IF pg_event_trigger_table_rewrite_reason() & 4 = 0 THEN
        RETURN;
    END IF;
    FOR graph_id IN
        SELECT DISTINCT g.registration
        FROM hydradb.graphs g
        JOIN (
            SELECT graph, relation FROM hydradb.graph_vertex_tables
            UNION ALL SELECT graph, relation FROM hydradb.graph_edge_tables
        ) declared ON declared.graph = g.name
        WHERE g.indexed AND declared.relation = rewritten
        ORDER BY 1
    LOOP
        PERFORM hydradb.flag_graph(graph_id,
            format('an ALTER TABLE rewrote the column values of %s without firing its triggers', rewritten));
    END LOOP;
END
$$;
REVOKE ALL ON FUNCTION hydradb.flag_column_rewrite() FROM PUBLIC;
CREATE EVENT TRIGGER pg_hydradb_flag_column_rewrite ON table_rewrite
    EXECUTE FUNCTION hydradb.flag_column_rewrite();

-- Whether `relation` carries the triggers that keep graph `graph_name` current.
CREATE FUNCTION hydradb.maintains(graph_name text, relation regclass) RETURNS boolean
LANGUAGE sql STABLE SET search_path = pg_catalog, pg_temp AS $$
    SELECT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid = relation AND tgfoid = 'hydradb.maintain_rows()'::regprocedure
            AND tgname IN (SELECT 'pg_hydradb_' || md5(graph_name) || suffix FROM unnest(ARRAY['_i', '_d', '_r', '_t']) suffix))
$$;
REVOKE ALL ON FUNCTION hydradb.maintains(text, regclass) FROM PUBLIC;

-- Records, before an ALTER TABLE runs, which pg_hydradb triggers fire for
-- every write, for check_source_changes to flag a graph one of them stops
-- firing for every write in: disabled, or enabled only for origin or replica
-- writes, it lets some writes go unmirrored. Only a change from always counts,
-- since a restore creates each trigger enabled for origin writes and then
-- makes it fire always. The record lasts until the transaction ends; this
-- runs before every ALTER TABLE in the database, so it only reads.
CREATE FUNCTION hydradb.record_trigger_modes() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    PERFORM set_config('pg_hydradb.firing_always', COALESCE((
        SELECT string_agg(t.oid::text, ',') FROM pg_trigger t
        WHERE t.tgfoid = to_regprocedure('hydradb.maintain_rows()') AND t.tgenabled = 'A'), ''), true);
END
$$;
REVOKE ALL ON FUNCTION hydradb.record_trigger_modes() FROM PUBLIC;
CREATE EVENT TRIGGER pg_hydradb_record_trigger_modes ON ddl_command_start WHEN TAG IN ('ALTER TABLE')
    EXECUTE FUNCTION hydradb.record_trigger_modes();

-- A declared column as the graph depends on it: its type with modifier,
-- collation, nullability and whether it is generated; NULL when the table has
-- no column of that name. The collation is kept by oid and by name: a rename
-- of it or its schema keeps the oid, and a dump restored elsewhere keeps the
-- name (hydradb.same_state).
CREATE FUNCTION hydradb.column_state(relation regclass, column_name name) RETURNS jsonb
LANGUAGE sql STABLE SET search_path = pg_catalog, pg_temp AS $$
    SELECT jsonb_strip_nulls(jsonb_build_object(
        'type', format_type(a.atttypid, a.atttypmod),
        'collation', CASE WHEN a.attcollation <> 0 THEN format('%I.%I', n.nspname, c.collname) END,
        'collation_oid', NULLIF(a.attcollation, 0)::bigint,
        'not_null', a.attnotnull,
        'generated', a.attgenerated <> ''))
    FROM pg_attribute a
    LEFT JOIN pg_collation c ON c.oid = a.attcollation
    LEFT JOIN pg_namespace n ON n.oid = c.collnamespace
    WHERE a.attrelid = relation AND a.attname = column_name AND a.attnum > 0 AND NOT a.attisdropped
$$;
REVOKE ALL ON FUNCTION hydradb.column_state(regclass, name) FROM PUBLIC;

-- Whether two column_state values describe the same column for the graph:
-- the same type, nullability and generation, and the same collation, by oid
-- or by name.
CREATE FUNCTION hydradb.same_state(recorded jsonb, now jsonb) RETURNS boolean
LANGUAGE sql IMMUTABLE SET search_path = pg_catalog, pg_temp AS $$
    SELECT recorded IS NOT NULL AND now IS NOT NULL
        AND recorded ->> 'type' = now ->> 'type'
        AND recorded ->> 'not_null' = now ->> 'not_null'
        AND recorded ->> 'generated' = now ->> 'generated'
        AND (recorded ->> 'collation_oid' IS NOT DISTINCT FROM now ->> 'collation_oid'
            OR recorded ->> 'collation' IS NOT DISTINCT FROM now ->> 'collation')
$$;
REVOKE ALL ON FUNCTION hydradb.same_state(jsonb, jsonb) FROM PUBLIC;

-- A column_state value as a column definition, for messages.
CREATE FUNCTION hydradb.state_text(state jsonb) RETURNS text
LANGUAGE sql IMMUTABLE SET search_path = pg_catalog, pg_temp AS $$
    SELECT (state ->> 'type') || COALESCE(' COLLATE ' || (state ->> 'collation'), '')
        || CASE WHEN (state ->> 'not_null')::boolean THEN ' NOT NULL' ELSE ' NULL' END
        || CASE WHEN (state ->> 'generated')::boolean THEN ' GENERATED' ELSE '' END
$$;
REVOKE ALL ON FUNCTION hydradb.state_text(jsonb) FROM PUBLIC;

-- Every declared column of graph `graph_name` (every graph when NULL) that is
-- not the column recorded at registration (hydradb.same_state), with both as
-- column definitions; `now` is NULL for a column no longer there by its name.
CREATE FUNCTION hydradb.declared_changes(graph_name text)
RETURNS TABLE (graph text, relation regclass, role text, column_name name, recorded text, now text)
LANGUAGE sql STABLE SET search_path = pg_catalog, pg_temp AS $$
    SELECT d.graph, d.relation, d.role, d.column_name, hydradb.state_text(d.recorded), hydradb.state_text(d.now)
    FROM (
        SELECT t.graph, t.relation, c.role, c.column_name, t.state -> c.role AS recorded,
            hydradb.column_state(t.relation, c.column_name) AS now
        FROM hydradb.graph_vertex_tables t
        CROSS JOIN LATERAL (VALUES ('key', t.key_column), ('sort key', t.sort_key_column)) c(role, column_name)
        WHERE c.column_name IS NOT NULL AND t.state IS NOT NULL AND (graph_name IS NULL OR t.graph = graph_name)
        UNION ALL
        SELECT t.graph, t.relation, c.role, c.column_name, t.state -> c.role,
            hydradb.column_state(t.relation, c.column_name)
        FROM hydradb.graph_edge_tables t
        CROSS JOIN LATERAL (VALUES ('key', t.key_column), ('source', t.source_column), ('target', t.target_column),
            ('kind', t.kind_column)) c(role, column_name)
        WHERE c.column_name IS NOT NULL AND t.state IS NOT NULL AND (graph_name IS NULL OR t.graph = graph_name)
    ) d
    WHERE NOT hydradb.same_state(d.recorded, d.now)
$$;
REVOKE ALL ON FUNCTION hydradb.declared_changes(text) FROM PUBLIC;

-- Records again, for every graph, each declared column that same_state finds
-- unchanged but whose recorded collation differs by oid or by name: a
-- collation or its schema renamed, or a restore that gave it a new oid. Kept
-- current, the record matches by both, so a later rename after a restore, or
-- a dump after a rename, still matches.
CREATE FUNCTION hydradb.rerecord_states() RETURNS void
LANGUAGE sql SET search_path = pg_catalog, pg_temp AS $$
    UPDATE hydradb.graph_vertex_tables t SET state = n.state
    FROM (
        SELECT t2.graph, t2.label, jsonb_object_agg(c.role,
            CASE WHEN hydradb.same_state(t2.state -> c.role, hydradb.column_state(t2.relation, c.column_name))
                THEN hydradb.column_state(t2.relation, c.column_name) ELSE t2.state -> c.role END) AS state
        FROM hydradb.graph_vertex_tables t2
        CROSS JOIN LATERAL (VALUES ('key', t2.key_column), ('sort key', t2.sort_key_column)) c(role, column_name)
        WHERE c.column_name IS NOT NULL AND t2.state IS NOT NULL
        GROUP BY t2.graph, t2.label
    ) n
    WHERE t.graph = n.graph AND t.label = n.label AND t.state <> n.state;
    UPDATE hydradb.graph_edge_tables t SET state = n.state
    FROM (
        SELECT t2.graph, t2.label, jsonb_object_agg(c.role,
            CASE WHEN hydradb.same_state(t2.state -> c.role, hydradb.column_state(t2.relation, c.column_name))
                THEN hydradb.column_state(t2.relation, c.column_name) ELSE t2.state -> c.role END) AS state
        FROM hydradb.graph_edge_tables t2
        CROSS JOIN LATERAL (VALUES ('key', t2.key_column), ('source', t2.source_column), ('target', t2.target_column),
            ('kind', t2.kind_column)) c(role, column_name)
        WHERE c.column_name IS NOT NULL AND t2.state IS NOT NULL
        GROUP BY t2.graph, t2.label
    ) n
    WHERE t.graph = n.graph AND t.label = n.label AND t.state <> n.state;
$$;
REVOKE ALL ON FUNCTION hydradb.rerecord_states() FROM PUBLIC;

-- Refuses a change to a table an indexed graph maintains that the graph could
-- not follow, as PostgreSQL refuses to drop a column a view reads: until it is
-- undone, hydradb.refresh_graph could not rebuild the graph, its queries
-- would fail, or its triggers would reject writes the table accepts. The way
-- round is to drop the graph first. Only while the table carries the graph's
-- triggers: a graph without them has none to protect, and dropping them is how
-- pg_dump --clean starts, before it drops constraints, tables and the
-- extension; a pg_restore creates them after the tables, columns and keys.
--
-- On sql_drop: dropping the table or a column the graph declares (even when
-- the same ALTER adds a column of that name back), and leaving a vertex key
-- without a unique index of its own, or an edge key without an immediate one
-- (DROP INDEX CONCURRENTLY flags the graph instead).
--
-- On ddl_command_end, whatever the command, by what it left rather than what
-- it said, so a kind of change nobody listed is caught too:
-- - any declared column whose state (hydradb.column_state) is not the one
--   recorded at registration: renamed, retyped, recollated, made nullable;
-- - a source that hydradb.source_unfit no longer accepts: made unlogged,
--   joined to an inheritance tree as a parent or a child (CREATE TABLE ...
--   INHERITS changes only the child, so the tables it inherits from are
--   checked too), or given row-level security that applies to the extension
--   owner;
-- - ALTER TRIGGER on a pg_hydradb trigger, whose name is how the graph finds it.
-- A change after which writes go unmirrored instead flags the graph, and the
-- flag stays when the change is undone, since the writes in between are lost
-- to the adjacency tables: one of its pg_hydradb triggers no longer firing
-- for every write (record_trigger_modes). That is how a data-only restore with
-- --disable-triggers loads a table, and refresh_graph then rebuilds it.
CREATE FUNCTION hydradb.check_source_changes() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    changed record;
    refusal text;
    changed_tables oid[];
    lost record;
    hint CONSTANT text := 'Drop the graph first with hydradb.drop_graph, make the change, then register the graph again with hydradb.create_graph.';
BEGIN
    -- Each pg_event_trigger_* function may only be called in its own event.
    IF TG_EVENT = 'sql_drop' THEN
        -- Dropping the extension drops the catalog read below.
        IF to_regclass('hydradb.graphs') IS NULL THEN
            RETURN;
        END IF;
        -- A dropped table's triggers are gone from pg_trigger by now, but are
        -- among the objects this command dropped.
        SELECT format('cannot drop table %s: it is a source of indexed graph %s', min(o.object_identity), string_agg(DISTINCT d.graph, ', '))
        INTO refusal
        FROM pg_event_trigger_dropped_objects() o
        JOIN (
            SELECT graph, relation FROM hydradb.graph_vertex_tables
            UNION ALL SELECT graph, relation FROM hydradb.graph_edge_tables
        ) d ON d.relation::oid = o.objid
        WHERE o.classid = 'pg_class'::regclass AND o.objsubid = 0
            AND EXISTS (SELECT 1 FROM pg_event_trigger_dropped_objects() t
                WHERE t.object_type = 'trigger' AND t.address_names[1:2] = o.address_names
                    AND t.address_names[3] IN (SELECT 'pg_hydradb_' || md5(d.graph) || suffix FROM unnest(ARRAY['_i', '_d', '_r', '_t']) suffix))
        GROUP BY o.objid
        ORDER BY min(o.object_identity)
        LIMIT 1;
        IF refusal IS NOT NULL THEN
            PERFORM hydradb.usage_count('schema_change_refused');
            RAISE EXCEPTION '%', refusal
                USING ERRCODE = 'object_in_use',
                    DETAIL = 'The graph reads this table; without it the graph can be neither queried nor refreshed.',
                    HINT = hint;
        END IF;
        SELECT format('cannot drop column %I of %s: indexed graph %s declares it', d.column_name, d.relation, string_agg(DISTINCT d.graph, ', '))
        INTO refusal
        FROM pg_event_trigger_dropped_objects() o
        JOIN (
            SELECT t.graph, t.relation, c.column_name
            FROM hydradb.graph_vertex_tables t
            CROSS JOIN LATERAL (VALUES (t.key_column), (t.sort_key_column)) c(column_name)
            UNION ALL
            SELECT t.graph, t.relation, c.column_name
            FROM hydradb.graph_edge_tables t
            CROSS JOIN LATERAL (VALUES (t.key_column), (t.source_column), (t.target_column), (t.kind_column)) c(column_name)
        ) d ON d.relation::oid = o.objid AND d.column_name = o.address_names[array_length(o.address_names, 1)]
        WHERE o.classid = 'pg_class'::regclass AND o.object_type = 'table column' AND hydradb.maintains(d.graph, d.relation)
        GROUP BY d.relation, d.column_name
        ORDER BY d.relation::text, d.column_name
        LIMIT 1;
        IF refusal IS NOT NULL THEN
            PERFORM hydradb.usage_count('schema_change_refused');
            RAISE EXCEPTION '%', refusal
                USING ERRCODE = 'object_in_use',
                    DETAIL = 'The graph reads this column by name; without it the graph can be neither queried nor refreshed, '
                        'and a column added under the same name would hold values its triggers never saw.',
                    HINT = hint;
        END IF;
        -- A dropped pg_hydradb trigger flags its graph rather than being
        -- refused: pg_dump --clean drops the triggers first. Queries already
        -- fail while one is missing (they count the triggers), but nothing
        -- would stop them once it is created again by hand, though the
        -- writes in between were never mirrored. refresh_graph puts it back.
        FOR lost IN
            SELECT g.registration, min(format('lost pg_hydradb trigger %s on %s.%s, so writes to it are not mirrored',
                    t.address_names[3], t.address_names[1], t.address_names[2])) AS reason
            FROM pg_event_trigger_dropped_objects() t
            JOIN hydradb.graphs g ON g.indexed AND g.needs_refresh IS NULL
                AND t.address_names[3] IN (SELECT 'pg_hydradb_' || md5(g.name) || suffix FROM unnest(ARRAY['_i', '_d', '_r', '_t']) suffix)
            -- On one of the graph's own tables: a trigger of that name elsewhere is not its.
            JOIN (
                SELECT graph, relation FROM hydradb.graph_vertex_tables
                UNION SELECT graph, relation FROM hydradb.graph_edge_tables
            ) d ON d.graph = g.name
            JOIN pg_class c ON c.oid = d.relation
            JOIN pg_namespace n ON n.oid = c.relnamespace AND t.address_names[1:2] = ARRAY[n.nspname::text, c.relname::text]
            WHERE t.object_type = 'trigger'
            GROUP BY g.registration
            ORDER BY g.registration
        LOOP
            PERFORM hydradb.flag_graph(lost.registration, lost.reason);
        END LOOP;
        -- A dropped endpoint foreign key flags the graph rather than being
        -- refused: pg_dump --clean drops foreign keys before the triggers it
        -- created them after. Flagged, the triggers mirror nothing, so writes
        -- cannot deadlock on them, and refresh_graph needs the key back.
        FOR lost IN
            SELECT g.registration, min(format('lost the foreign key from column %I of %s to key %I of %s',
                f.column_name, f.relation, f.key_column, f.vertex_relation)) AS reason
            FROM hydradb.endpoint_foreign_keys(NULL) f
            JOIN hydradb.graphs g ON g.name = f.graph AND g.needs_refresh IS NULL
            WHERE f.problem = 'missing' AND hydradb.maintains(f.graph, f.relation)
                AND EXISTS (SELECT 1 FROM pg_event_trigger_dropped_objects() o WHERE o.object_type = 'table constraint')
            GROUP BY g.registration
            ORDER BY g.registration
        LOOP
            PERFORM hydradb.flag_graph(lost.registration, lost.reason);
        END LOOP;
        IF EXISTS (SELECT 1 FROM pg_event_trigger_dropped_objects() o WHERE o.object_type IN ('index', 'table constraint')) THEN
            -- DROP INDEX CONCURRENTLY commits its first two steps, which stop
            -- the index from being read or kept, in transactions of their own
            -- before this one, which only removes it: raising here would roll
            -- back only that, and leave the key unenforced while the graph
            -- trusted it. refuse_concurrent_index_drop refuses such a drop
            -- before it starts; one it could not read gets here, and the loss
            -- stands and flags the graph, whose
            -- triggers then mirror nothing until refresh_graph, which needs
            -- the index back. No catalog says a drop is concurrent, but it
            -- cannot run in a transaction block or a function, so its word is
            -- in the client's statement; a plain drop that mentions the word
            -- is flagged rather than refused, which is safe. A flagged graph
            -- has nothing to protect, so it is not checked.
            FOR lost IN
                SELECT g.registration, string_agg(DISTINCT g.name, ', ') AS graphs,
                    format('%s key %I of %s', e.role, e.key_column, e.relation) AS what,
                    CASE e.role WHEN 'edge' THEN 'immediate ' ELSE '' END AS kind
                FROM (
                    SELECT graph, relation, key_column, 'vertex' AS role FROM hydradb.graph_vertex_tables
                    UNION ALL SELECT graph, relation, key_column, 'edge' FROM hydradb.graph_edge_tables
                ) e
                JOIN hydradb.graphs g ON g.name = e.graph AND g.needs_refresh IS NULL
                WHERE hydradb.maintains(e.graph, e.relation)
                    AND NOT EXISTS (
                        SELECT 1 FROM pg_index i
                        JOIN pg_attribute a ON a.attrelid = e.relation AND a.attname = e.key_column AND NOT a.attisdropped
                        WHERE i.indrelid = e.relation AND i.indisunique AND (i.indimmediate OR e.role = 'vertex') AND i.indisvalid
                            AND i.indpred IS NULL AND i.indexprs IS NULL AND i.indnkeyatts = 1 AND i.indkey[0] = a.attnum)
                GROUP BY g.registration, e.relation, e.key_column, e.role
                ORDER BY e.relation::text, e.role, g.registration
            LOOP
                IF current_query() ~* '\mconcurrently\M' THEN
                    PERFORM hydradb.flag_graph(lost.registration, format('lost the last %sunique index of %s', lost.kind, lost.what));
                ELSE
                    PERFORM hydradb.usage_count('schema_change_refused');
                    RAISE EXCEPTION 'cannot drop the last %unique index of %: indexed graph % needs it', lost.kind, lost.what, lost.graphs
                        USING ERRCODE = 'object_in_use',
                            DETAIL = 'Without it the table accepts a duplicate key, which the graph does not represent.',
                            HINT = hint;
                END IF;
            END LOOP;
        END IF;
        RETURN;
    END IF;
    -- ddl_command_end. A renamed collation or schema changes no column, only
    -- the name a declared column's collation is recorded by.
    PERFORM hydradb.rerecord_states();
    IF TG_TAG IN ('ALTER COLLATION', 'ALTER SCHEMA') THEN
        RETURN;
    END IF;
    IF TG_TAG = 'ALTER TRIGGER' THEN
        SELECT format('cannot alter a pg_hydradb trigger on %s: it keeps the adjacency tables of graph %s current',
                t.tgrelid::regclass, convert_from(substring(t.tgargs FROM 1 FOR length(t.tgargs) - 1), 'UTF8'))
        INTO refusal
        FROM pg_event_trigger_ddl_commands() c
        JOIN pg_trigger t ON t.oid = c.objid
        WHERE c.classid = 'pg_trigger'::regclass AND t.tgfoid = 'hydradb.maintain_rows()'::regprocedure
        LIMIT 1;
        IF refusal IS NOT NULL THEN
            RAISE EXCEPTION '%', refusal
                USING ERRCODE = 'object_in_use',
                    DETAIL = 'pg_hydradb finds the triggers of a graph by name.',
                    HINT = hint;
        END IF;
        RETURN;
    END IF;
    changed_tables := ARRAY(SELECT c.objid FROM pg_event_trigger_ddl_commands() c WHERE c.classid = 'pg_class'::regclass);
    SELECT d.relation, d.role, d.column_name, d.recorded, d.now, d.graph INTO changed
    FROM hydradb.declared_changes(NULL) d
    JOIN hydradb.graphs g ON g.name = d.graph AND g.indexed
    WHERE d.relation = ANY (changed_tables) AND hydradb.maintains(d.graph, d.relation)
    ORDER BY d.relation::text, d.column_name, d.graph
    LIMIT 1;
    IF FOUND THEN
        IF changed.now IS NULL THEN
            RAISE EXCEPTION 'cannot rename column % of %: it is the % of indexed graph %',
                    quote_ident(changed.column_name), changed.relation, changed.role, changed.graph
                USING ERRCODE = 'object_in_use',
                    DETAIL = 'The graph reads this column by name.',
                    HINT = hint;
        END IF;
        RAISE EXCEPTION 'cannot change column % of %: it is the % of indexed graph %',
                quote_ident(changed.column_name), changed.relation, changed.role, changed.graph
            USING ERRCODE = 'object_in_use',
                DETAIL = format('The graph was registered with it as %s, and this would make it %s.', changed.recorded, changed.now),
                HINT = hint;
    END IF;
    SELECT format('cannot make foreign key %I on column %I of %s %s: indexed graph %s needs it enforced and not deferrable',
            f.conname, f.column_name, f.relation, f.problem, f.graph)
    INTO refusal
    FROM hydradb.endpoint_foreign_keys(NULL) f
    JOIN hydradb.graphs g ON g.name = f.graph AND g.indexed
    WHERE f.problem IN ('deferrable', 'not enforced') AND f.relation = ANY (changed_tables)
        AND hydradb.maintains(f.graph, f.relation)
    ORDER BY f.relation::text, f.column_name
    LIMIT 1;
    IF refusal IS NOT NULL THEN
        PERFORM hydradb.usage_count('schema_change_refused');
        RAISE EXCEPTION '%', refusal
            USING ERRCODE = 'object_in_use',
                DETAIL = 'It must check every write when it is made, before the graph''s triggers run.',
                HINT = hint;
    END IF;
    SELECT format('cannot make %s unfit for indexed graph %s: %s', s.relation, s.graph, s.unfit) INTO refusal
    FROM (
        SELECT d.graph, d.relation, hydradb.source_unfit(d.relation) AS unfit
        FROM (
            SELECT graph, relation FROM hydradb.graph_vertex_tables
            UNION SELECT graph, relation FROM hydradb.graph_edge_tables
        ) d
        JOIN hydradb.graphs g ON g.name = d.graph AND g.indexed
        WHERE hydradb.maintains(d.graph, d.relation)
            AND (d.relation = ANY (changed_tables) OR EXISTS (SELECT 1 FROM pg_inherits i
                WHERE (i.inhrelid = ANY (changed_tables) AND i.inhparent = d.relation)
                    OR (i.inhparent = ANY (changed_tables) AND i.inhrelid = d.relation)))
    ) s
    WHERE s.unfit IS NOT NULL
    ORDER BY s.relation::text, s.graph
    LIMIT 1;
    IF refusal IS NOT NULL THEN
        RAISE EXCEPTION '%', refusal
            USING ERRCODE = 'object_in_use',
                DETAIL = 'Queries read only the adjacency tables, which could not hold the table as this change leaves it.',
                HINT = hint;
    END IF;
    FOR changed IN
        SELECT g.registration,
            min(format('a pg_hydradb trigger on %s stopped firing for every write, so some writes to it are not mirrored', d.relation)) AS reason
        FROM hydradb.graphs g
        JOIN (
            SELECT graph, relation FROM hydradb.graph_vertex_tables
            UNION SELECT graph, relation FROM hydradb.graph_edge_tables
        ) d ON d.graph = g.name
        WHERE g.needs_refresh IS NULL AND d.relation = ANY (changed_tables) AND EXISTS (
            SELECT 1 FROM pg_trigger t
            WHERE t.tgrelid = d.relation AND t.tgfoid = 'hydradb.maintain_rows()'::regprocedure AND t.tgenabled <> 'A'
                AND t.oid::text = ANY (string_to_array(current_setting('pg_hydradb.firing_always', true), ','))
                AND t.tgname IN (SELECT 'pg_hydradb_' || md5(g.name) || suffix FROM unnest(ARRAY['_i', '_d', '_r', '_t']) suffix))
        GROUP BY g.registration
    LOOP
        PERFORM hydradb.flag_graph(changed.registration, changed.reason);
    END LOOP;
END
$$;
REVOKE ALL ON FUNCTION hydradb.check_source_changes() FROM PUBLIC;
CREATE EVENT TRIGGER pg_hydradb_refuse_source_drops ON sql_drop
    EXECUTE FUNCTION hydradb.check_source_changes();
CREATE EVENT TRIGGER pg_hydradb_check_altered_sources ON ddl_command_end WHEN TAG IN ('ALTER TABLE', 'CREATE TABLE', 'ALTER TRIGGER', 'ALTER COLLATION', 'ALTER SCHEMA')
    EXECUTE FUNCTION hydradb.check_source_changes();

-- Refuses DROP INDEX CONCURRENTLY of the last unique index a graph key relies
-- on before it starts. Its first steps commit in transactions of their own,
-- leaving the index unenforced, before sql_drop runs, so check_source_changes
-- could only flag the graph by then; at ddl_command_start nothing has
-- happened yet. PostgreSQL does not say here which index a DROP names, but a
-- concurrent drop names exactly one and runs alone, outside a transaction
-- block, so it is the name after CONCURRENTLY [IF EXISTS] in the client's
-- statement. A statement that cannot be read that way (a comment inside it,
-- say) passes, and sql_drop flags the graph as before.
--
-- The name must resolve as the drop resolves it, under the session's
-- search_path, so this function keeps the caller's: it runs as the caller,
-- names every function and operator with its schema, and hands the index to
-- hydradb.check_concurrent_index_drop, which reads the catalog as the
-- extension owner.
CREATE FUNCTION hydradb.refuse_concurrent_index_drop() RETURNS event_trigger
LANGUAGE plpgsql AS $$
DECLARE
    named text;
    target oid;
BEGIN
    IF pg_catalog.current_query() OPERATOR(pg_catalog.!~*) '^\s*drop\s+index\s+concurrently\s'
        OR pg_catalog.to_regclass('hydradb.graphs') IS NULL THEN
        RETURN;
    END IF;
    -- DROP INDEX CONCURRENTLY [IF EXISTS] name [CASCADE | RESTRICT] [;]
    named := pg_catalog.regexp_replace(pg_catalog.current_query(), '^\s*drop\s+index\s+concurrently\s+(if\s+exists\s+)?', '', 'i');
    named := pg_catalog.regexp_replace(named, '\s*;?\s*$', '');
    named := pg_catalog.regexp_replace(named, '\s+(cascade|restrict)$', '', 'i');
    BEGIN
        target := pg_catalog.to_regclass(named);
    EXCEPTION WHEN OTHERS THEN
        target := NULL;
    END;
    IF target IS NOT NULL THEN
        PERFORM hydradb.check_concurrent_index_drop(target);
    END IF;
END
$$;

-- Raises if index `target` is the last unique index of its own that a key of
-- an indexed graph relies on (immediate, for an edge key), as dropping it
-- with plain DROP INDEX is refused (check_source_changes).
CREATE FUNCTION hydradb.check_concurrent_index_drop(target oid) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    lost record;
BEGIN
    SELECT string_agg(DISTINCT g.name, ', ') AS graphs,
        min(format('%s key %I of %s', e.role, e.key_column, e.relation)) AS what,
        min(CASE e.role WHEN 'edge' THEN 'immediate ' ELSE '' END) AS kind
    INTO lost
    FROM (
        SELECT graph, relation, key_column, 'vertex' AS role FROM hydradb.graph_vertex_tables
        UNION ALL SELECT graph, relation, key_column, 'edge' FROM hydradb.graph_edge_tables
    ) e
    JOIN hydradb.graphs g ON g.name = e.graph AND g.needs_refresh IS NULL
    JOIN pg_attribute a ON a.attrelid = e.relation AND a.attname = e.key_column AND NOT a.attisdropped
    WHERE hydradb.maintains(e.graph, e.relation)
        -- The index this drops is one the key counts ...
        AND EXISTS (
            SELECT 1 FROM pg_index i
            WHERE i.indexrelid = target AND i.indrelid = e.relation AND i.indisunique AND (i.indimmediate OR e.role = 'vertex')
                AND i.indisvalid AND i.indpred IS NULL AND i.indexprs IS NULL AND i.indnkeyatts = 1 AND i.indkey[0] = a.attnum)
        -- ... and no other is.
        AND NOT EXISTS (
            SELECT 1 FROM pg_index i
            WHERE i.indexrelid <> target AND i.indrelid = e.relation AND i.indisunique AND (i.indimmediate OR e.role = 'vertex')
                AND i.indisvalid AND i.indpred IS NULL AND i.indexprs IS NULL AND i.indnkeyatts = 1 AND i.indkey[0] = a.attnum)
    HAVING count(*) > 0;
    IF FOUND THEN
        RAISE EXCEPTION 'cannot drop the last %unique index of %: indexed graph % needs it', lost.kind, lost.what, lost.graphs
            USING ERRCODE = 'object_in_use',
                DETAIL = 'Without it the table accepts a duplicate key, which the graph does not represent.',
                HINT = 'Drop the graph first with hydradb.drop_graph, make the change, then register the graph again with hydradb.create_graph.';
    END IF;
END
$$;
CREATE EVENT TRIGGER pg_hydradb_refuse_concurrent_index_drop ON ddl_command_start WHEN TAG IN ('DROP INDEX')
    EXECUTE FUNCTION hydradb.refuse_concurrent_index_drop();

-- Fills an indexed graph's adjacency tables from its source tables, or, when
-- a source holds a key the indexes cannot (hydradb.long_key_check), fills nothing
-- and returns why.
CREATE FUNCTION hydradb.fill_graph(graph_name text) RETURNS text
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    graph_id bigint;
    source_relation regclass;
    statement text;
    analyzed boolean := false;
    long_key text;
    unfit text;
BEGIN
    SELECT registration INTO STRICT graph_id FROM hydradb.graphs WHERE name = graph_name;
    FOR source_relation IN
        SELECT DISTINCT relation FROM (
            SELECT relation FROM hydradb.graph_vertex_tables WHERE graph = graph_name
            UNION ALL SELECT relation FROM hydradb.graph_edge_tables WHERE graph = graph_name
        ) declared ORDER BY 1
    LOOP
        long_key := hydradb.long_key_check(graph_name, source_relation, format('%s r', source_relation));
        IF long_key IS NOT NULL THEN
            EXECUTE long_key INTO unfit;
        END IF;
        IF unfit IS NOT NULL THEN
            RETURN unfit;
        END IF;
    END LOOP;
    -- Vertex tables first, so a well-formed graph creates no placeholder vids,
    -- and fresh statistics before the edges look their endpoints up.
    FOR source_relation IN
        SELECT relation FROM (
            SELECT relation, 0 AS edges FROM hydradb.graph_vertex_tables WHERE graph = graph_name
            UNION ALL SELECT relation, 1 FROM hydradb.graph_edge_tables WHERE graph = graph_name
        ) declared GROUP BY relation ORDER BY min(edges), relation
    LOOP
        IF NOT analyzed AND EXISTS (SELECT 1 FROM hydradb.graph_edge_tables WHERE graph = graph_name AND relation = source_relation) THEN
            ANALYZE hydradb.graph_vertices;
            analyzed := true;
        END IF;
        FOR statement IN SELECT * FROM hydradb.adjacency_statements(graph_name, source_relation, 'i', format('%s r', source_relation)) LOOP
            EXECUTE statement USING graph_id;
        END LOOP;
    END LOOP;
    ANALYZE hydradb.graph_vertices, hydradb.graph_adjacency;
    RETURN NULL;
END
$$;
REVOKE ALL ON FUNCTION hydradb.fill_graph(text) FROM PUBLIC;

-- Validates one table declaration and resolves its schema-qualified name.
CREATE FUNCTION hydradb.graph_table_relation(spec jsonb, fields text[], required text[])
RETURNS regclass LANGUAGE plpgsql STABLE SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    field text;
    relation regclass;
BEGIN
    IF jsonb_typeof(spec) IS DISTINCT FROM 'object' THEN
        RAISE EXCEPTION 'each table declaration must be a JSON object';
    END IF;
    SELECT string_agg(k, ', ' ORDER BY k) INTO field FROM jsonb_object_keys(spec) k WHERE k <> ALL (fields);
    IF field IS NOT NULL THEN
        RAISE EXCEPTION 'unknown table declaration field(s): %', field;
    END IF;
    FOREACH field IN ARRAY fields LOOP
        IF spec ? field AND (jsonb_typeof(spec -> field) <> 'string' OR spec ->> field = '') THEN
            RAISE EXCEPTION 'table declaration field % must be a non-empty string', field;
        END IF;
    END LOOP;
    FOREACH field IN ARRAY required LOOP
        IF NOT spec ? field THEN
            RAISE EXCEPTION 'table declaration % is missing %', spec, field;
        END IF;
    END LOOP;
    relation := to_regclass(spec ->> 'table');
    IF relation IS NULL THEN
        RAISE EXCEPTION 'table % does not exist; use a schema-qualified name', spec ->> 'table';
    END IF;
    RETURN relation;
END
$$;
REVOKE ALL ON FUNCTION hydradb.graph_table_relation(jsonb, text[], text[]) FROM PUBLIC;

-- The type of a declared column.
CREATE FUNCTION hydradb.graph_column_type(relation regclass, column_name name)
RETURNS oid LANGUAGE plpgsql STABLE SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    column_type oid;
BEGIN
    SELECT atttypid INTO column_type FROM pg_attribute
        WHERE attrelid = relation AND attname = column_name AND attnum > 0 AND NOT attisdropped;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'column % does not exist in %', column_name, relation;
    END IF;
    RETURN column_type;
END
$$;
REVOKE ALL ON FUNCTION hydradb.graph_column_type(regclass, name) FROM PUBLIC;

-- Why the adjacency tables cannot hold a relation's rows, or NULL when they
-- can: they can for an ordinary permanent table, neither a parent nor a child
-- in an inheritance tree (so every write to its rows fires its own triggers),
-- whose row-level security, if enabled, does not apply to the extension owner,
-- who fills and reads the adjacency tables. The one test create_graph,
-- refresh_graph and every query apply.
CREATE FUNCTION hydradb.source_unfit(relation oid) RETURNS text
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
    SELECT CASE
        WHEN c.relkind <> 'r' THEN format('%s is not an ordinary table', c.oid::regclass)
        WHEN c.relpersistence <> 'p' THEN format('%s is not a permanent table', c.oid::regclass)
        WHEN c.relhassubclass OR EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = c.oid)
            THEN format('%s is part of an inheritance tree, whose writes would not all fire its triggers', c.oid::regclass)
        WHEN row_security_active(c.oid)
            THEN format('row-level security on %s applies to the extension owner, who fills and reads the adjacency tables', c.oid::regclass)
        END
    FROM pg_class c WHERE c.oid = relation
$$;

-- The usage report's code for a reason hydradb.source_unfit gave, in the
-- same order as its cases.
CREATE FUNCTION hydradb.unfit_code(reason text) RETURNS text
LANGUAGE sql IMMUTABLE SET search_path = pg_catalog, pg_temp AS $$
    SELECT CASE
        WHEN reason LIKE '% is not an ordinary table' THEN 'not_ordinary_table'
        WHEN reason LIKE '% is not a permanent table' THEN 'not_permanent_table'
        WHEN reason LIKE '% is part of an inheritance tree,%' THEN 'inheritance'
        WHEN reason LIKE 'row-level security on %' THEN 'rls_owner'
    END
$$;

-- Whether hydradb.source_unfit finds nothing wrong with a relation.
CREATE FUNCTION hydradb.source_indexable(relation oid) RETURNS boolean
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
    SELECT hydradb.source_unfit(relation) IS NULL
$$;

-- Each edge endpoint of graph `graph_name` (every graph when NULL) with the
-- foreign key that makes it reference the vertex key it names: one column, to
-- that key, enforced and not deferrable. `problem` says what is wrong, or is
-- NULL: 'missing', 'deferrable' or 'not enforced' (PostgreSQL 18). Without
-- such a key, a link can name a vertex another open transaction is creating:
-- its trigger then waits for that transaction to give the vertex a vid, and
-- two transactions that each link to the other's new vertex deadlock. With
-- one, the link waits for the vertex row itself, or fails, before any trigger
-- runs. A NOT VALID key counts: it checks every write from when it was added.
CREATE FUNCTION hydradb.endpoint_foreign_keys(graph_name text)
RETURNS TABLE (graph text, relation regclass, column_name name, role text, vertex_relation regclass, key_column name,
    conname name, problem text)
LANGUAGE sql STABLE SET search_path = pg_catalog, pg_temp AS $$
    SELECT e.graph, e.relation, ep.column_name, ep.role, v.relation, v.key_column, fk.conname,
        CASE WHEN fk.conname IS NULL THEN 'missing' WHEN NOT fk.enforced THEN 'not enforced'
            WHEN fk.condeferrable THEN 'deferrable' END
    FROM hydradb.graph_edge_tables e
    CROSS JOIN LATERAL (VALUES (e.source_column, e.source_label, 'source'), (e.target_column, e.target_label, 'target'))
        AS ep(column_name, label, role)
    JOIN hydradb.graph_vertex_tables v ON v.graph = e.graph AND v.label = ep.label
    JOIN pg_attribute a ON a.attrelid = e.relation AND a.attname = ep.column_name AND NOT a.attisdropped
    JOIN pg_attribute k ON k.attrelid = v.relation AND k.attname = v.key_column AND NOT k.attisdropped
    LEFT JOIN LATERAL (
        -- conenforced exists from PostgreSQL 18.
        SELECT c.conname, c.condeferrable, (to_jsonb(c) ->> 'conenforced') IS DISTINCT FROM 'false' AS enforced
        FROM pg_constraint c
        WHERE c.contype = 'f' AND c.conrelid = e.relation AND c.confrelid = v.relation
            AND c.conkey = ARRAY[a.attnum] AND c.confkey = ARRAY[k.attnum]
        ORDER BY (to_jsonb(c) ->> 'conenforced') IS NOT DISTINCT FROM 'false', c.condeferrable, c.conname LIMIT 1
    ) fk ON true
    WHERE graph_name IS NULL OR e.graph = graph_name
$$;
REVOKE ALL ON FUNCTION hydradb.endpoint_foreign_keys(text) FROM PUBLIC;

-- Raises unless the triggers of an indexed graph can mirror every write its
-- tables accept, so registering a graph never makes a write fail: every
-- declared column is NOT NULL, and each vertex and edge table's key column has
-- a non-partial unique index of its own: for edges an immediate one, as the
-- adjacency tables' primary key requires; for vertices it keeps a key to one
-- row once a transaction commits, so a query never meets a duplicate the table
-- let in. A deferred vertex key check is enough: `rows` counts a duplicate a
-- statement holds until commit. Also raises for a declared generated column: ALTER
-- COLUMN ... SET EXPRESSION (PostgreSQL 17) recomputes one in a table rewrite
-- that fires no triggers, and the rewrite looks to an event trigger like one
-- for an added column's default, so it could neither be mirrored nor told
-- apart and refused. Also raises for an edge endpoint without an enforced,
-- not deferrable foreign key to the vertex key it names
-- (hydradb.endpoint_foreign_keys). With `warn`, also warns for a text key
-- without a length bound, since a B-tree entry holds about 2.7 kB and a write
-- of a longer key would stop the graph's queries.
CREATE FUNCTION hydradb.check_mirrorable(graph_name text, warn boolean) RETURNS void
LANGUAGE plpgsql STABLE SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    declared record;
BEGIN
    FOR declared IN
        SELECT d.relation, d.column_name, d.role, a.attnotnull, a.attgenerated, a.atttypid, a.atttypmod, d.is_key
        FROM (
            SELECT relation, key_column AS column_name, 'key' AS role, true AS is_key
                FROM hydradb.graph_vertex_tables WHERE graph = graph_name
            UNION ALL SELECT relation, sort_key_column, 'sort key', false
                FROM hydradb.graph_vertex_tables WHERE graph = graph_name AND sort_key_column IS NOT NULL
            UNION ALL SELECT relation, key_column, 'key', true FROM hydradb.graph_edge_tables WHERE graph = graph_name
            UNION ALL SELECT relation, source_column, 'source', false FROM hydradb.graph_edge_tables WHERE graph = graph_name
            UNION ALL SELECT relation, target_column, 'target', false FROM hydradb.graph_edge_tables WHERE graph = graph_name
            UNION ALL SELECT relation, kind_column, 'kind', false
                FROM hydradb.graph_edge_tables WHERE graph = graph_name AND kind_column IS NOT NULL
        ) d
        JOIN pg_attribute a ON a.attrelid = d.relation AND a.attname = d.column_name AND NOT a.attisdropped
        ORDER BY d.relation, d.column_name
    LOOP
        IF declared.attgenerated <> '' THEN
            PERFORM hydradb.usage_count('generated_column');
            RAISE EXCEPTION 'graph %: column % of % is a declared % but a generated column, which ALTER COLUMN ... SET EXPRESSION would recompute without firing its triggers',
                    graph_name, declared.column_name, declared.relation, declared.role
                USING HINT = 'Declare an ordinary column instead, filled by the application or a BEFORE trigger.';
        END IF;
        IF NOT declared.attnotnull THEN
            PERFORM hydradb.usage_count('nullable_column');
            RAISE EXCEPTION 'graph %: column % of % is a declared % but allows NULL, and its triggers would reject a write of one',
                    graph_name, declared.column_name, declared.relation, declared.role
                USING HINT = format('ALTER TABLE %s ALTER COLUMN %I SET NOT NULL', declared.relation, declared.column_name);
        END IF;
        IF warn AND declared.is_key AND (declared.atttypid = 'text'::regtype
                OR (declared.atttypid IN ('varchar'::regtype, 'bpchar'::regtype)
                    AND (declared.atttypmod < 0 OR declared.atttypmod - 4 > 600))) THEN
            RAISE WARNING 'graph %: key % of % has no length bound; a value longer than 2000 bytes stops its queries until hydradb.refresh_graph',
                    graph_name, declared.column_name, declared.relation
                USING HINT = format('ALTER TABLE %s ADD CHECK (octet_length(%I) <= 2000)', declared.relation, declared.column_name);
        END IF;
    END LOOP;
    FOR declared IN
        SELECT k.relation, k.key_column, k.role FROM (
            SELECT relation, key_column, 'vertex' AS role FROM hydradb.graph_vertex_tables WHERE graph = graph_name
            UNION ALL SELECT relation, key_column, 'edge' FROM hydradb.graph_edge_tables WHERE graph = graph_name
        ) k
        WHERE NOT EXISTS (
            SELECT 1 FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = k.relation AND a.attname = k.key_column AND NOT a.attisdropped
            WHERE i.indrelid = k.relation AND i.indisunique AND (i.indimmediate OR k.role = 'vertex') AND i.indisvalid
                AND i.indpred IS NULL AND i.indexprs IS NULL AND i.indnkeyatts = 1 AND i.indkey[0] = a.attnum)
        ORDER BY k.role, k.relation, k.key_column
    LOOP
        PERFORM hydradb.usage_count('no_unique_index');
        RAISE EXCEPTION 'graph %: % key % of % has no %unique index of its own, so the table would accept a duplicate key',
                graph_name, declared.role, declared.key_column, declared.relation,
                CASE declared.role WHEN 'edge' THEN 'immediate ' ELSE '' END
            USING HINT = format('ALTER TABLE %s ADD UNIQUE (%I)', declared.relation, declared.key_column);
    END LOOP;
    FOR declared IN
        SELECT * FROM hydradb.endpoint_foreign_keys(graph_name) f
        WHERE f.problem IS NOT NULL
        ORDER BY f.relation, f.column_name
    LOOP
        IF declared.problem = 'missing' THEN
            RAISE EXCEPTION 'graph %: column % of % is an edge % with no foreign key to key % of %',
                    graph_name, declared.column_name, declared.relation, declared.role, declared.key_column, declared.vertex_relation
                USING DETAIL = 'Without one, two transactions that each create a vertex the other links to can deadlock.',
                    HINT = format('ALTER TABLE %s ADD FOREIGN KEY (%I) REFERENCES %s (%I)',
                        declared.relation, declared.column_name, declared.vertex_relation, declared.key_column);
        END IF;
        RAISE EXCEPTION 'graph %: foreign key % on column % of % is %', graph_name, declared.conname, declared.column_name,
                declared.relation, declared.problem
            USING DETAIL = 'It must check every write when it is made, before the graph''s triggers run.',
                HINT = format('ALTER TABLE %s ALTER CONSTRAINT %I %s', declared.relation, declared.conname,
                    CASE declared.problem WHEN 'deferrable' THEN 'NOT DEFERRABLE' ELSE 'ENFORCED' END);
    END LOOP;
END
$$;
REVOKE ALL ON FUNCTION hydradb.check_mirrorable(text, boolean) FROM PUBLIC;

-- Raises unless this transaction reads at READ COMMITTED. create_graph and
-- refresh_graph fill the adjacency tables after locking the sources; under
-- REPEATABLE READ or SERIALIZABLE the fill would read the snapshot the
-- transaction took before those locks, and miss rows committed since, with
-- nothing to tell the graph it is incomplete.
CREATE FUNCTION hydradb.require_read_committed(graph_name text, operation text) RETURNS void
LANGUAGE plpgsql STABLE SET search_path = pg_catalog, pg_temp AS $$
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        PERFORM hydradb.usage_count('isolation_level');
        RAISE EXCEPTION 'graph %: % needs a READ COMMITTED transaction, not %', graph_name, operation,
                upper(current_setting('transaction_isolation'))
            USING ERRCODE = 'feature_not_supported',
                HINT = 'Its fill reads the sources after locking them, which a transaction-wide snapshot taken earlier cannot see.';
    END IF;
END
$$;
REVOKE ALL ON FUNCTION hydradb.require_read_committed(text, text) FROM PUBLIC;

-- Locks every relation in SHARE ROW EXCLUSIVE mode, all or none. Each attempt
-- takes them with NOWAIT inside a subtransaction, so a busy relation rolls the
-- attempt back and releases the locks it took: the caller never holds one
-- relation while waiting for another, so it cannot deadlock with a writer that
-- writes them in another order, and never queues ahead of writers. Attempts
-- repeat every 50 ms until lock_timeout, or a minute when that is 0.
CREATE FUNCTION hydradb.lock_sources(relations regclass[]) RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    budget CONSTANT interval := COALESCE(NULLIF(current_setting('lock_timeout')::interval, interval '0'), interval '1 minute');
    deadline CONSTANT timestamptz := clock_timestamp() + budget;
    relation regclass;
BEGIN
    LOOP
        BEGIN
            FOREACH relation IN ARRAY relations LOOP
                EXECUTE format('LOCK TABLE %s IN SHARE ROW EXCLUSIVE MODE NOWAIT', relation);
            END LOOP;
            RETURN;
        EXCEPTION WHEN lock_not_available THEN
            IF clock_timestamp() >= deadline THEN
                RAISE EXCEPTION 'could not lock graph source % within %: a transaction is writing it', relation, budget
                    USING ERRCODE = 'lock_not_available',
                        HINT = 'Retry when the writes finish, or raise lock_timeout.';
            END IF;
        END;
        PERFORM pg_sleep(0.05);
    END LOOP;
END
$$;
REVOKE ALL ON FUNCTION hydradb.lock_sources(regclass[]) FROM PUBLIC;

-- The type of a declared key or edge endpoint, which must be one whose text
-- form identifies its value: queries compare keys as text, so a type whose
-- equal values print differently (numeric 1 and 1.00, a case-insensitive
-- text) would make an edge miss the vertex it references. For the same reason
-- its collation must be deterministic: a nondeterministic one can call 'Alice'
-- and 'alice' equal.
CREATE FUNCTION hydradb.graph_key_type(relation regclass, column_name name)
RETURNS oid LANGUAGE plpgsql STABLE SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    column_type oid := hydradb.graph_column_type(relation, column_name);
    collation_name name;
BEGIN
    SELECT c.collname INTO collation_name FROM pg_attribute a JOIN pg_collation c ON c.oid = a.attcollation
    WHERE a.attrelid = relation AND a.attname = column_name AND NOT c.collisdeterministic;
    IF FOUND THEN
        PERFORM hydradb.usage_count('nondeterministic_collation');
        RAISE EXCEPTION 'column % of % is a graph key or endpoint with nondeterministic collation %, under which differently spelled keys are equal',
                column_name, relation, collation_name
            USING HINT = 'Keys and endpoints need a deterministic collation; declare the column with COLLATE "C" or the database default.';
    END IF;
    IF column_type <> ALL (ARRAY['int2'::regtype, 'int4'::regtype, 'int8'::regtype, 'text'::regtype,
            'varchar'::regtype, 'bpchar'::regtype, 'uuid'::regtype]::oid[]) THEN
        PERFORM hydradb.usage_count('unsupported_type');
        RAISE EXCEPTION 'column % of % is a graph key or endpoint of type %, whose text form does not identify its value',
                column_name, relation, column_type::regtype
            USING HINT = 'Keys and endpoints must be integer, text, varchar, char or uuid; declare a column of one of those types.';
    END IF;
    RETURN column_type;
END
$$;
REVOKE ALL ON FUNCTION hydradb.graph_key_type(regclass, name) FROM PUBLIC;

-- Raises unless the session's login role can act as the owner of every
-- relation. create_graph, refresh_graph and drop_graph run as the extension
-- owner, so without this a role granted EXECUTE on them could add triggers to,
-- lock, rebuild or deregister graphs over tables it does not own, which only
-- an owner may do. A SECURITY DEFINER function cannot see the role that called
-- it, so the check is on session_user: SET ROLE is not a boundary, since a
-- session can always RESET ROLE to its login role. The login role acts as an
-- owner when it has the owner's privileges, or may SET ROLE to the owner; from
-- PostgreSQL 16 a membership can grant neither (WITH INHERIT FALSE, SET
-- FALSE), and before it every membership allowed SET ROLE. Superusers pass.
CREATE FUNCTION hydradb.require_owner(graph_name text, relations regclass[]) RETURNS void
LANGUAGE plpgsql STABLE SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    relation regclass;
    owner_name name;
BEGIN
    SELECT c.oid::regclass, pg_get_userbyid(c.relowner) INTO relation, owner_name
    FROM pg_class c
    WHERE c.oid = ANY (relations) AND NOT CASE
        WHEN current_setting('server_version_num')::integer >= 160000
            THEN pg_has_role(session_user, c.relowner, 'USAGE') OR pg_has_role(session_user, c.relowner, 'SET')
        ELSE pg_has_role(session_user, c.relowner, 'MEMBER') END
    ORDER BY c.oid LIMIT 1;
    IF FOUND THEN
        PERFORM hydradb.usage_count('not_owner');
        RAISE EXCEPTION 'graph %: % is owned by %, and only its owner may register, refresh or drop a graph over it',
                graph_name, relation, owner_name
            USING ERRCODE = 'insufficient_privilege';
    END IF;
END
$$;
REVOKE ALL ON FUNCTION hydradb.require_owner(text, regclass[]) FROM PUBLIC;

-- Gives each table of graph `graph_name` the four statement-level triggers
-- that keep its adjacency tables current (see hydradb.maintain_rows), firing
-- for every write: creates any that are missing and sets all of them to
-- ENABLE ALWAYS. create_graph installs them; refresh_graph puts back any that
-- were dropped or disabled since, so that once it has rebuilt the graph every
-- later write is mirrored again. Neither change of mode is a step away from
-- always, so neither flags the graph (check_source_changes).
CREATE FUNCTION hydradb.install_triggers(graph_name text) RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    trigger_prefix CONSTANT text := 'pg_hydradb_' || md5(graph_name);
    suffix text;
    source_relation regclass;
    foreign_trigger name;
BEGIN
    FOR source_relation IN
        SELECT relation FROM hydradb.graph_vertex_tables WHERE graph = graph_name
        UNION SELECT relation FROM hydradb.graph_edge_tables WHERE graph = graph_name
        ORDER BY 1
    LOOP
        -- CREATE OR REPLACE replaces a trigger of the same name whatever it
        -- runs. Replacing pg_hydradb's own (left by a restore, say) is the
        -- point; replacing someone else's would silently drop their logic.
        SELECT tgname INTO foreign_trigger FROM pg_trigger
        WHERE tgrelid = source_relation
            AND tgname IN (SELECT trigger_prefix || s FROM unnest(ARRAY['_i', '_d', '_r', '_t']) s)
            AND tgfoid NOT IN ('hydradb.maintain_rows()'::regprocedure)
        ORDER BY tgname LIMIT 1;
        IF FOUND THEN
            RAISE EXCEPTION 'trigger % on % does not belong to pg_hydradb; rename it before registering or refreshing graph %',
                foreign_trigger, source_relation, graph_name;
        END IF;
        EXECUTE format('CREATE OR REPLACE TRIGGER %I AFTER INSERT ON %s REFERENCING NEW TABLE AS new_rows
            FOR EACH STATEMENT EXECUTE FUNCTION hydradb.maintain_rows(%L)',
            trigger_prefix || '_i', source_relation, graph_name);
        EXECUTE format('CREATE OR REPLACE TRIGGER %I AFTER DELETE ON %s REFERENCING OLD TABLE AS old_rows
            FOR EACH STATEMENT EXECUTE FUNCTION hydradb.maintain_rows(%L)',
            trigger_prefix || '_d', source_relation, graph_name);
        -- Every UPDATE, not UPDATE OF the declared columns: PostgreSQL picks
        -- column triggers from the SET list, so a BEFORE trigger that changes
        -- a declared column would go unseen. The transition tables hold the
        -- rows as stored, and rows whose declared columns did not change
        -- cancel out.
        EXECUTE format('CREATE OR REPLACE TRIGGER %I AFTER UPDATE ON %s
            REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
            FOR EACH STATEMENT EXECUTE FUNCTION hydradb.maintain_rows(%L)',
            trigger_prefix || '_r', source_relation, graph_name);
        EXECUTE format('CREATE OR REPLACE TRIGGER %I AFTER TRUNCATE ON %s
            FOR EACH STATEMENT EXECUTE FUNCTION hydradb.maintain_rows(%L)',
            trigger_prefix || '_t', source_relation, graph_name);
        FOREACH suffix IN ARRAY ARRAY['_i', '_d', '_r', '_t'] LOOP
            EXECUTE format('ALTER TABLE %s ENABLE ALWAYS TRIGGER %I', source_relation, trigger_prefix || suffix);
        END LOOP;
    END LOOP;
END
$$;
REVOKE ALL ON FUNCTION hydradb.install_triggers(text) FROM PUBLIC;

-- Registers a graph over any number of vertex tables and edge tables:
--
--   vertex_tables: [{"label", "table", "key", "sort_key"?}, ...]
--   edge_tables:   [{"label", "table", "key", "source", "source_label",
--                    "target", "target_label", "kind"?}, ...]
--
-- Table names must be schema-qualified and name ordinary tables; views and
-- partitioned tables are rejected. `key` columns hold values unique within their table. An
-- edge's `source` and `target` columns must have the type of the
-- key they reference, or both must be integers. A graph that would be indexed
-- must also pass hydradb.check_mirrorable, so that its triggers never reject a
-- write its tables accept.
--
-- Every graph is indexed: triggers keep its adjacency tables current, and
-- they are filled here, with writes to the sources blocked until this
-- transaction ends. Queries read only the adjacency tables, so a graph they
-- could not hold is refused: its sources must be ordinary permanent tables
-- without inheritance, with declared columns of types whose text form does
-- not depend on session settings, and whose row-level security, if enabled,
-- does not apply to this function's owner; and a value too long for them is
-- refused too. A caller row-level security applies to cannot query the graph.
CREATE FUNCTION hydradb.create_graph(graph_name text, vertex_tables jsonb, edge_tables jsonb DEFAULT '[]'::jsonb)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    integer_types CONSTANT oid[] := ARRAY['int2'::regtype, 'int4'::regtype, 'int8'::regtype]::oid[];
    stable_types CONSTANT oid[] := ARRAY['int2'::regtype, 'int4'::regtype, 'int8'::regtype, 'text'::regtype,
        'varchar'::regtype, 'bpchar'::regtype, 'uuid'::regtype]::oid[];
    vertex_relations regclass[] := '{}';
    edge_relations regclass[] := '{}';
    spec jsonb;
    source_relation regclass;
    relation_kind "char";
    vertex record;
    edge record;
    endpoint record;
    key_type oid;
    endpoint_type oid;
    column_types oid[] := '{}';
    unfit text;
BEGIN
    PERFORM hydradb.usage_count('create_graph');
    IF graph_name IS NULL OR graph_name = '' THEN
        RAISE EXCEPTION 'graph name is required';
    END IF;
    PERFORM hydradb.require_read_committed(graph_name, 'create_graph');
    IF jsonb_typeof(vertex_tables) IS DISTINCT FROM 'array' OR jsonb_array_length(vertex_tables) = 0
        OR jsonb_typeof(edge_tables) IS DISTINCT FROM 'array' THEN
        RAISE EXCEPTION 'vertex_tables must be a non-empty JSON array and edge_tables a JSON array';
    END IF;
    IF jsonb_array_length(vertex_tables) > 64 OR jsonb_array_length(edge_tables) > 64 THEN
        RAISE EXCEPTION 'a graph has at most 64 vertex tables and 64 edge tables';
    END IF;
    IF EXISTS (SELECT 1 FROM hydradb.graphs WHERE name = graph_name) THEN
        RAISE EXCEPTION 'graph % is already registered', graph_name;
    END IF;
    FOR spec IN SELECT value FROM jsonb_array_elements(vertex_tables) WITH ORDINALITY AS t(value, i) ORDER BY i LOOP
        vertex_relations := vertex_relations || hydradb.graph_table_relation(spec,
            ARRAY['label', 'table', 'key', 'sort_key'], ARRAY['label', 'table', 'key']);
    END LOOP;
    FOR spec IN SELECT value FROM jsonb_array_elements(edge_tables) WITH ORDINALITY AS t(value, i) ORDER BY i LOOP
        edge_relations := edge_relations || hydradb.graph_table_relation(spec,
            ARRAY['label', 'table', 'key', 'source', 'source_label', 'target', 'target_label', 'kind'],
            ARRAY['label', 'table', 'key', 'source', 'source_label', 'target', 'target_label']);
    END LOOP;
    FOR source_relation IN
        SELECT DISTINCT r FROM unnest(vertex_relations || edge_relations) r ORDER BY r
    LOOP
        SELECT relkind INTO relation_kind FROM pg_class WHERE oid = source_relation;
        IF relation_kind <> 'r' THEN
            PERFORM hydradb.usage_count('not_ordinary_table');
            RAISE EXCEPTION 'graph sources must be ordinary tables; % is not', source_relation;
        END IF;
        -- The registration outlives the session; a temporary table does not,
        -- and its OID could later name another table.
        IF (SELECT relpersistence FROM pg_class WHERE oid = source_relation) = 't' THEN
            PERFORM hydradb.usage_count('not_permanent_table');
            RAISE EXCEPTION 'graph sources must be permanent tables; % is temporary', source_relation;
        END IF;
    END LOOP;
    PERFORM hydradb.require_owner(graph_name, vertex_relations || edge_relations);
    PERFORM hydradb.lock_sources(ARRAY(SELECT DISTINCT r FROM unnest(vertex_relations || edge_relations) r ORDER BY r));
    -- An owner can change while the locks wait: check again once they are held,
    -- since ALTER TABLE ... OWNER TO now waits for this transaction.
    PERFORM hydradb.require_owner(graph_name, vertex_relations || edge_relations);
    INSERT INTO hydradb.graphs(name, indexed) VALUES (graph_name, true);
    BEGIN
        INSERT INTO hydradb.graph_vertex_tables (graph, label, ordinal, relation, key_column, sort_key_column)
        SELECT graph_name, t.spec ->> 'label', rank() OVER (ORDER BY t.spec ->> 'label' COLLATE "C") - 1,
            vertex_relations[t.i], t.spec ->> 'key', t.spec ->> 'sort_key'
        FROM jsonb_array_elements(vertex_tables) WITH ORDINALITY AS t(spec, i);
        INSERT INTO hydradb.graph_edge_tables (graph, label, ordinal, relation, key_column, source_column, source_label,
            target_column, target_label, kind_column)
        SELECT graph_name, t.spec ->> 'label', rank() OVER (ORDER BY t.spec ->> 'label' COLLATE "C") - 1,
            edge_relations[t.i], t.spec ->> 'key', t.spec ->> 'source',
            t.spec ->> 'source_label', t.spec ->> 'target', t.spec ->> 'target_label', t.spec ->> 'kind'
        FROM jsonb_array_elements(edge_tables) WITH ORDINALITY AS t(spec, i);
    EXCEPTION
        WHEN unique_violation THEN
            RAISE EXCEPTION 'vertex labels and edge labels must each be unique within a graph';
        WHEN foreign_key_violation THEN
            RAISE EXCEPTION 'every source_label and target_label must name a vertex table of the graph';
    END;
    FOR vertex IN SELECT * FROM hydradb.graph_vertex_tables WHERE graph = graph_name LOOP
        column_types := column_types || hydradb.graph_key_type(vertex.relation, vertex.key_column);
        IF vertex.sort_key_column IS NOT NULL THEN
            column_types := column_types || hydradb.graph_column_type(vertex.relation, vertex.sort_key_column);
        END IF;
    END LOOP;
    FOR edge IN SELECT * FROM hydradb.graph_edge_tables WHERE graph = graph_name LOOP
        column_types := column_types || hydradb.graph_key_type(edge.relation, edge.key_column);
        IF edge.kind_column IS NOT NULL THEN
            column_types := column_types || hydradb.graph_column_type(edge.relation, edge.kind_column);
        END IF;
        FOR endpoint IN
            SELECT e.column_name, v.relation, v.key_column
            FROM (VALUES (edge.source_column, edge.source_label), (edge.target_column, edge.target_label))
                AS e(column_name, label)
            JOIN hydradb.graph_vertex_tables v ON v.graph = graph_name AND v.label = e.label
        LOOP
            endpoint_type := hydradb.graph_key_type(edge.relation, endpoint.column_name);
            key_type := hydradb.graph_key_type(endpoint.relation, endpoint.key_column);
            IF endpoint_type <> key_type AND NOT (endpoint_type = ANY (integer_types) AND key_type = ANY (integer_types)) THEN
                PERFORM hydradb.usage_count('endpoint_type_mismatch');
                RAISE EXCEPTION 'column % of % has type %, but the key % of % has type %',
                    endpoint.column_name, edge.relation, endpoint_type::regtype,
                    endpoint.key_column, endpoint.relation, key_type::regtype;
            END IF;
            column_types := column_types || endpoint_type;
        END LOOP;
    END LOOP;
    SELECT hydradb.source_unfit(r) INTO unfit FROM unnest(vertex_relations || edge_relations) r
    WHERE hydradb.source_unfit(r) IS NOT NULL LIMIT 1;
    IF unfit IS NOT NULL THEN
        PERFORM hydradb.usage_count(hydradb.unfit_code(unfit));
        RAISE EXCEPTION 'graph %: %', graph_name, unfit
            USING HINT = 'Queries read only the adjacency tables, so every source must be one they can hold.';
    END IF;
    IF NOT column_types <@ stable_types THEN
        PERFORM hydradb.usage_count('unsupported_type');
        RAISE EXCEPTION 'graph %: a declared column has type %, whose text form depends on session settings', graph_name,
                (SELECT string_agg(DISTINCT t::regtype::text, ', ') FROM unnest(column_types) t WHERE t <> ALL (stable_types))
            USING HINT = 'Declare columns of type integer, text, varchar, char or uuid.';
    END IF;
    PERFORM hydradb.check_mirrorable(graph_name, true);
    -- What the graph depends on each declared column being, which an ALTER
    -- TABLE may not change while the graph exists (check_source_changes).
    UPDATE hydradb.graph_vertex_tables SET state = jsonb_strip_nulls(jsonb_build_object(
            'key', hydradb.column_state(relation, key_column),
            'sort key', hydradb.column_state(relation, sort_key_column)))
        WHERE graph = graph_name;
    UPDATE hydradb.graph_edge_tables SET state = jsonb_strip_nulls(jsonb_build_object(
            'key', hydradb.column_state(relation, key_column),
            'source', hydradb.column_state(relation, source_column),
            'target', hydradb.column_state(relation, target_column),
            'kind', hydradb.column_state(relation, kind_column)))
        WHERE graph = graph_name;
    PERFORM hydradb.install_triggers(graph_name);
    unfit := hydradb.fill_graph(graph_name);
    IF unfit IS NOT NULL THEN
        PERFORM hydradb.usage_count('long_key');
        RAISE EXCEPTION 'graph %: %', graph_name, unfit
            USING HINT = 'Shorten or remove that value, then register the graph.';
    END IF;
END
$$;
REVOKE ALL ON FUNCTION hydradb.create_graph(text, jsonb, jsonb) FROM PUBLIC;

-- Deregisters a graph: drops the triggers create_graph installed on its tables
-- and deletes its catalog and adjacency rows, leaving the tables and their
-- data as they are. The triggers are found by name and by the function they
-- run, so this also cleans up after a graph whose `hydradb.graphs` row was
-- deleted by hand, whose leftover triggers would otherwise fail every write to
-- its tables, and never drops a trigger of the same name that runs anything
-- else.
CREATE FUNCTION hydradb.drop_graph(graph_name text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    trigger_prefix CONSTANT text := 'pg_hydradb_' || md5(graph_name);
    registered boolean;
    graph_id bigint;
    expected bigint;
    source_relations regclass[];
    source_relation regclass;
    trigger_name name;
BEGIN
    PERFORM hydradb.usage_count('drop_graph');
    IF graph_name IS NULL OR graph_name = '' THEN
        RAISE EXCEPTION 'graph name is required';
    END IF;
    -- Its owner check after the locks must read who owns the tables now, not
    -- a transaction snapshot taken before it waited.
    PERFORM hydradb.require_read_committed(graph_name, 'drop_graph');
    registered := false;
    -- The registration this call found, before waiting for the tables.
    SELECT registration INTO expected FROM hydradb.graphs WHERE name = graph_name;
    -- Tables first, all at once as create_graph locks them, then the graph
    -- row: a writer holds its table lock while its trigger reads the graph row.
    source_relations := ARRAY(
        SELECT DISTINCT tgrelid::regclass FROM pg_trigger
        WHERE tgname IN (SELECT trigger_prefix || suffix FROM unnest(ARRAY['_i', '_d', '_r', '_t']) suffix)
            AND tgfoid IN ('hydradb.maintain_rows()'::regprocedure)
        ORDER BY 1);
    PERFORM hydradb.require_owner(graph_name, source_relations || ARRAY(
        SELECT relation FROM hydradb.graph_vertex_tables WHERE graph = graph_name
        UNION SELECT relation FROM hydradb.graph_edge_tables WHERE graph = graph_name));
    PERFORM hydradb.lock_sources(source_relations);
    -- An owner can change while the locks wait: check again once they are held,
    -- since ALTER TABLE ... OWNER TO now waits for this transaction.
    PERFORM hydradb.require_owner(graph_name, source_relations || ARRAY(
        SELECT relation FROM hydradb.graph_vertex_tables WHERE graph = graph_name
        UNION SELECT relation FROM hydradb.graph_edge_tables WHERE graph = graph_name));
    -- While it waited, the graph may have been dropped and registered again
    -- under its name, over other tables this call neither checked nor locked.
    IF (SELECT registration FROM hydradb.graphs WHERE name = graph_name) IS DISTINCT FROM expected THEN
        RAISE EXCEPTION 'graph % changed while drop_graph waited for its tables; retry', graph_name
            USING ERRCODE = '40001';
    END IF;
    -- These drops deregister the graph, and check_source_changes would flag
    -- it for losing a trigger. Flagged first, it is skipped, as a flagged
    -- graph is; the row goes below, and the flag rolls back with this call if
    -- it fails. Only the extension owner writes hydradb.graphs, so a caller
    -- cannot mark a graph this way to drop its triggers unnoticed.
    UPDATE hydradb.graphs SET needs_refresh = 'being dropped by hydradb.drop_graph'
    WHERE name = graph_name AND needs_refresh IS NULL;
    FOREACH source_relation IN ARRAY source_relations LOOP
        FOR trigger_name IN
            SELECT tgname FROM pg_trigger WHERE tgrelid = source_relation
                AND tgname IN (SELECT trigger_prefix || suffix FROM unnest(ARRAY['_i', '_d', '_r', '_t']) suffix)
                AND tgfoid IN ('hydradb.maintain_rows()'::regprocedure)
            ORDER BY tgname
        LOOP
            EXECUTE format('DROP TRIGGER %I ON %s', trigger_name, source_relation);
        END LOOP;
        registered := true;
    END LOOP;
    SELECT registration INTO graph_id FROM hydradb.graphs WHERE name = graph_name FOR UPDATE;
    IF NOT (registered OR FOUND) THEN
        RAISE EXCEPTION 'unknown graph %', graph_name;
    END IF;
    IF graph_id IS NOT NULL THEN
        DELETE FROM hydradb.graph_adjacency WHERE graph = graph_id;
        DELETE FROM hydradb.graph_vertices WHERE graph = graph_id;
    ELSE
        -- The graph row was deleted by hand, so its registration is unknown:
        -- remove every adjacency row no registered graph owns.
        DELETE FROM hydradb.graph_adjacency WHERE graph NOT IN (SELECT registration FROM hydradb.graphs);
        DELETE FROM hydradb.graph_vertices WHERE graph NOT IN (SELECT registration FROM hydradb.graphs);
    END IF;
    -- Cascades to the graph's vertex and edge table declarations.
    DELETE FROM hydradb.graphs WHERE name = graph_name;
END
$$;
REVOKE ALL ON FUNCTION hydradb.drop_graph(text) FROM PUBLIC;

-- Rebuilds an indexed graph's adjacency tables from its source tables, with
-- writes to them blocked until this transaction ends, once hydradb.lock_sources
-- has locked them all. Run it after anything
-- that changed the sources without firing their triggers, such as a disabled
-- or dropped trigger or a data-only restore without the adjacency tables.
-- It first puts the graph's triggers back as create_graph installs them
-- (hydradb.install_triggers), so a refresh run while one is still disabled or
-- missing does not leave later writes unmirrored.
CREATE FUNCTION hydradb.refresh_graph(graph_name text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    graph_id bigint;
    expected bigint;
    is_indexed boolean;
    source_relations regclass[];
    source_relation regclass;
    unfit text;
BEGIN
    PERFORM hydradb.usage_count('refresh_graph');
    PERFORM hydradb.require_read_committed(graph_name, 'refresh_graph');
    -- The registration this call found, before waiting for its tables.
    SELECT registration INTO expected FROM hydradb.graphs WHERE name = graph_name;
    source_relations := ARRAY(
        SELECT DISTINCT relation FROM (
            SELECT relation FROM hydradb.graph_vertex_tables WHERE graph = graph_name
            UNION ALL SELECT relation FROM hydradb.graph_edge_tables WHERE graph = graph_name
        ) declared ORDER BY 1);
    PERFORM hydradb.require_owner(graph_name, source_relations);
    PERFORM hydradb.lock_sources(source_relations);
    -- An owner can change while the locks wait: check again once they are held,
    -- since ALTER TABLE ... OWNER TO now waits for this transaction.
    PERFORM hydradb.require_owner(graph_name, source_relations);
    SELECT registration, indexed INTO graph_id, is_indexed FROM hydradb.graphs WHERE name = graph_name FOR UPDATE;
    IF NOT FOUND AND expected IS NULL THEN RAISE EXCEPTION 'unknown graph %', graph_name; END IF;
    -- While it waited, the graph may have been dropped and registered again
    -- under its name, over other tables this call neither checked nor locked.
    IF graph_id IS DISTINCT FROM expected THEN
        RAISE EXCEPTION 'graph % changed while refresh_graph waited for its tables; retry', graph_name
            USING ERRCODE = '40001';
    END IF;
    IF is_indexed THEN
        SELECT hydradb.source_unfit(relation) INTO unfit FROM (
            SELECT relation FROM hydradb.graph_vertex_tables WHERE graph = graph_name
            UNION ALL SELECT relation FROM hydradb.graph_edge_tables WHERE graph = graph_name
        ) declared WHERE hydradb.source_unfit(relation) IS NOT NULL LIMIT 1;
        IF unfit IS NOT NULL THEN
            PERFORM hydradb.usage_count(hydradb.unfit_code(unfit));
            RAISE EXCEPTION 'graph %: %', graph_name, unfit
                USING HINT = 'Fix that source, then refresh the graph again; until then it cannot be queried.';
        END IF;
        PERFORM hydradb.rerecord_states();
        -- A declared column changed while event triggers were off, which
        -- check_source_changes would have refused: the registration no
        -- longer describes the table.
        SELECT format('column %I of %s, the graph''s %s, was %s at registration and is now %s',
                d.column_name, d.relation, d.role, d.recorded, COALESCE(d.now, 'missing'))
        INTO unfit FROM hydradb.declared_changes(graph_name) d ORDER BY d.relation::text, d.column_name LIMIT 1;
        IF unfit IS NOT NULL THEN
            RAISE EXCEPTION 'graph %: %', graph_name, unfit
                USING HINT = 'Undo that change, or drop the graph and register it again.';
        END IF;
        PERFORM hydradb.check_mirrorable(graph_name, false);
        -- Keys and endpoints as create_graph checks them: a collation changed
        -- since registration needs no rewrite, so nothing refused it.
        PERFORM hydradb.graph_key_type(d.relation, d.column_name)
        FROM (
            SELECT relation, key_column AS column_name FROM hydradb.graph_vertex_tables WHERE graph = graph_name
            UNION ALL SELECT relation, key_column FROM hydradb.graph_edge_tables WHERE graph = graph_name
            UNION ALL SELECT relation, source_column FROM hydradb.graph_edge_tables WHERE graph = graph_name
            UNION ALL SELECT relation, target_column FROM hydradb.graph_edge_tables WHERE graph = graph_name
        ) d;
        -- While the graph is still flagged, so putting a trigger back flags
        -- nothing.
        PERFORM hydradb.install_triggers(graph_name);
        DELETE FROM hydradb.graph_adjacency WHERE graph = graph_id;
        DELETE FROM hydradb.graph_vertices WHERE graph = graph_id;
        -- A new registration changes the row's key, which a repeatable-read
        -- writer's KEY SHARE lock in maintain_rows detects: one whose snapshot
        -- predates this refresh retries instead of mirroring with old vids.
        UPDATE hydradb.graphs SET registration = nextval('hydradb.graph_registrations'), needs_refresh = NULL
        WHERE name = graph_name;
        unfit := hydradb.fill_graph(graph_name);
        IF unfit IS NOT NULL THEN
            PERFORM hydradb.usage_count('long_key');
            RAISE EXCEPTION 'graph %: %', graph_name, unfit
                USING HINT = 'Shorten or remove that value, then refresh the graph again; until then it cannot be queried.';
        END IF;
    END IF;
END
$$;
REVOKE ALL ON FUNCTION hydradb.refresh_graph(text) FROM PUBLIC;

-- Seeds name vertices by table label and key, as {"person": [7, 8],
-- "company": "acme"}: a JSON integer for an integer key column, a JSON string
-- for every other type. Results name vertices and edges as
-- {"label": ..., "key": ...}.
CREATE FUNCTION hydradb.reachable_vertices(graph_name text, seeds jsonb, options jsonb DEFAULT '{}'::jsonb)
RETURNS jsonb AS 'MODULE_PATHNAME', 'reachable_vertices_wrapper'
LANGUAGE c STABLE STRICT PARALLEL UNSAFE SET search_path = pg_catalog, pg_temp;

CREATE FUNCTION hydradb.reachable_edges(graph_name text, seeds jsonb, options jsonb DEFAULT '{}'::jsonb)
RETURNS jsonb AS 'MODULE_PATHNAME', 'reachable_edges_wrapper'
LANGUAGE c STABLE STRICT PARALLEL UNSAFE SET search_path = pg_catalog, pg_temp;

-- Simple paths, shortest first, from the sources to any vertex or to the
-- targets. The graph is unweighted.
CREATE FUNCTION hydradb.paths_from(graph_name text, sources jsonb, options jsonb DEFAULT '{}'::jsonb)
RETURNS jsonb AS 'MODULE_PATHNAME', 'paths_from_wrapper'
LANGUAGE c STABLE STRICT PARALLEL UNSAFE SET search_path = pg_catalog, pg_temp;

CREATE FUNCTION hydradb.paths_between(graph_name text, sources jsonb, targets jsonb, options jsonb DEFAULT '{}'::jsonb)
RETURNS jsonb AS 'MODULE_PATHNAME', 'paths_between_wrapper'
LANGUAGE c STABLE STRICT PARALLEL UNSAFE SET search_path = pg_catalog, pg_temp;

-- Counts a call or a refusal for the anonymous usage report, in shared memory,
-- so the count survives the refusal's rollback. Does nothing unless
-- pg_hydradb is in shared_preload_libraries; ignores names it does not know.
CREATE FUNCTION hydradb.usage_count(event text) RETURNS void
AS 'MODULE_PATHNAME', 'usage_count_wrapper'
LANGUAGE c VOLATILE STRICT PARALLEL SAFE SET search_path = pg_catalog, pg_temp;
-- Only the extension's own functions count, so no role can invent calls or
-- refusals. Its callers are SECURITY DEFINER functions, or helpers only they
-- call, so they run as the extension owner.
REVOKE ALL ON FUNCTION hydradb.usage_count(text) FROM PUBLIC;

-- The anonymous usage report this server sends once an hour, for this database
-- only: counts, ranges and timestamps, never names, keys or query text. See
-- "Usage reports" in README.md.
CREATE FUNCTION hydradb.usage_report() RETURNS jsonb
AS 'MODULE_PATHNAME', 'usage_report_wrapper'
LANGUAGE c VOLATILE PARALLEL UNSAFE SECURITY DEFINER SET search_path = pg_catalog, pg_temp;
REVOKE ALL ON FUNCTION hydradb.usage_report() FROM PUBLIC;
