\set ON_ERROR_STOP on
CREATE EXTENSION pg_hydradb;
CREATE SCHEMA hydradb_test;
CREATE ROLE hydradb_test_reader;
GRANT USAGE ON SCHEMA hydradb_test TO hydradb_test_reader;

CREATE TABLE hydradb_test.vertices (id bigint PRIMARY KEY, sort_key text NOT NULL);
CREATE TABLE hydradb_test.edges (
    id bigint PRIMARY KEY,
    source bigint NOT NULL REFERENCES hydradb_test.vertices,
    target bigint NOT NULL REFERENCES hydradb_test.vertices,
    kind text NOT NULL
);
INSERT INTO hydradb_test.vertices
SELECT n, lpad(n::text, 4, '0') FROM generate_series(1, 12) n;
INSERT INTO hydradb_test.vertices VALUES (50, '0050'), (60, '0060');
INSERT INTO hydradb_test.edges
SELECT n, n, n + 1, 'link' FROM generate_series(1, 11) n;
INSERT INTO hydradb_test.edges VALUES (100, 1, 2, 'link'), (101, 2, 2, 'link'), (102, 1, 50, 'other');
SELECT hydradb.create_graph('chain',
    '[{"label": "vertex", "table": "hydradb_test.vertices", "key": "id", "sort_key": "sort_key"}]',
    '[{"label": "edge", "table": "hydradb_test.edges", "key": "id", "source": "source", "source_label": "vertex",
       "target": "target", "target_label": "vertex", "kind": "kind"}]');
GRANT SELECT ON hydradb_test.vertices, hydradb_test.edges TO hydradb_test_reader;

CREATE TABLE hydradb_test.mutable_vertices (id bigint PRIMARY KEY, sort_key text NOT NULL);
CREATE TABLE hydradb_test.mutable_edges (id bigint PRIMARY KEY, source bigint NOT NULL REFERENCES hydradb_test.mutable_vertices,
    target bigint NOT NULL REFERENCES hydradb_test.mutable_vertices, kind text NOT NULL);
INSERT INTO hydradb_test.mutable_vertices VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');
INSERT INTO hydradb_test.mutable_edges VALUES (1, 1, 2, 'link');
SELECT hydradb.create_graph('mutable',
    '[{"label": "vertex", "table": "hydradb_test.mutable_vertices", "key": "id", "sort_key": "sort_key"}]',
    '[{"label": "edge", "table": "hydradb_test.mutable_edges", "key": "id", "source": "source", "source_label": "vertex",
       "target": "target", "target_label": "vertex", "kind": "kind"}]');

CREATE TABLE hydradb_test.mvcc_vertices (id bigint PRIMARY KEY, sort_key text NOT NULL);
CREATE TABLE hydradb_test.mvcc_edges (id bigint PRIMARY KEY, source bigint NOT NULL REFERENCES hydradb_test.mvcc_vertices,
    target bigint NOT NULL REFERENCES hydradb_test.mvcc_vertices, kind text NOT NULL);
INSERT INTO hydradb_test.mvcc_vertices VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');
INSERT INTO hydradb_test.mvcc_edges VALUES (1, 1, 2, 'link');
SELECT hydradb.create_graph('mvcc',
    '[{"label": "vertex", "table": "hydradb_test.mvcc_vertices", "key": "id", "sort_key": "sort_key"}]',
    '[{"label": "edge", "table": "hydradb_test.mvcc_edges", "key": "id", "source": "source", "source_label": "vertex",
       "target": "target", "target_label": "vertex", "kind": "kind"}]');

CREATE TABLE hydradb_test.private_vertices (id bigint PRIMARY KEY, sort_key text NOT NULL, visible boolean NOT NULL);
CREATE TABLE hydradb_test.private_edges (id bigint PRIMARY KEY, source bigint NOT NULL REFERENCES hydradb_test.private_vertices,
    target bigint NOT NULL REFERENCES hydradb_test.private_vertices, kind text NOT NULL);
INSERT INTO hydradb_test.private_vertices VALUES (1, 'a', true), (2, 'b', false), (3, 'c', true);
INSERT INTO hydradb_test.private_edges VALUES (1, 1, 2, 'link'), (2, 2, 3, 'link');
ALTER TABLE hydradb_test.private_vertices ENABLE ROW LEVEL SECURITY;
ALTER TABLE hydradb_test.private_edges ENABLE ROW LEVEL SECURITY;
CREATE POLICY visible_vertices ON hydradb_test.private_vertices USING (visible);
CREATE POLICY visible_edges ON hydradb_test.private_edges USING (
    EXISTS (SELECT 1 FROM hydradb_test.private_vertices v WHERE v.id = source)
    AND EXISTS (SELECT 1 FROM hydradb_test.private_vertices v WHERE v.id = target)
);
GRANT SELECT ON hydradb_test.private_vertices, hydradb_test.private_edges TO hydradb_test_reader;
SELECT hydradb.create_graph('private',
    '[{"label": "vertex", "table": "hydradb_test.private_vertices", "key": "id", "sort_key": "sort_key"}]',
    '[{"label": "edge", "table": "hydradb_test.private_edges", "key": "id", "source": "source", "source_label": "vertex",
       "target": "target", "target_label": "vertex", "kind": "kind"}]');

