use hydradb_cypher_ast::{
    BinaryOperator, Clause, CypherDialect, CypherVersion, Direction, ElementTest, Expression, FloatSize, IntegerSize, LabelExpression,
    Literal, NullOrdering, OrderDirection, PathLength, PathMode, PathPrefix, PathSearch, PathSearchKind, PatternElement, ProjectionItems,
    QueryFailureReason, SetItem, SetQuantifier, Statement, TemporalLiteralKind, UnaryOperator, ValueKeyword, ValueTypeKind,
    WriteClassification,
};
use hydradb_cypher_parser_antlr::{
    generated::cypher25_parser::{StatementsContext, ValidatedTreeContext},
    parse_cypher25, parse_cypher25_syntax,
};

#[test]
fn parses_graph_pattern_projection_and_window() {
    let parsed = parse_cypher25_syntax(
        "MATCH (u:User)-[:FOLLOWS]->(v:User) \
         WHERE u.age > 18 \
         RETURN v.name AS name ORDER BY name LIMIT 10",
    )
    .expect("valid Cypher 25 query");

    let root = parsed
        .tree()
        .downcast_ref::<StatementsContext<'_, ValidatedTreeContext>>()
        .expect("the parser entry rule is statements");
    assert_eq!(root.statement_children().count(), 1);
}

#[test]
fn parses_cypher25_let_filter_and_group_by() {
    parse_cypher25_syntax(
        "MATCH (p:Product) \
         LET taxed = p.price * 1.18 \
         FILTER taxed >= 100 \
         RETURN p.category AS category, count(*) AS products \
         GROUP BY p.category ORDER BY products DESC",
    )
    .expect("Cypher 25 LET, FILTER, and GROUP BY should parse");
}

#[test]
fn parses_cypher25_insert_and_repeatable_elements() {
    parse_cypher25_syntax(
        "INSERT (:Person {name: 'Alice'})-[:KNOWS]->(:Person {name: 'Bob'}); \
         MATCH REPEATABLE ELEMENTS (a)-[:KNOWS]->(b) RETURN b",
    )
    .expect("Cypher 25 write and match-mode syntax should parse");
}

#[test]
fn lowers_match_modes_into_the_shared_ast() {
    use hydradb_cypher_ast::{Clause, MatchMode, Statement};

    for (source, expected) in [
        (
            "MATCH REPEATABLE ELEMENTS (a)-[:KNOWS]->(b) RETURN b",
            MatchMode::RepeatableElements,
        ),
        (
            "MATCH DIFFERENT RELATIONSHIPS (a)-[:KNOWS]->(b) RETURN b",
            MatchMode::DifferentRelationships,
        ),
    ] {
        let document = parse_cypher25(source).expect("match mode should lower");
        let [Statement::Query(query)] = document.statements.as_slice() else {
            panic!("expected one typed query");
        };
        let Some(Clause::Match(matched)) = query.clauses.first() else {
            panic!("expected a MATCH clause");
        };
        assert_eq!(matched.mode, Some(expected));
    }
}

#[test]
fn parses_unions_parameters_and_collection_expressions() {
    parse_cypher25_syntax(
        "UNWIND $rows AS row \
         RETURN row.id AS id, [x IN row.values WHERE x > 0 | x * 2] AS values \
         UNION ALL \
         MATCH (n) RETURN n.id AS id, [] AS values",
    )
    .expect("representative expressions and UNION ALL should parse");
}

#[test]
fn rejects_a_syntax_error() {
    assert!(parse_cypher25_syntax("MATCH (n RETURN n").is_err());
}

#[test]
fn lowers_match_where_return_into_the_shared_ast() {
    let document = parse_cypher25(
        "MATCH (u:User)-[:FOLLOWS]->(v:User) \
         WHERE u.age > 18 \
         RETURN v.name AS name ORDER BY name LIMIT 10",
    )
    .expect("valid Cypher 25 query");

    assert_eq!(document.dialect, CypherDialect::Neo4j(CypherVersion::V25));
    let [Statement::Query(query)] = document.statements.as_slice() else {
        panic!("the representative query has typed lowering");
    };
    assert_eq!(query.clauses.len(), 2);

    let Clause::Match(matched) = &query.clauses[0] else {
        panic!("MATCH is the first typed clause");
    };
    let pattern = matched.patterns.first().expect("one pattern");
    let [PatternElement::Node(left), PatternElement::Relationship(relationship), PatternElement::Node(right)] = pattern.elements.as_slice()
    else {
        panic!("node, relationship, node pattern");
    };
    assert_eq!(left.variable.as_ref().map(|value| value.as_str()), Some("u"));
    assert_eq!(left.labels[0].as_str(), "User");
    assert_eq!(relationship.types[0].as_str(), "FOLLOWS");
    assert_eq!(relationship.direction, Direction::Outgoing);
    assert_eq!(right.variable.as_ref().map(|value| value.as_str()), Some("v"));
    assert_eq!(right.labels[0].as_str(), "User");
    assert_eq!(
        matched.predicate,
        Some(Expression::Binary {
            left: Box::new(Expression::Property {
                target: Box::new(Expression::Identifier("u".into())),
                key: "age".into(),
            }),
            operator: BinaryOperator::GreaterThan,
            right: Box::new(Expression::Literal(Literal::Integer(18))),
        })
    );

    let Clause::Return(returned) = &query.clauses[1] else {
        panic!("RETURN is the second typed clause");
    };
    let ProjectionItems::Items(items) = &returned.items else {
        panic!("one explicit projection");
    };
    assert_eq!(items[0].alias.as_ref().map(|value| value.as_str()), Some("name"));
    assert_eq!(returned.order_by[0].expression, Expression::Identifier("name".into()));
    assert_eq!(returned.limit, Some(Expression::Literal(Literal::Integer(10))));
}

#[test]
fn adapter_normalizes_escaped_identifiers() {
    let document = parse_cypher25(
        "MATCH (`user name`:User)-[:FOLLOWS]->(v:User) \
         WHERE `user name`.age > 18 \
         RETURN v.name AS `result name` ORDER BY `result name` LIMIT 10",
    )
    .expect("valid Cypher 25 query");

    let [Statement::Query(query)] = document.statements.as_slice() else {
        panic!("the representative query has typed lowering");
    };
    let Clause::Match(matched) = &query.clauses[0] else {
        panic!("MATCH is the first typed clause");
    };
    let PatternElement::Node(node) = &matched.patterns[0].elements[0] else {
        panic!("the pattern begins with a node");
    };
    assert_eq!(node.variable.as_ref().map(|value| value.as_str()), Some("user name"));
    let Clause::Return(returned) = &query.clauses[1] else {
        panic!("RETURN is the second typed clause");
    };
    let ProjectionItems::Items(items) = &returned.items else {
        panic!("one explicit projection");
    };
    assert_eq!(items[0].alias.as_ref().map(|value| value.as_str()), Some("result name"));
}

#[test]
fn retains_valid_unimplemented_syntax_as_an_owned_fragment() {
    let document = parse_cypher25("MATCH (p:Product) CALL db.labels() YIELD label RETURN label")
        .expect("the generated parser accepts a named procedure call");

    let [Statement::Syntax(fragment)] = document.statements.as_slice() else {
        panic!("unimplemented syntax must not leak generated parser contexts");
    };
    assert!(fragment.text().starts_with("MATCH"));
}

#[test]
fn lowers_single_node_parameter_predicate() {
    let document = parse_cypher25("MATCH (e:Entity) WHERE e.entity_id = $id RETURN e").expect("valid parameterized Cypher 25 query");

    let [Statement::Query(query)] = document.statements.as_slice() else {
        panic!("the parameterized query has typed lowering");
    };
    let Clause::Match(matched) = &query.clauses[0] else {
        panic!("MATCH is typed");
    };
    let [PatternElement::Node(node)] = matched.patterns[0].elements.as_slice() else {
        panic!("the path contains one node");
    };
    assert_eq!(node.variable.as_ref().map(|value| value.as_str()), Some("e"));
    assert_eq!(node.labels[0].as_str(), "Entity");
    assert_eq!(
        matched.predicate,
        Some(Expression::Binary {
            left: Box::new(Expression::Property {
                target: Box::new(Expression::Identifier("e".into())),
                key: "entity_id".into(),
            }),
            operator: BinaryOperator::Equal,
            right: Box::new(Expression::Parameter("id".into())),
        })
    );
}

#[test]
fn lowers_same_property_or_predicate() {
    let document = parse_cypher25("MATCH (e:Entity) WHERE e.entity_id = $a OR e.entity_id = $b OR e.entity_id = $c RETURN e")
        .expect("valid multi-value predicate");

    let [Statement::Query(query)] = document.statements.as_slice() else {
        panic!("the OR query has typed lowering");
    };
    let Clause::Match(matched) = &query.clauses[0] else {
        panic!("MATCH is typed");
    };
    let predicate = matched.predicate.as_ref().expect("WHERE predicate");
    assert!(matches!(
        predicate,
        Expression::Binary {
            operator: BinaryOperator::Or,
            ..
        }
    ));
}

#[test]
fn lowers_incoming_relationship_direction_into_the_shared_ast() {
    let document = parse_cypher25("MATCH (a:Entity)<-[r:KNOWS]-(b:Person) WHERE a.entity_id = $id RETURN r")
        .expect("valid incoming relationship query");

    let [Statement::Query(query)] = document.statements.as_slice() else {
        panic!("the relationship query has typed lowering");
    };
    let Clause::Match(matched) = &query.clauses[0] else {
        panic!("MATCH is typed");
    };
    let [PatternElement::Node(_), PatternElement::Relationship(relationship), PatternElement::Node(_)] =
        matched.patterns[0].elements.as_slice()
    else {
        panic!("node, relationship, node path");
    };
    assert_eq!(relationship.direction, Direction::Incoming);
    assert_eq!(relationship.variable.as_ref().map(|value| value.as_str()), Some("r"));
    assert_eq!(relationship.types[0].as_str(), "KNOWS");
}

#[test]
fn lowers_optional_where_multiple_projections_and_result_window() {
    let document = parse_cypher25(
        "MATCH (e:Entity) \
         RETURN e.entity_id AS id, e.rank AS rank \
         ORDER BY rank DESC, id ASC SKIP $skip LIMIT $limit",
    )
    .expect("valid projected and windowed query");

    let [Statement::Query(query)] = document.statements.as_slice() else {
        panic!("the windowed query has typed lowering");
    };
    let Clause::Match(matched) = &query.clauses[0] else {
        panic!("MATCH is typed");
    };
    assert_eq!(matched.predicate, None);

    let Clause::Return(returned) = &query.clauses[1] else {
        panic!("RETURN is typed");
    };
    let ProjectionItems::Items(items) = &returned.items else {
        panic!("explicit projections");
    };
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].alias.as_ref().map(|value| value.as_str()), Some("id"));
    assert_eq!(items[1].alias.as_ref().map(|value| value.as_str()), Some("rank"));
    assert_eq!(returned.order_by.len(), 2);
    assert_eq!(returned.order_by[0].direction, Some(hydradb_cypher_ast::OrderDirection::Descending));
    assert_eq!(returned.order_by[1].direction, Some(hydradb_cypher_ast::OrderDirection::Ascending));
    assert_eq!(returned.skip, Some(Expression::Parameter("skip".into())));
    assert_eq!(returned.limit, Some(Expression::Parameter("limit".into())));
}

#[test]
fn retains_signed_window_constants_in_the_shared_ast() {
    let document = parse_cypher25("MATCH (e:Entity) RETURN e SKIP -1 LIMIT +2").expect("signed window constants are syntactically valid");

    let [Statement::Query(query)] = document.statements.as_slice() else {
        panic!("signed window query has typed lowering");
    };
    let Clause::Return(returned) = &query.clauses[1] else {
        panic!("RETURN is typed");
    };
    assert!(matches!(
        returned.skip,
        Some(Expression::Unary {
            operator: hydradb_cypher_ast::UnaryOperator::Minus,
            ..
        })
    ));
    assert!(matches!(
        returned.limit,
        Some(Expression::Unary {
            operator: hydradb_cypher_ast::UnaryOperator::Plus,
            ..
        })
    ));
}

#[test]
fn lowers_multi_hop_multiple_match_comma_join_and_optional_match() {
    let document = parse_cypher25(
        "MATCH (a:Entity {id: $id})-[:R1]->(b)-[:R2]->(c) \
         MATCH (c)-[:R3]->(d), (e)-[:R4]->(d) \
         OPTIONAL MATCH (d)-[:R5]->(f) RETURN a.id, f.id",
    )
    .expect("valid composed MATCH query");

    let [Statement::Query(query)] = document.statements.as_slice() else {
        panic!("composed query has typed lowering");
    };
    assert_eq!(query.clauses.len(), 4);
    let Clause::Match(first) = &query.clauses[0] else {
        panic!("first MATCH is typed");
    };
    let [PatternElement::Node(a), PatternElement::Relationship(r1), PatternElement::Node(b), PatternElement::Relationship(r2), PatternElement::Node(c)] =
        first.patterns[0].elements.as_slice()
    else {
        panic!("first MATCH retains both directed hops");
    };
    assert_eq!(a.variable.as_ref().map(|value| value.as_str()), Some("a"));
    assert_eq!(a.labels[0].as_str(), "Entity");
    assert!(a.properties.is_some());
    assert_eq!(r1.types[0].as_str(), "R1");
    assert_eq!(r1.direction, Direction::Outgoing);
    assert_eq!(b.variable.as_ref().map(|value| value.as_str()), Some("b"));
    assert_eq!(r2.types[0].as_str(), "R2");
    assert_eq!(r2.direction, Direction::Outgoing);
    assert_eq!(c.variable.as_ref().map(|value| value.as_str()), Some("c"));

    let Clause::Match(second) = &query.clauses[1] else {
        panic!("second MATCH is typed");
    };
    assert_eq!(second.patterns.len(), 2);
    let [PatternElement::Node(c), PatternElement::Relationship(r3), PatternElement::Node(d)] = second.patterns[0].elements.as_slice()
    else {
        panic!("first comma-joined pattern is retained");
    };
    assert_eq!(c.variable.as_ref().map(|value| value.as_str()), Some("c"));
    assert_eq!(r3.types[0].as_str(), "R3");
    assert_eq!(r3.direction, Direction::Outgoing);
    assert_eq!(d.variable.as_ref().map(|value| value.as_str()), Some("d"));
    let [PatternElement::Node(e), PatternElement::Relationship(r4), PatternElement::Node(joined_d)] =
        second.patterns[1].elements.as_slice()
    else {
        panic!("second comma-joined pattern is retained");
    };
    assert_eq!(e.variable.as_ref().map(|value| value.as_str()), Some("e"));
    assert_eq!(r4.types[0].as_str(), "R4");
    assert_eq!(r4.direction, Direction::Outgoing);
    assert_eq!(joined_d.variable.as_ref().map(|value| value.as_str()), Some("d"));

    let Clause::Match(optional) = &query.clauses[2] else {
        panic!("OPTIONAL MATCH is typed");
    };
    assert!(optional.optional);
    let [PatternElement::Node(d), PatternElement::Relationship(r5), PatternElement::Node(f)] = optional.patterns[0].elements.as_slice()
    else {
        panic!("OPTIONAL MATCH pattern is retained");
    };
    assert_eq!(d.variable.as_ref().map(|value| value.as_str()), Some("d"));
    assert_eq!(r5.types[0].as_str(), "R5");
    assert_eq!(r5.direction, Direction::Outgoing);
    assert_eq!(f.variable.as_ref().map(|value| value.as_str()), Some("f"));

    let Clause::Return(returned) = &query.clauses[3] else {
        panic!("RETURN is typed");
    };
    let ProjectionItems::Items(items) = &returned.items else {
        panic!("RETURN retains explicit projections");
    };
    assert_eq!(
        items.iter().map(|item| &item.expression).collect::<Vec<_>>(),
        vec![
            &Expression::Property {
                target: Box::new(Expression::Identifier("a".into())),
                key: "id".into(),
            },
            &Expression::Property {
                target: Box::new(Expression::Identifier("f".into())),
                key: "id".into(),
            },
        ]
    );
}

/// `IS NULL` used to end typed lowering, so the whole statement fell back to a
/// syntax fragment and the planner rejected it as untyped.
#[test]
fn lowers_postfix_null_predicates_tighter_than_comparison() {
    let document = parse_cypher25(
        "MATCH (a)-[r:RELATES]->(b) \
         WHERE r.superseded_by IS NULL OR r.superseded_by = '' AND r.is IS NOT NULL \
         RETURN r.is AS is",
    )
    .expect("valid Cypher 25 null predicates");

    let [Statement::Query(query)] = document.statements.as_slice() else {
        panic!("IS NULL has typed lowering");
    };
    let Clause::Match(matched) = &query.clauses[0] else {
        panic!("MATCH is typed");
    };
    let property = |key: &str| {
        Box::new(Expression::Property {
            target: Box::new(Expression::Identifier("r".into())),
            key: key.into(),
        })
    };
    assert_eq!(
        matched.predicate,
        Some(Expression::Binary {
            left: Box::new(Expression::Unary {
                operator: UnaryOperator::IsNull,
                expression: property("superseded_by"),
            }),
            operator: BinaryOperator::Or,
            right: Box::new(Expression::Binary {
                left: Box::new(Expression::Binary {
                    left: property("superseded_by"),
                    operator: BinaryOperator::Equal,
                    right: Box::new(Expression::Literal(Literal::String("".into()))),
                }),
                operator: BinaryOperator::And,
                right: Box::new(Expression::Unary {
                    operator: UnaryOperator::IsNotNull,
                    expression: property("is"),
                }),
            }),
        })
    );
}

#[test]
fn an_incomplete_null_predicate_is_not_typed() {
    let document = parse_cypher25("MATCH (n) WHERE n.x IS NOT n.y RETURN n");
    if let Ok(document) = document {
        assert!(matches!(document.statements.as_slice(), [Statement::Syntax(_)]));
    }
}

#[test]
fn lowers_create_merge_set_remove_delete_and_detach_delete() {
    let create = parse_cypher25("CREATE (a:Person {name: 'A'})-[:KNOWS]->(b:Person)").expect("valid CREATE");
    let [Statement::Query(create)] = create.statements.as_slice() else {
        panic!("CREATE should lower to a query");
    };
    let [Clause::Create(create_clause)] = create.clauses.as_slice() else {
        panic!("CREATE has a typed clause");
    };
    assert_eq!(create_clause.patterns.len(), 1);
    assert_eq!(create.write_classification(), WriteClassification::Writes);

    let merge = parse_cypher25("MERGE (n:Person {id: $id}) ON CREATE SET n.created = true ON MATCH SET n.seen = true RETURN n")
        .expect("valid MERGE");
    let [Statement::Query(merge)] = merge.statements.as_slice() else {
        panic!("MERGE should lower to a query");
    };
    let [Clause::Merge(merge_clause), Clause::Return(_)] = merge.clauses.as_slice() else {
        panic!("MERGE actions remain attached to the MERGE clause");
    };
    assert_eq!(merge_clause.on_create.len(), 1);
    assert_eq!(merge_clause.on_match.len(), 1);

    let update_document = parse_cypher25("MATCH (n) SET n.name = 'A', n += {age: 2}, n:Person REMOVE n.old, n:Legacy DELETE n")
        .expect("valid property, map, label, and delete mutations");
    let [Statement::Query(update)] = update_document.statements.as_slice() else {
        panic!("mutation clauses lower to a query: {update_document:?}");
    };
    let [Clause::Match(_), Clause::Set(set), Clause::Remove(remove), Clause::Delete(delete)] = update.clauses.as_slice() else {
        panic!("each mutation stays in source order");
    };
    assert!(matches!(&set.items[0], SetItem::Property { .. }));
    assert!(matches!(&set.items[1], SetItem::Properties { .. }));
    assert_eq!(set.items.len(), 3);
    assert_eq!(remove.items.len(), 2);
    assert!(!delete.detach);
    assert_eq!(update.write_classification(), WriteClassification::Writes);

    let detach = parse_cypher25("MATCH (n) DETACH DELETE n").expect("valid DETACH DELETE");
    let [Statement::Query(detach)] = detach.statements.as_slice() else {
        panic!("DETACH DELETE lowers to a query");
    };
    let Some(Clause::Delete(delete)) = detach.clauses.last() else {
        panic!("DETACH DELETE has a typed delete clause");
    };
    assert!(delete.detach);
}

#[test]
fn lowers_foreach_bodies_as_ordered_typed_mutations() {
    let document =
        parse_cypher25("MATCH (n) FOREACH (item IN n.items | CREATE (:Item {value: item}) MERGE (:Item {value: item}) SET n.count = item)")
            .expect("valid FOREACH mutation");
    let [Statement::Query(query)] = document.statements.as_slice() else {
        panic!("FOREACH lowers to a query");
    };
    let [Clause::Match(_), Clause::Foreach(foreach)] = query.clauses.as_slice() else {
        panic!("FOREACH remains in clause order");
    };
    assert_eq!(foreach.variable.as_str(), "item");
    assert_eq!(foreach.clauses.len(), 3);
    assert!(matches!(&foreach.clauses[0], hydradb_cypher_ast::MutationClause::Create(_)));
    let hydradb_cypher_ast::MutationClause::Merge(merge) = &foreach.clauses[1] else {
        panic!("FOREACH MERGE lowers to its typed mutation variant");
    };
    let _: Box<hydradb_cypher_ast::MergeClause> = merge.clone();
    assert_eq!(merge.pattern.elements.len(), 1);
    assert!(matches!(&foreach.clauses[2], hydradb_cypher_ast::MutationClause::Set(_)));
    assert!(query.effect_classification().writes);
}

/// A statement typed lowering cannot finish still names the clause it stopped
/// in, so a failure counter can put the experimental engine's gaps in the
/// same buckets as the legacy engine's.
#[test]
fn an_untyped_statement_names_the_clause_lowering_stopped_in() {
    for (query, expected) in [
        ("UNWIND [x IN [1] | x] AS row RETURN row", QueryFailureReason::Unwind),
        (
            "MATCH (n) RETURN n.id AS id ORDER BY n.name =~ 'x' DESC",
            QueryFailureReason::OrderWindow,
        ),
        ("MATCH (n) RETURN [x IN [1] | x] AS row", QueryFailureReason::Return),
        ("MATCH (n) CALL db.labels() YIELD label RETURN label", QueryFailureReason::Procedure),
    ] {
        let Ok(document) = parse_cypher25(query) else {
            panic!("{query} is valid Cypher 25");
        };
        let [Statement::Syntax(fragment)] = document.statements.as_slice() else {
            panic!("{query} has no typed lowering yet, so it cannot test attribution");
        };
        assert_eq!(fragment.unsupported_reason(), Some(expected), "{query}");
    }
}

/// A trailing `WITH ... LIMIT` keeps its own window.
///
/// It used to have no AST node at all: the parser folded its window into the
/// RETURN window and dropped the clause, which was sound only while RETURN was
/// a plain per-row projection. Every shape below was refused for that reason.
/// Now the clause is typed and carries its own window, so the parser types all
/// of them and lowering decides what they mean.
#[test]
fn a_trailing_windowed_with_keeps_its_own_window() {
    let document = parse_cypher25(
        "MATCH (s:Source {tenant_id: $t})-[:HAS_CHUNK]-(c:Chunk) \
         WITH c LIMIT $fetch_limit RETURN c.chunk_id AS chunk_id",
    )
    .expect("valid Cypher 25");
    let [Statement::Query(query)] = document.statements.as_slice() else {
        panic!("a trailing WITH ... LIMIT lowers to a typed query");
    };
    let [Clause::Match(_), Clause::With(with), Clause::Return(returned)] = query.clauses.as_slice() else {
        panic!("expected MATCH, WITH then RETURN, got {:?}", query.clauses);
    };
    assert_eq!(with.limit, Some(Expression::Parameter("fetch_limit".into())));
    assert_eq!(with.skip, None);
    // The window stayed where it was written rather than moving to RETURN.
    assert_eq!(returned.limit, None);
    assert_eq!(returned.skip, None);

    for query in [
        "MATCH (s:Source)-[:HAS_CHUNK]-(c:Chunk) WITH c LIMIT 2 RETURN s.source_id AS id",
        "MATCH (s:Source)-[:HAS_CHUNK]-(c:Chunk) WITH c LIMIT 2 RETURN c.id AS id ORDER BY id",
        "MATCH (s:Source)-[:HAS_CHUNK]-(c:Chunk) WITH c LIMIT 2 RETURN DISTINCT c.id AS id",
        "MATCH (s:Source)-[:HAS_CHUNK]-(c:Chunk) WITH c LIMIT 2 RETURN count(*) AS n",
        "MATCH (s:Source)-[:HAS_CHUNK]-(c:Chunk) WITH c LIMIT 2 RETURN c.id AS id LIMIT 1",
        // Typed relationship rather than a bare `--`: an untyped undirected
        // hop does not lower in this adapter at all, which has nothing to do
        // with WITH and would hide what this case is testing.
        "MATCH (s:Source)-[:HAS_CHUNK]-(c:Chunk) WITH s, c LIMIT 2 MATCH (c)-[:HAS_CHUNK]-(s) RETURN c.id AS id",
    ] {
        let document = parse_cypher25(query).expect("valid Cypher 25");
        assert!(
            matches!(document.statements.as_slice(), [Statement::Query(_)]),
            "{query} types now that WITH is a clause"
        );
    }
}

#[test]
fn with_is_a_typed_clause_rather_than_something_the_parser_discards() {
    let document = parse_cypher25(
        "MATCH (u:User) \
         WITH DISTINCT u.name AS name, u.age AS age \
         ORDER BY name DESC SKIP 1 LIMIT 5 \
         WHERE age > 18 \
         RETURN name",
    )
    .expect("valid Cypher 25 query");

    let [Statement::Query(query)] = document.statements.as_slice() else {
        panic!("one typed query statement");
    };
    assert_eq!(query.clauses.len(), 3, "MATCH, WITH and RETURN");

    let Clause::With(with) = &query.clauses[1] else {
        panic!("WITH is the second typed clause");
    };
    assert_eq!(with.quantifier, Some(SetQuantifier::Distinct));
    let ProjectionItems::Items(items) = &with.items else {
        panic!("WITH projects an item list");
    };
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].alias.as_ref().map(ToString::to_string).as_deref(), Some("name"));
    assert_eq!(with.order_by.len(), 1);
    assert_eq!(with.order_by[0].direction, Some(OrderDirection::Descending));
    assert!(with.skip.is_some() && with.limit.is_some());
    assert!(with.predicate.is_some(), "the WHERE after WITH belongs to the WITH clause");
}

#[test]
fn a_pass_through_with_keeps_its_bindings_in_the_tree() {
    let document = parse_cypher25("MATCH (a:Entity)-[r:KNOWS]->(b:Person) WITH a, r, b RETURN a.id AS id").expect("valid Cypher 25 query");
    let [Statement::Query(query)] = document.statements.as_slice() else {
        panic!("one typed query statement");
    };
    let Clause::With(with) = &query.clauses[1] else {
        panic!("WITH is the second typed clause");
    };
    let ProjectionItems::Items(items) = &with.items else {
        panic!("WITH projects an item list");
    };
    let projected: Vec<String> = items
        .iter()
        .map(|item| match &item.expression {
            Expression::Identifier(name) => name.to_string(),
            other => panic!("a pass-through WITH projects bindings, got {other:?}"),
        })
        .collect();
    assert_eq!(projected, ["a", "r", "b"]);
    assert!(with.predicate.is_none() && with.quantifier.is_none());
}

#[test]
fn lowers_node_and_relationship_local_where_into_the_shared_ast() {
    let document = parse_cypher25("MATCH (h:Hub {name: 'hub'} WHERE h.open)-[r:HAS WHERE r.weight > 1]->(i WHERE i.score >= 0) RETURN i")
        .expect("valid Cypher 25 pattern predicates");
    let [Statement::Query(query)] = document.statements.as_slice() else {
        panic!("pattern-local WHERE is typed");
    };
    let Clause::Match(matched) = &query.clauses[0] else {
        panic!("first clause is MATCH");
    };
    assert_eq!(matched.predicate, None, "element predicates stay on their elements");
    let [PatternElement::Node(hub), PatternElement::Relationship(has), PatternElement::Node(item)] =
        matched.patterns[0].elements.as_slice()
    else {
        panic!("one hop");
    };
    assert!(hub.properties.is_some(), "an inline map and a WHERE may both appear");
    assert!(matches!(hub.predicate, Some(Expression::Property { .. })));
    assert!(matches!(
        has.predicate,
        Some(Expression::Binary {
            operator: BinaryOperator::GreaterThan,
            ..
        })
    ));
    assert!(matches!(
        item.predicate,
        Some(Expression::Binary {
            operator: BinaryOperator::GreaterThanOrEqual,
            ..
        })
    ));
}

#[test]
fn lowers_unwind_into_the_shared_ast() {
    let document = parse_cypher25("UNWIND $rows AS row MATCH (n {id: row}) UNWIND [1, 2] AS copy RETURN n, copy").expect("valid UNWIND");
    let [Statement::Query(query)] = document.statements.as_slice() else {
        panic!("UNWIND is typed");
    };
    let [Clause::Unwind(first), Clause::Match(_), Clause::Unwind(second), Clause::Return(_)] = query.clauses.as_slice() else {
        panic!("clauses in written order: {:?}", query.clauses);
    };
    assert_eq!(first.variable.as_str(), "row");
    assert!(matches!(first.expression, Expression::Parameter(_)));
    assert_eq!(second.variable.as_str(), "copy");
    assert!(matches!(&second.expression, Expression::List(items) if items.len() == 2));
}

#[test]
fn lowers_group_by_on_return_and_with() {
    let document =
        parse_cypher25("MATCH (i) WITH i.kind AS kind, count(*) AS n GROUP BY kind RETURN kind, n GROUP BY kind, n ORDER BY kind")
            .expect("valid GROUP BY");
    let [Statement::Query(query)] = document.statements.as_slice() else {
        panic!("GROUP BY is typed");
    };
    let [Clause::Match(_), Clause::With(with), Clause::Return(returned)] = query.clauses.as_slice() else {
        panic!("three clauses");
    };
    assert_eq!(with.group_by.len(), 1);
    assert_eq!(returned.group_by.len(), 2);
    assert_eq!(returned.order_by.len(), 1, "ORDER BY follows GROUP BY");
}

#[test]
fn lowers_let_and_filter_into_the_shared_ast() {
    let document =
        parse_cypher25("MATCH (p:Product) LET taxed = p.price * 2, label = 'x' FILTER WHERE taxed > 10 FILTER label = 'x' RETURN taxed")
            .expect("valid LET and FILTER");
    let [Statement::Query(query)] = document.statements.as_slice() else {
        panic!("LET and FILTER are typed");
    };
    let [Clause::Match(_), Clause::Let(binding), Clause::Filter(_), Clause::Filter(_), Clause::Return(_)] = query.clauses.as_slice() else {
        panic!("clauses in written order: {:?}", query.clauses);
    };
    assert_eq!(binding.bindings.len(), 2);
    assert_eq!(binding.bindings[0].0.as_str(), "taxed");
}

#[test]
fn lowers_simple_and_searched_case_into_the_shared_ast() {
    let document = parse_cypher25(
        "MATCH (i) RETURN CASE WHEN i.score >= 3 THEN 'high' ELSE 'low' END AS band, \
         CASE i.kind WHEN 'fruit' THEN 1 WHEN 'veg' THEN 2 END AS code",
    )
    .expect("valid CASE");
    let [Statement::Query(query)] = document.statements.as_slice() else {
        panic!("CASE is typed");
    };
    let Some(Clause::Return(returned)) = query.clauses.last() else {
        panic!("RETURN last");
    };
    let ProjectionItems::Items(items) = &returned.items else {
        panic!("items");
    };
    assert!(matches!(&items[0].expression, Expression::Case { operand: None, branches, default: Some(_) } if branches.len() == 1));
    assert!(matches!(&items[1].expression, Expression::Case { operand: Some(_), branches, default: None } if branches.len() == 2));
}

/// The single `RETURN` projection of a typed query.
fn returned_items(query: &str) -> Vec<Expression> {
    let document = parse_cypher25(query).unwrap_or_else(|error| panic!("{query} is valid Cypher 25: {error:?}"));
    let [Statement::Query(query)] = document.statements.as_slice() else {
        panic!("{query:?} has typed lowering");
    };
    let Some(Clause::Return(returned)) = query.clauses.last() else {
        panic!("RETURN last");
    };
    let ProjectionItems::Items(items) = &returned.items else {
        panic!("explicit items");
    };
    items.iter().map(|item| item.expression.clone()).collect()
}

/// Lowering reads the grammar's tree, so whatever the lexer hides --
/// comments, a trailing statement separator -- cannot stop it.
#[test]
fn comments_and_a_trailing_semicolon_do_not_stop_lowering() {
    let items = returned_items("MATCH (n) /* block */ // line\n RETURN n.name /* trailing */;");
    assert_eq!(
        items,
        [Expression::Property {
            target: Box::new(Expression::Identifier("n".into())),
            key: "name".into(),
        }]
    );
}

#[test]
fn string_literals_decode_every_escape() {
    let items = returned_items(r#"RETURN 'a\tb\\c\'d' AS one, "é\n" AS two, 'x\U0001F600' AS three"#);
    assert_eq!(
        items,
        [
            Expression::Literal(Literal::String("a\tb\\c'd".into())),
            Expression::Literal(Literal::String("\u{e9}\n".into())),
            Expression::Literal(Literal::String("x\u{1F600}".into())),
        ]
    );
}

#[test]
fn numeric_literals_follow_the_lexer() {
    let items = returned_items("RETURN 1_000 AS a, 0x1F AS b, 0o17 AS c, 1.50 AS d, 1.5e3 AS e, .5 AS f, -2 AS g");
    assert_eq!(items[0], Expression::Literal(Literal::Integer(1_000)));
    assert_eq!(items[1], Expression::Literal(Literal::Integer(0x1F)));
    assert_eq!(items[2], Expression::Literal(Literal::Integer(0o17)));
    assert_eq!(
        items[3],
        Expression::Literal(Literal::Float("1.50".into())),
        "a plain spelling is kept as written"
    );
    assert_eq!(items[4], Expression::Literal(Literal::Float("1500.0".into())));
    assert_eq!(items[5], Expression::Literal(Literal::Float("0.5".into())));
    assert!(matches!(
        &items[6],
        Expression::Unary { operator: UnaryOperator::Minus, expression } if **expression == Expression::Literal(Literal::Integer(2))
    ));
}

#[test]
fn long_sort_keywords_offset_and_is_labels_lower() {
    let document =
        parse_cypher25("MATCH (n IS Person&Admin)-[r:KNOWS|LIKES]->(m) RETURN m.name AS name ORDER BY name DESCENDING OFFSET 2 LIMIT 3")
            .expect("valid Cypher 25");
    let [Statement::Query(query)] = document.statements.as_slice() else {
        panic!("typed");
    };
    let [Clause::Match(matched), Clause::Return(returned)] = query.clauses.as_slice() else {
        panic!("MATCH then RETURN");
    };
    let [PatternElement::Node(n), PatternElement::Relationship(r), _] = matched.patterns[0].elements.as_slice() else {
        panic!("one hop");
    };
    let labels: Vec<_> = n.labels.iter().map(|label| label.as_str()).collect();
    assert_eq!(labels, ["Person", "Admin"], "a conjunction of node labels");
    let types: Vec<_> = r.types.iter().map(|label| label.as_str()).collect();
    assert_eq!(types, ["KNOWS", "LIKES"], "a disjunction of relationship types");
    assert_eq!(returned.order_by[0].direction, Some(OrderDirection::Descending));
    assert_eq!(returned.skip, Some(Expression::Literal(Literal::Integer(2))));
}

/// `ALL` is the default set quantifier: it must not read as DISTINCT.
#[test]
fn an_explicit_all_quantifier_is_the_default() {
    let document = parse_cypher25("MATCH (n) WITH ALL n RETURN ALL n.x AS x").expect("valid Cypher 25");
    let [Statement::Query(query)] = document.statements.as_slice() else {
        panic!("typed");
    };
    let [_, Clause::With(with), Clause::Return(returned)] = query.clauses.as_slice() else {
        panic!("MATCH, WITH, RETURN");
    };
    assert_eq!(with.quantifier, None);
    assert_eq!(returned.quantifier, None);
}

/// Comparisons chain as `a < b AND b < c`; `IN` binds tighter than `=`.
#[test]
fn comparison_precedence_is_the_grammars() {
    let items = returned_items("RETURN 1 < 2 < 3 AS chained, 1 = 2 IN [false] AS membership");
    assert!(matches!(
        &items[0],
        Expression::Binary {
            operator: BinaryOperator::And,
            ..
        }
    ));
    let Expression::Binary { operator, right, .. } = &items[1] else {
        panic!("a comparison");
    };
    assert_eq!(*operator, BinaryOperator::Equal);
    assert!(matches!(
        **right,
        Expression::Binary {
            operator: BinaryOperator::In,
            ..
        }
    ));
}

/// Cypher's three character-string predicates lower to distinct operators.
#[test]
fn character_string_predicates_are_typed() {
    let items = returned_items(
        "RETURN 'alphabet' STARTS WITH 'alpha' AS starts, \
         'alphabet' ENDS WITH 'bet' AS ends, \
         'alphabet' CONTAINS 'pha' AS contains",
    );
    for (expression, expected) in items
        .iter()
        .zip([BinaryOperator::StartsWith, BinaryOperator::EndsWith, BinaryOperator::Contains])
    {
        assert!(
            matches!(expression, Expression::Binary { operator, .. } if *operator == expected),
            "expected {expected:?}, got {expression:?}"
        );
    }
}

/// Operators with no executor stay refused, in the bucket of the clause they
/// sit in.
#[test]
fn constructs_without_an_executor_stay_untyped() {
    for (query, expected) in [
        ("MATCH (n) WHERE n.a =~ 'x' RETURN n", QueryFailureReason::Where),
        ("RETURN 2 ^ 3 AS power", QueryFailureReason::Return),
        ("MATCH (n), (m), q = shortestPath((n)-->(m)) RETURN n", QueryFailureReason::Pattern),
        ("MATCH (n) RETURN n ORDER BY n.x =~ 'z'", QueryFailureReason::OrderWindow),
    ] {
        let document = parse_cypher25(query).unwrap_or_else(|error| panic!("{query} is valid Cypher 25: {error:?}"));
        let [Statement::Syntax(fragment)] = document.statements.as_slice() else {
            panic!("{query} has no typed lowering yet");
        };
        assert_eq!(fragment.unsupported_reason(), Some(expected), "{query}");
    }
}

/// GQL puts XOR on OR's level (§20.20), where the grammar binds it tighter:
/// `a OR b XOR c` is `(a OR b) XOR c`, and AND still binds tighter than both.
#[test]
fn xor_shares_the_level_of_or() {
    let items = returned_items("RETURN a OR b XOR c AS v, a XOR b AND c AS w");
    let Expression::Binary { left, operator, .. } = &items[0] else {
        panic!("a binary expression");
    };
    assert_eq!(*operator, BinaryOperator::Xor);
    assert!(matches!(
        **left,
        Expression::Binary {
            operator: BinaryOperator::Or,
            ..
        }
    ));
    let Expression::Binary { operator, right, .. } = &items[1] else {
        panic!("a binary expression");
    };
    assert_eq!(*operator, BinaryOperator::Xor);
    assert!(matches!(
        **right,
        Expression::Binary {
            operator: BinaryOperator::And,
            ..
        }
    ));
}

/// The element predicates of §19.8 to §19.10 and §19.13. `IS DIRECTED` is the
/// directed predicate unless backticked, which keeps it a label test.
#[test]
fn element_predicates_lower_to_element_tests() {
    let items = returned_items(
        "RETURN e IS NOT DIRECTED AS a, e IS `DIRECTED` AS b, n IS LABELED Hub AS c, n:Hub AS d, n IS SOURCE OF e AS f, \
         n IS NOT DESTINATION OF e AS g, PROPERTY_EXISTS(n, name) AS h",
    );
    let tests: Vec<_> = items
        .iter()
        .map(|item| match item {
            Expression::ElementTest { element, test, negated } => match element.as_ref() {
                Expression::Identifier(element) => (element.as_str().to_string(), test.clone(), *negated),
                other => panic!("an element variable, not {other:?}"),
            },
            other => panic!("an element test, not {other:?}"),
        })
        .collect();
    let label = |name: &str| ElementTest::Labeled(LabelExpression::Label(name.into()));
    let edge = |name: &str| Box::new(Expression::Identifier(name.into()));
    assert_eq!(
        tests,
        [
            ("e".to_string(), ElementTest::Directed, true),
            ("e".to_string(), label("DIRECTED"), false),
            ("n".to_string(), label("Hub"), false),
            ("n".to_string(), label("Hub"), false),
            ("n".to_string(), ElementTest::SourceOf(edge("e")), false),
            ("n".to_string(), ElementTest::DestinationOf(edge("e")), true),
            ("n".to_string(), ElementTest::PropertyExists("name".into()), false),
        ]
    );
    // An element predicate takes a variable, not an expression.
    assert!(matches!(
        parse_cypher25("RETURN n.x IS LABELED Hub AS v").unwrap().statements.as_slice(),
        [Statement::Syntax(_)]
    ));
}

/// `NULLS FIRST` and `NULLS LAST` (§16.17, GA03), and the words staying names.
#[test]
fn sort_items_carry_a_null_ordering() {
    let document = parse_cypher25("MATCH (n) RETURN n.first AS first ORDER BY first DESC NULLS LAST, n.last NULLS FIRST, n.nulls").unwrap();
    let [Statement::Query(query)] = document.statements.as_slice() else {
        panic!("typed");
    };
    let Some(Clause::Return(returned)) = query.clauses.last() else {
        panic!("RETURN");
    };
    let nulls: Vec<_> = returned.order_by.iter().map(|item| item.nulls).collect();
    assert_eq!(nulls, [Some(NullOrdering::Last), Some(NullOrdering::First), None]);
}

/// Approximate number suffixes (§21.2, GL07 to GL10) make FLOAT literals.
#[test]
fn approximate_number_suffixes_make_floats() {
    assert_eq!(
        returned_items("RETURN 1.5F AS a, 2d AS b, 1e3D AS c"),
        vec![
            Expression::Literal(Literal::Float("1.5".into())),
            Expression::Literal(Literal::Float("2.0".into())),
            Expression::Literal(Literal::Float("1000.0".into())),
        ]
    );
}

/// GQL's query conjunctions (§14.2), NEXT (§9.2), FINISH (§14.10),
/// OPTIONAL blocks (§14.4), FOR positions (§14.8) and VALUE (§20.6) lower to
/// typed queries; the new keywords stay usable as names, and Cypher's `FOR x
/// IN list WITH ...` keeps its parse.
#[test]
fn gql_query_composition_lowers() {
    for query in [
        "MATCH (n) RETURN n.a AS v EXCEPT ALL MATCH (n) RETURN n.a AS v",
        "MATCH (n) RETURN n.a AS v INTERSECT MATCH (n) RETURN n.a AS v",
        "MATCH (n) RETURN n.a AS v OTHERWISE MATCH (n) RETURN n.a AS v",
        "MATCH (n) RETURN n AS m NEXT MATCH (m)-->(o) RETURN o",
        "MATCH (n) FINISH",
        "MATCH (n) OPTIONAL { MATCH (n)-->(m) MATCH (m)-->(o) } RETURN o",
        "MATCH (n) OPTIONAL ( MATCH (n)-->(m) OPTIONAL { MATCH (m)-->(o) } ) RETURN o",
        "FOR x IN [1] WITH ORDINALITY i RETURN x, i",
        "FOR x IN [1] WITH OFFSET i RETURN x, i",
        "RETURN VALUE { MATCH (n) RETURN count(n) AS c } AS v",
        "MATCH (n) RETURN n.except AS except, n.intersect AS intersect, n.otherwise AS otherwise, n.ordinality AS ordinality",
        "FOR x IN [1, 2] WITH x AS y RETURN y",
    ] {
        let document = parse_cypher25(query).unwrap_or_else(|error| panic!("{query} is valid: {error:?}"));
        assert!(
            matches!(document.statements.as_slice(), [Statement::Query(_)]),
            "{query} lowers to a typed query"
        );
    }
}

/// GQL's path modes (§16.6), quantified edges (§16.11) and parenthesized
/// paths (§16.7) lower; a GQL quantifier is marked as one, and `simple`
/// stays a name.
#[test]
fn quantifier_bounds_preserve_values_and_gql_syntax_across_radices() {
    use hydradb_cypher_ast::PathLength;
    for (quantifier, minimum, maximum) in [
        ("{0o5,}", 5, None),
        ("{0x2,0b11}", 2, Some(3)),
        ("{0b1}", 1, Some(1)),
        ("{,0xff}", 0, Some(255)),
        ("{0o0}", 0, Some(0)),
        ("{0b11,0x1}", 3, Some(1)),
        ("{0x100}", 256, Some(256)),
        ("{4294967295}", u32::MAX, Some(u32::MAX)),
        ("{0xffffffff}", u32::MAX, Some(u32::MAX)),
        ("{٠,٠٠١}", 0, Some(1)),
        ("+", 1, None),
        ("*", 0, None),
    ] {
        let source = format!("MATCH (a)-[:R]->{quantifier}(b) RETURN b");
        let document = parse_cypher25(&source).expect("unsigned integer grammar");
        let [Statement::Query(query)] = document.statements.as_slice() else {
            panic!("typed {quantifier}");
        };
        let Clause::Match(matched) = &query.clauses[0] else {
            panic!("MATCH");
        };
        let PatternElement::Relationship(relationship) = &matched.patterns[0].elements[1] else {
            panic!("edge");
        };
        assert_eq!(
            relationship.length,
            Some(PathLength {
                minimum: Some(minimum),
                maximum,
                quantifier: true
            }),
            "{quantifier}"
        );
    }
}

#[test]
fn written_search_presence_survives_nonselective_and_parenthesized_paths() {
    for (path, written) in [
        ("(a)", false),
        ("TRAIL (a)", false),
        ("ALL (a)", true),
        ("ALL TRAIL (a)", true),
        ("ANY (a)", true),
        ("((ALL (a)-[:R]->{1}(b)))", true),
        ("ALL ((a)-[:R]->(b))", true),
        ("(TRAIL (a)-[:R]->(b))", false),
    ] {
        let document = parse_cypher25(&format!("MATCH {path} RETURN a")).unwrap();
        let [Statement::Query(query)] = document.statements.as_slice() else {
            panic!("typed {path}");
        };
        let Clause::Match(matched) = &query.clauses[0] else {
            panic!("MATCH");
        };
        assert_eq!(matched.patterns[0].search_prefix_written, written, "{path}");
    }
}

#[test]
fn path_modes_and_quantified_edges_lower() {
    let document = parse_cypher25("MATCH (ACYCLIC (a)-[e:R]->{1,3}(b) WHERE a.x = 1) RETURN e AS simple").unwrap();
    let [Statement::Query(query)] = document.statements.as_slice() else {
        panic!("typed");
    };
    let Some(Clause::Match(matched)) = query.clauses.first() else {
        panic!("MATCH");
    };
    assert!(
        matched.predicate.is_none(),
        "the parenthesized WHERE is the path's, not the clause's"
    );
    let [path] = matched.patterns.as_slice() else {
        panic!("one path");
    };
    assert!(path.predicate.is_some(), "the parenthesized WHERE holds of its path");
    assert_eq!(path.modes, [PathMode::Acyclic]);
    let Some(PatternElement::Relationship(edge)) = path.elements.get(1) else {
        panic!("an edge");
    };
    assert_eq!(
        edge.length,
        Some(PathLength {
            minimum: Some(1),
            maximum: Some(3),
            quantifier: true,
        })
    );
    for query in [
        "MATCH TRAIL PATH (a)-->+(b) RETURN b",
        "MATCH SIMPLE (a)-[:R]->*(b) RETURN b",
        "MATCH WALK (a)-[:R*1..2]->(b) RETURN b",
    ] {
        assert!(
            matches!(parse_cypher25(query).unwrap().statements.as_slice(), [Statement::Query(_)]),
            "{query} lowers"
        );
    }
}

/// Path search prefixes (§16.6) normalize by Syntax Rule 2, and KEEP (§16.4)
/// carries its prefix; `keep` stays a name.
#[test]
fn path_search_prefixes_and_keep_lower() {
    let search = |query: &str| {
        let document = parse_cypher25(query).unwrap_or_else(|error| panic!("{query}: {error:?}"));
        let [Statement::Query(query)] = document.statements.as_slice() else {
            panic!("typed");
        };
        let Some(Clause::Match(matched)) = query.clauses.first() else {
            panic!("MATCH");
        };
        (matched.patterns[0].search.clone(), matched.keep.clone())
    };
    let shortest = |kind, count| {
        Some(PathSearch {
            kind,
            count: Expression::Literal(Literal::Integer(count)),
        })
    };
    assert_eq!(
        search("MATCH ANY SHORTEST (a)-->+(b) RETURN b").0,
        shortest(PathSearchKind::Shortest, 1)
    );
    assert_eq!(
        search("MATCH ALL SHORTEST (a)-->+(b) RETURN b").0,
        shortest(PathSearchKind::ShortestGroups, 1)
    );
    assert_eq!(
        search("MATCH SHORTEST 3 GROUPS (a)-->+(b) RETURN b").0,
        shortest(PathSearchKind::ShortestGroups, 3)
    );
    assert_eq!(search("MATCH ANY 2 TRAIL (a)-->+(b) RETURN b").0, shortest(PathSearchKind::Any, 2));
    assert_eq!(search("MATCH ALL (a)-->+(b) RETURN b").0, None);
    let (_, keep) = search("MATCH (a)-->+(b) KEEP SHORTEST 2 PATHS RETURN b AS keep");
    assert_eq!(
        keep,
        Some(PathPrefix {
            mode: None,
            search: shortest(PathSearchKind::Shortest, 2),
        })
    );
}

/// A path pattern union and a multiset alternation (§16.7) lower to their
/// operands, each keeping its parenthesized WHERE; the two operators do not
/// mix, and `|` elsewhere keeps its Cypher meaning.
#[test]
fn path_pattern_unions_lower_to_their_operands() {
    let document = parse_cypher25("MATCH p = ((a)-[:R]->(b) WHERE a.x = 1) | (a)<-[:R]-(b) RETURN p").unwrap();
    let [Statement::Query(query)] = document.statements.as_slice() else {
        panic!("typed");
    };
    let Some(Clause::Match(matched)) = query.clauses.first() else {
        panic!("MATCH");
    };
    assert!(matched.predicate.is_none(), "an operand's WHERE stays with the operand");
    let [path] = matched.patterns.as_slice() else {
        panic!("one path");
    };
    assert!(path.binding.is_some() && path.elements.is_empty());
    let alternatives = path.alternatives.as_ref().expect("a union");
    assert!(!alternatives.multiset);
    assert_eq!(alternatives.operands.len(), 2);
    assert!(alternatives.operands[0].predicate.is_some() && alternatives.operands[1].predicate.is_none());
    let multiset = |query: &str| {
        let document = parse_cypher25(query).unwrap_or_else(|error| panic!("{query}: {error:?}"));
        let [Statement::Query(query)] = document.statements.as_slice() else {
            panic!("typed");
        };
        let Some(Clause::Match(matched)) = query.clauses.first() else {
            panic!("MATCH");
        };
        matched.patterns[0]
            .alternatives
            .as_ref()
            .map(|alternatives| (alternatives.multiset, alternatives.operands.len()))
    };
    assert_eq!(multiset("MATCH (a)-->(b) |+| (a)<--(b) |+| (a)-->(a) RETURN a"), Some((true, 3)));
    assert_eq!(multiset("MATCH ((a)-->(b) | (a)<--(b)) RETURN a"), Some((false, 2)));
    assert!(parse_cypher25("MATCH (a)-->(b) | (a)<--(b) |+| (a)-->(a) RETURN a").is_err());
    // Comprehensions and `=~` are not typed yet, but still parse, and `4/-2`
    // is still a division.
    for query in [
        "RETURN [x IN [1, 2] | x + 1] AS l",
        "MATCH (a) RETURN [(a)-->(b) | b.name] AS l",
        "RETURN 'ab' =~ 'a.' AS m",
        "RETURN 4/-2 AS q",
    ] {
        assert!(parse_cypher25(query).is_ok(), "{query} parses");
    }
}

/// Edges with no node pattern around them get implicit empty ones (§16.7
/// Syntax Rule 14), node patterns may stand side by side (G046), edges may be
/// undirected (G043, G045), and a simplified path pattern (§16.12) is the
/// edges it abbreviates.
#[test]
fn relaxed_undirected_and_simplified_edges_lower() {
    let elements = |query: &str| {
        let document = parse_cypher25(query).unwrap_or_else(|error| panic!("{query}: {error:?}"));
        let [Statement::Query(query)] = document.statements.as_slice() else {
            panic!("typed");
        };
        let Some(Clause::Match(matched)) = query.clauses.first() else {
            panic!("MATCH");
        };
        matched.patterns[0].elements.clone()
    };
    let shape = |elements: &[PatternElement]| {
        elements
            .iter()
            .map(|element| match element {
                PatternElement::Node(node) => node.variable.as_ref().map_or("()".to_string(), |name| name.to_string()),
                PatternElement::Relationship(edge) => format!("{:?}", edge.direction),
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        shape(&elements("MATCH -[:R]->-[:R]-> RETURN 1 AS x")),
        ["()", "Outgoing", "()", "Outgoing", "()"]
    );
    assert_eq!(shape(&elements("MATCH (a)(b) RETURN a")), ["a", "b"]);
    assert_eq!(
        shape(&elements("MATCH (a)~(b)<~(c)~>(d) RETURN a")),
        ["a", "UndirectedEdge", "b", "LeftOrUndirected", "c", "UndirectedOrRight", "d"]
    );
    assert_eq!(
        shape(&elements("MATCH (a)-/R <S ~T>/-(b) RETURN a")),
        ["a", "Undirected", "()", "Incoming", "()", "UndirectedOrRight", "b"]
    );
    let union = elements("MATCH (a)-/R|!S/->(b) RETURN a");
    let Some(PatternElement::Relationship(edge)) = union.get(1) else {
        panic!("an edge");
    };
    assert_eq!(
        edge.label_expression,
        Some(LabelExpression::Or(vec![
            LabelExpression::Label("R".into()),
            LabelExpression::Not(Box::new(LabelExpression::Label("S".into()))),
        ]))
    );
}

/// GQL's value types (§18.9) lower by their words, in a type predicate
/// (§19.6) and in CAST (§20.8); the new type words stay names.
#[test]
fn value_types_lower_in_type_predicates_and_casts() {
    let predicate_type = |query: &str| {
        let document = parse_cypher25(query).unwrap_or_else(|error| panic!("{query}: {error:?}"));
        let [Statement::Query(query)] = document.statements.as_slice() else {
            panic!("typed: {query}");
        };
        let Some(Clause::Return(returned)) = query.clauses.last() else {
            panic!("RETURN");
        };
        let ProjectionItems::Items(items) = &returned.items else {
            panic!("items");
        };
        match &items[0].expression {
            Expression::TypeTest { value_type, .. } | Expression::Cast { value_type, .. } => value_type.clone(),
            other => panic!("{other:?}"),
        }
    };
    let kind = |query: &str| predicate_type(query).kind;
    assert_eq!(
        kind("RETURN 1 IS :: UNSIGNED INTEGER16 AS v"),
        ValueTypeKind::Integer {
            signed: false,
            size: IntegerSize::Bits(16)
        }
    );
    assert_eq!(
        kind("RETURN 1 IS TYPED INT(10) AS v"),
        ValueTypeKind::Integer {
            signed: true,
            size: IntegerSize::Precision(10)
        }
    );
    assert_eq!(
        kind("RETURN 1 IS :: DOUBLE PRECISION AS v"),
        ValueTypeKind::Float(FloatSize::Double)
    );
    assert_eq!(
        kind("RETURN 'a' IS :: STRING(1, 2) AS v"),
        ValueTypeKind::CharacterString {
            min_length: Some(1),
            max_length: Some(2)
        }
    );
    let list = predicate_type("RETURN [1] IS :: INTEGER NOT NULL LIST[3] AS v");
    let ValueTypeKind::List { element, max_length } = list.kind else {
        panic!("a list type");
    };
    assert_eq!(max_length, Some(3));
    assert!(element.expect("an element type").not_null);
    assert!(matches!(kind("RETURN 1 IS :: ANY<INT | STRING> AS v"), ValueTypeKind::Union(parts) if parts.len() == 2));
    assert!(matches!(
        kind("RETURN CAST(1 AS STRING) AS v"),
        ValueTypeKind::CharacterString { .. }
    ));
    assert!(predicate_type("RETURN CAST(1 AS INTEGER NOT NULL) AS v").not_null);
    for query in ["MATCH (real:Double {char: 1}) RETURN real.bytes AS cast", "RETURN 1 AS bigint"] {
        assert!(
            matches!(parse_cypher25(query).unwrap().statements.as_slice(), [Statement::Query(_)]),
            "{query} lowers"
        );
    }
}

/// Temporal literals (§21.2), DURATION_BETWEEN with its qualifier (§20.28)
/// and the current date and time functions (§20.27) lower; a variable may
/// still be named like one.
#[test]
fn temporal_syntax_lowers() {
    let item = |query: &str| {
        let document = parse_cypher25(query).unwrap_or_else(|error| panic!("{query}: {error:?}"));
        let [Statement::Query(query)] = document.statements.as_slice() else {
            panic!("typed: {query}");
        };
        let Some(Clause::Return(returned)) = query.clauses.last() else {
            panic!("RETURN");
        };
        let ProjectionItems::Items(items) = &returned.items else {
            panic!("items");
        };
        items[0].expression.clone()
    };
    assert!(matches!(
        item("RETURN TIMESTAMP '2026-01-01T00:00' AS v"),
        Expression::TemporalLiteral { kind: TemporalLiteralKind::Timestamp, text } if text.as_ref() == "2026-01-01T00:00"
    ));
    assert!(matches!(
        item("RETURN DURATION 'P1D' AS v"),
        Expression::TemporalLiteral {
            kind: TemporalLiteralKind::Duration,
            ..
        }
    ));
    let Expression::Function { name, arguments, .. } = item("RETURN DURATION_BETWEEN(a, b) YEAR TO MONTH AS v") else {
        panic!("a function");
    };
    assert_eq!(name[0].as_str(), "duration_between");
    assert_eq!(arguments.len(), 3);
    // A value keyword the engine resolves by scope: the function, unless a
    // visible variable has the name.
    assert!(matches!(
        item("RETURN CURRENT_DATE AS v"),
        Expression::ValueKeyword {
            keyword: ValueKeyword::CurrentTemporal(_),
            ..
        }
    ));
    assert!(matches!(
        item("WITH 1 AS current_date RETURN current_date AS v"),
        Expression::ValueKeyword { .. }
    ));
    assert!(matches!(item("RETURN {year: 2026, month: 1, day: 1} AS v"), Expression::Map(_)));
}

#[test]
fn simplified_semantic_restrictions_retain_their_algebra() {
    use hydradb_cypher_ast::PathFactor;
    for contents in ["!(R R)", "<(<R)", "R&(R?)", "R R|S", "R |+| R"] {
        let document = parse_cypher25(&format!("MATCH (a)-/{contents}/->(b) RETURN a")).unwrap();
        let [Statement::Query(query)] = document.statements.as_slice() else {
            panic!("typed query")
        };
        let Clause::Match(matched) = &query.clauses[0] else {
            panic!("typed match")
        };
        let Some(PathFactor::Concatenation(factors)) = &matched.patterns[0].factor else {
            panic!("retained factors")
        };
        assert!(
            factors.iter().any(|factor| matches!(factor, PathFactor::Simplified(_))),
            "{contents}"
        );
    }
}

#[test]
fn composite_next_preserves_both_arms() {
    fn query(source: &str) -> hydradb_cypher_ast::Query {
        let parsed = parse_cypher25(source).unwrap();
        let [Statement::Query(query)] = parsed.statements.as_slice() else {
            panic!("{source} lowers to a typed query")
        };
        query.clone()
    }

    use hydradb_cypher_ast::QueryConjunction;

    let other = query("RETURN 3 AS v");
    for source in [
        "FINISH NEXT RETURN 7 AS v",
        "RETURN 5 AS v NEXT RETURN 7 AS v",
        "FINISH NEXT FINISH",
    ] {
        let next = query(source);
        assert!(matches!(next.clauses.as_slice(), [_, Clause::Next, _]));
        assert!(next.unions.is_empty());
        for (operator, conjunction, all) in [
            ("UNION", QueryConjunction::Union, false),
            ("UNION DISTINCT", QueryConjunction::Union, false),
            ("UNION ALL", QueryConjunction::Union, true),
            ("EXCEPT ALL", QueryConjunction::Except, true),
            ("INTERSECT", QueryConjunction::Intersect, false),
            ("OTHERWISE", QueryConjunction::Otherwise, false),
        ] {
            for nested in [format!("{{ {source} }}"), format!("{{ {{ {source} }} }}")] {
                let first = query(&format!("{nested} {operator} RETURN 3 AS v"));
                assert_eq!(first.clauses, next.clauses);
                let [arm] = first.unions.as_slice() else {
                    panic!("one retained arm")
                };
                assert_eq!((arm.conjunction, arm.all), (conjunction, all));
                assert_eq!(arm.query.as_ref(), &other);

                let last = query(&format!("RETURN 3 AS v {operator} {nested}"));
                assert_eq!(last.clauses, other.clauses);
                let [arm] = last.unions.as_slice() else {
                    panic!("one retained arm")
                };
                assert_eq!((arm.conjunction, arm.all), (conjunction, all));
                assert_eq!(arm.query.as_ref(), &next);
            }
        }
    }
}

#[test]
fn standalone_sort_is_not_a_projection_boundary() {
    let parsed = parse_cypher25("MATCH (n) ORDER BY EXISTS { MATCH (n)-->(m) } OFFSET 1 LIMIT 2 RETURN n").unwrap();
    let [Statement::Query(query)] = parsed.statements.as_slice() else {
        panic!("typed query")
    };
    let Clause::OrderByAndPage(page) = &query.clauses[1] else {
        panic!("standalone order statement")
    };
    assert_eq!(page.order_by.len(), 1);
    assert!(matches!(page.order_by[0].expression, Expression::Exists(_)));
    assert_eq!(page.skip, Some(Expression::Literal(Literal::Integer(1))));
    assert_eq!(page.limit, Some(Expression::Literal(Literal::Integer(2))));
}
/// A chain of property reads lowers to nested property references; the
/// engine decides that only a node or an edge has properties (GQL §20.1).
#[test]
fn property_chains_lower_to_nested_references() {
    assert_eq!(
        returned_items("MATCH (n) RETURN n.a.b AS deep"),
        vec![Expression::Property {
            target: Box::new(Expression::Property {
                target: Box::new(Expression::Identifier("n".into())),
                key: "a".into(),
            }),
            key: "b".into(),
        }]
    );
}

/// GQL's `LET ... IN ... END` (§20.5) and `PATH[...]` (§20.14) lower to their
/// own expressions; a MATCH's `YIELD` (§16.3) to its yield items; and a
/// questioned path (§16.7, G037) to the union of its choices.
#[test]
fn let_path_yield_and_questioned_paths_lower() {
    assert!(matches!(
        &returned_items("RETURN LET x = 1, y = 2 IN x + y END AS v")[0],
        Expression::Let { bindings, .. } if bindings.len() == 2 && bindings[1].0.as_str() == "y"
    ));
    assert!(matches!(
        &returned_items("MATCH (a)-[e]->(b) RETURN PATH[a, e, b] AS p")[0],
        Expression::PathConstructor(elements) if elements.len() == 3
    ));
    let document = parse_cypher25("MATCH (a)-[e:R]->?(b) WHERE a.x = 1 YIELD a, e RETURN a").expect("valid YIELD and ?");
    let [Statement::Query(query)] = document.statements.as_slice() else {
        panic!("typed");
    };
    let Some(Clause::Match(matched)) = query.clauses.first() else {
        panic!("MATCH first");
    };
    let yielded: Vec<&str> = matched.yield_items.iter().flatten().map(|name| name.as_str()).collect();
    assert_eq!(yielded, ["a", "e"]);
    let alternatives = matched.patterns[0].alternatives.as_ref().expect("a union of the choices");
    assert!(!alternatives.multiset);
    let shapes: Vec<usize> = alternatives.operands.iter().map(|operand| operand.path.elements.len()).collect();
    // Matched: node, edge, node. Skipped: the two nodes side by side.
    assert_eq!(shapes, [3, 2]);
}

/// SQL's `TIMESTAMP` keeps its own kind, and `INTERVAL` its qualifier, with
/// the sign before the string leading the text (GL12); the new words stay
/// names.
#[test]
fn sql_datetime_and_interval_literals_lower() {
    use hydradb_cypher_ast::{IntervalField, IntervalQualifier};
    assert!(matches!(
        &returned_items("RETURN TIMESTAMP '2026-01-01 12:00:00' AS v")[0],
        Expression::TemporalLiteral {
            kind: TemporalLiteralKind::Timestamp,
            ..
        }
    ));
    let items = returned_items("RETURN INTERVAL -'1 02:03:04.5' DAY(3) TO SECOND(2) AS v");
    let [Expression::TemporalLiteral {
        kind: TemporalLiteralKind::Interval(qualifier),
        text,
    }] = items.as_slice()
    else {
        panic!("an interval literal");
    };
    assert_eq!(text.as_ref(), "-1 02:03:04.5");
    assert_eq!(
        *qualifier,
        IntervalQualifier {
            start: IntervalField::Day,
            end: Some(IntervalField::Second),
            leading_precision: Some(3),
            fractional_precision: Some(2),
        }
    );
    assert!(matches!(
        &returned_items("WITH 1 AS hour, 2 AS interval RETURN hour + interval AS v")[0],
        Expression::Binary { .. }
    ));
}

/// `CALL { }` lowers to an inline procedure call (§15.2): no scope clause and
/// `(*)` import everything, `()` nothing, a list its names.
#[test]
fn inline_procedure_calls_lower() {
    let scope = |query: &str| {
        let document = parse_cypher25(query).unwrap_or_else(|error| panic!("{query}: {error:?}"));
        let [Statement::Query(query)] = document.statements.as_slice() else {
            panic!("typed");
        };
        let Some(Clause::Call(call)) = query.clauses.get(1) else {
            panic!("CALL second: {:?}", query.clauses);
        };
        (
            call.optional,
            call.scope
                .as_ref()
                .map(|names| names.iter().map(|name| name.as_str().to_string()).collect::<Vec<_>>()),
        )
    };
    assert_eq!(scope("MATCH (n) CALL { RETURN 1 AS x } RETURN x"), (false, None));
    assert_eq!(scope("MATCH (n) CALL (*) { RETURN 1 AS x } RETURN x"), (false, None));
    assert_eq!(scope("MATCH (n) OPTIONAL CALL () { RETURN 1 AS x } RETURN x"), (true, Some(vec![])));
    assert_eq!(
        scope("MATCH (n), (m) CALL (n, m) { RETURN 1 AS x } RETURN x"),
        (false, Some(vec!["n".to_string(), "m".to_string()]))
    );
}

/// A procedure body's `VALUE x = e` definitions (§10.3) lower to the LETs they
/// mean, one per definition, at the head of the body, marked as definitions:
/// NEXT restores them under their original definition scope.
#[test]
fn value_variable_definitions_lower_to_lets() {
    let document = parse_cypher25("VALUE x = 1 VALUE y = x RETURN y NEXT RETURN 1 AS z").expect("valid definitions");
    let [Statement::Query(query)] = document.statements.as_slice() else {
        panic!("typed");
    };
    let shapes: Vec<&str> = query
        .clauses
        .iter()
        .map(|clause| match clause {
            Clause::Let(_) => "LET",
            Clause::Return(_) => "RETURN",
            Clause::Next => "NEXT",
            _ => "other",
        })
        .collect();
    assert_eq!(shapes, ["LET", "LET", "RETURN", "NEXT", "RETURN"]);
    assert!(query
        .clauses
        .iter()
        .all(|clause| !matches!(clause, Clause::Let(binding) if !binding.definition)));
}

#[test]
fn let_value_items_lower_to_table_bindings_not_record_definitions() {
    for text in ["LET VALUE x = 1, y = 2 RETURN x, y", "LET x = 1, VALUE y = 2 RETURN x, y"] {
        let document = parse_cypher25(text).expect("GP05 LET VALUE");
        let [Statement::Query(query)] = document.statements.as_slice() else {
            panic!("typed");
        };
        let Clause::Let(binding) = &query.clauses[0] else {
            panic!("LET");
        };
        assert!(!binding.definition);
        assert_eq!(
            binding.bindings.iter().map(|(name, _)| name.as_str()).collect::<Vec<_>>(),
            ["x", "y"]
        );
    }
}
