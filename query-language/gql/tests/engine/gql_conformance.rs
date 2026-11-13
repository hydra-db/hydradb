//! ISO/IEC 39075:2024 (GQL), as amended by Technical Corrigendum 1:2026: the
//! features the experimental engine executes, mandatory and optional, each
//! pinned to the subclause that defines it, and every exception to its
//! GQLSTATUS.
//!
//! See `query-language/gql/features.md` for the matrix these tests cover, and
//! `query-language/gql/deviations.md` for every difference from the books.

#[path = "../../../shared/tests/engine/common/mod.rs"]
mod common;

use common::compat::{graph, run, table};
use hydradb_cypher_ast::GqlStatus;
use hydradb_cypher_engine::{CypherEngine, ListParameterMap, ParameterMap, QueryValue, ScalarValue};

/// The condition a query raises, from preparation or execution.
async fn status(query: &str) -> GqlStatus {
    status_with(query, &ParameterMap::new()).await
}

/// The condition a query raises with these parameters.
async fn status_with(query: &str, parameters: &ParameterMap) -> GqlStatus {
    let engine = CypherEngine::new(graph());
    let error = match engine.prepare_with_lists(query, parameters, &ListParameterMap::new()) {
        Err(error) => error,
        Ok(prepared) => match engine.execute_prepared(&prepared).await {
            Err(error) => error,
            Ok(result) => panic!("{query:?} was expected to fail, and returned {:?}", result.rows),
        },
    };
    error.gql_status()
}

/// The single value of a one-row, one-column query.
async fn value(query: &str) -> String {
    let rows = table(&run(query).await);
    assert_eq!(rows.len(), 1, "{query} returns one row");
    assert_eq!(rows[0].len(), 1, "{query} returns one column");
    rows[0][0].clone()
}

// ---- §20.21 numeric value expressions ----

#[tokio::test]
async fn integer_arithmetic_stays_integer() {
    assert_eq!(value("RETURN 7 + 2 * 3 - 1 AS v").await, "12");
    assert_eq!(value("RETURN 7 / 2 AS v").await, "3", "exact division truncates toward zero");
    assert_eq!(value("RETURN -7 / 2 AS v").await, "-3");
    assert_eq!(value("RETURN -7 % 3 AS v").await, "-1", "the remainder keeps the dividend's sign");
}

#[tokio::test]
async fn a_float_operand_makes_the_result_a_float() {
    assert_eq!(value("RETURN 1.5 + 1 AS v").await, "2.5f");
    assert_eq!(value("RETURN 2 * 1.5e0 AS v").await, "3.0f");
    assert_eq!(value("RETURN 7 / 2.0 AS v").await, "3.5f");
    assert_eq!(value("RETURN -(1.25) AS v").await, "-1.25f");
    assert_eq!(value("RETURN 5.5 % 2 AS v").await, "1.5f");
}

#[tokio::test]
async fn arithmetic_on_null_is_null() {
    assert_eq!(value("RETURN 1 + null AS v").await, "null");
    assert_eq!(value("RETURN -null AS v").await, "null");
}

#[tokio::test]
async fn numeric_exceptions_carry_their_conditions() {
    assert_eq!(status("RETURN 1 / 0 AS v").await, GqlStatus::DIVISION_BY_ZERO);
    assert_eq!(status("RETURN 1.0 / 0 AS v").await, GqlStatus::DIVISION_BY_ZERO);
    assert_eq!(status("RETURN 5 % 0 AS v").await, GqlStatus::DIVISION_BY_ZERO);
    assert_eq!(
        status("RETURN 9223372036854775807 + 1 AS v").await,
        GqlStatus::NUMERIC_VALUE_OUT_OF_RANGE,
        "INTEGER is 64-bit"
    );
    assert_eq!(status("RETURN 1.0e308 * 10 AS v").await, GqlStatus::NUMERIC_VALUE_OUT_OF_RANGE);
    assert_eq!(status("RETURN 'a' - 1 AS v").await, GqlStatus::INVALID_VALUE_TYPE);
}

#[tokio::test]
async fn with_sort_reads_restored_fixed_record() {
    assert_eq!(
        status("VALUE fixed = 7 UNWIND [2, 1] AS x WITH x AS fixed WITH 0 AS t ORDER BY 1 / (fixed - 7) RETURN t").await,
        GqlStatus::DIVISION_BY_ZERO
    );
}

#[tokio::test]
async fn distinct_with_sort_reads_restored_fixed_record() {
    assert_eq!(
        status("VALUE fixed = 7 UNWIND [2, 1] AS x WITH x AS fixed WITH DISTINCT 0 AS t ORDER BY 1 / (fixed - 7) RETURN t").await,
        GqlStatus::DIVISION_BY_ZERO
    );
}

#[tokio::test]
async fn with_sort_reads_restored_fixed_record_before_windows() {
    for (projection, input) in [
        ("v AS t", "[1, 2]"),
        ("DISTINCT v AS t", "[1, 2, 1]"),
        ("v AS t, count(*) AS c", "[1, 2, 1]"),
    ] {
        for (direction, first, second) in [("ASC", "2", "1"), ("DESC", "1", "2")] {
            for (window, expected) in [("LIMIT 1", first), ("SKIP 1 LIMIT 1", second)] {
                let query = format!(
                    "VALUE fixed = 7 UNWIND {input} AS x WITH x AS fixed, x AS v \
                     WITH {projection} ORDER BY fixed - t {direction} {window} RETURN t"
                );
                assert_eq!(value(&query).await, expected, "{query}");
            }
        }
    }
}

#[tokio::test]
async fn with_sort_reads_restored_fixed_record_without_forcing_dropped_columns() {
    assert_eq!(
        value(
            "VALUE fixed = 7 UNWIND [1, 2] AS x WITH x AS fixed, x AS v WITH v AS t, 1 / (v - 1) AS w ORDER BY fixed - t LIMIT 1 RETURN w"
        )
        .await,
        "1"
    );
    assert_eq!(
        value("VALUE fixed = 7 UNWIND [1, 2] AS x WITH x AS fixed, x AS v WITH v AS fixed, v AS t ORDER BY fixed - t LIMIT 1 RETURN t")
            .await,
        "1"
    );
    assert_eq!(
        value("VALUE fixed = 7 UNWIND [1, 2] AS x WITH null AS fixed, x AS t ORDER BY CASE WHEN fixed IS NULL THEN -t ELSE t END LIMIT 1 RETURN t").await,
        "2"
    );
}

// ---- §21.2 literals ----

#[tokio::test]
async fn gql_decimal_digits_are_normalized_before_typing() {
    // §21.2 and §21.4 SR2: decimal digits include Unicode Nd, not other
    // Unicode numbers. The supported INTEGER range is unchanged here.
    for spelling in ["٣", "３", "𝟛", "३", "0003", "٠٠٣", "0_0_3"] {
        assert_eq!(value(&format!("RETURN {spelling} AS v")).await, "3", "{spelling}");
    }
    for (spelling, expected) in [("8.E8", "800000000.0f"), ("٨.E٨", "800000000.0f"), ("٣.٥", "3.5f"), ("8.", "8.0f")] {
        assert_eq!(value(&format!("RETURN {spelling} AS v")).await, expected, "{spelling}");
    }
    for digit in ['0', '1', '7'] {
        for count in [18, 19] {
            let spelling: String = std::iter::repeat_n(digit, count).collect();
            let expected = spelling.parse::<i64>().unwrap().to_string();
            assert_eq!(value(&format!("RETURN {spelling} AS v")).await, expected);
        }
    }
    for spelling in ["³", "18446744073709551616", "99999999999999999999"] {
        assert!(CypherEngine::new(graph())
            .prepare(&format!("RETURN {spelling} AS v"), &ParameterMap::new())
            .is_err());
    }
}

#[tokio::test]
async fn list_constructors_and_parenthesized_match_blocks_lower_normatively() {
    // §20.17 and §19.4.
    for constructor in ["[1, 2]", "LIST [1, 2]", "ARRAY [1, 2]"] {
        assert_eq!(value(&format!("RETURN {constructor} AS v")).await, "[1, 2]");
    }
    let direct = table(&run("MATCH (n) WHERE EXISTS ((n)->()) RETURN n.name AS name ORDER BY name").await);
    let block = table(&run("MATCH (n) WHERE EXISTS (MATCH (n)->()) RETURN n.name AS name ORDER BY name").await);
    assert_eq!(direct, block);
    assert!(!block.is_empty());
}

#[tokio::test]
async fn coalesce_only_evaluates_needed_arguments_in_rows_and_groups() {
    // §20.7: expressions after the first non-null value are not evaluated.
    assert_eq!(value("RETURN coalesce(1, 1 / 0) AS v").await, "1");
    assert_eq!(value("UNWIND [1, 2] AS x RETURN coalesce(sum(x), 1 / 0) AS v").await, "3");
    assert_eq!(status("RETURN coalesce(null, 1 / 0) AS v").await, GqlStatus::DIVISION_BY_ZERO);
}

#[tokio::test]
async fn recognized_operations_survive_preparation_and_execution_errors() {
    use hydradb_cypher_ast::operation;
    let engine = CypherEngine::new(graph());
    for (query, expected) in [
        ("RETURN abs(absent)", operation::RETURN),
        ("FILTER EXISTS { RETURN absent } RETURN 1", operation::RETURN),
        ("RETURN unimplemented_function(1)", operation::RETURN),
        ("not a query", operation::UNRECOGNIZED),
        ("MATCH (n) WHERE sum(1) > 0 RETURN n", operation::MATCH),
    ] {
        let diagnostic = engine.prepare(query, &ParameterMap::new()).unwrap_err().status_object();
        let record = diagnostic.diagnostic.as_ref().unwrap();
        assert_eq!((record.operation, record.operation_code), expected, "{query}");
    }
    for name in ["absent", "unbound", "missing_value"] {
        for (query, expected) in [
            (format!("RETURN {name}"), operation::RETURN),
            (format!("FILTER {name} RETURN 1"), operation::FILTER),
        ] {
            let diagnostic = engine.prepare(&query, &ParameterMap::new()).unwrap_err().status_object();
            assert_eq!(diagnostic.status, GqlStatus::INVALID_REFERENCE, "{query}");
            assert_eq!(
                (
                    diagnostic.diagnostic.as_ref().unwrap().operation,
                    diagnostic.diagnostic.as_ref().unwrap().operation_code
                ),
                expected,
                "{query}"
            );
            assert_eq!(diagnostic.diagnostic.as_ref().unwrap().invalid_reference.as_deref(), Some(name));
            assert!(diagnostic.diagnostic.as_ref().unwrap().current_schema.is_none());
        }
    }
    for numerator in [1, -2] {
        let query = format!("RETURN {numerator} / 0 AS value");
        let prepared = engine.prepare(&query, &ParameterMap::new()).unwrap();
        let diagnostic = engine.execute_prepared(&prepared).await.unwrap_err().status_object();
        assert_eq!(diagnostic.status, GqlStatus::DIVISION_BY_ZERO);
        assert_eq!(
            (
                diagnostic.diagnostic.as_ref().unwrap().operation,
                diagnostic.diagnostic.as_ref().unwrap().operation_code
            ),
            operation::RETURN
        );
        assert!(diagnostic.diagnostic.as_ref().unwrap().invalid_reference.is_none());
        assert!(diagnostic.diagnostic.as_ref().unwrap().current_schema.is_none());
    }
}

#[tokio::test]
async fn folded_filters_keep_the_failing_statement() {
    use hydradb_cypher_ast::operation;
    // §23.2 GR2: OPERATION identifies the statement raising the exception.
    for (predicates, expected, status) in [
        (
            "WHERE 1 / (m.id - m.id) = 0 FILTER m.id > 0",
            operation::MATCH,
            GqlStatus::INVALID_VALUE_TYPE,
        ),
        (
            "WHERE 1 / (coalesce(m.score, 1) - coalesce(m.score, 1)) = 0 FILTER m.id > 0",
            operation::MATCH,
            GqlStatus::DIVISION_BY_ZERO,
        ),
        (
            "WHERE m.id > 0 FILTER 1 / (coalesce(m.score, 1) - coalesce(m.score, 1)) = 0",
            operation::FILTER,
            GqlStatus::DIVISION_BY_ZERO,
        ),
        ("WHERE m.id FILTER m.id > 0", operation::MATCH, GqlStatus::INVALID_VALUE_TYPE),
    ] {
        for window in ["LIMIT 1", "SKIP 1 LIMIT 1", ""] {
            for (pattern, probes) in [("()-[r]->(m)", false), ("()-[r:R]->(m)", true), ("()-[r*1..2]->(m)", false)] {
                let mut storage = graph();
                storage.answers_relationship_probes = probes;
                let query = format!("MATCH {pattern} {predicates} RETURN m {window}");
                let error = CypherEngine::new(storage).execute(&query, &ParameterMap::new()).await.unwrap_err();
                let object = error.status_object();
                assert_eq!(object.status, status, "{query}");
                let record = object.diagnostic.unwrap();
                assert_eq!((record.operation, record.operation_code), expected, "{query}");
            }
        }
    }
}

// Cor1 §14.10 SR4c: grouping in the written RETURN permits sort aggregates,
// even when lowering implements that grouping as DISTINCT.
#[tokio::test]
async fn path_candidate_failures_keep_match_operation_provenance() {
    use hydradb_cypher_ast::operation;
    let engine = CypherEngine::new(graph());
    for query in [
        "MATCH ANY ((a)-[:R]->(b) WHERE 1 / 0 = 1) RETURN a",
        "MATCH ALL ((a)-[:R]->(b) WHERE 1 / 0 = 1) RETURN a",
        "MATCH ANY (a WHERE 1 / 0 = 1)-[:R]->(b) RETURN a",
        "MATCH ANY (a)-[:R WHERE 1 / 0 = 1]->(b) RETURN a",
        "MATCH (s:Node {name: 'a'})((a)-[:R]->(b) WHERE 1 / 0 = 1){1,2}(z) RETURN z",
    ] {
        let prepared = engine.prepare(query, &ParameterMap::new()).unwrap();
        let diagnostic = engine.execute_prepared(&prepared).await.unwrap_err().status_object();
        assert_eq!(diagnostic.status, GqlStatus::DIVISION_BY_ZERO, "{query}");
        let record = diagnostic.diagnostic.as_ref().unwrap();
        assert_eq!((record.operation, record.operation_code), operation::MATCH, "{query}");
    }
}

// Cor1 §14.10 SR4c: grouping in the written RETURN permits sort aggregates,
// even when lowering implements that grouping as DISTINCT.
#[tokio::test]
async fn explicit_grouping_preserves_sort_aggregate_context() {
    assert!(run("UNWIND [] AS x RETURN 0 AS k GROUP BY x ORDER BY count(*)")
        .await
        .rows
        .is_empty());
    assert_eq!(
        table(&run("UNWIND [1, 2, 2] AS x WITH x * 10 AS k GROUP BY x ORDER BY count(*) DESC RETURN k").await),
        [["20"], ["10"]]
    );
    for distinct in ["", "DISTINCT "] {
        for aggregate in ["", ", count(*) AS c"] {
            let query = format!("UNWIND [2, 1, 2] AS x RETURN {distinct}x{aggregate} GROUP BY x ORDER BY count(*)");
            let expected = if aggregate.is_empty() {
                vec![vec!["1"], vec!["2"]]
            } else {
                vec![vec!["1", "1"], vec!["2", "2"]]
            };
            assert_eq!(table(&run(&query).await), expected, "{query}");
        }
    }
    for distinct in ["", "DISTINCT "] {
        for alias in ["x", "k"] {
            let query = format!("UNWIND [2, 1, 2] AS x RETURN {distinct}x AS {alias} GROUP BY x ORDER BY count(*) DESC, {alias}");
            assert_eq!(table(&run(&query).await), vec![vec!["2"], vec!["1"]], "{query}");
        }
    }
    for distinct in ["", "DISTINCT "] {
        for (items, keys, expected) in [
            ("x * 10 AS k", "x, x + 1", vec![vec!["20"], vec!["10"]]),
            ("sum(x) AS k", "x", vec![vec!["4"], vec!["1"]]),
        ] {
            let query = format!("UNWIND [1, 2, 2] AS x RETURN {distinct}{items} GROUP BY {keys} ORDER BY count(*) DESC, k");
            assert_eq!(table(&run(&query).await), expected, "{query}");
        }
    }
    assert_eq!(
        status("UNWIND [2, 1, 2] AS x RETURN x AS k ORDER BY count(*)").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
}

#[tokio::test]
async fn an_integer_literal_needs_a_supported_exact_numeric_type() {
    // Syntax Rule 18: the literal needs an exact numeric type that holds it.
    assert_eq!(value("RETURN 9223372036854775807 AS v").await, "9223372036854775807");
    assert_eq!(
        value("RETURN -9223372036854775808 AS v").await,
        "-9223372036854775808",
        "an extension (IE005): the negated literal is read as one value, as Cypher reads it"
    );
    for spelling in [
        "9223372036854775808",
        "9223372036854775809",
        "9999999999999999999",
        "18446744073709551615",
    ] {
        assert_eq!(value(&format!("RETURN {spelling} AS v")).await, spelling);
        assert_eq!(value(&format!("RETURN {spelling} IS TYPED UINT64 AS v")).await, "true");
    }
    assert_eq!(
        status("RETURN -9223372036854775809 AS v").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    assert_eq!(value("RETURN 0x8000000000000000 AS v").await, "9223372036854775808");
    for spelling in ["18446744073709551616", "0x10000000000000000"] {
        assert_eq!(
            status(&format!("RETURN {spelling} AS v")).await,
            GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
        );
    }
}

#[tokio::test]
async fn a_float_literal_must_fit_float() {
    // Syntax Rule 20.
    let largest = value("RETURN 1.7976931348623157E308 AS v").await;
    assert_eq!(largest.trim_end_matches('f').parse::<f64>(), Ok(f64::MAX));
    assert_eq!(status("RETURN 1e400 AS v").await, GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION);
}

#[tokio::test]
async fn repaired_value_cardinality_counts_record_fields_including_nulls() {
    // §20.22 GR4 d), §4.16.8: fields, not material field values.
    for (record, expected) in [
        ("{}", "0"),
        ("{a: null}", "1"),
        ("{a: 1, b: null}", "2"),
        ("{a: 1, b: null, c: {}, d: [], e: 5}", "5"),
    ] {
        assert_eq!(value(&format!("RETURN CARDINALITY({record}) AS v")).await, expected);
        assert_eq!(
            value(&format!("UNWIND [1, 2] AS x RETURN CARDINALITY(max({record})) AS v")).await,
            expected
        );
    }
    assert_eq!(value("RETURN CARDINALITY(NULL) AS v").await, "null");
    assert_eq!(value("RETURN CARDINALITY([1, null]) AS v").await, "2");
    assert_eq!(status("RETURN CARDINALITY(1) AS v").await, GqlStatus::INVALID_VALUE_TYPE);
}

#[tokio::test]
async fn repaired_value_datetime_records_require_complete_field_sets() {
    // §20.27 GR4: a datetime requires year/month/day and a permitted time set.
    for function in ["LOCAL_DATETIME", "ZONED_DATETIME", "DATETIME"] {
        for record in [
            "{}",
            "{year: 2024}",
            "{year: 2024, hour: 1}",
            "{year: 2024, month: 2, hour: 1}",
            "{year: 2024, month: 2, day: 29}",
            "{year: 2024, month: 2, day: 29, minute: 1}",
            "{year: 2024, month: 2, day: 29, hour: 1, second: 1}",
            "{year: 2024, month: 2, day: 29, hour: 1, nanosecond: 1}",
        ] {
            assert_eq!(
                status(&format!("RETURN {function}({record}) AS v")).await,
                GqlStatus::INVALID_DATETIME_FUNCTION_FIELD_NAME,
                "{function}({record})"
            );
        }
        assert_eq!(
            value(&format!(
                "RETURN CAST({function}({{year: 2024, month: 2, day: 29, hour: 1}}) AS DATE) AS v"
            ))
            .await,
            "DATE '2024-02-29'"
        );
    }
    for (record, expected) in [
        ("{year: 2024, month: 2, day: 29, hour: 1}", "DATETIME '2024-02-29T01:00:00'"),
        (
            "{year: 2024, month: 2, day: 29, hour: 1, minute: 2}",
            "DATETIME '2024-02-29T01:02:00'",
        ),
        (
            "{year: 2024, month: 2, day: 29, hour: 1, minute: 2, second: 3, nanosecond: 4}",
            "DATETIME '2024-02-29T01:02:03.000000004'",
        ),
    ] {
        assert_eq!(value(&format!("RETURN LOCAL_DATETIME({record}) = {expected} AS v")).await, "true");
    }
    assert_eq!(value("RETURN ZONED_DATETIME({year: 2024, month: 2, day: 29, hour: 1, timezone: '+00:30'}) = DATETIME '2024-02-29T01:00:00+00:30' AS v").await, "true");
    assert_eq!(value("RETURN DATE({year: 2024}) AS v").await, "DATE '2024-01-01'");
    assert_eq!(value("RETURN DATE({year: 2024, month: 2}) AS v").await, "DATE '2024-02-01'");
}

#[tokio::test]
async fn repaired_value_reduced_dates_share_function_cast_and_literal_parsing() {
    // §21.2 SR59, §20.27 GR7/9: ISO reduced precision fills omitted minima.
    for (text, expected) in [
        ("2000", "2000-01-01"),
        ("2000-02", "2000-02-01"),
        ("2024", "2024-01-01"),
        ("2024-02", "2024-02-01"),
    ] {
        for expression in [
            format!("DATE('{text}')"),
            format!("DATE '{text}'"),
            format!("CAST('{text}' AS DATE)"),
        ] {
            assert_eq!(value(&format!("RETURN {expression} AS v")).await, format!("DATE '{expected}'"));
        }
    }
    for text in ["2024-00", "2024-13", "2024-2", "2024-", "24", "2024-02-30"] {
        assert_eq!(
            status(&format!("RETURN DATE('{text}') AS v")).await,
            GqlStatus::INVALID_DATETIME_FORMAT
        );
        assert_eq!(
            status(&format!("RETURN CAST('{text}' AS DATE) AS v")).await,
            GqlStatus::INVALID_DATETIME_FORMAT
        );
        assert_eq!(
            status(&format!("RETURN DATE '{text}' AS v")).await,
            GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
        );
    }
}

#[tokio::test]
async fn repaired_value_zoned_to_local_datetime_adds_displacement() {
    // §20.8 GR4 n) vi) 5/8 adds the displacement, not UTC normalization.
    for (source, expected) in [
        ("2024-01-01T00:15:00+01:00", "2024-01-01T01:15:00"),
        ("2024-01-01T23:15:00+01:00", "2024-01-02T00:15:00"),
        ("2024-01-29T00:15:00+01:00", "2024-01-29T01:15:00"),
        ("2024-01-29T23:15:00+01:00", "2024-01-30T00:15:00"),
        ("2024-01-31T23:45:00+00:30", "2024-02-01T00:15:00"),
        ("2024-02-29T23:45:00+00:30", "2024-03-01T00:15:00"),
        ("2024-12-31T23:45:00+00:30", "2025-01-01T00:15:00"),
        ("2024-03-01T00:15:00-00:30", "2024-02-29T23:45:00"),
        ("2024-01-01T00:15:00-01:00", "2023-12-31T23:15:00"),
        ("2024-02-29T23:59:59.123456789Z", "2024-02-29T23:59:59.123456789"),
    ] {
        assert_eq!(
            value(&format!(
                "RETURN CAST(DATETIME '{source}' AS LOCAL DATETIME) = DATETIME '{expected}' AS v"
            ))
            .await,
            "true"
        );
    }
    // GR4 n) iv): time wraps before attaching the current date.
    for (time, expected) in [("23:45+00:30", "00:15:00"), ("00:15-00:30", "23:45:00"), ("12:00Z", "12:00:00")] {
        assert_eq!(
            value(&format!(
                "RETURN CAST(CAST(TIME '{time}' AS LOCAL DATETIME) AS LOCAL TIME) = TIME '{expected}' AS v"
            ))
            .await,
            "true"
        );
        assert_eq!(
            value(&format!("RETURN CAST(CAST(TIME '{time}' AS LOCAL DATETIME) AS DATE) = DATE() AS v")).await,
            "true"
        );
    }
    assert_eq!(
        status("RETURN CAST(DATETIME '+262142-12-31T23:59:59+14:00' AS LOCAL DATETIME) AS v").await,
        GqlStatus::DATETIME_FIELD_OVERFLOW
    );
}

#[tokio::test]
async fn repaired_value_nested_list_casts_report_canonical_truncation() {
    // Cor1 §20.8 GR4 d) ii): 2202F at every nested type boundary.
    for query in [
        "RETURN CAST([1, 2] AS LIST<INTEGER>[1]) AS v",
        "RETURN CAST([[1, 2]] AS LIST<LIST<INTEGER>[1]>) AS v",
        "RETURN CAST({a: [1, 2]} AS RECORD {a LIST<INTEGER>[1]}) AS v",
    ] {
        assert_eq!(status(query).await, GqlStatus::LIST_DATA_RIGHT_TRUNCATION, "{query}");
    }
    assert_eq!(value("RETURN CAST([[1]] AS LIST<LIST<INTEGER>[1]>) AS v").await, "[[1]]");
    assert_eq!(value("RETURN COALESCE(1, CAST([1, 2] AS LIST<INTEGER>[1])) AS v").await, "1");
    assert_eq!(value("RETURN CAST(VALUE { RETURN 3 AS v LIMIT 1 } AS INTEGER) AS v").await, "3");
    assert_eq!(value("UNWIND [1, 2] AS x RETURN CAST(sum(x) AS INTEGER) AS v").await, "3");
    assert_eq!(value("WITH 1 AS x RETURN CAST(x AS INTEGER) AS x ORDER BY x").await, "1");
    assert_eq!(
        status("RETURN CAST(unbound_value AS INTEGER) AS v").await,
        GqlStatus::INVALID_REFERENCE
    );
}

#[tokio::test]
async fn tokens_are_separated_where_the_lexical_rules_say() {
    // §21.2 Syntax Rule 1: nothing between a radix prefix and its digits.
    assert_eq!(status("RETURN 0x 1F AS v").await, GqlStatus::INVALID_SYNTAX);
    // §21.3 Syntax Rule 14: a non-delimiter token is followed by a delimiter
    // token or a separator.
    assert_eq!(status("RETURN 1AS v").await, GqlStatus::INVALID_SYNTAX);
    assert_eq!(status("RETURN 0o 17 AS v").await, GqlStatus::INVALID_SYNTAX);
    assert_eq!(status("RETURN 0b 101 AS v").await, GqlStatus::INVALID_SYNTAX);
    // A digit outside the radix makes the text no literal at all.
    assert_eq!(status("RETURN 0b12 AS v").await, GqlStatus::INVALID_SYNTAX);
    assert_eq!(status("RETURN 0xZZ AS v").await, GqlStatus::INVALID_SYNTAX);
    assert_eq!(value("RETURN 0b101 AS v").await, "5");
}

#[tokio::test]
async fn double_quoted_names_and_doubled_quotes() {
    // §21.2: a doubled quote of the delimiting kind is one quote.
    assert_eq!(value("RETURN 'it''s' AS v").await, "\"it's\"");
    assert_eq!(value("RETURN \"a\"\"b\" AS v").await, "\"a\\\"b\"");
    // §21.3: a double-quoted delimited identifier names a label, a property
    // or an alias; in a value expression it is still a string.
    assert_eq!(value("MATCH (n:\"Hub\") RETURN n.\"name\" AS v").await, "\"hub\"");
    let result = run("MATCH (n:Hub) RETURN n.name AS \"the name\"").await;
    assert_eq!(common::compat::columns(&result), ["the name"]);
    assert_eq!(value("RETURN \"x\" AS v").await, "\"x\"");
    // A variable is a regular identifier (§21.1), so it cannot be quoted.
    assert_eq!(status("MATCH (\"n\") RETURN count(*) AS c").await, GqlStatus::INVALID_SYNTAX);
}

#[tokio::test]
async fn no_escape_strings_keep_backslashes() {
    // §21.2 GL11.
    assert_eq!(value("RETURN @'a\\tb' AS v").await, "\"a\\\\tb\"");
    assert_eq!(value("RETURN @'it''s' AS v").await, "\"it's\"");
}

#[tokio::test]
async fn an_integer_sum_must_fit_64_bits() {
    assert_eq!(
        status("UNWIND [9223372036854775807, 1] AS x RETURN sum(x) AS v").await,
        GqlStatus::NUMERIC_VALUE_OUT_OF_RANGE
    );
    assert_eq!(
        value("UNWIND [9223372036854775807, 1, -1] AS x RETURN sum(x) AS v").await,
        "9223372036854775807"
    );
}

#[tokio::test]
async fn approximate_number_suffixes() {
    // Syntax Rule 19: `F` is the regular and `D` the double approximate type;
    // FLOAT is binary64, so both are FLOAT (GL07 to GL10).
    let rows = table(&run("RETURN 1.5F AS a, 1.5E3d AS b, 5D AS c, 2e-1f AS d").await);
    assert_eq!(rows, [["1.5f", "1500.0f", "5.0f", "0.2f"]]);
    // Syntax Rule 11: no separator before the suffix.
    assert_eq!(status("RETURN 1.5 F AS v").await, GqlStatus::INVALID_SYNTAX);
    // Syntax Rule 20: the value fits its type.
    assert_eq!(status("RETURN 1e400F AS v").await, GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION);
}

// ---- §20.22 numeric value functions ----

#[tokio::test]
async fn char_length_counts_characters() {
    assert_eq!(value("RETURN char_length('héllo') AS v").await, "5");
    assert_eq!(value("RETURN character_length('') AS v").await, "0");
    assert_eq!(value("RETURN char_length(null) AS v").await, "null");
    assert_eq!(status("RETURN char_length(1) AS v").await, GqlStatus::INVALID_VALUE_TYPE);
}

#[tokio::test]
async fn numeric_functions_follow_their_general_rules() {
    assert_eq!(value("RETURN abs(-4) AS v").await, "4");
    assert_eq!(value("RETURN abs(-2.5) AS v").await, "2.5f");
    assert_eq!(value("RETURN mod(-7, 3) AS v").await, "-1");
    assert_eq!(value("RETURN floor(2.7) AS v").await, "2.0f");
    assert_eq!(value("RETURN ceil(2.1) AS v").await, "3.0f");
    assert_eq!(value("RETURN ceiling(-2.1) AS v").await, "-2.0f");
    assert_eq!(value("RETURN floor(3) AS v").await, "3", "an exact argument stays exact");
    assert_eq!(value("RETURN sqrt(16) AS v").await, "4.0f");
    assert_eq!(value("RETURN power(2, 10) AS v").await, "1024.0f");
    assert_eq!(value("RETURN power(-2, 3) AS v").await, "-8.0f");
    assert_eq!(value("RETURN power(0, 0) AS v").await, "1.0f");
    assert_eq!(value("RETURN log10(1000) AS v").await, "3.0f");
    assert_eq!(value("RETURN log(2, 8) AS v").await, "3.0f");
    assert_eq!(value("RETURN ln(1) AS v").await, "0.0f");
    assert_eq!(value("RETURN degrees(0) AS v").await, "0.0f");
    assert_eq!(value("RETURN abs(null) AS v").await, "null");
}

#[tokio::test]
async fn numeric_function_domain_errors() {
    assert_eq!(status("RETURN ln(0) AS v").await, GqlStatus::INVALID_ARGUMENT_FOR_NATURAL_LOGARITHM);
    assert_eq!(
        status("RETURN power(0, -1) AS v").await,
        GqlStatus::INVALID_ARGUMENT_FOR_POWER_FUNCTION
    );
    assert_eq!(
        status("RETURN power(-8, 0.5) AS v").await,
        GqlStatus::INVALID_ARGUMENT_FOR_POWER_FUNCTION
    );
    assert_eq!(status("RETURN sqrt(-1) AS v").await, GqlStatus::INVALID_ARGUMENT_FOR_POWER_FUNCTION);
    assert_eq!(
        status("RETURN log(1, 5) AS v").await,
        GqlStatus::INVALID_ARGUMENT_FOR_GENERAL_LOGARITHM_FUNCTION
    );
    assert_eq!(
        status("RETURN log10(-1) AS v").await,
        GqlStatus::INVALID_ARGUMENT_FOR_GENERAL_LOGARITHM_FUNCTION
    );
    assert_eq!(
        status("RETURN acos(2) AS v").await,
        GqlStatus::INVALID_ARGUMENT_FOR_TRIGONOMETRIC_FUNCTION
    );
    assert_eq!(status("RETURN mod(1, 0) AS v").await, GqlStatus::DIVISION_BY_ZERO);
    assert_eq!(status("RETURN exp(1000) AS v").await, GqlStatus::NUMERIC_VALUE_OUT_OF_RANGE);
    assert_eq!(
        status("RETURN abs(-9223372036854775807 - 1) AS v").await,
        GqlStatus::NUMERIC_VALUE_OUT_OF_RANGE
    );
}

#[tokio::test]
async fn cardinality_counts_a_list() {
    // General Rule 4: the number of elements; null for null.
    assert_eq!(value("RETURN cardinality([1, 2, 3]) AS v").await, "3");
    assert_eq!(value("RETURN cardinality([]) AS v").await, "0");
    assert_eq!(value("RETURN cardinality(NULL) AS v").await, "null");
    // Syntax Rule 3: a list, path, record or binding table; the engine finds
    // a string while running.
    assert_eq!(status("RETURN cardinality('ab') AS v").await, GqlStatus::INVALID_VALUE_TYPE);
}

// ---- §20.23 string value expressions ----

#[tokio::test]
async fn character_string_predicates_match_prefix_suffix_and_substrings() {
    assert_eq!(value("RETURN 'alphabet' STARTS WITH 'alpha' AS v").await, "true");
    assert_eq!(value("RETURN 'alphabet' ENDS WITH 'bet' AS v").await, "true");
    assert_eq!(value("RETURN 'alphabet' CONTAINS 'pha' AS v").await, "true");
    assert_eq!(value("RETURN 'alphabet' CONTAINS 'xyz' AS v").await, "false");
    assert_eq!(value("RETURN '😀é' CONTAINS 'é' AS v").await, "true");
    assert_eq!(value("RETURN 'alphabet' ENDS WITH '' AS v").await, "true");
}

#[tokio::test]
async fn character_string_predicates_filter_stored_properties() {
    assert_eq!(
        table(&run("MATCH (i:Item) WHERE i.name CONTAINS 'an' RETURN i.name AS name ORDER BY name").await),
        rows![["\"Durian\""], ["\"banana\""]]
    );
    assert_eq!(
        table(&run("MATCH (i:Item) WHERE i.name ENDS WITH 'ot' RETURN i.name AS name").await),
        rows![["\"carrot\""]]
    );
}

#[tokio::test]
async fn character_string_predicates_preserve_unknown() {
    for operator in ["STARTS WITH", "ENDS WITH", "CONTAINS"] {
        assert_eq!(value(&format!("RETURN null {operator} 'x' AS v")).await, "null");
        assert_eq!(value(&format!("RETURN 'x' {operator} null AS v")).await, "null");
        assert_eq!(value(&format!("RETURN (null = 1) {operator} 'x' AS v")).await, "null");
        assert_eq!(value(&format!("RETURN 'x' {operator} (null = 1) AS v")).await, "null");
    }
    assert_eq!(value("RETURN ('x' CONTAINS null) ENDS WITH 'x' AS v").await, "null");
}

#[tokio::test]
async fn character_string_predicates_require_strings() {
    for operator in ["STARTS WITH", "ENDS WITH", "CONTAINS"] {
        assert_eq!(
            status(&format!("RETURN 1 {operator} 'x' AS v")).await,
            GqlStatus::INVALID_VALUE_TYPE
        );
        assert_eq!(
            status(&format!("RETURN 'x' {operator} 1 AS v")).await,
            GqlStatus::INVALID_VALUE_TYPE
        );
    }
}

#[tokio::test]
async fn trim_from_needs_a_side_or_a_character() {
    // §20.24 Syntax Rule 3 (GF06).
    assert_eq!(
        status("RETURN trim(FROM ' a ') AS v").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    assert_eq!(value("RETURN trim(BOTH FROM ' a ') AS v").await, "\"a\"");
}

#[tokio::test]
async fn character_strings_concatenate() {
    assert_eq!(value("RETURN 'ab' || 'cd' || '' AS v").await, "\"abcd\"");
    assert_eq!(value("RETURN 'ab' || null AS v").await, "null");
    assert_eq!(status("RETURN 'ab' || 1 AS v").await, GqlStatus::INVALID_VALUE_TYPE);
    let rows = table(&run("MATCH (i:Item) WHERE i.name IS NOT NULL RETURN i.name || '!' AS v ORDER BY v").await);
    assert_eq!(rows[0], ["\"Durian!\""]);
}

// ---- §20.24 character string functions ----

#[tokio::test]
async fn fold_changes_case_by_unicode_mapping() {
    assert_eq!(value("RETURN upper('straße') AS v").await, "\"STRASSE\"");
    assert_eq!(value("RETURN lower('ÀB') AS v").await, "\"àb\"");
    assert_eq!(value("RETURN upper(null) AS v").await, "null");
}

#[tokio::test]
async fn left_and_right_take_characters() {
    assert_eq!(value("RETURN left('héllo', 2) AS v").await, "\"hé\"");
    assert_eq!(value("RETURN right('héllo', 3) AS v").await, "\"llo\"");
    assert_eq!(value("RETURN left('ab', 10) AS v").await, "\"ab\"");
    assert_eq!(value("RETURN right('ab', 0) AS v").await, "\"\"");
    assert_eq!(value("RETURN left(null, 1) AS v").await, "null");
    assert_eq!(status("RETURN left('ab', -1) AS v").await, GqlStatus::SUBSTRING_ERROR);
}

#[tokio::test]
async fn trim_removes_spaces_only() {
    assert_eq!(value("RETURN trim('  a b  ') AS v").await, "\"a b\"");
    assert_eq!(
        value("RETURN trim('\\ta ') AS v").await,
        "\"\\ta\"",
        "GQL trims the space character, not every whitespace"
    );
    assert_eq!(value("RETURN trim(LEADING 'x' FROM 'xxaxx') AS v").await, "\"axx\"");
    assert_eq!(value("RETURN trim(TRAILING 'x' FROM 'xxaxx') AS v").await, "\"xxa\"");
    assert_eq!(value("RETURN trim(BOTH FROM '  a  ') AS v").await, "\"a\"");
    assert_eq!(status("RETURN trim('xy' FROM 'xya') AS v").await, GqlStatus::TRIM_ERROR);
    assert_eq!(value("RETURN ltrim('xyxab', 'xy') AS v").await, "\"ab\"");
    assert_eq!(value("RETURN rtrim('abyx', 'xy') AS v").await, "\"ab\"");
    assert_eq!(value("RETURN btrim('  ab  ') AS v").await, "\"ab\"");
}

#[tokio::test]
async fn normalize_converts_between_normal_forms() {
    // U+00E9 is NFC; e + U+0301 is its NFD.
    assert_eq!(value("RETURN char_length(normalize('e\\u0301')) AS v").await, "1");
    assert_eq!(value("RETURN char_length(normalize('\\u00e9', NFD)) AS v").await, "2");
    assert_eq!(value("RETURN normalize('\\uFB01', NFKC) AS v").await, "\"fi\"");
    assert_eq!(value("RETURN normalize(null) AS v").await, "null");
}

// ---- §20.7 case abbreviations ----

#[tokio::test]
async fn nullif_and_coalesce() {
    assert_eq!(value("RETURN nullif(1, 1) AS v").await, "null");
    assert_eq!(value("RETURN nullif(1, 2) AS v").await, "1");
    assert_eq!(value("RETURN coalesce(null, nullif('a', 'a'), 'b') AS v").await, "\"b\"");
}

// ---- §20.20 boolean value expressions ----

#[tokio::test]
async fn boolean_tests_are_two_valued() {
    for (query, expected) in [
        ("RETURN (1 = 1) IS TRUE AS v", "true"),
        ("RETURN (1 = 2) IS TRUE AS v", "false"),
        ("RETURN null IS TRUE AS v", "false"),
        ("RETURN null IS NOT TRUE AS v", "true"),
        ("RETURN (1 = 2) IS FALSE AS v", "true"),
        ("RETURN null IS FALSE AS v", "false"),
        ("RETURN (1 = null) IS UNKNOWN AS v", "true"),
        ("RETURN true IS UNKNOWN AS v", "false"),
        ("RETURN false IS NOT UNKNOWN AS v", "true"),
    ] {
        assert_eq!(value(query).await, expected, "{query}");
    }
    assert_eq!(status("RETURN 1 IS TRUE AS v").await, GqlStatus::INVALID_VALUE_TYPE);
}

#[tokio::test]
async fn unknown_is_the_null_boolean() {
    assert_eq!(value("RETURN UNKNOWN AS v").await, "null");
    assert_eq!(value("RETURN (true AND UNKNOWN) IS UNKNOWN AS v").await, "true");
    assert_eq!(value("RETURN (false AND UNKNOWN) AS v").await, "false");
    assert_eq!(value("RETURN (true OR UNKNOWN) AS v").await, "true");
}

/// Cypher lets `unknown` name a variable; a query that declares one keeps it.
#[tokio::test]
async fn a_declared_unknown_variable_is_still_a_variable() {
    let rows = table(&run("MATCH (unknown:Item {name: 'banana'}) RETURN unknown.score AS v").await);
    assert_eq!(rows, [["1"]]);
}

#[tokio::test]
async fn three_valued_logic_filters_only_on_true() {
    let rows = table(&run("MATCH (i:Item) WHERE (i.score > 2) IS NOT FALSE RETURN i.score AS s ORDER BY s").await);
    // score > 2 is false for 0, 1, 2; unknown for the item with no score.
    assert_eq!(rows, [["3"], ["5"], ["null"]]);
}

#[tokio::test]
async fn xor_is_or_without_and() {
    // Syntax Rule 4: `X XOR Y` is `(X OR Y) AND NOT (X AND Y)`, so it is
    // unknown whenever either operand is.
    let rows = table(
        &run("RETURN true XOR true AS a, true XOR false AS b, false XOR false AS c, true XOR UNKNOWN AS d, false XOR UNKNOWN AS e").await,
    );
    assert_eq!(rows, [["false", "true", "false", "null", "null"]]);
    // XOR shares OR's level (the Format): `true OR false XOR true` is
    // `(true OR false) XOR true`, and AND binds tighter than both.
    assert_eq!(value("RETURN true OR false XOR true AS v").await, "false");
    assert_eq!(value("RETURN true XOR true AND false AS v").await, "true");
}

// ---- §19.7 normalized predicate ----

#[tokio::test]
async fn normalized_predicate() {
    assert_eq!(value("RETURN '\\u00e9' IS NORMALIZED AS v").await, "true");
    assert_eq!(value("RETURN 'e\\u0301' IS NORMALIZED AS v").await, "false");
    assert_eq!(value("RETURN 'e\\u0301' IS NFD NORMALIZED AS v").await, "true");
    assert_eq!(value("RETURN '\\u00e9' IS NOT NFKD NORMALIZED AS v").await, "true");
    assert_eq!(value("RETURN null IS NORMALIZED AS v").await, "null");
}

// ---- §19.8 to §19.13 element predicates ----

#[tokio::test]
async fn element_predicates_read_the_bound_element() {
    let invalid = GqlStatus::INVALID_VALUE_TYPE;
    // §19.8: every HydraDB edge is directed; a null element is unknown.
    assert_eq!(value("MATCH ()-[e:R]->() WHERE e IS DIRECTED RETURN count(*) AS c").await, "6");
    assert_eq!(value("MATCH ()-[e:R]->() WHERE e IS NOT DIRECTED RETURN count(*) AS c").await, "0");
    assert_eq!(
        value("MATCH (n:Hub) OPTIONAL MATCH (n)-[e:R]->() RETURN e IS DIRECTED AS d").await,
        "null"
    );
    // A backticked name is still a label test.
    assert_eq!(value("MATCH ()-[e:R]->() WHERE e IS `DIRECTED` RETURN count(*) AS c").await, "0");
    // §19.9: `x IS LABELED L`, `x:L` and its negation.
    assert_eq!(value("MATCH (n) WHERE n IS LABELED Node RETURN count(*) AS c").await, "5");
    assert_eq!(value("MATCH (n) WHERE n:Hub|Node RETURN count(*) AS c").await, "6");
    assert_eq!(value("MATCH (n:Item) WHERE n IS NOT LABELED Item RETURN count(*) AS c").await, "0");
    assert_eq!(value("MATCH ()-[e]->() WHERE e IS LABELED HAS RETURN count(*) AS c").await, "3");
    // §19.10: the node at each end of an edge, as stored. The self-loop's
    // node is both.
    assert_eq!(value("MATCH (a)-[e:R]->(b) WHERE a IS SOURCE OF e RETURN count(*) AS c").await, "6");
    assert_eq!(value("MATCH (a)-[e:R]->(b) WHERE b IS SOURCE OF e RETURN count(*) AS c").await, "1");
    assert_eq!(
        value("MATCH (a)<-[e:R]-(b) WHERE a IS DESTINATION OF e RETURN count(*) AS c").await,
        "6"
    );
    assert_eq!(
        value("MATCH (a)-[e:R]->(b) WHERE a IS NOT DESTINATION OF e RETURN count(*) AS c").await,
        "5"
    );
    // §19.13: whether the element has the property; every node has `id`.
    assert_eq!(
        value("MATCH (i:Item) WHERE PROPERTY_EXISTS(i, score) RETURN count(*) AS c").await,
        "5"
    );
    assert_eq!(value("MATCH (i:Item) WHERE PROPERTY_EXISTS(i, id) RETURN count(*) AS c").await, "6");
    assert_eq!(
        value("MATCH ()-[e:HAS]->() WHERE PROPERTY_EXISTS(e, weight) RETURN count(*) AS c").await,
        "3"
    );
    // Each predicate takes an element of its kind. The engine types values
    // as it evaluates them, so the wrong kind raises `22G03` rather than the
    // `42000` of §19.8 Syntax Rule 3 and §19.10 Syntax Rules 2 and 3.
    assert_eq!(status("MATCH (n:Hub) RETURN n IS DIRECTED AS d").await, invalid);
    assert_eq!(status("MATCH (a)-[e:R]->(b) RETURN e IS SOURCE OF e AS s").await, invalid);
    assert_eq!(status("MATCH (a)-[e:R]->(b) RETURN a IS SOURCE OF b AS s").await, invalid);
    assert_eq!(
        status("MATCH (i:Item) WITH i.name AS x RETURN PROPERTY_EXISTS(x, name) AS p").await,
        invalid
    );
}

#[tokio::test]
async fn all_different_and_same_compare_elements() {
    // §19.11 and §19.12: identity, not equality of properties. The two
    // `apple` items are different elements.
    assert_eq!(
        value("MATCH (a)-[:R]->(b) WHERE ALL_DIFFERENT(a, b) RETURN count(*) AS c").await,
        "5"
    );
    assert_eq!(value("MATCH (a)-[:R]->(b) WHERE SAME(a, b) RETURN count(*) AS c").await, "1");
    assert_eq!(
        value("MATCH (x:Item {name: 'apple'}), (y:Item {name: 'apple'}) WHERE SAME(x, y) RETURN count(*) AS c").await,
        "2"
    );
    // Six two-hop paths: the self-loop cannot reuse its one edge.
    assert_eq!(
        value("MATCH (a)-[:R]->(b)-[:R]->(c) WHERE ALL_DIFFERENT(a, b, c) RETURN count(*) AS c").await,
        "6"
    );
    // General Rule 2 a) and b): a null element raises `22004`, a node beside
    // an edge `22G04`.
    assert_eq!(
        status("MATCH (n:Hub) OPTIONAL MATCH (n)-[:R]->(m) RETURN SAME(n, m) AS s").await,
        GqlStatus::NULL_VALUE_NOT_ALLOWED
    );
    assert_eq!(
        status("MATCH (a)-[e:R]->(b) RETURN ALL_DIFFERENT(a, e) AS d").await,
        GqlStatus::VALUES_NOT_COMPARABLE
    );
    // The Format takes element variable references, not expressions.
    assert_eq!(
        status("MATCH (a)-[:R]->(b) RETURN SAME(a, b.name) AS s").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
}

// ---- §20.10 element_id function ----

#[tokio::test]
async fn element_id_names_an_element() {
    // General Rule 2: a value that stands for the element's identity within
    // the request, so it is equal exactly when the elements are the same.
    assert_eq!(value("MATCH (n:Hub) RETURN element_id(n) = element_id(n) AS v").await, "true");
    assert_eq!(
        value("MATCH (x:Item {name: 'apple'}), (y:Item {name: 'apple'}) WHERE element_id(x) = element_id(y) RETURN count(*) AS c").await,
        "2"
    );
    assert_eq!(
        value("MATCH (a)-[e:R]->(a) RETURN element_id(a) = element_id(e) AS v").await,
        "false"
    );
    assert_eq!(
        value("MATCH (n:Hub) OPTIONAL MATCH (n)-[:R]->(m) RETURN element_id(m) AS v").await,
        "null"
    );
    assert_eq!(
        status("MATCH (n:Hub) RETURN element_id(n.name) AS v").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
}

// ---- §16.8 label expressions, §22.5 satisfaction ----

#[tokio::test]
async fn node_label_expressions() {
    let names = |query| async move { table(&run(query).await).into_iter().map(|row| row[0].clone()).collect::<Vec<_>>() };
    assert_eq!(
        names("MATCH (n:Hub|Node) RETURN n.name AS name ORDER BY name").await,
        ["\"a\"", "\"b\"", "\"c\"", "\"d\"", "\"hub\"", "\"loop\""]
    );
    assert_eq!(
        names("MATCH (n IS !Item&!Node) RETURN n.name AS name").await,
        ["\"hub\""],
        "IS spelling, negation and conjunction"
    );
    assert_eq!(names("MATCH (n:(Hub|Node)&!Node) RETURN n.name AS name").await, ["\"hub\""]);
    assert_eq!(
        names("MATCH (:Hub)-[]->(n:!Node) WHERE n.score > 2 RETURN n.name AS name").await,
        ["\"apple\""]
    );
    assert_eq!(
        table(&run("MATCH (n:%) RETURN count(*) AS c").await),
        [["12"]],
        "every vertex here has a label"
    );
}

#[tokio::test]
async fn edge_label_expressions() {
    assert_eq!(table(&run("MATCH (:Hub)-[e:!R]->(i) RETURN count(e) AS c").await), [["3"]]);
    assert_eq!(table(&run("MATCH ()-[e:HAS|R]->() RETURN count(*) AS c").await), [["9"]]);
    assert_eq!(
        table(&run("MATCH ()-[e:HAS&R]->() RETURN count(*) AS c").await),
        [["0"]],
        "an edge has one type, so no edge has two"
    );
    assert_eq!(
        table(&run("MATCH (a)-[:!HAS]->(b:Node {name: 'd'}) RETURN a.name AS a ORDER BY a").await),
        [["\"b\""], ["\"c\""]]
    );
}

// ---- §20.7 case expressions ----

#[tokio::test]
async fn simple_case_with_when_operand_lists_and_predicates() {
    let rows = table(
        &run("MATCH (i:Item) RETURN CASE i.score WHEN 0, 1 THEN 'low' WHEN > 2 THEN 'high' WHEN IS NULL THEN 'none' ELSE 'mid' END AS band, i.score AS s ORDER BY s")
            .await,
    );
    assert_eq!(
        rows,
        [
            ["\"low\"", "0"],
            ["\"low\"", "1"],
            ["\"mid\"", "2"],
            ["\"high\"", "3"],
            ["\"high\"", "5"],
            ["\"none\"", "null"]
        ]
    );
    assert_eq!(value("RETURN CASE 'x' WHEN 'y' THEN 1 WHEN 'x' THEN 2 END AS v").await, "2");
    assert_eq!(value("RETURN CASE 3 WHEN <> 3 THEN 1 END AS v").await, "null");
}

// ---- §19.4 exists predicate ----

#[tokio::test]
async fn exists_with_a_graph_pattern_is_correlated() {
    // Items the hub points at: apple(3), banana(1), carrot(2).
    let rows = table(&run("MATCH (i:Item) WHERE EXISTS { (:Hub)-[:HAS]->(i) } RETURN i.score AS s ORDER BY s").await);
    assert_eq!(rows, [["1"], ["2"], ["3"]]);
    let rows = table(&run("MATCH (i:Item) WHERE NOT EXISTS { (:Hub)-[:HAS]->(i) } RETURN i.score AS s ORDER BY s").await);
    assert_eq!(rows, [["0"], ["5"], ["null"]]);
}

#[tokio::test]
async fn exists_with_a_match_statement_and_where() {
    let rows =
        table(&run("MATCH (i:Item) WHERE EXISTS { MATCH (h:Hub)-[e:HAS]->(i) WHERE e.weight >= 2 } RETURN i.score AS s ORDER BY s").await);
    assert_eq!(rows, [["2"], ["3"]]);
}

#[tokio::test]
async fn exists_with_a_nested_query_specification() {
    let rows =
        table(&run("MATCH (n:Node) WHERE EXISTS { MATCH (n)-[:R]->(m:Node) RETURN m.name } RETURN n.name AS name ORDER BY name").await);
    assert_eq!(rows, [["\"a\""], ["\"b\""], ["\"c\""], ["\"d\""], ["\"loop\""]]);
}

#[tokio::test]
async fn exists_is_a_value_in_a_return_item() {
    let rows = table(&run("MATCH (n:Node) RETURN n.name AS name, EXISTS { (n)-[:R]->(n) } AS self_loop ORDER BY name").await);
    assert_eq!(
        rows,
        [
            ["\"a\"", "false"],
            ["\"b\"", "false"],
            ["\"c\"", "false"],
            ["\"d\"", "false"],
            ["\"loop\"", "true"]
        ]
    );
}

/// An anonymous node inside the subquery is not the enclosing query's
/// anonymous node, though both are unnamed.
#[tokio::test]
async fn subquery_anonymous_names_do_not_collide_with_the_outer_query() {
    let rows = table(&run("MATCH (a:Node)-[:R]->() WHERE EXISTS { (a)<-[:R]-() } RETURN a.name AS name ORDER BY name").await);
    // a <- d, b <- a, c <- a, d <- b/c, loop <- loop; all of them have an
    // outgoing R too.
    assert_eq!(rows, [["\"a\""], ["\"a\""], ["\"b\""], ["\"c\""], ["\"d\""], ["\"loop\""]]);
}

#[tokio::test]
async fn exists_in_filter() {
    let rows = table(&run("MATCH (n:Node) FILTER EXISTS { (n)-[:R]->(:Node {name: 'd'}) } RETURN n.name AS name ORDER BY name").await);
    assert_eq!(rows, [["\"b\""], ["\"c\""]]);
}

// ---- §14.2 composite query expression (GQ03) ----

#[tokio::test]
async fn union_arms_return_the_same_column_names() {
    // Syntax Rule 10: the arms are column name-equal.
    assert_eq!(
        status("MATCH (n:Hub) RETURN n.name AS a UNION MATCH (n:Node) RETURN n.name AS b").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
}

// ---- §14.2 composite query expression: EXCEPT, INTERSECT, OTHERWISE ----

/// A query's first column, sorted: a composite query's order is not defined.
async fn sorted_column(query: &str) -> Vec<String> {
    let mut rows: Vec<String> = table(&run(query).await).into_iter().map(|row| row[0].clone()).collect();
    rows.sort();
    rows
}

#[tokio::test]
async fn except_and_intersect_count_duplicates_as_general_rule_2_says() {
    // The kinds are fruit four times and veg twice; the carrot is veg, and
    // the items scoring over 1 are one fruit and two veg.
    const KINDS: &str = "MATCH (i:Item) RETURN i.kind AS k";
    const CARROT: &str = "MATCH (i:Item {name: 'carrot'}) RETURN i.kind AS k";
    let (fruit, veg) = ("\"fruit\"".to_string(), "\"veg\"".to_string());
    assert_eq!(
        sorted_column(&format!("{KINDS} EXCEPT {CARROT}")).await,
        std::slice::from_ref(&fruit)
    );
    assert_eq!(
        sorted_column(&format!("{KINDS} EXCEPT ALL {CARROT}")).await,
        [fruit.clone(), fruit.clone(), fruit.clone(), fruit.clone(), veg.clone()]
    );
    assert_eq!(
        sorted_column(&format!("{KINDS} INTERSECT {CARROT}")).await,
        std::slice::from_ref(&veg)
    );
    assert_eq!(
        sorted_column(&format!(
            "{KINDS} INTERSECT ALL MATCH (i:Item) WHERE i.score > 1 RETURN i.kind AS k"
        ))
        .await,
        [fruit.clone(), veg.clone(), veg]
    );
    // Left-associative: (kinds EXCEPT veg) EXCEPT fruit.
    assert!(sorted_column(&format!(
        "{KINDS} EXCEPT {CARROT} EXCEPT MATCH (i:Item {{name: 'apple'}}) RETURN i.kind AS k"
    ))
    .await
    .is_empty());
}

#[tokio::test]
async fn otherwise_answers_its_first_arm_with_rows() {
    // General Rule 2 a) iii) 3).
    assert_eq!(
        value("MATCH (n:Missing) RETURN n.name AS v OTHERWISE MATCH (n:Hub) RETURN n.name AS v").await,
        "\"hub\""
    );
    assert_eq!(
        value("MATCH (n:Hub) RETURN n.name AS v OTHERWISE MATCH (n:Node {name: 'a'}) RETURN n.name AS v").await,
        "\"hub\""
    );
}

#[tokio::test]
async fn a_composite_query_uses_one_conjunction_and_one_column_set() {
    let syntax = GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION;
    // Syntax Rule 3: every conjunction of one composite query is the same.
    assert_eq!(
        status("MATCH (n:Hub) RETURN n.name AS v UNION MATCH (n:Hub) RETURN n.name AS v EXCEPT MATCH (n:Hub) RETURN n.name AS v").await,
        syntax
    );
    assert_eq!(
        status("MATCH (n:Hub) RETURN n.name AS v EXCEPT MATCH (n:Hub) RETURN n.name AS v EXCEPT ALL MATCH (n:Hub) RETURN n.name AS v")
            .await,
        syntax
    );
    // Syntax Rule 10 a) v): the arms are column name-equal.
    assert_eq!(
        status("MATCH (n:Hub) RETURN n.name AS v INTERSECT MATCH (n:Hub) RETURN n.name AS w").await,
        syntax
    );
    // Syntax Rule 5: FINISH stands in a composite query of one primary.
    assert_eq!(status("MATCH (n:Hub) FINISH UNION MATCH (n:Hub) FINISH").await, syntax);
}

// ---- §14.4 match statement: OPTIONAL { } (GQ21) ----

#[tokio::test]
async fn an_optional_block_matches_as_one() {
    // Syntax Rule 4 f) ii): the block's statements match together, and the
    // row survives with every name they bind null when they do not.
    let rows = table(
        &run("MATCH (n:Node {name: 'a'}) OPTIONAL { MATCH (n)-[:R]->(m) MATCH (m)-[:R]->(o) } RETURN m.name AS m, o.name AS o ORDER BY m")
            .await,
    );
    assert_eq!(rows, [["\"b\"", "\"d\""], ["\"c\"", "\"d\""]]);
    // `b` reaches `d`, but `d` reaches no Item: the whole block misses, so
    // `m` is null too, where two OPTIONAL MATCHes would keep it.
    let rows = table(
        &run("MATCH (n:Node {name: 'b'}) OPTIONAL ( MATCH (n)-[:R]->(m) MATCH (m)-[:R]->(o:Item) ) RETURN n.name AS n, m.name AS m").await,
    );
    assert_eq!(rows, [["\"b\"", "null"]]);
    let rows = table(
        &run("MATCH (n:Node {name: 'b'}) OPTIONAL MATCH (n)-[:R]->(m) OPTIONAL MATCH (m)-[:R]->(o:Item) RETURN n.name AS n, m.name AS m")
            .await,
    );
    assert_eq!(rows, [["\"b\"", "\"d\""]]);
    // A WHERE inside the block filters the block, not the outer rows.
    let rows =
        table(&run("MATCH (n:Node {name: 'a'}) OPTIONAL { MATCH (n)-[:R]->(m) WHERE m.name = 'z' } RETURN n.name AS n, m.name AS m").await);
    assert_eq!(rows, [["\"a\"", "null"]]);
}

// ---- §14.8 for statement: WITH ORDINALITY and WITH OFFSET ----

#[tokio::test]
async fn for_numbers_its_elements() {
    // General Rule 2 c) ii): ORDINALITY counts from 1, OFFSET from 0.
    assert_eq!(
        table(&run("FOR x IN ['p', 'q'] WITH ORDINALITY i RETURN x, i").await),
        [["\"p\"", "1"], ["\"q\"", "2"]]
    );
    assert_eq!(
        table(&run("FOR x IN ['p', 'q'] WITH OFFSET i RETURN x, i").await),
        [["\"p\"", "0"], ["\"q\"", "1"]]
    );
    // Syntax Rule 10 c) i) 2): the position name differs from the element's,
    // and Syntax Rule 11: both are new.
    let syntax = GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION;
    assert_eq!(status("FOR x IN [1] WITH OFFSET x RETURN x").await, syntax);
    assert_eq!(status("MATCH (n:Hub) FOR x IN [1] WITH ORDINALITY n RETURN x").await, syntax);
}

// ---- §14.10 primitive result statement: FINISH (GQ27) ----

#[tokio::test]
async fn finish_omits_the_result() {
    // General Rule 2: a successful outcome with an omitted result, whether or
    // not the statement matched anything.
    for query in ["MATCH (n:Hub) FINISH", "MATCH (n:Missing) FINISH"] {
        let outcome = outcome(query).await;
        assert_eq!(outcome.status.status, GqlStatus::OMITTED_RESULT, "{query}");
        assert!(outcome.result.columns.is_empty() && outcome.result.rows.is_empty(), "{query}");
    }
}

// ---- §9.2 procedure body: NEXT (GQ20) ----

#[tokio::test]
async fn next_runs_over_the_table_before_it() {
    // General Rule 3 a): the next statement's working table is the result.
    let rows = table(&run("MATCH (n:Hub) RETURN n.name AS x NEXT MATCH (m:Node {name: 'a'}) RETURN x, m.name AS y").await);
    assert_eq!(rows, [["\"hub\"", "\"a\""]]);
    assert_eq!(
        value("MATCH (i:Item) RETURN i.kind AS k, count(*) AS c GROUP BY k NEXT FILTER c > 2 RETURN k").await,
        "\"fruit\""
    );
    // Syntax Rule 7: every statement of the block has a result statement.
    assert_eq!(
        status("MATCH (n:Hub) NEXT RETURN 1 AS v").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
}

#[tokio::test]
async fn next_keyword_columns_resolve_in_the_current_statement_scope() {
    // §9.2 SR12/GR3 passes the result table; §14.11 SR8a names a bare
    // binding reference. UNKNOWN/SESSION_USER identifiers are the existing
    // Cypher extension; delimited names are always binding references (§21.3).
    for (query, expected) in [
        ("WITH 1 AS unknown RETURN unknown NEXT RETURN unknown AS v", "1"),
        ("WITH 2 AS session_user RETURN session_user NEXT RETURN session_user AS v", "2"),
        ("WITH 3 AS integer RETURN integer NEXT RETURN integer AS v", "3"),
        ("WITH 4 AS boolean RETURN boolean NEXT RETURN boolean AS v", "4"),
        ("WITH 4 AS typed RETURN typed NEXT RETURN typed AS v", "4"),
        ("WITH 5 AS `unknown` RETURN `unknown` NEXT RETURN `unknown` AS v", "5"),
        ("WITH 6 AS unknown RETURN UNKNOWN NEXT RETURN unknown AS v", "6"),
        ("WITH 1 AS unknown, 7 AS UNKNOWN RETURN UNKNOWN NEXT RETURN UNKNOWN AS v", "7"),
        ("LET unknown = 8 RETURN unknown NEXT RETURN unknown AS v", "8"),
        (
            "MATCH (unknown:Hub) RETURN unknown NEXT MATCH (unknown)-[:HAS]->(n) RETURN count(n) AS v",
            "3",
        ),
        (
            "MATCH (:Hub)-[session_user:HAS]->() RETURN session_user NEXT RETURN sum(session_user.weight) AS v",
            "6",
        ),
        (
            "WITH SESSION_USER AS unknown RETURN unknown NEXT RETURN unknown AS v",
            "\"anonymous\"",
        ),
        ("RETURN 1 AS unknown NEXT RETURN unknown NEXT RETURN unknown AS v", "1"),
        ("RETURN UNKNOWN, 1 AS unknown NEXT RETURN unknown AS v", "1"),
        (
            "WITH 1 AS unknown RETURN unknown NEXT WITH unknown + 1 AS unknown RETURN unknown NEXT RETURN unknown AS v",
            "2",
        ),
        (
            "WITH 1 AS unknown RETURN unknown NEXT RETURN EXISTS { FILTER unknown = 1 RETURN 1 AS x } AS v",
            "true",
        ),
        ("WITH 1 AS unknown RETURN unknown NEXT RETURN unknown NEXT RETURN unknown AS v", "1"),
    ] {
        assert_eq!(value(query).await, expected, "{query}");
    }
    assert_eq!(
        table(&run("WITH 1 AS unknown RETURN unknown AS kept, 2 AS unknown NEXT RETURN unknown AS v, kept AS k").await),
        [["2", "1"]]
    );
    assert_eq!(
        table(&run("RETURN UNKNOWN AS original, 1 AS unknown NEXT RETURN original, unknown").await),
        [["null", "1"]]
    );
    assert_eq!(
        table(
            &run("WITH 1 AS session_user RETURN session_user AS old, SESSION_USER + 1 AS session_user NEXT RETURN old, session_user").await
        ),
        [["1", "2"]]
    );
    for query in [
        "FOR unknown IN [2,1,2] RETURN DISTINCT unknown ORDER BY unknown NEXT RETURN unknown AS v",
        "FOR unknown IN [2,1,2] RETURN unknown GROUP BY unknown ORDER BY unknown NEXT RETURN unknown AS v",
    ] {
        assert_eq!(table(&run(query).await), [["1"], ["2"]], "{query}");
    }
}

#[tokio::test]
async fn next_keyword_scope_preserves_headers_nulls_and_empty_tables() {
    for (source, rows, completion) in [
        ("WITH 1 AS unknown", vec![vec!["1"]], GqlStatus::SUCCESS),
        ("WITH null AS unknown", vec![vec!["null"]], GqlStatus::SUCCESS),
        ("OPTIONAL { MATCH (unknown:Missing) }", vec![vec!["null"]], GqlStatus::SUCCESS),
        ("FOR unknown IN []", Vec::new(), GqlStatus::NO_DATA),
    ] {
        let query = format!("{source} RETURN unknown NEXT RETURN *");
        let result = outcome(&query).await;
        assert_eq!(
            result.result.columns.iter().map(|column| column.name.as_str()).collect::<Vec<_>>(),
            ["unknown"],
            "{query}"
        );
        assert_eq!(table(&result.result), rows, "{query}");
        assert_eq!(result.status.status, completion, "{query}");
    }
    assert_eq!(
        table(&run("OPTIONAL { MATCH (session_user:Hub) } RETURN session_user NEXT RETURN session_user.name AS v").await),
        [["\"hub\""]]
    );
    let result = run("WITH 1 AS unknown, 2 AS session_user RETURN UNKNOWN, SESSION_USER NEXT RETURN *").await;
    assert_eq!(
        result.columns.iter().map(|column| column.name.as_str()).collect::<Vec<_>>(),
        ["session_user", "unknown"]
    );
    assert_eq!(table(&result), [["2", "1"]]);
}

#[tokio::test]
async fn next_unbound_keywords_remain_values_and_unsupported_scopes_stay_refused() {
    for (query, expected) in [
        ("RETURN UNKNOWN NEXT RETURN UNKNOWN AS v", "null"),
        ("RETURN SESSION_USER NEXT RETURN SESSION_USER AS v", "\"anonymous\""),
        ("RETURN UNKNOWN AS unknown NEXT RETURN unknown AS v", "null"),
        ("WITH 1 AS unknown WITH 2 AS x RETURN UNKNOWN NEXT RETURN UNKNOWN AS v", "null"),
        ("WITH 1 AS unknown RETURN unknown + 1 NEXT RETURN UNKNOWN AS v", "null"),
        ("WITH 1 AS unknown RETURN unknown AS x NEXT RETURN UNKNOWN AS v", "null"),
        ("WITH 1 AS unknown FINISH NEXT RETURN UNKNOWN AS v", "null"),
        ("WITH 1 AS unknown RETURN unknown NEXT FINISH NEXT RETURN UNKNOWN AS v", "null"),
        ("WITH 1 AS `true` RETURN true NEXT RETURN true AS v", "true"),
        ("WITH 1 AS `null` RETURN null NEXT RETURN null AS v", "null"),
        (
            "RETURN UNKNOWN AS unknown, SESSION_USER AS session_user NEXT RETURN SESSION_USER AS v",
            "\"anonymous\"",
        ),
    ] {
        assert_eq!(value(query).await, expected, "{query}");
    }
    for query in [
        "RETURN UNKNOWN NEXT RETURN `unknown` AS v",
        "RETURN SESSION_USER NEXT RETURN `session_user` AS v",
        "WITH 1 AS unknown RETURN unknown + 1 NEXT RETURN `unknown` AS v",
        "WITH 1 AS unknown FINISH NEXT RETURN `unknown` AS v",
    ] {
        assert_eq!(status(query).await, GqlStatus::INVALID_REFERENCE, "{query}");
    }
    for operator in [
        "UNION",
        "UNION ALL",
        "EXCEPT",
        "EXCEPT ALL",
        "INTERSECT",
        "INTERSECT ALL",
        "OTHERWISE",
    ] {
        let arm = "{ WITH 1 AS unknown RETURN unknown NEXT RETURN unknown AS v }";
        for query in [format!("{arm} {operator} RETURN 2 AS v"), format!("RETURN 2 AS v {operator} {arm}")] {
            assert_eq!(status(&query).await, GqlStatus::FEATURE_NOT_SUPPORTED, "{query}");
        }
    }
    for query in [
        "RETURN EXISTS { WITH 1 AS unknown RETURN unknown NEXT RETURN unknown AS v } AS e",
        "WITH 1 AS unknown RETURN EXISTS { RETURN unknown NEXT RETURN unknown AS v } AS e",
    ] {
        assert_eq!(status(query).await, GqlStatus::FEATURE_NOT_SUPPORTED, "{query}");
    }
    for (operator, expected) in [
        ("UNION", vec![vec!["1"]]),
        ("UNION ALL", vec![vec!["1"], vec!["1"]]),
        ("EXCEPT", Vec::new()),
        ("EXCEPT ALL", Vec::new()),
        ("INTERSECT", vec![vec!["1"]]),
        ("INTERSECT ALL", vec![vec!["1"]]),
        ("OTHERWISE", vec![vec!["1"]]),
    ] {
        let query = format!("WITH 1 AS unknown RETURN unknown AS v {operator} WITH 1 AS session_user RETURN session_user AS v");
        assert_eq!(table(&run(&query).await), expected, "{query}");
    }
    assert_eq!(
        status("WITH 1 AS unknown RETURN unknown NEXT RETURN unknown / 0 AS v").await,
        GqlStatus::DIVISION_BY_ZERO
    );
    assert_eq!(
        status("WITH 1 AS unknown RETURN unknown NEXT MATCH (unknown) RETURN unknown AS v").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    assert_eq!(
        value("RETURN 1 NEXT FINISH NEXT WITH 2 AS __hydradb_next_column_0 RETURN *").await,
        "2"
    );
}

// ---- §20.6 value query expression (GQ18) ----

#[tokio::test]
async fn standalone_sort_scopes_and_hoists_correlated_subqueries() {
    // §14.9 and §16.17: these keys read the input binding table, not a
    // primitive RETURN's output scope (§14.10 SR4).
    let witness = run("MATCH (n) ORDER BY CASE WHEN EXISTS { (n)-[]->() } THEN 1 ELSE 0 END RETURN n").await;
    assert_eq!(witness.rows.len(), 12);
    assert!(witness.rows[..6]
        .iter()
        .all(|row| matches!(row.values[0], QueryValue::VertexId(1000..=1005))));
    assert!(witness.rows[6..]
        .iter()
        .all(|row| matches!(row.values[0], QueryValue::VertexId(1010..=1024))));
    assert_eq!(table(&run("MATCH (n:Node) ORDER BY CASE WHEN EXISTS { (n)-[:R]->(:Node {name: 'd'}) } THEN 1 ELSE 0 END, n.name RETURN n.name AS name").await), [["\"a\""], ["\"d\""], ["\"loop\""], ["\"b\""], ["\"c\""]]);
    assert_eq!(value("MATCH (n:Node) ORDER BY EXISTS { MATCH (n)-[:R]->(:Node {name: 'd'}) RETURN 1 AS x } DESCENDING, n.name OFFSET 1 LIMIT 1 RETURN n.name AS name").await, "\"c\"");
    assert_eq!(
        value("MATCH (n:Node) LET name = n.name ORDER BY EXISTS { MATCH (n)-[:R]->(:Node {name: 'd'}) }, name LIMIT 1 RETURN name").await,
        "\"a\""
    );
    assert_eq!(
        value("MATCH (n:Node) ORDER BY VALUE { RETURN n.name AS name LIMIT 1 } DESCENDING LIMIT 1 RETURN n.name AS name").await,
        "\"loop\""
    );
    assert_eq!(
        value("MATCH (n:Node) ORDER BY EXISTS { WITH 1 AS n RETURN n }, n.name LIMIT 1 RETURN n.name AS name").await,
        "\"a\""
    );
    assert_eq!(value("MATCH (n:Node) ORDER BY CASE WHEN EXISTS { MATCH (m:Hub) FILTER EXISTS { MATCH (m)-[:HAS]->(i) RETURN i } RETURN m } THEN 0 ELSE 1 END, n.name LIMIT 1 RETURN n.name AS name").await, "\"a\"");
    assert_eq!(
        status("MATCH (n:Node) ORDER BY VALUE { RETURN missing AS x LIMIT 1 } RETURN n").await,
        GqlStatus::INVALID_REFERENCE
    );
    assert_eq!(
        status("MATCH (n:Node) ORDER BY EXISTS { FILTER missing > 0 RETURN 1 AS x } RETURN n").await,
        GqlStatus::INVALID_REFERENCE
    );
    // The sort neither projects its marks nor computes RETURN on discarded rows.
    assert_eq!(
        value("FOR x IN [0, 1] ORDER BY EXISTS { FILTER x > 0 RETURN x } DESC LIMIT 1 RETURN 1 / x AS v").await,
        "1"
    );
    assert_eq!(
        value("FOR x IN [0, 1] ORDER BY CASE WHEN x = 0 THEN false ELSE EXISTS { RETURN 1 / x AS v } END DESC LIMIT 1 RETURN x").await,
        "1"
    );
    assert_eq!(
        run("FOR x IN [] ORDER BY EXISTS { RETURN 1 / 0 AS v } RETURN x").await.rows.len(),
        0
    );
    // Primitive RETURN's query-specification restriction remains separate.
    for key in ["VALUE { RETURN 1 AS x }", "EXISTS { RETURN 1 AS x }"] {
        assert_eq!(
            status(&format!("RETURN 1 AS v ORDER BY {key}")).await,
            GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
        );
        assert_eq!(
            status(&format!("RETURN 1 AS v ORDER BY {key} NEXT RETURN v")).await,
            GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
        );
    }
}

#[tokio::test]
async fn next_checks_every_statement_before_capability_refusal() {
    // §9.2 SR7 and §14.3: each query statement has a primitive result.
    let engine = CypherEngine::new(graph());
    for query in [
        "FOR a IN [3, 3] NEXT FOR b IN [7]",
        "FOR a IN [] NEXT FOR b IN [7]",
        "FOR a IN [3, 3] RETURN a AS carried NEXT FOR b IN [7]",
        "FOR a IN [] RETURN a AS carried NEXT FOR b IN [7]",
        "MATCH (n) NEXT RETURN 7 AS v",
        "MATCH (n) UNION MATCH (m) NEXT RETURN 7 AS v",
        "RETURN 7 AS v NEXT MATCH (n) UNION MATCH (m)",
        "RETURN 7 AS v NEXT MATCH (n)",
        "RETURN EXISTS { MATCH (n) NEXT RETURN 7 AS v } AS v",
        "DEFINE FUNCTION f() = 1 MATCH (n) NEXT RETURN 7 AS v",
        "RETURN missing_function() AS v NEXT MATCH (n)",
        "WHEN true THEN MATCH (n) ELSE RETURN 1 AS v NEXT RETURN 7 AS v",
    ] {
        assert_eq!(
            engine.prepare(query, &ParameterMap::new()).unwrap_err().gql_status(),
            GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "{query}"
        );
    }
    for query in [
        "RETURN 1 AS v UNION RETURN 2 AS v NEXT RETURN v",
        "RETURN 1 AS v NEXT RETURN v UNION RETURN 2 AS v",
        "RETURN EXISTS { RETURN 1 AS v NEXT RETURN v } AS v",
        "DEFINE FUNCTION f() = 1 FINISH NEXT RETURN 7 AS v",
        "WHEN true THEN RETURN 1 AS v ELSE RETURN 2 AS v NEXT RETURN v",
    ] {
        assert_eq!(
            engine.prepare(query, &ParameterMap::new()).unwrap_err().gql_status(),
            GqlStatus::FEATURE_NOT_SUPPORTED,
            "{query}"
        );
    }
}

#[tokio::test]
async fn composite_next_never_discards_an_arm() {
    // GQ20 remains unsupported in a composite, including nested query specifications.
    // §14.2 SR5 restricts directly contained FINISH, not an earlier NEXT statement.
    let engine = CypherEngine::new(graph());
    let mut outcomes = Vec::new();
    for query in [
        "{ FINISH NEXT RETURN 7 AS v } UNION RETURN 3 AS v",
        "RETURN 3 UNION { FINISH NEXT RETURN 7 AS v }",
        "RETURN 3 AS v UNION { FINISH NEXT RETURN 7 AS v }",
    ] {
        let result = match engine.prepare(query, &ParameterMap::new()) {
            Err(error) => Err(error.gql_status()),
            Ok(prepared) => engine
                .execute_prepared(&prepared)
                .await
                .map(|result| result.rows)
                .map_err(|error| error.gql_status()),
        };
        outcomes.push((query, result));
    }
    assert!(
        outcomes
            .iter()
            .all(|(_, result)| result.as_ref().err() == Some(&GqlStatus::FEATURE_NOT_SUPPORTED)),
        "{outcomes:#?}"
    );
    for operator in [
        "UNION",
        "UNION DISTINCT",
        "UNION ALL",
        "EXCEPT",
        "EXCEPT ALL",
        "INTERSECT ALL",
        "OTHERWISE",
    ] {
        for next in [
            "{ FINISH NEXT RETURN 7 AS v }",
            "{ RETURN 5 AS v NEXT RETURN 7 AS v }",
            "{ RETURN 5 AS v NEXT FINISH }",
            "{ FINISH NEXT FINISH }",
            "{ { FINISH NEXT RETURN 7 AS v } }",
        ] {
            for query in [
                format!("{next} {operator} RETURN 3 AS v"),
                format!("RETURN 3 AS v {operator} {next}"),
                format!("{{ {next} {operator} RETURN 3 AS v }} {operator} RETURN 9 AS v"),
                format!("RETURN 9 AS v {operator} {{ RETURN 3 AS v {operator} {next} }}"),
            ] {
                assert_eq!(status(&query).await, GqlStatus::FEATURE_NOT_SUPPORTED, "{query}");
            }
        }
    }
}

#[tokio::test]
async fn composite_next_preserves_syntax_and_supported_controls() {
    // §9.2 SR7: a missing primitive result wins over capability refusal.
    for query in [
        "{ MATCH (n) NEXT RETURN 7 AS v } UNION RETURN 3 AS v",
        "RETURN 3 AS v UNION { MATCH (n) NEXT RETURN 7 AS v }",
        "{ FINISH NEXT MATCH (n) } UNION ALL RETURN 3 AS v",
        "RETURN 3 AS v UNION ALL { FINISH NEXT MATCH (n) }",
    ] {
        assert_eq!(status(query).await, GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION, "{query}");
    }
    // §14.2 SR5 still forbids a FINISH directly in a multi-primary composite.
    for query in [
        "FINISH UNION RETURN 3 AS v",
        "RETURN 3 AS v UNION ALL FINISH",
        "FINISH UNION FINISH",
        "FINISH UNION { FINISH NEXT RETURN 7 AS v }",
        "{ FINISH NEXT RETURN 7 AS v } UNION FINISH",
        "RETURN 3 AS v UNION { FINISH UNION { FINISH NEXT RETURN 7 AS v } }",
    ] {
        assert_eq!(status(query).await, GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION, "{query}");
    }
    assert_eq!(value("FINISH NEXT RETURN 7 AS v").await, "7");
    assert_eq!(value("{ FINISH NEXT RETURN 7 AS v }").await, "7");
    assert_eq!(value("RETURN 3 AS v NEXT RETURN v + 4 AS v").await, "7");
    assert_eq!(value("FINISH NEXT RETURN 3 AS v NEXT FINISH NEXT RETURN 7 AS v").await, "7");
    assert_eq!(outcome("FINISH NEXT FINISH").await.status.status, GqlStatus::OMITTED_RESULT);
    for query in ["RETURN 7 AS v UNION RETURN 3 AS v", "{ RETURN 7 AS v } UNION ALL RETURN 3 AS v"] {
        let mut rows = table(&run(query).await);
        rows.sort();
        assert_eq!(rows, vec![vec!["3".to_string()], vec!["7".to_string()]], "{query}");
    }
    assert_eq!(run("RETURN 7 AS v UNION ALL RETURN 7 AS v").await.rows.len(), 2);
    assert_eq!(run("RETURN 7 AS v UNION RETURN 7 AS v").await.rows.len(), 1);
}

#[test]
fn composite_next_validates_ast_results_before_refusal() {
    use hydradb_cypher_ast::{Clause, Statement};
    use hydradb_cypher_engine::lower_cypher_ast;
    use hydradb_cypher_parser_antlr::parse_cypher25;

    for source in [
        "{ FINISH NEXT RETURN 7 AS v } UNION RETURN 3 AS v",
        "RETURN 3 AS v UNION { FINISH NEXT RETURN 7 AS v }",
    ] {
        let parsed = parse_cypher25(source).unwrap();
        assert!(matches!(parsed.statements.as_slice(), [Statement::Query(_)]));
        assert_eq!(
            lower_cypher_ast(&parsed).unwrap_err().gql_status(),
            GqlStatus::FEATURE_NOT_SUPPORTED
        );
        for boundary in 0..3 {
            let mut invalid = parsed.clone();
            let [Statement::Query(query)] = invalid.statements.as_mut_slice() else {
                unreachable!()
            };
            let next = if query.clauses.iter().any(|clause| matches!(clause, Clause::Next)) {
                query
            } else {
                &mut query.unions[0].query
            };
            match boundary {
                0 => next.clauses.insert(0, Clause::Next),
                1 => next.clauses.insert(1, Clause::Next),
                _ => {
                    next.clauses.pop();
                }
            }
            assert_eq!(
                lower_cypher_ast(&invalid).unwrap_err().gql_status(),
                GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
                "{source}: invalid NEXT boundary {boundary}"
            );
        }
    }
}

#[tokio::test]
async fn finish_next_restarts_from_unit_table_without_previous_columns() {
    // §9.2 GR3a(ii), §14.10 GR2: omitted is not an empty table result.
    for source in ["FOR a IN [3, 3]", "FOR a IN []"] {
        assert_eq!(value(&format!("{source} FINISH NEXT FOR b IN [7] RETURN b AS v")).await, "7");
        assert_eq!(
            outcome(&format!("{source} FINISH NEXT FOR b IN [7] FINISH")).await.status.status,
            GqlStatus::OMITTED_RESULT
        );
    }
    for source in ["FOR x IN []", "FOR x IN [1]", "FOR x IN [1, 2, 3]"] {
        assert_eq!(value(&format!("{source} FINISH NEXT RETURN 7 AS v")).await, "7");
        assert_eq!(
            outcome(&format!("{source} FINISH NEXT FINISH")).await.status.status,
            GqlStatus::OMITTED_RESULT
        );
        assert_eq!(
            value(&format!("{source} FINISH NEXT RETURN 5 AS y NEXT RETURN y + 2 AS v")).await,
            "7"
        );
        assert_eq!(value(&format!("{source} FINISH NEXT FINISH NEXT RETURN 7 AS v")).await, "7");
        assert_eq!(
            status(&format!("{source} FINISH NEXT RETURN x")).await,
            GqlStatus::INVALID_REFERENCE
        );
    }
    assert!(run("FOR x IN [] RETURN x NEXT RETURN 7 AS v").await.rows.is_empty());
    assert_eq!(
        value("RETURN 1 NEXT FOR x IN [3, 1, 2] RETURN x ORDER BY x LIMIT 1 NEXT FINISH NEXT RETURN 7 AS v").await,
        "7"
    );
    // An earlier statement still executes even when the continuation needs no rows.
    assert_eq!(
        status("FOR x IN [0] FILTER 1 / x > 0 FINISH NEXT RETURN 7 AS v LIMIT 0").await,
        GqlStatus::DIVISION_BY_ZERO
    );
    let result = outcome("FOR x IN [null, 1] WITH sum(x) AS s FINISH NEXT RETURN 7 AS v").await;
    assert_eq!(table(&result.result), [["7"]]);
    assert_eq!(result.status.status, GqlStatus::NULL_VALUE_ELIMINATED_IN_SET_FUNCTION);
    assert!(!result.result_omitted());
    let omitted = outcome("FOR x IN [null, 1] WITH sum(x) AS s FINISH NEXT FINISH").await;
    assert!(omitted.result_omitted());
    assert_eq!(omitted.status.status, GqlStatus::NULL_VALUE_ELIMINATED_IN_SET_FUNCTION);
    assert!(omitted
        .additional
        .iter()
        .any(|condition| condition.status == GqlStatus::OMITTED_RESULT));
    let engine = CypherEngine::new(graph());
    let prepared = engine.prepare("FINISH NEXT RETURN 7 AS v", &ParameterMap::new()).unwrap();
    assert!(prepared.explain().contains("NextExec\n  FinishExec\n"));
}

#[tokio::test]
async fn value_is_the_one_value_a_subquery_returns() {
    // General Rule 2: the value of the one field of the one row, or null.
    assert_eq!(
        value("MATCH (n:Hub) RETURN VALUE { MATCH (n)-[e:HAS]->() RETURN sum(e.weight) AS w } AS total").await,
        "6"
    );
    assert_eq!(
        value("MATCH (n:Node {name: 'd'}) RETURN VALUE { MATCH (n)-[:R]->(m) RETURN m.name AS x LIMIT 1 } AS v").await,
        "\"a\""
    );
    assert_eq!(
        value("MATCH (n:Hub) RETURN VALUE { MATCH (n)-[:R]->(m) RETURN m.name AS x LIMIT 1 } AS v").await,
        "null"
    );
    assert_eq!(
        value("MATCH (n:Node) WHERE VALUE { MATCH (n)-[:R]->(m) RETURN count(m) AS c } = 2 RETURN n.name AS n").await,
        "\"a\""
    );
    let syntax = GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION;
    // Syntax Rule 3: one column. Syntax Rule 10: LIMIT 1, or an aggregate
    // with no GROUP BY.
    assert_eq!(
        status("RETURN VALUE { MATCH (i:Item) RETURN count(*) AS c, max(i.score) AS m } AS v").await,
        syntax
    );
    assert_eq!(status("RETURN VALUE { MATCH (i:Item) RETURN i.name AS x } AS v").await, syntax);
    assert_eq!(
        status("RETURN VALUE { MATCH (i:Item) RETURN count(*) AS c GROUP BY i.kind } AS v").await,
        syntax
    );
}

// ---- §16.7, §16.11 quantified edges (G036, G060, G061) ----

#[tokio::test]
async fn a_quantified_edge_repeats_as_a_trail() {
    // The R edges are a->b, a->c, b->d, c->d, d->a and a self-loop on
    // `loop`. Under DIFFERENT EDGES (ID086) every match is a trail.
    assert_eq!(
        sorted_column("MATCH (a:Node {name: 'a'})-[:R]->{1,2}(b) RETURN b.name AS n").await,
        ["\"b\"", "\"c\"", "\"d\"", "\"d\""]
    );
    assert_eq!(
        sorted_column("MATCH (a:Node {name: 'a'})-[:R]->{2}(b) RETURN b.name AS n").await,
        ["\"d\"", "\"d\""]
    );
    // From a, `+` finds two trails of each length 1 to 5; `*` adds a itself,
    // and `{0,1}` is a, b and c.
    assert_eq!(value("MATCH (a:Node {name: 'a'})-[:R]->+(b) RETURN count(*) AS c").await, "10");
    assert_eq!(value("MATCH (a:Node {name: 'a'})-[:R]->*(b) RETURN count(*) AS c").await, "11");
    assert_eq!(value("MATCH (a:Node {name: 'a'})-[:R]->{0,1}(b) RETURN count(*) AS c").await, "3");
    assert_eq!(value("MATCH (a)-[:R]->{1,3}(b) RETURN count(*) AS c").await, "20");
    // The edge variable is a group variable: the list of the path's edges.
    assert_eq!(
        sorted_column("MATCH (a:Node {name: 'a'})-[e:R]->{2}(b) RETURN size(e) AS n").await,
        ["2", "2"]
    );
    // §16.11 Syntax Rules 1 and 3 a): a positive upper bound no less than the
    // lower and within IL018 (255).
    let syntax = GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION;
    assert_eq!(status("MATCH (a)-[:R]->{0}(b) RETURN count(*) AS c").await, syntax);
    assert_eq!(status("MATCH (a)-[:R]->{3,1}(b) RETURN count(*) AS c").await, syntax);
    assert_eq!(status("MATCH (a)-[:R]->{1,256}(b) RETURN count(*) AS c").await, syntax);
}

// ---- §16.6, §22.2 path modes (G010 to G014), §16.4 match modes (G003) ----

#[tokio::test]
async fn quantified_edge_bounds_use_unsigned_integer_values_in_every_radix() {
    // §16.11 SR1-3, §21.2 SR7. The diamond/cycle has two trails of
    // each length 1..=5 from a; the empty path contributes one more row.
    for (quantifier, expected) in [
        ("{0o5,}", "2"),
        ("{0x2,0b11}", "4"),
        ("{0b1}", "2"),
        ("{,0o1}", "3"),
        ("{0x0,0b1}", "3"),
        ("{0b0,}", "11"),
        ("{1,0xff}", "10"),
        ("{0xff}", "0"),
        ("{٠,٠٠١}", "3"),
    ] {
        let query = format!("MATCH (a:Node {{name: 'a'}})-[:R]->{quantifier}(b) RETURN count(*) AS c");
        assert_eq!(value(&query).await, expected, "{quantifier}");
    }
    for (quantifier, expected) in [
        ("{0o5,}", vec![vec![r#""d""#, "5"], vec![r#""d""#, "5"]]),
        (
            "{0x2,0b11}",
            vec![vec![r#""a""#, "3"], vec![r#""a""#, "3"], vec![r#""d""#, "2"], vec![r#""d""#, "2"]],
        ),
        ("{0b1}", vec![vec![r#""b""#, "1"], vec![r#""c""#, "1"]]),
    ] {
        let query =
            format!("MATCH (a:Node {{name: 'a'}})-[e:R]->{quantifier}(b) RETURN b.name AS target, size(e) AS hops ORDER BY target, hops");
        assert_eq!(table(&run(&query).await), expected, "{quantifier}");
    }
    for quantifier in [
        "{0x0}",
        "{0b0,0o0}",
        "{0x3,0b1}",
        "{0x100}",
        "{1,0o400}",
        "{0b100000000,}",
        "{4294967295}",
        "{0xffffffff}",
    ] {
        let query = format!("MATCH (a)-[:R]->{quantifier}(b) RETURN count(*) AS c");
        assert_eq!(
            status(&query).await,
            GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "{quantifier}"
        );
    }
    for quantifier in ["{0x100000000}", "{1f}", "{0b2}", "{0o8}", "{0xG}", "{-1}"] {
        let query = format!("MATCH (a)-[:R]->{quantifier}(b) RETURN count(*) AS c");
        assert_eq!(status(&query).await, GqlStatus::INVALID_SYNTAX, "{quantifier}");
    }
    // The Cypher form has distinct zero-bound semantics; no GQL check leaks.
    assert_eq!(value("MATCH (a:Node {name: 'a'})-[:R*0..0]->(b) RETURN count(*) AS c").await, "1");
    assert_eq!(value("MATCH (a:Node {name: 'a'})-[:R*1..2]->(b) RETURN count(*) AS c").await, "4");
}

#[tokio::test]
async fn path_modes_restrict_the_path() {
    // §22.2 General Rule 16 d) ii): from a, the trails that repeat no node
    // are a->b, a->c, a->b->d and a->c->d; SIMPLE also admits the two that
    // end back at a.
    assert_eq!(
        value("MATCH ACYCLIC (a:Node {name: 'a'})-[:R]->+(b) RETURN count(*) AS c").await,
        "4"
    );
    assert_eq!(
        value("MATCH SIMPLE (a:Node {name: 'a'})-[:R]->+(b) RETURN count(*) AS c").await,
        "6"
    );
    assert_eq!(
        value("MATCH TRAIL (a:Node {name: 'a'})-[:R]->+(b) RETURN count(*) AS c").await,
        "10"
    );
    assert_eq!(
        value("MATCH WALK PATH (a:Node {name: 'a'})-[:R]->+(b) RETURN count(*) AS c").await,
        "10"
    );
    // §16.4 NOTE 224: ACYCLIC (x)-[]->(x) finds nothing, even on a self-loop;
    // SIMPLE lets the path end where it began.
    assert_eq!(value("MATCH (x)-[:R]->(x) RETURN count(*) AS c").await, "1");
    assert_eq!(value("MATCH ACYCLIC (x)-[:R]->(x) RETURN count(*) AS c").await, "0");
    assert_eq!(value("MATCH SIMPLE (x)-[:R]->(x) RETURN count(*) AS c").await, "1");
    // A mode inside parentheses holds of the path they enclose.
    assert_eq!(
        value("MATCH (ACYCLIC (a:Node {name: 'a'})-[:R]->+(b)) RETURN count(*) AS c").await,
        "4"
    );
}

#[tokio::test]
async fn repeatable_elements_lets_a_match_reuse_an_edge() {
    // §16.4 General Rule 8: without DIFFERENT EDGES, two patterns may bind
    // one edge, and a bounded quantifier walks.
    assert_eq!(value("MATCH (a)-[e:R]->(b), (c)-[f:R]->(d) RETURN count(*) AS c").await, "30");
    assert_eq!(
        value("MATCH REPEATABLE ELEMENTS (a)-[e:R]->(b), (c)-[f:R]->(d) RETURN count(*) AS c").await,
        "36"
    );
    assert_eq!(
        value("MATCH (l:Node {name: 'loop'})-[:R]->{1,3}(m) RETURN count(*) AS c").await,
        "1"
    );
    assert_eq!(
        value("MATCH REPEATABLE ELEMENTS (l:Node {name: 'loop'})-[:R]->{1,3}(m) RETURN count(*) AS c").await,
        "3"
    );
    // Syntax Rule 14: an unbounded quantifier needs DIFFERENT EDGES, a
    // restrictive path mode or a selective path pattern.
    assert_eq!(
        status("MATCH REPEATABLE ELEMENTS (a)-[:R]->+(b) RETURN count(*) AS c").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    assert_eq!(
        value("MATCH REPEATABLE ELEMENTS TRAIL (a:Node {name: 'a'})-[:R]->+(b) RETURN count(*) AS c").await,
        "10"
    );
}

// ---- §16.7 parenthesized path pattern expressions (G038, G049 to G051) ----

#[tokio::test]
async fn a_parenthesized_path_carries_its_mode_and_where() {
    assert_eq!(value("MATCH ((a)-[:R]->(b)) RETURN count(*) AS c").await, "6");
    // Its WHERE holds of the path it encloses (§16.4 General Rule 9 a) ii)),
    // and may read a variable from outside it (G051).
    assert_eq!(value("MATCH ((a)-[e:HAS]->(b) WHERE e.weight > 1) RETURN count(*) AS c").await, "2");
    assert_eq!(
        value("MATCH (x:Hub), ((a)-[e:HAS]->(b) WHERE a = x) RETURN count(*) AS c").await,
        "3"
    );
}

// ---- §16.6, §22.4 selective path patterns (G005, G015 to G020) ----

/// Each row's target name and path length, sorted.
async fn targets_and_lengths(query: &str) -> Vec<(String, String)> {
    let mut rows: Vec<(String, String)> = table(&run(query).await)
        .into_iter()
        .map(|row| (row[0].clone(), row[1].clone()))
        .collect();
    rows.sort();
    rows
}

#[tokio::test]
async fn a_path_search_selects_within_each_pair_of_endpoints() {
    // §22.4 General Rules 12 and 13: the trails from a, partitioned by their
    // last node. To b and to c the shortest is 1 edge, to d 2 (two paths),
    // back to a 3 (two paths).
    let pairs = |rows: &[(&str, &str)]| rows.iter().map(|(n, l)| (n.to_string(), l.to_string())).collect::<Vec<_>>();
    const FROM_A: &str = "(a:Node {name: 'a'})-[e:R]->+(b)";
    const PAIRS: &str = "RETURN b.name AS n, size(e) AS l";
    const COUNT: &str = "RETURN count(*) AS c";
    assert_eq!(
        targets_and_lengths(&format!("MATCH ANY SHORTEST {FROM_A} {PAIRS}")).await,
        pairs(&[("\"a\"", "3"), ("\"b\"", "1"), ("\"c\"", "1"), ("\"d\"", "2")])
    );
    assert_eq!(
        targets_and_lengths(&format!("MATCH ALL SHORTEST {FROM_A} {PAIRS}")).await,
        pairs(&[
            ("\"a\"", "3"),
            ("\"a\"", "3"),
            ("\"b\"", "1"),
            ("\"c\"", "1"),
            ("\"d\"", "2"),
            ("\"d\"", "2")
        ])
    );
    // SHORTEST 2: the two shortest to each node, ties among them in any order.
    assert_eq!(
        targets_and_lengths(&format!("MATCH SHORTEST 2 {FROM_A} {PAIRS}")).await,
        pairs(&[
            ("\"a\"", "3"),
            ("\"a\"", "3"),
            ("\"b\"", "1"),
            ("\"b\"", "4"),
            ("\"c\"", "1"),
            ("\"c\"", "4"),
            ("\"d\"", "2"),
            ("\"d\"", "2")
        ])
    );
    // SHORTEST 2 GROUPS: every path of the two shortest lengths, which here
    // is every trail; ANY 2: two paths to each node.
    assert_eq!(value(&format!("MATCH SHORTEST 2 GROUPS {FROM_A} {COUNT}")).await, "10");
    assert_eq!(value(&format!("MATCH ANY 2 {FROM_A} {COUNT}")).await, "8");
    assert_eq!(value(&format!("MATCH ANY {FROM_A} {COUNT}")).await, "4");
    // ALL selects nothing (§16.6 Syntax Rule 4: it is not selective).
    assert_eq!(value(&format!("MATCH ALL {FROM_A} {COUNT}")).await, "10");
}

#[tokio::test]
async fn a_path_search_selects_before_the_graph_pattern_where() {
    // §16.4 General Rule 9 b): the WHERE holds of the selected matches. The
    // shortest paths to b and c have one edge, so they go; d's and a's stay.
    assert_eq!(
        value("MATCH ANY SHORTEST (a:Node {name: 'a'})-[e:R]->+(b) WHERE size(e) > 1 RETURN count(*) AS c").await,
        "2"
    );
    // A selective path anchored on an earlier MATCH selects for that row.
    assert_eq!(
        value("MATCH (x:Node {name: 'b'}) MATCH ANY SHORTEST (x)-[e:R]->+(y:Node {name: 'c'}) RETURN size(e) AS l").await,
        "3"
    );
}

#[tokio::test]
async fn a_path_search_counts_a_positive_integer_and_stands_alone() {
    let syntax = GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION;
    // §16.6 Syntax Rule 2 b) ii): a literal count is positive.
    assert_eq!(status("MATCH SHORTEST 0 (a)-[:R]->+(b) RETURN count(*) AS c").await, syntax);
    // §22.4 General Rule 7: a parameter that is not raises 22G0F.
    let zero = ParameterMap::from([("k".to_string(), ScalarValue::Integer(0))]);
    assert_eq!(
        status_with("MATCH ANY $k (a)-[:R]->+(b) RETURN count(*) AS c", &zero).await,
        GqlStatus::INVALID_NUMBER_OF_PATHS_OR_GROUPS
    );
    // §16.4 Syntax Rule 12: under DIFFERENT EDGES a selective path pattern is
    // the only one of its graph pattern.
    assert_eq!(
        status("MATCH ANY SHORTEST (a)-[:R]->+(b), (h:Hub) RETURN count(*) AS c").await,
        syntax
    );
    assert_eq!(
        value("MATCH REPEATABLE ELEMENTS ANY SHORTEST TRAIL (a:Node {name: 'a'})-[:R]->+(b), (h:Hub) RETURN count(*) AS c").await,
        "4"
    );
    // Selective, so Syntax Rule 14 admits unbounded walks, but the engine
    // cannot enumerate their candidates (limitation L15).
    assert_eq!(
        status("MATCH REPEATABLE ELEMENTS ANY SHORTEST (a:Node {name: 'a'})-[:R]->+(b) RETURN count(*) AS c").await,
        GqlStatus::FEATURE_NOT_SUPPORTED
    );
    // §16.6 Syntax Rule 7: a strict interior variable is not an exterior one.
    assert_eq!(
        status("MATCH (x:Node {name: 'b'}) MATCH ANY SHORTEST (a:Node {name: 'a'})-[:R]->(x)-[:R]->+(b) RETURN count(*) AS c").await,
        syntax
    );
}

// ---- §16.4 KEEP (G006, G007) ----

#[tokio::test]
#[ignore = "bounded dense selector resource baseline; run explicitly with --ignored --nocapture"]
async fn dense_path_selection_resource_baseline() {
    use common::profiling::{operator, profile_query};
    use common::MemoryStorage;
    use hydradb_cypher_engine::{RelationshipRecord, VertexRecord};
    for n in [4u64, 5, 6] {
        let mut storage = MemoryStorage::default();
        for id in 0..n {
            storage.vertices.insert(id, VertexRecord::new(id).with_label("Dense"));
            for target_id in (0..n).filter(|target| *target != id) {
                storage
                    .relationships
                    .push(RelationshipRecord::new(id * n + target_id, id, target_id, "R"));
            }
        }
        // §22.4 partitions by endpoints. One shortest group includes a
        // direct edge per distinct pair and every two-hop return tie.
        for (mode, search, filter, limit, expected) in [
            ("", "ANY 1", "", "", n * n),
            ("", "ANY 1", "", " LIMIT 1", 1),
            ("", "SHORTEST 2", "", "", 2 * n * n),
            ("", "SHORTEST 1 GROUP", "", "", 2 * n * (n - 1)),
            ("", "SHORTEST 2 GROUPS", "", "", 2 * n * (n - 1) * (n - 1)),
            ("", "SHORTEST 1 GROUP", " WHERE size(e) > 1", "", n * (n - 1) * (n - 1)),
            ("REPEATABLE ELEMENTS ", "ANY 1 WALK", "", "", n * n),
            ("REPEATABLE ELEMENTS ", "ANY 1 TRAIL", "", "", n * n),
            ("REPEATABLE ELEMENTS ", "ANY 1 SIMPLE", "", "", n * n),
            ("REPEATABLE ELEMENTS ", "ANY 1 ACYCLIC", "", "", n * (n - 1)),
        ] {
            let path = "(a:Dense)-[e:R]->{1,4}(b)".to_string();
            let path = if filter.is_empty() { path } else { format!("({path}{filter})") };
            let query = format!("MATCH {mode}{search} {path} RETURN a, b, size(e) AS hops{limit}");
            let (result, profile, _) = profile_query(storage.clone(), &query, &ParameterMap::new()).await;
            assert_eq!(result.rows.len() as u64, expected, "{query}");
            let selection = operator(&profile, "SelectPathsExec");
            let peak = profile.operators.iter().map(|op| op.peak_retained_rows).max().unwrap();
            eprintln!("dense n={n} search={search:?} filter={filter:?} limit={limit:?} candidates={} selected={} rows={} peak_rows={peak} selection_us={} total_us={}",
                selection.rows_in, selection.rows_out, result.rows.len(), selection.self_elapsed.as_micros(), selection.elapsed.as_micros());
            eprintln!(
                "plan={:?}",
                profile.operators.iter().map(|operator| operator.operator).collect::<Vec<_>>()
            );
        }
    }
}

#[tokio::test]
async fn keep_rejects_every_written_path_search_prefix() {
    // §16.4 SR6 b)i), §16.6: ALL is a search even though nonselective;
    // the prohibition holds under mode-only KEEP as well (unchanged by Cor1).
    for own in [
        "ALL",
        "ALL TRAIL PATHS",
        "ANY",
        "ANY SHORTEST",
        "ALL SHORTEST",
        "SHORTEST 2",
        "SHORTEST 2 GROUPS",
    ] {
        for keep in [
            "ALL",
            "WALK",
            "TRAIL",
            "SIMPLE",
            "ACYCLIC",
            "ANY",
            "ANY SHORTEST",
            "SHORTEST 2",
            "SHORTEST 2 GROUPS",
        ] {
            let query = format!("MATCH {own} (a)-[:R]->(b) KEEP {keep} RETURN count(*) AS c");
            assert_eq!(status(&query).await, GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION, "{query}");
        }
    }
    for path in ["((ALL (a)-[:R]->(b)))", "ALL ((a)-[:R]->{1}(b))", "(ALL (a)-[:R]->{1}(b))"] {
        for keep in ["ALL", "TRAIL", "ANY SHORTEST"] {
            let query = format!("MATCH {path} KEEP {keep} RETURN count(*) AS c");
            assert_eq!(status(&query).await, GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION, "{query}");
        }
    }
    for path in ["(a)-[:R]->(b)", "TRAIL (a)-[:R]->(b)", "(TRAIL (a)-[:R]->(b))"] {
        for keep in ["ALL", "TRAIL", "ANY SHORTEST", "SHORTEST 2 GROUPS"] {
            let query = format!("MATCH {path} KEEP {keep} RETURN count(*) AS c");
            assert_eq!(value(&query).await, "6", "{query}");
        }
    }
    assert_eq!(value("MATCH ALL (a)-[:R]->(b) RETURN count(*) AS c").await, "6");
}

#[tokio::test]
async fn keep_gives_every_path_its_prefix() {
    // Syntax Rule 6: KEEP P makes each path pattern P ( ... ).
    assert_eq!(
        value("MATCH (a:Node {name: 'a'})-[:R]->+(b) KEEP ANY SHORTEST RETURN count(*) AS c").await,
        "4"
    );
    assert_eq!(
        value("MATCH (a:Node {name: 'a'})-[:R]->+(b) KEEP ACYCLIC RETURN count(*) AS c").await,
        "4"
    );
    // Syntax Rule 6 b) i): a path pattern under KEEP has no search prefix of
    // its own.
    assert_eq!(
        status("MATCH ANY (a)-[:R]->+(b) KEEP SHORTEST 1 RETURN count(*) AS c").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
}

// ---- §16.4 path variables (G004), §16.7 subpath variables (G048), §20.22 and §20.16 path functions (GF04) ----

#[tokio::test]
async fn a_path_variable_binds_the_matched_path() {
    // Nodes 1020 to 1023 are a to d; R edges 4 to 9 are a->b, a->c, b->d,
    // c->d, d->a and the self-loop.
    assert_eq!(
        sorted_column("MATCH p = (a:Node {name: 'a'})-[:R]->(b) RETURN p").await,
        ["<#1020, rel#4, #1021>", "<#1020, rel#5, #1022>"]
    );
    assert_eq!(
        sorted_column("MATCH p = (a:Node {name: 'a'})-[:R]->{2}(b) RETURN p").await,
        ["<#1020, rel#4, #1021, rel#6, #1023>", "<#1020, rel#5, #1022, rel#7, #1023>"]
    );
    // Written order, whichever way the edges point.
    assert_eq!(
        sorted_column("MATCH p = (d:Node {name: 'd'})<-[:R]-(b) RETURN p").await,
        ["<#1023, rel#6, #1021>", "<#1023, rel#7, #1022>"]
    );
    // An OPTIONAL MATCH that misses binds the path to null.
    assert_eq!(value("MATCH (h:Hub) OPTIONAL MATCH p = (h)-[:R]->(x) RETURN p").await, "null");
    // A selected path, here the one shortest from b to c.
    assert_eq!(
        value("MATCH p = ANY SHORTEST (a:Node {name: 'b'})-[:R]->+(c:Node {name: 'c'}) RETURN p").await,
        "<#1021, rel#6, #1023, rel#8, #1020, rel#5, #1022>"
    );
    // §16.4 Syntax Rule 15, §16.3 Syntax Rules 7 and 9: a path variable is a
    // new name, and not an element's.
    let syntax = GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION;
    assert_eq!(status("MATCH p = (p)-[:R]->(b) RETURN count(*) AS c").await, syntax);
    assert_eq!(status("MATCH (h:Hub) MATCH h = (x)-[:R]->(y) RETURN count(*) AS c").await, syntax);
    assert_eq!(
        status("MATCH p = (a)-[:R]->(b), p = (c)-[:R]->(d) RETURN count(*) AS c").await,
        syntax
    );
}

#[tokio::test]
async fn a_subpath_variable_names_the_parenthesized_path() {
    assert_eq!(
        sorted_column("MATCH (q = (a:Node {name: 'a'})-[:R]->(b)) RETURN path_length(q) AS n").await,
        ["1", "1"]
    );
    assert_eq!(
        table(
            &run("MATCH p = (q = (a:Node {name: 'a'})-[:R]->{2}(b:Node {name: 'd'})) RETURN path_length(p) AS m, path_length(q) AS n")
                .await
        ),
        [["2", "2"], ["2", "2"]]
    );
}

#[tokio::test]
async fn path_functions_read_a_path() {
    // §20.22 General Rule 3: the number of edges; §4.15.2: a path's
    // cardinality counts its nodes and edges.
    assert_eq!(
        sorted_column("MATCH p = (a:Node {name: 'a'})-[:R]->{1,3}(b) RETURN path_length(p) AS n").await,
        ["1", "1", "2", "2", "3", "3"]
    );
    assert_eq!(
        sorted_column("MATCH p = (a:Node {name: 'a'})-[:R]->{2}(b) RETURN cardinality(p) AS n").await,
        ["5", "5"]
    );
    // §20.16 General Rule 2: the path element list, whose nodes keep their
    // records.
    assert_eq!(
        sorted_column(
            "MATCH p = (a:Node {name: 'a'})-[:R]->{2}(b:Node {name: 'd'}) UNWIND ELEMENTS(p) AS x WITH x WHERE x:Node RETURN x.name AS n"
        )
        .await,
        ["\"a\"", "\"a\"", "\"b\"", "\"c\"", "\"d\"", "\"d\""]
    );
    assert_eq!(
        value("MATCH (h:Hub) OPTIONAL MATCH p = (h)-[:R]->(x) RETURN path_length(p) AS n").await,
        "null"
    );
    assert_eq!(status("RETURN path_length(1) AS n").await, GqlStatus::INVALID_VALUE_TYPE);
}

// ---- §16.7 quantified paths (G035) ----

#[tokio::test]
async fn a_quantified_path_binds_group_variables() {
    // `((a)-[e:R]->(b)){1,2}` is the R trails of one or two edges: six and six.
    assert_eq!(value("MATCH ((a)-[:R]->(b)){1,2} RETURN count(*) AS c").await, "12");
    // Each iteration's nodes: `a` holds every first, `b` every last, and
    // consecutive iterations meet at one node.
    assert_eq!(
        sorted_column("MATCH (x:Node {name: 'a'})((a)-[e:R]->(b)){2}(y) UNWIND a AS n RETURN n.name AS n").await,
        ["\"a\"", "\"a\"", "\"b\"", "\"c\""]
    );
    assert_eq!(
        sorted_column("MATCH (x:Node {name: 'a'})((a)-[e:R]->(b)){2}(y) UNWIND b AS n RETURN n.name AS n").await,
        ["\"b\"", "\"c\"", "\"d\"", "\"d\""]
    );
    assert_eq!(
        table(&run("MATCH (x:Node {name: 'a'})((a)-[e:R]->(b)){2}(y) RETURN size(a) AS i, size(e) AS j, y.name AS n").await),
        [["2", "2", "\"d\""], ["2", "2", "\"d\""]]
    );
    // A group variable read as one element reads null (an extension of §16.3
    // Syntax Rule 16, as on Cypher's relationship list).
    assert_eq!(
        sorted_column("MATCH (x:Node {name: 'a'})((a)-[e:R]->(b)){2}(y) RETURN a.name AS n").await,
        ["null", "null"]
    );
    // §16.7 Syntax Rule 9 and the names' rules: a group variable is a new
    // name, and not an element's.
    let syntax = GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION;
    assert_eq!(
        status("MATCH (a:Hub) MATCH ((a)-[:R]->(b)){1,2} RETURN count(*) AS c").await,
        syntax
    );
    assert_eq!(status("MATCH (x)((x)-[:R]->(b)){1,2} RETURN count(*) AS c").await, syntax);
    assert_eq!(status("MATCH ((a)-[:R]->{1,2}(b)){2} RETURN count(*) AS c").await, syntax);
}

// ---- §16.7 path pattern union and multiset alternation (G030 to G033) ----

#[tokio::test]
async fn a_path_pattern_union_keeps_each_reduced_match_once() {
    // The R edges are a->b, a->c, b->d, c->d, d->a and a self-loop on
    // `loop`. Each direction finds six; only the self-loop is the same
    // reduced match both ways (§16.4 General Rule 10), so `|` finds 11 and
    // `|+|` 12 (§16.7 Syntax Rule 10).
    const BOTH: &str = "(a)-[:R]->(b) | (a)<-[:R]-(b)";
    assert_eq!(value(&format!("MATCH {BOTH} RETURN count(*) AS c")).await, "11");
    assert_eq!(value(&format!("MATCH ({BOTH}) RETURN count(*) AS c")).await, "11");
    assert_eq!(value("MATCH (a)-[:R]->(b) |+| (a)<-[:R]-(b) RETURN count(*) AS c").await, "12");
    // The word names its variables in order: written the other way round,
    // the self-loop's two matches are not one.
    assert_eq!(value("MATCH (a)-[:R]->(b) | (b)-[:R]->(a) RETURN count(*) AS c").await, "12");
    assert_eq!(value("MATCH (a)-[:R]->(b) | (a)-[:R]->(b) RETURN count(*) AS c").await, "6");
    assert_eq!(value("MATCH (a)-[:R]->(b) |+| (a)-[:R]->(b) RETURN count(*) AS c").await, "12");
    assert_eq!(value("MATCH (a)-[e:R]->(b) | (a)-[:R]->(b) RETURN count(*) AS c").await, "12");
    // Operands may declare one subpath variable (§16.7 Syntax Rule 22 b)).
    assert_eq!(
        value("MATCH (q = (a)-[:R]->(b)) | (q = (a)<-[:R]-(b)) RETURN count(*) AS c").await,
        "11"
    );
    // G031, G033: from a, the one-edge trails of `{1,2}` reduce to the same
    // matches as the plain edge's (§22.2 General Rule 14 f)).
    const FROM_A: &str = "(x:Node {name: 'a'})";
    assert_eq!(
        value(&format!(
            "MATCH {FROM_A}-[:R]->{{1,2}}(y) | {FROM_A}-[:R]->(y) RETURN count(*) AS c"
        ))
        .await,
        "4"
    );
    assert_eq!(
        value(&format!(
            "MATCH {FROM_A}-[:R]->{{1,2}}(y) |+| {FROM_A}-[:R]->(y) RETURN count(*) AS c"
        ))
        .await,
        "6"
    );
    assert_eq!(value("MATCH ((a)-[:R]->{1,2}(b) | (a)<-[:R]-(b)) RETURN count(*) AS c").await, "17");
    assert_eq!(value("MATCH (a)-[:R]->{1,2}(b) |+| (a)<-[:R]-(b) RETURN count(*) AS c").await, "18");
}

#[tokio::test]
async fn a_path_pattern_union_operand_binds_only_its_own_variables() {
    // A variable one operand declares is a conditional singleton (§16.7
    // Syntax Rule 22 d) iv)): null on the other operand's matches.
    const FROM_A: &str = "(x:Node {name: 'a'})";
    assert_eq!(
        table(
            &run(&format!(
                "MATCH {FROM_A}-[e:R]->(y) | {FROM_A}<-[f:R]-(y) RETURN count(e) AS e, count(f) AS f"
            ))
            .await
        ),
        [["2", "1"]]
    );
    // An operand's WHERE holds of its own matches.
    assert_eq!(
        value("MATCH ((a)-[e:HAS]->(b) WHERE e.weight > 1) | (a)<-[:R]-(b) RETURN count(*) AS c").await,
        "8"
    );
    // The path variable binds each operand's path.
    assert_eq!(
        sorted_column(&format!("MATCH p = {FROM_A}-[:R]->(y) | {FROM_A}<-[:R]-(y) RETURN y.name AS n")).await,
        ["\"b\"", "\"c\"", "\"d\""]
    );
    // Under DIFFERENT EDGES the union's edge is not another pattern's.
    assert_eq!(
        value("MATCH (a)-[:R]->(b) | (a)<-[:R]-(b), (c)-[:R]->(d) RETURN count(*) AS c").await,
        "55"
    );
    let syntax = GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION;
    // §16.7 Syntax Rule 11: an operand reads no variable only another
    // operand declares.
    assert_eq!(
        status("MATCH ((a)-[:R]->(b) WHERE b.name = c.name) | (c)<-[:R]-(b) RETURN count(*) AS c").await,
        syntax
    );
    // §16.7 Syntax Rule 10 c): multiset alternation operands' subpaths differ.
    assert_eq!(
        status("MATCH (q = (a)-[:R]->(b)) |+| (q = (a)<-[:R]-(b)) RETURN count(*) AS c").await,
        syntax
    );
    // §16.4 Syntax Rule 3: a conditional singleton joins no other pattern.
    assert_eq!(
        status("MATCH (a)-[:R]->(b) | (a)<-[:R]-(c), (c:Hub) RETURN count(*) AS c").await,
        syntax
    );
    // Not run: a path search over a union, and a variable that is a group in
    // one operand and a single element in another.
    let unsupported = GqlStatus::FEATURE_NOT_SUPPORTED;
    assert_eq!(
        status("MATCH ANY SHORTEST (a)-[:R]->(b) | (a)<-[:R]-(b) RETURN count(*) AS c").await,
        unsupported
    );
    assert_eq!(
        status("MATCH (a)-[e:R]->(b) | (a)-[e:R]->{2}(b) RETURN count(*) AS c").await,
        unsupported
    );
}

// ---- §16.7 edges that may be undirected (G043, G045) ----

#[tokio::test]
async fn an_edge_that_may_be_undirected_matches_the_directed_edges_it_allows() {
    // HydraDB stores no undirected edge: `~[ ]~` matches none, `<~[ ]~` the
    // edges pointing left, `~[ ]~>` those pointing right.
    assert_eq!(value("MATCH (a)~[e:R]~(b) RETURN count(*) AS c").await, "0");
    assert_eq!(value("MATCH (a)<~[e:R]~(b) RETURN count(*) AS c").await, "6");
    assert_eq!(value("MATCH (a)~[e:R]~>(b) RETURN count(*) AS c").await, "6");
    assert_eq!(value("MATCH (a)<-[e:R]->(b) RETURN count(*) AS c").await, "11");
    assert_eq!(value("MATCH (a)~(b) RETURN count(*) AS c").await, "0");
    assert_eq!(value("MATCH (a)<~(b) RETURN count(*) AS c").await, "9");
    assert_eq!(value("MATCH (x:Node {name: 'a'})~>(b) RETURN count(*) AS c").await, "2");
    // Of `~[ ]~{0,2}` only the iteration of no edge matches.
    assert_eq!(value("MATCH (x:Node {name: 'a'})~[:R]~{0,2}(b) RETURN b.name AS n").await, "\"a\"");
}

// ---- §16.7 relaxed topological consistency (G046, G047) ----

#[tokio::test]
async fn node_patterns_side_by_side_bind_one_node() {
    // §22.2 General Rule 16 b): node bindings no edge separates bind the
    // same node.
    assert_eq!(value("MATCH (a:Hub)(b) RETURN count(*) AS c").await, "1");
    assert_eq!(value("MATCH (a:Hub)(b) RETURN a = b AS same").await, "true");
    assert_eq!(value("MATCH (a:Node)(b:Hub) RETURN count(*) AS c").await, "0");
    assert_eq!(value("MATCH (x:Node {name: 'a'})(y)-[:R]->(z) RETURN count(*) AS c").await, "2");
    assert_eq!(
        table(&run("MATCH p = (a:Hub)(b) RETURN PATH_LENGTH(p) AS l, size(ELEMENTS(p)) AS n").await),
        [["0", "1"]]
    );
}

#[tokio::test]
async fn an_edge_without_node_patterns_has_implicit_ones() {
    // §16.7 Syntax Rule 14 b) to d): an empty node pattern between two edges,
    // before the first and after the last.
    assert_eq!(value("MATCH (a)-[:R]->-[:R]->(b) RETURN count(*) AS c").await, "6");
    assert_eq!(value("MATCH -[:R]-> RETURN count(*) AS c").await, "6");
    assert_eq!(value("MATCH -[e:HAS]->(i) RETURN count(e) AS c").await, "3");
    assert_eq!(
        sorted_column("MATCH p = (x:Node {name: 'a'})-[:R]->-[:R]-> RETURN PATH_LENGTH(p) AS l").await,
        ["2", "2"]
    );
}

// ---- §16.12 simplified path pattern expressions (G039, G080 to G082) ----

#[tokio::test]
async fn a_simplified_path_pattern_is_the_edges_it_abbreviates() {
    // Table 3: the delimiters give the direction.
    for (query, count) in [
        ("MATCH (a)-/R/->(b) RETURN count(*) AS c", "6"),
        ("MATCH (a)<-/R/-(b) RETURN count(*) AS c", "6"),
        ("MATCH (a)-/R/-(b) RETURN count(*) AS c", "11"),
        ("MATCH (a)<-/R/->(b) RETURN count(*) AS c", "11"),
        ("MATCH (a)~/R/~(b) RETURN count(*) AS c", "0"),
        ("MATCH (a)~/R/~>(b) RETURN count(*) AS c", "6"),
        ("MATCH (a)<~/R/~(b) RETURN count(*) AS c", "6"),
        // Syntax Rule 6 b) vii): an override gives its edge its own.
        ("MATCH (a)-/R>/-(b) RETURN count(*) AS c", "6"),
        ("MATCH (a)-/<R/-(b) RETURN count(*) AS c", "6"),
        ("MATCH (a)-/-R/->(b) RETURN count(*) AS c", "11"),
        ("MATCH (a)-/<R>/-(b) RETURN count(*) AS c", "11"),
        ("MATCH (a)-/~R/-(b) RETURN count(*) AS c", "0"),
        ("MATCH (a)-/~R>/-(b) RETURN count(*) AS c", "6"),
        ("MATCH (a)-/<~R/-(b) RETURN count(*) AS c", "6"),
        // Labels: union, negation, conjunction and parentheses.
        ("MATCH ()-/R|HAS/->() RETURN count(*) AS c", "9"),
        ("MATCH ()-/(R|HAS)/->() RETURN count(*) AS c", "9"),
        ("MATCH ()-/!HAS/->() RETURN count(*) AS c", "6"),
        ("MATCH ()-/R&HAS/->() RETURN count(*) AS c", "0"),
        // Syntax Rule 6 b) iii): a concatenation is a path of its edges.
        ("MATCH (x:Node {name: 'a'})-/R R/->(y) RETURN count(*) AS c", "2"),
        // The delimiters' direction is either: from b, R to d and then
        // c's edge into d, or R back to a and then d's edge into a.
        ("MATCH (x:Node {name: 'b'})-/R <R/-(y) RETURN count(*) AS c", "2"),
        ("MATCH (x:Node {name: 'b'})-/R> <R/-(y) RETURN y.name AS n", "\"c\""),
        // v): a quantified edge, and the whole expression quantified.
        ("MATCH (x:Node {name: 'a'})-/R{2}/->(y) RETURN count(*) AS c", "2"),
        ("MATCH (x:Node {name: 'a'})-/R+/->(y) RETURN count(*) AS c", "10"),
        ("MATCH (x:Node {name: 'a'})-/R/->{2}(y) RETURN count(*) AS c", "2"),
    ] {
        assert_eq!(value(query).await, count, "{query}");
    }
    // §16.12 Syntax Rules 1 to 4: no concatenation, quantifier or override
    // inside an override, negation or conjunction.
    let invalid = GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION;
    for query in [
        "MATCH ()-/<(R R)/-() RETURN count(*) AS c",
        "MATCH ()-/!(R R)/->() RETURN count(*) AS c",
        "MATCH ()-/R & R{2}/->() RETURN count(*) AS c",
        "MATCH ()-/<(<R)/-() RETURN count(*) AS c",
        // An override inside a negation or a conjunction (see the README).
        "MATCH ()-/!(<R)/->() RETURN count(*) AS c",
        "MATCH ()-/R & <S/->() RETURN count(*) AS c",
    ] {
        assert_eq!(status(query).await, invalid, "{query}");
    }
    // §16.7 Syntax Rule 9: quantifiers do not nest.
    assert_eq!(
        status("MATCH ()-/R{2}/->{2}() RETURN count(*) AS c").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    // Questioned semantics (G037) are still a separate capability.
    assert_eq!(
        status("MATCH ()-/R?/->() RETURN count(*) AS c").await,
        GqlStatus::FEATURE_NOT_SUPPORTED
    );
    // `/` and `-` stay arithmetic outside a pattern.
    assert_eq!(value("RETURN 4/-2 AS q").await, "-2");
}

// ---- §19.6 value type predicate (GA06), §18.9 value types (GV) ----

#[tokio::test]
async fn a_value_is_of_the_types_that_include_it() {
    for (query, answer) in [
        // Exact and approximate numbers have different base types (§4.12.5).
        ("RETURN 1 IS TYPED INTEGER AS v", "true"),
        ("RETURN 1 IS :: FLOAT AS v", "false"),
        ("RETURN 1.0 IS :: INTEGER AS v", "false"),
        ("RETURN 1 IS NOT TYPED STRING AS v", "true"),
        // An integer is of each binary exact numeric type whose range holds
        // it (§18.9 Syntax Rules 35 and 36).
        ("RETURN 127 IS :: INT8 AS v", "true"),
        ("RETURN 128 IS :: INT8 AS v", "false"),
        ("RETURN -128 IS :: SIGNED INTEGER8 AS v", "true"),
        ("RETURN 255 IS :: UINT8 AS v", "true"),
        ("RETURN -1 IS :: UINT8 AS v", "false"),
        ("RETURN 40000 IS :: SMALLINT AS v", "false"),
        ("RETURN 40000 IS :: USMALLINT AS v", "true"),
        ("RETURN 1023 IS :: INT(10) AS v", "true"),
        ("RETURN 1024 IS :: INT(10) AS v", "false"),
        ("RETURN 4294967295 IS :: UINT AS v", "true"),
        ("RETURN 4294967296 IS :: UINT AS v", "false"),
        ("RETURN -1 IS :: UINT256 AS v", "false"),
        ("RETURN 9223372036854775807 IS :: BIGINT AS v", "true"),
        // A float is of each approximate type that represents it exactly.
        ("RETURN 1.5 IS :: FLOAT16 AS v", "true"),
        ("RETURN 0.1 IS :: FLOAT32 AS v", "false"),
        ("RETURN 0.1 IS :: DOUBLE PRECISION AS v", "true"),
        ("RETURN 0.1 IS :: FLOAT128 AS v", "true"),
        ("RETURN 0.5 IS :: REAL AS v", "true"),
        // Strings by their length in characters.
        ("RETURN 'ab' IS :: STRING(1, 2) AS v", "true"),
        ("RETURN 'abc' IS :: VARCHAR(2) AS v", "false"),
        ("RETURN 'a' IS :: CHAR AS v", "true"),
        ("RETURN 'ab' IS :: CHAR(1) AS v", "false"),
        ("RETURN true IS :: BOOL AS v", "true"),
        // The null value is of every nullable type (§4.12.5).
        ("RETURN null IS :: INTEGER AS v", "true"),
        ("RETURN null IS :: INTEGER NOT NULL AS v", "false"),
        ("RETURN null IS :: NULL AS v", "true"),
        ("RETURN 1 IS :: NULL AS v", "false"),
        ("RETURN null IS :: NOTHING AS v", "false"),
        ("RETURN null IS :: NULL NOT NULL AS v", "false"),
        // Lists, unions, ANY and PROPERTY VALUE.
        ("RETURN [1, 2] IS :: LIST<INTEGER> AS v", "true"),
        ("RETURN [1, 'a'] IS :: LIST<INTEGER> AS v", "false"),
        ("RETURN [1, 'a'] IS :: LIST<INTEGER | STRING> AS v", "true"),
        ("RETURN [1, 'a'] IS :: LIST AS v", "true"),
        ("RETURN [1, 2, 3] IS :: INTEGER LIST[2] AS v", "false"),
        ("RETURN [1, null] IS :: LIST<INTEGER NOT NULL> AS v", "false"),
        ("RETURN 'a' IS :: INTEGER | STRING AS v", "true"),
        ("RETURN 1.5 IS :: ANY<INTEGER | STRING> AS v", "false"),
        ("RETURN [1] IS :: ANY AS v", "true"),
        ("RETURN null IS :: ANY NOT NULL AS v", "false"),
        ("RETURN 1 IS :: PROPERTY VALUE AS v", "true"),
        ("RETURN [1] IS :: PROPERTY VALUE AS v", "false"),
        // Reference and path types.
        ("MATCH (n:Hub) RETURN n IS :: NODE AS v", "true"),
        ("MATCH (n:Hub) RETURN n IS :: ANY EDGE AS v", "false"),
        ("MATCH ()-[e:HAS]->() RETURN e IS :: RELATIONSHIP AS v LIMIT 1", "true"),
        ("MATCH p = (n:Hub)-[:HAS]->() RETURN p IS :: PATH AS v LIMIT 1", "true"),
    ] {
        assert_eq!(value(query).await, answer, "{query}");
    }
}

#[tokio::test]
async fn a_value_type_obeys_its_syntax_rules() {
    let syntax = GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION;
    for query in [
        // §18.9 Syntax Rules 7, 13, 29, 32, 40, 42, 73 and 88.
        "RETURN 'a' IS :: STRING(0) AS v",
        "RETURN 'a' IS :: STRING(3, 2) AS v",
        "RETURN 'a' IS :: CHAR(0) AS v",
        "RETURN 1 IS :: INT(0) AS v",
        "RETURN 1 IS :: INT(257) AS v",
        "RETURN 1.0 IS :: FLOAT(1) AS v",
        "RETURN 1.0 IS :: FLOAT(300) AS v",
        "RETURN [1] IS :: LIST<INTEGER>[0] AS v",
        "RETURN 1 IS :: INTEGER | STRING NOT NULL AS v",
    ] {
        assert_eq!(status(query).await, syntax, "{query}");
    }
    let unsupported = GqlStatus::FEATURE_NOT_SUPPORTED;
    for query in [
        "RETURN 1 IS :: GRAPH AS v",
        "RETURN 1 IS :: UUID AS v",
        "RETURN 1 IS :: VECTOR AS v",
        "RETURN 1 IS :: POINT AS v",
    ] {
        assert_eq!(status(query).await, unsupported, "{query}");
    }
}

// ---- §20.8 cast specification (GA05) ----

#[tokio::test]
async fn cast_converts_between_types() {
    for (query, answer) in [
        // General Rule 4 j): to a string, the value's shortest literal.
        ("RETURN CAST(1 AS STRING) AS v", "\"1\""),
        ("RETURN CAST(-12 AS VARCHAR) AS v", "\"-12\""),
        ("RETURN CAST(1.5 AS STRING) AS v", "\"1.5E0\""),
        ("RETURN CAST(100.0 AS STRING) AS v", "\"1.0E2\""),
        ("RETURN CAST(0.0 AS STRING) AS v", "\"0E0\""),
        ("RETURN CAST(true AS STRING) AS v", "\"TRUE\""),
        ("RETURN CAST('abc' AS CHAR(2)) AS v", "\"ab\""),
        ("RETURN CAST('a' AS CHAR(3)) AS v", "\"a  \""),
        // g) and h): to an exact number, rounding a half away from zero
        // (IA005); a string is read as a literal, its whitespace trimmed.
        ("RETURN CAST(' 12 ' AS INTEGER) AS v", "12"),
        ("RETURN CAST('0x1F' AS INT) AS v", "31"),
        ("RETURN CAST('1_000' AS INT32) AS v", "1000"),
        ("RETURN CAST('1.5' AS INTEGER) AS v", "2"),
        ("RETURN CAST(2.5 AS INTEGER) AS v", "3"),
        ("RETURN CAST(-2.5 AS INTEGER) AS v", "-3"),
        ("RETURN CAST('255' AS UINT8) AS v", "255"),
        // i): to an approximate number, the nearest value of its format.
        ("RETURN CAST(1 AS FLOAT) AS v", "1.0f"),
        ("RETURN CAST('1e2' AS DOUBLE) AS v", "100.0f"),
        ("RETURN CAST(0.1 AS FLOAT16) AS v", "0.0999755859375f"),
        ("RETURN CAST(0.1 AS FLOAT32) AS v", "0.10000000149011612f"),
        // q): to a Boolean, a Boolean literal.
        ("RETURN CAST(' False ' AS BOOLEAN) AS v", "false"),
        ("RETURN CAST('true' AS BOOL) AS v", "true"),
        ("RETURN CAST('unknown' AS BOOLEAN) AS v", "null"),
        // d): a list, element by element.
        ("RETURN CAST([1, 2] AS LIST<STRING>) AS v", "[\"1\", \"2\"]"),
        // a) and c): a union or reference type keeps the value.
        ("RETURN CAST(1 AS INTEGER | STRING) AS v", "1"),
        ("RETURN CAST(1 AS ANY) AS v", "1"),
        ("MATCH (n:Hub) RETURN CAST(n AS NODE) = n AS v", "true"),
        // 2): the null value stays null.
        ("RETURN CAST(NULL AS INTEGER) AS v", "null"),
        ("MATCH (n:Hub) RETURN CAST(n.missing AS STRING) AS v", "null"),
        // Over a group.
        ("MATCH (n:Node) RETURN CAST(count(*) AS STRING) AS v", "\"5\""),
    ] {
        assert_eq!(value(query).await, answer, "{query}");
    }
}

#[tokio::test]
async fn cast_raises_the_conversion_conditions() {
    for (query, condition) in [
        ("RETURN CAST('abc' AS INTEGER) AS v", GqlStatus::INVALID_CHARACTER_VALUE_FOR_CAST),
        ("RETURN CAST('-1' AS UINT8) AS v", GqlStatus::INVALID_CHARACTER_VALUE_FOR_CAST),
        ("RETURN CAST('1__0' AS INTEGER) AS v", GqlStatus::INVALID_CHARACTER_VALUE_FOR_CAST),
        ("RETURN CAST('x' AS FLOAT) AS v", GqlStatus::INVALID_CHARACTER_VALUE_FOR_CAST),
        ("RETURN CAST('yes' AS BOOLEAN) AS v", GqlStatus::INVALID_CHARACTER_VALUE_FOR_CAST),
        ("RETURN CAST(300 AS INT8) AS v", GqlStatus::NUMERIC_VALUE_OUT_OF_RANGE),
        ("RETURN CAST(-1 AS UINT64) AS v", GqlStatus::NUMERIC_VALUE_OUT_OF_RANGE),
        ("RETURN CAST(70000 AS FLOAT16) AS v", GqlStatus::NUMERIC_VALUE_OUT_OF_RANGE),
        ("RETURN CAST(123 AS STRING(2)) AS v", GqlStatus::STRING_DATA_RIGHT_TRUNCATION),
        (
            "RETURN CAST([1, 2, 3] AS LIST<INTEGER>[2]) AS v",
            GqlStatus::LIST_DATA_RIGHT_TRUNCATION,
        ),
        // General Rule 2 b) ii): the null value to a material type.
        (
            "MATCH (n:Hub) RETURN CAST(n.missing AS INTEGER NOT NULL) AS v",
            GqlStatus::INVALID_VALUE_TYPE,
        ),
        // Table 4's invalid combinations, found as the cast runs.
        ("RETURN CAST(1 AS BOOLEAN) AS v", GqlStatus::INVALID_VALUE_TYPE),
        ("RETURN CAST(1.5 AS INTEGER | STRING) AS v", GqlStatus::INVALID_VALUE_TYPE),
        ("RETURN CAST('a' AS NODE) AS v", GqlStatus::INVALID_VALUE_TYPE),
    ] {
        assert_eq!(status(query).await, condition, "{query}");
    }
    // Syntax Rules 4 and 5.
    let syntax = GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION;
    assert_eq!(status("RETURN CAST(1 AS NULL) AS v").await, syntax);
    assert_eq!(status("RETURN CAST(1 AS NOTHING) AS v").await, syntax);
    assert_eq!(status("RETURN CAST(NULL AS INTEGER NOT NULL) AS v").await, syntax);
}

// ---- §20.18 records (GV45 to GV48) ----

#[tokio::test]
async fn a_record_holds_named_fields() {
    for (query, answer) in [
        // §20.18: a record constructor, with or without the word.
        ("RETURN {b: 2, a: 1} AS v", "{a: 1, b: 2}"),
        ("RETURN RECORD {a: 'x'} AS v", "{a: \"x\"}"),
        ("RETURN {} AS v", "{}"),
        // GV48: a record inside one.
        ("RETURN {a: {b: 1}} AS v", "{a: {b: 1}}"),
        // §20.11 General Rule 3: a field, or null when there is none.
        ("RETURN {a: 1}.a AS v", "1"),
        ("RETURN {a: 1}.z AS v", "null"),
        ("WITH {a: {b: 1}} AS r RETURN r.a.b AS v", "1"),
        ("WITH {id: 7} AS r RETURN r.id AS v", "7"),
        ("MATCH (n:Hub) WITH {name: n.name} AS r RETURN r.name AS v", "\"hub\""),
        // §19.3 General Rule 2 b) iii): field by field, the names alike.
        ("RETURN {a: 1, b: 2} = {b: 2, a: 1} AS v", "true"),
        ("RETURN {a: 1} = {a: 2} AS v", "false"),
        ("RETURN {a: 1} = {b: 1} AS v", "false"),
        ("RETURN {a: null} = {a: 1} AS v", "null"),
        ("UNWIND [{a: 1}, {a: 1}, {a: 2}] AS r RETURN count(DISTINCT r) AS v", "2"),
        // §4.15.4: record types, open and closed.
        ("RETURN {a: 1} IS :: RECORD AS v", "true"),
        ("RETURN {a: 1} IS :: ANY RECORD AS v", "true"),
        ("RETURN {a: 1} IS :: MAP AS v", "true"),
        ("RETURN {a: 1} IS :: RECORD {a INTEGER} AS v", "true"),
        ("RETURN {a: 1} IS :: {a :: STRING} AS v", "false"),
        ("RETURN {a: 1, b: 2} IS :: RECORD {a INTEGER} AS v", "false"),
        ("RETURN 1 IS :: RECORD AS v", "false"),
        ("RETURN {a: 1} IS :: PROPERTY VALUE AS v", "false"),
        // §20.8 General Rule 4 e).
        ("RETURN CAST({a: 1, b: 'x'} AS RECORD {a STRING}) AS v", "{a: \"1\"}"),
        ("RETURN CAST({a: 1} AS RECORD) AS v", "{a: 1}"),
    ] {
        assert_eq!(value(query).await, answer, "{query}");
    }
}

#[tokio::test]
async fn a_record_obeys_its_rules() {
    let syntax = GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION;
    // §20.19 Syntax Rule 2 and §18.10 Syntax Rule 2: names are distinct.
    assert_eq!(status("RETURN {a: 1, a: 2} AS v").await, syntax);
    assert_eq!(status("RETURN {a: 1} IS :: RECORD {a INTEGER, a STRING} AS v").await, syntax);
    // §20.1 Syntax Rules 4 and 5: a node's property value has no fields.
    assert_eq!(status("MATCH (n:Hub) RETURN n.name.first AS v").await, syntax);
    // General Rule 4 e) i) 2): the record lacks a field the type has.
    assert_eq!(
        status("RETURN CAST({a: 1} AS RECORD {b INTEGER}) AS v").await,
        GqlStatus::RECORD_FIELDS_DO_NOT_MATCH
    );
    // A field of a value that is not a record.
    assert_eq!(
        status("RETURN (CASE WHEN true THEN 'x' END).a AS v").await,
        GqlStatus::INVALID_VALUE_TYPE
    );
}

// ---- §4.16.6 temporal values (GV39 to GV41) ----

#[tokio::test]
async fn temporal_values_come_from_literals_and_functions() {
    for (query, answer) in [
        // §21.2: a literal's type follows its string.
        ("RETURN DATE '2026-01-31' AS v", "DATE '2026-01-31'"),
        ("RETURN TIME '12:30' AS v", "TIME '12:30:00.000000000'"),
        ("RETURN TIME '12:30+01:00' AS v", "TIME '12:30:00.000000000+01'"),
        (
            "RETURN TIMESTAMP '2026-01-01T08:00:00.5' AS v",
            "DATETIME '2026-01-01T08:00:00.500000000'",
        ),
        (
            "RETURN DATETIME '2026-01-01T08:00Z' AS v",
            "DATETIME '2026-01-01T08:00:00.000000000Z'",
        ),
        ("RETURN DURATION 'P1Y2M' AS v", "DURATION 'P1Y2M'"),
        // §20.28 General Rule 6: a day and time-based duration normalized.
        ("RETURN DURATION 'PT25H' AS v", "DURATION 'P1DT1H'"),
        // §20.27: from a string or a record's fields.
        ("RETURN date('2026-01-01') AS v", "DATE '2026-01-01'"),
        ("RETURN DATE({year: 2026, month: 2}) AS v", "DATE '2026-02-01'"),
        (
            "RETURN LOCAL_TIME({hour: 7, minute: 5, second: 3, millisecond: 9}) AS v",
            "TIME '07:05:03.009000000'",
        ),
        (
            "RETURN ZONED_TIME({hour: 12, timezone: '-05:30'}) AS v",
            "TIME '12:00:00.000000000-05:30'",
        ),
        (
            "RETURN LOCAL_DATETIME({year: 2026, month: 2, day: 3, hour: 4}) AS v",
            "DATETIME '2026-02-03T04:00:00.000000000'",
        ),
        (
            "RETURN zoned_datetime('2026-01-01T00:00:00+02') AS v",
            "DATETIME '2026-01-01T00:00:00.000000000+02'",
        ),
        // §20.29.
        ("RETURN duration('P1D') AS v", "DURATION 'P1D'"),
        ("RETURN DURATION({years: 1, months: 2}) AS v", "DURATION 'P1Y2M'"),
        (
            "RETURN DURATION({days: 1, hours: 2, nanoseconds: 5}) AS v",
            "DURATION 'P1DT2H0.000000005S'",
        ),
        // §20.27 General Rules 1 and 10 a): the request's own timestamp, once.
        ("RETURN CURRENT_DATE IS :: DATE AS v", "true"),
        ("RETURN CURRENT_TIME IS :: ZONED TIME AS v", "true"),
        ("RETURN LOCAL_TIME IS :: TIME WITHOUT TIME ZONE AS v", "true"),
        ("RETURN CURRENT_TIMESTAMP IS :: TIMESTAMP WITH TIME ZONE AS v", "true"),
        ("RETURN LOCAL_TIMESTAMP IS :: LOCAL DATETIME AS v", "true"),
        ("RETURN CURRENT_TIMESTAMP = ZONED_DATETIME() AS v", "true"),
        // A variable may still be named like one.
        ("WITH 1 AS current_date RETURN current_date AS v", "1"),
    ] {
        assert_eq!(value(query).await, answer, "{query}");
    }
}

#[tokio::test]
async fn temporal_arithmetic_follows_the_calendar() {
    for (query, answer) in [
        // §20.26 General Rule 4: past a month's end, its last day.
        ("RETURN DATE '2026-01-31' + DURATION 'P1M' AS v", "DATE '2026-02-28'"),
        ("RETURN DATE '2026-03-01' - DURATION 'P1D' AS v", "DATE '2026-02-28'"),
        ("RETURN DURATION 'P1D' + DATE '2026-03-01' AS v", "DATE '2026-03-02'"),
        (
            "RETURN DATETIME '2026-01-01T23:00' + DURATION 'PT1H30M' AS v",
            "DATETIME '2026-01-02T00:30:00.000000000'",
        ),
        // A time of day wraps at midnight and keeps its displacement.
        ("RETURN TIME '20:00+01:00' + DURATION 'PT5H' AS v", "TIME '01:00:00.000000000+01'"),
        // §20.28.
        ("RETURN DURATION 'P1Y' + DURATION 'P2M' AS v", "DURATION 'P1Y2M'"),
        ("RETURN DURATION 'PT1H' * 2.5 AS v", "DURATION 'PT2H30M'"),
        ("RETURN 2 * DURATION 'P5M' AS v", "DURATION 'P10M'"),
        ("RETURN DURATION 'PT1H' / 4 AS v", "DURATION 'PT15M'"),
        ("RETURN -DURATION 'P1M' AS v", "DURATION 'P-1M'"),
        ("RETURN abs(DURATION '-PT5S') AS v", "DURATION 'PT5S'"),
        (
            "RETURN DURATION_BETWEEN(DATE '2026-01-31', DATE '2026-03-30') AS v",
            "DURATION 'P58D'",
        ),
        (
            "RETURN DURATION_BETWEEN(DATE '2026-01-31', DATE '2026-03-30') YEAR TO MONTH AS v",
            "DURATION 'P1M'",
        ),
        (
            "RETURN DURATION_BETWEEN(TIME '12:00Z', TIME '13:00+02:00') AS v",
            "DURATION 'PT-1H'",
        ),
        // Table 1: an instant minus an instant is their duration.
        (
            "RETURN DATETIME '2026-01-01T10:00' - DATETIME '2026-01-01T08:30' AS v",
            "DURATION 'PT1H30M'",
        ),
        ("RETURN DATE '2026-01-01' + null AS v", "null"),
    ] {
        assert_eq!(value(query).await, answer, "{query}");
    }
    for (query, condition) in [
        (
            "RETURN DURATION 'P1M' + DURATION 'P1D' AS v",
            GqlStatus::INCOMPATIBLE_TEMPORAL_INSTANT_UNIT_GROUPS,
        ),
        ("RETURN DURATION 'PT1H' / 0 AS v", GqlStatus::DIVISION_BY_ZERO),
        (
            "RETURN DURATION_BETWEEN(DATE '2026-01-01', TIME '12:00') AS v",
            GqlStatus::VALUES_NOT_COMPARABLE,
        ),
        ("RETURN DURATION_BETWEEN(1, DATE '2026-01-01') AS v", GqlStatus::INVALID_VALUE_TYPE),
        ("RETURN DATE '2026-01-01' + DATE '2026-01-01' AS v", GqlStatus::INVALID_VALUE_TYPE),
        (
            "RETURN DATE '9999-12-31' + DURATION 'P1000000Y' AS v",
            GqlStatus::DATETIME_FIELD_OVERFLOW,
        ),
    ] {
        assert_eq!(status(query).await, condition, "{query}");
    }
}

#[tokio::test]
async fn temporal_values_compare_within_their_type() {
    for (query, answer) in [
        ("RETURN DATE '2026-01-01' < DATE '2026-01-02' AS v", "true"),
        // Zoned values compare as the instants they denote.
        ("RETURN TIME '12:00+01:00' = TIME '11:00Z' AS v", "true"),
        (
            "RETURN DATETIME '2026-01-01T00:00+01:00' < DATETIME '2025-12-31T23:30Z' AS v",
            "true",
        ),
        ("RETURN DURATION 'PT36H' > DURATION 'P1D' AS v", "true"),
        // §4.16.6: values of different types are not essentially comparable.
        ("RETURN DATE '2026-01-01' = DATETIME '2026-01-01T00:00' AS v", "false"),
        ("RETURN DATE '2026-01-01' < DATETIME '2026-01-01T00:00' AS v", "null"),
        ("RETURN DURATION 'P1M' < DURATION 'P30D' AS v", "null"),
    ] {
        assert_eq!(value(query).await, answer, "{query}");
    }
    assert_eq!(
        sorted_column("UNWIND [DATE '2026-03-01', DATE '2026-01-01', DATE '2026-02-01'] AS d RETURN CAST(d AS STRING) AS v").await,
        ["\"2026-01-01\"", "\"2026-02-01\"", "\"2026-03-01\""]
    );
    assert_eq!(
        value("UNWIND [DATE '2026-03-01', DATE '2026-01-01'] AS d WITH d ORDER BY d LIMIT 1 RETURN d AS v").await,
        "DATE '2026-01-01'"
    );
}

#[tokio::test]
async fn temporal_types_test_and_cast() {
    for (query, answer) in [
        ("RETURN DATE '2026-01-01' IS :: DATE AS v", "true"),
        ("RETURN TIME '12:00Z' IS :: TIME WITH TIME ZONE AS v", "true"),
        ("RETURN TIME '12:00' IS :: ZONED TIME AS v", "false"),
        ("RETURN DATETIME '2026-01-01T00:00' IS :: TIMESTAMP AS v", "true"),
        ("RETURN DURATION 'P1Y' IS :: DURATION(YEAR TO MONTH) AS v", "true"),
        ("RETURN DURATION 'P1Y' IS :: DURATION(DAY TO SECOND) AS v", "false"),
        ("RETURN DURATION 'PT1S' IS :: DURATION AS v", "true"),
        ("RETURN DATE '2026-01-01' IS :: PROPERTY VALUE AS v", "false"),
        // §20.8 General Rule 4 j) iv): the literal's string.
        ("RETURN CAST(DATE '2026-01-01' AS STRING) AS v", "\"2026-01-01\""),
        ("RETURN CAST(DURATION 'P14M' AS STRING) AS v", "\"P1Y2M\""),
        // k) to p): a string read as the target's literal.
        ("RETURN CAST(' 2026-01-01 ' AS DATE) AS v", "DATE '2026-01-01'"),
        ("RETURN CAST('12:00:01' AS LOCAL TIME) AS v", "TIME '12:00:01.000000000'"),
        ("RETURN CAST('PT90M' AS DURATION(DAY TO SECOND)) AS v", "DURATION 'PT1H30M'"),
        // Instant casts extract typed fields; n) vi) adds the source displacement.
        ("RETURN CAST(DATETIME '2026-01-02T03:04+05:00' AS DATE) AS v", "DATE '2026-01-02'"),
        (
            "RETURN CAST(DATETIME '2026-01-02T03:04+05:00' AS LOCAL DATETIME) AS v",
            "DATETIME '2026-01-02T08:04:00.000000000'",
        ),
        (
            "RETURN CAST(DATE '2026-01-02' AS LOCAL DATETIME) AS v",
            "DATETIME '2026-01-02T00:00:00.000000000'",
        ),
        (
            "RETURN CAST(DATETIME '2026-01-02T03:04' AS ZONED DATETIME) AS v",
            "DATETIME '2026-01-02T03:04:00.000000000Z'",
        ),
        ("RETURN CAST(TIME '03:04' AS ZONED DATETIME) IS :: ZONED DATETIME AS v", "true"),
    ] {
        assert_eq!(value(query).await, answer, "{query}");
    }
    for (query, condition) in [
        ("RETURN CAST('2026-13-01' AS DATE) AS v", GqlStatus::INVALID_DATETIME_FORMAT),
        ("RETURN CAST('12:00Z' AS LOCAL TIME) AS v", GqlStatus::INVALID_DATETIME_FORMAT),
        (
            "RETURN CAST('P1D' AS DURATION(YEAR TO MONTH)) AS v",
            GqlStatus::INVALID_DURATION_FORMAT,
        ),
        ("RETURN CAST(TIME '12:00' AS DATE) AS v", GqlStatus::INVALID_VALUE_TYPE),
    ] {
        assert_eq!(status(query).await, condition, "{query}");
    }
}

#[tokio::test]
async fn temporal_strings_and_records_are_checked() {
    let syntax = GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION;
    // §21.2 Syntax Rules 59, 62, 64 and 68: a literal's string conforms.
    for query in [
        "RETURN DATE '2026-13-01' AS v",
        "RETURN TIME '25:00' AS v",
        "RETURN DATETIME '2026-01-01' AS v",
    ] {
        assert_eq!(status(query).await, syntax, "{query}");
    }
    for (query, condition) in [
        // Syntax Rule 71 c): a duration of both unit groups.
        ("RETURN DURATION 'P1Y2D' AS v", GqlStatus::INVALID_DURATION_FORMAT),
        ("RETURN duration('nonsense') AS v", GqlStatus::INVALID_DURATION_FORMAT),
        ("RETURN date('2026/01/01') AS v", GqlStatus::INVALID_DATETIME_FORMAT),
        ("RETURN local_time('12:00Z') AS v", GqlStatus::INVALID_DATETIME_FORMAT),
        // §20.27 General Rules 2 to 6.
        ("RETURN DATE({month: 1}) AS v", GqlStatus::INVALID_DATETIME_FUNCTION_FIELD_NAME),
        (
            "RETURN LOCAL_TIME({minute: 1}) AS v",
            GqlStatus::INVALID_DATETIME_FUNCTION_FIELD_NAME,
        ),
        (
            "RETURN LOCAL_TIME({hour: 1, timezone: 'Z'}) AS v",
            GqlStatus::INVALID_DATETIME_FUNCTION_FIELD_NAME,
        ),
        (
            "RETURN DATE({year: 2026, month: 13}) AS v",
            GqlStatus::INVALID_DATETIME_FUNCTION_VALUE,
        ),
        (
            "RETURN LOCAL_TIME({hour: 1, minute: 0, second: 0, millisecond: 1000}) AS v",
            GqlStatus::INVALID_DATETIME_FUNCTION_VALUE,
        ),
        // §20.29 General Rule 2 c).
        ("RETURN DURATION({weeks: 1}) AS v", GqlStatus::INVALID_DURATION_FUNCTION_FIELD_NAME),
    ] {
        assert_eq!(status(query).await, condition, "{query}");
    }
}

// ---- §4.16.5 exact decimals (GV17, GL05, GL06) ----

#[tokio::test]
async fn numeric_common_notation_keeps_its_exact_or_approximate_type() {
    // §21.2 common notation and suffixes, §21.4 SR2 Nd digits; X17 still
    // makes an unsuffixed common-notation literal approximate.
    for (query, answer) in [
        ("RETURN 9.D AS v", "9.0f"),
        ("RETURN 9.M AS v", "9M"),
        ("RETURN .25M AS v", "0.25M"),
        ("RETURN 9.E+1M AS v", "90M"),
        ("RETURN 9.E-1M AS v", "0.9M"),
        ("RETURN ٠٩.٢٥M AS v", "9.25M"),
        ("RETURN 09.25M AS v", "9.25M"),
        ("RETURN CAST('9.D' AS FLOAT) AS v", "9.0f"),
        ("RETURN CAST('9.M' AS DECIMAL(3, 1)) AS v", "9.0M"),
        ("RETURN CAST('٩.٢٥M' AS DECIMAL(3, 1)) AS v", "9.3M"),
        ("RETURN CAST('-٩.٢٥M' AS DECIMAL(3, 1)) AS v", "-9.3M"),
        ("RETURN CAST('٠٩' AS INTEGER) AS v", "9"),
        ("RETURN CAST('٩.E+١D' AS FLOAT) AS v", "90.0f"),
        ("RETURN 9.D IS :: DECIMAL AS v", "false"),
        ("RETURN 9.M IS :: FLOAT AS v", "false"),
        ("RETURN 9.M + 0.10M AS v", "9.10M"),
        ("RETURN CAST('0.1_25M' AS DECIMAL(3, 2)) AS v", "0.13M"),
    ] {
        assert_eq!(value(query).await, answer, "{query}");
    }
    for spelling in ["9_.M", "9._M", "9E_1M", "9__1M", ".M", "٩__١M", "٩._٢M"] {
        let query = format!("RETURN CAST('{spelling}' AS DECIMAL(4, 1)) AS v");
        assert_eq!(status(&query).await, GqlStatus::INVALID_CHARACTER_VALUE_FOR_CAST, "{query}");
    }
    assert_eq!(status("RETURN 9.E38M AS v").await, GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION);
}

#[tokio::test]
async fn numeric_rounding_and_byte_helpers_preserve_type_and_null_dispatch() {
    // §20.22, §20.23, §20.25: exact rounding, byte operations and nulls.
    for (query, answer) in [
        ("RETURN floor(-1.5M) AS v", "-2M"),
        ("RETURN ceil(1.5M) AS v", "2M"),
        ("RETURN floor(2.00M) AS v", "2M"),
        ("RETURN ceil(-2.00M) AS v", "-2M"),
        ("RETURN floor(2) AS v", "2"),
        ("RETURN ceil(-1.5) AS v", "-1.0f"),
        ("RETURN floor(null) AS v", "null"),
        ("RETURN X'' || X'00' AS v", "X'00'"),
        ("RETURN X'00' || null AS v", "null"),
        ("RETURN null || X'00' AS v", "null"),
        ("RETURN 'e' || '\u{301}' AS v", "\"é\""),
        ("RETURN TRIM(BOTH null FROM X'000100') AS v", "null"),
        ("RETURN TRIM(BOTH X'00' FROM null) AS v", "null"),
        ("RETURN TRIM(BOTH null FROM ' abc ') AS v", "null"),
        ("RETURN TRIM(BOTH X'00' FROM X'0000') AS v", "X''"),
    ] {
        assert_eq!(value(query).await, answer, "{query}");
    }
    for query in [
        "RETURN X'00' || 'a' AS v",
        "RETURN 'a' || X'00' AS v",
        "RETURN TRIM('a' FROM X'00') AS v",
    ] {
        assert_eq!(status(query).await, GqlStatus::INVALID_VALUE_TYPE, "{query}");
    }
}

#[tokio::test]
async fn an_exact_decimal_keeps_its_digits() {
    for (query, answer) in [
        // §21.2: the suffix `M` makes a number exact; `1.5` stays a FLOAT
        // (X17).
        ("RETURN 1.50M AS v", "1.50M"),
        ("RETURN 2M AS v", "2M"),
        ("RETURN 1.5E2M AS v", "150M"),
        ("RETURN 1.5 AS v", "1.5f"),
        // §20.21 Syntax Rule 2 b): the larger scale for a sum, the scales'
        // total for a product, six more for a quotient (ID067).
        ("RETURN 1.25M + 0.5M AS v", "1.75M"),
        ("RETURN 0.1M + 0.2M = 0.3M AS v", "true"),
        ("RETURN 1.25M * 2 AS v", "2.50M"),
        ("RETURN 1M / 3M AS v", "0.333333M"),
        ("RETURN 7.5M % 2 AS v", "1.5M"),
        ("RETURN -1.5M AS v", "-1.5M"),
        // A float operand makes the result a float.
        ("RETURN 1.5M + 1.5 AS v", "3.0f"),
        // Numbers compare by value.
        ("RETURN 1.0M = 1 AS v", "true"),
        ("RETURN 1.5M > 1 AS v", "true"),
        ("RETURN 1.5M = 1.5 AS v", "true"),
        ("UNWIND [1.0M, 1M, 1.00M] AS d RETURN count(DISTINCT d) AS v", "1"),
        // §20.22.
        ("RETURN abs(-1.5M) AS v", "1.5M"),
        ("RETURN floor(1.5M) AS v", "1M"),
        ("RETURN ceil(-1.5M) AS v", "-1M"),
        ("RETURN sqrt(4M) AS v", "2.0f"),
        // §20.9: a sum stays exact, an average a decimal quotient.
        ("UNWIND [1.1M, 2.2M] AS d RETURN sum(d) AS v", "3.3M"),
        ("UNWIND [1M, 2M] AS d RETURN avg(d) AS v", "1.500000M"),
        // §18.9: DECIMAL(p, s) holds the exact numbers of p digits, s after
        // the point; an exact number of integral value is an integer.
        ("RETURN 1.5M IS :: DECIMAL(2, 1) AS v", "true"),
        ("RETURN 1.55M IS :: DECIMAL(3, 1) AS v", "false"),
        ("RETURN 2.0M IS :: INTEGER AS v", "true"),
        ("RETURN 1.5M IS :: FLOAT AS v", "false"),
        ("RETURN 1 IS :: DEC AS v", "true"),
        // §20.8: rounded to the scale, a half away from zero (IA005).
        ("RETURN CAST(1.25 AS DECIMAL(3, 1)) AS v", "1.3M"),
        ("RETURN CAST('1.005' AS DECIMAL(4, 2)) AS v", "1.01M"),
        ("RETURN CAST(1.50M AS STRING) AS v", "\"1.50\""),
        ("RETURN CAST(2.5M AS INTEGER) AS v", "3"),
        ("RETURN CAST(1.5M AS FLOAT) AS v", "1.5f"),
    ] {
        assert_eq!(value(query).await, answer, "{query}");
    }
    let syntax = GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION;
    for query in [
        "RETURN 1 IS :: DECIMAL(0) AS v",
        "RETURN 1 IS :: DECIMAL(39) AS v",
        "RETURN 1 IS :: DECIMAL(2, 3) AS v",
        "RETURN 1234567890123456789012345678901234567890M AS v",
    ] {
        assert_eq!(status(query).await, syntax, "{query}");
    }
    assert_eq!(status("RETURN 1M / 0 AS v").await, GqlStatus::DIVISION_BY_ZERO);
    assert_eq!(
        status("RETURN CAST(123.4M AS DECIMAL(3, 1)) AS v").await,
        GqlStatus::NUMERIC_VALUE_OUT_OF_RANGE
    );
}

// ---- §4.16.4 byte strings (GV35 to GV38, GF07) ----

#[tokio::test]
async fn byte_continuations_use_comment_separators_not_comment_digits() {
    // §21.2 SR54–58, GR9; §21.3 SR15–16. Includes all six K19 pins.
    for left in ["", "A5", "00FF"] {
        for right in ["", "C3"] {
            for separator in [
                " /*\n*/ ",
                "\n",
                "\r",
                "\r\n",
                " \t\u{2003}\n\t ",
                " /* cafe dead ' not a piece */\n",
                "/* a*\n'ff' **/",
                " // 'dead' cafe\r\n ",
                "/*no newline*/\n/*dead*/",
            ] {
                let query = format!("RETURN X'{left}'{separator}'{right}' AS v");
                assert_eq!(value(&query).await, format!("X'{left}{right}'"), "{query:?}");
                assert_eq!(outcome(&query).await.status.status.code(), "00000");
            }
        }
    }
    assert_eq!(value("RETURN X'AB'/*\n*/'CD'// ff '\n'EF' AS v").await, "X'ABCDEF'");
    for query in [
        "RETURN X'AB' 'CD' AS v",
        "RETURN X'AB'/*dead*/'CD' AS v",
        "RETURN X'AB'/*dead*/ 'CD'\n'EF' AS v",
        "RETURN X'AB'\n'CD' /*dead*/'EF' AS v",
        "RETURN X'A'\n'B' AS v",
        "RETURN X'AB'\n'G0' AS v",
        "RETURN X'AB'/*\n*/'C' AS v",
        "RETURN X'AB'\n'\\x43' AS v",
    ] {
        assert_eq!(status(query).await, GqlStatus::INVALID_SYNTAX, "{query:?}");
    }
    // Cypher string escapes/doubled quotes and -- arithmetic remain unchanged.
    assert_eq!(value("RETURN 'AB''CD' AS v").await, "\"AB'CD\"");
    assert_eq!(value("RETURN 1--1 AS v").await, "2");
}

#[tokio::test]
async fn a_byte_string_holds_bytes() {
    for (query, answer) in [
        ("RETURN X'00FF' AS v", "X'00FF'"),
        ("RETURN x'00 ff' AS v", "X'00FF'"),
        ("RETURN X'' AS v", "X''"),
        // §21.2 Syntax Rule 57: a continuation follows a newline.
        ("RETURN X'00'\n  'FF' AS v", "X'00FF'"),
        // §20.23, §20.25.
        ("RETURN X'01' || X'02' AS v", "X'0102'"),
        ("RETURN LEFT(X'010203', 2) AS v", "X'0102'"),
        ("RETURN RIGHT(X'010203', 1) AS v", "X'03'"),
        ("RETURN TRIM(X'000100') AS v", "X'01'"),
        ("RETURN TRIM(LEADING X'00' FROM X'000100') AS v", "X'0100'"),
        ("RETURN TRIM(BOTH X'01' FROM X'0102') AS v", "X'02'"),
        ("RETURN BYTE_LENGTH(X'0102') AS v", "2"),
        ("RETURN OCTET_LENGTH(X'') AS v", "0"),
        // §4.16.4: any two byte strings compare, byte by byte.
        ("RETURN X'01' < X'0101' AS v", "true"),
        ("RETURN X'02' > X'0101' AS v", "true"),
        ("RETURN X'01' = X'01' AS v", "true"),
        // §18.9 Syntax Rules 22 to 27.
        ("RETURN X'00' IS :: BYTES AS v", "true"),
        ("RETURN X'0001' IS :: BINARY(2) AS v", "true"),
        ("RETURN X'0001' IS :: BYTES(3, 4) AS v", "false"),
        ("RETURN X'0001' IS :: VARBINARY(1) AS v", "false"),
        // §20.8 General Rule 4 r).
        ("RETURN CAST(X'01' AS BINARY(3)) AS v", "X'010000'"),
        ("RETURN CAST(X'010203' AS BYTES(2)) AS v", "X'0102'"),
    ] {
        assert_eq!(value(query).await, answer, "{query}");
    }
    for (query, condition) in [
        ("RETURN TRIM(X'0001' FROM X'00') AS v", GqlStatus::TRIM_ERROR),
        ("RETURN LEFT(X'00', -1) AS v", GqlStatus::SUBSTRING_ERROR),
        ("RETURN CAST('a' AS BYTES) AS v", GqlStatus::INVALID_VALUE_TYPE),
        ("RETURN X'00' || 'a' AS v", GqlStatus::INVALID_VALUE_TYPE),
    ] {
        assert_eq!(status(query).await, condition, "{query}");
    }
    let invalid = GqlStatus::INVALID_SYNTAX;
    for query in ["RETURN X'0' AS v", "RETURN X'00' 'FF' AS v"] {
        assert_eq!(status(query).await, invalid, "{query}");
    }
    let syntax = GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION;
    assert_eq!(status("RETURN X'00' IS :: BYTES(0) AS v").await, syntax);
    assert_eq!(status("RETURN X'00' IS :: BYTES(3, 2) AS v").await, syntax);
}

#[tokio::test]
async fn simplified_restrictions_are_semantic_syntax_rules() {
    // §16.12 SR1–4 constrain a grammatically complete algebra, not its tokens.
    for wrapper in ["!({})", "<({})", "R&({})"] {
        for contents in ["R R", "R{2}", "R?", "R |+| S"] {
            let expression = wrapper.replace("{}", contents);
            let query = format!("MATCH (a)-/{expression}/->(b) RETURN count(*) AS c");
            assert_eq!(status(&query).await, GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION, "{query}");
        }
    }
    for expression in ["<(<R)", "~(R>)", "-(~R)", "<(<R>)>"] {
        let query = format!("MATCH (a)-/{expression}/->(b) RETURN count(*) AS c");
        assert_eq!(status(&query).await, GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION, "{query}");
    }
    for expression in ["R & & S", "(R|S", "!", "R{"] {
        let query = format!("MATCH (a)-/{expression}/->(b) RETURN count(*) AS c");
        assert_eq!(status(&query).await, GqlStatus::INVALID_SYNTAX, "{query}");
    }
    // Single-edge union/conjunction/negation remain labels; overrides remain directions.
    for (expression, count) in [
        ("(R|HAS)", "9"),
        ("R&(R|HAS)", "6"),
        ("!(HAS&HAS)", "6"),
        ("<R", "6"),
        ("R>", "6"),
        ("-R", "11"),
    ] {
        let query = format!("MATCH (a)-/{expression}/->(b) RETURN count(*) AS c");
        assert_eq!(value(&query).await, count, "{query}");
    }
}

#[tokio::test]
async fn normalized_factors_match_a_finite_multigraph_model() {
    use common::MemoryStorage;
    use hydradb_cypher_engine::{RelationshipRecord, VertexRecord};
    type Path = (Vec<u64>, Vec<usize>);
    let edges = [(0_u64, 1_u64, "R"), (1, 2, "R"), (2, 0, "R"), (0, 0, "R"), (0, 1, "R"), (2, 1, "S")];
    let bag = |rows: &[hydradb_cypher_engine::QueryRow]| {
        let mut rows = rows.iter().map(|row| format!("{:?}", row.values)).collect::<Vec<_>>();
        rows.sort();
        rows
    };
    // §16.7 SR14–16, §16.12 SR6 and §22.3/§22.6: enumerate edge
    // identities, not endpoint pairs. A self-loop has one orientation;
    // parallel edges remain distinct and no trail reuses an identity.
    for mask in [0_u8, 1, 3, 15, 63] {
        let graph = MemoryStorage::with_graph(
            (0..3).map(|n| {
                VertexRecord::new(10 + n)
                    .with_label("N")
                    .with_label(if n % 2 == 0 { "Even" } else { "Odd" })
                    .with_property("k", ScalarValue::Integer(i128::from(n)))
            }),
            edges
                .iter()
                .enumerate()
                .filter(|(i, _)| mask & (1 << i) != 0)
                .map(|(i, (a, b, kind))| RelationshipRecord::new(100 + i as u64, 10 + a, 10 + b, *kind)),
        );
        let engine = CypherEngine::new(graph);
        for start in 0..3 {
            let rows = |paths: Vec<Path>| {
                paths
                    .into_iter()
                    .map(|(nodes, used)| {
                        let mut word = vec![QueryValue::VertexId(10 + nodes[0])];
                        for (edge, node) in used.into_iter().zip(&nodes[1..]) {
                            word.extend([QueryValue::RelationshipId(100 + edge as u64), QueryValue::VertexId(10 + node)]);
                        }
                        hydradb_cypher_engine::QueryRow {
                            values: vec![
                                QueryValue::Scalar(ScalarValue::Integer(i128::from(*nodes.last().unwrap()))),
                                QueryValue::List(word),
                            ],
                        }
                    })
                    .collect::<Vec<_>>()
            };
            for (direction, left, right) in [(0, "-", "->"), (1, "<-", "-"), (2, "-", "-")] {
                let trails = |steps: &[(&str, u8)]| {
                    let mut paths = vec![(vec![start], Vec::<usize>::new())];
                    for (label, direction) in steps {
                        let mut next = Vec::new();
                        for (nodes, used) in paths {
                            let here = *nodes.last().unwrap();
                            for (i, (a, b, kind)) in edges.iter().enumerate() {
                                if mask & (1 << i) == 0 || used.contains(&i) || kind != label {
                                    continue;
                                }
                                let mut ends = std::collections::BTreeSet::new();
                                if *direction != 1 && *a == here {
                                    ends.insert(*b);
                                }
                                if *direction != 0 && *b == here {
                                    ends.insert(*a);
                                }
                                for end in ends {
                                    let mut n = nodes.clone();
                                    n.push(end);
                                    let mut e = used.clone();
                                    e.push(i);
                                    next.push((n, e));
                                }
                            }
                        }
                        paths = next;
                    }
                    paths
                };
                for (expression, sequences) in [
                    ("R |+| R", vec![vec!["R"], vec!["R"]]),
                    ("R R | S", vec![vec!["R", "R"], vec!["S"]]),
                    ("(R R | S) R", vec![vec!["R", "R", "R"], vec!["S", "R"]]),
                    ("R (R R | S)", vec![vec!["R", "R", "R"], vec!["R", "S"]]),
                    ("(R |+| R) R", vec![vec!["R", "R"], vec!["R", "R"]]),
                    ("R R | R R", vec![vec!["R", "R"]]),
                    ("R{0,1}", vec![vec![], vec!["R"]]),
                    ("R{2}", vec![vec!["R", "R"]]),
                    ("R S", vec![vec!["R", "S"]]),
                ] {
                    let expected = rows(
                        sequences
                            .into_iter()
                            .flat_map(|labels| trails(&labels.into_iter().map(|label| (label, direction)).collect::<Vec<_>>()))
                            .collect(),
                    );
                    let query =
                        format!("MATCH p = (a:N {{k: {start}}}){left}/{expression}/{right}(b) RETURN b.k AS target, ELEMENTS(p) AS word");
                    let actual = engine
                        .execute_with_status(&query, &ParameterMap::new())
                        .await
                        .unwrap_or_else(|e| panic!("mask={mask}, {query}: {e:?}"));
                    assert_eq!(bag(&actual.result.rows), bag(&expected), "mask={mask}, {query}");
                }
                let expected = rows(trails(&[("R", direction), ("R", 1)]));
                let query = format!("MATCH p = (a:N {{k: {start}}}){left}/R <R/{right}(b) RETURN b.k AS target, ELEMENTS(p) AS word");
                let actual = engine
                    .execute_with_status(&query, &ParameterMap::new())
                    .await
                    .unwrap_or_else(|e| panic!("{query}: {e:?}"));
                assert_eq!(bag(&actual.result.rows), bag(&expected), "mask={mask}, {query}");
                // Parenthesized edge factors use the identical adapter;
                // adjacent endpoint aliases must not duplicate path nodes.
                let edge = format!("{left}[:R]{right}");
                for (pattern, steps) in [
                    (format!("(a:N {{k: {start}}})((s){edge}(t))(b)"), vec![("R", direction)]),
                    (
                        format!("((a:N {{k: {start}}}){edge}(x))((y){edge}(b))"),
                        vec![("R", direction), ("R", direction)],
                    ),
                ] {
                    let expected = rows(trails(&steps));
                    let query = format!("MATCH p = {pattern} RETURN b.k AS target, ELEMENTS(p) AS word");
                    let actual = engine
                        .execute_with_status(&query, &ParameterMap::new())
                        .await
                        .unwrap_or_else(|e| panic!("{query}: {e:?}"));
                    assert_eq!(bag(&actual.result.rows), bag(&expected), "mask={mask}, {query}");
                }
                let expected = rows([trails(&[("R", direction)]), trails(&[("S", direction)])].concat());
                let edge_r = format!("{left}[:R]{right}");
                let edge_s = format!("{left}[:S]{right}");
                let query =
                    format!("MATCH p = (s:N {{k: {start}}})((a){edge_r}(b) | (a){edge_s}(b))(t) RETURN t.k AS target, ELEMENTS(p) AS word");
                let actual = engine
                    .execute_with_status(&query, &ParameterMap::new())
                    .await
                    .unwrap_or_else(|e| panic!("{query}: {e:?}"));
                assert_eq!(bag(&actual.result.rows), bag(&expected), "mask={mask}, {query}");
            }
            // All 45 parenthesized-adjacent-node pins, including a projected
            // path alias: the shared node appears once in ELEMENTS.
            for label in ["N", "Even", "Odd"] {
                let expected = if label == "N" || (label == "Even") == (start % 2 == 0) {
                    rows(vec![(vec![start], vec![])])
                } else {
                    vec![]
                };
                let query = format!("MATCH p = (a:N {{k: {start}}})((b:{label})) WITH p AS q, b RETURN b.k AS target, ELEMENTS(q) AS word");
                let actual = engine
                    .execute_with_status(&query, &ParameterMap::new())
                    .await
                    .unwrap_or_else(|e| panic!("{query}: {e:?}"));
                assert_eq!(bag(&actual.result.rows), bag(&expected), "mask={mask}, {query}");
            }
        }
    }
}

#[tokio::test]
async fn composition_does_not_erase_unrepresented_scopes_or_operators() {
    for query in [
        "MATCH (a)-/R?/->(b) RETURN b",
        "MATCH (a)-/((R |+| R) | S)/->(b) RETURN b",
        "MATCH (a)-/(R R | S)(R R | S)/->(b) RETURN b",
        "MATCH (a)((b) WHERE false) RETURN b",
        "MATCH (a)((q = (b))) RETURN b",
    ] {
        assert_eq!(status(query).await, GqlStatus::FEATURE_NOT_SUPPORTED, "{query}");
    }
    assert_eq!(
        value("MATCH ((a) WHERE COALESCE(false, 1 / 0 > 0)) RETURN count(*) AS c").await,
        "0"
    );
}

// ---- §14.11 return statement ----

#[tokio::test]
async fn quantified_factor_minima_precede_execution_subset_checks() {
    // §16.7 SR14–16 and SR20: implicit nodes are inside the repeated
    // primary. Flattening's outer endpoints must not make them mandatory.
    for query in [
        "MATCH ((a)-[:R]->(b)){0,1} RETURN count(*) AS c",
        "MATCH -[e:R]->{0,1} RETURN count(*) AS c",
        "MATCH (()-[e:R]->){0,1} RETURN count(*) AS c",
        "MATCH (-[e:R]->()){0,1} RETURN count(*) AS c",
        "MATCH (q = ((a)-[:R]->(b)){0,1}) RETURN ELEMENTS(q) AS elements",
        "MATCH (x)(q = ((a)-[:R]->(b)){0,1})(y) RETURN count(*) AS c",
        "MATCH ((a)){1} RETURN count(*) AS c",
        "MATCH (((a)-[:R]->(b)){1,2}){1,2} RETURN count(*) AS c",
    ] {
        assert_eq!(status(query).await, GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION, "{query}");
    }
    // §16.7 SR14 c), d): bare edges and either missing endpoint receive
    // boundary nodes. SR14 a) puts a repeated edge's nodes inside its primary.
    for pattern in ["-[e:R]->", "()-[e:R]->", "-[e:R]->()", "()-[e:R]->()"] {
        assert_eq!(value(&format!("MATCH {pattern} RETURN count(*) AS c")).await, "6", "{pattern}");
    }
    for pattern in ["-[e:R]->{1}", "()-[e:R]->{1}", "-[e:R]->{1}()", "()-[e:R]->{1}()"] {
        assert_eq!(value(&format!("MATCH {pattern} RETURN count(*) AS c")).await, "6", "{pattern}");
    }
    // An explicit endpoint outside the quantifier is mandatory: twelve
    // zero-hop matches (one per fixture node) plus six one-edge matches.
    for pattern in ["()-[e:R]->{0,1}", "-[e:R]->{0,1}()", "()-[e:R]->{0,1}()"] {
        assert_eq!(value(&format!("MATCH {pattern} RETURN count(*) AS c")).await, "18", "{pattern}");
    }
    assert_eq!(value("MATCH (:Node {name: 'a'})-[e:R]->{0,1} RETURN count(*) AS c").await, "3");
    assert_eq!(value("MATCH -[e:R]->{0,1}(:Node {name: 'a'}) RETURN count(*) AS c").await, "2");
    // The six two-edge trails are a-b-d, a-c-d, b-d-a, c-d-a,
    // d-a-b and d-a-c; the single loop edge cannot repeat in a trail.
    assert_eq!(value("MATCH (a)-[:R]->-[:R]->(b) RETURN count(*) AS c").await, "6");
    assert_eq!(value("MATCH ((a)-[:R]->(b)){1} RETURN count(*) AS c").await, "6");
    // A real mandatory endpoint permits zero repetitions. From a: the
    // zero-length match, a->b and a->c, with their complete group lists.
    assert_eq!(
        table(&run("MATCH (q = (x:Node {name: 'a'})((a)-[e:R]->(b)){0,1}(y)) RETURN path_length(q) AS n, size(a) AS starts, size(b) AS ends, size(e) AS edges ORDER BY n").await),
        [["0", "0", "0", "0"], ["1", "1", "1", "1"], ["1", "1", "1", "1"]]
    );
}

#[tokio::test]
async fn exact_one_repetition_still_has_group_exposure() {
    // §16.4 SR3 and §16.7 SR22: {1} exposes lists, not singletons.
    // Labels/properties cannot hide an illegal join of a group variable.
    for query in [
        "MATCH REPEATABLE ELEMENTS ((a:M {key: 0})-[:R]->(b)){1}, (b:M) RETURN count(*) AS v",
        "MATCH REPEATABLE ELEMENTS ((a)-[e:R]->(b)){1}, (b) RETURN count(*) AS c",
        "MATCH REPEATABLE ELEMENTS ((a)-[e:R]->(b)){1}, ()-[e:R]->() RETURN count(*) AS c",
        "MATCH REPEATABLE ELEMENTS (a)((a)-[:R]->(b)){1} RETURN count(*) AS c",
    ] {
        assert_eq!(status(query).await, GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION, "{query}");
    }
    assert_eq!(
        value("MATCH REPEATABLE ELEMENTS (a)-[:R]->(b), (b) RETURN count(*) AS c").await,
        "6"
    );
    assert_eq!(value("MATCH ((a)-[e:R]->(b)){1} RETURN min(size(a)) AS n").await, "1");
    assert_eq!(value("MATCH ((a:M {key: 0})-[:R]->(b)){1} RETURN count(*) AS c").await, "0");
}

#[tokio::test]
async fn quantified_predicates_keep_their_matching_depth() {
    // §16.7 SR9 and §22.6: each iteration evaluates its predicate; EXISTS
    // opens a different matching depth and may reuse the outer trail's edges.
    for predicate in [
        "EXISTS { (b)-[:R]->{1,2}(t) }",
        "EXISTS { MATCH (b)-[:R]->{1,2}(t) RETURN t }",
        "EXISTS { MATCH (b)-[:R]->{1,2}(t) WHERE t <> b RETURN t }",
        "false",
    ] {
        let query = format!("MATCH (s:N {{k: 0}})((a)-[:R]->(b) WHERE {predicate}){{1,2}}(z) RETURN z.k AS target");
        assert!(run(&query).await.rows.is_empty(), "{query}");
    }
    assert_eq!(
        status("MATCH ((a)-[:R]->(b)-[:R]->(c)){1,2} RETURN count(*) AS c").await,
        GqlStatus::FEATURE_NOT_SUPPORTED
    );
}

#[tokio::test]
async fn quantified_predicates_match_a_finite_edge_identity_model() {
    use common::MemoryStorage;
    use hydradb_cypher_engine::{RelationshipRecord, VertexRecord};
    // §16.7 SR9, §22.3, §22.6: the inner matching has its own trail;
    // outer paths retain multiplicity, but cannot reuse an edge identity.
    let edges = [(0_u64, 1_u64, "R"), (1, 2, "R"), (2, 0, "R"), (0, 0, "R"), (0, 1, "R"), (2, 1, "S")];
    for mask in [0_u8, 1, 3, 15, 63] {
        let present: Vec<_> = edges.iter().enumerate().filter(|(i, _)| mask & (1 << i) != 0).collect();
        let graph = MemoryStorage::with_graph(
            (0..3).map(|n| {
                VertexRecord::new(10 + n)
                    .with_label("N")
                    .with_property("k", ScalarValue::Integer(i128::from(n)))
            }),
            present
                .iter()
                .map(|(i, (a, b, kind))| RelationshipRecord::new(100 + *i as u64, 10 + a, 10 + b, *kind)),
        );
        let engine = CypherEngine::new(graph);
        for start in 0..3 {
            let mut expected = Vec::new();
            for (first, (a, middle, kind)) in &present {
                if *a != start || *kind != "R" {
                    continue;
                }
                // Positive bounded EXISTS is true iff an outgoing R exists.
                if !present.iter().any(|(_, (a, _, kind))| a == middle && *kind == "R") {
                    continue;
                }
                expected.push(vec![middle.to_string()]);
                for (second, (a, end, kind)) in &present {
                    if first != second && a == middle && *kind == "R" && present.iter().any(|(_, (a, _, kind))| a == end && *kind == "R") {
                        expected.push(vec![end.to_string()]);
                    }
                }
            }
            expected.sort();
            for body in ["(b)-[:R]->{1,2}(t)", "MATCH (b)-[:R]->{1,2}(t) RETURN t"] {
                let query = format!("MATCH (s:N {{k: {start}}})((a)-[:R]->(b) WHERE EXISTS {{ {body} }}){{1,2}}(z) RETURN z.k AS target");
                let outcome = engine
                    .execute_with_status(&query, &ParameterMap::new())
                    .await
                    .unwrap_or_else(|e| panic!("mask={mask}, {query}: {e:?}"));
                let mut actual = table(&outcome.result);
                actual.sort();
                assert_eq!(actual, expected, "mask={mask}, start={start}, body={body}");
                assert_eq!(
                    outcome.status.status,
                    if expected.is_empty() {
                        GqlStatus::NO_DATA
                    } else {
                        GqlStatus::SUCCESS
                    }
                );
            }
        }
    }
}

#[tokio::test]
async fn quantified_predicates_use_singleton_scope_and_keep_group_records() {
    let query = |predicate: &str| {
        format!("WITH 'd' AS wanted MATCH (s:Node {{name: 'a'}})((a)-[e:R]->(b) WHERE {predicate}){{1,2}}(z) RETURN count(*) AS c")
    };
    // §22.3/§22.6: every step, not merely the last one, must pass. A deeper
    // query correlates singleton b, while its own WITH may shadow that b.
    for (predicate, expected) in [
        ("b.name = wanted", "0"),
        ("EXISTS { MATCH (b) WHERE b.name = wanted RETURN b }", "0"),
        ("EXISTS { WITH 1 AS b RETURN b }", "4"),
        ("b.name <> 'a'", "4"),
        ("COALESCE(true, 1 / 0 = 1)", "4"),
    ] {
        assert_eq!(value(&query(predicate)).await, expected, "{predicate}");
    }
    assert_eq!(
        value("MATCH (s:Node {name: 'a'})((a:Node)-[:R]->(b:Node {name: 'b'})){1,2}(z) RETURN count(*) AS c").await,
        "1"
    );
    assert_eq!(value("MATCH ((a)-[:R]->(a)){1} RETURN count(*) AS c").await, "1");
    assert_eq!(table(&run("MATCH (s:Node {name: 'a'})((a)-[:R]->(b) WHERE b.name <> 'a'){1,2}(z) WITH b AS bs UNWIND bs AS n RETURN n.name AS name ORDER BY name").await), [["\"b\""], ["\"b\""], ["\"c\""], ["\"c\""], ["\"d\""], ["\"d\""]]);
    assert_eq!(
        status("MATCH ((a)-[:R]->(b) WHERE count(*) > 0){1} RETURN b").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    for predicate in ["b = z", "EXISTS { MATCH (z) RETURN z }"] {
        let query = format!("MATCH (s:Node {{name: 'a'}})((a)-[:R]->(b) WHERE {predicate}){{1,2}}(z) RETURN z");
        assert_eq!(status(&query).await, GqlStatus::FEATURE_NOT_SUPPORTED, "{query}");
    }
    assert_eq!(
        status("MATCH ((a)-[:R]->(b) WHERE missing.name = 'a'){1} RETURN b").await,
        GqlStatus::INVALID_REFERENCE
    );
}

#[tokio::test]
async fn quantified_predicates_read_the_incoming_working_table() {
    // §14.7, §14.8 and §22.6: iteration predicates read the current
    // working-table row, including values that are not pattern anchors.
    let engine = CypherEngine::new(graph());
    for (source, predicate, expected) in [
        ("UNWIND [1] AS wanted", "wanted = 1", "6"),
        ("UNWIND [0] AS wanted", "wanted = 1", "0"),
        ("UNWIND [null] AS wanted", "wanted = 1", "0"),
        ("UNWIND [] AS wanted", "wanted = 1", "0"),
        ("UNWIND [0, 1, 2, 1, null] AS wanted", "wanted = 1", "12"),
        ("FOR wanted IN [1]", "wanted = 1", "6"),
        ("FOR wanted IN [0, 1, 1, null] WITH ORDINALITY pos", "wanted = 1 AND pos = 2", "6"),
        ("FOR wanted IN [0, 1, 1, null] WITH OFFSET pos", "wanted = 1 AND pos = 2", "6"),
        ("LET wanted = 1", "wanted = 1", "6"),
        ("LET wanted = 0", "wanted = 1", "0"),
        ("LET wanted = null", "wanted = 1", "0"),
        ("UNWIND [0, 1] AS wanted LET copied = wanted", "copied = 1", "6"),
        ("MATCH (n:Node) UNWIND [n] AS wanted", "wanted.name = a.name", "6"),
        (
            "MATCH (n:Node) UNWIND [n] AS wanted",
            "EXISTS { FILTER wanted.name = a.name RETURN 1 }",
            "6",
        ),
    ] {
        let query = format!("{source} MATCH ((a)-[:R]->(b) WHERE {predicate}){{1}} RETURN count(*) AS c");
        let outcome = engine
            .execute_with_status(&query, &ParameterMap::new())
            .await
            .unwrap_or_else(|error| panic!("{query}: {error} ({})", error.gql_status()));
        assert_eq!(table(&outcome.result), [[expected]], "{query}");
        assert_eq!(outcome.status.status, GqlStatus::SUCCESS, "{query}");
    }
    let query =
        "MATCH (u:Hub)-[r:HAS]->() UNWIND [r] AS wanted MATCH ((a)-[e:HAS]->(b) WHERE wanted.weight = e.weight){1} RETURN count(*) AS c";
    let outcome = engine.execute_with_status(query, &ParameterMap::new()).await.unwrap();
    assert_eq!(table(&outcome.result), [["3"]]);
    assert_eq!(outcome.status.status, GqlStatus::SUCCESS);
    for query in [
        "UNWIND ['a', 'b', null] AS wanted MATCH ((a)-[:R]->(b:Node {name: wanted})){1} RETURN count(*) AS c",
        "FOR wanted IN [1, 2] MATCH ((a)-[e:HAS {weight: wanted}]->(b)){1} RETURN count(*) AS c",
        "UNWIND [1, 2] AS wanted MATCH ((a)-[e:HAS WHERE e.weight = wanted]->(b)){1} RETURN count(*) AS c",
    ] {
        let outcome = engine.execute_with_status(query, &ParameterMap::new()).await.unwrap();
        assert_eq!(table(&outcome.result), [["2"]], "{query}");
        assert_eq!(outcome.status.status, GqlStatus::SUCCESS, "{query}");
    }
    for query in [
        "UNWIND [1] AS wanted MATCH (s:Node)((a)-[:R]->(b) WHERE wanted = 1 AND b = z){1}(z) RETURN z",
        "UNWIND [1] AS wanted MATCH ((a)-[:R]->(b) WHERE wanted = 1 AND c.name = 'a'){1}, (c:Node) RETURN c",
        "UNWIND [1] AS wanted MATCH p = ((a)-[:R]->(b) WHERE wanted = 1 AND path_length(p) = 1){1} RETURN p",
    ] {
        assert_eq!(status(query).await, GqlStatus::FEATURE_NOT_SUPPORTED, "{query}");
    }
    for query in [
        "UNWIND [1] AS wanted WITH 1 AS kept MATCH ((a)-[:R]->(b) WHERE wanted = 1){1} RETURN b",
        "UNWIND [1] AS wanted MATCH ((a)-[:R]->(b) WHERE missing = wanted){1} RETURN b",
    ] {
        assert_eq!(status(query).await, GqlStatus::INVALID_REFERENCE, "{query}");
    }
}

#[tokio::test]
async fn constrained_iteration_keeps_captured_and_incoming_records() {
    // §22.2, §22.6 and §22.7: iteration-local singletons and captured
    // group/path values have distinct lifetimes. The fixture's two trails
    // from a through {b,c} to d determine these bags independently.
    let engine = CypherEngine::new(graph());
    let pattern = "(s:Node {name: 'a'})((a)-[e:R]->(b) WHERE a.name <> 'never' AND b.name <> 'never'){2}(z)";
    let cases = [
        (
            format!("MATCH (h:Hub) LET wanted = 1 MATCH {pattern} RETURN h.name, wanted, z.name, size(a), size(b), size(e)"),
            vec![vec!["\"hub\"", "1", "\"d\"", "2", "2", "2"]; 2],
        ),
        (
            format!("MATCH {pattern} WITH a AS captured UNWIND captured AS n RETURN n.name AS name ORDER BY name"),
            vec![vec!["\"a\""], vec!["\"a\""], vec!["\"b\""], vec!["\"c\""]],
        ),
        (
            format!("MATCH {pattern} WITH b AS captured UNWIND captured AS n RETURN n.name AS name ORDER BY name"),
            vec![vec!["\"b\""], vec!["\"c\""], vec!["\"d\""], vec!["\"d\""]],
        ),
        (
            format!("MATCH {pattern} WITH e AS captured UNWIND captured AS r RETURN r AS edge ORDER BY edge"),
            vec![vec!["rel#4"], vec!["rel#5"], vec!["rel#6"], vec!["rel#7"]],
        ),
        (
            format!("MATCH {pattern} WITH min(z) AS picked RETURN picked.name"),
            vec![vec!["\"d\""]],
        ),
        (
            format!("MATCH {pattern} WITH collect(b) AS groups UNWIND groups AS bs UNWIND bs AS n RETURN n.name AS name ORDER BY name"),
            vec![vec!["\"b\""], vec!["\"c\""], vec!["\"d\""], vec!["\"d\""]],
        ),
        (
            format!("MATCH p = (q = {pattern}) WITH q AS alias UNWIND ELEMENTS(alias) AS n WITH n WHERE n:Node RETURN n.name AS name ORDER BY name"),
            vec![vec!["\"a\""], vec!["\"a\""], vec!["\"b\""], vec!["\"c\""], vec!["\"d\""], vec!["\"d\""]],
        ),
        (
            "MATCH (h:Hub) WITH coalesce(h, null) AS held MATCH (s:Node {name: 'a'})((a)-[e:R]->(b) WHERE EXISTS { FILTER EXISTS { FILTER a.name <> 'never' AND b.name <> 'never' AND held.name = 'hub' AND e IS DIRECTED RETURN 1 } RETURN 1 }){2}(z) RETURN held.name, z.name".to_owned(),
            vec![vec!["\"hub\"", "\"d\""]; 2],
        ),
        (
            "MATCH (h:Hub) WITH min(h) AS held MATCH (s:Node {name: 'a'})((a)-[:R]->(b) WHERE held.name = 'hub'){2}(z) RETURN held.name, z.name".to_owned(),
            vec![vec!["\"hub\"", "\"d\""]; 2],
        ),
        (
            "MATCH (h:Hub)-[r:HAS {weight: 1}]->() WITH coalesce(r, null) AS held MATCH (s:Node {name: 'a'})((a)-[:R]->(b) WHERE held.weight = 1){2}(z) RETURN held.weight, z.name".to_owned(),
            vec![vec!["1", "\"d\""]; 2],
        ),
        (
            "MATCH (s:Hub)((a)-[:R]->(b) WHERE 1 / 0 = 0){0,1}(z) RETURN z.name, size(b)".to_owned(),
            vec![vec!["\"hub\"", "0"]],
        ),
        (
            "MATCH (s:Node {name: 'a'})((a)-[:R]->(b) WHERE a.name <> 'c'){2}(z) RETURN z.name".to_owned(),
            vec![vec!["\"d\""]],
        ),
        (
            format!("MATCH {pattern} WITH b AS incoming UNWIND incoming AS held MATCH (s:Node {{name: 'a'}})((a)-[:R]->(b) WHERE a.name <> 'c' AND held.name = 'd'){{2}}(z) RETURN held.name, z.name"),
            vec![vec!["\"d\"", "\"d\""]; 2],
        ),
    ];
    for (query, expected) in cases {
        let outcome = engine
            .execute_with_status(&query, &ParameterMap::new())
            .await
            .unwrap_or_else(|error| panic!("{query}: {error} ({})", error.gql_status()));
        assert_eq!(table(&outcome.result), expected, "{query}");
        assert_eq!(outcome.status.status, GqlStatus::SUCCESS, "{query}");
    }

    let query = format!("MATCH p = {pattern} RETURN p");
    let outcome = engine.execute_with_status(&query, &ParameterMap::new()).await.unwrap();
    assert_eq!(outcome.status.status, GqlStatus::SUCCESS);
    assert_eq!(outcome.result.rows.len(), 2);
    let stored = graph();
    let mut identities = Vec::new();
    for row in outcome.result.rows {
        let QueryValue::Path(path) = &row.values[0] else {
            panic!("expected a path")
        };
        for node in &path.nodes {
            assert_eq!(node, &stored.vertices[&node.id], "path nodes keep labels and properties");
        }
        for edge in &path.relationships {
            assert_eq!(edge, stored.relationships.iter().find(|record| record.id == edge.id).unwrap());
        }
        identities.push((
            path.nodes.iter().map(|n| n.id).collect::<Vec<_>>(),
            path.relationships.iter().map(|e| e.id).collect::<Vec<_>>(),
        ));
    }
    identities.sort();
    assert_eq!(
        identities,
        [(vec![1020, 1021, 1023], vec![4, 6]), (vec![1020, 1022, 1023], vec![5, 7])]
    );
}

#[tokio::test]
async fn quantified_predicates_correlate_incoming_values_against_a_multigraph_model() {
    use common::MemoryStorage;
    use hydradb_cypher_engine::{RelationshipRecord, VertexRecord};
    // §14.8 GR2 and §22.2/§22.6: each incoming row has its own predicate
    // value. Parallel edge identities and duplicate input rows remain bags;
    // every hop must pass, and an outer trail cannot reuse an edge identity.
    let edges = [(0_u64, 1_u64, "R"), (1, 2, "R"), (2, 0, "R"), (0, 0, "R"), (0, 1, "R"), (2, 1, "S")];
    for mask in [0_u8, 1, 3, 15, 63] {
        let present: Vec<_> = edges.iter().enumerate().filter(|(i, _)| mask & (1 << i) != 0).collect();
        let graph = MemoryStorage::with_graph(
            (0..3).map(|n| {
                VertexRecord::new(10 + n)
                    .with_label("N")
                    .with_property("k", ScalarValue::Integer(i128::from(n)))
            }),
            present
                .iter()
                .map(|(i, (a, b, kind))| RelationshipRecord::new(100 + *i as u64, 10 + a, 10 + b, *kind)),
        );
        let engine = CypherEngine::new(graph);
        for start in 0..3 {
            let mut expected = Vec::new();
            // The null input cannot make an equality true (§19.2).
            for wanted in [0, 1, 1, 2] {
                for (first, (a, middle, kind)) in &present {
                    if *a != start || *middle != wanted || *kind != "R" {
                        continue;
                    }
                    expected.push(vec![wanted.to_string(), middle.to_string(), "1".to_owned()]);
                    for (second, (a, end, kind)) in &present {
                        if first != second && a == middle && *end == wanted && *kind == "R" {
                            expected.push(vec![wanted.to_string(), end.to_string(), "2".to_owned()]);
                        }
                    }
                }
            }
            expected.sort();
            for source in ["UNWIND [null, 0, 1, 1, 2] AS wanted", "FOR wanted IN [null, 0, 1, 1, 2]"] {
                for predicate in ["b.k = wanted", "EXISTS { FILTER b.k = wanted RETURN 1 }"] {
                    let query = format!("{source} MATCH p = (s:N {{k: {start}}})((a)-[e:R]->(b) WHERE {predicate}){{1,2}}(z) RETURN wanted AS wanted, z.k AS target, path_length(p) AS n");
                    let outcome = engine
                        .execute_with_status(&query, &ParameterMap::new())
                        .await
                        .unwrap_or_else(|error| panic!("mask={mask}, {query}: {error}"));
                    let mut actual = table(&outcome.result);
                    actual.sort();
                    assert_eq!(actual, expected, "mask={mask}, {query}");
                    assert_eq!(
                        outcome.status.status,
                        if expected.is_empty() {
                            GqlStatus::NO_DATA
                        } else {
                            GqlStatus::SUCCESS
                        },
                        "mask={mask}, {query}"
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn return_star_needs_variables_and_no_group_by() {
    // Syntax Rule 6.
    assert_eq!(status("RETURN *").await, GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION);
    assert_eq!(
        status("MATCH (n:Hub) RETURN * GROUP BY n").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
}

#[tokio::test]
async fn with_group_by_every_item_groups_or_aggregates() {
    // Syntax Rule 10 a) iii).
    assert_eq!(
        status("MATCH (i:Item) RETURN i.kind AS k, i.score AS s, count(*) AS c GROUP BY k").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
}

#[tokio::test]
async fn without_group_by_an_aggregating_return_covers_the_whole_table() {
    // General Rule 3 b), read as §14.10 Syntax Rule 4 and §14.12 Syntax Rule
    // 12 require (query-language/gql/README.md, "Where the text needs a reading"): with no
    // GROUP BY, a return whose items all aggregate is one row over the whole
    // working table, as `GROUP BY ()` would give.
    assert_eq!(table(&run("MATCH (i:Item) RETURN count(*) AS c").await), [["6"]]);
    assert_eq!(table(&run("MATCH (i:Missing) RETURN count(*) AS c").await), [["0"]]);
}

#[tokio::test]
async fn return_columns_have_distinct_names() {
    assert_eq!(
        status("RETURN 1 AS a, 2 AS a").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    assert_eq!(
        status("MATCH (n:Hub) RETURN n, n").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    assert_eq!(
        status("MATCH (n:Hub) RETURN n, 1 AS n").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
}

#[tokio::test]
async fn group_by_may_not_name_an_aggregating_item() {
    assert_eq!(
        status("MATCH (i:Item) RETURN i.kind AS k, count(*) AS c GROUP BY k, c").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
        "Corrigendum 1: grouping and aggregating items are disjoint"
    );
    let rows = table(&run("MATCH (i:Item) RETURN i.kind AS k, count(*) AS c GROUP BY k ORDER BY k").await);
    assert!(!rows.is_empty());
}

// ---- §16.4 graph pattern, §16.17 sort specification list ----

#[tokio::test]
async fn node_and_edge_variables_have_different_names() {
    // §16.4 Syntax Rule 5.
    assert_eq!(
        status("MATCH (a)-[a]->(b) RETURN count(*) AS c").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
}

#[tokio::test]
async fn abbreviated_edges_match_and_leave_arithmetic_alone() {
    // §16.7 G044, G045: `->`, `<-`, `-` and `<->` from the grammar overlay.
    assert_eq!(value("MATCH (a:Node {name: 'a'})->(b) RETURN count(*) AS c").await, "2");
    assert_eq!(value("MATCH (a:Node {name: 'a'})<-(b) RETURN count(*) AS c").await, "1");
    assert_eq!(value("MATCH (a:Node {name: 'a'})-(b) RETURN count(*) AS c").await, "3");
    assert_eq!(value("MATCH (a:Node {name: 'a'})<->(b) RETURN count(*) AS c").await, "3");
    // Outside a graph pattern the same text is still arithmetic and comparison.
    assert_eq!(
        table(&run("WITH 3 AS a, 1 AS b RETURN (a)-(b) AS v, (a)<-(b) AS w").await),
        [["2", "false"]]
    );
    // DIFFERENT EDGES is the default match mode, spelled out (G002).
    assert_eq!(value("MATCH DIFFERENT EDGES (a:Node)-[]->(b) RETURN count(*) AS c").await, "6");
}

#[tokio::test]
async fn nulls_sort_after_every_value() {
    // IS001, the same in every sort (§16.17 Syntax Rule 6): nulls are greater
    // than every value, so last ascending and first descending.
    let rows = table(&run("MATCH (i:Item) RETURN i.score AS s ORDER BY s").await);
    assert_eq!(rows.first().unwrap(), &["0"]);
    assert_eq!(rows.last().unwrap(), &["null"]);
    let rows = table(&run("MATCH (i:Item) RETURN i.score AS s ORDER BY s DESC").await);
    assert_eq!(rows.first().unwrap(), &["null"]);
}

#[tokio::test]
async fn explicit_null_ordering_places_nulls_whatever_the_direction() {
    // §16.17 General Rule 1 g) ii): NULLS FIRST and NULLS LAST place the
    // null score against the others, ascending or descending.
    let scores = |query: &'static str| async move { table(&run(query).await).into_iter().map(|row| row[0].clone()).collect::<Vec<_>>() };
    assert_eq!(
        scores("MATCH (i:Item) RETURN i.score AS s ORDER BY s NULLS FIRST").await,
        ["null", "0", "1", "2", "3", "5"]
    );
    assert_eq!(
        scores("MATCH (i:Item) RETURN i.score AS s ORDER BY s DESC NULLS LAST").await,
        ["5", "3", "2", "1", "0", "null"]
    );
    // The implicit order (IS001) is the same as these.
    assert_eq!(
        scores("MATCH (i:Item) RETURN i.score AS s ORDER BY s ASC NULLS LAST").await,
        ["0", "1", "2", "3", "5", "null"]
    );
    assert_eq!(
        scores("MATCH (i:Item) RETURN i.score AS s ORDER BY s DESC NULLS FIRST").await,
        ["null", "5", "3", "2", "1", "0"]
    );
}

// ---- §20.9 aggregate function ----

#[tokio::test]
async fn an_aggregate_appears_only_where_it_can_aggregate() {
    let syntax = GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION;
    // Syntax Rule 4: no aggregate inside another.
    assert_eq!(status("MATCH (i:Item) RETURN count(count(*)) AS c").await, syntax);
    // Syntax Rule 7, §20.1 Syntax Rule 7: only a projection item or a sort
    // key aggregates.
    assert_eq!(status("MATCH (n:Node) WHERE count(*) > 1 RETURN n").await, syntax);
    assert_eq!(status("MATCH (n:Node {name: count(*)}) RETURN n").await, syntax);
    assert_eq!(status("MATCH (i:Item) FILTER count(*) > 1 RETURN i").await, syntax);
    // Syntax Rule 3: no subquery inside an aggregate. EXISTS over a graph
    // pattern has none, so it may be aggregated.
    assert_eq!(
        status("MATCH (n:Node) RETURN count(EXISTS { MATCH (n)-[:R]->(m) RETURN m }) AS c").await,
        syntax
    );
    assert_eq!(value("MATCH (n:Node) RETURN count(EXISTS { (n)-[:R]->() }) AS c").await, "5");
}

#[tokio::test]
async fn standard_deviations_and_percentiles() {
    // GF10: the scores are 3, 1, 2, 5 and 0 (one item has none), so the
    // mean is 2.2 and the squared deviations sum to 14.8.
    let rows = table(&run("MATCH (i:Item) RETURN stddev_samp(i.score) AS s, stddev_pop(i.score) AS p").await);
    assert_eq!(rows, [["1.9235384061671346f", "1.7204650534085253f"]]);
    // General Rule 7 a): no values give null, and STDDEV_SAMP of one too.
    assert_eq!(
        value("MATCH (i:Item {name: 'carrot'}) RETURN stddev_samp(i.score) AS s").await,
        "null"
    );
    assert_eq!(value("MATCH (i:Item {name: 'carrot'}) RETURN stddev_pop(i.score) AS s").await, "0f");
    assert_eq!(value("MATCH (i:Missing) RETURN stddev_pop(i.score) AS s").await, "null");
    // GF11, General Rule 7 b): position 1 + p * (N - 1) of 0, 1, 2, 3, 5.
    // At p = 0.3 that is 2.2: CONT interpolates 0.8 * 1 + 0.2 * 2, DISC
    // rounds to position 2 (IA005).
    let rows = table(
        &run("MATCH (i:Item) RETURN percentile_cont(i.score, 0.3) AS c, percentile_disc(i.score, 0.3) AS d, percentile_cont(i.score, 1) AS top").await,
    );
    assert_eq!(rows, [["1.2f", "1f", "5f"]]);
    assert_eq!(value("MATCH (i:Missing) RETURN percentile_cont(i.score, 0.5) AS v").await, "null");
    // A fraction outside [0, 1] names no position; the standard defines no
    // result and no condition, so a data exception with no subclass.
    assert_eq!(
        status("MATCH (i:Item) RETURN percentile_cont(i.score, 1.5) AS v").await,
        GqlStatus::DATA_EXCEPTION
    );
    // Syntax Rules 8 e) and 9 b): numeric inputs only; the engine finds a
    // string while running.
    assert_eq!(
        status("MATCH (i:Item) RETURN stddev_pop(i.name) AS v").await,
        GqlStatus::INVALID_VALUE_TYPE
    );
    // COLLECT_LIST leaves the null score out, with the warning of General
    // Rule 5.
    assert_eq!(value("MATCH (i:Item) RETURN size(collect_list(i.score)) AS n").await, "5");
    assert_eq!(
        outcome("MATCH (i:Item) RETURN collect_list(i.score) AS l").await.status.status,
        GqlStatus::NULL_VALUE_ELIMINATED_IN_SET_FUNCTION
    );
}

// ---- §20.3 value specification ----

#[tokio::test]
async fn session_user_is_the_sessions_authorization_identifier() {
    // §20.3: SESSION_USER is the session's authorization identifier; with
    // none given, the engine's is `anonymous` (ID061, IV015).
    assert_eq!(value("RETURN SESSION_USER AS u").await, "\"anonymous\"");
    let engine = CypherEngine::new(graph()).with_session_user("alice");
    let prepared = engine.prepare("RETURN session_user AS u", &ParameterMap::new()).unwrap();
    assert_eq!(table(&engine.execute_prepared(&prepared).await.unwrap()), [["\"alice\""]]);
    // A request cannot choose its own session user.
    let spoof = ParameterMap::from([(
        hydradb_cypher_engine::SESSION_USER_PARAMETER.to_string(),
        ScalarValue::String("mallory".into()),
    )]);
    let prepared = engine.prepare("RETURN SESSION_USER AS u", &spoof).unwrap();
    assert_eq!(table(&engine.execute_prepared(&prepared).await.unwrap()), [["\"alice\""]]);
    // A query that declares a variable with the name keeps Cypher's meaning.
    assert_eq!(value("UNWIND [1] AS session_user RETURN session_user AS v").await, "1");
}

// ---- §20.3 value specification (LIMIT and OFFSET, GQ12 and GQ13) ----

#[tokio::test]
async fn a_limit_or_offset_is_a_non_negative_integer() {
    // General Rules 2 to 4 of a non-negative integer specification.
    let null = ParameterMap::from([("p".to_string(), ScalarValue::Null)]);
    assert_eq!(
        status_with("MATCH (i:Item) RETURN i.name AS n ORDER BY n LIMIT $p", &null).await,
        GqlStatus::NULL_VALUE_NOT_ALLOWED
    );
    assert_eq!(
        status_with("MATCH (i:Item) RETURN i.name AS n ORDER BY n OFFSET $p", &null).await,
        GqlStatus::NULL_VALUE_NOT_ALLOWED
    );
    assert_eq!(
        status("MATCH (i:Item) RETURN i.name AS n ORDER BY n LIMIT 1.5E0").await,
        GqlStatus::INVALID_VALUE_TYPE
    );
    assert_eq!(
        status("MATCH (i:Item) RETURN i.name AS n ORDER BY n LIMIT -1").await,
        GqlStatus::NEGATIVE_LIMIT_VALUE
    );
    assert_eq!(
        status("MATCH (i:Item) RETURN i.name AS n ORDER BY n OFFSET 1.5E0").await,
        GqlStatus::INVALID_VALUE_TYPE
    );
    assert_eq!(
        status("MATCH (i:Item) RETURN i.name AS n ORDER BY n OFFSET -1").await,
        GqlStatus::NEGATIVE_LIMIT_VALUE
    );
}

// ---- §20.1, §20.11 property references; §20.12, §22.1 references ----

#[tokio::test]
async fn only_a_node_or_an_edge_has_properties() {
    // §20.1 Syntax Rules 4 and 5: the source of a property reference is a
    // node or an edge (a record needs GV45).
    assert_eq!(
        status("MATCH (n:Hub) RETURN n.name.first AS v").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
}

#[tokio::test]
async fn every_name_and_parameter_resolves() {
    // §20.12 Syntax Rule 8: a variable is in the incoming record.
    assert_eq!(status("MATCH (n:Hub) RETURN m AS v").await, GqlStatus::INVALID_REFERENCE);
    // §22.1 Syntax Rule 5: a parameter is one the request supplies.
    assert_eq!(status("RETURN $missing AS v").await, GqlStatus::INVALID_REFERENCE);
}

// ---- §14.10 primitive result statement ----

#[tokio::test]
async fn after_distinct_or_aggregation_a_sort_key_reads_only_return_items() {
    let syntax = GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION;
    // Syntax Rule 4 c) i) 2) A) III) and IV) (GA07, GQ14).
    assert_eq!(status("MATCH (i:Item) RETURN DISTINCT i.kind AS k ORDER BY i.score").await, syntax);
    assert_eq!(
        status("MATCH (i:Item) RETURN i.kind AS k, count(*) AS c ORDER BY i.score").await,
        syntax
    );
    let rows = table(&run("MATCH (i:Item) RETURN i.kind AS k, count(*) AS c GROUP BY k ORDER BY c, k").await);
    assert_eq!(rows, [["\"veg\"", "2"], ["\"fruit\"", "4"]]);
    // A grouping key may be read, and so may anything inside an aggregate.
    let rows = table(&run("MATCH (i:Item) RETURN i.kind AS k, count(*) AS c GROUP BY k ORDER BY max(i.score) DESC").await);
    assert_eq!(rows[0], ["\"veg\"", "2"]);
    // Without DISTINCT or aggregation any variable in scope may be read.
    let rows = table(&run("MATCH (n:Hub) RETURN n.name AS v ORDER BY n").await);
    assert_eq!(rows, [["\"hub\""]]);
}

#[tokio::test]
async fn a_sort_key_contains_no_subquery() {
    // Syntax Rule 4 c) i) 2) A) I) (GQ14).
    assert_eq!(
        status("MATCH (n:Node) RETURN n.name AS a ORDER BY EXISTS { MATCH (n)-[:R]->(m) RETURN m }").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
}

// ---- §14.7 let statement, §14.8 for statement ----

#[tokio::test]
async fn let_definitions_are_independent_and_new() {
    // §14.7 Syntax Rules 11 and 12 as Corrigendum 1 replaces them: each
    // definition sees only what came in, not its neighbours.
    assert_eq!(
        status("MATCH (i:Item) LET a = 1, b = a RETURN b").await,
        GqlStatus::INVALID_REFERENCE
    );
    // §15.1 Syntax Rule 9: the names LET adds are new.
    assert_eq!(
        status("MATCH (i:Item) LET i = 1 RETURN i").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    assert_eq!(value("MATCH (n:Hub) LET a = 1 LET b = a + 1 RETURN b").await, "2");
}

#[tokio::test]
async fn for_iterates_over_a_list_into_a_new_name() {
    // §14.8 Syntax Rule 10: the source is a list.
    assert_eq!(
        status("FOR x IN 5 RETURN x").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    let five = ParameterMap::from([("p".to_string(), ScalarValue::Integer(5))]);
    assert_eq!(status_with("FOR x IN $p RETURN x", &five).await, GqlStatus::INVALID_VALUE_TYPE);
    // §14.8 Syntax Rule 11: the element's name is new.
    assert_eq!(
        status("MATCH (n:Hub) FOR n IN [1] RETURN n").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    assert_eq!(table(&run("FOR x IN [1, 2] RETURN x").await), [["1"], ["2"]]);
}

#[tokio::test]
async fn a_sort_key_aggregates_only_when_the_result_does() {
    assert_eq!(
        status("MATCH (i:Item) RETURN i.kind AS k ORDER BY count(*)").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
        "Corrigendum 1: no group for count(*) to count"
    );
    assert_eq!(
        status("MATCH (i:Item) RETURN DISTINCT i.kind AS k ORDER BY count(*)").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    let rows = table(&run("MATCH (i:Item) RETURN i.kind AS k, count(*) AS c GROUP BY k ORDER BY count(*)").await);
    assert_eq!(rows, [["\"veg\"", "2"], ["\"fruit\"", "4"]]);
}

// ---- §14.9 order by and page statement, §14.3 nested query ----

#[tokio::test]
async fn a_standalone_order_by_orders_the_rows_in_flight() {
    let rows = table(&run("MATCH (i:Item) WHERE i.score IS NOT NULL ORDER BY i.score DESC RETURN i.score AS s").await);
    assert_eq!(rows, [["5"], ["3"], ["2"], ["1"], ["0"]]);
    let rows = table(&run("MATCH (i:Item) WHERE i.score IS NOT NULL ORDER BY i.score OFFSET 1 LIMIT 2 RETURN i.score AS s").await);
    assert_eq!(rows, [["1"], ["2"]]);
}

#[tokio::test]
async fn a_nested_query_specification_is_its_query() {
    let rows = table(&run("{ MATCH (n:Hub) RETURN n.name AS name }").await);
    assert_eq!(rows, [["\"hub\""]]);
    let rows = table(&run("{ MATCH (n:Hub) RETURN n.name AS name } UNION { MATCH (n:Node {name: 'a'}) RETURN n.name AS name }").await);
    assert_eq!(rows.len(), 2);
}

// ---- §4.9 GQL-status objects, §23 GQLSTATUS ----

async fn outcome(query: &str) -> hydradb_cypher_engine::ExecutionOutcome {
    CypherEngine::new(graph())
        .execute_with_status(query, &ParameterMap::new())
        .await
        .unwrap_or_else(|error| panic!("{query:?} failed: {error}"))
}

#[tokio::test]
async fn a_result_with_rows_is_successful_completion() {
    let outcome = outcome("MATCH (n:Hub) RETURN n.name AS name").await;
    assert_eq!(outcome.status.status, GqlStatus::SUCCESS);
    assert_eq!(outcome.status.description, "note: successful completion");
    assert!(outcome.additional.is_empty());
    // §23.2 General Rule 1: a completion condition's diagnostic record is
    // empty (GA08).
    assert!(outcome.status.diagnostic.is_none());
}

#[tokio::test]
async fn an_empty_result_is_no_data() {
    let outcome = outcome("MATCH (n:Missing) RETURN n").await;
    assert_eq!(outcome.status.status, GqlStatus::NO_DATA);
    assert_eq!(outcome.status.description, "note: no data");
}

#[tokio::test]
async fn an_aggregate_that_skips_a_null_warns() {
    // One item has no score, one has no name.
    for query in [
        "MATCH (i:Item) RETURN sum(i.score) AS total",
        "MATCH (i:Item) RETURN count(i.name) AS named",
        "MATCH (i:Item) RETURN i.kind AS kind, min(i.score) AS low GROUP BY kind",
        "MATCH (i:Item) WITH i.kind AS kind, max(i.score) AS high RETURN kind, high",
    ] {
        let outcome = outcome(query).await;
        assert_eq!(outcome.status.status, GqlStatus::NULL_VALUE_ELIMINATED_IN_SET_FUNCTION, "{query}");
        assert_eq!(outcome.status.description, "warn: null value eliminated in set function");
    }
    let outcome = outcome("MATCH (i:Item) RETURN count(*) AS all_rows").await;
    assert_eq!(
        outcome.status.status,
        GqlStatus::SUCCESS,
        "COUNT(*) reads no value and eliminates nothing"
    );
}

#[tokio::test]
async fn case_publishes_only_conditions_from_selected_subqueries() {
    // §20.7 GR2: only the selected CASE result is evaluated; §20.9 GR5b
    // raises 01G11 when an evaluated aggregate eliminates a null value.
    for (query, expected) in [
        ("RETURN CASE WHEN true THEN 1 ELSE EXISTS { UNWIND [null] AS x RETURN count(x) AS c } END AS v", GqlStatus::SUCCESS),
        ("RETURN CASE WHEN false THEN 1 ELSE EXISTS { UNWIND [null] AS x RETURN count(x) AS c } END AS v", GqlStatus::NULL_VALUE_ELIMINATED_IN_SET_FUNCTION),
        ("RETURN CASE WHEN false THEN EXISTS { UNWIND [null] AS x RETURN count(x) AS c } ELSE 1 END AS v", GqlStatus::SUCCESS),
        ("RETURN CASE 1 WHEN 1 THEN 1 ELSE EXISTS { UNWIND [null] AS x RETURN count(x) AS c } END AS v", GqlStatus::SUCCESS),
        ("RETURN EXISTS { UNWIND [null] AS x RETURN count(x) AS c } AS v", GqlStatus::NULL_VALUE_ELIMINATED_IN_SET_FUNCTION),
        ("UNWIND [null] AS x WITH count(x) AS c RETURN CASE WHEN true THEN c ELSE EXISTS { UNWIND [null] AS y RETURN count(y) AS n } END AS v", GqlStatus::NULL_VALUE_ELIMINATED_IN_SET_FUNCTION),
        ("RETURN CASE WHEN true THEN 1 ELSE EXISTS { RETURN CASE WHEN false THEN 1 ELSE EXISTS { UNWIND [null] AS x RETURN count(x) AS c } END AS v } END AS v", GqlStatus::SUCCESS),
        ("RETURN EXISTS { RETURN CASE WHEN true THEN 1 ELSE EXISTS { UNWIND [null] AS x RETURN count(x) AS c } END AS v } AS v", GqlStatus::SUCCESS),
        ("RETURN EXISTS { RETURN EXISTS { UNWIND [null] AS x RETURN count(x) AS c } AS v } AS v", GqlStatus::NULL_VALUE_ELIMINATED_IN_SET_FUNCTION),
    ] {
        let result = outcome(query).await;
        assert_eq!(result.status.status, expected, "{query}");
        assert!(result.additional.is_empty(), "{query}");
    }
}

#[tokio::test]
async fn scalar_subquery_conditions_follow_value_reads() {
    // §20.7 GR1c/GR2 and §20.9 GR5b: unselected results raise no conditions.
    for (query, expected) in [
        (
            "RETURN CASE WHEN true THEN 1 ELSE VALUE { UNWIND [null] AS x RETURN count(x) AS c } END AS v",
            GqlStatus::SUCCESS,
        ),
        (
            "RETURN CASE WHEN false THEN 1 ELSE VALUE { UNWIND [null] AS x RETURN count(x) AS c } END AS v",
            GqlStatus::NULL_VALUE_ELIMINATED_IN_SET_FUNCTION,
        ),
        (
            "RETURN coalesce(1, VALUE { UNWIND [null] AS x RETURN count(x) AS c }) AS v",
            GqlStatus::SUCCESS,
        ),
        (
            "RETURN coalesce(null, VALUE { UNWIND [null] AS x RETURN count(x) AS c }) AS v",
            GqlStatus::NULL_VALUE_ELIMINATED_IN_SET_FUNCTION,
        ),
    ] {
        let result = outcome(query).await;
        assert_eq!(result.status.status, expected, "{query}");
        assert!(result.additional.is_empty(), "{query}");
    }
}

#[tokio::test]
async fn procedure_conditions_stay_with_the_enclosing_mark() {
    // §20.7 GR2: CALL does not make an unselected CASE result observable.
    for (query, expected) in [
        (
            "RETURN CASE WHEN true THEN 1 ELSE EXISTS { CALL () { UNWIND [null] AS x RETURN count(x) AS c } RETURN c } END AS v",
            GqlStatus::SUCCESS,
        ),
        (
            "RETURN EXISTS { CALL () { UNWIND [null] AS x RETURN count(x) AS c } RETURN c } AS v",
            GqlStatus::NULL_VALUE_ELIMINATED_IN_SET_FUNCTION,
        ),
        (
            "RETURN CASE WHEN true THEN 1 ELSE VALUE { CALL () { UNWIND [null] AS x RETURN count(x) AS c } RETURN c LIMIT 1 } END AS v",
            GqlStatus::SUCCESS,
        ),
        (
            "RETURN VALUE { CALL () { UNWIND [null] AS x RETURN count(x) AS c } RETURN c LIMIT 1 } AS v",
            GqlStatus::NULL_VALUE_ELIMINATED_IN_SET_FUNCTION,
        ),
    ] {
        let result = outcome(query).await;
        assert_eq!(result.status.status, expected, "{query}");
        assert!(result.additional.is_empty(), "{query}");
    }
}

/// No data takes precedence over a warning, which is kept as an additional
/// status object (§4.9.3).
#[tokio::test]
async fn no_data_outranks_a_warning() {
    let outcome = outcome("MATCH (i:Item) WITH i.kind AS kind, sum(i.score) AS total WHERE total > 100 RETURN kind").await;
    assert_eq!(outcome.status.status, GqlStatus::NO_DATA);
    assert_eq!(
        outcome.additional.iter().map(|object| object.status).collect::<Vec<_>>(),
        [GqlStatus::NULL_VALUE_ELIMINATED_IN_SET_FUNCTION]
    );
}

#[tokio::test]
async fn a_failure_reports_its_condition_with_a_diagnostic_record() {
    let engine = CypherEngine::new(graph());
    let error = engine.prepare("MATCH (n) RETURN m", &ParameterMap::new()).unwrap_err();
    let object = error.status_object();
    assert_eq!(object.status, GqlStatus::INVALID_REFERENCE);
    assert!(object
        .description
        .starts_with("error: syntax error or access rule violation - invalid reference"));
    let diagnostic = object.diagnostic.expect("an exception carries a diagnostic record");
    assert_eq!(diagnostic.invalid_reference.as_deref(), Some("m"));

    let error = engine.prepare("MATCH (n RETURN n", &ParameterMap::new()).unwrap_err();
    assert_eq!(error.gql_status(), GqlStatus::INVALID_SYNTAX);

    let error = engine.prepare("MATCH (n) RETURN n LIMIT -1", &ParameterMap::new()).unwrap_err();
    assert_eq!(error.gql_status(), GqlStatus::NEGATIVE_LIMIT_VALUE);

    let error = engine.prepare("CREATE (n:Item)", &ParameterMap::new()).unwrap_err();
    assert_eq!(
        error.gql_status(),
        GqlStatus::FEATURE_NOT_SUPPORTED,
        "valid syntax this engine does not execute"
    );
}

// ---- §20.5 LET value expressions (GE03) ----

#[tokio::test]
async fn let_binds_values_for_its_body() {
    assert_eq!(value("RETURN LET x = 1 IN x + 1 END AS v").await, "2");
    assert_eq!(value("RETURN LET x = 1, y = 2 IN x + y END AS v").await, "3");
    assert_eq!(
        value("MATCH (a:Node {name: 'a'}) RETURN LET x = a.name IN x || '!' END AS v").await,
        "\"a!\""
    );
    // A LET name holds a value, so a record's field has fields.
    assert_eq!(value("RETURN LET x = {a: {b: 1}} IN x.a.b END AS v").await, "1");
}

#[tokio::test]
async fn let_obeys_its_syntax_rules() {
    // Syntax Rule 3 b): a LET name is new to the incoming record.
    assert_eq!(
        status("MATCH (n:Node {name: 'a'}) RETURN LET n = 1 IN n END AS v").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    assert_eq!(
        status("MATCH (n:Node) WHERE LET n = 1 IN n = 1 END RETURN count(*) AS v").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    // Syntax Rule 4: the names are distinct.
    assert_eq!(
        status("RETURN LET x = 1, x = 2 IN x END AS v").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    // Syntax Rule 7: each value reads the incoming record, not its siblings.
    assert_eq!(status("RETURN LET x = 1, y = x IN y END AS v").await, GqlStatus::INVALID_REFERENCE);
    assert_eq!(
        status("MATCH (i:Item) RETURN LET x = count(*) IN x END AS v").await,
        GqlStatus::FEATURE_NOT_SUPPORTED
    );
}

// ---- §20.13, §20.14 path values (GE06) and §19.3 path comparison (GA09) ----

#[tokio::test]
async fn path_constructor_enumerates_a_path() {
    let edge = "MATCH (a:Node {name: 'a'})-[e:R]->(b:Node {name: 'b'}) RETURN";
    assert_eq!(value(&format!("{edge} PATH[a, e, b] AS v")).await, "<#1020, rel#4, #1021>");
    // An edge joins its neighbours in either direction.
    assert_eq!(value(&format!("{edge} PATH[b, e, a] AS v")).await, "<#1021, rel#4, #1020>");
    assert_eq!(value("MATCH (a:Node {name: 'a'}) RETURN PATH[a] AS v").await, "<#1020>");
    // General Rule 1 b): null, or a list that is not a path, is malformed.
    for elements in ["a, e, a", "a, e", "a, b, e", "a, e, null"] {
        assert_eq!(
            status(&format!("{edge} PATH[{elements}] AS v")).await,
            GqlStatus::MALFORMED_PATH,
            "PATH[{elements}]"
        );
    }
}

#[tokio::test]
async fn paths_concatenate_at_a_shared_node() {
    let paths = "MATCH p = (a:Node {name: 'a'})-[:R]->(b:Node {name: 'b'}), q = (b)-[:R]->(d) RETURN";
    assert_eq!(value(&format!("{paths} p || q AS v")).await, "<#1020, rel#4, #1021, rel#6, #1023>");
    assert_eq!(value(&format!("{paths} p || null AS v")).await, "null");
    // §20.13 General Rule 1 b) ii).
    assert_eq!(status(&format!("{paths} p || p AS v")).await, GqlStatus::MALFORMED_PATH);
}

#[tokio::test]
async fn paths_compare_element_by_element() {
    assert_eq!(
        value("MATCH REPEATABLE ELEMENTS p = (a:Node {name: 'b'})-[:R]->(b), q = (a)-[:R]->(b) RETURN p = q AS v").await,
        "true"
    );
    assert_eq!(
        value("MATCH p = (a:Node {name: 'a'})-[:R]->(b:Node {name: 'b'}), q = (b)-[:R]->(d) RETURN p = q AS v").await,
        "false"
    );
    assert_eq!(
        value("MATCH (a:Node {name: 'a'})-[e:R]->(b:Node {name: 'b'}) RETURN PATH[a, e, b] = PATH[a, e, b] AS v").await,
        "true"
    );
}

// ---- §16.3 graph pattern YIELD (GQ19) ----

#[tokio::test]
async fn yield_keeps_the_names_it_lists() {
    assert_eq!(value("MATCH (a)-[:R]->(b) YIELD a RETURN count(*) AS c").await, "6");
    assert_eq!(
        table(&run("MATCH (a:Node {name: 'a'})-[:R]->(b) WHERE b.name = 'b' YIELD b RETURN b.name AS n").await),
        [["\"b\""]]
    );
    assert_eq!(
        value("MATCH p = (a:Node {name: 'a'})-[:R]->(b) YIELD p RETURN count(p) AS c").await,
        "2"
    );
    // Syntax Rule 12 a) iii): what the clause read from before stays.
    assert_eq!(
        table(&run("MATCH (a:Node {name: 'a'}) MATCH (a)-[:R]->(b) YIELD b RETURN a.name AS x, b.name AS n ORDER BY n").await),
        [["\"a\"", "\"b\""], ["\"a\"", "\"c\""]]
    );
    // A name YIELD drops is free for a later pattern to declare again.
    assert_eq!(
        value("MATCH (a:Node {name: 'a'})-[:R]->(b) YIELD a MATCH (b:Hub) RETURN count(*) AS c").await,
        "2"
    );
    assert_eq!(status("MATCH (a)-[:R]->(b) YIELD a RETURN b").await, GqlStatus::INVALID_REFERENCE);
}

#[tokio::test]
async fn yield_obeys_its_syntax_rules() {
    // Syntax Rule 12 a) i) and ii).
    for query in [
        "MATCH (a)-[:R]->(b) YIELD a, a RETURN count(*) AS c",
        "MATCH (a)-[:R]->(b) YIELD z RETURN count(*) AS c",
    ] {
        assert_eq!(status(query).await, GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION, "{query}");
    }
}

// ---- §16.7 questioned paths (G037) ----

#[tokio::test]
async fn a_questioned_path_matches_once_or_not_at_all() {
    assert_eq!(value("MATCH (h:Hub)-[e:HAS]->?(i) RETURN count(*) AS c").await, "4");
    // §16.7 Syntax Rule 22 f) ii): the edge is a singleton, null where the
    // path matched nothing, and the nodes either side are then one node.
    assert_eq!(
        table(&run("MATCH (h:Hub)-[e:HAS]->?(i) RETURN e.weight AS w, i.name AS n ORDER BY w").await),
        [["1", "\"banana\""], ["2", "\"apple\""], ["3", "\"carrot\""], ["null", "\"hub\""]]
    );
    // A parenthesized path with its WHERE.
    assert_eq!(
        table(
            &run("MATCH (h:Hub) ((x)-[e:HAS]->(y) WHERE e.weight > 1)? (i) RETURN e.weight AS w, x = h AS s, i.name AS n ORDER BY w").await
        ),
        [["2", "true", "\"apple\""], ["3", "true", "\"carrot\""], ["null", "null", "\"hub\""]]
    );
    assert_eq!(value("MATCH (a:Node {name: 'loop'})-[:R]->?(b) RETURN count(*) AS c").await, "2");
    assert_eq!(
        value("MATCH (a:Node {name: 'a'})-[:R]->?(b)-[:R]->?(c) RETURN count(*) AS c").await,
        "7"
    );
    // Syntax Rule 16: a path pattern matches at least one node.
    assert_eq!(
        status("MATCH ((x)-[e:HAS]->(y))? RETURN count(*) AS c").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
}

// ---- §20.1 Syntax Rule 7, §22.7 horizontal aggregation (GE09, GQ17) ----

#[tokio::test]
async fn a_where_aggregates_a_group_variable_within_the_record() {
    assert_eq!(
        value("MATCH (a:Node {name: 'a'})-[e:R]->{2}(b) WHERE count(e) = 2 RETURN count(*) AS c").await,
        "2"
    );
    assert_eq!(
        value("MATCH (a:Node {name: 'a'})-[e:R]->{1,3}(b) WHERE count(e) > 2 RETURN count(*) AS c").await,
        "2"
    );
    // GQ17: the argument may read the element through any expression.
    assert_eq!(
        value("MATCH (h:Hub)-[e:HAS]->{1}(i) WHERE sum(e.weight) > 1 RETURN count(*) AS c").await,
        "2"
    );
    assert_eq!(
        value("MATCH (h:Hub)-[e:HAS]->{1}(i) WHERE avg(e.weight) >= 2 AND min(e.weight) > 0 RETURN count(*) AS c").await,
        "2"
    );
    // A quantified path's node group variables, in a WHERE and a FILTER.
    assert_eq!(
        value("MATCH (a:Node {name: 'a'}) ((x)-[e:R]->(y)){2} (b) WHERE count(DISTINCT x) = 2 RETURN count(*) AS c").await,
        "2"
    );
    assert_eq!(
        value("MATCH (a:Node {name: 'a'}) ((x)-[e:R]->(y)){2} (b) FILTER max(y.name) = 'd' RETURN count(*) AS c").await,
        "2"
    );
}

#[tokio::test]
async fn a_horizontal_aggregate_reads_exactly_one_group_variable() {
    // §20.1 Syntax Rule 7 a) and b), §20.9 Syntax Rule 4.
    for query in [
        "MATCH (a:Node)-[e:R]->(b) WHERE count(e) = 1 RETURN count(*) AS c",
        "MATCH (a:Node) WHERE count(*) = 1 RETURN count(*) AS c",
        "MATCH (a:Node {name: 'a'}) ((x)-[e:R]->(y)){2} (b) WHERE count(x.name || y.name) = 2 RETURN count(*) AS c",
        "MATCH (a:Node {name: 'a'})-[e:R]->{2}(b) WHERE count(sum(e.weight)) = 2 RETURN count(*) AS c",
    ] {
        assert_eq!(status(query).await, GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION, "{query}");
    }
}

// ---- §21.2 SQL datetime and interval literals (GL12) ----

#[tokio::test]
async fn sql_datetime_literals_take_sql_forms() {
    // Syntax Rule 69: ISO/IEC 9075-2's forms, read as the values they name.
    assert_eq!(
        value("RETURN TIMESTAMP '2026-01-01 12:00:00' AS v").await,
        "DATETIME '2026-01-01T12:00:00.000000000'"
    );
    assert_eq!(
        value("RETURN TIMESTAMP '2026-1-5 9:05:03.25+05:30' AS v").await,
        "DATETIME '2026-01-05T09:05:03.250000000+05:30'"
    );
    assert_eq!(value("RETURN DATE '2026-1-5' AS v").await, "DATE '2026-01-05'");
    assert_eq!(value("RETURN TIME '9:05:03' AS v").await, "TIME '09:05:03.000000000'");
    // DATETIME is GQL's keyword, not SQL's, so it keeps the ISO form.
    assert_eq!(
        status("RETURN DATETIME '2026-01-01 12:00:00' AS v").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
}

#[tokio::test]
async fn sql_interval_literals_are_durations() {
    // Syntax Rule 72: the duration literal of the same fields and sign.
    for (literal, duration) in [
        ("INTERVAL '1-2' YEAR TO MONTH", "DURATION 'P1Y2M'"),
        ("INTERVAL '3' DAY", "DURATION 'P3D'"),
        ("INTERVAL -'1 02:03:04.5' DAY TO SECOND", "DURATION 'P-1DT-2H-3M-4.500000000S'"),
        ("INTERVAL '-10:30' HOUR TO MINUTE", "DURATION 'PT-10H-30M'"),
        ("INTERVAL -'-5' MINUTE", "DURATION 'PT5M'"),
        ("INTERVAL '100' DAY(3)", "DURATION 'P100D'"),
        ("INTERVAL '12.345' SECOND(2, 3)", "DURATION 'PT12.345000000S'"),
    ] {
        assert_eq!(value(&format!("RETURN {literal} AS v")).await, duration, "{literal}");
    }
    assert_eq!(
        value("RETURN DATE '2026-01-01' + INTERVAL '1' MONTH AS v").await,
        "DATE '2026-02-01'"
    );
    // Syntax Rule 70: the string has its qualifier's fields, within their
    // precisions and ranges, and the qualifier runs from a more significant
    // field to a less significant one of the same kind.
    for literal in [
        "INTERVAL '100' DAY",
        "INTERVAL '1-12' YEAR TO MONTH",
        "INTERVAL '1 24' DAY TO HOUR",
        "INTERVAL '1.1234567' SECOND",
        "INTERVAL '1' DAY TO HOUR",
        "INTERVAL '1' YEAR TO DAY",
        "INTERVAL '1' MONTH TO YEAR",
    ] {
        assert_eq!(
            status(&format!("RETURN {literal} AS v")).await,
            GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "{literal}"
        );
    }
}

// ---- §15.1, §15.2 inline procedure calls (GP01 to GP03) ----

#[tokio::test]
async fn an_inline_procedure_runs_for_each_record() {
    assert_eq!(
        value("MATCH (n:Hub) CALL { MATCH (m:Node) RETURN count(*) AS c } RETURN c").await,
        "5"
    );
    // General Rule 3 d): each record beside every record the call returns.
    assert_eq!(
        table(&run("MATCH (a:Node) CALL (a) { MATCH (a)-[:R]->(b) RETURN b } RETURN a.name AS a, b.name AS b ORDER BY a, b").await),
        [
            ["\"a\"", "\"b\""],
            ["\"a\"", "\"c\""],
            ["\"b\"", "\"d\""],
            ["\"c\"", "\"d\""],
            ["\"d\"", "\"a\""],
            ["\"loop\"", "\"loop\""]
        ]
    );
    assert_eq!(
        value("UNWIND [1, 2] AS x CALL (x) { UNWIND [10, 20] AS y RETURN x * y AS z } RETURN collect(z) AS zs").await,
        "[10, 20, 20, 40]"
    );
    assert_eq!(
        table(&run("CALL { RETURN 1 AS a UNION RETURN 2 AS a } RETURN a ORDER BY a").await),
        [["1"], ["2"]]
    );
    // A node the call returns is one a later pattern anchors on.
    assert_eq!(
        value("MATCH (a:Node {name: 'a'}) CALL (a) { MATCH (a)-[:R]->(b) RETURN b } MATCH (b)-[:R]->(c) RETURN count(*) AS c").await,
        "2"
    );
    // General Rule 3 c): an OPTIONAL call keeps a record it returns nothing
    // for, its columns null; a required one drops it.
    let to_d = "CALL (a) { MATCH (a)-[:R]->(b:Node {name: 'd'}) RETURN b } RETURN a.name AS a, b.name AS b ORDER BY a";
    assert_eq!(
        table(&run(&format!("MATCH (a:Node) OPTIONAL {to_d}")).await),
        [
            ["\"a\"", "null"],
            ["\"b\"", "\"d\""],
            ["\"c\"", "\"d\""],
            ["\"d\"", "null"],
            ["\"loop\"", "null"]
        ]
    );
    assert_eq!(
        table(&run(&format!("MATCH (a:Node) {to_d}")).await),
        [["\"b\"", "\"d\""], ["\"c\"", "\"d\""]]
    );
}

#[tokio::test]
async fn an_inline_procedure_reads_the_names_its_scope_imports() {
    // Syntax Rule 3, GP02: with no scope clause, every incoming name.
    assert_eq!(value("MATCH (n:Hub) CALL { RETURN n.name AS x } RETURN x").await, "\"hub\"");
    // GP03: the names listed, or none; a name not imported is new inside.
    assert_eq!(value("MATCH (n:Hub) CALL (n) { RETURN n.name AS x } RETURN x").await, "\"hub\"");
    assert_eq!(
        table(&run("MATCH (a:Node {name: 'a'}) CALL () { MATCH (a:Hub) RETURN a.name AS h } RETURN a.name AS a, h").await),
        [["\"a\"", "\"hub\""]]
    );
    assert_eq!(
        status("MATCH (a:Node) CALL () { RETURN a.name AS x } RETURN x").await,
        GqlStatus::INVALID_REFERENCE
    );
    // Syntax Rule 6: the scope lists names in scope, each once.
    assert_eq!(
        status("MATCH (a:Node) CALL (z) { RETURN 1 AS x } RETURN x").await,
        GqlStatus::INVALID_REFERENCE
    );
    assert_eq!(
        status("MATCH (a:Node) CALL (a, a) { RETURN 1 AS x } RETURN x").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    // §15.1 Syntax Rule 9: what the call returns is new to the table.
    assert_eq!(
        status("MATCH (a:Node) CALL (a) { RETURN a } RETURN a").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
}

// ---- §9.2, §10.3 binding variable definitions (GP05 to GP07, GP17) ----

#[tokio::test]
async fn value_variable_definitions_amend_the_working_record() {
    assert_eq!(value("VALUE x = 1 RETURN x").await, "1");
    // §9.2 Syntax Rule 2 f): each definition sees the ones before it (GP06).
    assert_eq!(table(&run("VALUE x = 2 VALUE y = x + 1 RETURN x, y").await), [["2", "3"]]);
    assert_eq!(
        value("VALUE k = 'fruit' MATCH (i:Item) WHERE i.kind = k RETURN count(*) AS c").await,
        "4"
    );
    // GP07: a value based on a subquery.
    assert_eq!(value("VALUE n = VALUE { MATCH (m:Node) RETURN count(*) AS c } RETURN n").await, "5");
    // §9.2 Syntax Rules 10 and 12: every statement and arm of the body starts
    // from the record the block amended.
    assert_eq!(
        table(&run("VALUE x = 1 RETURN x AS v UNION RETURN x + 1 AS v").await),
        [["1"], ["2"]]
    );
    assert_eq!(value("VALUE x = 1 RETURN 5 AS a NEXT RETURN a + x AS v").await, "6");
    // A nested procedure has a block of its own.
    assert_eq!(
        table(&run("MATCH (n:Hub) CALL (n) { VALUE w = 10 RETURN n.name || '!' AS t, w } RETURN t, w").await),
        [["\"hub!\"", "10"]]
    );
}

#[tokio::test]
async fn a_value_variable_is_new_to_the_record() {
    // §10.3 Syntax Rule 7.
    assert_eq!(
        status("VALUE x = 1 VALUE x = 2 RETURN x").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    // §9.2 Syntax Rule 12 c): a statement after NEXT gets a table whose
    // fields are not the record's.
    assert_eq!(
        status("VALUE x = 1 RETURN x NEXT RETURN x").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
}

#[tokio::test]
async fn let_value_items_create_table_columns_with_parallel_scope() {
    // §14.7 SR3 and Cor1 SR11–12/CR2: explicit VALUE has the same
    // per-item imports as shorthand LET; siblings do not see each other.
    for query in [
        "LET VALUE x = 1 RETURN x AS v",
        "LET x = 1 RETURN x AS v",
        "LET VALUE x = 1 RETURN *",
        "VALUE fixed = 1 LET VALUE x = fixed RETURN *",
        "CALL () { LET VALUE x = 1 RETURN x } RETURN *",
    ] {
        assert_eq!(table(&run(query).await), [["1"]], "{query}");
    }
    assert_eq!(table(&run("LET VALUE x = 1, y = 2 RETURN x, y").await), [["1", "2"]]);
    assert_eq!(table(&run("LET x = 1, VALUE y = 2 RETURN x, y").await), [["1", "2"]]);
    for query in ["LET VALUE x = 1, y = x RETURN y", "LET x = 1, VALUE y = x RETURN y"] {
        assert_eq!(status(query).await, GqlStatus::INVALID_REFERENCE, "{query}");
    }
    assert_eq!(
        status("LET VALUE x = 1, x = 2 RETURN x").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    assert_eq!(status("LET VALUE x :: INT = 1 RETURN x").await, GqlStatus::FEATURE_NOT_SUPPORTED);
}

#[tokio::test]
async fn with_preserves_fixed_working_record_values() {
    // §4.8/§9.2/§15.1: a table projection does not discard the fixed record.
    for query in [
        "VALUE fixed = 7 WITH 1 AS t RETURN fixed + t AS v",
        "VALUE fixed = 7 CALL (fixed) { WITH 1 AS t RETURN fixed + t AS v } RETURN v",
        "VALUE fixed = 7 CALL { WITH 1 AS t RETURN fixed + t AS v } RETURN v",
        "VALUE fixed = 7 WITH DISTINCT 1 AS t WITH t AS a RETURN fixed + a AS v",
    ] {
        assert_eq!(value(query).await, "8", "{query}");
    }
}

#[tokio::test]
async fn with_keeps_fixed_records_out_of_empty_aggregate_grouping() {
    // The fixed record is not an implicit grouping key (§4.8/§14.11).
    for query in [
        "VALUE fixed = 7 MATCH (n:Missing) WITH count(*) AS c RETURN fixed + c AS v",
        "VALUE fixed = 7 MATCH (n:Missing) WITH fixed + count(*) AS c RETURN c AS v",
        "VALUE fixed = 7 MATCH (n:Missing) WITH count(*) AS c WHERE fixed = 7 RETURN fixed + c AS v",
    ] {
        assert_eq!(value(query).await, "7", "{query}");
    }
}

#[tokio::test]
async fn correlated_scope_value_copies_caller_scalars_into_the_working_record() {
    // §20.6 SR11–12 and §19.4: correlation starts from a record and unit table.
    for (input, expected) in [("[]", "7"), ("[1, 2]", "9")] {
        let query = format!("FOR parent_value IN [7] RETURN VALUE {{ FOR x IN {input} RETURN parent_value + count(*) AS v }} AS result");
        assert_eq!(value(&query).await, expected, "{query}");
    }
    assert_eq!(
        table(
            &run("FOR parent_value IN [7, 9] RETURN VALUE { WITH 1 AS t FOR x IN [] RETURN parent_value + count(*) AS v } AS result").await
        ),
        [["7"], ["9"]]
    );
    assert_eq!(
        status("FOR parent_value IN [7] RETURN VALUE { FOR x IN [1, 2] RETURN parent_value + x + count(*) AS v } AS result").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    assert_eq!(
        value("FOR parent_value IN [7] RETURN VALUE { FOR x IN [1, 2] RETURN parent_value + x + count(*) AS v LIMIT 1 } AS result").await,
        "9"
    );
    assert_eq!(
        status("FOR parent_value IN [7] RETURN VALUE { FOR x IN [] RETURN count(*) AS v GROUP BY parent_value } AS result").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    assert_eq!(
        status("FOR parent_value IN [7] RETURN VALUE { FOR x IN [1, 2] RETURN count(*) AS v ORDER BY parent_value } AS result").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
}

#[tokio::test]
async fn correlated_scope_value_keeps_caller_graph_records_in_empty_groups() {
    assert_eq!(
        value("MATCH (n:Node {name: 'a'}) RETURN VALUE { FOR x IN [] RETURN n.name || CAST(count(*) AS STRING) AS v } AS result").await,
        "\"a0\""
    );
    assert_eq!(
        value("MATCH (n:Node {name: 'a'}) WITH {node: n} AS rec RETURN VALUE { FOR x IN [] RETURN rec.node.name || CAST(count(*) AS STRING) AS v } AS result").await,
        "\"a0\""
    );
}

#[tokio::test]
async fn correlated_scope_exists_preserves_caller_scalars_after_projection() {
    assert_eq!(
        value("FOR parent_value IN [7] RETURN EXISTS { WITH 1 AS t WHERE parent_value = 7 RETURN t } AS result").await,
        "true"
    );
    assert_eq!(
        status("FOR parent_value IN [7] RETURN EXISTS { CALL () { WITH 1 AS t RETURN parent_value + t AS v } RETURN v } AS result").await,
        GqlStatus::INVALID_REFERENCE
    );
}

#[tokio::test]
async fn correlated_scope_exists_keeps_caller_graph_bindings_anchored() {
    assert_eq!(
        value("MATCH (n:Node {name: 'a'}) RETURN EXISTS { WITH 1 AS t MATCH (n)-[:R]->(m) WHERE m.name = 'd' RETURN m } AS result").await,
        "false"
    );
    assert_eq!(
        table(&run("MATCH (n:Node) WHERE n.name IN ['a', 'b'] RETURN n.name AS name, EXISTS { WITH 1 AS t MATCH (n)-[:R]->(m) WHERE m.name = 'd' RETURN m } AS result ORDER BY name").await),
        [["\"a\"", "false"], ["\"b\"", "true"]]
    );
}

#[tokio::test]
async fn explicit_fixed_record_grouping_is_not_global_on_empty_input() {
    // §14.11 SR10 a), GR3 a): explicit keys form groups from input rows.
    assert_eq!(
        table(&run("VALUE fixed = 7 FOR x IN [] RETURN fixed AS k, count(*) AS c GROUP BY k").await),
        Vec::<Vec<String>>::new()
    );
    assert_eq!(
        table(&run("VALUE fixed = 7 FOR x IN [1, 2] RETURN fixed AS k, count(*) AS c GROUP BY k").await),
        [["7", "2"]]
    );
    for query in [
        "VALUE fixed = 7 FOR x IN [] WITH fixed AS k, count(*) AS c GROUP BY k RETURN k, c",
        "VALUE fixed = 7 FOR x IN [] RETURN fixed AS k, count(*) AS c GROUP BY k NEXT RETURN k, c",
        "VALUE fixed = 7 FOR x IN [] RETURN count(*) AS c GROUP BY fixed",
        "VALUE fixed = 7 FOR x IN [] RETURN fixed + 1 AS k, count(*) AS c GROUP BY k",
        "VALUE fixed = 7 CALL (fixed) { FOR x IN [] RETURN fixed AS k, count(*) AS c GROUP BY k } RETURN k, c",
        "MATCH (n:Node {name: 'a'}) WITH {inner: n} AS rec CALL (rec) { FOR x IN [] RETURN rec.inner.name AS k, count(*) AS c GROUP BY k } RETURN k, c",
    ] {
        assert_eq!(table(&run(query).await), Vec::<Vec<String>>::new(), "{query}");
    }
    for query in [
        "VALUE fixed = 7 FOR x IN [1, 2] WITH fixed AS k, count(*) AS c GROUP BY k RETURN k, c",
        "VALUE fixed = 7 FOR x IN [1, 2] RETURN fixed AS k, count(*) AS c GROUP BY k NEXT RETURN k, c",
        "VALUE fixed = 7 CALL (fixed) { FOR x IN [1, 2] RETURN fixed AS k, count(*) AS c GROUP BY k } RETURN k, c",
    ] {
        assert_eq!(table(&run(query).await), [["7", "2"]], "{query}");
    }
    assert_eq!(
        status("VALUE fixed = 7 RETURN VALUE { FOR x IN [] RETURN count(*) AS c GROUP BY fixed } AS result").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    assert_eq!(
        value("VALUE fixed = 7 RETURN VALUE { FOR x IN [] RETURN count(*) AS c GROUP BY fixed LIMIT 1 } AS result").await,
        "null"
    );
}

#[tokio::test]
async fn fixed_record_compositions_keep_graph_records_in_empty_groups() {
    // §4.8/§20.11: a record's graph fields keep their element records;
    // README §14.11 and §20.9 COUNT give a global row on empty input.
    for (input, expected) in [("[]", "\"a0\""), ("[1]", "\"a1\"")] {
        let query = format!("MATCH (n:Node {{name: 'a'}}) WITH {{inner: n}} AS rec CALL (rec) {{ FOR x IN {input} RETURN rec.inner.name || CAST(count(*) AS STRING) AS v }} RETURN v");
        assert_eq!(value(&query).await, expected, "{query}");
    }
    for (query, expected) in [
        (
            "MATCH (n:Node {name: 'a'}) WITH {node: n} AS rec CALL (rec) { FOR x IN [] RETURN rec.node.name AS v, count(*) AS c } RETURN v",
            "\"a\"",
        ),
        (
            "MATCH (n:Node {name: 'a'}) WITH {inner: n} AS rec CALL (rec) { RETURN VALUE { FOR x IN [] RETURN rec.inner.name || CAST(count(*) AS STRING) AS v } AS result } RETURN result",
            "\"a0\"",
        ),
        (
            "MATCH (n:Node {name: 'a'}) WITH {outer: {inner: n}} AS rec CALL (rec) { FOR x IN [] RETURN rec.outer.inner.name || CAST(count(*) AS STRING) AS v } RETURN v",
            "\"a0\"",
        ),
        (
            "MATCH (:Hub)-[e:HAS]->(:Item {name: 'apple', score: 3}) WITH {inner: e} AS rec CALL (rec) { FOR x IN [] RETURN rec.inner.weight + count(*) AS v } RETURN v",
            "2",
        ),
        (
            "MATCH (a:Node {name: 'a'})-[e:R]->(b:Node {name: 'b'}) WITH {left: a, edge: e, right: b} AS rec CALL (rec) { FOR x IN [] RETURN path_length(PATH[coalesce(min(rec.left), rec.left), coalesce(min(rec.edge), rec.edge), coalesce(min(rec.right), rec.right)]) AS v } RETURN v",
            "1",
        ),
        (
            "MATCH (n:Node {name: 'a'}) WITH {inner: n} AS rec CALL (rec) { FOR x IN [] WITH coalesce(min(rec.inner), rec.inner) AS kept, count(*) AS c ORDER BY kept.name RETURN kept.name || CAST(c AS STRING) AS v } RETURN v",
            "\"a0\"",
        ),
        (
            "MATCH (:Hub)-[e:HAS]->(:Item {name: 'apple', score: 3}) WITH {inner: e} AS rec CALL (rec) { FOR x IN [] WITH coalesce(min(rec.inner), rec.inner) AS kept, count(*) AS c RETURN kept.weight + c AS v } RETURN v",
            "2",
        ),
    ] {
        assert_eq!(value(query).await, expected, "{query}");
    }
}

#[tokio::test]
async fn fixed_record_compositions_infer_value_cardinality_from_provenance() {
    // §20.6 SR10 b), SR11 and §20.9: fixed fields are not table group keys.
    assert_eq!(
        value("VALUE fixed = 7 RETURN VALUE { FOR x IN [] RETURN fixed + count(*) AS v } AS result").await,
        "7"
    );
    for (query, expected) in [
        (
            "VALUE fixed = 7 RETURN VALUE { WITH 1 AS t FOR x IN [] RETURN fixed + count(*) AS v } AS result",
            "7",
        ),
        (
            "RETURN VALUE { VALUE fixed = 7 FOR x IN [] RETURN fixed + count(*) AS v } AS result",
            "7",
        ),
        (
            "VALUE fixed = 7 RETURN VALUE { FOR x IN [1, 2] RETURN fixed + count(*) AS v } AS result",
            "9",
        ),
    ] {
        assert_eq!(value(query).await, expected, "{query}");
    }
    assert_eq!(
        status("RETURN VALUE { FOR x IN [1, 2] RETURN x + count(*) AS v } AS result").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    assert_eq!(
        status("VALUE fixed = 7 RETURN VALUE { WITH fixed AS projected FOR x IN [1, 2] RETURN projected + count(*) AS v } AS result").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    assert_eq!(
        value("RETURN VALUE { FOR x IN [1, 2] RETURN x + count(*) AS v LIMIT 1 } AS result").await,
        "2"
    );
}

#[tokio::test]
async fn with_fixed_frames_preserve_table_projection_and_windows() {
    // §14.10 SR4 c): readable records do not widen restricted RETURN keys.
    for query in [
        "VALUE fixed = 7 FOR x IN [1, 2] RETURN DISTINCT x AS t ORDER BY fixed",
        "VALUE fixed = 7 FOR x IN [1, 2] RETURN count(*) AS t ORDER BY fixed",
    ] {
        assert_eq!(status(query).await, GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION, "{query}");
    }
    assert_eq!(
        table(&run("VALUE fixed = 7 FOR x IN [1, 2] RETURN x AS t ORDER BY fixed - t").await),
        [["2"], ["1"]]
    );
    // WITH is E2; the fixed-frame expectations follow §4.8/§9.2/§15.2.
    for query in [
        "VALUE fixed = 7 FOR x IN [2, 1, 2] WITH DISTINCT x AS t WHERE fixed = 7 RETURN fixed + t AS v ORDER BY v",
        "VALUE fixed = 7 FOR x IN [2, 1, 2] WITH DISTINCT x AS t ORDER BY fixed + t RETURN fixed + t AS v",
    ] {
        assert_eq!(table(&run(query).await), [["8"], ["9"]], "{query}");
    }
    for query in [
        "VALUE fixed = 7 WITH fixed WITH 1 AS t RETURN fixed + t AS v",
        "VALUE fixed = 7 WITH fixed AS renamed WITH renamed AS t RETURN fixed + t AS v",
        "VALUE fixed = 7 WITH 1 AS t WITH fixed AS f RETURN fixed + f AS v",
    ] {
        assert_eq!(
            value(query).await,
            if query.contains("WITH fixed WITH") { "8" } else { "14" },
            "{query}"
        );
    }
    for query in [
        "FOR x IN [1] WITH 1 AS t RETURN x",
        "VALUE fixed = 7 FOR x IN [1] WITH 1 AS t RETURN x",
        "VALUE fixed = 7 WITH 1 AS t WITH 2 AS u RETURN t",
        "VALUE fixed = 7 CALL () { WITH 1 AS t RETURN fixed + t AS v } RETURN v",
    ] {
        assert_eq!(status(query).await, GqlStatus::INVALID_REFERENCE, "{query}");
    }
    for quantifier in ["", "ALL ", "DISTINCT "] {
        let query = format!("VALUE fixed = 7 FOR x IN [2, 1, 2] WITH DISTINCT x AS t RETURN {quantifier}*");
        let result = run(&query).await;
        assert_eq!(result.columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), ["t"]);
        assert_eq!(table(&result), [["2"], ["1"]]);
    }
}

#[tokio::test]
async fn with_fixed_frames_preserve_empty_global_aggregates() {
    // README's §14.11 reading plus §20.9 COUNT: one global row, COUNT zero.
    // This is not the N5 case of a separate table-valued grouping item.
    for query in [
        "VALUE fixed = 7 FOR x IN [] RETURN fixed + count(*) AS v",
        "VALUE fixed = 7 CALL (fixed) { FOR x IN [] RETURN fixed + count(*) AS v } RETURN v",
        "VALUE fixed = 7 FOR x IN [] WITH DISTINCT fixed + count(*) AS v RETURN v",
        "VALUE fixed = 7 FOR x IN [] WITH count(*) AS c WHERE fixed = 7 RETURN fixed + c AS v",
        "VALUE fixed = 7 FOR x IN [] WITH fixed, count(*) AS c RETURN fixed + c AS v",
        "VALUE fixed = 7 WITH 1 AS t LIMIT 0 WITH count(*) AS c RETURN fixed + c AS v",
        "VALUE fixed = 7 WITH 1 AS t WHERE false WITH count(*) AS c RETURN fixed + c AS v",
    ] {
        assert_eq!(value(query).await, "7", "{query}");
    }
    assert_eq!(
        table(&run("VALUE fixed = 7 FOR x IN [] WITH x, count(*) AS c RETURN c").await),
        Vec::<Vec<String>>::new()
    );
    assert_eq!(table(&run("FOR x IN [] RETURN x + count(*) AS v").await), Vec::<Vec<String>>::new());
    assert_eq!(table(&run("FOR x IN [1, 2] RETURN x + count(*) AS v").await), [["2"], ["3"]]);
    assert_eq!(
        table(&run("FOR x IN [1, 2, 1] WITH x, count(*) AS c RETURN x, c ORDER BY x").await),
        [["1", "2"], ["2", "1"]]
    );
    assert_eq!(
        value("VALUE fixed = 7 FOR x IN [1, 2] WITH sum(x) AS c RETURN fixed + c AS v").await,
        "10"
    );
}

#[tokio::test]
async fn with_fixed_frames_keep_imported_elements_anchored() {
    for query in [
        "MATCH (n:Node {name: 'a'}) CALL (n) { WITH 1 AS t MATCH (n)-[:R]->(m) RETURN m.name AS v } RETURN v ORDER BY v",
        "MATCH (n:Node {name: 'a'}) CALL { WITH DISTINCT 1 AS t WITH t AS u MATCH (n)-[:R]->(m) RETURN m.name AS v } RETURN v ORDER BY v",
        "MATCH (n:Node {name: 'a'}) CALL (n) { WITH 1 AS t RETURN n AS v } WITH v MATCH (v)-[:R]->(m) RETURN m.name AS v ORDER BY v",
    ] {
        assert_eq!(table(&run(query).await), [["\"b\""], ["\"c\""]], "{query}");
    }
    assert_eq!(
        value(
            "MATCH (a:Node {name: 'a'})-[r:R]->(b:Node {name: 'b'}) CALL (r) { WITH 1 AS t RETURN r AS edge } RETURN edge IS :: EDGE AS v"
        )
        .await,
        "true"
    );
    for query in [
        "MATCH p = (:Node {name: 'a'})-[:R]->(:Node {name: 'b'}) CALL (p) { WITH 1 AS t RETURN p AS q } UNWIND ELEMENTS(q) AS e RETURN count(*) AS c",
        "MATCH p = (:Node {name: 'a'})-[:R]->(:Node {name: 'b'}) WITH {p: p} AS rec CALL (rec) { WITH 1 AS t RETURN rec.p AS q } UNWIND ELEMENTS(q) AS e RETURN count(*) AS c",
    ] {
        assert_eq!(value(query).await, "3", "{query}");
    }
    assert_eq!(
        value("MATCH (n:Node {name: 'a'}) CALL (n) { FOR x IN [] WITH count(*) AS c RETURN n.name AS v } RETURN v").await,
        "\"a\""
    );
}

#[tokio::test]
async fn projected_bindings_replace_fixed_value_kinds() {
    for quantifier in ["", "DISTINCT "] {
        let query =
            format!("VALUE fixed = 7 MATCH (n:Node) WITH {quantifier}n AS fixed MATCH (fixed:Node) RETURN fixed.name AS v ORDER BY v");
        assert_eq!(
            table(&run(&query).await),
            [["\"a\""], ["\"b\""], ["\"c\""], ["\"d\""], ["\"loop\""]],
            "{query}"
        );
    }
    assert_eq!(
        value("VALUE fixed = 7 MATCH (:Node {name: 'a'})-[r:R]->(:Node {name: 'b'}) WITH r AS fixed MATCH ()-[fixed:R]->() RETURN count(*) AS v").await,
        "1"
    );
    assert_eq!(
        status("VALUE fixed = 7 MATCH (n:Node) WITH 9 AS fixed MATCH (fixed:Node) RETURN fixed").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
}

#[tokio::test]
async fn projected_nodes_replace_imported_relationship_kinds() {
    for grouping in ["", "GROUP BY r "] {
        for (direction, expected) in [("ASC", "#1020"), ("DESC", "#1024")] {
            let query = format!(
                "MATCH ()-[r:R]->() CALL (r) {{ MATCH (n:Node) WITH n AS r, count(*) AS c {grouping}ORDER BY r.id {direction} LIMIT 1 RETURN r AS chosen }} RETURN chosen"
            );
            assert_eq!(table(&run(&query).await), vec![vec![expected.to_string()]; 6], "{query}");
        }
    }
    assert_eq!(
        value("MATCH (:Node {name: 'a'})-[r:R]->(:Node {name: 'b'}) CALL (r) { WITH 1 AS t RETURN r.id AS v } RETURN v").await,
        "null"
    );
}

#[tokio::test]
async fn dropping_a_shadow_restores_the_fixed_record_kind() {
    assert_eq!(
        status("VALUE fixed = 7 MATCH (n:Node) WITH n AS fixed WITH 1 AS t MATCH (fixed:Node) RETURN fixed").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    assert_eq!(
        table(&run("MATCH (n:Node {name: 'a'}) CALL (n) { WITH 7 AS n WITH 1 AS t MATCH (n)-[:R]->(m) RETURN m.name AS v } RETURN v ORDER BY v").await),
        [["\"b\""], ["\"c\""]]
    );
}

#[tokio::test]
async fn dropping_a_relationship_shadow_restores_fixed_node_identity() {
    for projection in ["1 AS t", "DISTINCT 1 AS t", "count(*) AS c"] {
        let query = format!(
            "MATCH (n:Node {{name: 'a'}}) CALL (n) {{ \
             MATCH ()-[r:R]->() WITH r AS n WITH {projection} RETURN n.id AS v \
             }} RETURN v"
        );
        let expected_rows = if projection == "1 AS t" { 6 } else { 1 };
        assert_eq!(table(&run(&query).await), vec![vec!["#1020".to_string()]; expected_rows], "{query}");
    }
    assert_eq!(
        value("MATCH (n:Node {name: 'a'}) CALL (n) { MATCH ()-[r:R]->() WITH r AS n WITH count(*) AS c ORDER BY coalesce(n.id, 1 / 0) RETURN n.id AS v } RETURN v").await,
        "#1020"
    );
    assert_eq!(
        value("MATCH (:Node {name: 'a'})-[r:R]->(:Node {name: 'b'}) CALL (r) { MATCH (n:Node) WITH n AS r WITH count(*) AS c RETURN r.id AS v } RETURN v").await,
        "null"
    );
    assert_eq!(
        value("VALUE n = VALUE { MATCH (n:Node {name: 'a'}) RETURN n LIMIT 1 } MATCH ()-[r:R]->() WITH r AS n WITH count(*) AS c RETURN n.id AS v")
            .await,
        "#1020"
    );
    assert_eq!(
        value("VALUE r = VALUE { MATCH (:Node {name: 'a'})-[r:R]->(:Node {name: 'b'}) RETURN r LIMIT 1 } RETURN 1 AS t NEXT MATCH (n:Node) WITH n AS r WITH count(*) AS c RETURN r.id AS v").await,
        "null"
    );
}

#[tokio::test]
async fn with_fixed_frames_keep_types_nulls_keywords_and_next() {
    for query in [
        "VALUE fixed = CAST(7 AS INT8) WITH 1 AS t RETURN fixed + t AS v",
        "VALUE unknown = 7 WITH 1 AS t WITH t AS u WHERE unknown = 7 ORDER BY unknown + u RETURN unknown + u AS v",
        "VALUE fixed = 7 WITH 1 AS t RETURN fixed + t AS v NEXT WITH v RETURN fixed + v AS v NEXT WITH v RETURN v - fixed AS v",
    ] {
        assert_eq!(value(query).await, "8", "{query}");
    }
    assert_eq!(
        value("VALUE fixed = CAST(NULL AS INT8) WITH 1 AS t RETURN fixed IS NULL AS v").await,
        "true"
    );
    assert_eq!(
        value("VALUE fixed = DATE '2026-01-02' WITH 1 AS t RETURN fixed IS :: DATE AS v").await,
        "true"
    );
    assert_eq!(value("VALUE fixed = [7, 8] WITH 1 AS t RETURN size(fixed) + t AS v").await, "3");
    assert_eq!(
        table(&run("VALUE fixed = 7 RETURN 1 NEXT WITH 2 AS __hydradb_next_column_0 RETURN fixed, __hydradb_next_column_0").await),
        [["7", "2"]]
    );
}

#[tokio::test]
async fn return_star_expands_table_columns_not_the_working_record() {
    // §9.2/§10.3 put definitions in the record; §14.11 SR6 expands only
    // the table, in ascending column-name order, for every set quantifier.
    for quantifier in ["", "ALL ", "DISTINCT "] {
        for query in [
            format!("RETURN {quantifier}*"),
            format!("MATCH (n:Hub) RETURN {quantifier}* GROUP BY n"),
            format!("VALUE hidden = 7 RETURN {quantifier}*"),
        ] {
            assert_eq!(status(&query).await, GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION, "{query}");
        }
        for (clauses, names, rows) in [
            ("FOR z IN [2, 1, 2]", vec!["z"], vec![vec!["2"], vec!["1"], vec!["2"]]),
            (
                "FOR z IN [2, 1, 2] LET a = z + hidden",
                vec!["a", "z"],
                vec![vec!["9", "2"], vec!["8", "1"], vec!["9", "2"]],
            ),
        ] {
            let query = format!("VALUE hidden = 7 {clauses} RETURN {quantifier}*");
            let result = run(&query).await;
            assert_eq!(
                result.columns.iter().map(|column| column.name.as_str()).collect::<Vec<_>>(),
                names,
                "{query}"
            );
            let mut expected = rows;
            let mut actual = table(&result);
            actual.sort();
            expected.sort();
            if quantifier == "DISTINCT " {
                expected.dedup();
            }
            assert_eq!(actual, expected, "{query}");
        }
    }
    assert_eq!(
        table(&run("VALUE hidden = 7 FOR z IN [2] RETURN *, hidden AS explicit").await),
        [["2", "7"]]
    );
    // The same spelling is an ordinary column when introduced by LET.
    assert_eq!(table(&run("LET hidden = 7 RETURN *").await), [["7"]]);
    assert_eq!(table(&run("VALUE hidden = 7 LET exposed = hidden RETURN *").await), [["7"]]);
    // A real projection makes a table column, unlike a definition's
    // internal identity projection. Provenance is not a name blacklist.
    assert_eq!(table(&run("VALUE hidden = 7 WITH hidden RETURN *").await), [["7"]]);
}

#[tokio::test]
async fn working_record_provenance_survives_next_and_call_boundaries() {
    for quantifier in ["", "ALL ", "DISTINCT "] {
        let query = format!("VALUE fixed = 7 FOR z IN [2] RETURN {quantifier}* NEXT LET a = z + fixed RETURN {quantifier}*");
        let result = run(&query).await;
        assert_eq!(
            result.columns.iter().map(|column| column.name.as_str()).collect::<Vec<_>>(),
            ["a", "z"],
            "{query}"
        );
        assert_eq!(table(&result), [["9", "2"]], "{query}");
        let query = format!("VALUE fixed = 7 RETURN {quantifier}* NEXT RETURN 1 AS a");
        assert_eq!(status(&query).await, GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION, "{query}");
    }
    assert_eq!(
        table(&run("VALUE fixed = 7 CALL (fixed) { RETURN fixed + 1 AS a } RETURN *").await),
        [["8"]]
    );
    assert_eq!(
        table(&run("VALUE fixed = 7 CALL () { VALUE fixed = 9 RETURN fixed AS a } RETURN *, fixed AS original").await),
        [["9", "7"]]
    );
    assert_eq!(
        table(&run("VALUE fixed = 7 CALL () { RETURN 2 AS z } RETURN * NEXT RETURN fixed + z AS a").await),
        [["9"]]
    );
    assert_eq!(
        status("VALUE fixed = 7 RETURN 2 AS fixed NEXT RETURN fixed").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    assert_eq!(
        table(&run("VALUE fixed = 7 MATCH (n:Missing) RETURN count(*) AS c NEXT RETURN *, fixed AS a").await),
        [["0", "7"]]
    );
}

#[tokio::test]
async fn finish_next_restores_the_working_record_without_table_columns() {
    // §9.2/§14.10: an omitted result resets the table, not the body's record.
    assert_eq!(value("VALUE x = 7 FINISH NEXT RETURN x AS v").await, "7");
    assert_eq!(value("VALUE x = 7 FOR z IN [1,2] FINISH NEXT RETURN x AS v").await, "7");
    assert_eq!(value("VALUE x = 7 FOR z IN [] FINISH NEXT RETURN x AS v").await, "7");
    assert_eq!(value("VALUE x = 7 FINISH NEXT FINISH NEXT RETURN x AS v").await, "7");
    assert_eq!(
        status("VALUE x = 7 FINISH NEXT RETURN *").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    assert_eq!(
        status("VALUE x = 7 FOR z IN [1] FINISH NEXT RETURN z").await,
        GqlStatus::INVALID_REFERENCE
    );
    assert_eq!(value("VALUE x = 7 RETURN 99 AS z NEXT FINISH NEXT RETURN x AS v").await, "7");
    assert_eq!(
        status("VALUE x = 7 FILTER 1 / 0 = 0 FINISH NEXT RETURN x AS v").await,
        GqlStatus::DIVISION_BY_ZERO
    );
}

#[tokio::test]
async fn table_provenance_survives_pattern_yield_and_optional_misses() {
    // §16.3 SR12: YIELD projects the table; it does not turn the incoming
    // working record into table columns. Its values remain readable.
    let result = run("VALUE hidden = 7 MATCH (n:Hub) YIELD n LET exposed = hidden RETURN *").await;
    assert_eq!(
        result.columns.iter().map(|column| column.name.as_str()).collect::<Vec<_>>(),
        ["exposed", "n"]
    );
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        table(&run("VALUE hidden = 7 OPTIONAL MATCH (n:Missing) RETURN *").await),
        [["null"]]
    );
    assert_eq!(
        status("VALUE hidden = 7 FILTER true RETURN *").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
}

#[tokio::test]
async fn a_subquery_in_a_let_or_with_item_is_computed_per_row() {
    assert_eq!(
        table(&run("MATCH (a:Node) LET has = EXISTS { (a)-[:R]->(:Node {name: 'd'}) } RETURN a.name AS a, has ORDER BY a").await),
        [
            ["\"a\"", "false"],
            ["\"b\"", "true"],
            ["\"c\"", "true"],
            ["\"d\"", "false"],
            ["\"loop\"", "false"]
        ]
    );
    assert_eq!(
        table(
            &run("MATCH (a:Node) WITH a, VALUE { MATCH (a)-[:R]->(b) RETURN count(b) AS c } AS out RETURN a.name AS a, out ORDER BY a")
                .await
        ),
        [["\"a\"", "2"], ["\"b\"", "1"], ["\"c\"", "1"], ["\"d\"", "1"], ["\"loop\"", "1"]]
    );
}

// ---- Review fixes: scope, marks, sort keys, CASE, POWER, windows ----

#[tokio::test]
async fn a_value_keyword_is_a_variable_only_where_one_is_in_scope() {
    // Its own alias is not in scope for the item it names.
    assert_eq!(value("RETURN SESSION_USER AS session_user").await, "\"anonymous\"");
    assert_eq!(value("RETURN UNKNOWN AS unknown").await, "null");
    // Nor is a name another arm or a subquery declares.
    assert_eq!(
        table(&run("RETURN SESSION_USER AS v UNION UNWIND [1] AS session_user RETURN session_user AS v").await),
        [["\"anonymous\""], ["1"]]
    );
    assert_eq!(
        table(&run("RETURN UNKNOWN AS u, EXISTS { UNWIND [1] AS unknown RETURN unknown } AS e").await),
        [["null", "true"]]
    );
    // In scope, the word is the variable.
    assert_eq!(value("UNWIND [2] AS unknown RETURN UNKNOWN AS v").await, "2");
    assert_eq!(value("WITH 3 AS session_user RETURN session_user + 1 AS v").await, "4");
    // A delimited name is always a variable.
    assert_eq!(value("WITH 5 AS `unknown` RETURN `unknown` AS v").await, "5");
}

#[tokio::test]
async fn a_generated_mark_never_shares_a_name_with_the_query() {
    // A query naming a variable the way a RETURN's EXISTS mark is named is
    // refused, never answered from the wrong slot.
    assert_eq!(
        status("WITH false AS __hydradb_exists_0 RETURN __hydradb_exists_0 AS original, EXISTS { RETURN 1 AS x } AS found").await,
        GqlStatus::FEATURE_NOT_SUPPORTED
    );
    // Without the clash the same names are fine.
    assert_eq!(
        table(&run("WITH false AS __hydradb_exists_7 RETURN __hydradb_exists_7 AS original, EXISTS { RETURN 1 AS x } AS found").await),
        [["false", "true"]]
    );
}

#[tokio::test]
async fn a_distinct_star_may_sort_by_what_the_star_returns() {
    assert_eq!(
        table(&run("MATCH (n:Node) WHERE n.name IN ['a', 'b'] RETURN DISTINCT * ORDER BY n.name").await),
        [["#1020"], ["#1021"]]
    );
}

#[tokio::test]
async fn a_subquery_in_a_branch_not_taken_does_not_run_into_its_errors() {
    // §20.7: only the chosen result is evaluated.
    assert_eq!(
        value("RETURN CASE WHEN true THEN 1 ELSE EXISTS { RETURN 1 / 0 AS x } END AS v").await,
        "1"
    );
    assert_eq!(
        status("RETURN CASE WHEN false THEN 1 ELSE EXISTS { RETURN 1 / 0 AS x } END AS v").await,
        GqlStatus::DIVISION_BY_ZERO
    );
}

#[tokio::test]
async fn power_keeps_the_parity_of_an_integer_exponent() {
    assert_eq!(value("RETURN power(-1, 9007199254740993) AS v").await, "-1.0f");
    assert_eq!(value("RETURN power(-2, 3) AS v").await, "-8.0f");
    assert_eq!(value("RETURN power(-2, 2) AS v").await, "4.0f");
}

#[tokio::test]
async fn a_window_that_leaves_no_rows_is_no_data() {
    let windowed = outcome("MATCH (n:Node) RETURN n.name AS name").await.windowed(100, None);
    assert!(windowed.result.rows.is_empty());
    assert_eq!(windowed.status.status, GqlStatus::NO_DATA);
    let kept = outcome("MATCH (n:Node) RETURN n.name AS name ORDER BY name")
        .await
        .windowed(1, Some(2));
    assert_eq!(kept.result.rows.len(), 2);
    assert_eq!(kept.status.status, GqlStatus::SUCCESS);
}

// ---- Review fixes: XOR, filters, percentiles, deviations, literals, records ----

#[tokio::test]
async fn xor_is_one_operation_however_long_the_chain() {
    // Forty operands would be 2^40 leaves if XOR were expanded.
    let chain = std::iter::repeat_n("true", 41).collect::<Vec<_>>().join(" XOR ");
    assert_eq!(value(&format!("RETURN {chain} AS v")).await, "true");
    assert_eq!(value("RETURN true XOR null AS v").await, "null");
    assert_eq!(value("RETURN false XOR true AS v").await, "true");
}

#[tokio::test]
async fn a_filter_never_runs_a_test_its_rows_were_spared() {
    // `n IS DIRECTED` raises 22G03 on a node, but no row reaches it.
    assert_eq!(
        outcome("MATCH (n:Hub) FILTER false FILTER (n IS DIRECTED) IS NULL RETURN n")
            .await
            .status
            .status,
        GqlStatus::NO_DATA
    );
}

#[tokio::test]
async fn a_deviation_of_large_equal_values_is_zero() {
    assert_eq!(
        value("UNWIND [1.0E308, 1.0E308, 1.0E308] AS x RETURN stddev_pop(x) AS s").await,
        "0f"
    );
    assert_eq!(value("UNWIND [2, 4, 4, 4, 5, 5, 7, 9] AS x RETURN stddev_pop(x) AS s").await, "2f");
}

#[tokio::test]
async fn approximate_literals_take_any_integer_stem() {
    assert_eq!(value("RETURN 1000000000000000000000000000000000000000F AS v").await, "1e39f");
    assert_eq!(value("RETURN 0F AS v").await, "0.0f");
    assert_eq!(value("RETURN 0d AS v").await, "0.0f");
}

#[tokio::test]
async fn a_node_held_as_a_value_keeps_its_record() {
    assert_eq!(
        table(&run("MATCH (n:Hub) WITH coalesce(n, n) AS m RETURN m.name AS a, PROPERTY_EXISTS(m, name) AS b, m:Hub AS c").await),
        [["\"hub\"", "true", "true"]]
    );
    // A null value is a null element: unknown, not a type error.
    assert_eq!(value("UNWIND [null] AS n RETURN n IS DIRECTED AS v").await, "null");
}

#[tokio::test]
async fn after_distinct_an_element_test_may_read_only_what_is_returned() {
    assert_eq!(
        status("MATCH (n:Node) RETURN DISTINCT n.name AS name ORDER BY n IS LABELED Node").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
}

// ---- Review fixes: FINISH columns, VALUE rows, NEXT sorting, OTHERWISE ----

#[tokio::test]
async fn finish_has_no_columns() {
    let engine = CypherEngine::new(graph());
    let prepared = engine.prepare("MATCH (n:Hub) FINISH", &ParameterMap::new()).unwrap();
    assert!(prepared.column_names().unwrap().is_empty());
}

#[tokio::test]
async fn a_value_subquery_that_groups_is_refused() {
    // `x + count(*)` groups by `x`: two rows, not one (§20.6 Syntax Rule 10).
    assert_eq!(
        status("RETURN VALUE { FOR x IN [1, 2] RETURN x + count(*) AS v } AS result").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    assert_eq!(value("RETURN VALUE { FOR x IN [1, 2] RETURN count(*) AS v } AS result").await, "2");
    // Its data exception is raised only where it is read.
    assert_eq!(
        value("RETURN CASE WHEN true THEN 1 ELSE VALUE { RETURN 1 / 0 AS x LIMIT 1 } END AS v").await,
        "1"
    );
}

#[tokio::test]
async fn value_subqueries_resolve_their_result_names_in_scope() {
    for name in ["x", "unknown", "UNKNOWN", "session_user", "SESSION_USER"] {
        for (body, expected) in [
            (format!("WITH 1 AS {name} RETURN {name} LIMIT 1"), "1"),
            (format!("FOR {name} IN [2] RETURN {name} LIMIT 1"), "2"),
            (format!("WITH 1 AS {name} WITH 3 AS {name} RETURN {name} LIMIT 1"), "3"),
            (format!("WITH 1 AS {name} WHERE false RETURN {name} LIMIT 1"), "null"),
        ] {
            let query = format!("RETURN VALUE {{ {body} }} AS v");
            assert_eq!(value(&query).await, expected, "{query}");
        }
    }
}

#[tokio::test]
async fn value_subquery_result_names_preserve_correlation_and_shadowing() {
    for (query, expected) in [
        ("WITH 3 AS unknown RETURN VALUE { RETURN unknown LIMIT 1 } AS v", "3"),
        (
            "WITH 3 AS unknown RETURN VALUE { WITH 4 AS unknown RETURN unknown LIMIT 1 } AS v",
            "4",
        ),
        (
            "RETURN VALUE { WITH 5 AS session_user RETURN VALUE { RETURN session_user LIMIT 1 } AS nested LIMIT 1 } AS v",
            "5",
        ),
        ("RETURN VALUE { RETURN UNKNOWN AS named LIMIT 1 } AS v", "null"),
        ("RETURN VALUE { RETURN 6 AS unknown LIMIT 1 } AS v", "6"),
        ("RETURN VALUE { RETURN VALUE { RETURN 7 AS x LIMIT 1 } AS named LIMIT 1 } AS v", "7"),
    ] {
        assert_eq!(value(query).await, expected, "{query}");
    }
}

#[tokio::test]
async fn value_subqueries_still_require_a_named_result() {
    for body in [
        "RETURN UNKNOWN LIMIT 1",
        "RETURN SESSION_USER LIMIT 1",
        "RETURN 1 LIMIT 1",
        "WITH 1 AS unknown RETURN unknown + 1 LIMIT 1",
        "WITH 1 AS unknown WITH 2 AS x RETURN unknown LIMIT 1",
        "RETURN VALUE { RETURN 1 AS x LIMIT 1 } LIMIT 1",
        "RETURN EXISTS { RETURN 1 AS x } LIMIT 1",
    ] {
        let query = format!("RETURN VALUE {{ {body} }} AS v");
        assert_eq!(status(&query).await, GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION, "{query}");
    }
}

#[tokio::test]
async fn a_return_before_next_may_sort_by_what_it_does_not_return() {
    assert_eq!(
        table(&run("MATCH (n:Item) RETURN n.name AS name ORDER BY n.score LIMIT 2 NEXT RETURN name ORDER BY name").await),
        [["\"apple\""], ["\"banana\""]]
    );
}

#[tokio::test]
async fn otherwise_never_runs_a_fallback_it_does_not_need() {
    assert_eq!(table(&run("RETURN 1 AS v OTHERWISE RETURN 1 / 0 AS v").await), [["1"]]);
    assert_eq!(
        table(&run("MATCH (n:Missing) RETURN n.name AS v OTHERWISE RETURN 2 AS v").await),
        [["2"]]
    );
}

// ---- Review fixes: path WHERE scope, selective path WHERE, SIMPLE walks ----

#[tokio::test]
async fn a_selective_path_filters_its_candidates_by_its_own_where() {
    // §22.4: the WHERE around a selective path is one of its predicates, so
    // the search selects among the trails it accepts. Of the trails from a
    // longer than one edge, the shortest to each endpoint: d, a, b and c.
    assert_eq!(
        value("MATCH ANY SHORTEST ((a:Node {name: 'a'})-[e:R]->+(b) WHERE size(e) > 1) RETURN count(*) AS c").await,
        "4"
    );
    // Its subqueries filter the candidates too: only d leads back to a.
    assert_eq!(
        value(
            "MATCH ANY SHORTEST ((a:Node {name: 'a'})-[e:R]->+(b) WHERE EXISTS { (b)-[:R]->(:Node {name: 'a'}) }) \
             RETURN b.name AS n"
        )
        .await,
        "\"d\""
    );
    // KEEP makes a path selective, and its WHERE goes with it.
    assert_eq!(
        value("MATCH ((a:Node {name: 'a'})-[e:R]->+(b) WHERE size(e) > 1) KEEP ANY SHORTEST RETURN count(*) AS c").await,
        "4"
    );
}

#[tokio::test]
async fn a_path_where_stays_with_its_own_match() {
    // The WHERE of the first path holds of this MATCH, not of the MATCH
    // inside the second path's subquery.
    assert_eq!(
        value(
            "MATCH ((x:Node)-[:R]->(y) WHERE x.name = 'a'), \
             ((h:Hub)-[e:HAS]->(i) WHERE NOT EXISTS { MATCH (i) WHERE i.name = 'none' RETURN i }) \
             RETURN count(*) AS c"
        )
        .await,
        "6"
    );
    // Nested parentheses: both WHEREs hold.
    assert_eq!(
        value("MATCH (((a)-[e:HAS]->(b) WHERE e.weight > 1) WHERE e.weight < 3) RETURN count(*) AS c").await,
        "1"
    );
}

#[tokio::test]
async fn a_simple_path_may_close_over_the_edge_it_left_by() {
    // §16.6, §22.2 General Rule 16 d) ii): SIMPLE repeats no node but lets
    // the path end where it began; under REPEATABLE ELEMENTS nothing stops
    // the closing step reusing an edge. a-b-a, a-c-a and a-d-a.
    assert_eq!(
        value("MATCH REPEATABLE ELEMENTS SIMPLE (a:Node {name: 'a'})-[:R]-{2}(a) RETURN count(*) AS c").await,
        "3"
    );
    assert_eq!(
        value("MATCH SIMPLE (a:Node {name: 'a'})-[:R]-{2}(a) RETURN count(*) AS c").await,
        "0"
    );
    assert_eq!(
        value("MATCH REPEATABLE ELEMENTS TRAIL (a:Node {name: 'a'})-[:R]-{2}(a) RETURN count(*) AS c").await,
        "0"
    );
    assert_eq!(
        value("MATCH REPEATABLE ELEMENTS ACYCLIC (a:Node {name: 'a'})-[:R]-{2}(a) RETURN count(*) AS c").await,
        "0"
    );
    // Unbounded, the node rule ends every walk: the three closings above, the
    // four triangles through d and the two squares.
    assert_eq!(
        value("MATCH REPEATABLE ELEMENTS SIMPLE (a:Node {name: 'a'})-[:R]-+(a) RETURN count(*) AS c").await,
        "9"
    );
    assert_eq!(value("MATCH SIMPLE (a:Node {name: 'a'})-[:R]-+(a) RETURN count(*) AS c").await, "6");
}

// ---- Review fixes: element records, written order, OPTIONAL paths ----

#[tokio::test]
async fn a_projected_element_keeps_its_record() {
    // A group variable's nodes, carried by WITH, still read as nodes.
    assert_eq!(
        sorted_column("MATCH (x:Node {name: 'a'})((a)-[e:R]->(b)){2}(y) WITH a UNWIND a AS n RETURN n.name AS n").await,
        ["\"a\"", "\"a\"", "\"b\"", "\"c\""]
    );
    // So do a path's elements, projected as a list: nodes and edges alike.
    assert_eq!(
        sorted_column(
            "MATCH p = (a:Node {name: 'a'})-[:R]->{2}(b:Node {name: 'd'}) WITH ELEMENTS(p) AS xs \
             UNWIND xs AS x WITH x WHERE x:Node RETURN x.name AS n"
        )
        .await,
        ["\"a\"", "\"a\"", "\"b\"", "\"c\"", "\"d\"", "\"d\""]
    );
    assert_eq!(
        value(
            "MATCH p = (a:Node {name: 'a'})-[:R]->{2}(b:Node {name: 'd'}) WITH ELEMENTS(p) AS xs \
             UNWIND xs AS x WITH x WHERE x:R RETURN count(*) AS c"
        )
        .await,
        "4"
    );
    // A node held by a computed value unwinds with its record.
    assert_eq!(
        value("MATCH (n:Node {name: 'b'}) WITH coalesce(n) AS m UNWIND [m] AS k RETURN k.name AS n").await,
        "\"b\""
    );
}

#[tokio::test]
async fn a_path_read_from_its_bound_end_keeps_its_written_order() {
    // `b` is bound, so the hop is read from it; the paths still run a to b.
    assert_eq!(
        sorted_column("MATCH (b:Node {name: 'd'}) MATCH (q = (a)-[:R]->(b)) RETURN q").await,
        ["<#1021, rel#6, #1023>", "<#1022, rel#7, #1023>"]
    );
    assert_eq!(
        sorted_column("MATCH (b:Node {name: 'd'}) MATCH p = (a)-[:R]->(b) RETURN p").await,
        ["<#1021, rel#6, #1023>", "<#1022, rel#7, #1023>"]
    );
}

#[tokio::test]
async fn an_optional_block_exports_its_path_variables() {
    assert_eq!(
        value("MATCH (n:Node {name: 'a'}) OPTIONAL { MATCH p = (n)-[:R]->(m:Node {name: 'b'}) } RETURN path_length(p) AS l").await,
        "1"
    );
    assert_eq!(
        value("MATCH (n:Node {name: 'a'}) OPTIONAL { MATCH p = (n)-[:R]->(m:Node {name: 'z'}) } RETURN p").await,
        "null"
    );
    assert_eq!(
        sorted_column("MATCH (x:Node {name: 'a'}) OPTIONAL { MATCH (x)((a)-[e:R]->(b)){2}(y) } RETURN size(a) AS n").await,
        ["2", "2"]
    );
}

// ---- Review fixes: operand WHERE, undirected edges of no edge ----

#[tokio::test]
async fn an_operand_where_reads_its_own_groups_and_subqueries() {
    // §16.7: the operand's WHERE holds of its matches, groups bound: the R
    // trails of one or two edges, and no S edge.
    assert_eq!(
        value("MATCH (((a)-[:R]->(b)){1,2} WHERE size(a) > 0) | (x)-[:S]->(y) RETURN count(*) AS c").await,
        "12"
    );
    // Its subqueries too: of a's two R edges both lead on to d; the hub has
    // three HAS edges.
    assert_eq!(
        value(
            "MATCH ((a:Node {name: 'a'})-[:R]->(b) WHERE EXISTS { (b)-[:R]->(:Node {name: 'd'}) }) \
             | ((a:Hub)-[:HAS]->(b)) RETURN count(*) AS c"
        )
        .await,
        "5"
    );
    // A selective path's WHERE reads its groups before the search: of the
    // trails from a of two or three edges, the shortest to d and to a.
    assert_eq!(
        value("MATCH ANY SHORTEST ((s:Node {name: 'a'})((x)-[:R]->(y)){1,3}(t) WHERE size(x) > 1) RETURN count(*) AS c").await,
        "2"
    );
}

#[tokio::test]
async fn an_edge_that_must_be_undirected_matches_nothing_here() {
    // HydraDB stores no undirected edge (§16.7, G043).
    assert_eq!(value("MATCH (a)~[:R]~+(b) RETURN count(*) AS c").await, "0");
    assert_eq!(
        value("MATCH (n:Node {name: 'a'}) OPTIONAL MATCH (n)~[e:R]~(m) RETURN m").await,
        "null"
    );
    // Of `~[ ]~{0,n}` only the iteration of no edge matches, and its bound
    // is checked all the same (§16.11 Syntax Rule 3, IL018).
    assert_eq!(value("MATCH (a:Node {name: 'a'})~[:R]~{0,2}(b) RETURN count(*) AS c").await, "1");
    assert_eq!(
        status("MATCH (a)~[:R]~{0,256}(b) RETURN count(*) AS c").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
}

// ---- Review fixes: distinct keys, casts, durations, temporal records ----

#[tokio::test]
async fn values_that_are_not_distinct_are_one() {
    // §4.4.3: records field by field, numbers by value, zoned times by the
    // instant they name.
    assert_eq!(value("UNWIND [{a: 1}, {a: 1.0}] AS r RETURN count(DISTINCT r) AS v").await, "1");
    assert_eq!(
        value("UNWIND [TIME '12:00+01:00', TIME '11:00Z'] AS t RETURN count(DISTINCT t) AS v").await,
        "1"
    );
    assert_eq!(
        value("UNWIND [DATETIME '2026-01-01T01:00+01:00', DATETIME '2026-01-01T00:00Z'] AS t WITH t, count(*) AS c RETURN c AS v").await,
        "2"
    );
    assert_eq!(
        value("UNWIND [[1, {a: 2}], [1.0, {a: 2.0}]] AS l RETURN count(DISTINCT l) AS v").await,
        "1"
    );
    // Zoned datetimes at chrono's edge compare by instant, not by shifting
    // the datetime past its range.
    assert_eq!(
        value("RETURN DATETIME '+262142-12-31T23:59:59-14:00' > DATETIME '2026-01-01T00:00Z' AS v").await,
        "true"
    );
}

#[tokio::test]
async fn casts_keep_exact_values_and_bounded_lengths() {
    // §20.8 GR 4 b): an exact string rounds in decimal, a half away from zero.
    for (query, answer) in [
        ("RETURN CAST('9007199254740993.0' AS INTEGER) AS v", "9007199254740993"),
        ("RETURN CAST('2.5' AS INTEGER) AS v", "3"),
        ("RETURN CAST('-2.5' AS INTEGER) AS v", "-3"),
        ("RETURN CAST('12.49' AS INTEGER) AS v", "12"),
        ("RETURN CAST('1.5e1' AS INTEGER) AS v", "15"),
        ("RETURN CAST(-1.5 AS FLOAT16) AS v", "-1.5f"),
        ("RETURN CAST('a' AS CHAR(3)) AS v", "\"a  \""),
        // A nested datetime target takes the current date too.
        ("RETURN CAST([TIME '12:00'] AS LIST<LOCAL DATETIME>) IS NOT NULL AS v", "true"),
        // A subquery inside a record is hoisted as anywhere else.
        ("RETURN {a: VALUE { RETURN 1 AS x LIMIT 1 }}.a AS v", "1"),
    ] {
        assert_eq!(value(query).await, answer, "{query}");
    }
    // Deviations L21 and L20: a declared length is bounded here, and the
    // engine's integers end at UINT64.
    assert_eq!(
        status("RETURN CAST('a' AS CHAR(18446744073709551615)) AS v").await,
        GqlStatus::FEATURE_NOT_SUPPORTED
    );
    assert_eq!(
        status("RETURN CAST('18446744073709551616' AS INT128) AS v").await,
        GqlStatus::FEATURE_NOT_SUPPORTED
    );
}

#[tokio::test]
async fn durations_stay_exact_and_in_range() {
    for (query, answer) in [
        // Division by an integer is exact; only the last remainder rounds.
        ("RETURN DURATION 'P105DT0.000000001S' / 1 AS v", "DURATION 'P105DT0.000000001S'"),
        ("RETURN DURATION 'PT1S' / 3 AS v", "DURATION 'PT0.333333333S'"),
        ("RETURN DURATION 'PT0.000000001S' / 2 AS v", "DURATION 'PT0.000000001S'"),
        ("RETURN DURATION 'P105DT0.000000001S' / 1.0 AS v", "DURATION 'P105DT0.000000001S'"),
        // A span of centuries keeps its fraction of a second.
        (
            "RETURN DURATION_BETWEEN(LOCAL_DATETIME('1600-01-01T00:00:00'), LOCAL_DATETIME('2000-01-01T00:00:00.5')) AS v",
            "DURATION 'P146097DT0.500000000S'",
        ),
    ] {
        assert_eq!(value(query).await, answer, "{query}");
    }
    // An amount past the range is an overflow, not a wrapped duration.
    assert_eq!(
        status("RETURN duration('PT170141183460469231731687303715884105727S') AS v").await,
        GqlStatus::DATETIME_FIELD_OVERFLOW
    );
    assert_eq!(
        status("RETURN DATE({year: 2026, mont: 2}) AS v").await,
        GqlStatus::INVALID_DATETIME_FUNCTION_FIELD_NAME
    );
}

// ---- Review fixes: exact decimals against floats, keys and indexes ----

#[tokio::test]
async fn decimals_compare_and_divide_exactly() {
    for (query, answer) in [
        // A quotient that fits is found however wide its numerator would be.
        ("RETURN 1E33M / 1E33M AS v", "1.000000M"),
        // §19.3: numbers by their algebraic values, to the last digit.
        ("RETURN 1M > 0.99999999999999999999999999999999999999M AS v", "true"),
        // 0.5 is exactly a half; the float 0.1 is a little more than a tenth.
        ("RETURN 0.5M = 0.5 AS v", "true"),
        ("RETURN 0.1M = 0.1 AS v", "false"),
        ("RETURN 0.1M < 0.1 AS v", "true"),
        ("RETURN 9223372036854775807 < 9223372036854775807.5M AS v", "true"),
        // DISTINCT keys agree with that equality.
        ("UNWIND [1, 1.0M, 1.0] AS d RETURN count(DISTINCT d) AS v", "1"),
        ("UNWIND [0.5, 0.50M] AS d RETURN count(DISTINCT d) AS v", "1"),
        ("UNWIND [0.1, 0.1M] AS d RETURN count(DISTINCT d) AS v", "2"),
        // The numeric aggregates and conversions take decimals.
        ("UNWIND [1M, 2M] AS d RETURN stddev_pop(d) = 0.5 AS v", "true"),
        ("UNWIND [1M, 2M] AS d RETURN percentile_cont(d, 0.5M) = 1.5 AS v", "true"),
        ("RETURN toString(1.50M) AS v", "\"1.50\""),
        ("RETURN toInteger(-2.7M) AS v", "-2"),
        // `0M` is one token, as `1M` is.
        ("RETURN 0M AS v", "0M"),
    ] {
        assert_eq!(value(query).await, answer, "{query}");
    }
}

#[tokio::test]
async fn a_decimal_finds_the_stored_numbers_it_equals() {
    // No property stores a decimal, but the stored integer 3 equals 3M.
    assert_eq!(value("MATCH (i:Item) WHERE i.score = 3M RETURN i.name AS n").await, "\"apple\"");
    assert_eq!(
        sorted_column("MATCH (i:Item) WHERE i.score IN [1M, 2.5M, 0.1M] RETURN i.name AS n").await,
        ["\"banana\""]
    );
    assert_eq!(
        sorted_column("MATCH (i:Item) WHERE i.score >= 2.5M RETURN i.score AS s ORDER BY i.score LIMIT 10").await,
        ["3", "5"]
    );
    assert_eq!(value("MATCH (i:Item) WHERE i.score = 0.1M RETURN count(*) AS c").await, "0");
}

#[tokio::test]
async fn declared_byte_lengths_are_bounded() {
    // Deviations L21: a BINARY(n) value is n bytes long.
    assert_eq!(
        status("RETURN CAST(X'00' AS BINARY(1099511627776)) AS v").await,
        GqlStatus::FEATURE_NOT_SUPPORTED
    );
    assert_eq!(value("RETURN CAST(X'01' AS BINARY(3)) AS v").await, "X'010000'");
    // An exponent past i64's range is no decimal, not an overflow panic.
    assert_eq!(
        status("RETURN 1.0E-9223372036854775808M AS v").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
}

// ---- Review fixes: LET elements, grouped PATH, YIELD names ----

#[tokio::test]
async fn let_and_path_read_the_elements_they_hold() {
    // A LET-bound node keeps its record.
    assert_eq!(
        value("MATCH (a:Node {name: 'a'}) RETURN LET n = a IN n.name END AS v").await,
        "\"a\""
    );
    // `PATH[...]` over a group's aggregate.
    assert_eq!(value("MATCH (a:Node {name: 'a'}) RETURN path_length(PATH[min(a)]) AS v").await, "0");
    // A LET body sees its own names before the value keywords.
    assert_eq!(value("RETURN LET unknown = 1 IN unknown + 1 END AS v").await, "2");
    // A subquery in a LET body could not see the LET's names (L24).
    assert_eq!(
        status("RETURN LET x = 1 IN EXISTS { MATCH (n) WHERE n.v = x } END AS v").await,
        GqlStatus::FEATURE_NOT_SUPPORTED
    );
}

#[tokio::test]
async fn yield_names_any_variable_its_pattern_declares() {
    assert_eq!(
        sorted_column("MATCH (s:Node {name: 'a'})((x)-[e:R]->(y)){2}(t) YIELD x RETURN size(x) AS n").await,
        ["2", "2"]
    );
    // §16.3 SR3–5, SR12: only element and path globals, not subpaths.
    for query in [
        "MATCH (q = (a:Node {name: 'a'})-[:R]->(b)) YIELD q RETURN path_length(q) AS n",
        "MATCH (q = (a:M {key: 2})<-[e:R]-(b)) YIELD q RETURN path_length(q) AS v",
    ] {
        assert_eq!(status(query).await, GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION, "{query}");
    }
    assert_eq!(
        sorted_column("MATCH q = (a:Node {name: 'a'})-[:R]->(b) YIELD q RETURN path_length(q) AS n").await,
        ["1", "1"]
    );
    assert_eq!(
        sorted_column("MATCH (q = (a:Node {name: 'a'})-[e:R]->(b)) YIELD b RETURN b.name AS n").await,
        ["\"b\"", "\"c\""]
    );
}

#[tokio::test]
async fn questioned_factors_use_structural_minima() {
    // §16.7 SR8, SR15–16: positive primary edge minimum and a mandatory node.
    for query in [
        "MATCH (a)(x)?(b) RETURN count(*) AS c",
        "MATCH (a)((x))?(b) RETURN count(*) AS c",
        "MATCH -[e:R]->? RETURN count(*) AS c",
    ] {
        assert_eq!(status(query).await, GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION, "{query}");
    }
    assert_eq!(value("MATCH (h:Hub)-[:HAS]->? RETURN count(*) AS c").await, "4");
}

#[tokio::test]
async fn nested_optional_blocks_export_their_final_scope() {
    // §14.4, §16.3: a nested optional block exports its final bindings only.
    assert_eq!(
        status("MATCH (a:Node {name: 'a'}) OPTIONAL { OPTIONAL { MATCH (a)-[:R]->(b) YIELD a } } RETURN b").await,
        GqlStatus::INVALID_REFERENCE
    );
    assert_eq!(
        value("MATCH (a:Node {name: 'a'}) OPTIONAL { OPTIONAL { MATCH (a)-[e:R]->{2}(b) } } FILTER count(e) = 2 RETURN count(*) AS c")
            .await,
        "2"
    );
    // An OPTIONAL's predicate filters its right side, not the preserved row.
    assert_eq!(
        value("MATCH (h:Hub) OPTIONAL MATCH (h)-[:HAS]->(i) WHERE i.score > 99 RETURN count(*) AS c").await,
        "1"
    );
}

#[tokio::test]
async fn lexical_children_keep_let_scope_and_deferred_values() {
    // §20.5: values use the enclosing record; the body reads the amended one.
    assert_eq!(value("RETURN LET x = {a: {b: 2}} IN LET y = x.a IN y.b END END AS v").await, "2");
    assert_eq!(
        status("RETURN LET x = 1 IN LET x = 2 IN x END END AS v").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    assert_eq!(status("RETURN LET x = 1, y = x IN y END AS v").await, GqlStatus::INVALID_REFERENCE);
    assert_eq!(value("RETURN LET x = COALESCE(1, 1 / 0) IN x END AS v").await, "1");
    // §20.14: aggregate-selected nodes and edges retain their actual records.
    assert_eq!(
        value(
            "MATCH (a:Node {name: 'a'})-[e:R]->(b:Node {name: 'b'}) FOR x IN [1, 2] RETURN path_length(PATH[min(a), min(e), min(b)]) AS n"
        )
        .await,
        "1"
    );
    assert_eq!(
        value("MATCH (a:Node {name: 'a'})-[e:R]->(b:Node {name: 'b'}) RETURN path_length(PATH[a, e, b]) AS n").await,
        "1"
    );
}

// ---- Review fixes: optional calls, call column kinds, hoisted marks ----

#[tokio::test]
async fn an_optional_call_that_misses_binds_its_columns_to_null() {
    // A later pattern cannot bind a null column to an unrelated node.
    assert_eq!(
        value("OPTIONAL CALL { MATCH (b:Missing) RETURN b } MATCH (b:Node) RETURN count(*) AS c").await,
        "0"
    );
    assert_eq!(value("OPTIONAL CALL { MATCH (b:Missing) RETURN b } RETURN b").await, "null");
    // A match keeps what the call carries, records included.
    assert_eq!(
        value("CALL { MATCH (b:Node {name: 'b'}) RETURN b } RETURN b.name AS n").await,
        "\"b\""
    );
    // Under a window the call asks for no more rows than are wanted.
    assert_eq!(
        value("MATCH (h:Hub) CALL { MATCH (n:Node) RETURN n } RETURN count(*) AS c").await,
        "5"
    );
    assert_eq!(
        sorted_column("MATCH (h:Hub) CALL { MATCH (n:Node) RETURN n } RETURN n.name AS n LIMIT 2")
            .await
            .len(),
        2
    );
}

#[tokio::test]
async fn a_value_cannot_be_matched_as_an_element() {
    let syntax = GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION;
    // A call column that holds a scalar is a value, however it is spelled.
    assert_eq!(status("CALL { WITH 1 AS x RETURN x } MATCH (x:Node) RETURN x").await, syntax);
    // So is a hoisted subquery's result.
    assert_eq!(
        status("WITH EXISTS { MATCH (m) } AS flag MATCH (flag:Node) RETURN flag").await,
        syntax
    );
    assert_eq!(
        status("LET flag = EXISTS { MATCH (m) } MATCH (flag:Node) RETURN flag").await,
        syntax
    );
    // An unchosen CASE branch's subquery raises nothing.
    assert_eq!(
        value("WITH CASE WHEN true THEN 1 ELSE VALUE { RETURN 1 / 0 AS v LIMIT 1 } END AS x RETURN x").await,
        "1"
    );
}

// ---- Review fixes, second round: keyword names and nested marks ----

#[tokio::test]
async fn keyword_spelled_names_resolve_exactly_and_in_group_by() {
    // The exact spelling first; any case only as a fallback.
    assert_eq!(
        table(&run("WITH 1 AS unknown, 2 AS UNKNOWN RETURN unknown AS a, UNKNOWN AS b").await),
        [["1", "2"]]
    );
    // GROUP BY may name an item spelled like a keyword.
    assert_eq!(
        table(&run("UNWIND [1, 2] AS x RETURN x AS unknown, count(*) AS c GROUP BY unknown ORDER BY unknown").await),
        [["1", "1"], ["2", "1"]]
    );
}

#[tokio::test]
async fn projecting_a_subquery_result_raises_its_exception() {
    // The inner RETURN reads the inner mark, so its exception is raised.
    assert_eq!(
        status("RETURN EXISTS { RETURN EXISTS { RETURN 1 / 0 AS x } AS e } AS v").await,
        GqlStatus::DIVISION_BY_ZERO
    );
    // A branch not taken still reads nothing.
    assert_eq!(
        value("RETURN CASE WHEN true THEN 1 ELSE EXISTS { RETURN 1 / 0 AS x } END AS v").await,
        "1"
    );
}

// ---- Review fixes, second round: deviations of large integers ----

#[tokio::test]
async fn a_deviation_keeps_the_differences_of_large_integers() {
    assert_eq!(
        value("UNWIND [9007199254740992, 9007199254740993] AS x RETURN stddev_pop(x) = 0.5 AS v").await,
        "true"
    );
    assert_eq!(value("UNWIND [1, 2, 3, 4] AS x RETURN stddev_samp(x) > 1.29 AS v").await, "true");
}

// ---- Review fixes, second round: NEXT columns and FINISH warnings ----

#[tokio::test]
async fn next_passes_unnamed_columns_and_finish_keeps_its_warning() {
    assert_eq!(value("RETURN 1 NEXT RETURN 2 AS v").await, "2");
    assert_eq!(value("RETURN 1, 2 AS b NEXT RETURN b AS v").await, "2");
    // FINISH under a warning: the warning, and the result still omitted.
    let outcome = outcome("FOR x IN [null, 1] WITH sum(x) AS s FINISH").await.windowed(0, None);
    assert_eq!(outcome.status.status, GqlStatus::NULL_VALUE_ELIMINATED_IN_SET_FUNCTION);
    assert!(outcome.additional.iter().any(|object| object.status == GqlStatus::OMITTED_RESULT));
    assert!(!outcome.additional.iter().any(|object| object.status == GqlStatus::NO_DATA));
}

// ---- Review fixes, second round: selective path predicates ----

#[tokio::test]
async fn a_selective_path_reads_no_other_path_of_its_pattern() {
    // §16.6 Syntax Rule 8: `h` is only the other path's.
    assert_eq!(
        status("MATCH REPEATABLE ELEMENTS ANY SHORTEST ((a)-[:R]->(b) WHERE a = h), (h:Hub) RETURN count(*) AS n").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    // A variable the enclosing query bound is read from its row.
    assert_eq!(
        value("MATCH (h:Node {name: 'a'}) MATCH ANY SHORTEST ((a)-[:R]->+(b) WHERE a = h) RETURN count(*) AS n").await,
        "4"
    );
}

// ---- Review fixes, second round: ordering paths ----

#[tokio::test]
async fn grouped_paths_order_by_their_elements() {
    assert_eq!(
        table(&run("MATCH p = (a:Node {name: 'a'})-[:R]->(b) RETURN p, count(*) AS c ORDER BY p DESC").await),
        [["<#1020, rel#5, #1022>", "1"], ["<#1020, rel#4, #1021>", "1"]]
    );
    assert_eq!(
        value("MATCH p = (a:Node {name: 'a'})-[:R]->(b) RETURN max(p) AS v").await,
        "<#1020, rel#5, #1022>"
    );
}

// ---- Review fixes, second round: union operand scope and windows ----

#[tokio::test]
async fn a_union_operand_reads_its_own_subpaths() {
    assert_eq!(
        value("MATCH (q = (a:Node {name: 'a'})-[:R]->(b) WHERE path_length(q) > 0) | (x)-[:S]->(y) RETURN count(*) AS c").await,
        "2"
    );
    // A multiset alternation under a window stops at the rows wanted.
    assert_eq!(
        table(&run("MATCH (a:Node {name: 'a'})-[:R]->(b) |+| (a)-[:S]->(b) RETURN a.name AS n LIMIT 1").await),
        [["\"a\""]]
    );
}

// ---- Review fixes, second round: temporal edges, records holding elements ----

#[tokio::test]
async fn temporal_values_keep_their_amounts_at_the_edges() {
    for (query, answer) in [
        // An integral factor multiplies exactly.
        ("RETURN DURATION 'P105DT0.000000001S' * 1.0 AS v", "DURATION 'P105DT0.000000001S'"),
        // A day-time span at chrono's edge needs no shifted datetime.
        (
            "RETURN DURATION_BETWEEN(DATETIME '+262142-12-31T23:59:59-14:00', DATETIME '+262142-12-31T23:59:59-14:00') AS v",
            "DURATION 'PT0S'",
        ),
        // A node in a record keeps its properties, read through the field
        // or after a projection.
        ("MATCH (n:Hub) RETURN {node: n}.node.name AS v", "\"hub\""),
        ("MATCH (n:Hub) WITH {node: n} AS r WITH r.node AS m RETURN m.name AS v", "\"hub\""),
        // An exponent past i64 rounds to nothing rather than overflowing.
        ("RETURN CAST('1e-9223372036854775808' AS INTEGER) AS v", "0"),
    ] {
        assert_eq!(value(query).await, answer, "{query}");
    }
    for (query, condition) in [
        // An absolute value that does not fit is an overflow, not another amount.
        (
            "RETURN ABS(DURATION({months: -9223372036854775808})) AS v",
            GqlStatus::INTERVAL_FIELD_OVERFLOW,
        ),
        // A misplaced colon is no offset.
        ("RETURN ZONED_TIME('12:00+1:234') AS v", GqlStatus::INVALID_DATETIME_FORMAT),
        // A duration is of one unit group.
        (
            "RETURN DURATION({years: 1, days: 5}) AS v",
            GqlStatus::INVALID_DURATION_FUNCTION_FIELD_NAME,
        ),
    ] {
        assert_eq!(status(query).await, condition, "{query}");
    }
}

// ---- Review fixes, second round: wide decimal intermediates ----

#[tokio::test]
async fn decimal_results_that_fit_are_found_through_wide_intermediates() {
    for (query, answer) in [
        ("RETURN 1E37M - 9999999999999999999999999999999999999.9M AS v", "0.1M"),
        (
            "RETURN CAST('1.0000000000000000000000000000000000000000' AS DECIMAL(2, 1)) AS v",
            "1.0M",
        ),
        ("RETURN -7.5M % 2M AS v", "-1.5M"),
    ] {
        assert_eq!(value(query).await, answer, "{query}");
    }
}

// ---- Review fixes, second round: local ids, path aggregates, optional groups ----

#[tokio::test]
async fn groups_and_local_names_keep_what_they_are() {
    // A LET-bound node's `.id` is the node's.
    assert_eq!(
        value("MATCH (a:Node {name: 'a'}) RETURN a.id = LET n = a IN n.id END AS v").await,
        "true"
    );
    // A selective path's own WHERE aggregates its group horizontally.
    assert_eq!(
        value("MATCH ANY SHORTEST ((a:Node {name: 'a'})-[e:R]->+(b) WHERE count(e) > 1) RETURN count(*) AS c").await,
        "4"
    );
    // An OPTIONAL block's group variables stay groups after it.
    assert_eq!(
        value("MATCH (x:Node {name: 'a'}) OPTIONAL { MATCH (x)((a)-[e:R]->(b)){2}(y) } FILTER count(a) = 2 RETURN count(*) AS c").await,
        "2"
    );
}

// ---- Review fixes, second round: calls, definitions across NEXT ----

#[tokio::test]
async fn definitions_and_calls_keep_their_values() {
    // A projected VALUE subquery's exception is raised, not nulled.
    assert_eq!(
        status("WITH VALUE { RETURN 1 / 0 AS v LIMIT 1 } AS x RETURN x").await,
        GqlStatus::DIVISION_BY_ZERO
    );
    // A call that does not import `r` has a fresh `r`, whose `.id` is a node's.
    assert_eq!(
        value("MATCH ()-[r:R]->() CALL () { MATCH (r:Node) RETURN r.id AS x } RETURN count(x) AS c").await,
        "30"
    );
    // A definition is evaluated once and its value crosses NEXT.
    assert_eq!(
        value("VALUE x = CURRENT_DATE RETURN 1 AS current_date NEXT RETURN x IS :: DATE AS v").await,
        "true"
    );
    assert_eq!(value("VALUE x = 1 RETURN 2 AS y NEXT RETURN x + y AS v").await, "3");
    // A call's sort may read its aggregate.
    assert_eq!(
        value("CALL { UNWIND [1, 2] AS x RETURN count(*) AS c ORDER BY c } RETURN c").await,
        "2"
    );
}

// ---- Review fixes, third round: NEXT's own column names ----

#[tokio::test]
async fn next_column_names_avoid_the_query_s_own() {
    assert_eq!(
        value("RETURN 1, 2 AS __hydradb_next_column_0 NEXT RETURN __hydradb_next_column_0 AS v").await,
        "2"
    );
}

// ---- Review fixes, third round: union operands' names ----

#[tokio::test]
async fn union_operands_resolve_and_check_every_name() {
    // A keyword-spelled node of an operand is that node.
    assert_eq!(
        value("MATCH ((UNKNOWN:Node {name: 'a'}) WHERE UNKNOWN IS NOT NULL) |+| (UNKNOWN:Node {name: 'a'}) RETURN count(*) AS c").await,
        "2"
    );
    let syntax = GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION;
    // A subquery beside it does not hide a read of another operand's name.
    assert_eq!(
        status("MATCH ((a)-[:R]->(b) WHERE b.name = c.name OR EXISTS { (b) }) | (c)-[:S]->(d) RETURN count(*) AS n").await,
        syntax
    );
    // Nor does an inline property map.
    assert_eq!(
        status("MATCH ((a)-[:R]->(b {name: c.name})) | (c)-[:S]->(d) RETURN count(*) AS n").await,
        syntax
    );
}

// ---- Review fixes, third round: fields of an aggregate's choice ----

#[tokio::test]
async fn a_field_of_an_aggregated_record_reads_the_chosen_element() {
    assert_eq!(
        value("MATCH (n:Node) WITH n ORDER BY n.name RETURN max({node: n.name, n: n}).n.name AS v").await,
        "\"loop\""
    );
}

// ---- Review fixes, third round: floats cast to decimals ----

#[tokio::test]
async fn a_float_cast_to_a_decimal_keeps_its_own_digits() {
    for (query, answer) in [
        ("RETURN CAST(0.1 AS DECIMAL(21, 20)) AS v", "0.10000000000000000555M"),
        ("RETURN CAST(0.5 AS DECIMAL(2, 1)) AS v", "0.5M"),
        ("RETURN CAST(-2.5 AS DECIMAL(2, 0)) AS v", "-3M"),
        ("RETURN CAST(1e10 AS DECIMAL(12, 1)) AS v", "10000000000.0M"),
    ] {
        assert_eq!(value(query).await, answer, "{query}");
    }
}

// ---- Review fixes, third round: questioned branches, OPTIONAL YIELD, groups ----

#[tokio::test]
async fn questioned_branches_optional_yields_and_groups() {
    // The branch that skips `?` still has a node before its edge.
    assert!(!table(&run("MATCH ((a)-[:R]->(b))? -[:R]->(c) RETURN c").await).is_empty());
    // An OPTIONAL block hands on only what its YIELD kept.
    assert_eq!(
        status("MATCH (x:Node {name: 'a'}) OPTIONAL { MATCH (x)-[:R]->(b) YIELD x } RETURN b").await,
        GqlStatus::INVALID_REFERENCE
    );
    // A horizontal aggregate reads each element of its group in turn.
    assert_eq!(
        value("MATCH (a:Node {name: 'a'})-[e:R]->{2}(b) WHERE count(e) = 2 RETURN count(*) AS c").await,
        "2"
    );
    assert_eq!(
        value("MATCH (s:Node {name: 'a'})((x)-[:R]->(y)){2}(t) FILTER count(x) = 2 RETURN count(*) AS c").await,
        "2"
    );
}

// ---- Review fixes, third round: definitions across NEXT, union call columns ----

#[tokio::test]
async fn definitions_leave_grouping_alone_and_union_columns_keep_identity() {
    // An aggregate-only statement before NEXT still has its one row.
    assert_eq!(
        value("VALUE x = 1 MATCH (n:Missing) RETURN count(*) AS c NEXT RETURN c + x AS v").await,
        "1"
    );
    // A call column that is an edge in one arm and a node in another keeps
    // `.id` an identity, answered by each row.
    assert_eq!(
        value(
            "CALL { MATCH ()-[r:R]->() RETURN r AS x UNION ALL MATCH (n:Node) RETURN n AS x } \
             RETURN count(x.id) AS c"
        )
        .await,
        "5"
    );
}

// ---- Review fixes, fourth round: fractional duration scaling, exponents ----

#[tokio::test]
async fn durations_scale_exactly_by_fractions() {
    for (query, answer) in [
        ("RETURN DURATION 'P105DT0.000000001S' * 0.5 AS v", "DURATION 'P52DT12H0.000000001S'"),
        ("RETURN DURATION 'PT1S' * 0.25 AS v", "DURATION 'PT0.250000000S'"),
        ("RETURN DURATION 'PT1S' / 0.5 AS v", "DURATION 'PT2S'"),
        ("RETURN DURATION 'PT3S' / 1.5 AS v", "DURATION 'PT2S'"),
        ("RETURN DURATION 'P10M' * 0.5 AS v", "DURATION 'P5M'"),
        ("RETURN DURATION 'P1M' * 0.5 AS v", "DURATION 'P1M'"),
    ] {
        assert_eq!(value(query).await, answer, "{query}");
    }
    // A misplaced underscore makes no exponent.
    assert_eq!(
        status("RETURN CAST('1.0E__5' AS INTEGER) AS v").await,
        GqlStatus::INVALID_CHARACTER_VALUE_FOR_CAST
    );
}

// ---- Review fixes, fourth round: approximate strings cast to decimals ----

#[tokio::test]
async fn an_approximate_string_casts_to_a_decimal_as_its_float_does() {
    for (query, answer) in [
        ("RETURN CAST('2F' AS DECIMAL(2, 0)) AS v", "2M"),
        ("RETURN CAST('2.5D' AS DECIMAL(2, 1)) AS v", "2.5M"),
        ("RETURN CAST('0.1F' AS DECIMAL(21, 20)) AS v", "0.10000000000000000555M"),
        ("RETURN CAST('0.1' AS DECIMAL(21, 20)) AS v", "0.10000000000000000000M"),
    ] {
        assert_eq!(value(query).await, answer, "{query}");
    }
}

// ---- Review fixes, fourth round: operand aggregates, groups after OPTIONAL ----

#[tokio::test]
async fn operands_aggregate_and_ended_groups_stay_ended() {
    assert_eq!(
        value("MATCH (((a)-[:R]->(b)){1,2} WHERE count(a) > 0) | (x)-[:S]->(y) RETURN count(*) AS c").await,
        "12"
    );
    // A YIELD inside an OPTIONAL block ends a group as it does outside one.
    let outside = status("MATCH (x:Node {name: 'a'}) MATCH (x)-[e:R]->{2}(b) YIELD e FILTER count(e) = 2 RETURN count(*) AS c").await;
    assert_eq!(
        status("MATCH (x:Node {name: 'a'}) OPTIONAL { MATCH (x)-[e:R]->{2}(b) YIELD e } FILTER count(e) = 2 RETURN count(*) AS c").await,
        outside
    );
}

// ---- Review fixes, fourth round: union call kinds, nested arms ----

#[tokio::test]
async fn union_call_kinds_and_nested_arms() {
    // A column that is a value in any arm is no element.
    assert_eq!(
        status("CALL { RETURN 1 AS x UNION ALL MATCH (n:Node) RETURN n AS x } MATCH (x:Node) RETURN x").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    // Every arm, nested or not, starts from the definitions.
    assert_eq!(
        sorted_column("VALUE x = 1 RETURN x AS v UNION { RETURN x + 1 AS v UNION RETURN x + 2 AS v }").await,
        ["1", "2", "3"]
    );
}

// ---- Review fixes, fifth round: zero durations ----

#[tokio::test]
async fn a_zero_duration_scales_to_zero() {
    assert_eq!(value("RETURN DURATION 'PT0S' * 1e100 AS v").await, "DURATION 'PT0S'");
    assert_eq!(value("RETURN DURATION 'P0M' / 1e-100 AS v").await, "DURATION 'P0M'");
}

// ---- Review fixes, fifth round: wide shifts, sums with floats ----

#[tokio::test]
async fn wide_casts_and_mixed_sums_do_not_depend_on_luck() {
    assert_eq!(
        status("RETURN CAST(2e54 AS DECIMAL(38, 0)) AS v").await,
        GqlStatus::NUMERIC_VALUE_OUT_OF_RANGE
    );
    // A float anywhere makes the sum approximate, wherever it comes.
    assert_eq!(value("UNWIND [9E37M, 9E37M, 0.0] AS d RETURN sum(d) > 1e38 AS v").await, "true");
    assert_eq!(value("UNWIND [0.0, 9E37M, 9E37M] AS d RETURN sum(d) > 1e38 AS v").await, "true");
}

// ---- Review fixes, fifth round: groups inside subqueries ----

#[tokio::test]
async fn a_subquery_aggregates_an_enclosing_group() {
    assert_eq!(
        value("MATCH (a)-[e:R]->{2}(b) FILTER EXISTS { MATCH (z:Hub) WHERE count(e) = 2 } RETURN count(*) AS c").await,
        "6"
    );
}

// ---- Review fixes, fifth round: grouped call sorts ----

#[tokio::test]
async fn a_grouped_call_sorts_by_a_renamed_key() {
    assert_eq!(
        value("CALL { UNWIND [2, 1] AS x RETURN x AS k, count(*) AS c ORDER BY k LIMIT 1 } RETURN k").await,
        "1"
    );
    // A WITH's sort still reads its projected names.
    assert_eq!(
        value("UNWIND [2, 1, 1] AS x WITH x AS k, count(*) AS c ORDER BY c DESC LIMIT 1 RETURN k").await,
        "1"
    );
}

#[tokio::test]
async fn an_ungrouped_call_sorts_by_a_renamed_key() {
    assert_eq!(
        value("CALL { UNWIND [2, 1, 3] AS x RETURN x AS k ORDER BY k LIMIT 1 } RETURN k").await,
        "1"
    );
    assert_eq!(
        value("CALL { UNWIND [2, 1, 3] AS x RETURN x * 10 AS k ORDER BY x DESC LIMIT 1 } RETURN k").await,
        "30"
    );
}

// ---- Review fixes, sixth round: strings cast to decimals round once ----

#[tokio::test]
async fn a_string_cast_to_a_decimal_rounds_once() {
    for (query, answer) in [
        (
            "RETURN CAST('0.4999999999999999999999999999999999999999' AS DECIMAL(1, 0)) AS v",
            "0M",
        ),
        ("RETURN CAST('0.5' AS DECIMAL(1, 0)) AS v", "1M"),
        ("RETURN CAST('1.25' AS DECIMAL(3, 1)) AS v", "1.3M"),
    ] {
        assert_eq!(value(query).await, answer, "{query}");
    }
}

// ---- Review fixes, sixth round: constant LETs, path subquery groups ----

#[tokio::test]
async fn constant_lets_and_path_subqueries_keep_their_rows_and_groups() {
    assert_eq!(
        table(&run("MATCH (n:Missing) RETURN LET x = 1 IN x END AS v, count(*) AS c").await),
        [["1", "0"]]
    );
    assert_eq!(
        table(&run("MATCH (n:Missing) RETURN LET x = 1 IN 2 END AS v, count(*) AS c").await),
        [["2", "0"]]
    );
    assert_eq!(
        value(
            "MATCH ANY SHORTEST ((a:Node {name: 'a'})-[e:R]->+(b) WHERE EXISTS { MATCH (z:Hub) WHERE count(e) > 1 }) RETURN count(*) AS c"
        )
        .await,
        "4"
    );
}

// ---- Review fixes, sixth round: sort keys read the input, groups in WITH ----

#[tokio::test]
async fn sort_keys_read_the_input_and_with_subqueries_see_groups() {
    // An alias reusing an input's name does not move a key.
    assert_eq!(
        value("CALL { UNWIND [2, 1] AS x RETURN x AS k, -x AS x, count(*) AS c ORDER BY k LIMIT 1 } RETURN k").await,
        "1"
    );
    assert_eq!(
        value("UNWIND [2, 1, 3] AS x WITH x AS k, -x AS x ORDER BY x LIMIT 1 RETURN k").await,
        "3"
    );
    assert_eq!(
        value("UNWIND [2, 1, 1] AS x WITH x AS k, count(*) AS total ORDER BY total * 2 DESC LIMIT 1 RETURN k").await,
        "1"
    );
    // A subquery in a WITH item aggregates the groups before it.
    assert_eq!(
        sorted_column("MATCH (a:Node {name: 'a'})-[e:R]->{2}(b) WITH EXISTS { MATCH (z:Hub) WHERE count(e) = 2 } AS ok RETURN ok").await,
        ["true", "true"]
    );
}

// ---- Review fixes, seventh round: subqueries in horizontal aggregates ----

#[tokio::test]
async fn a_subquery_inside_a_horizontal_aggregate_is_refused() {
    assert_eq!(
        status("MATCH (h:Hub)-[e:HAS]->{1}(i) WHERE sum(CASE WHEN EXISTS { MATCH (z) WHERE e.weight > 0 } THEN e.weight ELSE 0 END) > 0 RETURN count(*) AS c").await,
        GqlStatus::FEATURE_NOT_SUPPORTED
    );
    // A vertical aggregate's subquery is computed per input row, as before.
    assert_eq!(
        value("MATCH (n:Node) RETURN count(CASE WHEN EXISTS { (n)-[:R]->() } THEN 1 END) AS c").await,
        "5"
    );
}

// ---- Review fixes, eighth round: an operand's subqueries keep its scope ----

#[tokio::test]
async fn an_operand_subquery_cannot_read_another_operand() {
    // `c` is the other operand's, so here it is a new variable: some node
    // exists, and every R match of the operand stands.
    assert_eq!(
        value("MATCH ((a)-[:R]->(b) WHERE EXISTS { (c) }) | (c)-[:S]->(d) RETURN count(*) AS n").await,
        "6"
    );
    // Not declared in the subquery, it is no reference at all.
    assert_eq!(
        status("MATCH ((a)-[:R]->(b) WHERE EXISTS { MATCH (z) WHERE z = c }) | (c)-[:S]->(d) RETURN count(*) AS n").await,
        GqlStatus::INVALID_REFERENCE
    );
}

// ---- Review fixes, seventh round: sort keys read projected names ----

#[tokio::test]
async fn sort_keys_read_the_projection_without_rewriting() {
    // An element test on a projected name, and a LET in a key, as written.
    assert_eq!(
        sorted_column("MATCH (n:Node) WITH n AS m ORDER BY PROPERTY_EXISTS(m, name) LIMIT 2 RETURN m.name AS v")
            .await
            .len(),
        2
    );
    assert_eq!(
        value("UNWIND [2, 1] AS x WITH x AS k ORDER BY LET x = 0 IN k END, k LIMIT 1 RETURN k").await,
        "1"
    );
    // A key that mixes a column with an aggregate.
    assert_eq!(
        table(&run("UNWIND [2, 1, 1] AS x RETURN x AS k, count(*) AS c ORDER BY c + count(*) DESC LIMIT 1").await),
        [["1", "2"]]
    );
}

// ---- Review fixes, eighth round: grouped sort keys read returned elements ----

#[tokio::test]
async fn a_grouped_sort_key_reads_a_returned_node() {
    // `n` is returned and grouped on; its key still reads the node's name.
    assert_eq!(
        table(&run("MATCH (n:Node) RETURN n.name AS v, count(*) AS c ORDER BY v DESC LIMIT 1").await),
        table(&run("MATCH (n:Node) RETURN n, count(*) AS c ORDER BY n.name DESC LIMIT 1 NEXT RETURN n.name AS v, c").await)
    );
    assert_eq!(
        table(&run("MATCH (n:Node) RETURN n, count(*) AS c ORDER BY n.name DESC LIMIT 1").await),
        table(&run("MATCH (n:Node) RETURN n, 1 AS c ORDER BY n.name DESC LIMIT 1").await)
    );
}

#[tokio::test]
async fn a_call_body_keeps_its_set_operation() {
    assert!(table(&run("CALL { RETURN 1 AS x EXCEPT RETURN 1 AS x } RETURN x").await).is_empty());
    assert!(table(&run("CALL { RETURN 1 AS x INTERSECT RETURN 2 AS x } RETURN x").await).is_empty());
    assert_eq!(value("CALL { RETURN 1 AS x OTHERWISE RETURN 2 AS x } RETURN x").await, "1");
}

// ---- Review fixes, ninth round: exact decimal exponents ----

#[tokio::test]
async fn a_decimal_exponent_keeps_its_parity_and_fraction() {
    assert_eq!(value("RETURN power(-1, 9007199254740993M) AS v").await, "-1.0f");
    assert_eq!(value("RETURN power(-2, 2.0M) AS v").await, "4.0f");
    assert_eq!(
        status("RETURN power(-1, 1.00000000000000000001M) AS v").await,
        GqlStatus::INVALID_ARGUMENT_FOR_POWER_FUNCTION
    );
}

// ---- Review fixes, ninth round: record identities and range-edge arithmetic ----

#[tokio::test]
async fn a_projected_identity_of_a_record_is_its_field() {
    // `r.id` of a record is its `id` field, not the record, in every projection.
    assert_eq!(
        sorted_column("UNWIND [2, 1] AS x WITH {id: x} AS r WITH r.id AS k RETURN k + 1 AS v").await,
        ["2", "3"]
    );
    assert_eq!(
        sorted_column("UNWIND [2, 1] AS x WITH {id: x} AS r WITH r.id AS k, count(*) AS c RETURN k + c AS v").await,
        ["2", "3"]
    );
    // A scalar has no identity.
    assert_eq!(value("WITH 5 AS r WITH r.id AS k RETURN k").await, "null");
    // A node's is the node.
    assert_eq!(
        value("MATCH (n:Node {name: 'a'}) WITH n.id AS k RETURN k.name AS v").await,
        value("RETURN 'a' AS v").await
    );
}

#[tokio::test]
async fn datetime_arithmetic_at_the_range_edge_does_not_overflow() {
    let edge = "LOCAL_DATETIME({year: -262143, month: 1, day: 1, hour: 0, minute: 0, second: 0, nanosecond: 2})";
    assert_eq!(
        value(&format!("RETURN {edge} - DURATION 'PT0.000000001S' = LOCAL_DATETIME({{year: -262143, month: 1, day: 1, hour: 0, minute: 0, second: 0, nanosecond: 1}}) AS v")).await,
        "true"
    );
}

#[tokio::test]
async fn a_grouped_sort_key_reads_a_record_field() {
    assert_eq!(
        table(&run("UNWIND [2, 1] AS x WITH {id: x} AS r RETURN r.id AS k, count(*) AS c ORDER BY k + 1 LIMIT 1").await),
        [["1", "1"]]
    );
}

// ---- Review fixes, tenth round: a grouped key's name replaced by the projection ----

#[tokio::test]
async fn a_grouped_sort_key_reads_the_node_that_replaces_an_edge_s_name() {
    // `r` is the returned node, so `r.id` is the node, sorted by identity.
    assert_eq!(
        table(&run("MATCH (n:Node)-[r:R]->() RETURN n AS r, count(*) AS c ORDER BY r.id DESC LIMIT 1").await),
        [["#1024", "1"]]
    );
    assert_eq!(
        value("MATCH (n:Node)-[r:R]->() WITH n AS r, count(*) AS c ORDER BY r.id DESC LIMIT 1 RETURN r").await,
        "#1024"
    );
}

// ---- Review fixes, tenth round: decimal arguments of numeric functions ----

#[tokio::test]
async fn a_decimal_argument_is_checked_and_measured_exactly() {
    // Outside [-1, 1] by 1e-20, though its nearest float is 1.
    assert_eq!(
        status("RETURN acos(1.00000000000000000001M) AS v").await,
        GqlStatus::INVALID_ARGUMENT_FOR_TRIGONOMETRIC_FUNCTION
    );
    assert_eq!(value("RETURN acos(1M) AS v").await, "0.0f");
    // A base near 1 is not 1, and keeps its distance from it.
    assert_eq!(
        value("RETURN log(1.00000000000000000001M, 2M) AS v").await,
        format!("{:?}f", 2f64.ln() / 1e-20)
    );
    assert_eq!(
        status("RETURN log(1M, 2M) AS v").await,
        GqlStatus::INVALID_ARGUMENT_FOR_GENERAL_LOGARITHM_FUNCTION
    );
    assert_eq!(value("RETURN ln(1.00000000000000000001M) AS v").await, "1e-20f");
    // An exact value a float holds keeps the dedicated routines.
    assert_eq!(value("RETURN log10(1000M) AS v").await, "3.0f");
}

// ---- Review fixes, tenth round: a node's identity through a record ----

#[tokio::test]
async fn a_node_s_identity_through_a_record_is_the_node() {
    assert_eq!(
        table(&run("MATCH (n:Node {name: 'a'}) RETURN {node: n}.node.id AS v, n.id AS w").await),
        [["#1020", "#1020"]]
    );
    assert_eq!(
        value("MATCH (n:Node {name: 'a'}) LET r = {node: n} RETURN r.node.id = n AS v").await,
        "true"
    );
}

// ---- Review fixes, tenth round: an operand's element tests and keywords ----

#[tokio::test]
async fn an_operand_s_element_test_reads_its_scope() {
    // §16.7 Syntax Rule 11: `r` and `c` are only the other operand's.
    for query in [
        "MATCH ((a)-[:R]->(b) WHERE b IS SOURCE OF r) | (c)-[r:R]->(d) RETURN count(*) AS n",
        "MATCH ((a)-[:R]->(b) WHERE c IS LABELED Node) | (c)-[:R]->(d) RETURN count(*) AS n",
    ] {
        assert_eq!(status(query).await, GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION, "{query}");
    }
}

#[tokio::test]
async fn an_operand_s_keyword_resolves_in_its_scope() {
    // Only the other operand declares `unknown`, so here UNKNOWN is the value.
    assert_eq!(
        value("MATCH ((a)-[:R]->(b) WHERE UNKNOWN IS NULL) | (unknown)-[:R]->(d) RETURN count(*) AS n").await,
        "12"
    );
    // An operand's own `unknown` is its variable.
    assert_eq!(
        value("MATCH ((unknown)-[:R]->(b) WHERE UNKNOWN IS NOT NULL) | (c)-[:R]->(d) RETURN count(*) AS n").await,
        "12"
    );
}

// ---- Review fixes, tenth round: a selective path's subqueries ----

#[tokio::test]
async fn a_selective_path_s_subqueries_read_no_other_path() {
    // §16.6 Syntax Rule 8: a reference inside the path's subquery is the
    // path's, and `h` is only the other path's.
    for query in [
        "MATCH REPEATABLE ELEMENTS ANY SHORTEST ((a)-[:R]->(b) WHERE EXISTS { MATCH (z) WHERE z = h }), (h:Hub) RETURN count(*) AS n",
        "MATCH REPEATABLE ELEMENTS ANY SHORTEST ((a)-[:R]->(b) WHERE EXISTS { (h)-[:HAS]->() }), (h:Hub) RETURN count(*) AS n",
        "MATCH REPEATABLE ELEMENTS ANY SHORTEST ((a)-[:R]->(b) WHERE a = h AND EXISTS { (b)-[:R]->() }), (h:Hub) RETURN count(*) AS n",
        "MATCH REPEATABLE ELEMENTS ANY SHORTEST (a {name: h.name})-[:R]->(b), (h:Hub) RETURN count(*) AS n",
    ] {
        assert_eq!(status(query).await, GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION, "{query}");
    }
    // Its own names, and the enclosing query's, it reads.
    assert_eq!(
        value("MATCH REPEATABLE ELEMENTS ANY SHORTEST ((a)-[:R]->(b) WHERE EXISTS { (b)-[:R]->(x) }), (h:Hub) RETURN count(*) AS n").await,
        "6"
    );
    assert_eq!(
        value("MATCH (h:Node {name: 'a'}) MATCH ANY SHORTEST ((a)-[:R]->+(b) WHERE EXISTS { MATCH (z) WHERE z = h AND z = a }) RETURN count(*) AS n").await,
        "4"
    );
}

// ---- Review fixes, tenth round: what a RETURN before NEXT sorts by ----

#[tokio::test]
async fn an_unnamed_next_column_does_not_shadow_a_sort_key() {
    let query = "MATCH (n:Item) WITH n.score AS __hydradb_next_column_0, n \
                 RETURN 1, n.name AS v ORDER BY __hydradb_next_column_0 DESC LIMIT 1";
    assert_eq!(value(&format!("{query} NEXT RETURN v")).await, "\"Durian\"");
}

#[tokio::test]
async fn a_sort_key_before_next_reads_the_alias_that_replaces_a_name() {
    // §14.10 Syntax Rule 4 c i 2 A VIII: `n` is the return item, the name.
    assert_eq!(
        value("MATCH (n:Item) RETURN n.name AS n ORDER BY n LIMIT 1 NEXT RETURN n").await,
        "\"Durian\""
    );
}

// ---- Review fixes, tenth round: float deviations keep their separations ----

#[tokio::test]
async fn a_deviation_of_nearby_large_floats_is_exact() {
    // Two adjacent floats at 1e308 are one ulp, 2^971, apart: half of it.
    let half_ulp = format!("{}f", 2f64.powi(970));
    assert_eq!(
        value("UNWIND [1e308, 1.0000000000000002e308] AS x RETURN stddev_pop(x) AS v").await,
        half_ulp
    );
    assert_eq!(
        value("UNWIND [1e308, -1e308] AS x RETURN stddev_pop(x) AS v").await,
        format!("{}f", 1e308)
    );
    assert_eq!(
        value("UNWIND [5e-324, 1.5e-323] AS x RETURN stddev_pop(x) AS v").await,
        format!("{}f", 5e-324)
    );
}

#[tokio::test]
async fn exact_numeric_deviations_preserve_small_spreads() {
    // §20.9 GR 7 a v: two values separated by d have deviations
    // |d| / 2 (population) and |d| / sqrt(2) (sample), at any origin.
    for inputs in [
        "[1M, 1.00000000000000000001M]",
        "[1.00000000000000000001M, 1M]",
        "[-1M, -1.00000000000000000001M]",
        "[1, 1.00000000000000000001M]",
        "[100000000000000000, 100000000000000000.00000000000000000001M]",
        "[null, 1M, 1.00000000000000000001M, null]",
    ] {
        for (function, expected) in [("STDDEV_POP", 5e-21), ("STDDEV_SAMP", 1e-20 / 2f64.sqrt())] {
            let query = format!("UNWIND {inputs} AS x RETURN {function}(x) AS v");
            let actual = value(&query).await.trim_end_matches('f').parse::<f64>().unwrap();
            assert!(
                actual.is_finite() && (actual / expected - 1.0).abs() < 4e-15,
                "{query}: {actual} != {expected}"
            );
        }
    }
    for (function, expected) in [("STDDEV_POP", 1e-38), ("STDDEV_SAMP", 2e-38 / 2f64.sqrt())] {
        let query = format!("UNWIND [1E-38M, 3E-38M] AS x RETURN {function}(x) AS v");
        let actual = value(&query).await.trim_end_matches('f').parse::<f64>().unwrap();
        assert!((actual / expected - 1.0).abs() < 4e-15, "{query}: {actual} != {expected}");
    }
    let distinct = "UNWIND [1M, 1.00M, 1.00000000000000000001M, null] AS x \
                    RETURN STDDEV_POP(DISTINCT x) AS v";
    let actual = value(distinct).await.trim_end_matches('f').parse::<f64>().unwrap();
    assert!((actual / 5e-21 - 1.0).abs() < 4e-15);
}

#[tokio::test]
async fn exact_numeric_deviations_keep_range_and_coercion_policy() {
    // Differences can exceed a public decimal's precision/scale without
    // exceeding the range of the approximate aggregate result (§20.9).
    for (inputs, distance) in [
        (
            "[99999999999999999999999999999999999999M, -99999999999999999999999999999999999999M]",
            2e38,
        ),
        ("[1E37M, 1E-38M]", 1e37),
        ("[1E-38M, 1E37M]", 1e37),
    ] {
        for (function, divisor) in [("STDDEV_POP", 2.0), ("STDDEV_SAMP", 2f64.sqrt())] {
            let query = format!("UNWIND {inputs} AS x RETURN {function}(x) AS v");
            let actual = value(&query).await.trim_end_matches('f').parse::<f64>().unwrap();
            let expected = distance / divisor;
            assert!(
                actual.is_finite() && (actual / expected - 1.0).abs() < 4e-15,
                "{query}: {actual} != {expected}"
            );
        }
    }
    // An approximate operand retains the existing binary64 coercion.
    assert_eq!(
        value("UNWIND [1.0, 1.00000000000000000001M] AS x RETURN STDDEV_POP(x) AS v").await,
        "0f"
    );
    assert_eq!(value("UNWIND [1M, 1M, null] AS x RETURN STDDEV_POP(x) AS v").await, "0f");
    assert_eq!(value("UNWIND [1M, 1M] AS x RETURN STDDEV_SAMP(DISTINCT x) AS v").await, "null");
    assert_eq!(value("UNWIND [null, null] AS x RETURN STDDEV_POP(x) AS v").await, "null");
    assert_eq!(value("RETURN COALESCE(1, STDDEV_POP('bad')) AS v").await, "1");
    assert_eq!(
        status("UNWIND [1M, 'bad'] AS x RETURN STDDEV_POP(x) AS v").await,
        GqlStatus::INVALID_VALUE_TYPE
    );
}

#[tokio::test]
async fn substring_lengths_accept_whole_numeric_values() {
    // §20.24 Format/GR 1 and §20.25 GR 1 accept a numeric length;
    // exact whole decimals select the same character/byte counts as integers.
    for (function, characters, bytes) in [("LEFT", "\"ä😀\"", "X'00C3'"), ("RIGHT", "\"\\u{301}Z\"", "X'A9FF'")] {
        for length in ["2", "2M", "2.00M", "CAST('2' AS DECIMAL(38, 37))"] {
            assert_eq!(
                value(&format!("RETURN {function}('ä😀e\u{301}Z', {length}) AS v")).await,
                characters
            );
            assert_eq!(value(&format!("RETURN {function}(X'00C3A9FF', {length}) AS v")).await, bytes);
        }
        for (source, empty, full) in [
            ("'ä😀e\u{301}Z'", "\"\"", "\"ä😀e\\u{301}Z\""),
            ("X'00C3A9FF'", "X''", "X'00C3A9FF'"),
        ] {
            assert_eq!(value(&format!("RETURN {function}({source}, 0E-38M) AS v")).await, empty);
            assert_eq!(value(&format!("RETURN {function}({source}, 1E37M) AS v")).await, full);
            assert_eq!(value(&format!("RETURN {function}({source}, 1E100) AS v")).await, full);
            assert_eq!(
                status(&format!("RETURN {function}({source}, -1M) AS v")).await,
                GqlStatus::SUBSTRING_ERROR
            );
            for fractional in ["2.1M", "1E-38M", "-0.1M"] {
                assert_eq!(
                    status(&format!("RETURN {function}({source}, {fractional}) AS v")).await,
                    GqlStatus::INVALID_VALUE_TYPE
                );
            }
            assert_eq!(
                status(&format!("RETURN {function}({source}, -2.0) AS v")).await,
                GqlStatus::SUBSTRING_ERROR
            );
            assert_eq!(
                status(&format!("RETURN {function}({source}, 2.5) AS v")).await,
                GqlStatus::INVALID_VALUE_TYPE
            );
            assert_eq!(value(&format!("RETURN {function}({source}, null) AS v")).await, "null");
            assert_eq!(
                value(&format!("RETURN COALESCE({source}, {function}({source}, -1M)) AS v")).await,
                full
            );
            assert_eq!(
                status(&format!("RETURN {function}({source}, CAST('1E38M' AS DECIMAL(38, 0))) AS v")).await,
                GqlStatus::NUMERIC_VALUE_OUT_OF_RANGE
            );
        }
        assert_eq!(value(&format!("RETURN {function}(null, 'bad') AS v")).await, "null");
        assert_eq!(
            value(&format!("RETURN {function}('abc', 2.0) AS v")).await,
            if function == "LEFT" { "\"ab\"" } else { "\"bc\"" }
        );
        assert_eq!(
            value(&format!("RETURN {function}(X'010203', 2.0) AS v")).await,
            if function == "LEFT" { "X'0102'" } else { "X'0203'" }
        );
    }
    assert_eq!(value("RETURN LEFT('abc', count(*)) AS v").await, "\"a\"");
    assert_eq!(value("RETURN RIGHT(X'010203', count(*)) AS v").await, "X'03'");
}

// ---- Review fixes, tenth round: projected keyword scope and the TCK's findings ----

#[tokio::test]
async fn a_dropped_variable_leaves_its_keyword_a_keyword() {
    // WITH projects only `v`, so after it UNKNOWN is the truth value.
    assert_eq!(
        value("MATCH (unknown:Node) WITH unknown.name AS v WHERE UNKNOWN IS NULL RETURN count(*) AS c").await,
        "5"
    );
    assert_eq!(
        value("MATCH (unknown:Node) WITH count(*) AS c ORDER BY UNKNOWN RETURN c").await,
        "5"
    );
    assert_eq!(value("MATCH (unknown:Node) RETURN count(*) AS c ORDER BY UNKNOWN").await, "5");
    // A key repeating an item, and an aggregate's argument, read the input.
    assert_eq!(
        table(&run("MATCH (unknown:Node) RETURN unknown.name AS v, count(*) AS c ORDER BY UNKNOWN.name LIMIT 1").await),
        [["\"a\"", "1"]]
    );
    assert_eq!(
        value("MATCH (unknown:Node) RETURN count(*) AS c ORDER BY count(UNKNOWN)").await,
        "5"
    );
}

#[tokio::test]
async fn max_and_min_over_values_of_different_types_take_the_sort_order() {
    // Deviation N6: values §19.3 cannot compare take the engine's order of
    // types (US007), as ORDER BY sorts them, rather than raising 22G04.
    assert_eq!(
        value("UNWIND [1, 'a', null, [1, 2], 0.2, 'b'] AS x RETURN max(x) AS m").await,
        "[1, 2]"
    );
    assert_eq!(
        value("UNWIND [1, 'a', null, [1, 2], 0.2, 'b'] AS x RETURN min(x) AS m").await,
        "0.2f"
    );
    assert_eq!(
        table(&run("UNWIND [1, 'a', 0.2] AS x WITH x ORDER BY x DESC LIMIT 1 RETURN x").await),
        [["\"a\""]]
    );
    // Numbers of any type compare by value, and nulls are left out.
    assert_eq!(
        table(&run("UNWIND [1, 2.0, 5, null] AS x RETURN max(x) AS m, min(x) AS n").await),
        [["5", "1"]]
    );
}

#[tokio::test]
async fn a_non_boolean_operand_names_its_operator() {
    assert!(common::compat::refusal("RETURN 1 AND true AS v")
        .await
        .contains("AND operands must be boolean"));
    assert!(common::compat::refusal("RETURN NOT 0 AS v")
        .await
        .contains("the NOT operand must be boolean"));
    assert!(common::compat::refusal("RETURN CASE WHEN 1 THEN 2 END AS v")
        .await
        .contains("a CASE condition must be boolean"));
}

// ---- Review fixes, eleventh round: an element test's variables are references ----

#[tokio::test]
async fn a_grouped_sort_key_tests_a_grouping_element() {
    // GROUP BY moves `n` into a hidden name; the element test follows it.
    assert_eq!(
        table(&run("MATCH (n) RETURN n, count(*) AS c GROUP BY n ORDER BY n IS LABELED Hub DESC LIMIT 1").await),
        [["#1010", "1"]]
    );
    assert_eq!(
        value("MATCH (a)-[e]->(b) RETURN count(*) AS c GROUP BY a, e ORDER BY a IS SOURCE OF e LIMIT 1").await,
        "1"
    );
}

// ---- Review fixes, eleventh round: what a selective path's subquery reads ----

#[tokio::test]
async fn a_selective_path_s_subquery_declares_its_own_names() {
    // A subquery's own aliases are declarations, not reads of the other
    // path's `h`.
    for query in [
        "MATCH REPEATABLE ELEMENTS ANY ((a)-[:R]->(b) WHERE VALUE { RETURN 1 AS h LIMIT 1 } = 1), (h:Hub) RETURN count(*) AS n",
        "MATCH REPEATABLE ELEMENTS ANY ((a)-[:R]->(b) WHERE EXISTS { RETURN 1 AS h }), (h:Hub) RETURN count(*) AS n",
        "MATCH REPEATABLE ELEMENTS ANY SHORTEST ((a)-[:R]->(b) WHERE EXISTS { MATCH (x) WITH x AS h RETURN h }), (h:Hub) RETURN count(*) AS n",
    ] {
        assert_eq!(value(query).await, "6", "{query}");
    }
}

// ---- Review fixes, eleventh round: a subquery's union operands are read ----

#[tokio::test]
async fn a_selective_path_s_subquery_reads_through_union_operands() {
    // `c` is only the other path's, read in an operand's WHERE.
    assert_eq!(
        status("MATCH REPEATABLE ELEMENTS ANY SHORTEST ((a)-[:R]->(b) WHERE EXISTS { MATCH ((x)-[:R]->(y) WHERE x = c) | (x)-[:HAS]->(y) }), (c:Hub) RETURN count(*) AS n").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    assert_eq!(
        value("MATCH REPEATABLE ELEMENTS ANY SHORTEST ((a)-[:R]->(b) WHERE EXISTS { MATCH ((x)-[:R]->(y) WHERE x = b) | (x)-[:HAS]->(y) }), (c:Hub) RETURN count(*) AS n").await,
        "6"
    );
}

// ---- Review fixes, eleventh round: comparisons of every kind of value ----

#[tokio::test]
async fn paths_compare_by_their_element_lists() {
    // §19.3 General Rule 2 b) iv): equal element lists are equal paths, and
    // `<` orders them by those lists.
    assert_eq!(
        table(
            &run(
                "MATCH p = (a:Node {name: 'a'})-[:R]->(b:Node {name: 'b'}), q = (a)-[:R]->(c:Node {name: 'c'}) \
                    RETURN p = p AS eq, p = q AS ne, p < q AS lt, p <= p AS le"
            )
            .await
        ),
        [["true", "false", "true", "true"]]
    );
}

#[tokio::test]
async fn records_order_by_their_fields() {
    // §19.3 General Rule 2 b) iii): `<=` is `<` or `=`, so equal records are
    // `<=`; `<` of records with the same fields is IV002, their values in name
    // order, as a sort orders them; other records cannot be compared (N1).
    assert_eq!(
        table(&run("RETURN {a: 1} <= {a: 1} AS le, {a: 1} < {a: 2} AS lt, [{a: 1}] < [{a: 2}] AS ll, {a: 1} < {b: 2} AS nc").await),
        [["true", "true", "true", "null"]]
    );
    assert_eq!(
        value("UNWIND [{a: 2}, {a: 1}] AS r WITH r WHERE r <= {a: 1} RETURN r.a AS v").await,
        "1"
    );
}

// ---- Review fixes, eleventh round: LET-bound names in a selective path ----

#[tokio::test]
async fn a_selective_path_s_let_reads_what_its_values_read() {
    // A LET's body sees its own names; its values read the enclosing scope.
    assert_eq!(
        status("MATCH REPEATABLE ELEMENTS ANY SHORTEST ((a)-[:R]->(b) WHERE LET x = h IN x END IS NOT NULL), (h:Hub) RETURN count(*) AS n")
            .await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    assert!(common::compat::refusal(
        "MATCH REPEATABLE ELEMENTS ANY SHORTEST ((a)-[:R]->(b) WHERE LET h = 1 IN h END = 1), (h:Hub) RETURN count(*) AS n"
    )
    .await
    .contains("LET cannot rebind h"));
}

// ---- Review fixes, eleventh round: logarithms of exact decimals ----

#[tokio::test]
async fn a_decimal_logarithm_uses_the_form_that_keeps_its_digits() {
    // Far from 1 the float is as precise as the result.
    assert_eq!(
        value("RETURN ln(0.00000000000000000001M) AS v").await,
        format!("{:?}f", 1e-20f64.ln())
    );
    assert_eq!(value("RETURN log(10, 0.00000000000000000001M) AS v").await, "-20.0f");
    // A decimal power of ten is exact through its digits and scale.
    assert_eq!(value("RETURN log10(0.01M) AS v").await, "-2.0f");
    assert_eq!(value("RETURN log10(0.0100M) AS v").await, "-2.0f");
    // Near 1 the offset from 1 is kept.
    assert_eq!(
        value("RETURN log10(1.00000000000000000001M) AS v").await,
        format!("{:?}f", 1e-20f64.ln_1p() / std::f64::consts::LN_10)
    );
}

// ---- Review fixes, eleventh round: an unnamed `.id` column is not its source ----

#[tokio::test]
async fn a_grouped_sort_key_reads_a_record_through_an_unnamed_id_column() {
    // The column `r.id` is not named `r`, so the key's `r` stays the record.
    assert_eq!(
        table(&run("UNWIND [2, 1] AS x WITH {id: x} AS r RETURN r.id, count(*) AS c ORDER BY r.id LIMIT 1").await),
        [["1", "1"]]
    );
}

// ---- Review fixes, twelfth round: MAX keeps the first of equal values ----

#[tokio::test]
async fn max_and_min_keep_the_first_of_equal_values() {
    assert_eq!(
        table(&run("UNWIND [1, 1.0] AS x RETURN max(x) AS m, min(x) AS n").await),
        [["1", "1"]]
    );
    assert_eq!(
        table(&run("UNWIND [1.0, 1] AS x RETURN max(x) AS m, min(x) AS n").await),
        [["1.0f", "1.0f"]]
    );
}

// ---- Review fixes, twelfth round: percentiles of tiny and wide inputs ----

#[tokio::test]
async fn percentile_cont_keeps_tiny_and_wide_inputs() {
    assert_eq!(
        value("UNWIND [5e-324, 5e-324] AS x RETURN percentile_cont(x, 0.5) AS v").await,
        format!("{}f", 5e-324)
    );
    assert_eq!(value("UNWIND [-1e308, 1e308] AS x RETURN percentile_cont(x, 0.5) AS v").await, "0f");
    assert_eq!(value("UNWIND [1, 2] AS x RETURN percentile_cont(x, 0.25) AS v").await, "1.25f");
}

// ---- Review fixes, twelfth round: unnamed columns stay behind NEXT ----

#[tokio::test]
async fn an_unnamed_column_is_not_seen_after_next() {
    // §14.11 Syntax Rule 8 b) gives `1` no name, so the next statement sees
    // only `v`, and the lowering's own name for it is no reference.
    assert_eq!(table(&run("RETURN 1, 2 AS v NEXT RETURN *").await), [["2"]]);
    assert_eq!(
        status("RETURN 1, 2 AS v NEXT RETURN __hydradb_next_column_0 AS x").await,
        GqlStatus::INVALID_REFERENCE
    );
    // The unnamed item still counts where it is: here it aggregates.
    assert_eq!(
        sorted_column("UNWIND [1, 1, 2] AS x RETURN x AS k, count(*) NEXT RETURN k").await,
        ["1", "2"]
    );
    assert_eq!(value("RETURN 1 NEXT RETURN 2 AS v").await, "2");
}

// ---- Review fixes, twelfth round: `*` keeps the enclosing names readable ----

#[tokio::test]
async fn a_star_projection_keeps_the_enclosing_names_read() {
    for query in [
        "MATCH REPEATABLE ELEMENTS ANY SHORTEST ((a)-[:R]->(b) WHERE EXISTS { WITH * WHERE h IS NOT NULL RETURN 1 AS x }), (h:Hub) RETURN count(*) AS n",
        "MATCH REPEATABLE ELEMENTS ANY SHORTEST ((a)-[:R]->(b) WHERE EXISTS { MATCH (x) WITH * RETURN h }), (h:Hub) RETURN count(*) AS n",
    ] {
        assert_eq!(status(query).await, GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION, "{query}");
    }
    assert_eq!(
        value("MATCH REPEATABLE ELEMENTS ANY SHORTEST ((a)-[:R]->(b) WHERE EXISTS { MATCH (x) WITH * RETURN x }), (h:Hub) RETURN count(*) AS n").await,
        "6"
    );
}

// ---- Review fixes, twelfth round: a quantified path keeps its inner WHERE ----

#[tokio::test]
async fn a_quantified_path_does_not_drop_a_nested_where() {
    // §22.3: a predicate retained under another pair of parentheses still
    // filters every iteration, not just the completed path.
    assert_eq!(value("MATCH (((a)-[:R]->(b) WHERE false)){1,2} RETURN count(*) AS c").await, "0");
}

#[tokio::test]
async fn a_quantified_path_reads_values_from_earlier_patterns() {
    for earlier in ["p = (x)-[:R]->(y)", "(p = (x)-[:R]->(y))"] {
        for predicate in ["path_length(p) = 1", "EXISTS { FILTER path_length(p) = 1 RETURN 1 AS v }"] {
            let query = format!(
                "MATCH REPEATABLE ELEMENTS {earlier}, \
                 ((a)-[:R]->(b) WHERE {predicate}){{1}} RETURN count(*) AS c"
            );
            assert_eq!(value(&query).await, "36", "{query}");
        }
    }
    assert_eq!(
        value("MATCH REPEATABLE ELEMENTS ((x)-[:R]->(y)){1}, ((a)-[:R]->(b) WHERE size(x) = 1){1} RETURN count(*) AS c").await,
        "36"
    );
    assert_eq!(
        value("MATCH REPEATABLE ELEMENTS p = (x)-[:R]->(y), ((a)-[:R]->(b) WHERE path_length(p) = 2){1} RETURN count(*) AS c").await,
        "0"
    );
}

#[tokio::test]
async fn a_quantified_path_still_refuses_values_from_later_patterns() {
    for later in ["p = (x)-[:R]->(y)", "(p = (x)-[:R]->(y))"] {
        let query = format!(
            "MATCH REPEATABLE ELEMENTS ((a)-[:R]->(b) WHERE path_length(p) = 1){{1}}, \
             {later} RETURN count(*) AS c"
        );
        assert_eq!(status(&query).await, GqlStatus::FEATURE_NOT_SUPPORTED, "{query}");
    }
}

// ---- Review fixes, twelfth round: a union inside a quantified path ----

#[tokio::test]
async fn a_quantified_path_does_not_drop_a_union() {
    assert_eq!(
        status("MATCH (((a)-[:R]->(b)) | ((a)-[:HAS]->(b))){1,2} RETURN count(*) AS c").await,
        GqlStatus::FEATURE_NOT_SUPPORTED
    );
}

// ---- Review fixes, twelfth round: a NaN inside a record stays distinct ----

#[tokio::test]
async fn a_record_holding_nan_stays_distinct() {
    let nan = ParameterMap::from([("nan".to_string(), ScalarValue::Float("NaN".into()))]);
    let result = common::compat::run_with(
        "UNWIND [1, 2] AS i RETURN count(DISTINCT {x: $nan}) AS v, count(DISTINCT [$nan]) AS w",
        &nan,
        &ListParameterMap::new(),
    )
    .await;
    assert_eq!(table(&result), [["2", "2"]]);
}

// ---- Review fixes, twelfth round: exact fractions, and out of range vs invalid text ----

#[tokio::test]
async fn a_percentile_fraction_is_checked_exactly() {
    for fraction in ["1.00000000000000000001M", "-0.00000000000000000001M"] {
        assert_eq!(
            status(&format!("UNWIND [1, 2] AS d RETURN percentile_cont(d, {fraction}) AS v")).await,
            GqlStatus::DATA_EXCEPTION,
            "{fraction}"
        );
    }
    assert_eq!(value("UNWIND [1, 2] AS d RETURN percentile_cont(d, 1M) AS v").await, "2f");
}

#[tokio::test]
async fn a_cast_tells_an_out_of_range_number_from_invalid_text() {
    let big = "123456789012345678901234567890123456789012";
    for query in [
        "RETURN CAST('1E40M' AS DECIMAL(38, 0)) AS v".to_string(),
        format!("RETURN CAST('{big}' AS DECIMAL(38, 0)) AS v"),
        format!("RETURN CAST('{big}' AS INT) AS v"),
        "RETURN CAST('0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF' AS INT) AS v".to_string(),
    ] {
        assert_eq!(status(&query).await, GqlStatus::NUMERIC_VALUE_OUT_OF_RANGE, "{query}");
    }
    assert_eq!(
        value(&format!("RETURN CAST('{big}' AS FLOAT) AS v")).await,
        "1.2345678901234568e41f"
    );
    assert_eq!(
        status("RETURN CAST('12x' AS INT) AS v").await,
        GqlStatus::INVALID_CHARACTER_VALUE_FOR_CAST
    );
}

// ---- Review fixes, twelfth round: one name for an unnamed `.id` ----

#[tokio::test]
async fn an_unnamed_id_is_named_as_its_column_everywhere() {
    // The capability check and execution name it alike, `n.id`, so a later
    // arm's unnamed `n.id` matches the first arm's and the plan runs.
    assert_eq!(
        value("RETURN EXISTS { MATCH (n:Node) RETURN n.id AS `n.id` UNION ALL MATCH (n:Node) RETURN n.id } AS ok").await,
        "true"
    );
    assert_eq!(
        table(&run("UNWIND [2, 1] AS x WITH {id: x} AS r RETURN r.id, count(*) AS c ORDER BY r.id LIMIT 1").await),
        [["1", "1"]]
    );
}

// ---- Review fixes, thirteenth round: each position of a subquery's scope ----

#[tokio::test]
async fn a_selective_path_s_subquery_reads_each_position_in_its_scope() {
    let query = |body: &str| {
        format!("MATCH REPEATABLE ELEMENTS ANY SHORTEST ((a)-[:R]->(b) WHERE EXISTS {{ {body} }}), (h:Hub) RETURN count(*) AS n")
    };
    // The subquery's own `h`: a WITH's WHERE and sort keys read what it
    // projects, a RETURN's sort keys its items' names first.
    for body in [
        "WITH 1 AS h WHERE h = 1 RETURN h",
        "WITH 1 AS h ORDER BY h RETURN h",
        "WITH 1 AS h RETURN h ORDER BY h",
        "RETURN 1 AS h ORDER BY h",
    ] {
        assert_eq!(value(&query(body)).await, "6", "{body}");
    }
    // The other path's `h`, wherever the subquery reads it.
    for body in [
        "MATCH (h) RETURN h",
        "WITH h AS x RETURN x",
        "WITH * WHERE h IS NOT NULL RETURN 1 AS x",
        "MATCH (x) RETURN x ORDER BY h",
        "MATCH (x) WHERE x = h RETURN x",
        "MATCH (h)-[:HAS]->() RETURN 1 AS x",
        "FILTER h IS NOT NULL RETURN 1 AS x",
    ] {
        assert_eq!(
            status(&query(body)).await,
            GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "{body}"
        );
    }
}

// ---- Review fixes, thirteenth round: graph types, and durations at their least ----

#[tokio::test]
async fn a_graph_reference_type_is_not_a_value_type() {
    for query in ["RETURN 1 IS :: PROPERTY GRAPH AS v", "RETURN CAST(1 AS PROPERTY GRAPH) AS v"] {
        assert_eq!(status(query).await, GqlStatus::FEATURE_NOT_SUPPORTED, "{query}");
    }
    assert_eq!(value("RETURN 1 IS :: PROPERTY VALUE AS v").await, "true");
}

#[tokio::test]
async fn the_least_duration_subtracts_and_is_written() {
    assert_eq!(
        value("WITH DURATION({months: -9223372036854775808}) AS d RETURN d - d AS v").await,
        "DURATION 'P0M'"
    );
    assert_eq!(
        value("RETURN DURATION('-P9223372036854775808M') = DURATION({months: -9223372036854775808}) AS v").await,
        "true"
    );
    assert_eq!(
        status("WITH DURATION({months: -9223372036854775808}) AS d RETURN -d AS v").await,
        GqlStatus::INTERVAL_FIELD_OVERFLOW
    );
}

// ---- Review fixes, thirteenth round: a large integer's text rounds once ----

#[tokio::test]
async fn a_large_integer_text_casts_to_its_nearest_float() {
    let ten_39 = format!("1{}", "0".repeat(39));
    assert_eq!(
        value(&format!("RETURN CAST('{ten_39}' AS FLOAT) AS v")).await,
        format!("{:?}f", 1e39)
    );
    assert_eq!(
        value("RETURN CAST('0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF' AS FLOAT) AS v").await,
        format!("{:?}f", 8.711228593176025e40)
    );
    // 55 bits past a half: rounds up, once.
    let bits = format!("0b1{}11", "0".repeat(52));
    assert_eq!(
        value(&format!("RETURN CAST('{bits}' AS FLOAT) AS v")).await,
        format!("{:?}f", 18014398509481988f64)
    );
}

// ---- Review fixes, fourteenth round: a braced first operand, and long trims ----

#[tokio::test]
async fn a_braced_first_union_operand_keeps_its_meaning() {
    let sorted = |rows: Vec<Vec<String>>| {
        let mut column: Vec<String> = rows.into_iter().map(|row| row[0].clone()).collect();
        column.sort();
        column
    };
    // One conjunction throughout associates, so the arms may join.
    assert_eq!(
        sorted(table(
            &run("{ RETURN 1 AS x UNION ALL RETURN 1 AS x } UNION ALL RETURN 2 AS x").await
        )),
        ["1", "1", "2"]
    );
    assert_eq!(
        sorted(table(&run("{ RETURN 1 AS x UNION RETURN 1 AS x } UNION RETURN 2 AS x").await)),
        ["1", "2"]
    );
    // Another conjunction would change the answer if they joined: refused.
    assert_eq!(
        status("{ RETURN 1 AS x UNION RETURN 1 AS x } UNION ALL RETURN 2 AS x").await,
        GqlStatus::FEATURE_NOT_SUPPORTED
    );
}

#[tokio::test]
async fn a_long_trim_set_is_looked_up_not_scanned() {
    let source = "z".repeat(100_000);
    let set = format!("{}z", "a".repeat(100_000));
    assert_eq!(value(&format!("RETURN size(btrim('{source}', '{set}')) AS v")).await, "0");
    assert_eq!(value("RETURN btrim('aaazzzb', 'za') AS v").await, "\"b\"");
}

// ---- Review fixes, fourteenth round: a braced first operand's conjunction ----

#[tokio::test]
async fn a_braced_first_operand_joins_only_its_own_conjunction() {
    // A chain of one conjunction associates: `{ 1 EXCEPT 2 } EXCEPT 1` is empty.
    assert!(table(&run("{ RETURN 1 AS x EXCEPT RETURN 2 AS x } EXCEPT RETURN 1 AS x").await).is_empty());
    // UNION inside, EXCEPT outside: joining the arms would change the answer.
    assert_eq!(
        status("{ RETURN 1 AS x UNION RETURN 2 AS x } EXCEPT RETURN 1 AS x").await,
        GqlStatus::FEATURE_NOT_SUPPORTED
    );
}

#[tokio::test]
async fn a_name_is_a_node_or_an_edge_across_a_match() {
    for query in [
        "MATCH (x), ()-[x]->() RETURN count(*) AS c",
        "MATCH (x)-[x]->(y) RETURN count(*) AS c",
    ] {
        assert_eq!(status(query).await, GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION, "{query}");
    }
    assert_eq!(value("MATCH (a)-[r]->(b), (b)-[r2]->(c) RETURN count(*) AS c").await, "6");
}

// ---- Review fixes, fourteenth round: one name, one kind, across union operands ----

#[tokio::test]
async fn a_union_operand_s_node_is_not_another_s_edge() {
    assert_eq!(
        status("MATCH (x)-[:R]->(b) | (a)-[x:R]->(b) RETURN x.id AS v").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
    assert_eq!(value("MATCH (x)-[:R]->(b) | (x)-[:HAS]->(b) RETURN count(*) AS c").await, "9");
}

// ---- Review fixes, fourteenth round: a duration's sign never overflows unchecked ----

#[tokio::test]
async fn a_duration_literal_summing_to_the_least_value_is_refused_not_wrapped() {
    assert_eq!(
        status("RETURN DURATION('-PT-47261439850130342147690917H-2515.884105728S') AS v").await,
        GqlStatus::DATETIME_FIELD_OVERFLOW
    );
}

// ---- Review fixes, fourteenth round: a decimal total held only at the end ----

#[tokio::test]
async fn a_decimal_sum_does_not_depend_on_row_order() {
    // The running total is wider than a decimal (UV007); only the result is
    // held to 38 digits.
    assert_eq!(value("UNWIND [9E37M, 9E37M, -9E37M, -9E37M] AS d RETURN sum(d) AS v").await, "0M");
    assert_eq!(value("UNWIND [9E37M, -9E37M, 9E37M, -9E37M] AS d RETURN sum(d) AS v").await, "0M");
    assert_eq!(
        status("UNWIND [9E37M, 9E37M] AS d RETURN sum(d) AS v").await,
        GqlStatus::NUMERIC_VALUE_OUT_OF_RANGE
    );
    assert_eq!(table(&run("UNWIND [1, 2.5M, 3] AS d RETURN sum(d) AS v").await), [["6.5M"]]);
}

#[tokio::test]
async fn zero_is_zero_at_any_exponent() {
    assert_eq!(
        table(&run("RETURN CAST('0E40' AS DECIMAL(1, 0)) AS v, 0E40M AS w").await),
        [["0M", "0M"]]
    );
}

// ---- Review fixes, fourteenth round: a sort reads the input only when it must ----

#[tokio::test]
async fn a_sort_key_reads_the_input_only_when_the_projection_dropped_its_name() {
    // A nested RETURN's key reads `n`, which it does not return: the merged
    // row. Keys on projected names read the projected row alone.
    let top = value("MATCH (n:Item) RETURN n.name AS v ORDER BY n.score DESC LIMIT 1").await;
    assert_eq!(
        value("CALL { MATCH (n:Item) RETURN n.name AS v ORDER BY n.score DESC LIMIT 1 } RETURN v").await,
        top
    );
    assert_eq!(
        value("MATCH (n:Item) WITH n.name AS v, n.score AS s ORDER BY s DESC LIMIT 1 RETURN v").await,
        top
    );
    assert_eq!(
        value("MATCH (n:Item) WITH n.kind AS k, count(*) AS c ORDER BY c DESC, k LIMIT 1 RETURN k").await,
        "\"fruit\""
    );
}

// ---- Review fixes, fifteenth round: an average from the wide total ----

#[tokio::test]
async fn a_decimal_average_divides_the_wide_total() {
    assert_eq!(
        value("UNWIND [9E37M, 9E37M] AS d RETURN avg(d) AS v").await,
        "90000000000000000000000000000000000000M"
    );
    // The usual scale where it fits.
    assert_eq!(value("UNWIND [1M, 2M, 2M] AS d RETURN avg(d) AS v").await, "1.666667M");
    assert_eq!(
        status("UNWIND [9E37M, 9E37M] AS d RETURN sum(d) AS v").await,
        GqlStatus::NUMERIC_VALUE_OUT_OF_RANGE
    );
}

// ---- Review fixes, fifteenth round: an instant minus a duration ----

#[tokio::test]
async fn a_time_minus_the_least_year_month_duration_is_the_time() {
    // A time of day ignores a year and month-based duration, so nothing is
    // negated for it.
    assert_eq!(
        value("RETURN TIME '12:00' - DURATION({months: -9223372036854775808}) AS v").await,
        "TIME '12:00:00.000000000'"
    );
    assert_eq!(value("RETURN DATE '2020-03-31' - DURATION('P1M') AS v").await, "DATE '2020-02-29'");
    assert_eq!(
        status("RETURN DATE '2020-01-01' - DURATION({months: -9223372036854775808}) AS v").await,
        GqlStatus::DATETIME_FIELD_OVERFLOW
    );
}

// ---- Review fixes, sixteenth round: an average rounds once ----

#[tokio::test]
async fn a_decimal_average_rounds_once() {
    // Only scale 0 holds these; the exact total is divided and its quotient
    // rounded once (expected values from exact decimal arithmetic).
    assert_eq!(
        value("UNWIND [9E37M, 0.99M] AS d RETURN avg(d) AS v").await,
        "45000000000000000000000000000000000000M"
    );
    assert_eq!(
        value("UNWIND [9E37M, 1M] AS d RETURN avg(d) AS v").await,
        "45000000000000000000000000000000000001M"
    );
    assert_eq!(
        value("UNWIND [-9E37M, -1M] AS d RETURN avg(d) AS v").await,
        "-45000000000000000000000000000000000001M"
    );
    assert_eq!(value("UNWIND [0.1M, 0.2M, 0.3M] AS d RETURN avg(d) AS v").await, "0.2000000M");
}

// ---- Review fixes, seventeenth round: a WITH's window chooses its rows first ----

#[tokio::test]
async fn a_with_window_chooses_its_rows_before_the_rest_is_computed() {
    // Without grouping or DISTINCT a WITH maps rows one to one, so its ORDER
    // BY, SKIP and LIMIT choose the rows first, as a result statement's do,
    // and a row they drop is never divided by zero. NEXT passes its table on
    // through such a WITH.
    for query in [
        "UNWIND [0, 1] AS x RETURN 1 / x AS v ORDER BY x DESC LIMIT 1",
        "UNWIND [0, 1] AS x WITH x, 1 / x AS v ORDER BY x DESC LIMIT 1 RETURN v",
        "UNWIND [0, 1] AS x WITH 1 / x AS v SKIP 1 RETURN v",
        "UNWIND [0, 1] AS x RETURN 1 / x AS v ORDER BY x DESC LIMIT 1 NEXT RETURN v",
    ] {
        assert_eq!(value(query).await, "1", "{query}");
    }
    // A key that reads an item reads it for every row, and DISTINCT sees
    // every whole row.
    for query in [
        "UNWIND [0, 1] AS x WITH x, 1 / x AS v ORDER BY v LIMIT 1 RETURN v",
        "UNWIND [0, 1] AS x WITH DISTINCT x, 1 / x AS v ORDER BY x DESC LIMIT 1 RETURN v",
    ] {
        assert_eq!(status(query).await, GqlStatus::DIVISION_BY_ZERO, "{query}");
    }
    assert_eq!(
        table(&run("UNWIND [3, 1, 2] AS x WITH x, x * 10 AS t ORDER BY x DESC SKIP 1 RETURN x, t").await),
        [["2", "20"], ["1", "10"]]
    );
    assert_eq!(
        table(&run("UNWIND [3, 1, 2] AS x WITH x * 2 AS v ORDER BY v + 1 LIMIT 2 RETURN v").await),
        [["2"], ["4"]]
    );
}

// ---- Review fixes, seventeenth round: the least duration is reached by scaling ----

#[tokio::test]
async fn a_duration_scales_to_the_least_value_and_not_past_it() {
    // -2^127 nanoseconds is the least duration; its magnitude has no
    // positive twin, so the sign is applied before it is held.
    let least = "DURATION('-PT85070591730234615865843651857.942052864S')";
    assert_eq!(value(&format!("WITH {least} AS d RETURN d / 0.5 = d * 2 AS v")).await, "true");
    assert_eq!(value(&format!("WITH {least} AS d RETURN d * 2.0 = d * 2 AS v")).await, "true");
    assert_eq!(
        status("WITH DURATION('PT85070591730234615865843651857.942052864S') AS d RETURN d / 0.5 AS v").await,
        GqlStatus::INTERVAL_FIELD_OVERFLOW
    );
    assert_eq!(
        value("RETURN CAST('-9223372036854775808.0' AS INT) AS v").await,
        "-9223372036854775808"
    );
}

// ---- Review fixes, seventeenth round: an endpoint test's edge is a name it reads ----

#[tokio::test]
async fn a_sort_key_testing_an_endpoint_reads_its_edge() {
    // `x IS SOURCE OF e` reads `e` as well as `x`, so a window-first WITH
    // computes `e` before sorting, and the false rows come first.
    assert_eq!(
        value("MATCH (a)-[e]->(b) UNWIND [a, b] AS x WITH x, e ORDER BY x IS SOURCE OF e LIMIT 1 RETURN x IS SOURCE OF e AS s").await,
        "false"
    );
    assert_eq!(
        value("MATCH (a)-[e]->(b) UNWIND [b, a] AS x WITH x, e ORDER BY x IS DESTINATION OF e LIMIT 1 RETURN x IS DESTINATION OF e AS d")
            .await,
        "false"
    );
}

// ---- Review fixes, seventeenth round: a cast to a float rounds once ----

#[tokio::test]
async fn a_cast_to_a_narrow_float_rounds_the_exact_value_once() {
    // 2^54 + 2^30 + 1 is just past a binary32 midpoint; through binary64 it
    // would land on the midpoint and round to even, down.
    for operand in ["18014399583223809", "'18014399583223809'"] {
        assert_eq!(
            value(&format!("RETURN CAST({operand} AS FLOAT32) = 18014400656965632.0 AS v")).await,
            "true",
            "{operand}"
        );
    }
    // A decimal just past a midpoint rounds up; one at it, to even.
    assert_eq!(
        value("RETURN CAST('1.00000005960464477539062500001' AS FLOAT32) AS v").await,
        "1.0000001192092896f"
    );
    assert_eq!(value("RETURN CAST('1.000000059604644775390625' AS FLOAT32) AS v").await, "1.0f");
    for (literal, expected) in [
        ("1.00048828125000000001", "1.0009765625f"),
        ("1.00048828125", "1.0f"),
        ("1.00048828124999999999", "1.0f"),
        ("-1.00048828125000000001", "-1.0009765625f"),
        ("65519.99999999999999", "65504.0f"),
    ] {
        assert_eq!(
            value(&format!("RETURN CAST('{literal}' AS FLOAT16) AS v")).await,
            expected,
            "{literal}"
        );
    }
    assert_eq!(
        status("RETURN CAST('65520' AS FLOAT16) AS v").await,
        GqlStatus::NUMERIC_VALUE_OUT_OF_RANGE
    );
    // The least INT128 is a literal's value, whatever the target holds.
    assert_eq!(
        value("RETURN CAST('-170141183460469231731687303715884105728' AS FLOAT32) AS v").await,
        "-1.7014118346046923e38f"
    );
}

// ---- Review fixes, seventeenth round: endpoint sort keys, and definitions past NEXT ----

#[tokio::test]
async fn a_grouped_sort_key_testing_an_endpoint_reads_the_edge_from_the_input() {
    // The key repeats a returned item and reads `x`, which is returned, and
    // `e`, which is not: the input row, so a false group comes first.
    let rows = table(
        &run("MATCH (a)-[e]->(b) UNWIND [b, a] AS x RETURN x, x IS DESTINATION OF e AS d, count(*) AS c ORDER BY x IS DESTINATION OF e LIMIT 1").await,
    );
    assert_eq!(rows[0][1], "false");
}

#[tokio::test]
async fn a_value_definition_means_the_same_after_next() {
    for query in [
        "VALUE saved = UNKNOWN WITH 2 AS unknown RETURN unknown NEXT RETURN unknown, saved",
        "VALUE saved = UNKNOWN WITH 2 AS unknown RETURN unknown NEXT RETURN unknown NEXT RETURN unknown, saved",
    ] {
        assert_eq!(table(&run(query).await), [["2", "null"]], "{query}");
    }
    assert_eq!(
        table(&run("VALUE saved = UNKNOWN WITH 2 AS unknown RETURN unknown NEXT FINISH NEXT RETURN saved AS v, UNKNOWN AS u").await),
        [["null", "null"]]
    );
    assert_eq!(value("VALUE unknown = 7 RETURN 1 AS x NEXT RETURN unknown + x AS v").await, "8");
    assert_eq!(
        table(&run("VALUE saved = UNKNOWN WITH 2 AS unknown RETURN unknown NEXT RETURN *").await),
        [["2"]]
    );
    // Replayed in a call that imports nothing, the definition's subquery
    // matches its own `n`, not the column the statement before returned.
    for returned in ["n", "1 AS n"] {
        let query =
            format!("VALUE total = VALUE {{ MATCH (n:Node) RETURN count(*) AS c }} MATCH (n:Hub) RETURN {returned} NEXT RETURN total");
        assert_eq!(value(&query).await, "5", "{query}");
    }
    assert_eq!(
        value("VALUE t = EXISTS { MATCH (n:Node) } MATCH (n:Hub) RETURN n NEXT RETURN t").await,
        "true"
    );
    // It crosses an aggregation over no rows, and every NEXT.
    assert_eq!(
        table(&run("VALUE t = 1 MATCH (n:Missing) RETURN count(*) AS c NEXT RETURN c, t").await),
        [["0", "1"]]
    );
    assert_eq!(
        table(&run("VALUE t = 1 RETURN 1 AS x NEXT RETURN x NEXT RETURN t, x").await),
        [["1", "1"]]
    );
    assert_eq!(
        status("VALUE total = 1 RETURN 2 AS total NEXT RETURN total").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
}

// ---- Review fixes, eighteenth round: a null operand first, and EXISTS over UNION ----

#[tokio::test]
async fn a_null_operand_answers_null_before_the_other_operand_s_type() {
    for query in [
        "RETURN 'x' + null AS v",
        "RETURN null - 'x' AS v",
        "MATCH (n:Node) RETURN n.name * n.missing AS v LIMIT 1",
        "RETURN 1 || null AS v",
        "RETURN left(1, null) AS v",
        "RETURN power('x', null) AS v",
        "RETURN trim(1 FROM null) AS v",
        "RETURN btrim(1, null) AS v",
    ] {
        assert_eq!(value(query).await, "null", "{query}");
    }
    assert_eq!(status("RETURN 'x' + 1 AS v").await, GqlStatus::INVALID_VALUE_TYPE);
}

#[tokio::test]
async fn exists_over_a_union_names_every_arm_alike() {
    assert_eq!(value("RETURN EXISTS { RETURN 1 UNION RETURN 1 } AS v").await, "true");
    assert_eq!(
        value("RETURN EXISTS { MATCH (n:Missing) RETURN 1 UNION MATCH (n:Missing) RETURN 1 } AS v").await,
        "false"
    );
    // Arms that would not agree as results are not made to.
    assert_eq!(
        status("RETURN EXISTS { RETURN 1 UNION RETURN 2 } AS v").await,
        GqlStatus::FEATURE_NOT_SUPPORTED
    );
}

// ---- Review fixes, eighteenth round: a RETURN before NEXT keeps its sort keys and star ----

#[tokio::test]
async fn a_return_before_next_sorts_as_it_would_alone() {
    // After DISTINCT or grouping a key may repeat an item's expression; the
    // WITH reads the item's column.
    assert_eq!(
        table(&run("UNWIND [2, 1] AS x RETURN DISTINCT x AS k ORDER BY x NEXT RETURN k").await),
        [["1"], ["2"]]
    );
    assert_eq!(
        table(&run("UNWIND [2, 1] AS x RETURN DISTINCT x + 1 AS k ORDER BY x + 1 DESC NEXT RETURN k").await),
        [["3"], ["2"]]
    );
    assert_eq!(
        table(&run("UNWIND [1, 2, 2] AS x RETURN x AS k, count(*) AS c ORDER BY count(*) DESC NEXT RETURN k, c").await),
        [["2", "2"], ["1", "1"]]
    );
}

#[tokio::test]
async fn a_star_s_unnamed_extra_stays_behind_next() {
    assert_eq!(value("UNWIND [1] AS x RETURN *, 2 NEXT RETURN x").await, "1");
    assert_eq!(table(&run("UNWIND [1] AS x RETURN *, 2 NEXT RETURN *").await), [["1"]]);
    assert_eq!(
        table(&run("UNWIND [1, 1, 2] AS x RETURN *, count(*) NEXT RETURN x").await),
        [["1"], ["2"]]
    );
    assert_eq!(
        status("UNWIND [1] AS x RETURN *, 2 NEXT RETURN __hydradb_next_column_0 AS y").await,
        GqlStatus::INVALID_REFERENCE
    );
}

// ---- Review fixes, eighteenth round: keyword-named variables, and a trail's hops ----

#[tokio::test]
async fn a_selective_path_s_keyword_named_read_is_a_variable_read() {
    let query = |body: &str, sibling: &str| {
        format!("MATCH REPEATABLE ELEMENTS ANY ((a)-[:R]->(b) WHERE EXISTS {{ {body} }}), ({sibling}:Hub) RETURN count(*) AS n")
    };
    // `unknown` and `session_user` name the other path's variable here,
    // which a selective path may not read.
    for (body, sibling) in [
        ("MATCH (z) WHERE z = unknown", "unknown"),
        ("MATCH (z) WHERE z = UNKNOWN", "unknown"),
        ("MATCH (z) WHERE z = session_user", "session_user"),
    ] {
        assert_eq!(
            status(&query(body, sibling)).await,
            GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "{body}"
        );
    }
    // A name of the subquery's own shadows it, as any variable does.
    assert_eq!(
        value(&query("WITH 1 AS unknown WHERE unknown = 1 RETURN unknown", "unknown")).await,
        "6"
    );
}

#[tokio::test]
async fn a_trail_does_not_traverse_one_edge_twice() {
    // One variable for two hops is one edge both times: no trail, in either
    // match mode; without TRAIL the edge may repeat.
    for query in [
        "MATCH REPEATABLE ELEMENTS TRAIL (a)-[e:R]->(b)<-[e:R]-(a) RETURN count(*) AS n",
        "MATCH TRAIL (a)-[e:R]->(b)<-[e:R]-(a) RETURN count(*) AS n",
    ] {
        assert_eq!(value(query).await, "0", "{query}");
    }
    assert_eq!(
        value("MATCH REPEATABLE ELEMENTS (a)-[e:R]->(b)<-[e:R]-(a) RETURN count(*) AS n").await,
        "6"
    );
}

// ---- Review fixes, eighteenth round: an operand's subquery reads are checked ----

#[tokio::test]
async fn a_union_operand_s_subquery_does_not_read_a_later_pattern() {
    // `c` is bound by a later path pattern, after the union runs: refused,
    // through a subquery as directly.
    for query in [
        "MATCH ((a)-[:R]->(b) WHERE EXISTS { MATCH (z) WHERE z = c }) | (a)-[:HAS]->(b), (c:Hub) RETURN count(*) AS n",
        "MATCH ((a)-[:R]->(b) WHERE EXISTS { MATCH (c) RETURN c }) | (a)-[:HAS]->(b), (c:Hub) RETURN count(*) AS n",
        "MATCH ((a)-[:R]->(b) WHERE a.score = c.score) | (a)-[:HAS]->(b), (c:Hub) RETURN count(*) AS n",
    ] {
        assert_eq!(status(query).await, GqlStatus::FEATURE_NOT_SUPPORTED, "{query}");
    }
    // Bound before the MATCH, or the subquery's own, it is read.
    for query in [
        "MATCH (c:Hub) MATCH ((a)-[:R]->(b) WHERE EXISTS { MATCH (z) WHERE z = c }) | (a)-[:HAS]->(b) RETURN count(*) AS n",
        "MATCH ((a)-[:R]->(b) WHERE EXISTS { WITH 1 AS c RETURN c }) | (a)-[:HAS]->(b), (c:Hub) RETURN count(*) AS n",
    ] {
        assert_eq!(value(query).await, "9", "{query}");
    }
}

// ---- Review fixes, eighteenth round: a selective path's predicate reads its path and groups ----

#[tokio::test]
async fn a_selective_path_s_predicate_reads_its_path_and_group_values() {
    // The path variable and the group variables are bound before the
    // candidates are filtered, so each predicate keeps the matches it holds of.
    for (query, expected) in [
        (
            "MATCH ANY SHORTEST (q = (a:Node {name: 'a'})-[:R]->{1,2}(b) WHERE path_length(q) = 1) RETURN count(*) AS c",
            "2",
        ),
        (
            "MATCH ANY SHORTEST (q = (a:Node {name: 'a'})-[r:R]->{1,2}(b) WHERE size(r) = 1) RETURN count(*) AS c",
            "2",
        ),
        (
            "MATCH ANY SHORTEST ((a:Node {name: 'a'})-[r:R]->{1,2}(b) WHERE size(r) = 2) RETURN count(*) AS c",
            "1",
        ),
        (
            "MATCH ANY SHORTEST (((x)-[:R]->(y)){1,2} WHERE size(x) = 1) RETURN count(*) AS c",
            "6",
        ),
    ] {
        assert_eq!(value(query).await, expected, "{query}");
    }
}

// ---- Review fixes, eighteenth round: an approximate literal's text is a binary64 ----

#[tokio::test]
async fn approximate_literal_text_casts_from_its_binary64_value() {
    // `F`, `D`, or an exponent without `M`: the value is the binary64 nearest
    // the text, so a narrower cast rounds that, as through FLOAT64 first.
    for literal in ["1.00000005960464477539062500001F", "1.00000005960464477539062500001e0"] {
        assert_eq!(
            value(&format!(
                "RETURN CAST('{literal}' AS FLOAT32) = CAST(CAST('{literal}' AS FLOAT64) AS FLOAT32) AS v"
            ))
            .await,
            "true",
            "{literal}"
        );
        assert_eq!(
            value(&format!("RETURN CAST('{literal}' AS FLOAT32) AS v")).await,
            "1.0f",
            "{literal}"
        );
    }
    assert_eq!(
        value("RETURN CAST('1.00000005960464477539062500001' AS FLOAT32) AS v").await,
        "1.0000001192092896f"
    );
    assert_eq!(value("RETURN CAST('9007199254740993E0' AS INT) AS v").await, "9007199254740992");
    assert_eq!(value("RETURN CAST('9007199254740993E0M' AS INT) AS v").await, "9007199254740993");
}

// ---- Review fixes, eighteenth round: decimal seek keys, approximate text, decimal powers ----

#[tokio::test]
async fn a_large_integral_decimal_seeks_the_float_that_holds_it() {
    // 1E20M is past every stored integer, but the float 1e20 holds it
    // exactly: the seek keys it as that float rather than dropping it.
    let engine = CypherEngine::new(common::MemoryStorage::with_vertices([
        hydradb_cypher_engine::VertexRecord::new(1)
            .with_label("Item")
            .with_property("score", ScalarValue::Float(format!("{:?}", 1e20_f64).into())),
        hydradb_cypher_engine::VertexRecord::new(2)
            .with_label("Item")
            .with_property("score", ScalarValue::Integer(1)),
    ]));
    let prepared = engine
        .prepare_with_lists(
            "MATCH (n:Item) WHERE n.score IN [1M, 1E20M] RETURN count(*) AS c",
            &ParameterMap::new(),
            &ListParameterMap::new(),
        )
        .unwrap();
    assert_eq!(table(&engine.execute_prepared(&prepared).await.unwrap()), [["2"]]);
}

#[tokio::test]
async fn scientific_text_is_approximate_when_cast_to_a_decimal() {
    // `0.1E0` is an approximate literal, as `0.1D` is: its binary64 value.
    for literal in ["0.1E0", "0.1D"] {
        assert_eq!(
            value(&format!("RETURN CAST('{literal}' AS DECIMAL(21, 20)) AS v")).await,
            "0.10000000000000000555M",
            "{literal}"
        );
    }
    assert_eq!(value("RETURN CAST('0.1' AS DECIMAL(21, 20)) AS v").await, "0.10000000000000000000M");
    assert_eq!(value("RETURN CAST('1E2M' AS DECIMAL(5, 0)) AS v").await, "100M");
}

#[tokio::test]
async fn a_decimal_power_keeps_its_base_s_distance_from_one() {
    assert_eq!(
        value("RETURN power(1.00000000000000000001M, 1E20M) AS v").await,
        format!("{:?}f", std::f64::consts::E)
    );
    assert_eq!(
        value("RETURN power(0.99999999999999999999M, 1E20M) AS v").await,
        format!("{:?}f", (-1.0_f64).exp())
    );
    // A base a float holds exactly is raised as that float.
    assert_eq!(value("RETURN power(1.25M, 2M) AS v").await, "1.5625f");
    assert_eq!(value("RETURN power(2M, 3M) AS v").await, "8.0f");
}

// ---- Review fixes, nineteenth round: only NEXT's own names leave the star ----

#[tokio::test]
async fn a_user_column_spelled_like_a_lowering_name_stays_a_column() {
    // NEXT's names for unnamed columns are tracked, not recognized by
    // their spelling: a user's column of that spelling is a column.
    assert_eq!(
        table(&run("WITH 1 AS __hydradb_next_column_0, 2 AS y RETURN *").await),
        [["1", "2"]]
    );
    assert_eq!(value("WITH 1 AS __hydradb_next_column_0 RETURN *").await, "1");
    assert_eq!(value("RETURN 1 NEXT WITH 2 AS __hydradb_next_column_0 RETURN *").await, "2");
    assert_eq!(
        value("WITH 5 AS __hydradb_next_column_0 RETURN 1, __hydradb_next_column_0 NEXT RETURN *").await,
        "5"
    );
}

// ---- Review fixes, nineteenth round: another operand's relationship list is null ----

#[tokio::test]
async fn a_union_operand_leaves_another_s_relationship_list_null() {
    // A HAS match binds no `rs`: null on its row, so the second MATCH, anchored
    // on it, matches nothing there.
    assert_eq!(
        value("MATCH (a)-[rs:R]->{1,2}(b) | (a)-[:HAS]->(b) MATCH (x)-[rs:R]->{1,2}(y) RETURN count(*) AS c").await,
        "12"
    );
    assert_eq!(
        table(&run("MATCH (a)-[rs:R]->{1,2}(b) | (a)-[:HAS]->(b) RETURN count(rs) AS c, count(*) AS n").await),
        [["12", "15"]]
    );
}

// ---- Review fixes, nineteenth round: a duration over a divisor past u128 ----

#[tokio::test]
async fn a_duration_divided_past_u128_rounds_its_one_half() {
    // -2^127 ns / 2^128 is -0.5 ns exactly, which rounds away from zero;
    // anything smaller, or a larger divisor, is below a half.
    let least = "(DURATION('-PT85070591730234615865843651857.942052864S') * 2)";
    assert_eq!(
        value(&format!("RETURN {least} / 3.402823669209385e38 AS v")).await,
        "DURATION 'PT-0.000000001S'"
    );
    assert_eq!(
        value(&format!("RETURN {least} / 6.80564733841877e38 AS v")).await,
        "DURATION 'PT0S'"
    );
    assert_eq!(
        value("RETURN (DURATION('PT85070591730234615865843651857.942052863S') * 2) / 3.402823669209385e38 AS v").await,
        "DURATION 'PT0S'"
    );
}

// ---- Review fixes, nineteenth round: the least INT128's digits ----

#[tokio::test]
async fn the_least_int128_is_too_many_digits_for_a_decimal() {
    // Its 39 digits are counted from its magnitude, which no positive i128
    // holds: out of range, not a one-digit decimal.
    assert_eq!(
        status("RETURN CAST('-170141183460469231731687303715884105728' AS DECIMAL(38, 0)) AS v").await,
        GqlStatus::NUMERIC_VALUE_OUT_OF_RANGE
    );
    assert_eq!(
        value("RETURN CAST('-99999999999999999999999999999999999999' AS DECIMAL(38, 0)) AS v").await,
        "-99999999999999999999999999999999999999M"
    );
}

// ---- Review fixes, nineteenth round: EXISTS names every nested arm alike ----

#[tokio::test]
async fn exists_names_a_nested_union_s_arms_alike() {
    assert_eq!(
        value("RETURN EXISTS { RETURN 1 UNION { RETURN 1 UNION RETURN 1 } } AS v").await,
        "true"
    );
    assert_eq!(
        status("RETURN EXISTS { RETURN 1 UNION { RETURN 1 UNION RETURN 2 } } AS v").await,
        GqlStatus::FEATURE_NOT_SUPPORTED
    );
}

// ---- Review fixes, nineteenth round: NEXT's names stay readable inside its rewrite ----

#[tokio::test]
async fn a_grouped_return_before_next_reads_its_own_names() {
    // The grouping rewrite reads the unnamed count's lowering name; only `*`
    // leaves it out.
    assert_eq!(
        table(&run("UNWIND [1, 2] AS x RETURN count(*) GROUP BY x NEXT RETURN 1 AS v").await),
        [["1"], ["1"]]
    );
    assert_eq!(
        table(&run("UNWIND [1, 2, 2] AS x RETURN x, count(*) GROUP BY x NEXT RETURN x").await),
        [["1"], ["2"]]
    );
}

// ---- Review fixes, twentieth round: another path's values are out of a selective path's reach ----

#[tokio::test]
async fn a_selective_path_does_not_read_another_path_s_values() {
    // §16.6 Syntax Rule 8: another path's path and group variables are no
    // more bound while the candidates are chosen than its elements are.
    for query in [
        "MATCH REPEATABLE ELEMENTS ANY SHORTEST ((a)-[:R]->(b) WHERE path_length(q) = 1), q = (x)-[:R]->(y) RETURN count(*) AS c",
        "MATCH REPEATABLE ELEMENTS ANY SHORTEST ((a)-[:R]->(b) WHERE size(g) = 1), (((g)-[:R]->(h)){1,2}) RETURN count(*) AS c",
    ] {
        assert_eq!(status(query).await, GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION, "{query}");
    }
    assert_eq!(
        value("MATCH REPEATABLE ELEMENTS ANY SHORTEST ((a)-[:R]->(b)), q = (x)-[:R]->(y) RETURN count(*) AS c").await,
        "36"
    );
}

// ---- Review fixes, twentieth round: an operand reads an earlier pattern's path ----

#[tokio::test]
async fn a_union_operand_reads_an_earlier_pattern_s_path() {
    // `p` is bound before the union runs, as the earlier pattern's elements
    // are; a later pattern's path is not.
    for query in [
        "MATCH p = (a)-[:R]->(b), ((c) WHERE path_length(p) > 0) | (c) RETURN count(*) AS n",
        "MATCH p = (a)-[:R]->(b), ((c) WHERE EXISTS { MATCH (z) WHERE path_length(p) > 0 }) | (c) RETURN count(*) AS n",
    ] {
        assert_eq!(value(query).await, "72", "{query}");
    }
    assert_eq!(
        status("MATCH ((c) WHERE path_length(p) > 0) | (c), p = (a)-[:R]->(b) RETURN count(*) AS n").await,
        GqlStatus::FEATURE_NOT_SUPPORTED
    );
}

// ---- Review fixes, twentieth round: a FOR position is a name the query uses ----

#[tokio::test]
async fn a_for_position_spelled_like_a_lowering_name_stays_a_column() {
    for (position, first) in [("ORDINALITY", "1"), ("OFFSET", "0")] {
        let query = format!("RETURN 1 NEXT FOR x IN [10, 20] WITH {position} __hydradb_next_column_0 RETURN *");
        assert_eq!(table(&run(&query).await)[0], [first, "10"], "{query}");
    }
}

// ---- Review fixes, twentieth round: a path variable is a name the query uses ----

#[tokio::test]
async fn a_path_variable_spelled_like_a_lowering_name_stays_a_column() {
    assert_eq!(
        value("RETURN 1 NEXT MATCH __hydradb_next_column_0 = (a)-[:R]->(b) WITH * RETURN count(__hydradb_next_column_0) AS c").await,
        "6"
    );
}

// ---- Review fixes, twentieth round: a path inside a record keeps its elements ----

#[tokio::test]
async fn a_path_stored_in_a_record_unpacks_as_it_would_alone() {
    let direct = value("MATCH p = (a)-[:R]->(b) UNWIND ELEMENTS(p) AS e RETURN count(e) AS c").await;
    for query in [
        "MATCH p = (a)-[:R]->(b) WITH {p: p} AS r UNWIND ELEMENTS(r.p) AS e RETURN count(e) AS c",
        "MATCH p = (a)-[:R]->(b) WITH {q: {p: p}} AS r UNWIND ELEMENTS(r.q.p) AS e RETURN count(e) AS c",
    ] {
        assert_eq!(value(query).await, direct, "{query}");
    }
}

// ---- Review fixes, twentieth round: a time minus the least duration ----

#[tokio::test]
async fn a_time_minus_the_least_duration_wraps_at_midnight() {
    // Only the duration's part of a day moves a time of day, and that part
    // negates; 12:00 + 2^127 ns is 01:41:55.884105728 (exact arithmetic).
    let least = "DURATION('-PT85070591730234615865843651857.942052864S') * 2";
    assert_eq!(
        value(&format!("WITH {least} AS d RETURN TIME '12:00' - d AS v")).await,
        "TIME '01:41:55.884105728'"
    );
    assert_eq!(
        value("RETURN TIME '12:00' - DURATION('PT13H') AS v").await,
        "TIME '23:00:00.000000000'"
    );
    assert_eq!(
        value("RETURN TIME '12:00+02:00' - DURATION('PT25H') AS v").await,
        "TIME '11:00:00.000000000+02'"
    );
}

// ---- Review fixes, twentieth round: a sort key reads an alias or a GROUP BY key at any depth ----

#[tokio::test]
async fn a_sort_key_reads_a_grouping_key_and_an_alias_inside_an_expression() {
    // GROUP BY x keeps x a grouping column for the sort, through the item
    // that carries it.
    assert_eq!(
        table(&run("UNWIND [2, 1, 2] AS x RETURN x AS k, count(*) AS c GROUP BY x ORDER BY x + 0").await),
        [["1", "1"], ["2", "2"]]
    );
    assert_eq!(
        table(&run("UNWIND [2, 1, 2] AS x WITH x AS k, count(*) AS c GROUP BY x ORDER BY x + 0 RETURN k, c").await),
        [["1", "1"], ["2", "2"]]
    );
    // An alias inside a key reads its item, before or after grouping.
    assert_eq!(
        table(&run("UNWIND [3, 1, 2] AS x RETURN x * 2 AS v ORDER BY v + 1").await),
        [["2"], ["4"], ["6"]]
    );
    assert_eq!(
        table(&run("UNWIND [3, 1, 2] AS x RETURN x * 2 AS v ORDER BY -v LIMIT 2").await),
        [["6"], ["4"]]
    );
    assert_eq!(
        table(&run("UNWIND [1, 2, 2, 3, 3, 3] AS x RETURN x AS k, count(*) AS c ORDER BY c * -1").await),
        [["3", "3"], ["2", "2"], ["1", "1"]]
    );
    // The result's name shadows the input's.
    assert_eq!(
        table(&run("UNWIND [1, 2] AS x RETURN x AS y, -x AS x ORDER BY x").await),
        [["2", "-2"], ["1", "-1"]]
    );
    // Without GROUP BY, x is no grouping column of its own.
    assert_eq!(
        status("UNWIND [2, 1, 2] AS x RETURN x AS k, count(*) AS c ORDER BY x + 0").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
}

// ---- Review fixes, twentieth round: integer text past INT128 is a number ----

#[tokio::test]
async fn integer_text_past_int128_casts_as_the_number_it_writes() {
    for (text, expected) in [
        ("170141183460469231731687303715884105728", "1.7014118346046923e38f"),
        ("-170141183460469231731687303715884105729", "-1.7014118346046923e38f"),
    ] {
        assert_eq!(value(&format!("RETURN CAST('{text}' AS FLOAT64) AS v")).await, expected, "{text}");
    }
    // Too large for an integer type, not invalid text.
    assert_eq!(
        status("RETURN CAST('170141183460469231731687303715884105728' AS INT) AS v").await,
        GqlStatus::NUMERIC_VALUE_OUT_OF_RANGE
    );
}

// ---- Review fixes, twentieth round: an angle keeps its decimal's distance from ±1 ----

#[tokio::test]
async fn an_inverse_trigonometric_function_keeps_a_decimal_s_distance_from_one() {
    let number = |text: String| text.trim_end_matches('f').parse::<f64>().unwrap();
    // acos(1 - 1e-20) is about sqrt(2e-20), not 0.
    let acos = number(value("RETURN acos(0.99999999999999999999M) AS v").await);
    assert!((acos - 2e-20_f64.sqrt()).abs() < 1e-24, "{acos}");
    let asin = number(value("RETURN asin(0.99999999999999999999M) AS v").await);
    assert!((std::f64::consts::FRAC_PI_2 - asin - 2e-20_f64.sqrt()).abs() < 1e-15, "{asin}");
    let near_minus_one = number(value("RETURN acos(-0.99999999999999999999M) AS v").await);
    assert!(
        (std::f64::consts::PI - near_minus_one - 2e-20_f64.sqrt()).abs() < 1e-15,
        "{near_minus_one}"
    );
    // A decimal a float holds is that float's angle.
    assert_eq!(value("RETURN acos(0.5M) AS v").await, value("RETURN acos(0.5) AS v").await);
    assert_eq!(value("RETURN acos(1M) AS v").await, "0.0f");
}

// ---- Review fixes, twenty-first round: an output name in a sort key is the output ----

#[tokio::test]
async fn a_grouped_sort_key_naming_an_output_reads_the_output() {
    // `y AS x` makes x the output column: GROUP BY's input x does not
    // replace it in the sort key.
    assert_eq!(
        table(&run("UNWIND [1, 2] AS x WITH x, -x AS y RETURN x AS k, y AS x, count(*) AS c GROUP BY k, x, y ORDER BY x").await),
        [["2", "-2", "1"], ["1", "-1", "1"]]
    );
    assert_eq!(
        table(&run("UNWIND [1, 2] AS x WITH x, -x AS y RETURN y AS x, count(*) AS c GROUP BY x ORDER BY x").await),
        [["-2", "1"], ["-1", "1"]]
    );
}

// ---- Review fixes, twenty-first round: a sort key tests a computed column ----

#[tokio::test]
async fn a_sort_key_tests_a_computed_element_column() {
    // An element test reads a variable: the computed column is bound for
    // the sort, as a plain one is.
    let plain = table(&run("MATCH (n) RETURN n AS x ORDER BY x IS LABELED Hub DESC LIMIT 1").await);
    for query in [
        "MATCH (n) RETURN coalesce(n, n) AS x ORDER BY x IS LABELED Hub DESC LIMIT 1",
        "MATCH (n) RETURN DISTINCT coalesce(n, n) AS x ORDER BY x IS LABELED Hub DESC LIMIT 1",
    ] {
        assert_eq!(table(&run(query).await), plain, "{query}");
    }
    assert_eq!(
        table(&run("MATCH (n) RETURN coalesce(n, n) AS x, count(*) AS c ORDER BY x IS LABELED Hub DESC LIMIT 1").await)[0][0],
        plain[0][0]
    );
    assert_eq!(
        table(&run("MATCH (a)-[e:R]->(b) RETURN a AS s, coalesce(e, e) AS r ORDER BY s IS SOURCE OF r DESC LIMIT 1").await).len(),
        1
    );
}

// ---- Review fixes, twenty-first round: a key before NEXT naming an output reads it ----

#[tokio::test]
async fn a_sort_key_before_next_naming_an_output_reads_the_output() {
    // `-x AS x`: ORDER BY x is the output, not the input `x AS y` reads.
    assert_eq!(
        table(&run("UNWIND [1, 2] AS x RETURN DISTINCT x AS y, -x AS x ORDER BY x NEXT RETURN y, x").await),
        [["2", "-2"], ["1", "-1"]]
    );
    assert_eq!(
        table(&run("UNWIND [1, 2] AS x RETURN DISTINCT x AS y, -x AS x ORDER BY x LIMIT 1 NEXT RETURN y, x").await),
        [["2", "-2"]]
    );
}

// ---- Review fixes, twenty-first round: an exact literal's underscores sit between digits ----

#[tokio::test]
async fn an_exact_literal_s_underscores_sit_between_digits() {
    // §21.2: as the string-cast reader holds them.
    for literal in ["1_M", "1E_2M", "1E2_M", "1__0M"] {
        assert_eq!(
            status(&format!("RETURN {literal} AS v")).await,
            GqlStatus::INVALID_SYNTAX,
            "{literal}"
        );
    }
    for (literal, expected) in [("1_000M", "1000M"), ("1_000.5_5M", "1000.55M"), ("1E1_0M", "10000000000M")] {
        assert_eq!(value(&format!("RETURN {literal} AS v")).await, expected, "{literal}");
    }
}

// ---- Review fixes, twenty-first round: an ordered window in a call body ----

#[tokio::test]
async fn a_call_body_s_ordered_relationship_window_runs_under_its_seed() {
    let engine = CypherEngine::new(common::MemoryStorage::with_graph(
        (1..=4)
            .map(|id| hydradb_cypher_engine::VertexRecord::new(id).with_label("N"))
            .collect::<Vec<_>>(),
        [("b", 10, 1, 2), ("a", 11, 2, 3), ("c", 12, 3, 4)].map(|(name, id, from, to)| {
            hydradb_cypher_engine::RelationshipRecord::new(id, from, to, "R").with_property("name", ScalarValue::String(name.into()))
        }),
    ));
    let run = |query: &'static str| {
        let engine = &engine;
        async move {
            let prepared = engine
                .prepare_with_lists(query, &ParameterMap::new(), &ListParameterMap::new())
                .unwrap();
            table(&engine.execute_prepared(&prepared).await.unwrap())
        }
    };
    // `CALL ()` imports nothing, so its window runs alone, joined to each row.
    assert_eq!(
        run("CALL () { MATCH (a)-[r:R]->(b) WHERE r.name > '' RETURN r.name AS name ORDER BY r.name LIMIT 1 } RETURN name").await,
        [["\"a\""]]
    );
    assert_eq!(
        run("UNWIND [1, 2] AS k CALL () { MATCH (a)-[r:R]->(b) WHERE r.name > '' RETURN r.name AS name ORDER BY r.name DESC LIMIT 1 } RETURN k, name").await,
        [["1", "\"c\""], ["2", "\"c\""]]
    );
    // A body that reads what it imports answers per row.
    assert_eq!(
        run("UNWIND ['a', 'b'] AS k CALL (k) { MATCH (a)-[r:R]->(b) WHERE r.name > k RETURN r.name AS name ORDER BY r.name LIMIT 1 } RETURN k, name").await,
        [["\"a\"", "\"b\""], ["\"b\"", "\"c\""]]
    );
}

#[tokio::test]
async fn element_functions_read_computed_sort_columns() {
    // §14.10 SR4 and §19.11, §19.12, §20.10: output aliases remain
    // element variables even when their defining expressions are computed.
    for projection in ["", "DISTINCT "] {
        for key in [
            "same(x, x)",
            "all_different(x, x)",
            "element_id(x)",
            "coalesce(element_id(x), 'unused')",
        ] {
            let result = run(&format!("MATCH (n:Hub) RETURN {projection}coalesce(n, n) AS x ORDER BY {key}")).await;
            assert_eq!(result.rows[0].values, [hydradb_cypher_engine::QueryValue::VertexId(1010)]);
        }
    }
    for key in ["same(x, x)", "all_different(x, x)", "element_id(x)"] {
        let result = run(&format!("MATCH (n:Hub) RETURN coalesce(n, n) AS x, count(*) AS c ORDER BY {key}")).await;
        assert_eq!(result.rows[0].values[0], hydradb_cypher_engine::QueryValue::VertexId(1010));
        let result = run(&format!(
            "MATCH ()-[e:HAS]->() RETURN coalesce(e, e) AS x ORDER BY {key}, e.id LIMIT 1"
        ))
        .await;
        assert_eq!(result.rows[0].values[0], hydradb_cypher_engine::QueryValue::RelationshipId(1));
    }
}

#[tokio::test]
async fn aggregate_element_sort_aliases_preserve_input_multiplicity() {
    // §14.10, §14.11 and §20.9: sorting result columns cannot regroup
    // the aggregate's input bag, or change COUNT/SUM/collection inputs.
    use hydradb_cypher_engine::QueryValue;
    for expression in [
        "max(n)",
        "min(n)",
        "coalesce(max(n), min(n))",
        "CASE WHEN count(*) = 3 THEN max(n) ELSE 1/0 END",
    ] {
        for key in [
            "x IS LABELED Hub",
            "element_id(x)",
            "same(x,x)",
            "all_different(x,x)",
            "c + max(k), x IS LABELED Hub, element_id(x)",
        ] {
            let query = format!(
                "MATCH (n:Hub) UNWIND [1,1,2] AS k RETURN {expression} AS x, count(*) AS c, sum(k) AS s, collect(k) AS ks ORDER BY {key}"
            );
            let result = run(&query).await;
            assert_eq!(result.rows.len(), 1, "{query}");
            assert_eq!(result.rows[0].values[0], QueryValue::VertexId(1010), "{query}");
            assert_eq!(result.rows[0].values[1], QueryValue::Count(3), "{query}");
            assert_eq!(result.rows[0].values[2], QueryValue::Scalar(ScalarValue::Integer(4)), "{query}");
            assert_eq!(
                result.rows[0].values[3],
                QueryValue::List(vec![
                    QueryValue::Scalar(ScalarValue::Integer(1)),
                    QueryValue::Scalar(ScalarValue::Integer(1)),
                    QueryValue::Scalar(ScalarValue::Integer(2)),
                ]),
                "{query}"
            );
        }
    }
    let result = run("MATCH (n:Hub) UNWIND [1,1] AS k RETURN max(n) AS x, count(*) AS c ORDER BY x IS LABELED Hub").await;
    assert_eq!(result.rows[0].values, [QueryValue::VertexId(1010), QueryValue::Count(2)]);
    let result = run("MATCH (n:Hub) UNWIND [1,1,2] AS k RETURN max(n) AS x, k, count(*) ORDER BY x IS LABELED Hub, count(*) DESC, k").await;
    assert_eq!(
        result.columns.iter().map(|column| column.name.as_str()).collect::<Vec<_>>(),
        ["x", "k", "count(*)"]
    );
    assert_eq!(
        result.rows.iter().map(|row| row.values[2].clone()).collect::<Vec<_>>(),
        [QueryValue::Count(2), QueryValue::Count(1)]
    );
}

#[tokio::test]
async fn aggregate_sort_alias_scope_preserves_null_types_and_lazy_groups() {
    use hydradb_cypher_engine::QueryValue;
    let result = run("MATCH (n:Hub) UNWIND [1,1] AS k RETURN coalesce(n,n) AS x, count(*) AS c ORDER BY element_id(x), c").await;
    assert_eq!(result.rows[0].values, [QueryValue::VertexId(1010), QueryValue::Count(2)]);
    for expression in ["max(n)", "coalesce(max(n), null)"] {
        let query = format!("MATCH (n:Missing) RETURN {expression} AS x, count(*) AS c ORDER BY element_id(x)");
        let result = run(&query).await;
        assert!(result.rows[0].values[0].is_null(), "{query}");
        assert_eq!(result.rows[0].values[1], QueryValue::Count(0), "{query}");
    }
    assert!(
        run("MATCH (n:Missing) RETURN coalesce(n,n) AS x, count(*) AS c ORDER BY element_id(x)")
            .await
            .rows
            .is_empty()
    );
    assert_eq!(
        status("MATCH (n:Missing) RETURN max(n) AS x, count(*) AS c ORDER BY same(x,x)").await,
        GqlStatus::NULL_VALUE_NOT_ALLOWED
    );
    assert_eq!(
        status("MATCH (n:Hub) RETURN count(*) AS n ORDER BY n IS LABELED Hub").await,
        GqlStatus::INVALID_VALUE_TYPE
    );
    assert_eq!(
        status("MATCH (n:Hub) RETURN max(n) AS x, count(*) AS c ORDER BY element_id(c)").await,
        GqlStatus::INVALID_VALUE_TYPE
    );
    let result = run("MATCH (n:Hub) UNWIND [1,1] AS k RETURN coalesce(max(n), 1/0) AS x, count(*) AS c ORDER BY CASE WHEN c = 2 THEN element_id(x) ELSE 1/0 END").await;
    assert_eq!(result.rows[0].values, [QueryValue::VertexId(1010), QueryValue::Count(2)]);
    for query in [
        "MATCH (n:Hub) RETURN max(n) AS x, count(*) AS c ORDER BY element_id(n)",
        "MATCH (n:Hub) RETURN max(n) AS x, count(*) AS c ORDER BY element_id(coalesce(x,x))",
    ] {
        assert_eq!(status(query).await, GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION, "{query}");
    }
    assert_eq!(
        status("MATCH (n:Hub) RETURN max(n) AS x, count(*) AS c ORDER BY element_id(missing)").await,
        GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION
    );
}

#[tokio::test]
async fn aggregate_sort_scope_survives_typed_expression_recursion() {
    use hydradb_cypher_engine::QueryValue;
    for key in [
        "CAST(c AS INTEGER)",
        "c IS TYPED INTEGER",
        "RECORD {total: c}.total",
        "CAST(RECORD {total: c} AS RECORD {total INTEGER}).total",
        "CAST(x AS NODE).name",
        "CASE WHEN c = 2 THEN CAST(c AS INTEGER) ELSE 1 / 0 END",
    ] {
        let query = format!("MATCH (n:Hub) UNWIND [1,1] AS k RETURN max(n) AS x, count(*) AS c ORDER BY {key}");
        let result = run(&query).await;
        assert_eq!(result.rows.len(), 1, "{query}");
        assert_eq!(result.rows[0].values, [QueryValue::VertexId(1010), QueryValue::Count(2)], "{query}");
    }
    for (key, counts) in [
        ("CAST(c AS INTEGER) DESC, k", [3, 2, 1]),
        ("RECORD {total: c}.total DESC, k", [3, 2, 1]),
        ("CAST(RECORD {total: c} AS RECORD {total INTEGER}).total DESC, k", [3, 2, 1]),
        (
            "(CASE WHEN c = 2 THEN c ELSE null END) IS TYPED INTEGER NOT NULL DESC, k",
            [2, 1, 3],
        ),
    ] {
        let query = format!("UNWIND [3,1,3,2,3,2] AS k RETURN k, count(*) AS c ORDER BY {key}");
        let result = run(&query).await;
        let actual = result.rows.iter().map(|row| row.values[1].clone()).collect::<Vec<_>>();
        assert_eq!(actual, counts.map(QueryValue::Count), "{query}");
    }
    let result = run("MATCH (n:Node) RETURN n.name AS name, max(n) AS x, count(*) AS c ORDER BY CAST(x AS NODE).name DESC").await;
    assert_eq!(result.rows.len(), 5);
    assert_eq!(
        result.rows[0].values,
        [
            QueryValue::Scalar(ScalarValue::String("loop".into())),
            QueryValue::VertexId(1024),
            QueryValue::Count(1),
        ]
    );
    let result = run("UNWIND [] AS k RETURN count(*) AS c ORDER BY CAST(c AS INTEGER)").await;
    assert_eq!(result.rows[0].values, [QueryValue::Count(0)]);
    let inputs = std::iter::repeat_n("1", 128).collect::<Vec<_>>().join(",");
    assert_eq!(
        status(&format!("UNWIND [{inputs}] AS k RETURN count(*) AS c ORDER BY CAST(c AS INT8)")).await,
        GqlStatus::NUMERIC_VALUE_OUT_OF_RANGE
    );
}

#[tokio::test]
async fn aggregate_sort_scope_keeps_element_records_and_raw_aggregate_operands() {
    use hydradb_cypher_engine::QueryValue;
    // Aliases shadow incoming names in scalar keys, not inside aggregate
    // operands. The selected element need not come from the first group row.
    let result = run("MATCH (n:Hub) UNWIND [1,2] AS k RETURN max(n) AS k, count(*) AS n ORDER BY element_id(k), max(k) + 0, n").await;
    assert_eq!(result.rows[0].values, [QueryValue::VertexId(1010), QueryValue::Count(2)]);
    let result = run("MATCH (n:Node) UNWIND [1,1] AS k RETURN max(n) AS x, count(*) AS c ORDER BY CASE WHEN x.name = 'loop' THEN 0 ELSE 1/0 END, x IS LABELED Node").await;
    assert_eq!(result.rows[0].values, [QueryValue::VertexId(1024), QueryValue::Count(10)]);
    let result = run("MATCH (n:Hub)-[e:HAS]->() UNWIND [1,1] AS k RETURN max(n) AS s, max(e) AS r, count(*) AS c ORDER BY s IS SOURCE OF r, r IS DIRECTED, r.weight").await;
    assert_eq!(
        result.rows[0].values,
        [QueryValue::VertexId(1010), QueryValue::RelationshipId(3), QueryValue::Count(6)]
    );
    for grouping in ["", " GROUP BY k"] {
        let result = run(&format!("MATCH (n:Hub) UNWIND [1,1,2] AS k RETURN DISTINCT coalesce(max(n), null) AS x, k, count(*) AS c{grouping} ORDER BY x IS LABELED Hub, c DESC, k SKIP 1 LIMIT 1")).await;
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].values,
            [
                QueryValue::VertexId(1010),
                QueryValue::Scalar(ScalarValue::Integer(2)),
                QueryValue::Count(1)
            ]
        );
    }
}

#[tokio::test]
async fn aggregate_path_sort_scope_preserves_projected_endpoints() {
    use hydradb_cypher_engine::QueryValue;

    for key in [
        "PATH[x, min(r), z]",
        "CASE WHEN PATH[x, min(r), z] IS NULL THEN 1 / 0 ELSE c END",
        "CASE WHEN PATH[max(a), min(r), max(b)] IS NULL THEN 1 / 0 ELSE c END",
    ] {
        let query = format!(
            "MATCH (a:Node {{name: 'a'}})-[r:R]->(b:Node {{name: 'b'}}) \
             UNWIND [1,1] AS k RETURN max(a) AS x, max(b) AS z, count(*) AS c ORDER BY {key}"
        );
        let result = run(&query).await;
        assert_eq!(result.rows.len(), 1, "{query}");
        assert_eq!(
            result.rows[0].values,
            [QueryValue::VertexId(1020), QueryValue::VertexId(1021), QueryValue::Count(2)],
            "{query}"
        );
    }
    assert_eq!(
        status(
            "MATCH (a:Node {name: 'a'})-[r:R]->(b:Node {name: 'b'}) \
             UNWIND [1,1] AS k RETURN max(a) AS x, max(b) AS z, count(*) AS c \
             ORDER BY PATH[x, min(r), x]"
        )
        .await,
        GqlStatus::MALFORMED_PATH
    );
}

#[tokio::test]
async fn computed_element_sorts_preserve_null_type_laziness_and_scope() {
    // Null identity propagates, while SAME/ALL_DIFFERENT reject null (§19.11/12 GR2a).
    assert_eq!(value("RETURN coalesce(null, null) AS x ORDER BY element_id(x)").await, "null");
    for function in ["same", "all_different"] {
        assert_eq!(
            status(&format!("RETURN coalesce(null, null) AS x ORDER BY {function}(x, x)")).await,
            GqlStatus::NULL_VALUE_NOT_ALLOWED
        );
        assert_eq!(
            status(&format!("RETURN coalesce(1, 1) AS x ORDER BY {function}(x, x)")).await,
            GqlStatus::INVALID_VALUE_TYPE
        );
    }
    assert_eq!(
        status("RETURN coalesce(1, 1) AS x ORDER BY element_id(x)").await,
        GqlStatus::INVALID_VALUE_TYPE
    );
    let result = run("MATCH (n:Hub) RETURN CASE WHEN true THEN n ELSE 1/0 END AS x ORDER BY element_id(x)").await;
    assert_eq!(result.rows[0].values[0], hydradb_cypher_engine::QueryValue::VertexId(1010));
    assert_eq!(
        status("MATCH (n) RETURN coalesce(n,n) AS x ORDER BY element_id(missing)").await,
        GqlStatus::INVALID_REFERENCE
    );
    for query in [
        "MATCH (n) RETURN DISTINCT coalesce(n,n) AS x ORDER BY element_id(n)",
        "MATCH (n) RETURN coalesce(n,n) AS x, count(*) AS c ORDER BY same(n,n)",
        "MATCH (n) RETURN coalesce(n,n) AS x ORDER BY element_id(coalesce(x,x))",
    ] {
        assert_eq!(status(query).await, GqlStatus::SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION, "{query}");
    }
}

#[tokio::test]
async fn aggregate_node_identity_sort_reads_projected_aliases() {
    use hydradb_cypher_engine::QueryValue;

    // §14.10: each key reads the projected name; every group keeps both inputs.
    for alias in ["x", "n"] {
        for (direction, ids) in [("ASC", [1020, 1021, 1022, 1023, 1024]), ("DESC", [1024, 1023, 1022, 1021, 1020])] {
            let query = format!(
                "MATCH (n:Node) UNWIND [1,1] AS k \
                 RETURN n.name AS name, max(n) AS {alias}, count(*) AS c ORDER BY {alias}.id {direction}"
            );
            let result = run(&query).await;
            assert_eq!(result.rows.len(), ids.len(), "{query}");
            for (row, id) in result.rows.iter().zip(ids) {
                assert_eq!(row.values[1], QueryValue::VertexId(id), "{query}");
                assert_eq!(row.values[2], QueryValue::Count(2), "{query}");
            }
        }
    }
    assert_eq!(
        table(&run("UNWIND [] AS k RETURN max(k) AS x, count(*) AS c ORDER BY x.id").await),
        [["null", "0"]]
    );
}
