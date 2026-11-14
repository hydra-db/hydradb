use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{Column, DFSchema, DFSchemaRef, ScalarValue as DfScalarValue};
use datafusion::logical_expr::{col, lit, BinaryExpr, Expr, LogicalPlan, LogicalPlanBuilder, Operator};
use hydradb_graph_datafusion::logical::{
    graph_extension, graph_mutation_program, Direction as DfDirection, GraphOperator, MutationAtomicity,
    RelationshipUniqueness as DfUniqueness,
};
use hydradb_graph_plan::{
    BoundSymbol, Direction, EntityKind, GraphBinaryOperator, GraphExpression, GraphIr, GraphIrKind, GraphSeekPredicate, MutationPolicy,
    Projection, RelationshipUniqueness, ScalarValue, Symbol,
};
use hydradb_graph_types::{canonical_value_data_type, value_array, CypherValue as GraphValue, CypherValueTag, DateValue};

use crate::error::{unsupported, DfError, DfResult};

/// Execution-specific inputs to graph IR lowering. It must be constructed for
/// each invocation and is never retained by [`crate::PreparedTemplate`].
pub struct LoweringState<'a> {
    pub parameters: &'a BTreeMap<String, GraphValue>,
    pub batch_size: usize,
    pub mutation_policy: MutationPolicy,
}

/// Lower a bound graph plan to DataFusion logical operators and graph
/// extension nodes. The resulting tree is per execution because parameter
/// values are materialized here; the reusable template remains parameter-free.
pub fn lower_graph_ir(ir: &GraphIr, state: &LoweringState<'_>) -> DfResult<LogicalPlan> {
    if state.batch_size == 0 {
        return Err(DfError::Invalid("graph batch size must be positive".into()));
    }
    lower(ir, state)
}

fn lower(ir: &GraphIr, state: &LoweringState<'_>) -> DfResult<LogicalPlan> {
    match &ir.kind {
        GraphIrKind::SingleRow => Ok(LogicalPlanBuilder::empty(true).build()?),
        GraphIrKind::NodeScan {
            input: None,
            binding,
            labels,
        } => graph_extension(
            GraphOperator::GraphScan {
                binding: binding.symbol.as_str().to_owned(),
                labels: labels.iter().map(|label| label.as_str().to_owned()).collect(),
                batch_size: state.batch_size,
            },
            Vec::new(),
            Vec::new(),
            schema_for_bindings([binding])?,
        )
        .map_err(Into::into),
        GraphIrKind::NodeScan { input: Some(_), .. } => unsupported("correlated or cross-product node scans are not implemented"),
        GraphIrKind::GraphSeek {
            input: None,
            binding,
            predicate,
        } => {
            let internal_id = matches!(predicate, GraphSeekPredicate::Id(_));
            let (property, value) = match predicate {
                GraphSeekPredicate::Id(value) => ("id", value),
                GraphSeekPredicate::PropertyEquals { property, value } => (property.as_str(), value),
                GraphSeekPredicate::PropertyIn { .. } => return unsupported("IN index seeks are not implemented"),
                GraphSeekPredicate::Composite(_) => return unsupported("composite index seeks are not implemented"),
            };
            if !internal_id && expression_resolves_to_unindexed_property(value, state)? {
                let scan = graph_extension(
                    GraphOperator::GraphScan {
                        binding: binding.symbol.as_str().to_owned(),
                        labels: Vec::new(),
                        batch_size: state.batch_size,
                    },
                    Vec::new(),
                    Vec::new(),
                    schema_for_bindings([binding])?,
                )?;
                let property_name = property_column(binding, &hydradb_graph_plan::Symbol::from(property));
                let mut fields: Vec<Field> = scan.schema().fields().iter().map(|field| field.as_ref().clone()).collect();
                fields.push(Field::new(property_name.clone(), canonical_value_data_type(), true));
                let hydrated = graph_extension(
                    GraphOperator::GraphHydrate {
                        binding: binding.symbol.as_str().to_owned(),
                        properties: vec![property.to_owned()],
                        batch_size: state.batch_size,
                    },
                    vec![scan],
                    Vec::new(),
                    df_schema(fields)?,
                )?;
                let predicate = hydradb_graph_datafusion::semantic_comparison::semantic_comparison(
                    GraphBinaryOperator::Equal,
                    col(property_name),
                    to_expr(value, state)?,
                )
                .ok_or_else(|| DfError::Invalid("property equality must support semantic comparison".into()))?;
                return Ok(LogicalPlanBuilder::from(hydrated).filter(predicate)?.build()?);
            }
            let value_index = 0_u32;
            let expression = to_expr(value, state)?;
            graph_extension(
                GraphOperator::GraphSeek {
                    binding: binding.symbol.as_str().to_owned(),
                    property: property.to_owned(),
                    internal_id,
                    value_expression: value_index,
                    batch_size: state.batch_size,
                },
                Vec::new(),
                vec![expression],
                schema_for_bindings([binding])?,
            )
            .map_err(Into::into)
        }
        GraphIrKind::GraphSeek { input: Some(_), .. } => unsupported("correlated graph seeks are not implemented"),
        GraphIrKind::Expand {
            input,
            from,
            relationship,
            to,
            types,
            direction,
            mode,
            uniqueness,
            excluded_relationships,
        } => {
            let relationship_type = exactly_one_type(types)?;
            let source = lower(input, state)?;
            let schema = schema_with_bindings(source.schema(), [relationship, to])?;
            let operator = match mode {
                hydradb_graph_plan::ExpansionMode::Expand => GraphOperator::Expand {
                    source: from.symbol.as_str().to_owned(),
                    target: to.symbol.as_str().to_owned(),
                    relationship: relationship.symbol.as_str().to_owned(),
                    relationship_type,
                    direction: df_direction(*direction),
                    uniqueness: df_uniqueness(*uniqueness)?,
                    excluded_relationships: excluded_relationship_names(*uniqueness, excluded_relationships),
                    batch_size: state.batch_size,
                },
                hydradb_graph_plan::ExpansionMode::Into => GraphOperator::ExpandInto {
                    source: from.symbol.as_str().to_owned(),
                    target: to.symbol.as_str().to_owned(),
                    relationship: relationship.symbol.as_str().to_owned(),
                    relationship_type,
                    direction: df_direction(*direction),
                    uniqueness: df_uniqueness(*uniqueness)?,
                    excluded_relationships: excluded_relationship_names(*uniqueness, excluded_relationships),
                    batch_size: state.batch_size,
                },
            };
            graph_extension(operator, vec![source], Vec::new(), schema).map_err(Into::into)
        }
        GraphIrKind::VariableExpand {
            input,
            from,
            relationship,
            to,
            path: _,
            types,
            direction,
            bounds,
            uniqueness,
            excluded_relationships,
        } => {
            let relationship_type = exactly_one_type(types)?;
            let max_hops = bounds
                .maximum_hops
                .ok_or_else(|| DfError::Unsupported("unbounded variable-length expansion is not implemented".into()))?;
            let relationship = relationship
                .as_ref()
                .ok_or_else(|| DfError::Invalid("variable expansion has no relationship binding".into()))?;
            let source = lower(input, state)?;
            let schema = schema_with_bindings(source.schema(), [relationship, to])?;
            graph_extension(
                GraphOperator::VariableExpand {
                    source: from.symbol.as_str().to_owned(),
                    target: to.symbol.as_str().to_owned(),
                    relationship: relationship.symbol.as_str().to_owned(),
                    relationship_type,
                    direction: df_direction(*direction),
                    min_hops: bounds.minimum_hops,
                    max_hops,
                    uniqueness: df_uniqueness(*uniqueness)?,
                    excluded_relationships: excluded_relationship_names(*uniqueness, excluded_relationships),
                    batch_size: state.batch_size,
                },
                vec![source],
                Vec::new(),
                schema,
            )
            .map_err(Into::into)
        }
        GraphIrKind::GraphHydrate { input, entity, properties } => {
            let input = lower(input, state)?;
            let mut fields: Vec<Field> = input.schema().fields().iter().map(|field| field.as_ref().clone()).collect();
            for property in properties {
                let name = property_column(entity, property);
                if fields.iter().all(|field| field.name() != &name) {
                    fields.push(Field::new(name, canonical_value_data_type(), true));
                }
            }
            let schema = df_schema(fields)?;
            graph_extension(
                GraphOperator::GraphHydrate {
                    binding: entity.symbol.as_str().to_owned(),
                    properties: properties.iter().map(|property| property.as_str().to_owned()).collect(),
                    batch_size: state.batch_size,
                },
                vec![input],
                Vec::new(),
                schema,
            )
            .map_err(Into::into)
        }
        GraphIrKind::Filter { input, predicate } => {
            let input = lower(input, state)?;
            let predicate = to_expr(predicate, state)?;
            Ok(LogicalPlanBuilder::from(input).filter(predicate)?.build()?)
        }
        GraphIrKind::Project { input, items } => {
            let input = lower(input, state)?;
            let expressions = projections(items, state)?;
            Ok(LogicalPlanBuilder::from(input).project(expressions)?.build()?)
        }
        GraphIrKind::MutationProgram { input, program } => {
            hydradb_graph_datafusion::mutation_runtime::validate_mutation_program(program)?;
            if program.policy != state.mutation_policy {
                return Err(DfError::Invalid(
                    "execution mutation policy differs from the prepared template".into(),
                ));
            }
            let input = lower(input, state)?;
            let schema = schema_for_projections(&program.returning, state)?;
            graph_mutation_program(
                input,
                program.clone(),
                match state.mutation_policy {
                    MutationPolicy::Statement => MutationAtomicity::Statement,
                    MutationPolicy::LegacyChunked => MutationAtomicity::LegacyChunked,
                },
                Vec::new(),
                schema,
            )
            .map_err(Into::into)
        }
        GraphIrKind::RelationshipScan { .. } => unsupported("relationship scans are not implemented by this graph extension"),
        GraphIrKind::Exists { .. } => unsupported("EXISTS subquery lowering is not implemented"),
        GraphIrKind::OptionalApply { .. } => unsupported("optional apply lowering is not implemented"),
        GraphIrKind::CorrelatedApply { .. } => unsupported("correlated apply lowering is not implemented"),
        GraphIrKind::Unwind { .. } => unsupported("UNWIND lowering is not implemented"),
        GraphIrKind::Union { .. } => unsupported("UNION lowering is not implemented"),
        GraphIrKind::Aggregation { .. } => unsupported("aggregation lowering is not implemented"),
        GraphIrKind::Sort { .. } => unsupported("sort lowering is not implemented"),
        GraphIrKind::Window { .. } => unsupported("window lowering is not implemented"),
        GraphIrKind::ResultWindow { .. } => unsupported("SKIP/LIMIT lowering is not implemented"),
        GraphIrKind::GraphProcedure { .. } => unsupported("procedure execution is not implemented"),
    }
}

fn expression_resolves_to_unindexed_property(expression: &GraphExpression, state: &LoweringState<'_>) -> DfResult<bool> {
    Ok(matches!(
        static_expression_value(expression, state)?,
        GraphValue::Date(_) | GraphValue::List(_)
    ))
}

fn projections(items: &[Projection], state: &LoweringState<'_>) -> DfResult<Vec<Expr>> {
    items
        .iter()
        .enumerate()
        .map(|(index, projection)| {
            let expression = to_expr(&projection.expression, state)?;
            Ok(match &projection.alias {
                Some(alias) => expression.alias(alias.symbol.as_str()),
                None if matches!(projection.expression, GraphExpression::Binding(_)) => expression,
                None => expression.alias(format!("__cypher_df_return_{index}")),
            })
        })
        .collect()
}

fn to_expr(expression: &GraphExpression, state: &LoweringState<'_>) -> DfResult<Expr> {
    Ok(match expression {
        GraphExpression::Binding(binding) => col(binding.symbol.as_str()),
        GraphExpression::Property { entity, property } => Expr::Column(Column::new_unqualified(property_column(entity, property))),
        GraphExpression::Parameter(name) => {
            let value = state
                .parameters
                .get(name.as_str())
                .ok_or_else(|| DfError::MissingParameter(name.as_str().to_owned()))?;
            lit(value_to_df_scalar(value)?)
        }
        GraphExpression::Literal(value) => lit(scalar_to_df(value)?),
        GraphExpression::Binary { left, operator, right } => {
            let integer_division = *operator == GraphBinaryOperator::Divide
                && expression_type(left, state)? == DataType::Decimal128(38, 0)
                && expression_type(right, state)? == DataType::Decimal128(38, 0);
            let compare_canonical_values = hydradb_graph_datafusion::semantic_comparison::is_semantic_comparison_operator(*operator)
                && (expression_type(left, state)? == canonical_value_data_type()
                    || expression_type(right, state)? == canonical_value_data_type());
            let left = Box::new(to_expr(left, state)?);
            let right = Box::new(to_expr(right, state)?);
            match operator {
                GraphBinaryOperator::In => hydradb_graph_datafusion::semantic_comparison::semantic_membership(*left, *right),
                GraphBinaryOperator::StartsWith => return unsupported("STARTS WITH expression lowering is not implemented"),
                GraphBinaryOperator::EndsWith => return unsupported("ENDS WITH expression lowering is not implemented"),
                GraphBinaryOperator::Contains => return unsupported("CONTAINS expression lowering is not implemented"),
                _ if compare_canonical_values => {
                    match hydradb_graph_datafusion::semantic_comparison::semantic_comparison(*operator, *left, *right) {
                        Some(expression) => expression,
                        None => return unsupported("unsupported canonical graph comparison operator"),
                    }
                }
                GraphBinaryOperator::Divide if integer_division => {
                    hydradb_graph_datafusion::integer_division::integer_division(*left, *right)
                }
                _ => Expr::BinaryExpr(BinaryExpr::new(left, df_operator(*operator)?, right)),
            }
        }
        GraphExpression::Unary { operator, expression } => {
            if matches!(
                operator,
                hydradb_graph_plan::UnaryOperator::IsNull | hydradb_graph_plan::UnaryOperator::IsNotNull
            ) && expression_type(expression, state)? == canonical_value_data_type()
            {
                // Canonical Null and Missing are valid Arrow struct rows. Their
                // tags, not the outer validity bitmap, define Cypher nullness.
                let tag = datafusion::functions::core::expr_fn::get_field(to_expr(expression, state)?, "tag");
                let is_null = tag
                    .clone()
                    .eq(lit(CypherValueTag::Null as u8))
                    .or(tag.eq(lit(CypherValueTag::Missing as u8)));
                return Ok(if matches!(operator, hydradb_graph_plan::UnaryOperator::IsNotNull) {
                    Expr::Not(Box::new(is_null))
                } else {
                    is_null
                });
            }
            let expression = Box::new(to_expr(expression, state)?);
            match operator {
                hydradb_graph_plan::UnaryOperator::Not => Expr::Not(expression),
                hydradb_graph_plan::UnaryOperator::Negate => Expr::Negative(expression),
                hydradb_graph_plan::UnaryOperator::IsNull => Expr::IsNull(expression),
                hydradb_graph_plan::UnaryOperator::IsNotNull => Expr::IsNotNull(expression),
            }
        }
        GraphExpression::Function { name, arguments, distinct } => {
            if *distinct {
                return unsupported("DISTINCT scalar functions are not implemented");
            }
            match name.as_str().to_ascii_lowercase().as_str() {
                "date" => {
                    let [argument] = arguments.as_slice() else {
                        return Err(DfError::Invalid("date requires one argument".into()));
                    };
                    hydradb_graph_datafusion::value_construction::date_value(to_expr(argument, state)?)
                }
                hydradb_graph_datafusion::value_construction::LIST_ELEMENT_FUNCTION => {
                    let [list, index] = arguments.as_slice() else {
                        return Err(DfError::Invalid("list element access takes a list and an index".into()));
                    };
                    hydradb_graph_datafusion::value_construction::list_element_value(to_expr(list, state)?, to_expr(index, state)?)
                }
                "tointeger" | "toint" => {
                    let [argument] = arguments.as_slice() else {
                        return unsupported("toInteger expects one argument");
                    };
                    hydradb_graph_datafusion::to_integer::to_integer(to_expr(argument, state)?)
                }
                _ => return unsupported("function lowering is not implemented"),
            }
        }
        GraphExpression::List(values) if values.is_empty() => lit(graph_value_to_df_scalar(&GraphValue::List(Vec::new()))?),
        GraphExpression::List(values) => hydradb_graph_datafusion::value_construction::list_value(
            values
                .iter()
                .map(|value| {
                    if supports_static_value(value) {
                        Ok(lit(graph_value_to_df_scalar(&static_expression_value(value, state)?)?))
                    } else {
                        to_expr(value, state)
                    }
                })
                .collect::<DfResult<Vec<_>>>()?,
        ),
        GraphExpression::Map(_) => return unsupported("map expression lowering is not implemented"),
        GraphExpression::Case { .. } => return unsupported("CASE expression lowering is not implemented"),
    })
}

fn value_to_df_scalar(value: &GraphValue) -> DfResult<DfScalarValue> {
    Ok(match value {
        GraphValue::Null => DfScalarValue::Null,
        GraphValue::Boolean(value) => DfScalarValue::Boolean(Some(*value)),
        GraphValue::Integer(value) => DfScalarValue::Decimal128(Some(*value), 38, 0),
        GraphValue::Float(value) => DfScalarValue::Float64(Some(*value)),
        GraphValue::String(value) => DfScalarValue::Utf8(Some(value.clone())),
        GraphValue::Missing => return unsupported("MISSING is not a scalar query parameter"),
        _ => graph_value_to_df_scalar(value)?,
    })
}

fn scalar_to_df(value: &ScalarValue) -> DfResult<DfScalarValue> {
    Ok(match value {
        ScalarValue::Null => DfScalarValue::Null,
        ScalarValue::Boolean(value) => DfScalarValue::Boolean(Some(*value)),
        ScalarValue::Integer(value) => DfScalarValue::Decimal128(Some(*value), 38, 0),
        ScalarValue::Float(value) => DfScalarValue::Float64(Some(
            value
                .parse()
                .map_err(|_| DfError::Invalid(format!("invalid floating-point literal `{value}`")))?,
        )),
        ScalarValue::String(value) => DfScalarValue::Utf8(Some(value.to_string())),
    })
}

fn df_operator(operator: GraphBinaryOperator) -> DfResult<Operator> {
    Ok(match operator {
        GraphBinaryOperator::Equal => Operator::Eq,
        GraphBinaryOperator::NotEqual => Operator::NotEq,
        GraphBinaryOperator::LessThan => Operator::Lt,
        GraphBinaryOperator::LessThanOrEqual => Operator::LtEq,
        GraphBinaryOperator::GreaterThan => Operator::Gt,
        GraphBinaryOperator::GreaterThanOrEqual => Operator::GtEq,
        GraphBinaryOperator::And => Operator::And,
        GraphBinaryOperator::Or => Operator::Or,
        GraphBinaryOperator::Add => Operator::Plus,
        GraphBinaryOperator::Subtract => Operator::Minus,
        GraphBinaryOperator::Multiply => Operator::Multiply,
        GraphBinaryOperator::Divide => Operator::Divide,
        GraphBinaryOperator::In => return unsupported("IN expressions require list-specific lowering"),
        GraphBinaryOperator::StartsWith | GraphBinaryOperator::EndsWith | GraphBinaryOperator::Contains => {
            return unsupported("string predicate lowering is not implemented")
        }
    })
}

fn schema_for_bindings<'a>(bindings: impl IntoIterator<Item = &'a BoundSymbol>) -> DfResult<DFSchemaRef> {
    let fields = bindings
        .into_iter()
        .map(|binding| Field::new(binding.symbol.as_str(), binding_type(binding.kind), binding.nullable))
        .collect();
    df_schema(fields)
}

fn schema_with_bindings<'a>(schema: &DFSchemaRef, bindings: impl IntoIterator<Item = &'a BoundSymbol>) -> DfResult<DFSchemaRef> {
    let mut fields: Vec<_> = schema.fields().iter().map(|field| field.as_ref().clone()).collect();
    for binding in bindings {
        if fields.iter().all(|field| field.name() != binding.symbol.as_str()) {
            fields.push(Field::new(binding.symbol.as_str(), binding_type(binding.kind), binding.nullable));
        }
    }
    df_schema(fields)
}

fn schema_for_projections(projections: &[Projection], state: &LoweringState<'_>) -> DfResult<DFSchemaRef> {
    let fields = projections
        .iter()
        .enumerate()
        .map(|(index, projection)| {
            let name = projection
                .alias
                .as_ref()
                .map_or_else(|| format!("__cypher_df_return_{index}"), |alias| alias.symbol.as_str().to_owned());
            Ok(Field::new(name, expression_type(&projection.expression, state)?, true))
        })
        .collect::<DfResult<Vec<_>>>()?;
    df_schema(fields)
}

fn expression_type(expression: &GraphExpression, state: &LoweringState<'_>) -> DfResult<DataType> {
    Ok(match expression {
        GraphExpression::Binding(binding) => binding_type(binding.kind),
        GraphExpression::Property { .. } => canonical_value_data_type(),
        GraphExpression::Parameter(name) => {
            let value = state
                .parameters
                .get(name.as_str())
                .ok_or_else(|| DfError::MissingParameter(name.as_str().to_owned()))?;
            value_to_df_scalar(value)?.data_type()
        }
        GraphExpression::Literal(value) => scalar_to_df(value)?.data_type(),
        GraphExpression::Binary { left, operator, right } => match operator {
            GraphBinaryOperator::Add | GraphBinaryOperator::Subtract | GraphBinaryOperator::Multiply | GraphBinaryOperator::Divide => {
                numeric_result_type(left, right, state)?
            }
            GraphBinaryOperator::And
            | GraphBinaryOperator::Or
            | GraphBinaryOperator::Equal
            | GraphBinaryOperator::NotEqual
            | GraphBinaryOperator::LessThan
            | GraphBinaryOperator::LessThanOrEqual
            | GraphBinaryOperator::GreaterThan
            | GraphBinaryOperator::GreaterThanOrEqual
            | GraphBinaryOperator::In
            | GraphBinaryOperator::StartsWith
            | GraphBinaryOperator::EndsWith
            | GraphBinaryOperator::Contains => DataType::Boolean,
        },
        GraphExpression::Unary { operator, expression } => match operator {
            hydradb_graph_plan::UnaryOperator::Not
            | hydradb_graph_plan::UnaryOperator::IsNull
            | hydradb_graph_plan::UnaryOperator::IsNotNull => DataType::Boolean,
            hydradb_graph_plan::UnaryOperator::Negate => numeric_operand_type(expression, state)?,
        },
        GraphExpression::Function { name, distinct, .. } => {
            if *distinct {
                return unsupported("DISTINCT scalar functions are not implemented");
            }
            match name.as_str().to_ascii_lowercase().as_str() {
                "coalesce" | "date" | hydradb_graph_datafusion::value_construction::LIST_ELEMENT_FUNCTION => canonical_value_data_type(),
                "tointeger" | "toint" => DataType::Decimal128(38, 0),
                _ => return unsupported("result type inference is not implemented for this function"),
            }
        }
        GraphExpression::List(_) => canonical_value_data_type(),
        GraphExpression::Map(_) | GraphExpression::Case { .. } => {
            return unsupported("result type inference is not implemented for this expression")
        }
    })
}

fn graph_value_to_df_scalar(value: &GraphValue) -> DfResult<DfScalarValue> {
    let array = value_array(std::slice::from_ref(value)).map_err(|error| DfError::Invalid(error.to_string()))?;
    Ok(DfScalarValue::try_from_array(&array, 0)?)
}

fn static_expression_value(expression: &GraphExpression, state: &LoweringState<'_>) -> DfResult<GraphValue> {
    match expression {
        GraphExpression::Parameter(name) => state
            .parameters
            .get(name.as_str())
            .cloned()
            .ok_or_else(|| DfError::MissingParameter(name.as_str().to_owned())),
        GraphExpression::Literal(value) => Ok(match value {
            ScalarValue::Null => GraphValue::Null,
            ScalarValue::Boolean(value) => GraphValue::Boolean(*value),
            ScalarValue::Integer(value) => GraphValue::Integer(*value),
            ScalarValue::Float(value) => GraphValue::Float(
                value
                    .parse()
                    .map_err(|_| DfError::Invalid(format!("invalid floating-point literal `{value}`")))?,
            ),
            ScalarValue::String(value) => GraphValue::String(value.to_string()),
        }),
        GraphExpression::Unary {
            operator: hydradb_graph_plan::UnaryOperator::Negate,
            expression,
        } => match static_expression_value(expression, state)? {
            GraphValue::Integer(value) => value
                .checked_neg()
                .map(GraphValue::Integer)
                .ok_or_else(|| DfError::Invalid("integer negation overflow".into())),
            GraphValue::Float(value) => Ok(GraphValue::Float(-value)),
            GraphValue::Null => Ok(GraphValue::Null),
            _ => Err(DfError::Invalid("unary negation requires a numeric value".into())),
        },
        GraphExpression::List(values) => values
            .iter()
            .map(|value| static_expression_value(value, state))
            .collect::<DfResult<Vec<_>>>()
            .map(GraphValue::List),
        GraphExpression::Function {
            name,
            arguments,
            distinct: false,
        } if name.as_str().eq_ignore_ascii_case("date") => match arguments.as_slice() {
            [argument] => match static_expression_value(argument, state)? {
                GraphValue::String(value) => DateValue::parse_iso(&value)
                    .map(GraphValue::Date)
                    .ok_or_else(|| DfError::Invalid(format!("invalid date `{value}`; expected an ISO 8601 date such as YYYY-MM-DD"))),
                GraphValue::Date(value) => Ok(GraphValue::Date(value)),
                GraphValue::Null | GraphValue::Missing => Ok(GraphValue::Null),
                _ => Err(DfError::Invalid("date requires one string or date argument".into())),
            },
            _ => Err(DfError::Invalid("date requires one argument".into())),
        },
        _ => unsupported("expression must be static for DataFusion literal lowering"),
    }
}

fn supports_static_value(expression: &GraphExpression) -> bool {
    match expression {
        GraphExpression::Parameter(_) | GraphExpression::Literal(_) => true,
        GraphExpression::Unary {
            operator: hydradb_graph_plan::UnaryOperator::Negate,
            expression,
        } => supports_static_value(expression),
        GraphExpression::List(values) => values.iter().all(supports_static_value),
        GraphExpression::Function {
            name,
            arguments,
            distinct: false,
        } if name.as_str().eq_ignore_ascii_case("date") => {
            matches!(arguments.as_slice(), [argument] if supports_static_value(argument))
        }
        _ => false,
    }
}

fn numeric_result_type(left: &GraphExpression, right: &GraphExpression, state: &LoweringState<'_>) -> DfResult<DataType> {
    let left = numeric_operand_type(left, state)?;
    let right = numeric_operand_type(right, state)?;
    if left == DataType::Float64 || right == DataType::Float64 {
        Ok(DataType::Float64)
    } else {
        Ok(DataType::Decimal128(38, 0))
    }
}

fn numeric_operand_type(expression: &GraphExpression, state: &LoweringState<'_>) -> DfResult<DataType> {
    match expression_type(expression, state)? {
        DataType::Float64 => Ok(DataType::Float64),
        DataType::Decimal128(_, _) => Ok(DataType::Decimal128(38, 0)),
        _ => unsupported("arithmetic RETURN expressions require numeric operands"),
    }
}

fn binding_type(kind: EntityKind) -> DataType {
    match kind {
        EntityKind::Node => DataType::UInt64,
        EntityKind::Relationship => hydradb_graph_types::canonical_relationship_binding_schema()
            .field(0)
            .data_type()
            .clone(),
        EntityKind::Path => hydradb_graph_types::canonical_path_field("path", true).data_type().clone(),
        EntityKind::Scalar | EntityKind::Map | EntityKind::Any => canonical_value_data_type(),
        EntityKind::List => DataType::List(Arc::new(Field::new("item", canonical_value_data_type(), true))),
    }
}

fn property_column(entity: &BoundSymbol, property: &Symbol) -> String {
    format!("{}.{}", entity.symbol.as_str(), property.as_str())
}

fn exactly_one_type(types: &[Symbol]) -> DfResult<String> {
    match types {
        [ty] => Ok(ty.as_str().to_owned()),
        [] => unsupported("untyped relationship expansion is not implemented"),
        _ => unsupported("multi-type relationship expansion is not implemented"),
    }
}

fn df_uniqueness(uniqueness: RelationshipUniqueness) -> DfResult<DfUniqueness> {
    match uniqueness {
        RelationshipUniqueness::RelationshipPerPath => Ok(DfUniqueness::Path),
        RelationshipUniqueness::None => Ok(DfUniqueness::None),
        RelationshipUniqueness::RelationshipPerBranch => unsupported("relationship-per-branch uniqueness is not implemented"),
    }
}

fn excluded_relationship_names(uniqueness: RelationshipUniqueness, excluded: &[BoundSymbol]) -> Vec<String> {
    match uniqueness {
        RelationshipUniqueness::RelationshipPerPath => excluded.iter().map(|binding| binding.symbol.as_str().to_owned()).collect(),
        RelationshipUniqueness::None | RelationshipUniqueness::RelationshipPerBranch => Vec::new(),
    }
}

fn df_direction(direction: Direction) -> DfDirection {
    match direction {
        Direction::Outgoing => DfDirection::Outgoing,
        Direction::Incoming => DfDirection::Incoming,
        Direction::Undirected => DfDirection::Undirected,
    }
}

fn df_schema(fields: Vec<Field>) -> DfResult<DFSchemaRef> {
    let schema = Schema::new(fields);
    Ok(Arc::new(DFSchema::try_from(schema)?))
}
