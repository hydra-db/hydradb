use hydradb_cypher_ast::{
    BinaryOperator as AstBinaryOperator, Clause, DeleteClause, Direction as AstDirection, Document, Expression, ForeachClause, Literal,
    MatchClause, MatchMode, MergeClause, MutationClause, NodePattern, PathPattern, PatternElement, ProjectionItems, PropertiesUpdate,
    Query, RemoveClause, RemoveItem, ReturnClause, SetClause, SetItem, Statement, UnaryOperator as AstUnaryOperator, ValueKeyword,
};
use hydradb_graph_plan::{
    BoundSymbol, Direction, EffectClass, EntityKind, GraphBinaryOperator, GraphExpression, GraphIr, GraphIrKind, GraphSeekPredicate,
    MutationOperation, MutationPattern, MutationPolicy, MutationProgram, MutationRelationship, MutationStep, MutationUpdate,
    MutationVertex, Projection, RelationshipUniqueness, Scope, ScopeId, Symbol, TraversalBounds, UnaryOperator,
};

use crate::error::{unsupported, DfError, DfResult};

/// Bind a typed AST into the backend-neutral graph IR without retaining any
/// request parameters, provider handles, or execution state.
pub fn bind_document(document: &Document, mutation_policy: MutationPolicy) -> DfResult<GraphIr> {
    let [Statement::Query(query)] = document.statements.as_slice() else {
        return unsupported("the DataFusion route accepts one typed query statement");
    };
    if !query.unions.is_empty() {
        return unsupported("UNION lowering is not implemented in this vertical slice");
    }
    Binder::new(mutation_policy).bind_query(query)
}

struct Binder {
    scope: Scope,
    current: GraphIr,
    mutation_policy: MutationPolicy,
    mutation_steps: Vec<MutationStep>,
    mutation_started: bool,
    next_ordinal: u64,
    next_synthetic: u64,
}

impl Binder {
    fn new(mutation_policy: MutationPolicy) -> Self {
        let scope = Scope::new(ScopeId(0));
        let current = GraphIr::new(scope.clone(), GraphIrKind::SingleRow);
        Self {
            scope,
            current,
            mutation_policy,
            mutation_steps: Vec::new(),
            mutation_started: false,
            next_ordinal: 0,
            next_synthetic: 0,
        }
    }

    fn bind_query(mut self, query: &Query) -> DfResult<GraphIr> {
        let mut returned = None;
        for clause in &query.clauses {
            match clause {
                Clause::Match(matched) if !self.mutation_started => self.bind_match(matched)?,
                Clause::Filter(filter) if !self.mutation_started => {
                    let predicate = self.expression(&filter.predicate)?;
                    if !self.pushdown_seek(&predicate) {
                        let mut needed = Vec::new();
                        collect_properties(&predicate, &mut needed);
                        for (entity, properties) in needed {
                            self.current = self.node(GraphIrKind::GraphHydrate {
                                input: Box::new(self.current.clone()),
                                entity,
                                properties,
                            });
                        }
                        self.filter(predicate);
                    }
                }
                Clause::Create(create) => {
                    let pattern = self.mutation_pattern(&create.patterns.iter().collect::<Vec<_>>())?;
                    self.push_mutation(MutationOperation::Create { pattern });
                }
                Clause::Merge(merge) => {
                    let operation = self.merge_operation(merge)?;
                    self.push_mutation(operation);
                }
                Clause::Set(set) => {
                    let operation = self.set_operation(set)?;
                    self.push_mutation(operation);
                }
                Clause::Remove(remove) => {
                    let operation = self.remove_operation(remove)?;
                    self.push_mutation(operation);
                }
                Clause::Delete(delete) => {
                    let operation = self.delete_operation(delete)?;
                    self.push_mutation(operation);
                }
                Clause::Foreach(foreach) => {
                    let operation = self.foreach_operation(foreach)?;
                    self.push_mutation(operation);
                }
                Clause::Call(_) => return unsupported("CALL subquery lowering is not implemented"),
                Clause::OptionalBlock(_) => return unsupported("OPTIONAL block lowering is not implemented"),
                Clause::OrderByAndPage(_) => return unsupported("standalone ORDER BY, SKIP, and LIMIT clauses are not implemented"),
                Clause::Finish => return unsupported("FINISH omitted-result queries are not implemented"),
                Clause::Next => return unsupported("NEXT multi-statement queries are not implemented"),
                Clause::Return(return_clause) => {
                    if returned.is_some() {
                        return unsupported("multiple RETURN clauses");
                    }
                    returned = Some(return_clause);
                }
                Clause::Match(_) | Clause::Filter(_) => {
                    return unsupported("MATCH or FILTER after a write clause is not implemented");
                }
                Clause::With(_) => return unsupported("WITH lowering is not implemented"),
                Clause::Unwind(_) => return unsupported("UNWIND lowering is not implemented"),
                Clause::Let(_) => return unsupported("LET lowering is not implemented"),
                Clause::Syntax(fragment) => {
                    return unsupported(format!("untyped clause `{}`", fragment.text()));
                }
            }
        }
        let (scope, items) = match returned {
            Some(returned) => self.projections(returned)?,
            None if self.mutation_started => (self.scope.clone(), Vec::new()),
            None => return Err(DfError::Unsupported("a read query must end in RETURN".into())),
        };
        if self.mutation_started {
            let program = MutationProgram {
                policy: self.mutation_policy,
                steps: self.mutation_steps,
                returning: items,
            };
            let effect = program.effect_class();
            debug_assert_eq!(effect, EffectClass::GraphWrite);
            Ok(GraphIr::new(
                scope,
                GraphIrKind::MutationProgram {
                    input: Box::new(self.current),
                    program,
                },
            ))
        } else {
            Ok(GraphIr::new(
                scope,
                GraphIrKind::Project {
                    input: Box::new(self.current),
                    items,
                },
            ))
        }
    }

    fn bind_match(&mut self, matched: &MatchClause) -> DfResult<()> {
        if matched.optional {
            return unsupported("OPTIONAL MATCH lowering is not implemented");
        }
        if matched.patterns.len() != 1 {
            return unsupported("one path pattern per MATCH is supported");
        }
        if matched.keep.is_some() {
            return unsupported("MATCH KEEP path selection is not implemented");
        }
        if matched.yield_items.is_some() {
            return unsupported("MATCH YIELD is not implemented");
        }
        let pattern = &matched.patterns[0];
        validate_path_pattern(pattern, "MATCH")?;
        let elements = &pattern.elements;
        let uniqueness = match matched.mode {
            Some(MatchMode::RepeatableElements) => RelationshipUniqueness::None,
            Some(MatchMode::DifferentRelationships) | None => RelationshipUniqueness::RelationshipPerPath,
        };
        let Some(PatternElement::Node(first_node)) = elements.first() else {
            return unsupported("MATCH pattern must start with a node");
        };
        let (start, property_seek) = self.bind_start_node(first_node)?;

        let mut pending_predicates = Vec::new();
        self.add_node_predicates(first_node, &start, !property_seek, &mut pending_predicates)?;
        let mut last_node = start.clone();
        let mut prior_relationships = Vec::new();
        let (pairs, trailing_elements) = elements[1..].as_chunks::<2>();
        if !trailing_elements.is_empty() {
            return unsupported("MATCH pattern must end with a node");
        }
        for pair in pairs {
            let (PatternElement::Relationship(relationship), PatternElement::Node(target_node)) = (&pair[0], &pair[1]) else {
                return unsupported("MATCH patterns must be linear node-relationship paths");
            };
            let from = last_node.clone();
            let target_name = target_node.variable.as_ref().map_or("", |name| name.as_str());
            let existing_target = if target_name.is_empty() {
                None
            } else {
                self.lookup_optional(target_name)
            };
            if existing_target.as_ref().is_some_and(|binding| binding.kind != EntityKind::Node) {
                return Err(DfError::Invalid(format!("variable `{target_name}` is not a node")));
            }
            let target = match existing_target.clone() {
                Some(target) => target,
                None => self.bind_node(target_node)?,
            };
            if !target_node.labels.is_empty() {
                return unsupported("labels on expanded target nodes are not represented by this graph IR slice");
            }
            if target_node.properties.is_some() {
                return unsupported("property constraints on expanded target nodes are not implemented");
            }
            let relationship_type = match relationship.types.as_slice() {
                [relationship_type] => relationship_type,
                [] => return unsupported("a relationship pattern must declare one relationship type"),
                _ => return unsupported("multiple relationship types on one pattern are not implemented"),
            };
            let variable = self.bind_relationship(relationship)?;
            let direction = direction(relationship.direction)?;
            if let Some(length) = relationship.length {
                if !relationship
                    .properties
                    .as_ref()
                    .is_none_or(|properties| matches!(properties, Expression::Map(_)))
                {
                    return unsupported("variable-length relationship property maps are not implemented");
                }
                let min_hops = length.minimum.unwrap_or(1);
                let max_hops = length.maximum;
                self.current = self.node(GraphIrKind::VariableExpand {
                    input: Box::new(self.current.clone()),
                    from,
                    relationship: Some(variable.clone()),
                    to: target.clone(),
                    path: None,
                    types: vec![Symbol::from(relationship_type.as_str())],
                    direction,
                    bounds: TraversalBounds {
                        minimum_hops: min_hops,
                        maximum_hops: max_hops,
                    },
                    uniqueness,
                    excluded_relationships: if uniqueness == RelationshipUniqueness::RelationshipPerPath {
                        prior_relationships.clone()
                    } else {
                        Vec::new()
                    },
                });
            } else {
                self.current = self.node(GraphIrKind::Expand {
                    input: Box::new(self.current.clone()),
                    from,
                    relationship: variable.clone(),
                    to: target.clone(),
                    types: vec![Symbol::from(relationship_type.as_str())],
                    direction,
                    mode: if existing_target.is_some() {
                        hydradb_graph_plan::ExpansionMode::Into
                    } else {
                        hydradb_graph_plan::ExpansionMode::Expand
                    },
                    uniqueness,
                    excluded_relationships: if uniqueness == RelationshipUniqueness::RelationshipPerPath {
                        prior_relationships.clone()
                    } else {
                        Vec::new()
                    },
                });
            }
            prior_relationships.push(variable.clone());
            last_node = target.clone();
            self.add_node_predicates(target_node, &target, true, &mut pending_predicates)?;
            self.add_relationship_predicates(relationship, &variable, &mut pending_predicates)?;
        }
        if elements.len().is_multiple_of(2) {
            return unsupported("MATCH pattern must end with a node");
        }
        if let Some(predicate) = &matched.predicate {
            pending_predicates.push(self.expression(predicate)?);
        }
        if pending_predicates.len() == 1 && self.pushdown_seek(&pending_predicates[0]) {
            pending_predicates.clear();
        }
        self.install_hydration(&pending_predicates);
        for predicate in pending_predicates {
            self.filter(predicate);
        }
        Ok(())
    }

    fn bind_start_node(&mut self, node: &NodePattern) -> DfResult<(BoundSymbol, bool)> {
        if !matches!(&self.current.kind, GraphIrKind::SingleRow) {
            return unsupported("additional or disconnected MATCH patterns are not implemented");
        }
        let binding = self.bind_node(node)?;
        let properties = node_properties(node)?;
        if !node.labels.is_empty() || properties.len() != 1 || !matches!(properties[0].1, Expression::Literal(_)) {
            self.current = self.node(GraphIrKind::NodeScan {
                input: None,
                binding: binding.clone(),
                labels: node.labels.iter().map(|label| Symbol::from(label.as_str())).collect(),
            });
            return Ok((binding, false));
        }
        let value = self.expression(properties[0].1)?;
        if !node.labels.is_empty() {
            if properties[0].0.as_str() != "id" {
                return unsupported("a property-seek node with labels needs provider index intersection support");
            }
            self.current = self.node(GraphIrKind::NodeScan {
                input: None,
                binding: binding.clone(),
                labels: node.labels.iter().map(|label| Symbol::from(label.as_str())).collect(),
            });
            self.filter(GraphExpression::Binary {
                left: Box::new(GraphExpression::Binding(binding.clone())),
                operator: GraphBinaryOperator::Equal,
                right: Box::new(value),
            });
            return Ok((binding, false));
        }
        let predicate = if properties[0].0.as_str() == "id" {
            GraphSeekPredicate::Id(value)
        } else {
            GraphSeekPredicate::PropertyEquals {
                property: Symbol::from(properties[0].0.as_str()),
                value,
            }
        };
        self.current = self.node(GraphIrKind::GraphSeek {
            input: None,
            binding: binding.clone(),
            predicate,
        });
        Ok((binding, true))
    }

    fn bind_node(&mut self, node: &NodePattern) -> DfResult<BoundSymbol> {
        if node.label_expression.is_some() {
            return unsupported("GQL node label expressions are not represented by this graph IR slice");
        }
        let name = node
            .variable
            .as_ref()
            .map_or_else(|| self.synthetic("node"), |name| name.as_str().to_owned());
        self.bind_name(&name, EntityKind::Node)
    }

    fn bind_mutation_node(&mut self, node: &NodePattern) -> DfResult<BoundSymbol> {
        if node.label_expression.is_some() {
            return unsupported("GQL node label expressions are not represented by this graph IR slice");
        }
        if let Some(name) = &node.variable {
            if let Some(binding) = self.lookup_optional(name.as_str()) {
                if binding.kind != EntityKind::Node {
                    return Err(DfError::Invalid(format!("variable `{name}` is not a node")));
                }
                return Ok(binding);
            }
        }
        self.bind_node(node)
    }

    fn bind_relationship(&mut self, relationship: &hydradb_cypher_ast::RelationshipPattern) -> DfResult<BoundSymbol> {
        if relationship.label_expression.is_some() {
            return unsupported("GQL relationship type expressions are not represented by this graph IR slice");
        }
        let name = relationship
            .variable
            .as_ref()
            .map_or_else(|| self.synthetic("relationship"), |name| name.as_str().to_owned());
        self.bind_name(
            &name,
            if relationship.length.is_some() {
                EntityKind::List
            } else {
                EntityKind::Relationship
            },
        )
    }

    fn bind_mutation_relationship(&mut self, relationship: &hydradb_cypher_ast::RelationshipPattern) -> DfResult<BoundSymbol> {
        if relationship.label_expression.is_some() {
            return unsupported("GQL relationship type expressions are not represented by this graph IR slice");
        }
        if let Some(name) = &relationship.variable {
            if let Some(binding) = self.lookup_optional(name.as_str()) {
                if !matches!(binding.kind, EntityKind::Relationship | EntityKind::List) {
                    return Err(DfError::Invalid(format!("variable `{name}` is not a relationship")));
                }
                return Ok(binding);
            }
        }
        self.bind_relationship(relationship)
    }

    fn bind_name(&mut self, name: &str, kind: EntityKind) -> DfResult<BoundSymbol> {
        if self.scope.bindings.iter().any(|binding| binding.symbol.as_str() == name) {
            return Err(DfError::Invalid(format!("variable `{name}` is bound more than once")));
        }
        Ok(self.scope.bind(name, kind))
    }

    fn add_node_predicates(
        &self,
        node: &NodePattern,
        binding: &BoundSymbol,
        include_properties: bool,
        predicates: &mut Vec<GraphExpression>,
    ) -> DfResult<()> {
        if include_properties {
            if let Some(properties) = node.properties.as_ref() {
                for (name, value) in expression_map(properties)? {
                    predicates.push(GraphExpression::Binary {
                        left: Box::new(GraphExpression::Property {
                            entity: binding.clone(),
                            property: Symbol::from(name.as_str()),
                        }),
                        operator: GraphBinaryOperator::Equal,
                        right: Box::new(self.expression(value)?),
                    });
                }
            }
        }
        if let Some(predicate) = &node.predicate {
            predicates.push(self.expression(predicate)?);
        }
        Ok(())
    }

    fn add_relationship_predicates(
        &self,
        relationship: &hydradb_cypher_ast::RelationshipPattern,
        binding: &BoundSymbol,
        predicates: &mut Vec<GraphExpression>,
    ) -> DfResult<()> {
        if let Some(properties) = relationship.properties.as_ref() {
            if relationship.length.is_some() {
                return unsupported("variable-length relationship property predicates are not implemented");
            }
            for (name, value) in expression_map(properties)? {
                predicates.push(GraphExpression::Binary {
                    left: Box::new(GraphExpression::Property {
                        entity: binding.clone(),
                        property: Symbol::from(name.as_str()),
                    }),
                    operator: GraphBinaryOperator::Equal,
                    right: Box::new(self.expression(value)?),
                });
            }
        }
        if let Some(predicate) = &relationship.predicate {
            predicates.push(self.expression(predicate)?);
        }
        Ok(())
    }

    fn install_hydration(&mut self, predicates: &[GraphExpression]) {
        let mut needed = Vec::<(BoundSymbol, Vec<Symbol>)>::new();
        for predicate in predicates {
            collect_properties(predicate, &mut needed);
        }
        for (entity, properties) in needed {
            self.current = self.node(GraphIrKind::GraphHydrate {
                input: Box::new(self.current.clone()),
                entity,
                properties,
            });
        }
    }

    fn pushdown_seek(&mut self, predicate: &GraphExpression) -> bool {
        let GraphIrKind::NodeScan {
            input: None,
            binding,
            labels,
        } = &self.current.kind
        else {
            return false;
        };
        if !labels.is_empty() {
            return false;
        }
        let (property, value) = match predicate {
            GraphExpression::Binary {
                left,
                operator: GraphBinaryOperator::Equal,
                right,
            } => match (left.as_ref(), right.as_ref()) {
                (GraphExpression::Property { entity, property }, value) if entity == binding && is_seek_value(value) => {
                    (property.clone(), value.clone())
                }
                (value, GraphExpression::Property { entity, property }) if entity == binding && is_seek_value(value) => {
                    (property.clone(), value.clone())
                }
                _ => return false,
            },
            _ => return false,
        };
        let binding = binding.clone();
        let predicate = if property.as_str() == "id" {
            GraphSeekPredicate::Id(value)
        } else {
            GraphSeekPredicate::PropertyEquals { property, value }
        };
        self.current = self.node(GraphIrKind::GraphSeek {
            input: None,
            binding,
            predicate,
        });
        true
    }

    fn filter(&mut self, predicate: GraphExpression) {
        self.current = self.node(GraphIrKind::Filter {
            input: Box::new(self.current.clone()),
            predicate,
        });
    }

    fn projections(&mut self, returned: &ReturnClause) -> DfResult<(Scope, Vec<Projection>)> {
        let ProjectionItems::Items(items) = &returned.items else {
            return unsupported("RETURN * lowering is not implemented");
        };
        if returned.quantifier.is_some()
            || !returned.group_by.is_empty()
            || !returned.order_by.is_empty()
            || returned.skip.is_some()
            || returned.limit.is_some()
        {
            return unsupported("DISTINCT, aggregation, ORDER BY, SKIP, and LIMIT are not implemented");
        }
        // Every expression resolves against the input scope. Output names must
        // neither collide with nor become visible to sibling expressions.
        let mut output_scope = Scope::nested(ScopeId(self.scope.id.0 + 1), self.scope.id);
        let projections = items
            .iter()
            .map(|item| {
                let expression = self.expression(&item.expression)?;
                let alias_kind = match &expression {
                    GraphExpression::Binding(binding) => binding.kind,
                    _ => EntityKind::Scalar,
                };
                let alias = item
                    .alias
                    .as_ref()
                    .map(|alias| {
                        if output_scope
                            .bindings
                            .iter()
                            .any(|binding| binding.symbol.as_str() == alias.as_str())
                        {
                            return Err(DfError::Invalid(format!("output name `{alias}` is bound more than once")));
                        }
                        Ok(output_scope.bind(alias.as_str(), alias_kind))
                    })
                    .transpose()?;
                if alias.is_none() {
                    if let GraphExpression::Binding(binding) = &expression {
                        output_scope.bind(binding.symbol.clone(), binding.kind);
                    }
                }
                Ok(Projection { expression, alias })
            })
            .collect::<DfResult<Vec<_>>>()?;
        if !self.mutation_started {
            let mut needed = Vec::new();
            for projection in &projections {
                collect_properties(&projection.expression, &mut needed);
            }
            for (entity, properties) in needed {
                self.current = self.node(GraphIrKind::GraphHydrate {
                    input: Box::new(self.current.clone()),
                    entity,
                    properties,
                });
            }
        }
        Ok((output_scope, projections))
    }

    fn expression(&self, expression: &Expression) -> DfResult<GraphExpression> {
        Ok(match expression {
            Expression::Identifier(identifier) => GraphExpression::Binding(self.lookup(identifier.as_str())?),
            Expression::Parameter(parameter) => GraphExpression::Parameter(Symbol::from(parameter.as_str())),
            Expression::Literal(literal) => GraphExpression::Literal(match literal {
                Literal::Null => hydradb_graph_plan::ScalarValue::Null,
                Literal::Boolean(value) => hydradb_graph_plan::ScalarValue::Boolean(*value),
                Literal::Integer(value) => hydradb_graph_plan::ScalarValue::Integer(*value),
                Literal::Float(value) => hydradb_graph_plan::ScalarValue::Float(value.clone()),
                Literal::String(value) => hydradb_graph_plan::ScalarValue::String(value.clone()),
                Literal::Decimal(_) => return unsupported("decimal literals are not represented by this graph IR slice"),
                Literal::Bytes(_) => return unsupported("byte string literals are not represented by this graph IR slice"),
            }),
            Expression::Property { target, key } => {
                let Expression::Identifier(identifier) = target.as_ref() else {
                    return unsupported("property access must target a bound variable");
                };
                GraphExpression::Property {
                    entity: self.lookup(identifier.as_str())?,
                    property: Symbol::from(key.as_str()),
                }
            }
            // `list[index]` travels as an internal function, so everything
            // that walks GraphExpression already handles its two arguments.
            Expression::Index { target, index } => GraphExpression::Function {
                name: Symbol::from(hydradb_graph_datafusion::value_construction::LIST_ELEMENT_FUNCTION),
                arguments: vec![self.expression(target)?, self.expression(index)?],
                distinct: false,
            },
            Expression::Unary { operator, expression } => {
                let operator = match operator {
                    AstUnaryOperator::Not => UnaryOperator::Not,
                    AstUnaryOperator::Plus => return self.expression(expression),
                    AstUnaryOperator::Minus => UnaryOperator::Negate,
                    AstUnaryOperator::IsNull => UnaryOperator::IsNull,
                    AstUnaryOperator::IsNotNull => UnaryOperator::IsNotNull,
                    AstUnaryOperator::IsTrue => return unsupported("GQL IS TRUE predicates are not implemented"),
                    AstUnaryOperator::IsNotTrue => return unsupported("GQL IS NOT TRUE predicates are not implemented"),
                    AstUnaryOperator::IsFalse => return unsupported("GQL IS FALSE predicates are not implemented"),
                    AstUnaryOperator::IsNotFalse => return unsupported("GQL IS NOT FALSE predicates are not implemented"),
                    AstUnaryOperator::IsUnknown => return unsupported("GQL IS UNKNOWN predicates are not implemented"),
                    AstUnaryOperator::IsNotUnknown => return unsupported("GQL IS NOT UNKNOWN predicates are not implemented"),
                };
                GraphExpression::Unary {
                    operator,
                    expression: Box::new(self.expression(expression)?),
                }
            }
            Expression::Binary { left, operator, right } => GraphExpression::Binary {
                left: Box::new(self.expression(left)?),
                operator: binary_operator(*operator)?,
                right: Box::new(self.expression(right)?),
            },
            Expression::Function { name, distinct, arguments } => GraphExpression::Function {
                name: Symbol::from(name.iter().map(|part| part.as_str()).collect::<Vec<_>>().join(".")),
                arguments: arguments
                    .iter()
                    .map(|argument| self.expression(argument))
                    .collect::<DfResult<_>>()?,
                distinct: *distinct,
            },
            Expression::List(values) => GraphExpression::List(values.iter().map(|value| self.expression(value)).collect::<DfResult<_>>()?),
            Expression::Map(entries) => GraphExpression::Map(
                entries
                    .iter()
                    .map(|(key, value)| Ok((Symbol::from(key.as_str()), self.expression(value)?)))
                    .collect::<DfResult<_>>()?,
            ),
            Expression::Case {
                operand,
                branches,
                default,
            } => {
                let branches = branches
                    .iter()
                    .map(|(condition, value)| {
                        let condition = self.expression(condition)?;
                        let condition = if let Some(operand) = operand {
                            GraphExpression::Binary {
                                left: Box::new(self.expression(operand)?),
                                operator: GraphBinaryOperator::Equal,
                                right: Box::new(condition),
                            }
                        } else {
                            condition
                        };
                        Ok((condition, self.expression(value)?))
                    })
                    .collect::<DfResult<_>>()?;
                GraphExpression::Case {
                    branches,
                    fallback: default.as_ref().map(|value| self.expression(value).map(Box::new)).transpose()?,
                }
            }
            Expression::Exists(_) => return unsupported("EXISTS subquery expressions are not implemented"),
            Expression::Value(_) => return unsupported("VALUE subquery expressions are not implemented"),
            Expression::Normalized { .. } => return unsupported("GQL NORMALIZED predicates are not implemented"),
            Expression::SessionUser => return unsupported("SESSION_USER requires a session value not available during binding"),
            Expression::ValueKeyword { keyword, spelling } => {
                let binding = self.lookup_optional(spelling.as_str()).or_else(|| {
                    self.scope
                        .bindings
                        .iter()
                        .rev()
                        .find(|binding| binding.symbol.as_str().eq_ignore_ascii_case(spelling.as_str()))
                        .cloned()
                });
                if let Some(binding) = binding {
                    GraphExpression::Binding(binding)
                } else {
                    match keyword {
                        ValueKeyword::Unknown => GraphExpression::Literal(hydradb_graph_plan::ScalarValue::Null),
                        ValueKeyword::SessionUser => {
                            return unsupported("SESSION_USER requires a session value not available during binding");
                        }
                        ValueKeyword::CurrentTemporal(_) => return unsupported("current temporal value keywords are not implemented"),
                    }
                }
            }
            Expression::Let { .. } => return unsupported("GQL LET expressions are not implemented"),
            Expression::PathConstructor(_) => return unsupported("GQL PATH constructors are not implemented"),
            Expression::TemporalLiteral { .. } => return unsupported("GQL temporal literals are not represented by this graph IR slice"),
            Expression::TypeTest { .. } => return unsupported("GQL type-test predicates are not implemented"),
            Expression::Cast { .. } => return unsupported("GQL CAST expressions are not implemented"),
            Expression::ElementTest { .. } => return unsupported("GQL element-test predicates are not implemented"),
            Expression::Syntax(fragment) => return unsupported(format!("untyped expression `{}`", fragment.text())),
        })
    }

    fn lookup(&self, name: &str) -> DfResult<BoundSymbol> {
        self.lookup_optional(name)
            .ok_or_else(|| DfError::Invalid(format!("unknown variable `{name}`")))
    }

    fn lookup_optional(&self, name: &str) -> Option<BoundSymbol> {
        self.scope
            .bindings
            .iter()
            .rev()
            .find(|binding| binding.symbol.as_str() == name)
            .cloned()
    }

    fn node(&self, kind: GraphIrKind) -> GraphIr {
        GraphIr::new(self.scope.clone(), kind)
    }

    fn synthetic(&mut self, kind: &str) -> String {
        let id = self.next_synthetic;
        self.next_synthetic = self.next_synthetic.wrapping_add(1);
        format!("__cypher_df_{kind}_{id}")
    }

    fn push_mutation(&mut self, operation: MutationOperation) {
        self.mutation_started = true;
        self.mutation_steps.push(MutationStep {
            ordinal: self.next_ordinal,
            operation,
        });
        self.next_ordinal = self.next_ordinal.wrapping_add(1);
    }

    fn mutation_pattern(&mut self, patterns: &[&PathPattern]) -> DfResult<MutationPattern> {
        let mut output = MutationPattern::default();
        for path in patterns {
            validate_path_pattern(path, "mutation")?;
            let mut previous: Option<(BoundSymbol, (BoundSymbol, &hydradb_cypher_ast::RelationshipPattern))> = None;
            let mut last_node = None;
            for element in &path.elements {
                match element {
                    PatternElement::Node(node) => {
                        if node.predicate.is_some() {
                            return unsupported("node predicates are not valid in CREATE/MERGE pattern lowering");
                        }
                        let binding = self.bind_mutation_node(node)?;
                        output.vertices.push(MutationVertex {
                            binding: Some(binding.clone()),
                            labels: node.labels.iter().map(|label| Symbol::from(label.as_str())).collect(),
                            properties: node_properties(node)?
                                .iter()
                                .map(|(name, value)| Ok((Symbol::from(name.as_str()), self.expression(value)?)))
                                .collect::<DfResult<_>>()?,
                        });
                        if let Some((from, relationship)) = previous.take() {
                            output.relationships.push(MutationRelationship {
                                binding: Some(relationship.0),
                                from: Some(from),
                                to: Some(binding.clone()),
                                types: relationship.1.types.iter().map(|ty| Symbol::from(ty.as_str())).collect(),
                                direction: direction(relationship.1.direction)?,
                                properties: relationship_properties(relationship.1)?
                                    .iter()
                                    .map(|(name, value)| Ok((Symbol::from(name.as_str()), self.expression(value)?)))
                                    .collect::<DfResult<_>>()?,
                            });
                        }
                        last_node = Some(binding);
                    }
                    PatternElement::Relationship(relationship) => {
                        if relationship.predicate.is_some() {
                            return unsupported("relationship predicates are not valid in mutation pattern lowering");
                        }
                        if previous.is_some() {
                            return unsupported("adjacent relationships without a node are invalid");
                        }
                        if relationship.length.is_some() {
                            return unsupported("variable-length mutation patterns are not implemented");
                        }
                        let binding = self.bind_mutation_relationship(relationship)?;
                        let from = last_node
                            .clone()
                            .ok_or_else(|| DfError::Invalid("mutation relationship must follow a node".into()))?;
                        previous = Some((from, (binding, relationship)));
                    }
                }
            }
            if previous.is_some() {
                return Err(DfError::Invalid("mutation path must end in a node".into()));
            }
        }
        Ok(output)
    }

    fn merge_operation(&mut self, merge: &MergeClause) -> DfResult<MutationOperation> {
        let pattern = self.mutation_pattern(&[&merge.pattern])?;
        Ok(MutationOperation::Merge {
            pattern,
            on_match: merge
                .on_match
                .iter()
                .map(|item| self.mutation_update(item))
                .collect::<DfResult<_>>()?,
            on_create: merge
                .on_create
                .iter()
                .map(|item| self.mutation_update(item))
                .collect::<DfResult<_>>()?,
        })
    }

    fn set_operation(&self, set: &SetClause) -> DfResult<MutationOperation> {
        Ok(MutationOperation::Set {
            updates: set.items.iter().map(|item| self.mutation_update(item)).collect::<DfResult<_>>()?,
        })
    }

    fn mutation_update(&self, item: &SetItem) -> DfResult<MutationUpdate> {
        match item {
            SetItem::Property { target, property, value } => {
                let Expression::Identifier(entity) = target else {
                    return unsupported("SET property target must be a binding");
                };
                Ok(MutationUpdate {
                    target: GraphExpression::Property {
                        entity: self.lookup(entity.as_str())?,
                        property: Symbol::from(property.as_str()),
                    },
                    value: self.expression(value)?,
                })
            }
            SetItem::Properties { operator, .. } => match operator {
                PropertiesUpdate::Replace => unsupported("SET n = map needs a replace-properties mutation operator"),
                PropertiesUpdate::Merge => unsupported("SET n += map needs a merge-properties mutation operator"),
            },
            SetItem::Labels { .. } => unsupported("SET label mutations are not represented by this graph IR slice"),
        }
    }

    fn remove_operation(&self, remove: &RemoveClause) -> DfResult<MutationOperation> {
        let targets = remove
            .items
            .iter()
            .map(|item| match item {
                RemoveItem::Property { target, property } => {
                    let Expression::Identifier(entity) = target else {
                        return unsupported("REMOVE property target must be a binding");
                    };
                    Ok(GraphExpression::Property {
                        entity: self.lookup(entity.as_str())?,
                        property: Symbol::from(property.as_str()),
                    })
                }
                RemoveItem::Labels { .. } => unsupported("REMOVE label mutations are not implemented"),
            })
            .collect::<DfResult<_>>()?;
        Ok(MutationOperation::Remove { targets })
    }

    fn delete_operation(&self, delete: &DeleteClause) -> DfResult<MutationOperation> {
        Ok(MutationOperation::Delete {
            targets: delete
                .expressions
                .iter()
                .map(|target| self.expression(target))
                .collect::<DfResult<_>>()?,
            detach: delete.detach,
        })
    }

    fn foreach_operation(&mut self, foreach: &ForeachClause) -> DfResult<MutationOperation> {
        let list = self.expression(&foreach.expression)?;
        if self.lookup_optional(foreach.variable.as_str()).is_some() {
            return Err(DfError::Invalid(format!(
                "FOREACH variable `{}` shadows an existing binding",
                foreach.variable
            )));
        }
        let binding_count = self.scope.bindings.len();
        let variable = self.scope.bind(foreach.variable.as_str(), EntityKind::Scalar);
        let steps = foreach
            .clauses
            .iter()
            .map(|clause| match clause {
                MutationClause::Create(create) => Ok(MutationOperation::Create {
                    pattern: self.mutation_pattern(&create.patterns.iter().collect::<Vec<_>>())?,
                }),
                MutationClause::Merge(merge) => self.merge_operation(merge.as_ref()),
                MutationClause::Set(set) => self.set_operation(set),
                MutationClause::Remove(remove) => self.remove_operation(remove),
                MutationClause::Delete(delete) => self.delete_operation(delete),
                MutationClause::Foreach(inner) => self.foreach_operation(inner),
            })
            .collect::<DfResult<_>>()?;
        self.scope.bindings.truncate(binding_count);
        Ok(MutationOperation::Foreach { variable, list, steps })
    }
}

fn direction(direction: AstDirection) -> DfResult<Direction> {
    Ok(match direction {
        AstDirection::Outgoing => Direction::Outgoing,
        AstDirection::Incoming => Direction::Incoming,
        AstDirection::Undirected => Direction::Undirected,
        AstDirection::Bidirectional => return unsupported("bidirectional relationship paths are not represented by this graph IR"),
        AstDirection::UndirectedEdge => return unsupported("GQL undirected-edge paths are not represented by this graph IR"),
        AstDirection::LeftOrUndirected => return unsupported("GQL left-or-undirected paths are not represented by this graph IR"),
        AstDirection::UndirectedOrRight => return unsupported("GQL undirected-or-right paths are not represented by this graph IR"),
    })
}

fn validate_path_pattern(path: &PathPattern, context: &str) -> DfResult<()> {
    if path.binding.is_some() || !path.subpaths.is_empty() {
        return unsupported(format!("path and subpath bindings in {context} patterns are not implemented"));
    }
    if path.search.is_some() || path.search_prefix_written {
        return unsupported(format!("GQL path search in {context} patterns is not implemented"));
    }
    if !path.modes.is_empty() {
        return unsupported(format!("GQL path modes in {context} patterns are not implemented"));
    }
    if path.predicate.is_some() {
        return unsupported(format!("parenthesized path predicates in {context} patterns are not implemented"));
    }
    if path.alternatives.is_some() {
        return unsupported(format!(
            "path unions and multiset alternation in {context} patterns are not implemented"
        ));
    }
    Ok(())
}

fn binary_operator(operator: AstBinaryOperator) -> DfResult<GraphBinaryOperator> {
    Ok(match operator {
        AstBinaryOperator::Equal => GraphBinaryOperator::Equal,
        AstBinaryOperator::NotEqual => GraphBinaryOperator::NotEqual,
        AstBinaryOperator::LessThan => GraphBinaryOperator::LessThan,
        AstBinaryOperator::LessThanOrEqual => GraphBinaryOperator::LessThanOrEqual,
        AstBinaryOperator::GreaterThan => GraphBinaryOperator::GreaterThan,
        AstBinaryOperator::GreaterThanOrEqual => GraphBinaryOperator::GreaterThanOrEqual,
        AstBinaryOperator::And => GraphBinaryOperator::And,
        AstBinaryOperator::Or => GraphBinaryOperator::Or,
        AstBinaryOperator::Add => GraphBinaryOperator::Add,
        AstBinaryOperator::Subtract => GraphBinaryOperator::Subtract,
        AstBinaryOperator::Multiply => GraphBinaryOperator::Multiply,
        AstBinaryOperator::Divide => GraphBinaryOperator::Divide,
        AstBinaryOperator::In => GraphBinaryOperator::In,
        AstBinaryOperator::StartsWith => GraphBinaryOperator::StartsWith,
        AstBinaryOperator::EndsWith => return unsupported("ENDS WITH expressions are not implemented"),
        AstBinaryOperator::Contains => return unsupported("CONTAINS expressions are not implemented"),
        AstBinaryOperator::Xor => return unsupported("XOR expressions are not implemented"),
        AstBinaryOperator::Modulo => return unsupported("modulo expressions are not implemented"),
        AstBinaryOperator::Power => return unsupported("power expressions are not implemented"),
        AstBinaryOperator::Concatenate => return unsupported("list concatenation is not implemented"),
    })
}

fn node_properties(node: &NodePattern) -> DfResult<Vec<(hydradb_cypher_ast::Identifier, &Expression)>> {
    node.properties.as_ref().map_or(Ok(Vec::new()), expression_map)
}

fn relationship_properties(
    relationship: &hydradb_cypher_ast::RelationshipPattern,
) -> DfResult<Vec<(hydradb_cypher_ast::Identifier, &Expression)>> {
    relationship.properties.as_ref().map_or(Ok(Vec::new()), expression_map)
}

fn expression_map(expression: &Expression) -> DfResult<Vec<(hydradb_cypher_ast::Identifier, &Expression)>> {
    match expression {
        Expression::Map(entries) => Ok(entries.iter().map(|(key, value)| (key.clone(), value)).collect()),
        _ => unsupported("pattern property constraints must be a map literal"),
    }
}

fn is_seek_value(expression: &GraphExpression) -> bool {
    match expression {
        GraphExpression::Parameter(_) | GraphExpression::Literal(_) => true,
        GraphExpression::Unary {
            operator: UnaryOperator::Negate,
            expression,
        } => is_seek_value(expression),
        _ => false,
    }
}
fn collect_properties(expression: &GraphExpression, output: &mut Vec<(BoundSymbol, Vec<Symbol>)>) {
    match expression {
        GraphExpression::Property { entity, property } => {
            if let Some((_, properties)) = output.iter_mut().find(|(binding, _)| binding == entity) {
                if !properties.contains(property) {
                    properties.push(property.clone());
                }
            } else {
                output.push((entity.clone(), vec![property.clone()]));
            }
        }
        GraphExpression::Binary { left, right, .. } => {
            collect_properties(left, output);
            collect_properties(right, output);
        }
        GraphExpression::Unary { expression, .. } => collect_properties(expression, output),
        GraphExpression::Function { arguments, .. } | GraphExpression::List(arguments) => {
            for argument in arguments {
                collect_properties(argument, output);
            }
        }
        GraphExpression::Map(entries) => {
            for (_, value) in entries {
                collect_properties(value, output);
            }
        }
        GraphExpression::Case { branches, fallback } => {
            for (condition, value) in branches {
                collect_properties(condition, output);
                collect_properties(value, output);
            }
            if let Some(fallback) = fallback {
                collect_properties(fallback, output);
            }
        }
        GraphExpression::Binding(_) | GraphExpression::Parameter(_) | GraphExpression::Literal(_) => {}
    }
}
